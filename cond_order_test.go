package ctxsync

import (
	"context"
	"slices"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startQueuedInOrder starts one waiter per context with startWaiter, one at a time, and waits
// after each start until the waiter is queued. It requires the first waiter's record to be at the
// front of the queue once all of them are queued, so a test can rely on which waiter a Signal
// wakes. It must run in a synctest bubble.
func startQueuedInOrder(t *testing.T, c *Cond, contexts ...context.Context) []<-chan error {
	t.Helper()
	results := make([]<-chan error, 0, len(contexts))
	var front *waiter
	for _, ctx := range contexts {
		results = append(results, startWaiter(ctx, c))
		synctest.Wait()
		if front == nil {
			front = frontRecord(c)
			require.NotNil(t, front, "the first waiter did not join the queue")
		}
	}
	require.Equal(t, len(contexts), queueLen(c))
	require.Same(t, front, frontRecord(c), "the first waiter is no longer at the front")
	return results
}

func TestCondCancelWhileQueued(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctxA, cancelA := context.WithCancel(t.Context())
		defer cancelA()
		ctxB, cancelB := context.WithCancel(t.Context())
		defer cancelB()
		cond := NewCond(make(chanLock, 1))
		results := startQueuedInOrder(t, cond, ctxA, ctxB)
		resultsA, resultsB := results[0], results[1]

		cancelA()
		synctest.Wait()
		assertCondWaitResult(t, cond, resultsA, cancelA, context.Canceled)
		assert.Empty(t, resultsB, "canceling A woke B")
		assert.Equal(t, 1, queueLen(cond))

		cond.Signal()
		synctest.Wait()
		assertCondWaitResult(t, cond, resultsB, cancelB, nil)
	})
}

// TestCondPassOn cancels A and wakes it with a Signal in one hold of the internal mutex, so A can
// decide only after both. A was confirmed at the front, so the Signal went to A, and B can return
// nil only through A's pass-on.
func TestCondPassOn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctxA, cancelA := context.WithCancel(t.Context())
		defer cancelA()
		ctxB, cancelB := context.WithCancel(t.Context())
		defer cancelB()
		cond := NewCond(make(chanLock, 1))
		results := startQueuedInOrder(t, cond, ctxA, ctxB)
		resultsA, resultsB := results[0], results[1]

		cond.mu.Lock()
		cancelA()
		cond.wakeFrontLocked(wokenBySignal)
		cond.mu.Unlock()
		synctest.Wait()
		assertCondWaitResult(t, cond, resultsA, cancelA, context.Canceled)
		assertCondWaitResult(t, cond, resultsB, cancelB, nil)
	})
}

// TestCondRelockBeforeDeciding checks that Wait decides its result only after it locks L again.
// The test holds L while it signals A, so A is woken with a live context but parked in L.Lock, and
// the test cancels A before releasing L. An implementation that decides before relocking makes A
// return nil, because its context was live when it decided, and leaves B queued.
func TestCondRelockBeforeDeciding(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctxA, cancelA := context.WithCancel(t.Context())
		defer cancelA()
		ctxB, cancelB := context.WithCancel(t.Context())
		defer cancelB()
		cond := NewCond(make(chanLock, 1))
		results := startQueuedInOrder(t, cond, ctxA, ctxB)
		resultsA, resultsB := results[0], results[1]

		cond.L.Lock()
		cond.Signal()
		synctest.Wait()
		cancelA()
		cond.L.Unlock()
		synctest.Wait()
		assertCondWaitResult(t, cond, resultsA, cancelA, context.Canceled)
		assertCondWaitResult(t, cond, resultsB, cancelB, nil)
	})
}

// TestCondPassOnChain calls decide directly, because two goroutines cannot force this chain: B can
// take L and remove itself from the queue before A passes the signal on.
func TestCondPassOnChain(t *testing.T) {
	canceled := make(chan struct{})
	close(canceled)
	live := make(chan struct{})
	cond := NewCond(make(chanLock, 1))
	first, second, third := newRecord(), newRecord(), newRecord()
	cond.enqueue(first)
	cond.enqueue(second)
	cond.enqueue(third)
	cond.Signal()

	firstDecided := false
	require.False(t, cond.decide(first, canceled, &firstDecided))
	assert.True(t, firstDecided)
	require.Equal(t, wokenBySignal, second.woken, "the first record did not pass the signal on")
	assert.True(t, isClosed(second.ready))
	assert.True(t, third.queued, "the first record's pass-on woke more than one record")
	assert.Equal(t, notWoken, third.woken)
	assert.False(t, isClosed(third.ready))

	secondDecided := false
	require.False(t, cond.decide(second, canceled, &secondDecided))
	assert.True(t, secondDecided)
	require.Equal(t, wokenBySignal, third.woken, "the second record did not pass the signal on")
	assert.True(t, isClosed(third.ready))

	thirdDecided := false
	assert.True(t, cond.decide(third, live, &thirdDecided))
	assert.True(t, thirdDecided)
	assert.Equal(t, 0, queueLen(cond))
}

// TestCondPassOnDropped checks that a pass-on that finds an empty queue is dropped, as a Signal
// with no waiters is, and does not wake a waiter that starts later.
func TestCondPassOnDropped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		canceled := make(chan struct{})
		close(canceled)
		cond := NewCond(make(chanLock, 1))
		record := newRecord()
		cond.enqueue(record)
		cond.Signal()
		decided := false
		require.False(t, cond.decide(record, canceled, &decided))
		require.Equal(t, 0, queueLen(cond))

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		results := startWaiter(ctx, cond)
		synctest.Wait()
		assert.Empty(t, results, "a dropped pass-on woke a later waiter")
		assert.Equal(t, 1, queueLen(cond))

		cancel()
		synctest.Wait()
		assertCondWaitResult(t, cond, results, cancel, context.Canceled)
	})
}

// TestCondBroadcastPassesNothing checks that a waiter that Broadcast woke and that is canceled
// before it decides passes nothing on. The record added after the Broadcast was not queued when
// Broadcast ran, so waking it would be a spurious pass-on.
func TestCondBroadcastPassesNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		cond := NewCond(make(chanLock, 1))
		results := startWaiter(ctx, cond)
		synctest.Wait()
		require.Equal(t, 1, queueLen(cond))

		cond.L.Lock()
		cond.Broadcast()
		synctest.Wait()
		late := newRecord()
		cond.enqueue(late)
		cancel()
		cond.L.Unlock()
		synctest.Wait()
		assertCondWaitResult(t, cond, results, cancel, context.Canceled)

		cond.mu.Lock()
		lateQueued, lateWoken := late.queued, late.woken
		cond.mu.Unlock()
		assert.True(t, lateQueued, "the canceled waiter passed a Broadcast on")
		assert.Equal(t, notWoken, lateWoken)
	})
}

func TestCondPreCanceledTakesNoSignal(t *testing.T) {
	t.Run("NeverJoinsQueue", func(t *testing.T) {
		locker := &queueRecordingLocker{lock: make(chanLock, 1)}
		cond := NewCond(locker)
		locker.cond = cond
		preCanceled, cancelPre := context.WithCancel(t.Context())
		cancelPre()

		cond.L.Lock()
		err := cond.Wait(preCanceled)
		cond.L.Unlock()
		assert.ErrorIs(t, err, context.Canceled)
		// The first Unlock is Wait's and the second is the test's after Wait returns. A
		// pre-canceled Wait that joins the queue records 1 at its own Unlock.
		assert.Equal(t, []int{0, 0}, locker.recordedQueueLens())
	})

	t.Run("SignalWakesQueued", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			cond := NewCond(make(chanLock, 1))
			results := startWaiter(ctx, cond)
			synctest.Wait()
			require.Equal(t, 1, queueLen(cond))

			preCanceled, cancelPre := context.WithCancel(t.Context())
			cancelPre()
			cond.L.Lock()
			err := cond.Wait(preCanceled)
			cond.L.Unlock()
			assert.ErrorIs(t, err, context.Canceled)
			assert.Equal(t, 1, queueLen(cond))

			cond.Signal()
			synctest.Wait()
			assertCondWaitResult(t, cond, results, cancel, nil)
		})
	})
}

// queueRecordingLocker is a chanLock whose Unlock records the length of cond's queue before it
// releases the lock. It records heldQueueLen if the Cond's internal mutex is held at that point,
// instead of blocking on it. A Wait that holds the mutex while it calls Unlock then fails the
// test's check of the recorded lengths instead of deadlocking, which synctest cannot detect
// because a goroutine blocked on a sync.Mutex is not durably blocked.
type queueRecordingLocker struct {
	lock chanLock
	cond *Cond

	mu        sync.Mutex
	queueLens []int
}

func (ql *queueRecordingLocker) Lock() {
	ql.lock.Lock()
}

// heldQueueLen is what queueRecordingLocker records when the Cond's internal mutex is held.
const heldQueueLen = -1

func (ql *queueRecordingLocker) Unlock() {
	queued := heldQueueLen
	if ql.cond.mu.TryLock() {
		queued = queueLenLocked(ql.cond)
		ql.cond.mu.Unlock()
	}
	ql.mu.Lock()
	ql.queueLens = append(ql.queueLens, queued)
	ql.mu.Unlock()
	ql.lock.Unlock()
}

// TryLock takes the lock without blocking and reports whether it did. It records nothing.
func (ql *queueRecordingLocker) TryLock() bool {
	return ql.lock.TryLock()
}

// recordedQueueLens returns a copy of the queue lengths recorded at each Unlock.
func (ql *queueRecordingLocker) recordedQueueLens() []int {
	ql.mu.Lock()
	defer ql.mu.Unlock()
	return slices.Clone(ql.queueLens)
}

// TestCondJoinsBeforeUnlock checks that Wait joins the queue before it unlocks L. The first Unlock
// is Wait's and the second is the waiter's after Wait returns. An implementation that unlocks
// before joining records 0 at Wait's Unlock, deterministically.
func TestCondJoinsBeforeUnlock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		locker := &queueRecordingLocker{lock: make(chanLock, 1)}
		cond := NewCond(locker)
		locker.cond = cond

		results := startWaiter(ctx, cond)
		synctest.Wait()
		cancel()
		synctest.Wait()
		assertCondWaitResult(t, cond, results, cancel, context.Canceled)
		assert.Equal(t, []int{1, 0}, locker.recordedQueueLens())
	})
}

func TestCondAbandon(t *testing.T) {
	t.Run("PassesOnSignal", func(t *testing.T) {
		cond := NewCond(make(chanLock, 1))
		first, second := newRecord(), newRecord()
		cond.enqueue(first)
		cond.enqueue(second)
		cond.Signal()

		cond.abandon(first)
		assert.Equal(t, wokenBySignal, second.woken, "abandon did not pass the signal on")
		assert.True(t, isClosed(second.ready))
		assert.Equal(t, 0, queueLen(cond))
	})

	t.Run("RemovesQueued", func(t *testing.T) {
		cond := NewCond(make(chanLock, 1))
		record, behind := newRecord(), newRecord()
		cond.enqueue(record)
		cond.enqueue(behind)

		cond.abandon(record)
		assert.False(t, record.queued)
		assert.Equal(t, notWoken, record.woken)
		assert.False(t, isClosed(record.ready))
		assert.True(t, behind.queued, "abandoning a queued record woke the record behind it")
		assert.Equal(t, notWoken, behind.woken)
		assert.Equal(t, 1, queueLen(cond))
	})

	t.Run("IgnoresBroadcast", func(t *testing.T) {
		cond := NewCond(make(chanLock, 1))
		woken := newRecord()
		cond.enqueue(woken)
		cond.Broadcast()
		late := newRecord()
		cond.enqueue(late)

		cond.abandon(woken)
		assert.True(t, late.queued, "abandon passed a Broadcast on")
		assert.Equal(t, notWoken, late.woken)
	})
}

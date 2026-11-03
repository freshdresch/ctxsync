package ctxsync

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// waitForQueueLen polls c's queue until it holds want records and reports whether it did before
// boundedWaitTimeout. It is for tests outside synctest, where synctest.Wait is not available.
func waitForQueueLen(c *Cond, want int) bool {
	deadline := time.Now().Add(boundedWaitTimeout)
	for queueLen(c) != want {
		if time.Now().After(deadline) {
			return false
		}
		runtime.Gosched()
	}
	return true
}

// errNoResult is what receiveResult returns when no result arrives in time.
var errNoResult = errors.New("no result before the timeout")

// receiveResult returns the next result from results, or errNoResult if none arrives before
// boundedWaitTimeout.
func receiveResult(results <-chan error) error {
	timer := time.NewTimer(boundedWaitTimeout)
	defer timer.Stop()
	select {
	case err := <-results:
		return err
	case <-timer.C:
		return errNoResult
	}
}

// TestCondHappensBefore hands a plain int from a writer to a waiter through the Cond, so the race
// detector reports the read if the wakeup gives no happens-before edge. The writer never touches
// L, so no edge through L can hide a missing one. The waiter holds L throughout, closes started
// and then waits, and reads the payload before it unlocks L or blocks again. The started channel
// orders the waiter before the writer, the opposite of the edge under test. The writer yields
// before it writes and then wakes the Cond until the waiter finishes, because a wakeup that comes
// before the waiter joins the queue is not remembered. The Changed case receives from the channel
// fetched before the goroutines start, so the mutex inside Changed cannot add an edge that hides
// the one under test.
//
// The test runs outside synctest, so every wait gives up after boundedWaitTimeout and the waiter
// reports a timeout as a failure instead of blocking the test forever.
func TestCondHappensBefore(t *testing.T) {
	tests := []struct {
		name        string
		wake        func(cond *Cond)
		waitForWake func(ctx context.Context, cond *Cond, changed <-chan struct{}) error
	}{
		{
			name:        "Signal",
			wake:        (*Cond).Signal,
			waitForWake: waitInCond,
		},
		{
			name:        "Broadcast",
			wake:        (*Cond).Broadcast,
			waitForWake: waitInCond,
		},
		{
			name:        "Changed",
			wake:        (*Cond).Broadcast,
			waitForWake: receiveChanged,
		},
	}
	const written = 42
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for i := range raceAwareIterations {
				cond := NewCond(&sync.Mutex{})
				changed := cond.Changed()
				var payload int
				var observed int
				var waitErr error
				var finished atomic.Bool
				started := make(chan struct{})
				var goroutines sync.WaitGroup
				goroutines.Go(func() {
					<-started
					runtime.Gosched()
					payload = written
					for !finished.Load() {
						tc.wake(cond)
						runtime.Gosched()
					}
				})
				goroutines.Go(func() {
					defer finished.Store(true)
					cond.L.Lock()
					defer cond.L.Unlock()
					close(started)
					waitErr = tc.waitForWake(t.Context(), cond, changed)
					if waitErr != nil {
						t.Errorf("iteration %d: wait for wakeup: %v", i, waitErr)
						return
					}
					observed = payload
				})
				goroutines.Wait()

				// The waiter already reported the failed wait, and observed was never read.
				if waitErr != nil {
					return
				}
				assert.Equal(t, written, observed, "iteration %d", i)
			}
		})
	}
}

// waitInCond calls cond.Wait, which the caller must hold cond.L for, with a context that ends
// after boundedWaitTimeout.
func waitInCond(ctx context.Context, cond *Cond, _ <-chan struct{}) error {
	ctx, cancel := context.WithTimeout(ctx, boundedWaitTimeout)
	defer cancel()
	return cond.Wait(ctx)
}

// receiveChanged waits until changed is closed, or returns an error after boundedWaitTimeout.
func receiveChanged(_ context.Context, _ *Cond, changed <-chan struct{}) error {
	timer := time.NewTimer(boundedWaitTimeout)
	defer timer.Stop()
	select {
	case <-changed:
		return nil
	case <-timer.C:
		return context.DeadlineExceeded
	}
}

// TestCondHappensBeforePassOn hands a plain int to B through a signal that the canceled A passes
// on. This test shows that the guarantee holds but cannot catch it missing. B's decide takes the
// internal mutex after the test releases it, so B gets an edge from the test's write whether or
// not the pass-on through A carries one. A missing edge through A would stay hidden behind it.
func TestCondHappensBeforePassOn(t *testing.T) {
	for i := range raceAwareIterations {
		runPassOnHandoff(t, i)
	}
}

// runPassOnHandoff runs one iteration of TestCondHappensBeforePassOn.
func runPassOnHandoff(t *testing.T, iteration int) {
	const written = 42
	ctxA, cancelA := context.WithCancel(t.Context())
	defer cancelA()
	ctxB, cancelB := context.WithTimeout(t.Context(), boundedWaitTimeout)
	defer cancelB()
	cond := NewCond(&sync.Mutex{})
	var payload int
	var observed int

	resultsA := startWaiter(ctxA, cond)
	require.True(t, waitForQueueLen(cond, 1), "iteration %d: A did not join the queue", iteration)
	front := frontRecord(cond)
	require.NotNil(t, front, "iteration %d: A did not join the queue", iteration)
	resultsB := make(chan error, 1)
	go func() {
		cond.L.Lock()
		defer cond.L.Unlock()
		err := cond.Wait(ctxB)
		if err == nil {
			observed = payload
		}
		resultsB <- err
	}()
	require.True(t, waitForQueueLen(cond, 2), "iteration %d: B did not join the queue", iteration)
	require.Same(t, front, frontRecord(cond), "iteration %d: A is not at the front", iteration)

	payload = written
	cond.mu.Lock()
	cancelA()
	cond.wakeFrontLocked(wokenBySignal)
	cond.mu.Unlock()

	assert.ErrorIs(t, receiveResult(resultsA), context.Canceled, "iteration %d: A", iteration)
	require.NoError(t, receiveResult(resultsB), "iteration %d: B", iteration)
	assert.Equal(t, written, observed, "iteration %d", iteration)
}

func TestCondRLocker(t *testing.T) {
	const readerCount = 4

	t.Run("Broadcast", func(t *testing.T) {
		var readers sync.WaitGroup
		defer readers.Wait()
		release := make(chan struct{})
		defer close(release)
		ctx, cancel := context.WithTimeout(t.Context(), boundedWaitTimeout)
		defer cancel()
		var rw sync.RWMutex
		cond := NewCond(rw.RLocker())
		ready := false // guarded by rw

		results := startReaders(ctx, t, readerGroup{
			cond:    cond,
			rw:      &rw,
			ready:   &ready,
			release: release,
			readers: &readers,
		}, readerCount)
		require.True(t, waitForQueueLen(cond, readerCount), "the readers did not all join the queue")

		rw.Lock()
		ready = true
		cond.Broadcast()
		rw.Unlock()
		for i := range readerCount {
			require.NoError(t, receiveResult(results), "result %d", i)
		}
		assertReadLocked(t, &rw)
	})

	t.Run("Signal", func(t *testing.T) {
		var readers sync.WaitGroup
		defer readers.Wait()
		release := make(chan struct{})
		defer close(release)
		ctx, cancel := context.WithTimeout(t.Context(), boundedWaitTimeout)
		defer cancel()
		var rw sync.RWMutex
		cond := NewCond(rw.RLocker())
		ready := false // guarded by rw

		results := startReaders(ctx, t, readerGroup{
			cond:    cond,
			rw:      &rw,
			ready:   &ready,
			release: release,
			readers: &readers,
		}, readerCount)
		require.True(t, waitForQueueLen(cond, readerCount), "the readers did not all join the queue")

		rw.Lock()
		ready = true
		rw.Unlock()
		for i := range readerCount {
			cond.Signal()
			stillQueued := readerCount - i - 1
			require.True(t, waitForQueueLen(cond, stillQueued), "Signal %d did not wake one reader", i)
			require.NoError(t, receiveResult(results), "Signal %d", i)
		}
		assert.Empty(t, results, "a reader returned more than once")
		assertReadLocked(t, &rw)
	})
}

// readerGroup holds what the readers in a TestCondRLocker subtest share.
type readerGroup struct {
	cond    *Cond
	rw      *sync.RWMutex
	ready   *bool // guarded by rw
	release <-chan struct{}
	readers *sync.WaitGroup
}

// startReaders starts count readers in group.readers. Each holds a read lock on group.rw and calls
// Wait once, and a reader that returns nil checks ready under its read lock. Each reader sends its
// result on the returned channel and keeps its read lock until group.release is closed.
func startReaders(ctx context.Context, t *testing.T, group readerGroup, count int) <-chan error {
	results := make(chan error, count)
	for range count {
		group.readers.Go(func() {
			group.rw.RLock()
			defer group.rw.RUnlock()
			err := group.cond.Wait(ctx)
			if err == nil {
				assert.True(t, *group.ready, "a reader returned nil before the condition changed")
			}
			results <- err
			<-group.release
		})
	}
	return results
}

// assertReadLocked checks that some goroutine holds a read lock on rw. If the write lock can be
// taken, it reports the failure and releases the write lock.
func assertReadLocked(t *testing.T, rw *sync.RWMutex) {
	t.Helper()
	if rw.TryLock() {
		t.Error("readers returned from Wait without holding their read locks")
		rw.Unlock()
	}
}

// lockerPanic is the value that a faultyLocker built with panicFromLocker panics with.
const lockerPanic = "ctxsync test: faulty locker"

func panicFromLocker() {
	panic(lockerPanic)
}

// lockerCall names the sync.Locker method that a faultyLocker fails.
type lockerCall int

const (
	callUnlock lockerCall = iota + 1
	callLock
)

// faultyLocker is a chanLock that fails one call to the method failOn names, once armed is set. A
// failing Unlock releases the lock before it fails, and a failing Lock fails without taking it,
// so no goroutine later blocks on a lock that nobody will release. The failing call clears armed
// with CompareAndSwap before it fails, so every later call behaves normally.
type faultyLocker struct {
	lock   chanLock
	failOn lockerCall
	fail   func() // panics or calls runtime.Goexit
	armed  atomic.Bool
}

func (fl *faultyLocker) Lock() {
	if fl.failOn == callLock && fl.armed.CompareAndSwap(true, false) {
		fl.fail()
	}
	fl.lock.Lock()
}

func (fl *faultyLocker) Unlock() {
	fl.lock.Unlock()
	if fl.failOn == callUnlock && fl.armed.CompareAndSwap(true, false) {
		fl.fail()
	}
}

// startRecoveringWaiter starts a goroutine that locks c.L and calls c.Wait(ctx), for a Wait that
// is expected to panic. The goroutine never unlocks c.L, because the failing locker call leaves
// c.L unlocked. Its only deferred call recovers the panic and sends the recovered value on the
// returned channel, which has capacity 1.
func startRecoveringWaiter(ctx context.Context, c *Cond) <-chan any {
	recovered := make(chan any, 1)
	go func() {
		defer func() {
			recovered <- recover()
		}()
		c.L.Lock()
		_ = c.Wait(ctx)
	}()
	return recovered
}

// assertRecovered checks, without blocking, that a goroutine from startRecoveringWaiter recovered
// lockerPanic. If nothing has arrived, it reports the failure, cancels the waiter's context, and
// receives the recovered value. It must run in a synctest bubble, after synctest.Wait.
func assertRecovered(t *testing.T, recovered <-chan any, cancel context.CancelFunc) {
	t.Helper()
	select {
	case value := <-recovered:
		assert.Equal(t, lockerPanic, value)
	default:
		t.Error("Wait did not panic")
		cancel()
		lateValue := <-recovered
		t.Logf("after cancel, the waiter recovered %v", lateValue)
	}
}

// assertLaterWaiterSignaled starts a waiter and checks that a Signal wakes it, which fails if an
// earlier Wait left a record in the queue. It must run in a synctest bubble.
func assertLaterWaiterSignaled(t *testing.T, c *Cond) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	results := startWaiter(ctx, c)
	synctest.Wait()
	assert.Equal(t, 1, queueLen(c), "the queue holds a record from an earlier Wait")

	c.Signal()
	synctest.Wait()
	assertWaitResult(t, results, cancel, nil)
}

// panickyDoneContext is a context whose Done method panics with donePanic.
type panickyDoneContext struct {
	context.Context
}

const donePanic = "ctxsync test: Done panicked"

func (panickyDoneContext) Done() <-chan struct{} {
	panic(donePanic)
}

func TestCondAbnormalExit(t *testing.T) {
	t.Run("UnlockPanics", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctxA, cancelA := context.WithCancel(t.Context())
			defer cancelA()
			locker := &faultyLocker{
				lock:   make(chanLock, 1),
				failOn: callUnlock,
				fail:   panicFromLocker,
			}
			locker.armed.Store(true)
			cond := NewCond(locker)

			recovered := startRecoveringWaiter(ctxA, cond)
			synctest.Wait()
			assertRecovered(t, recovered, cancelA)
			assert.Equal(t, 0, queueLen(cond), "the panicking Wait left its record queued")
			assertLaterWaiterSignaled(t, cond)
		})
	})

	t.Run("LockPanics", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctxA, cancelA := context.WithCancel(t.Context())
			defer cancelA()
			ctxB, cancelB := context.WithCancel(t.Context())
			defer cancelB()
			locker := &faultyLocker{
				lock:   make(chanLock, 1),
				failOn: callLock,
				fail:   panicFromLocker,
			}
			cond := NewCond(locker)

			recovered := startRecoveringWaiter(ctxA, cond)
			synctest.Wait()
			front := frontRecord(cond)
			require.NotNil(t, front, "A did not join the queue")
			resultsB := startWaiter(ctxB, cond)
			synctest.Wait()
			require.Equal(t, 2, queueLen(cond))
			require.Same(t, front, frontRecord(cond), "A is no longer at the front")

			locker.armed.Store(true)
			cond.Signal()
			synctest.Wait()
			assertRecovered(t, recovered, cancelA)
			assertWaitResult(t, resultsB, cancelB, nil)
		})
	})

	t.Run("LockGoexits", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctxA, cancelA := context.WithCancel(t.Context())
			defer cancelA()
			ctxB, cancelB := context.WithCancel(t.Context())
			defer cancelB()
			locker := &faultyLocker{
				lock:   make(chanLock, 1),
				failOn: callLock,
				fail:   runtime.Goexit,
			}
			cond := NewCond(locker)

			// A never unlocks L, because the failing Lock did not take it. A deferred Unlock
			// would take the lock that B acquires after the pass-on.
			exited := make(chan struct{})
			waitReturned := false
			go func() {
				defer close(exited)
				cond.L.Lock()
				_ = cond.Wait(ctxA)
				waitReturned = true
			}()
			synctest.Wait()
			front := frontRecord(cond)
			require.NotNil(t, front, "A did not join the queue")
			resultsB := startWaiter(ctxB, cond)
			synctest.Wait()
			require.Equal(t, 2, queueLen(cond))
			require.Same(t, front, frontRecord(cond), "A is no longer at the front")

			locker.armed.Store(true)
			cond.Signal()
			synctest.Wait()
			if isClosed(exited) {
				assert.False(t, waitReturned, "Wait returned instead of exiting the goroutine")
			} else {
				t.Error("A did not exit")
				cancelA()
				<-exited
			}
			assertWaitResult(t, resultsB, cancelB, nil)
		})
	})

	t.Run("DonePanics", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			locker := newRecordingLocker()
			cond := NewCond(locker)
			ctx := panickyDoneContext{Context: t.Context()}

			locker.Lock()
			assert.PanicsWithValue(t, donePanic, func() {
				_ = cond.Wait(ctx)
			})
			assert.Equal(t, []string{"lock"}, locker.events(), "Wait used L before Done panicked")
			assert.False(t, locker.TryLock(), "Wait left L unlocked after Done panicked")
			assert.Equal(t, 0, queueLen(cond), "the panicking Wait joined the queue")
			locker.Unlock()
			assertLaterWaiterSignaled(t, cond)
		})
	})
}

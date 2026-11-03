package ctxsync

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCondWaitNilContext uses a Cond with a nil L, so the nil-context message shows that Wait
// checks the context before it reads L.
func TestCondWaitNilContext(t *testing.T) {
	var nilContext context.Context
	var cond Cond
	assert.PanicsWithValue(t, "ctxsync: nil Context", func() {
		_ = cond.Wait(nilContext)
	})
}

func TestCondWaitPreCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	locker := newRecordingLocker()
	cond := NewCond(locker)

	locker.Lock()
	err := cond.Wait(ctx)

	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, []string{"lock", "unlock", "lock"}, locker.events())
	assert.Equal(t, 0, queueLen(cond))
	assert.False(t, locker.TryLock(), "Wait returned without holding L")
}

func TestCondNilLocker(t *testing.T) {
	t.Run("LiveContext", func(t *testing.T) {
		var cond Cond
		assert.Panics(t, func() {
			_ = cond.Wait(t.Context())
		})
	})

	t.Run("CanceledContext", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		var cond Cond
		assert.Panics(t, func() {
			_ = cond.Wait(ctx)
		})
	})

	t.Run("SignalBroadcastChanged", func(t *testing.T) {
		var cond Cond
		assert.NotPanics(t, func() {
			cond.Signal()
			cond.Broadcast()
			_ = cond.Changed()
		})
	})
}

func TestCondWaitDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
		defer cancel()
		cond := NewCond(make(chanLock, 1))
		results := startWait(func() error {
			cond.L.Lock()
			defer cond.L.Unlock()
			return cond.Wait(ctx)
		})
		synctest.Wait()
		require.Empty(t, results, "Wait returned before the deadline")

		time.Sleep(waitTimeout)
		synctest.Wait()
		assertWaitResult(t, results, cancel, context.DeadlineExceeded)
	})
}

func TestCondSignal(t *testing.T) {
	t.Run("WakesOneThenBroadcastWakesOther", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctxFirst, cancelFirst := context.WithCancel(t.Context())
			defer cancelFirst()
			ctxSecond, cancelSecond := context.WithCancel(t.Context())
			defer cancelSecond()
			cond := NewCond(make(chanLock, 1))
			first := startWaiter(ctxFirst, cond)
			second := startWaiter(ctxSecond, cond)
			synctest.Wait()
			require.Equal(t, 2, queueLen(cond))

			cond.Signal()
			synctest.Wait()
			var stillQueued <-chan error
			var cancelStillQueued context.CancelFunc
			select {
			case err := <-first:
				assert.NoError(t, err)
				stillQueued, cancelStillQueued = second, cancelSecond
			case err := <-second:
				assert.NoError(t, err)
				stillQueued, cancelStillQueued = first, cancelFirst
			default:
				require.Fail(t, "Signal woke neither waiter")
			}
			require.Empty(t, stillQueued, "Signal woke both waiters")
			assert.Equal(t, 1, queueLen(cond))

			cond.Broadcast()
			synctest.Wait()
			assertWaitResult(t, stillQueued, cancelStillQueued, nil)
		})
	})

	t.Run("BroadcastWakesAll", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			const waiterCount = 3
			cond := NewCond(make(chanLock, 1))
			results := make([]<-chan error, 0, waiterCount)
			cancels := make([]context.CancelFunc, 0, waiterCount)
			for range waiterCount {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				results = append(results, startWaiter(ctx, cond))
				cancels = append(cancels, cancel)
				synctest.Wait()
			}
			require.Equal(t, waiterCount, queueLen(cond))

			cond.Broadcast()
			synctest.Wait()
			for i := range waiterCount {
				assertWaitResult(t, results[i], cancels[i], nil)
			}
		})
	})

	t.Run("NotRemembered", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			cond := NewCond(make(chanLock, 1))
			cond.Signal()

			results := startWaiter(ctx, cond)
			synctest.Wait()
			assert.Empty(t, results, "a Signal with no waiters woke a later waiter")
			assert.Equal(t, 1, queueLen(cond))

			cancel()
			synctest.Wait()
			assertWaitResult(t, results, cancel, context.Canceled)
		})
	})
}

func TestCondSignalFIFO(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const waiterCount = 3
		cond := NewCond(make(chanLock, 1))
		results := make([]<-chan error, 0, waiterCount)
		cancels := make([]context.CancelFunc, 0, waiterCount)
		for range waiterCount {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			results = append(results, startWaiter(ctx, cond))
			cancels = append(cancels, cancel)
			synctest.Wait()
		}
		require.Equal(t, waiterCount, queueLen(cond))

		for i := range waiterCount {
			cond.Signal()
			synctest.Wait()
			assertWaitResult(t, results[i], cancels[i], nil)
			for _, later := range results[i+1:] {
				assert.Empty(t, later, "Signal %d woke a waiter that started later", i)
			}
		}
	})
}

func TestCondChanged(t *testing.T) {
	t.Run("OnlyBroadcastCloses", func(t *testing.T) {
		cond := NewCond(make(chanLock, 1))
		changed := cond.Changed()

		cond.Signal()
		assert.False(t, isClosed(changed), "Signal closed the Changed channel")

		cond.Broadcast()
		assert.True(t, isClosed(changed), "Broadcast did not close the Changed channel")
		assert.False(t, isClosed(cond.Changed()), "Changed after Broadcast returned a closed channel")
	})

	t.Run("SelectLoop", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cond := NewCond(make(chanLock, 1))
			ready := false // guarded by cond.L
			other := make(chan struct{})
			exitedOnReady := make(chan bool, 1)
			go func() {
				cond.L.Lock()
				for !ready {
					changed := cond.Changed()
					cond.L.Unlock()
					select {
					case <-changed:
					case <-other:
						exitedOnReady <- false
						return
					}
					cond.L.Lock()
				}
				cond.L.Unlock()
				exitedOnReady <- true
			}()
			synctest.Wait()
			require.Empty(t, exitedOnReady, "loop exited before the state changed")

			cond.L.Lock()
			ready = true
			cond.L.Unlock()
			cond.Broadcast()
			synctest.Wait()
			select {
			case onReady := <-exitedOnReady:
				assert.True(t, onReady, "loop exited through the other channel")
			default:
				t.Error("loop did not exit after the state change and Broadcast")
				close(other)
				onReady := <-exitedOnReady
				t.Logf("after closing other, the loop exited with onReady %v", onReady)
			}
		})
	})
}

// TestCondCanceledHoldsLock checks that a Wait canceled while queued returns holding L. The waiter
// keeps L until the test closes release, so TryLock fails only if Wait locked L again.
func TestCondCanceledHoldsLock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		lock := make(chanLock, 1)
		cond := NewCond(lock)
		results := make(chan error, 1)
		release := make(chan struct{})
		finished := make(chan struct{})
		go func() {
			defer close(finished)
			cond.L.Lock()
			results <- cond.Wait(ctx)
			<-release
			cond.L.Unlock()
		}()
		synctest.Wait()
		require.Equal(t, 1, queueLen(cond))

		cancel()
		synctest.Wait()
		assertWaitResult(t, results, cancel, context.Canceled)
		assert.False(t, lock.TryLock(), "Wait returned without holding L")

		close(release)
		<-finished
	})
}

// signalingLocker is a chanLock whose Unlock calls Signal on cond after it releases the lock.
type signalingLocker struct {
	lock chanLock
	cond *Cond
}

func (sl *signalingLocker) Lock() {
	sl.lock.Lock()
}

func (sl *signalingLocker) Unlock() {
	sl.lock.Unlock()
	sl.cond.Signal()
}

// TestCondUnlockSignals checks that Wait joins the queue before it unlocks L and calls Unlock
// without holding the internal mutex. The Signal inside Unlock then wakes the waiter's own record
// instead of deadlocking.
func TestCondUnlockSignals(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		locker := &signalingLocker{lock: make(chanLock, 1)}
		cond := NewCond(locker)
		locker.cond = cond

		results := startWaiter(ctx, cond)
		synctest.Wait()
		assertWaitResult(t, results, cancel, nil)
		assert.Equal(t, 0, queueLen(cond))
	})
}

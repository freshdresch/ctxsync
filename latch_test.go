package ctxsync

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLatchZeroValue runs in a bubble, so a Wait that blocked instead of returning at once would
// leave the bubble deadlocked and fail the test.
func TestLatchZeroValue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var latch Latch
		assert.Equal(t, 0, latch.Count())
		assert.NoError(t, latch.Wait(t.Context()))
		assert.True(t, isClosed(latch.Released()), "Released returned an open channel")
	})
}

func TestNewLatch(t *testing.T) {
	assert.PanicsWithValue(t, "ctxsync: negative Latch count", func() {
		NewLatch(-1)
	})

	latch := NewLatch(0)
	assert.Equal(t, 0, latch.Count())
	assert.True(t, isClosed(latch.Released()), "NewLatch(0) returned a latch that is not open")
}

// TestLatchCountDownPastZero calls Released first so the channel exists, which makes a CountDown
// that closed it again panic.
func TestLatchCountDownPastZero(t *testing.T) {
	latch := NewLatch(1)
	released := latch.Released()
	assert.NotPanics(t, func() {
		latch.CountDown()
		latch.CountDown()
	})
	assert.Equal(t, 0, latch.Count())
	assert.True(t, isClosed(released), "channel open after the count reached zero")
}

func TestLatchReleasedIdentity(t *testing.T) {
	t.Run("ReleasedBeforeZero", func(t *testing.T) {
		latch := NewLatch(1)
		first := latch.Released()
		second := latch.Released()
		assert.Equal(t, first, second, "Released returned two different channels")
		assert.False(t, isClosed(first), "channel closed before the count reached zero")

		latch.CountDown()
		assert.True(t, isClosed(first), "channel open after the count reached zero")
	})

	t.Run("ReleasedAfterZero", func(t *testing.T) {
		latch := NewLatch(1)
		latch.CountDown()
		first := latch.Released()
		assert.True(
			t,
			isClosed(first),
			"first Released after the count reached zero returned an open channel",
		)
		assert.Equal(t, first, latch.Released(), "Released returned two different channels")
	})
}

func TestLatchWaitNilContext(t *testing.T) {
	var nilContext context.Context
	latch := NewLatch(1)
	assert.PanicsWithValue(t, "ctxsync: nil Context", func() {
		_ = latch.Wait(nilContext)
	}, "before the count reached zero")

	latch.CountDown()
	assert.PanicsWithValue(t, "ctxsync: nil Context", func() {
		_ = latch.Wait(nilContext)
	}, "after the count reached zero")
}

func TestLatchWaitReleasedCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	latch := NewLatch(1)
	latch.CountDown()
	assert.NoError(t, latch.Wait(ctx))
}

// TestLatchWaitUnreleasedCanceledContext runs in a bubble, so a Wait that blocked instead of
// returning at once would leave the bubble deadlocked and fail the test.
func TestLatchWaitUnreleasedCanceledContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		latch := NewLatch(1)
		assert.ErrorIs(t, latch.Wait(ctx), context.Canceled)
	})
}

func TestLatchWaitCountsDown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		latch := NewLatch(3)
		results := startWait(func() error { return latch.Wait(ctx) })
		synctest.Wait()
		require.Empty(t, results, "Wait returned before any CountDown")

		latch.CountDown()
		latch.CountDown()
		synctest.Wait()
		require.Empty(t, results, "Wait returned before the count reached zero")
		assert.False(t, isClosed(latch.Released()), "channel closed before the count reached zero")
		assert.Equal(t, 1, latch.Count())

		latch.CountDown()
		synctest.Wait()
		assertWaitResult(t, results, cancel, nil)
	})
}

// TestLatchWaitCanceled has no case where the release and cancellation are both ready. The
// outcome-wins proof for Latch's blocking path is TestWaitClosedOutcomeWins, so Latch.Wait must
// keep waiting through waitClosed.
func TestLatchWaitCanceled(t *testing.T) {
	tests := []struct {
		name     string
		trigger  func(cancel context.CancelFunc)
		expected error
	}{
		{
			name: "Cancel",
			trigger: func(cancel context.CancelFunc) {
				cancel()
			},
			expected: context.Canceled,
		},
		{
			name: "Deadline",
			trigger: func(_ context.CancelFunc) {
				time.Sleep(waitTimeout)
			},
			expected: context.DeadlineExceeded,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
				defer cancel()
				latch := NewLatch(1)
				results := startWait(func() error { return latch.Wait(ctx) })
				synctest.Wait()
				require.Empty(t, results, "Wait returned before the trigger")

				tc.trigger(cancel)
				synctest.Wait()
				assertWaitResult(t, results, cancel, tc.expected)
			})
		})
	}
}

// TestLatchHappensBefore hands plain ints from the counting goroutines to a reader through the
// latch, so the race detector reports the read if the latch gives no happens-before edge. Each
// counting goroutine writes its own slot, and the reader reads every slot before it blocks on
// anything else. The test calls Released first, and each counting goroutine waits for the reader
// to start and yields before it writes, so the final CountDown usually closes the channel while
// the reader is blocked on it. The started channel orders the reader before the counting
// goroutines, the opposite of the edge under test, so it cannot hide a missing edge from the
// CountDown calls to the reader.
//
// The test runs outside synctest, so every wait gives up after boundedWaitTimeout and the
// reader reports a timeout as a failure instead of blocking the test forever.
func TestLatchHappensBefore(t *testing.T) {
	const counters = 4
	tests := []struct {
		name           string
		waitForRelease func(ctx context.Context, latch *Latch, released <-chan struct{}) error
	}{
		{
			name: "Wait",
			waitForRelease: func(ctx context.Context, latch *Latch, _ <-chan struct{}) error {
				ctx, cancel := context.WithTimeout(ctx, boundedWaitTimeout)
				defer cancel()
				return latch.Wait(ctx)
			},
		},
		{
			name: "Released",
			waitForRelease: func(_ context.Context, _ *Latch, released <-chan struct{}) error {
				timer := time.NewTimer(boundedWaitTimeout)
				defer timer.Stop()
				select {
				case <-released:
					return nil
				case <-timer.C:
					return errors.New("Released channel not closed before the timeout")
				}
			},
		},
		{
			name: "Count",
			waitForRelease: func(_ context.Context, latch *Latch, _ <-chan struct{}) error {
				deadline := time.Now().Add(boundedWaitTimeout)
				for latch.Count() != 0 {
					if time.Now().After(deadline) {
						return errors.New("Count still above zero at the timeout")
					}
					runtime.Gosched()
				}
				return nil
			},
		},
	}
	written := make([]int, counters)
	for slot := range counters {
		written[slot] = slot + 1
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for i := range raceAwareIterations {
				latch := NewLatch(counters)
				released := latch.Released()
				slots := make([]int, counters)
				var observed []int
				var waitErr error
				started := make(chan struct{})
				var goroutines sync.WaitGroup
				for slot := range counters {
					goroutines.Go(func() {
						<-started
						runtime.Gosched()
						slots[slot] = written[slot]
						latch.CountDown()
					})
				}
				goroutines.Go(func() {
					close(started)
					waitErr = tc.waitForRelease(t.Context(), latch, released)
					if waitErr != nil {
						t.Errorf("iteration %d: wait for release: %v", i, waitErr)
						return
					}
					observed = slices.Clone(slots)
				})
				goroutines.Wait()

				// The reader already reported the failed wait, and observed was never read.
				if waitErr != nil {
					return
				}
				assert.Equal(t, written, observed, "iteration %d", i)
			}
		})
	}
}

func TestLatchStress(t *testing.T) {
	const (
		counters = 4
		waiters  = 4
	)
	synctest.Test(t, func(t *testing.T) {
		for i := range raceAwareIterations {
			latch := NewLatch(counters)
			var countDowns atomic.Int64
			var goroutines sync.WaitGroup
			for range counters {
				goroutines.Go(func() {
					countDowns.Add(1)
					latch.CountDown()
				})
			}
			for range waiters {
				goroutines.Go(func() {
					err := latch.Wait(t.Context())
					if !assert.NoError(t, err, "iteration %d", i) {
						return
					}
					assert.Equal(t, int64(counters), countDowns.Load(), "iteration %d", i)
				})
			}
			goroutines.Wait()
		}
	})
}

func TestLatchAllocations(t *testing.T) {
	latch := NewLatch(1)
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		_ = latch.Count()
	}), "Count")

	latch.Released()
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		_ = latch.Released()
	}), "Released before the count reached zero after the first Released")

	latch.CountDown()
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		_ = latch.Released()
	}), "Released after the count reached zero")

	// The context is already canceled, so a broken Wait returns an error instead of blocking the
	// test.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.NoError(t, latch.Wait(ctx), "Wait on an open latch with a canceled context")
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		_ = latch.Wait(ctx)
	}), "Wait on an open latch")
}

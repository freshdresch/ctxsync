package ctxsync

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newBrokenBarrier returns a barrier for two parties that a caller with an already canceled
// context has broken.
func newBrokenBarrier(t *testing.T) *Barrier {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	barrier := NewBarrier(2)
	require.ErrorIs(t, barrier.Await(ctx), context.Canceled, "breaking Await")
	require.True(t, barrier.IsBroken(), "barrier not broken by a canceled Await")
	return barrier
}

// receiveResults collects one result from each channel. A waiter that has not sent its result is
// reported, and its result is then received with a blocking receive, so every waiter whose
// context the test already canceled is joined.
func receiveResults(t *testing.T, resultChannels ...<-chan error) []error {
	t.Helper()
	var received []error
	for i, results := range resultChannels {
		select {
		case err := <-results:
			received = append(received, err)
		default:
			t.Errorf("waiter %d did not return", i)
			received = append(received, <-results)
		}
	}
	return received
}

func TestNewBarrier(t *testing.T) {
	for _, parties := range []int{0, -1} {
		assert.PanicsWithValue(t, "ctxsync: Barrier needs at least one party", func() {
			NewBarrier(parties)
		}, "parties %d", parties)
	}

	barrier := NewBarrier(3)
	assert.Equal(t, 3, barrier.Parties())
	assert.Equal(t, 0, barrier.Waiting())
	assert.False(t, barrier.IsBroken())
}

func TestBarrierZeroValue(t *testing.T) {
	var barrier Barrier
	assert.Equal(t, 0, barrier.Parties())
	assert.Equal(t, 0, barrier.Waiting())
	assert.False(t, barrier.IsBroken())

	assert.PanicsWithValue(t, "ctxsync: zero Barrier, use NewBarrier", func() {
		_ = barrier.Await(t.Context())
	})
	var nilContext context.Context
	assert.PanicsWithValue(t, "ctxsync: nil Context", func() {
		_ = barrier.Await(nilContext)
	}, "the nil-context check comes before the zero-Barrier check")
}

func TestBarrierAwaitNilContext(t *testing.T) {
	var nilContext context.Context
	tests := []struct {
		name    string
		barrier func(t *testing.T) *Barrier
	}{
		{
			name:    "OneParty",
			barrier: func(_ *testing.T) *Barrier { return NewBarrier(1) },
		},
		{
			name:    "TwoParties",
			barrier: func(_ *testing.T) *Barrier { return NewBarrier(2) },
		},
		{
			name:    "Broken",
			barrier: newBrokenBarrier,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			barrier := tc.barrier(t)
			assert.PanicsWithValue(t, "ctxsync: nil Context", func() {
				_ = barrier.Await(nilContext)
			})
		})
	}
}

// TestBarrierTrips runs three generations on one barrier. In each, every party but the last must
// stay blocked until the last arrives. A trip that left its generation attached to the barrier
// would let the first party of the next generation join the tripped one and return at once.
func TestBarrierTrips(t *testing.T) {
	const parties = 3
	synctest.Test(t, func(t *testing.T) {
		barrier := NewBarrier(parties)
		for generationIndex := range 3 {
			ctx, cancel := context.WithCancel(t.Context())
			var resultChannels []<-chan error
			for range parties - 1 {
				resultChannels = append(
					resultChannels,
					startWait(func() error { return barrier.Await(ctx) }),
				)
			}
			synctest.Wait()
			for _, results := range resultChannels {
				assert.Empty(
					t,
					results,
					"generation %d: Await returned before the last party arrived",
					generationIndex,
				)
			}
			assert.Equal(t, parties-1, barrier.Waiting(), "generation %d", generationIndex)

			resultChannels = append(
				resultChannels,
				startWait(func() error { return barrier.Await(ctx) }),
			)
			synctest.Wait()
			for _, results := range resultChannels {
				assertWaitResult(t, results, cancel, nil)
			}
			assert.Equal(t, 0, barrier.Waiting(), "generation %d", generationIndex)
			assert.False(t, barrier.IsBroken(), "generation %d", generationIndex)
			cancel()
		}
	})
}

// TestBarrierOneParty runs in a bubble, so an Await that blocked instead of tripping at once
// would leave the bubble deadlocked and fail the test.
func TestBarrierOneParty(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		barrier := NewBarrier(1)
		for i := range 3 {
			assert.NoError(t, barrier.Await(t.Context()), "call %d", i)
			assert.Equal(t, 0, barrier.Waiting(), "call %d", i)
		}
		assert.False(t, barrier.IsBroken())

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		assert.ErrorIs(t, barrier.Await(ctx), context.Canceled)
		assert.True(t, barrier.IsBroken(), "a canceled Await did not break the barrier")
		assert.ErrorIs(t, barrier.Await(t.Context()), ErrBrokenBarrier)
	})
}

func TestBarrierCanceledWhileWaiting(t *testing.T) {
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
				breakerCtx, cancelBreaker := context.WithTimeout(t.Context(), waitTimeout)
				defer cancelBreaker()
				otherCtx, cancelOther := context.WithCancel(t.Context())
				defer cancelOther()
				barrier := NewBarrier(3)
				otherResults := startWait(func() error { return barrier.Await(otherCtx) })
				breakerResults := startWait(func() error { return barrier.Await(breakerCtx) })
				synctest.Wait()
				require.Empty(t, otherResults, "Await returned before the trigger")
				require.Empty(t, breakerResults, "Await returned before the trigger")
				assert.Equal(t, 2, barrier.Waiting())

				tc.trigger(cancelBreaker)
				synctest.Wait()
				assertWaitResult(t, breakerResults, cancelBreaker, tc.expected)
				assertWaitResult(t, otherResults, cancelOther, ErrBrokenBarrier)
				assert.True(t, barrier.IsBroken())
				assert.Equal(t, 0, barrier.Waiting())

				canceledCtx, cancel := context.WithCancel(t.Context())
				cancel()
				assert.ErrorIs(
					t,
					barrier.Await(canceledCtx),
					ErrBrokenBarrier,
					"the broken state wins over a canceled context",
				)
			})
		})
	}
}

func TestBarrierPreCanceled(t *testing.T) {
	// NotCounted calls the unexported arrive directly, because once the caller breaks the
	// barrier, Waiting reports 0 whether or not the caller arrived.
	t.Run("NotCounted", func(t *testing.T) {
		live := make(chan struct{})
		canceled := make(chan struct{})
		close(canceled)
		barrier := NewBarrier(3)
		gen, result := barrier.arrive(live)
		require.Equal(t, arrivalWaiting, result)
		require.NotNil(t, gen)

		_, result = barrier.arrive(canceled)
		assert.Equal(t, arrivalCanceled, result)
		assert.Equal(t, 1, gen.arrived, "the canceled caller arrived")
		assert.True(t, gen.broken, "the canceled caller did not break the generation")
		assert.True(t, isClosed(gen.released), "the canceled caller did not release the waiter")
		assert.True(t, barrier.IsBroken())
		assert.Equal(t, 0, barrier.Waiting())
	})

	t.Run("NoWaiters", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		barrier := NewBarrier(3)
		assert.ErrorIs(t, barrier.Await(ctx), context.Canceled)
		assert.True(t, barrier.IsBroken())
		assert.Equal(t, 0, barrier.Waiting())
	})

	t.Run("LastArrivalBreaks", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			waiterCtx, cancelWaiter := context.WithCancel(t.Context())
			defer cancelWaiter()
			barrier := NewBarrier(2)
			results := startWait(func() error { return barrier.Await(waiterCtx) })
			synctest.Wait()
			require.Empty(t, results, "Await returned before the second party arrived")
			assert.Equal(t, 1, barrier.Waiting())

			canceledCtx, cancel := context.WithCancel(t.Context())
			cancel()
			assert.ErrorIs(t, barrier.Await(canceledCtx), context.Canceled)
			synctest.Wait()
			assertWaitResult(t, results, cancelWaiter, ErrBrokenBarrier)
			assert.True(t, barrier.IsBroken())
		})
	})
}

// TestBarrierBrokenBySomeoneElse cancels two waiters of one generation while holding the
// barrier's internal mutex, so both wake on their context and neither can decide before both are
// canceled. The first to take the mutex breaks the barrier, and the second must see that its
// generation was already broken.
func TestBarrierBrokenBySomeoneElse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		firstCtx, cancelFirst := context.WithCancel(t.Context())
		defer cancelFirst()
		secondCtx, cancelSecond := context.WithCancel(t.Context())
		defer cancelSecond()
		barrier := NewBarrier(3)
		firstResults := startWait(func() error { return barrier.Await(firstCtx) })
		secondResults := startWait(func() error { return barrier.Await(secondCtx) })
		synctest.Wait()
		require.Equal(t, 2, barrier.Waiting())

		barrier.mu.Lock()
		cancelFirst()
		cancelSecond()
		barrier.mu.Unlock()
		synctest.Wait()

		received := receiveResults(t, firstResults, secondResults)
		canceledCount := 0
		brokenCount := 0
		for _, err := range received {
			switch {
			case errors.Is(err, context.Canceled):
				canceledCount++
			case errors.Is(err, ErrBrokenBarrier):
				brokenCount++
			default:
				t.Errorf("unexpected result %v", err)
			}
		}
		assert.Equal(t, 1, canceledCount, "results %v", received)
		assert.Equal(t, 1, brokenCount, "results %v", received)
		assert.True(t, barrier.IsBroken())
	})
}

// TestBarrierCancelAfterTrip calls the unexported functions directly, because two goroutines
// cannot force a waiter whose generation tripped to decide on a cancellation only after the next
// generation has started filling. The waiter must decide from its own generation, which tripped,
// and leave the next generation alone.
func TestBarrierCancelAfterTrip(t *testing.T) {
	live := make(chan struct{})
	barrier := NewBarrier(2)

	firstGen, result := barrier.arrive(live)
	require.Equal(t, arrivalWaiting, result, "first arrival")
	_, result = barrier.arrive(live)
	require.Equal(t, arrivalTripped, result, "second arrival")
	secondGen, result := barrier.arrive(live)
	require.Equal(t, arrivalWaiting, result, "third arrival")
	require.NotSame(t, firstGen, secondGen, "the third arrival joined the tripped generation")

	assert.Equal(t, arrivalTripped, barrier.afterCancel(firstGen))
	assert.False(t, barrier.IsBroken(), "a late cancellation broke the next generation")
	assert.Equal(t, 1, barrier.Waiting())
}

func TestBarrierErrBrokenBarrierUnwrapped(t *testing.T) {
	t.Run("AlreadyBroken", func(t *testing.T) {
		barrier := newBrokenBarrier(t)
		assert.Same(t, ErrBrokenBarrier, barrier.Await(t.Context()))
	})

	t.Run("BrokenWhileWaiting", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			waiterCtx, cancelWaiter := context.WithCancel(t.Context())
			defer cancelWaiter()
			barrier := NewBarrier(2)
			results := startWait(func() error { return barrier.Await(waiterCtx) })
			synctest.Wait()
			require.Empty(t, results, "Await returned before the barrier broke")

			canceledCtx, cancel := context.WithCancel(t.Context())
			cancel()
			require.ErrorIs(t, barrier.Await(canceledCtx), context.Canceled)
			synctest.Wait()
			cancelWaiter()
			received := receiveResults(t, results)
			assert.Same(t, ErrBrokenBarrier, received[0])
		})
	})
}

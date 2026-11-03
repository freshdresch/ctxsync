package ctxsync

import (
	"context"
	"errors"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventZeroValue(t *testing.T) {
	var event Event
	assert.False(t, event.IsSet())
}

// TestEventSetIdempotent calls Done first so the channel exists, which makes a second Set that
// closed it again panic.
func TestEventSetIdempotent(t *testing.T) {
	var event Event
	done := event.Done()
	assert.NotPanics(t, func() {
		event.Set()
		event.Set()
	})
	assert.True(t, event.IsSet())
	assert.True(t, isClosed(done), "channel open after Set")
}

func TestEventDoneIdentity(t *testing.T) {
	t.Run("DoneBeforeSet", func(t *testing.T) {
		var event Event
		first := event.Done()
		second := event.Done()
		assert.Equal(t, first, second, "Done returned two different channels")
		assert.False(t, isClosed(first), "channel closed before Set")

		event.Set()
		assert.True(t, isClosed(first), "channel open after Set")
	})

	t.Run("DoneAfterSet", func(t *testing.T) {
		var event Event
		event.Set()
		assert.True(t, isClosed(event.Done()), "first Done after Set returned an open channel")
	})
}

func TestEventWaitNilContext(t *testing.T) {
	var nilContext context.Context
	var event Event
	assert.PanicsWithValue(t, "ctxsync: nil Context", func() {
		_ = event.Wait(nilContext)
	}, "before Set")

	event.Set()
	assert.PanicsWithValue(t, "ctxsync: nil Context", func() {
		_ = event.Wait(nilContext)
	}, "after Set")
}

func TestEventWaitSetCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var event Event
	event.Set()
	assert.NoError(t, event.Wait(ctx))
}

// TestEventWaitUnsetCanceledContext runs in a bubble, so a Wait that blocked instead of returning
// at once would leave the bubble deadlocked and fail the test.
func TestEventWaitUnsetCanceledContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		var event Event
		assert.ErrorIs(t, event.Wait(ctx), context.Canceled)
	})
}

// TestEventWaitBlocking has no case where Set and cancellation are both ready. The outcome-wins
// proof for Event's blocking path is TestWaitClosedOutcomeWins, so Event.Wait must keep waiting
// through waitClosed.
func TestEventWaitBlocking(t *testing.T) {
	tests := []struct {
		name     string
		trigger  func(event *Event, cancel context.CancelFunc)
		expected error
	}{
		{
			name: "Set",
			trigger: func(event *Event, _ context.CancelFunc) {
				event.Set()
			},
			expected: nil,
		},
		{
			name: "Cancel",
			trigger: func(_ *Event, cancel context.CancelFunc) {
				cancel()
			},
			expected: context.Canceled,
		},
		{
			name: "Deadline",
			trigger: func(_ *Event, _ context.CancelFunc) {
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
				var event Event
				results := startWait(func() error { return event.Wait(ctx) })
				synctest.Wait()
				require.Empty(t, results, "Wait returned before the trigger")

				tc.trigger(&event, cancel)
				synctest.Wait()
				assertWaitResult(t, results, cancel, tc.expected)
			})
		})
	}
}

// TestEventHappensBefore hands a plain int from a writer to a reader through the event, so the
// race detector reports the read if the event gives no happens-before edge. The reader reads
// before it blocks on anything else. The test calls Done first, and the writer waits for the
// reader to start and yields before it writes, so Set usually closes the channel while the reader
// is blocked on it. The started channel orders the reader before the writer, the opposite of the
// edge under test, so it cannot hide a missing edge from Set to the reader.
//
// The test runs outside synctest, so every wait gives up after boundedWaitTimeout and the
// reader reports a timeout as a failure instead of blocking the test forever.
func TestEventHappensBefore(t *testing.T) {
	tests := []struct {
		name       string
		waitForSet func(ctx context.Context, event *Event) error
	}{
		{
			name: "Wait",
			waitForSet: func(ctx context.Context, event *Event) error {
				ctx, cancel := context.WithTimeout(ctx, boundedWaitTimeout)
				defer cancel()
				return event.Wait(ctx)
			},
		},
		{
			name: "Done",
			waitForSet: func(_ context.Context, event *Event) error {
				timer := time.NewTimer(boundedWaitTimeout)
				defer timer.Stop()
				select {
				case <-event.Done():
					return nil
				case <-timer.C:
					return errors.New("Done channel not closed before the timeout")
				}
			},
		},
		{
			name: "IsSet",
			waitForSet: func(_ context.Context, event *Event) error {
				deadline := time.Now().Add(boundedWaitTimeout)
				for !event.IsSet() {
					if time.Now().After(deadline) {
						return errors.New("IsSet still false at the timeout")
					}
					runtime.Gosched()
				}
				return nil
			},
		},
	}
	const written = 42
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for i := range raceAwareIterations {
				var event Event
				event.Done()
				var payload int
				var observed int
				var waitErr error
				started := make(chan struct{})
				var goroutines sync.WaitGroup
				goroutines.Go(func() {
					<-started
					runtime.Gosched()
					payload = written
					event.Set()
				})
				goroutines.Go(func() {
					close(started)
					waitErr = tc.waitForSet(t.Context(), &event)
					if waitErr != nil {
						t.Errorf("iteration %d: wait for Set: %v", i, waitErr)
						return
					}
					observed = payload
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

func TestEventStress(t *testing.T) {
	const (
		setters = 4
		waiters = 4
	)
	synctest.Test(t, func(t *testing.T) {
		for i := range raceAwareIterations {
			var event Event
			var setCount atomic.Int64
			var goroutines sync.WaitGroup
			for range setters {
				goroutines.Go(func() {
					setCount.Add(1)
					event.Set()
				})
			}
			for range waiters {
				ctx, cancel := context.WithCancel(t.Context())
				goroutines.Go(func() {
					err := event.Wait(ctx)
					if err != nil {
						assert.ErrorIs(t, err, context.Canceled, "iteration %d", i)
						return
					}
					assert.GreaterOrEqual(t, setCount.Load(), int64(1), "iteration %d", i)
				})
				goroutines.Go(func() {
					for range rand.IntN(51) {
						runtime.Gosched()
					}
					cancel()
				})
			}
			goroutines.Wait()
		}
	})
}

func TestEventAllocations(t *testing.T) {
	var event Event
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		_ = event.IsSet()
	}), "IsSet")

	event.Done()
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		_ = event.Done()
	}), "Done on an unset event after the first Done")

	event.Set()
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		_ = event.Done()
	}), "Done on a set event")

	// The context is already canceled, so a broken Wait returns an error instead of blocking the
	// test.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.NoError(t, event.Wait(ctx), "Wait on a set event with a canceled context")
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		_ = event.Wait(ctx)
	}), "Wait on a set event")
}

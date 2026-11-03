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

func TestGateZeroValue(t *testing.T) {
	var gate Gate
	assert.False(t, gate.IsOpen())
}

// TestGateOpenShutIdempotent calls Opened first so the channel exists, which makes a second Open
// that closed it again panic.
func TestGateOpenShutIdempotent(t *testing.T) {
	var gate Gate
	opened := gate.Opened()
	assert.NotPanics(t, func() {
		gate.Open()
		gate.Open()
	})
	assert.True(t, gate.IsOpen())
	assert.True(t, isClosed(opened), "channel open after Open")

	assert.NotPanics(t, func() {
		gate.Shut()
		gate.Shut()
	})
	assert.False(t, gate.IsOpen())
}

func TestGateOpened(t *testing.T) {
	t.Run("OpenedBeforeOpen", func(t *testing.T) {
		var gate Gate
		first := gate.Opened()
		assert.False(t, isClosed(first), "Opened while shut returned a closed channel")

		gate.Open()
		assert.True(t, isClosed(first), "Open did not close the channel")
		assert.True(t, isClosed(gate.Opened()), "Opened while open returned an open channel")

		gate.Shut()
		second := gate.Opened()
		assert.NotEqual(t, first, second, "Opened after Shut returned the old channel")
		assert.False(t, isClosed(second), "Opened after Shut returned a closed channel")
		assert.True(t, isClosed(first), "old channel open after Shut")
	})

	t.Run("OpenedAfterOpen", func(t *testing.T) {
		var gate Gate
		gate.Open()
		assert.True(
			t,
			isClosed(gate.Opened()),
			"first Opened while open returned an open channel",
		)
	})
}

func TestGateOpenedIdentity(t *testing.T) {
	t.Run("WhileShut", func(t *testing.T) {
		var gate Gate
		assert.Equal(t, gate.Opened(), gate.Opened(), "Opened returned two different channels")
	})

	t.Run("WhileOpen", func(t *testing.T) {
		var gate Gate
		gate.Open()
		assert.Equal(t, gate.Opened(), gate.Opened(), "Opened returned two different channels")
	})
}

func TestGateWaitNilContext(t *testing.T) {
	var nilContext context.Context
	var gate Gate
	assert.PanicsWithValue(t, "ctxsync: nil Context", func() {
		_ = gate.Wait(nilContext)
	}, "while shut")

	gate.Open()
	assert.PanicsWithValue(t, "ctxsync: nil Context", func() {
		_ = gate.Wait(nilContext)
	}, "while open")
}

func TestGateWaitOpenCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var gate Gate
	gate.Open()
	assert.NoError(t, gate.Wait(ctx))
}

// TestGateWaitShutCanceledContext runs in a bubble, so a Wait that blocked instead of returning
// at once would leave the bubble deadlocked and fail the test.
func TestGateWaitShutCanceledContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		var gate Gate
		assert.ErrorIs(t, gate.Wait(ctx), context.Canceled)
	})
}

// TestGateWaitBlocking has no case where Open and cancellation are both ready. The outcome-wins
// proof for Gate's blocking path is TestWaitClosedOutcomeWins, so Gate.Wait must keep waiting
// through waitClosed.
func TestGateWaitBlocking(t *testing.T) {
	tests := []struct {
		name     string
		trigger  func(gate *Gate, cancel context.CancelFunc)
		expected error
	}{
		{
			name: "Open",
			trigger: func(gate *Gate, _ context.CancelFunc) {
				gate.Open()
			},
			expected: nil,
		},
		{
			name: "OpenThenShut",
			trigger: func(gate *Gate, _ context.CancelFunc) {
				gate.Open()
				gate.Shut()
			},
			expected: nil,
		},
		{
			name: "Cancel",
			trigger: func(_ *Gate, cancel context.CancelFunc) {
				cancel()
			},
			expected: context.Canceled,
		},
		{
			name: "Deadline",
			trigger: func(_ *Gate, _ context.CancelFunc) {
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
				var gate Gate
				results := startWait(func() error { return gate.Wait(ctx) })
				synctest.Wait()
				require.Empty(t, results, "Wait returned before the trigger")

				tc.trigger(&gate, cancel)
				synctest.Wait()
				assertWaitResult(t, results, cancel, tc.expected)
			})
		})
	}
}

// TestGateRedundantShut checks that a Shut on a shut gate keeps the channel for the next opening,
// so the next Open still closes it.
func TestGateRedundantShut(t *testing.T) {
	t.Run("Opened", func(t *testing.T) {
		var gate Gate
		opened := gate.Opened()
		gate.Shut()
		gate.Open()
		assert.True(t, isClosed(opened), "Open did not close the channel after a redundant Shut")
	})

	t.Run("Wait", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
			defer cancel()
			var gate Gate
			results := startWait(func() error { return gate.Wait(ctx) })
			synctest.Wait()
			require.Empty(t, results, "Wait returned before Open")

			gate.Shut()
			gate.Open()
			synctest.Wait()
			assertWaitResult(t, results, cancel, nil)
		})
	})
}

// TestGateWaitAfterOpenShut checks that a Wait that starts after an opening has ended does not
// report it. The test calls Opened before that opening, so a Shut that kept the opening's closed
// channel would make both Wait calls return nil.
func TestGateWaitAfterOpenShut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var gate Gate
		gate.Opened()
		gate.Open()
		gate.Shut()

		canceledCtx, cancelNow := context.WithCancel(t.Context())
		cancelNow()
		assert.ErrorIs(t, gate.Wait(canceledCtx), context.Canceled)

		ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
		defer cancel()
		results := startWait(func() error { return gate.Wait(ctx) })
		synctest.Wait()
		require.Empty(t, results, "Wait returned for an opening that ended before it started")

		gate.Open()
		synctest.Wait()
		assertWaitResult(t, results, cancel, nil)
	})
}

// TestGateHappensBefore hands a plain int from a writer to a reader through the gate, so the race
// detector reports the read if the gate gives no happens-before edge. The reader reads before it
// blocks on anything else. The test calls Opened first, and the writer waits for the reader to
// start and yields before it writes, so Open usually closes the channel while the reader is
// blocked on it. The started channel orders the reader before the writer, the opposite of the
// edge under test, so it cannot hide a missing edge from Open to the reader. The Opened case
// receives from the channel fetched before the goroutines start, so the mutex inside Opened
// cannot add an edge that hides the one under test.
//
// The test runs outside synctest, so every wait gives up after boundedWaitTimeout and the
// reader reports a timeout as a failure instead of blocking the test forever.
func TestGateHappensBefore(t *testing.T) {
	tests := []struct {
		name        string
		waitForOpen func(ctx context.Context, gate *Gate, opened <-chan struct{}) error
	}{
		{
			name: "Wait",
			waitForOpen: func(ctx context.Context, gate *Gate, _ <-chan struct{}) error {
				ctx, cancel := context.WithTimeout(ctx, boundedWaitTimeout)
				defer cancel()
				return gate.Wait(ctx)
			},
		},
		{
			name: "Opened",
			waitForOpen: func(_ context.Context, _ *Gate, opened <-chan struct{}) error {
				timer := time.NewTimer(boundedWaitTimeout)
				defer timer.Stop()
				select {
				case <-opened:
					return nil
				case <-timer.C:
					return errors.New("Opened channel not closed before the timeout")
				}
			},
		},
		{
			name: "IsOpen",
			waitForOpen: func(_ context.Context, gate *Gate, _ <-chan struct{}) error {
				if !pollUntil(gate.IsOpen) {
					return errors.New("IsOpen still false at the timeout")
				}
				return nil
			},
		},
	}
	const written = 42
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for i := range raceAwareIterations {
				var gate Gate
				opened := gate.Opened()
				var payload int
				var observed int
				var waitErr error
				started := make(chan struct{})
				var goroutines sync.WaitGroup
				goroutines.Go(func() {
					<-started
					runtime.Gosched()
					payload = written
					gate.Open()
				})
				goroutines.Go(func() {
					close(started)
					waitErr = tc.waitForOpen(t.Context(), &gate, opened)
					if waitErr != nil {
						t.Errorf("iteration %d: wait for Open: %v", i, waitErr)
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

// TestGateStress checks that every nil Wait reports an opening it could have observed. The opener
// keeps opening set whenever the gate is open and advances openings before every Open. A waiter
// that read opening as false saw the gate shut, so an opening it reports must have started later
// and advanced openings past the value it read first. The Open-to-Wait happens-before edge makes
// the waiter's later read of openings see that advance. Each waiter calls Wait once per cycle, so
// Wait calls that start during later cycles are checked too. The gate stays open after the last
// opening, so every remaining Wait returns nil and every waiter ends.
func TestGateStress(t *testing.T) {
	const (
		cycles  = 8
		waiters = 4
	)
	synctest.Test(t, func(t *testing.T) {
		for i := range raceAwareIterations {
			var gate Gate
			var opening atomic.Bool
			var openings atomic.Int64
			var goroutines sync.WaitGroup
			goroutines.Go(func() {
				for range cycles {
					opening.Store(true)
					openings.Add(1)
					gate.Open()
					gate.Shut()
					opening.Store(false)
				}
				// The gate stays open after the last opening, so every waiter returns.
				opening.Store(true)
				openings.Add(1)
				gate.Open()
			})
			for range waiters {
				goroutines.Go(func() {
					for range cycles {
						openingsBefore := openings.Load()
						wasOpening := opening.Load()
						err := gate.Wait(t.Context())
						if !assert.NoError(t, err, "iteration %d", i) {
							return
						}
						advanced := openings.Load() > openingsBefore
						assert.True(
							t,
							wasOpening || advanced,
							"iteration %d: Wait returned nil with no opening it could observe",
							i,
						)
					}
				})
			}
			goroutines.Wait()
		}
	})
}

func TestGateAllocations(t *testing.T) {
	var gate Gate
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		_ = gate.IsOpen()
	}), "IsOpen")

	gate.Opened()
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		_ = gate.Opened()
	}), "Opened while shut after the first Opened")

	gate.Open()
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		_ = gate.Opened()
	}), "Opened while open")

	// The context is already canceled, so a broken Wait returns an error instead of blocking the
	// test.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.NoError(t, gate.Wait(ctx), "Wait on an open gate with a canceled context")
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		_ = gate.Wait(ctx)
	}), "Wait on an open gate")
}

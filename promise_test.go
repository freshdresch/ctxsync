package ctxsync

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	errAbandoned = errors.New("producer abandoned the promise")
	errProducer  = errors.New("producer failed")
)

func TestPromiseZeroHandles(t *testing.T) {
	var resolver Resolver[int]
	assert.PanicsWithValue(t, "ctxsync: zero Resolver, use NewPromise", func() {
		resolver.Resolve(1, nil)
	}, "Resolve")

	var future Future[int]
	tests := []struct {
		name   string
		method func()
	}{
		{name: "Done", method: func() { future.Done() }},
		{name: "IsResolved", method: func() { future.IsResolved() }},
		{name: "Result", method: func() { _, _, _ = future.Result() }},
		{name: "Wait", method: func() { _ = future.Wait(t.Context()) }},
		{name: "Value", method: func() { _, _ = future.Value(t.Context()) }},
	}
	for _, tc := range tests {
		assert.PanicsWithValue(t, "ctxsync: zero Future, use NewPromise", tc.method, tc.name)
	}
}

func TestFutureNilContext(t *testing.T) {
	var nilContext context.Context
	var zeroFuture Future[int]
	unresolved, _ := NewPromise[int]()
	resolved, resolver := NewPromise[int]()
	resolver.Resolve(1, nil)

	futures := []struct {
		name   string
		future Future[int]
	}{
		{name: "Zero", future: zeroFuture},
		{name: "Unresolved", future: unresolved},
		{name: "Resolved", future: resolved},
	}
	for _, tc := range futures {
		assert.PanicsWithValue(t, "ctxsync: nil Context", func() {
			_ = tc.future.Wait(nilContext)
		}, "Wait on a %s Future", tc.name)
		assert.PanicsWithValue(t, "ctxsync: nil Context", func() {
			_, _ = tc.future.Value(nilContext)
		}, "Value on a %s Future", tc.name)
	}
}

func TestResolverFirstWins(t *testing.T) {
	future, resolver := NewPromise[int]()
	require.True(t, resolver.Resolve(1, errProducer), "first Resolve")

	assert.False(t, resolver.Resolve(2, nil), "second Resolve")
	assert.False(t, resolver.Resolve(3, errAbandoned), "third Resolve")
	assert.True(t, future.IsResolved(), "IsResolved after a Resolve that returned false")
	value, ok, err := future.Result()
	assert.True(t, ok, "Result after a Resolve that returned false")
	assert.Equal(t, 1, value)
	assert.ErrorIs(t, err, errProducer)
}

// loserObservation is what a Resolve caller that returned false observed right after it returned.
type loserObservation struct {
	lost       bool
	isResolved bool
	value      int
	ok         bool
	err        error
}

// readerObservation is what a reader that polled Result until ok observed.
type readerObservation struct {
	polled bool
	value  int
	err    error
}

// TestResolverConcurrent races many Resolve calls while readers poll Result. Resolver i resolves
// with the value i and its own error. Under -race, a Result that reads the value before it loads
// the flag races with the winner's store. A Resolve that lets a loser return false before the
// winner stores its outcome fails the loser's checks.
func TestResolverConcurrent(t *testing.T) {
	const (
		resolvers = 8
		readers   = 4
	)
	resolverErrors := make([]error, resolvers)
	for i := range resolverErrors {
		resolverErrors[i] = fmt.Errorf("resolver %d", i)
	}
	for iteration := range raceAwareIterations {
		future, resolver := NewPromise[int]()
		start := make(chan struct{})
		won := make([]bool, resolvers)
		losers := make([]loserObservation, resolvers)
		readerResults := make([]readerObservation, readers)
		var goroutines sync.WaitGroup
		for i := range readers {
			goroutines.Go(func() {
				<-start
				polled := pollUntil(func() bool {
					_, ok, _ := future.Result()
					return ok
				})
				if !polled {
					return
				}
				value, _, err := future.Result()
				readerResults[i] = readerObservation{polled: true, value: value, err: err}
			})
		}
		for i := range resolvers {
			goroutines.Go(func() {
				<-start
				if resolver.Resolve(i, resolverErrors[i]) {
					won[i] = true
					return
				}
				isResolved := future.IsResolved()
				value, ok, err := future.Result()
				losers[i] = loserObservation{
					lost:       true,
					isResolved: isResolved,
					value:      value,
					ok:         ok,
					err:        err,
				}
			})
		}
		close(start)
		goroutines.Wait()

		winner := -1
		for i, resolverWon := range won {
			if !resolverWon {
				continue
			}
			require.Equal(t, -1, winner, "iteration %d: resolver %d also won", iteration, i)
			winner = i
		}
		require.NotEqual(t, -1, winner, "iteration %d: no Resolve returned true", iteration)

		for i, loser := range losers {
			if i == winner {
				continue
			}
			where := fmt.Sprintf("iteration %d: after resolver %d lost", iteration, i)
			require.True(t, loser.lost, where)
			assert.True(t, loser.isResolved, "%s: IsResolved", where)
			assert.True(t, loser.ok, "%s: Result ok", where)
			assert.Equal(t, winner, loser.value, "%s: Result value", where)
			assert.ErrorIs(t, loser.err, resolverErrors[winner], "%s: Result error", where)
		}
		for i, observed := range readerResults {
			where := fmt.Sprintf("iteration %d: reader %d", iteration, i)
			require.True(t, observed.polled, "%s: timed out", where)
			assert.Equal(t, winner, observed.value, where)
			assert.ErrorIs(t, observed.err, resolverErrors[winner], where)
		}
	}
}

// runRecommendedProducer resolves a new promise with the pattern that the doc comment of
// NewPromise recommends, in a producer goroutine whose deferred recover stops a panic in work,
// as errgroup-style wrappers do. It waits for the producer goroutine to end and returns the
// Future and the value that recover returned.
func runRecommendedProducer(
	ctx context.Context,
	work func(ctx context.Context) (int, error),
) (Future[int], any) {
	future, resolver := NewPromise[int]()
	var recovered any
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		defer func() { recovered = recover() }()
		defer resolver.Resolve(0, errAbandoned)
		resolver.Resolve(work(ctx))
	}()
	<-finished
	return future, recovered
}

// TestPromiseRecommendedProducer checks the pattern that the docs and ExampleNewPromise teach.
// It mostly exercises Go's defer rules and first-wins.
func TestPromiseRecommendedProducer(t *testing.T) {
	errEarly := errors.New("early return")
	tests := []struct {
		name          string
		work          func(ctx context.Context) (int, error)
		expectedValue int
		expectedErr   error
		expectedPanic any
	}{
		{
			name: "Success",
			work: func(_ context.Context) (int, error) {
				return 42, nil
			},
			expectedValue: 42,
		},
		{
			name: "EarlyReturn",
			work: func(ctx context.Context) (int, error) {
				if ctx.Err() == nil {
					return 7, errEarly
				}
				return 42, nil
			},
			expectedValue: 7,
			expectedErr:   errEarly,
		},
		{
			name: "Panic",
			work: func(_ context.Context) (int, error) {
				panic("work panicked")
			},
			expectedErr:   errAbandoned,
			expectedPanic: "work panicked",
		},
		{
			name: "Goexit",
			work: func(_ context.Context) (int, error) {
				runtime.Goexit()
				return 42, nil
			},
			expectedErr: errAbandoned,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			future, recovered := runRecommendedProducer(t.Context(), tc.work)
			assert.Equal(t, tc.expectedPanic, recovered, "recovered panic")
			value, ok, err := future.Result()
			require.True(t, ok, "promise not resolved after the producer ended")
			assert.Equal(t, tc.expectedValue, value)
			if tc.expectedErr == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, tc.expectedErr)
			}
		})
	}
}

// TestPromisePerPathProducer checks the form for producers that cannot be split: a deferred
// abandonment call and an explicit Resolve on each normal path.
func TestPromisePerPathProducer(t *testing.T) {
	for _, forgetToResolve := range []bool{false, true} {
		future, resolver := NewPromise[int]()
		finished := make(chan struct{})
		go func() {
			defer close(finished)
			defer resolver.Resolve(0, errAbandoned)
			if forgetToResolve {
				return
			}
			resolver.Resolve(1, nil)
		}()
		<-finished

		value, ok, err := future.Result()
		require.True(t, ok, "forgetToResolve %t: promise not resolved", forgetToResolve)
		if forgetToResolve {
			assert.Equal(t, 0, value)
			assert.ErrorIs(t, err, errAbandoned)
		} else {
			assert.Equal(t, 1, value)
			assert.NoError(t, err)
		}
	}
}

func TestFutureResult(t *testing.T) {
	future, resolver := NewPromise[string]()
	assert.False(t, future.IsResolved(), "IsResolved before Resolve")
	value, ok, err := future.Result()
	assert.False(t, ok, "Result ok before Resolve")
	assert.Empty(t, value, "Result value before Resolve")
	assert.NoError(t, err, "Result error before Resolve")

	require.True(t, resolver.Resolve("partial", errProducer))
	assert.True(t, future.IsResolved(), "IsResolved after Resolve")
	value, ok, err = future.Result()
	assert.True(t, ok, "Result ok after Resolve")
	assert.Equal(t, "partial", value, "a failure still carries its value")
	assert.ErrorIs(t, err, errProducer)
}

func TestFutureDoneIdentity(t *testing.T) {
	future, resolver := NewPromise[int]()
	first := future.Done()
	second := future.Done()
	assert.Equal(t, first, second, "Done returned two different channels")
	assert.False(t, isClosed(first), "channel closed before Resolve")

	resolver.Resolve(1, nil)
	assert.True(t, isClosed(first), "channel open after Resolve")
	assert.Equal(t, first, future.Done(), "Done after Resolve returned a different channel")
}

func TestFutureResolvedCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	future, resolver := NewPromise[int]()
	resolver.Resolve(42, errProducer)

	assert.NoError(t, future.Wait(ctx), "Wait")
	value, err := future.Value(ctx)
	assert.Equal(t, 42, value, "Value")
	assert.ErrorIs(t, err, errProducer, "Value")
}

// TestFutureUnresolvedCanceledContext runs each wait in a goroutine in a bubble, so a wait that
// blocks instead of returning at once is reported by assertFutureWaitResult, which then resolves
// the promise to free it.
func TestFutureUnresolvedCanceledContext(t *testing.T) {
	t.Run("Wait", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			future, resolver := NewPromise[int]()

			results := startWait(func() error { return future.Wait(ctx) })
			synctest.Wait()
			assertFutureWaitResult(t, resolver, results, cancel, context.Canceled)
		})
	})

	t.Run("Value", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			future, resolver := NewPromise[int]()

			// The waiter writes value before it sends its error, and the receive in
			// assertFutureWaitResult orders that write before the read below.
			var value int
			results := startWait(func() error {
				var err error
				value, err = future.Value(ctx)
				return err
			})
			synctest.Wait()
			assertFutureWaitResult(t, resolver, results, cancel, context.Canceled)
			// The promise is unresolved when Value gives up, so this is a sanity check. It
			// cannot catch a Value that returns the stored value with ctx.Err().
			assert.Equal(t, 0, value, "Value")
		})
	})
}

// futureTrigger is one way to end a blocked wait in the blocking tests.
type futureTrigger struct {
	name    string
	trigger func(resolver Resolver[int], cancel context.CancelFunc)
	// waitErr is what Wait returns, and valueErr and value are what Value returns.
	waitErr  error
	valueErr error
	value    int
}

var futureTriggers = []futureTrigger{
	{
		name: "Resolve",
		trigger: func(resolver Resolver[int], _ context.CancelFunc) {
			resolver.Resolve(42, errProducer)
		},
		waitErr:  nil,
		valueErr: errProducer,
		value:    42,
	},
	{
		name: "Cancel",
		trigger: func(_ Resolver[int], cancel context.CancelFunc) {
			cancel()
		},
		waitErr:  context.Canceled,
		valueErr: context.Canceled,
	},
	{
		name: "Deadline",
		trigger: func(_ Resolver[int], _ context.CancelFunc) {
			time.Sleep(waitTimeout)
		},
		waitErr:  context.DeadlineExceeded,
		valueErr: context.DeadlineExceeded,
	},
}

// TestFutureWaitBlocking has no case where Resolve and cancellation are both ready. The
// outcome-wins proof for Future's blocking path is TestWaitClosedOutcomeWins, so Future.Wait must
// keep waiting through waitClosed.
func TestFutureWaitBlocking(t *testing.T) {
	for _, tc := range futureTriggers {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
				defer cancel()
				future, resolver := NewPromise[int]()
				results := startWait(func() error { return future.Wait(ctx) })
				synctest.Wait()
				require.Empty(t, results, "Wait returned before the trigger")

				tc.trigger(resolver, cancel)
				synctest.Wait()
				assertFutureWaitResult(t, resolver, results, cancel, tc.waitErr)
			})
		})
	}
}

func TestFutureValueBlocking(t *testing.T) {
	for _, tc := range futureTriggers {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
				defer cancel()
				future, resolver := NewPromise[int]()
				// The waiter writes value before it sends its error, and the receive in
				// assertFutureWaitResult orders that write before the read below.
				var value int
				results := startWait(func() error {
					var err error
					value, err = future.Value(ctx)
					return err
				})
				synctest.Wait()
				require.Empty(t, results, "Value returned before the trigger")

				tc.trigger(resolver, cancel)
				synctest.Wait()
				assertFutureWaitResult(t, resolver, results, cancel, tc.valueErr)
				// In the Cancel and Deadline cases, the promise is unresolved when Value gives
				// up, so this is a sanity check. It cannot catch a Value that returns the stored
				// value with ctx.Err().
				assert.Equal(t, tc.value, value)
			})
		})
	}
}

// TestFutureWaitIgnoresProducerError checks that Wait reports only the waiter's own cancellation
// and Result reports only the producer's outcome, even when both are context.Canceled.
func TestFutureWaitIgnoresProducerError(t *testing.T) {
	t.Run("ProducerError", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			future, resolver := NewPromise[int]()
			resolver.Resolve(0, errProducer)
			results := startWait(func() error { return future.Wait(ctx) })
			synctest.Wait()
			assertFutureWaitResult(t, resolver, results, cancel, nil)
			_, ok, err := future.Result()
			assert.True(t, ok)
			assert.ErrorIs(t, err, errProducer)
		})
	})

	t.Run("ProducerCanceled", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			future, resolver := NewPromise[int]()
			resolver.Resolve(0, context.Canceled)
			results := startWait(func() error { return future.Wait(ctx) })
			synctest.Wait()
			assertFutureWaitResult(t, resolver, results, cancel, nil)
			_, ok, err := future.Result()
			assert.True(t, ok)
			assert.ErrorIs(t, err, context.Canceled, "Result")
		})
	})

	t.Run("WaitCanceled", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			future, resolver := NewPromise[int]()
			results := startWait(func() error { return future.Wait(ctx) })
			synctest.Wait()
			// Check Result before assertFutureWaitResult, which resolves the promise if Wait
			// did not return.
			_, ok, err := future.Result()
			assert.False(t, ok, "Result ok on an unresolved promise")
			assert.NoError(t, err, "Result error on an unresolved promise")
			assertFutureWaitResult(t, resolver, results, cancel, context.Canceled)
		})
	})
}

// errReaderWon reports that the reader of the LosingResolve case resolved the promise before the
// writer did, so that iteration tests nothing.
var errReaderWon = errors.New("reader's Resolve won")

// TestPromiseHappensBefore hands a plain int from a writer to a reader through the promise, so
// the race detector reports the read if the promise gives no happens-before edge. The reader
// reads before it blocks on anything else. The writer waits for the reader to start and yields
// before it writes, so Resolve usually runs while the reader is blocked or polling. The started
// channel orders the reader before the writer, the opposite of the edge under test, so it cannot
// hide a missing edge from Resolve to the reader.
//
// In the LosingResolve case the reader yields and then calls Resolve once. It cannot poll,
// because a Resolve before the writer's would win. An iteration where the reader wins tests
// nothing, and the case requires at least one iteration where the reader lost.
//
// The test runs outside synctest, so every wait gives up after boundedWaitTimeout and the
// reader reports a timeout as a failure instead of blocking the test forever.
func TestPromiseHappensBefore(t *testing.T) {
	tests := []struct {
		name           string
		waitForResolve func(ctx context.Context, future Future[int], resolver Resolver[int]) error
	}{
		{
			name: "Wait",
			waitForResolve: func(ctx context.Context, future Future[int], _ Resolver[int]) error {
				ctx, cancel := context.WithTimeout(ctx, boundedWaitTimeout)
				defer cancel()
				return future.Wait(ctx)
			},
		},
		{
			name: "Done",
			waitForResolve: func(_ context.Context, future Future[int], _ Resolver[int]) error {
				timer := time.NewTimer(boundedWaitTimeout)
				defer timer.Stop()
				select {
				case <-future.Done():
					return nil
				case <-timer.C:
					return errors.New("Done channel not closed before the timeout")
				}
			},
		},
		{
			name: "IsResolved",
			waitForResolve: func(_ context.Context, future Future[int], _ Resolver[int]) error {
				if !pollUntil(future.IsResolved) {
					return errors.New("IsResolved still false at the timeout")
				}
				return nil
			},
		},
		{
			name: "Result",
			waitForResolve: func(_ context.Context, future Future[int], _ Resolver[int]) error {
				polled := pollUntil(func() bool {
					_, ok, _ := future.Result()
					return ok
				})
				if !polled {
					return errors.New("Result still not ok at the timeout")
				}
				return nil
			},
		},
		{
			name: "Value",
			waitForResolve: func(ctx context.Context, future Future[int], _ Resolver[int]) error {
				ctx, cancel := context.WithTimeout(ctx, boundedWaitTimeout)
				defer cancel()
				_, err := future.Value(ctx)
				return err
			},
		},
		{
			name: "LosingResolve",
			waitForResolve: func(_ context.Context, _ Future[int], resolver Resolver[int]) error {
				for range 100 {
					runtime.Gosched()
				}
				if resolver.Resolve(-1, nil) {
					return errReaderWon
				}
				return nil
			},
		},
	}
	const written = 42
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			readerLost := 0
			for i := range raceAwareIterations {
				future, resolver := NewPromise[int]()
				var payload int
				var observed int
				var waitErr error
				started := make(chan struct{})
				var goroutines sync.WaitGroup
				goroutines.Go(func() {
					<-started
					runtime.Gosched()
					payload = written
					resolver.Resolve(written, nil)
				})
				goroutines.Go(func() {
					close(started)
					waitErr = tc.waitForResolve(t.Context(), future, resolver)
					if errors.Is(waitErr, errReaderWon) {
						return
					}
					if waitErr != nil {
						t.Errorf("iteration %d: wait for Resolve: %v", i, waitErr)
						return
					}
					observed = payload
				})
				goroutines.Wait()

				// The reader already reported a failed wait, or it won and tested nothing, and
				// observed was never read.
				if errors.Is(waitErr, errReaderWon) {
					continue
				}
				if waitErr != nil {
					return
				}
				readerLost++
				assert.Equal(t, written, observed, "iteration %d", i)
			}
			assert.Positive(t, readerLost, "no iteration observed the resolution")
		})
	}
}

func TestPromiseHandleCopies(t *testing.T) {
	future, resolver := NewPromise[int]()
	futureCopy := future
	resolverCopy := resolver
	otherFuture, otherResolver := NewPromise[int]()

	// == and not assert.Equal, because reflect.DeepEqual follows the pointer and reads the
	// promise's value without its mutex.
	assert.True(t, future == futureCopy, "copied Futures not equal")
	assert.True(t, resolver == resolverCopy, "copied Resolvers not equal")
	assert.False(t, future == otherFuture, "Futures of two promises equal")
	assert.False(t, resolver == otherResolver, "Resolvers of two promises equal")

	byFuture := map[Future[int]]string{future: "first", otherFuture: "other"}
	assert.Equal(t, "first", byFuture[futureCopy], "map lookup by a copied Future")

	assert.Equal(t, future.Done(), futureCopy.Done(), "copies returned different channels")
	require.True(t, resolverCopy.Resolve(5, nil), "Resolve through a copy")
	assert.False(t, resolver.Resolve(6, nil), "Resolve through the original after the copy")
	assert.True(t, future.IsResolved(), "original Future after Resolve through a copy")
	value, ok, err := futureCopy.Result()
	assert.True(t, ok)
	assert.Equal(t, 5, value)
	assert.NoError(t, err)
	assert.False(t, otherFuture.IsResolved(), "Resolve reached another promise")
}

func TestPromiseAllocations(t *testing.T) {
	future, resolver := NewPromise[int]()
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		_ = future.IsResolved()
	}), "IsResolved before Resolve")
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		_, _, _ = future.Result()
	}), "Result before Resolve")
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		_ = future.Done()
	}), "Done before Resolve")

	resolver.Resolve(42, errProducer)
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		_ = future.IsResolved()
	}), "IsResolved after Resolve")
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		_, _, _ = future.Result()
	}), "Result after Resolve")
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		_ = future.Done()
	}), "Done after Resolve")

	// The context is already canceled, so a broken Wait or Value returns an error instead of
	// blocking the test.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.NoError(t, future.Wait(ctx), "Wait on a resolved promise with a canceled context")
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		_ = future.Wait(ctx)
	}), "Wait on a resolved promise")
	_, err := future.Value(ctx)
	require.ErrorIs(t, err, errProducer, "Value on a resolved promise with a canceled context")
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		_, _ = future.Value(ctx)
	}), "Value on a resolved promise")
}

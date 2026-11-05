package ctxsync

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// valueWithin returns what future.Value returns under a context that ends after waitTimeout. It
// runs inside a synctest bubble, so a Future that is never resolved fails the test with
// context.DeadlineExceeded instead of deadlocking the bubble, and takes no real time.
func valueWithin[T any](t *testing.T, future Future[T]) (T, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()
	return future.Value(ctx)
}

// goFromHelper calls Go(ctx, work) from a helper goroutine and returns the Future. A broken Go
// that ran work in its caller's goroutine would run a runtime.Goexit in work on that goroutine.
// On the test's goroutine, that ends the test binary. On the helper, it ends only the helper, and
// goFromHelper fails the test by name. goFromHelper waits for the helper without a timeout. work
// must not block, because a broken Go that ran it in the helper would then deadlock the bubble.
func goFromHelper[T any](
	ctx context.Context,
	t *testing.T,
	work func(ctx context.Context) (T, error),
) Future[T] {
	t.Helper()
	futures := make(chan Future[T], 1)
	go func() {
		defer close(futures)
		futures <- Go(ctx, work)
	}()
	future, ok := <-futures
	require.True(t, ok, "Go did not return to its caller")
	return future
}

// TestGoReturnsBeforeWorkFinishes calls Go from a helper goroutine, because a Go that ran work in
// the caller's goroutine would block the test's goroutine and deadlock the bubble, which ends the
// test binary. The checks before work is released use assert, because a require would end the
// bubble's main goroutine while the helper is blocked, which also ends the test binary.
func TestGoReturnsBeforeWorkFinishes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		releaseWork := sync.OnceFunc(func() { close(release) })
		defer releaseWork()

		futures := make(chan Future[int], 1)
		go func() {
			futures <- Go(t.Context(), func(_ context.Context) (int, error) {
				<-release
				return 42, nil
			})
		}()
		synctest.Wait()
		var future Future[int]
		select {
		case future = <-futures:
			assert.False(t, future.IsResolved(), "Future resolved before work finished")
		default:
			assert.Fail(t, "Go did not return before work finished")
		}

		releaseWork()
		synctest.Wait()
		if future == (Future[int]{}) {
			return
		}
		value, ok, err := future.Result()
		assert.True(t, ok, "Future not resolved after work returned")
		assert.Equal(t, 42, value)
		assert.NoError(t, err)
	})
}

func TestGoResult(t *testing.T) {
	tests := []struct {
		name  string
		value int
		err   error
	}{
		{name: "Success", value: 42},
		{name: "Error", value: 7, err: errProducer},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				future := goFromHelper(t.Context(), t, func(_ context.Context) (int, error) {
					return tc.value, tc.err
				})
				value, err := valueWithin(t, future)
				assert.Equal(t, tc.value, value)
				if tc.err == nil {
					assert.NoError(t, err)
				} else {
					// The Future holds the error exactly as work returned it. ErrorIs alone
					// would pass if Go wrapped it, so == checks that Go did not.
					assert.ErrorIs(t, err, tc.err)
					assert.True(t, err == tc.err, "work's error wrapped: %v", err)
				}
			})
		})
	}
}

func TestGoDoneContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		var received context.Context
		future := goFromHelper(ctx, t, func(ctx context.Context) (int, error) {
			received = ctx
			return 42, ctx.Err()
		})
		value, err := valueWithin(t, future)
		assert.Equal(t, 42, value, "work did not run with a done context")
		assert.ErrorIs(t, err, context.Canceled, "work's error")
		assert.True(t, received == ctx, "work received a different context")
	})
}

// goContextKey keys the value that TestGoContext stores in the context it passes to Go.
type goContextKey struct{}

func TestGoContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.WithValue(t.Context(), goContextKey{}, "passed to Go")
		var received context.Context
		future := goFromHelper(ctx, t, func(ctx context.Context) (int, error) {
			received = ctx
			return 0, nil
		})
		_, err := valueWithin(t, future)
		require.NoError(t, err)
		assert.True(t, received == ctx, "work received a different context")
	})
}

func TestGoGoexit(t *testing.T) {
	tests := []struct {
		name string
		work func(ctx context.Context) (int, error)
	}{
		{
			name: "InWork",
			work: func(_ context.Context) (int, error) {
				runtime.Goexit()
				return 42, nil
			},
		},
		{
			name: "InDeferredCall",
			work: func(_ context.Context) (int, error) {
				defer runtime.Goexit()
				return 42, nil
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				value, err := valueWithin(t, goFromHelper(t.Context(), t, tc.work))
				assert.Equal(t, 0, value)
				assert.ErrorIs(t, err, ErrGoexit)
				assert.True(t, err == ErrGoexit, "ErrGoexit wrapped: %v", err)
			})
		})
	}
}

// panicValue is the panic value of the tests that call runWork directly. It is a pointer to a
// struct of non-zero size, so assert.Same tells it apart from any other value.
type panicValue struct {
	text string
}

// runWorkRecovered calls runWork with work in a goroutine of its own, under a deferred recover
// that stops a panic that runWork raises again, so the test binary keeps running. It waits up to
// boundedWaitTimeout for that goroutine to end and returns the Future, what recover returned,
// and whether the goroutine ended. It runs outside synctest, so the wait is bounded in real time.
func runWorkRecovered(
	ctx context.Context,
	work func(ctx context.Context) (int, error),
) (future Future[int], recovered any, ended bool) {
	future, resolver := NewPromise[int]()
	// The goroutine writes panicked, not the result recovered, so a timeout that returns while
	// the goroutine still runs does not race with it.
	var panicked any
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		defer func() { panicked = recover() }()
		runWork(ctx, work, resolver)
	}()
	select {
	case <-finished:
		return future, panicked, true
	case <-time.After(boundedWaitTimeout):
		return future, nil, false
	}
}

// TestGoRunWorkReraisesPanic calls runWork directly, because a panic that runWork raises again
// in the goroutine that Go starts ends the test binary.
func TestGoRunWorkReraisesPanic(t *testing.T) {
	sentinel := &panicValue{text: "work panicked"}
	tests := []struct {
		name string
		work func(ctx context.Context) (int, error)
	}{
		{
			name: "Panic",
			work: func(_ context.Context) (int, error) {
				panic(sentinel)
			},
		},
		{
			name: "PanicAfterGoexit",
			work: func(_ context.Context) (int, error) {
				defer panic(sentinel)
				runtime.Goexit()
				return 42, nil
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			future, recovered, ended := runWorkRecovered(t.Context(), tc.work)
			require.True(t, ended, "runWork did not end")
			assert.Same(t, sentinel, recovered, "the panic that runWork raised again")
			assert.False(t, future.IsResolved(), "Future resolved on a panic")
		})
	}
}

// TestGoGoexitDuringPanic calls runWork directly, so the test binary keeps running even if
// runWork lets the discarded panic escape.
func TestGoGoexitDuringPanic(t *testing.T) {
	future, recovered, ended := runWorkRecovered(t.Context(), func(_ context.Context) (int, error) {
		defer runtime.Goexit()
		panic(&panicValue{text: "discarded by runtime.Goexit"})
	})
	require.True(t, ended, "runWork did not end")
	assert.Nil(t, recovered, "a panic escaped runWork")
	value, ok, err := future.Result()
	assert.True(t, ok, "Future not resolved")
	assert.Equal(t, 0, value)
	assert.ErrorIs(t, err, ErrGoexit)
}

func TestGoDeferredCallsInWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cleaned := false
		future := goFromHelper(t.Context(), t, func(_ context.Context) (int, error) {
			defer func() { cleaned = true }()
			return 1, nil
		})
		_, err := valueWithin(t, future)
		require.NoError(t, err)
		assert.True(t, cleaned, "a deferred call in work had not run when Wait returned")
	})
}

// TestGoArguments relies on Go checking for nil work in the caller. If Go lost that check, the
// nil call would panic inside the goroutine Go starts, which no test can recover, so this test
// would fail by name but the test binary would then end.
func TestGoArguments(t *testing.T) {
	var nilContext context.Context
	var nilWork func(ctx context.Context) (int, error)
	synctest.Test(t, func(t *testing.T) {
		var ran atomic.Bool
		work := func(_ context.Context) (int, error) {
			ran.Store(true)
			return 0, nil
		}
		assert.PanicsWithValue(t, "ctxsync: nil Context", func() {
			Go(nilContext, work)
		}, "nil context")
		assert.PanicsWithValue(t, "ctxsync: nil work", func() {
			Go(t.Context(), nilWork)
		}, "nil work")
		assert.PanicsWithValue(t, "ctxsync: nil Context", func() {
			Go(nilContext, nilWork)
		}, "nil context and nil work")
		synctest.Wait()
		assert.False(t, ran.Load(), "work ran after Go panicked")
	})
}

// TestGoHappensBefore passes plain ints between the caller and work, so the race detector reports
// an access that Go does not order. The caller-to-work edge is checked through a chain: the test
// goroutine starts goFromHelper's helper goroutine, which calls Go, which runs work. Any correct
// Go must order that chain through its go statement.
func TestGoHappensBefore(t *testing.T) {
	t.Run("Return", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			input := 21
			var output int
			future := goFromHelper(t.Context(), t, func(_ context.Context) (int, error) {
				output = input * 2
				return 0, nil
			})
			_, err := valueWithin(t, future)
			require.NoError(t, err)
			assert.Equal(t, 42, output)
		})
	})

	t.Run("Goexit", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			input := 21
			var output int
			future := goFromHelper(t.Context(), t, func(_ context.Context) (int, error) {
				output = input * 2
				runtime.Goexit()
				return 0, nil
			})
			_, err := valueWithin(t, future)
			require.ErrorIs(t, err, ErrGoexit)
			assert.Equal(t, 42, output)
		})
	})
}

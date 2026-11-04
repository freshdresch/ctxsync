package ctxsync

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// waitTimeout bounds the waits in the blocking wait tests. Those tests run in a synctest bubble,
// so the deadline case advances the fake clock past it and takes no real time.
const waitTimeout = time.Minute

// boundedWaitTimeout bounds every wait in the tests that run outside synctest, so a wrong
// implementation fails the test instead of blocking it forever.
const boundedWaitTimeout = 10 * time.Second

// pollUntil calls condition until it returns true and reports whether it did before
// boundedWaitTimeout. It is for tests outside synctest, where synctest.Wait is not available.
func pollUntil(condition func() bool) bool {
	deadline := time.Now().Add(boundedWaitTimeout)
	for !condition() {
		if time.Now().After(deadline) {
			return false
		}
		runtime.Gosched()
	}
	return true
}

// midChangeWindow is how long checkQueryMidChange waits to confirm that a query is still blocked
// on the mutex of a state change in progress.
const midChangeWindow = 100 * time.Millisecond

// pollUntilBlockedInWait reports whether a goroutine started by checkQueryMidChange is blocked in
// waitClosed before boundedWaitTimeout. It reads the stacks of all goroutines, because outside
// synctest nothing else shows that a waiter holds its channel and has released the mutex.
func pollUntilBlockedInWait() bool {
	stacks := make([]byte, 1<<20)
	return pollUntil(func() bool {
		length := runtime.Stack(stacks, true)
		for goroutine := range strings.SplitSeq(string(stacks[:length]), "\n\n") {
			header, _, _ := strings.Cut(goroutine, "\n")
			if strings.Contains(header, "[select") &&
				strings.Contains(goroutine, "ctxsync.waitClosed(") &&
				strings.Contains(goroutine, "ctxsync.checkQueryMidChange[") {
				return true
			}
		}
		return false
	})
}

// midChange is a state change split at the point where it has closed the waiters' channel but has
// not stored the new state, for checkQueryMidChange.
type midChange[T any] struct {
	wait  func(ctx context.Context) error
	query func() T
	// begin locks the type's mutex and closes the waiters' channel without storing the new state.
	begin func()
	// finish stores the new state and unlocks the mutex.
	finish func()
}

// checkQueryMidChange checks that a query agrees with a nil wait in the middle of a state change.
// It blocks a waiter, then runs change.begin. The waiter wakes, returns nil, and then calls the
// query. checkQueryMidChange requires that the waiter's Wait returned nil and that its query does
// not return while the state change is in progress. It then runs change.finish and requires that
// the query reports expected.
//
// It receives the Wait result before it checks the query, so a slow waiter cannot query after
// change.finish and let a lock-free query pass. It runs outside synctest, because a goroutine
// blocked on a mutex is not durably blocked, so every wait is bounded.
func checkQueryMidChange[T any](t *testing.T, change midChange[T], expected T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), boundedWaitTimeout)
	defer cancel()
	waitResults := make(chan error, 1)
	queryResults := make(chan T, 1)
	go func() {
		waitResults <- change.wait(ctx)
		queryResults <- change.query()
	}()
	require.True(t, pollUntilBlockedInWait(), "the waiter did not block in Wait")

	finish := sync.OnceFunc(change.finish)
	change.begin()
	defer finish()
	select {
	case err := <-waitResults:
		require.NoError(t, err, "Wait after the channel closed")
	case <-time.After(boundedWaitTimeout):
		require.FailNow(t, "Wait did not return after the channel closed")
	}
	select {
	case early := <-queryResults:
		require.FailNowf(t, "the query returned while the state change was in progress",
			"it returned %v", early)
	case <-time.After(midChangeWindow):
	}

	finish()
	select {
	case result := <-queryResults:
		assert.Equal(t, expected, result, "the query after the state change finished")
	case <-time.After(boundedWaitTimeout):
		require.FailNow(t, "the query did not return after the state change finished")
	}
}

// startWait starts a goroutine that calls wait and sends the result on the returned channel. The
// channel has capacity 1, so the goroutine never blocks on the send.
func startWait(wait func() error) <-chan error {
	results := make(chan error, 1)
	go func() {
		results <- wait()
	}()
	return results
}

// assertWaitResult checks, without blocking, that a waiter has sent expected on results. If no
// result has arrived, it reports the failure, cancels the waiter's context, and receives the
// waiter's result, so a wrong implementation fails this test instead of deadlocking the bubble.
func assertWaitResult(
	t *testing.T,
	results <-chan error,
	cancel context.CancelFunc,
	expected error,
) {
	t.Helper()
	select {
	case err := <-results:
		if expected == nil {
			assert.NoError(t, err)
		} else {
			assert.ErrorIs(t, err, expected)
		}
	default:
		t.Errorf("Wait did not return, want %v", expected)
		cancel()
		lateErr := <-results
		t.Logf("after cancel, Wait returned %v", lateErr)
	}
}

// assertCondWaitResult is assertWaitResult for a waiter in cond.Wait. If no result has arrived, it
// also calls cond.Broadcast after canceling, so a Wait that ignores its context still returns and
// the bubble does not deadlock.
func assertCondWaitResult(
	t *testing.T,
	cond *Cond,
	results <-chan error,
	cancel context.CancelFunc,
	expected error,
) {
	t.Helper()
	cancelAndBroadcast := func() {
		cancel()
		cond.Broadcast()
	}
	assertWaitResult(t, results, cancelAndBroadcast, expected)
}

// assertFutureWaitResult is assertWaitResult for a waiter in Future.Wait or Future.Value. If no
// result has arrived, it also resolves the promise after canceling, so a wait that ignores its
// context still returns and the bubble does not deadlock.
func assertFutureWaitResult(
	t *testing.T,
	resolver Resolver[int],
	results <-chan error,
	cancel context.CancelFunc,
	expected error,
) {
	t.Helper()
	cancelAndResolve := func() {
		cancel()
		resolver.Resolve(0, nil)
	}
	assertWaitResult(t, results, cancelAndResolve, expected)
}

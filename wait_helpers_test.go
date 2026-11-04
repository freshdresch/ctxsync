package ctxsync

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

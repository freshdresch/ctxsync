package ctxsync

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// waitTimeout bounds the waits in the blocking wait tests. Those tests run in a synctest bubble,
// so the deadline case advances the fake clock past it and takes no real time.
const waitTimeout = time.Minute

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

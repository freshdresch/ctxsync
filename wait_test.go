package ctxsync

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pointerContext lets a test build a non-nil context.Context interface that holds a nil
// pointer.
type pointerContext struct {
	context.Context
}

func TestCheckContextNil(t *testing.T) {
	var nilContext context.Context
	assert.PanicsWithValue(t, "ctxsync: nil Context", func() {
		checkContext(nilContext)
	})

	var nilPointer *pointerContext
	assert.NotPanics(t, func() {
		checkContext(nilPointer)
	}, "an interface holding a nil pointer is not a nil context")
}

func TestIsClosed(t *testing.T) {
	var nilChannel chan struct{}
	assert.False(t, isClosed(nilChannel), "nil channel")

	open := make(chan struct{})
	assert.False(t, isClosed(open), "open channel")

	closed := make(chan struct{})
	close(closed)
	assert.True(t, isClosed(closed), "closed channel")
}

func TestResultAfterWake(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	closed := make(chan struct{})
	close(closed)
	assert.NoError(t, resultAfterWake(ctx, closed), "ready closed")

	open := make(chan struct{})
	assert.ErrorIs(t, resultAfterWake(ctx, open), context.Canceled, "ready open")
}

// TestWaitClosedOutcomeWins proves the outcome-wins rule for every type that waits through
// waitClosed. With both channels ready, select picks a case at random, so without the recheck
// in resultAfterWake each iteration would fail about half the time. A test per type cannot reach
// this tie reliably, because a goroutine already parked in select takes whichever channel closes
// first.
func TestWaitClosedOutcomeWins(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	ready := make(chan struct{})
	close(ready)

	for i := range raceAwareIterations {
		require.NoError(t, waitClosed(ctx, ready), "iteration %d", i)
	}
}

package ctxsync

import "context"

const nilContextMessage = "ctxsync: nil Context"

// checkContext panics if ctx is nil. Every exported function that takes a context calls it first.
func checkContext(ctx context.Context) {
	if ctx == nil {
		panic(nilContextMessage)
	}
}

// isClosed reports whether ch is closed, without blocking. A nil channel reports false.
// It is only for channels that are never sent on, because it would consume a sent value.
func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// waitClosed blocks until ready closes or ctx is done, then returns resultAfterWake(ctx, ready).
func waitClosed(ctx context.Context, ready <-chan struct{}) error {
	select {
	case <-ready:
	case <-ctx.Done():
	}
	return resultAfterWake(ctx, ready)
}

// resultAfterWake returns nil if ready is closed and ctx.Err() otherwise. It implements the
// outcome-wins rule: a wait that woke because ctx is done still reports the outcome if ready
// closed too.
func resultAfterWake(ctx context.Context, ready <-chan struct{}) error {
	if isClosed(ready) {
		return nil
	}
	return ctx.Err()
}

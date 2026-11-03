// Package ctxsync provides synchronization primitives whose waits take a [context.Context]:
// Event, Gate, Latch, Cond, and Barrier.
//
// Every wait panics if its context is nil. It checks before anything else, so the panic does
// not depend on whether the wait would block. A caller that wants no cancellation passes
// [context.Background].
//
// A wait returns nil when it has observed its outcome by the time it decides, and ctx.Err() when
// the context has ended and it has not observed its outcome. A wait that reports cancellation
// returns ctx.Err(), not context.Cause(ctx).
//
// When the outcome and the cancellation are both ready, Event, Gate, Latch, and Barrier report
// the outcome. An event stays set, a gate opening has already happened, and a barrier trip has
// already released the other parties, so reporting the cancellation would report a failure that
// did not happen. A wait on an Event, Gate, or Latch whose state is already reached returns nil
// at once, even if its context is done. Cond reports the cancellation instead, because a signal
// can be handed to another waiter. A Cond waiter whose context is done when it decides returns
// ctx.Err() and passes any signal it received to the next waiter.
//
// No method starts a goroutine or registers a [context.AfterFunc]. A blocked wait selects on
// ctx.Done() directly.
//
// Every type holds an internal mutex, so a value must not be copied after first use. The
// copylocks check of go vet reports such copies.
//
// Every method of every type is safe for concurrent use, except where Cond requires its caller
// to hold Cond.L.
//
// # Testing with synctest
//
// Construct each primitive inside the [testing/synctest] bubble that uses it. Never share a
// primitive across a bubble boundary, whether between two bubbles or between a bubble and code
// outside every bubble. The primitives create channels as they run, so any use can tie a
// primitive to a bubble. Using a channel created inside a bubble from outside it ends the
// process. Blocking inside a bubble on a channel created outside it is not durable blocking, so
// synctest.Wait hangs and a leaked goroutine does not fail the bubble.
package ctxsync

// NewLatch returns a [Latch] whose count starts at count. It panics if count is negative, because
// a negative count is a bug in the caller.
func NewLatch(count int) *Latch {
	if count < 0 {
		panic("ctxsync: negative Latch count")
	}
	latch := &Latch{}
	latch.count.Store(int64(count))
	return latch
}

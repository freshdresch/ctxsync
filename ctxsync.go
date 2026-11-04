// Package ctxsync provides synchronization primitives whose waits take a [context.Context]:
// [Event], [Gate], [Latch], [Cond], and [Barrier], and a promise, which [NewPromise] creates as a
// [Future] to read and a [Resolver] to write.
//
// Every wait panics if its context is nil. It checks before anything else, so the panic does
// not depend on whether the wait would block. A caller that wants no cancellation passes
// [context.Background].
//
// A wait returns nil when it has observed its outcome by the time it decides, [ErrBrokenBarrier]
// when it waits on a broken Barrier, and ctx.Err() when the context has ended and it has not
// observed its outcome. [Future.Value] can also return the producer's error, because returning
// the producer's outcome is its purpose. When a cancellation breaks a Barrier, both rules can
// apply, and the doc of [Barrier.Await] says which of its parties returns ErrBrokenBarrier and
// which returns ctx.Err(). A wait that reports cancellation returns ctx.Err(), not
// context.Cause(ctx).
//
// When the outcome and the cancellation are both ready, Event, Gate, Latch, Barrier, and Future
// report the outcome. An event stays set, a gate opening has already happened, a barrier trip has
// already released the other parties, and a promise stays resolved, so reporting the cancellation
// would report a failure that did not happen. A wait on an Event, Gate, Latch, or Future whose
// state is already reached returns its outcome at once, even if its context is done. Cond reports
// the cancellation instead, because a signal can be handed to another waiter. A Cond waiter whose
// context is done when it decides returns ctx.Err() and passes any signal it received to the next
// waiter.
//
// No method starts a goroutine or registers a [context.AfterFunc]. A blocked wait selects on
// ctx.Done() directly.
//
// Every type except Future and Resolver holds an internal mutex, so a value must not be copied
// after first use. The copylocks check of go vet reports such copies. Future and Resolver are
// handles that hold only a pointer, so they may be copied freely.
//
// Every method of every type is safe for concurrent use, except where Cond requires its caller
// to hold Cond.L.
//
// # Testing with synctest
//
// Construct each primitive inside the [testing/synctest] bubble that uses it. Never share a
// primitive across a bubble boundary, whether between two bubbles or between a bubble and code
// outside every bubble. Event, Gate, Latch, Cond, and Barrier create channels as they run, so any
// use can tie one of them to a bubble. A promise is tied to the bubble that ran NewPromise, which
// creates its channel. Using a channel created inside a bubble from outside it ends the process,
// so resolving a promise from outside the bubble that created it ends the process. Blocking
// inside a bubble on a channel created outside it is not durable blocking, so synctest.Wait hangs
// and a leaked goroutine does not fail the bubble.
package ctxsync

import (
	"errors"
	"sync"
)

// ErrBrokenBarrier is returned by [Barrier.Await] when the barrier is broken. Await returns it
// unwrapped, so errors.Is and == both match it.
var ErrBrokenBarrier = errors.New("ctxsync: barrier broken")

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

// NewCond returns a [Cond] whose L is locker, as [sync.NewCond] does.
func NewCond(locker sync.Locker) *Cond {
	return &Cond{L: locker}
}

// NewBarrier returns a [Barrier] for parties parties. It panics if parties is less than 1,
// because a barrier with no parties is a bug in the caller.
func NewBarrier(parties int) *Barrier {
	if parties < 1 {
		panic("ctxsync: Barrier needs at least one party")
	}
	return &Barrier{parties: parties}
}

// NewPromise returns the two ends of one unresolved promise: a [Future] that reads it and a
// [Resolver] that resolves it. A promise holds a value and an error that the first
// [Resolver.Resolve] call stores, and any number of goroutines can wait for them.
//
// Every path through the producer must resolve the promise, including early returns on errors,
// panics, and [runtime.Goexit]. A promise that is never resolved blocks every wait whose context
// never ends. The recommended pattern puts the work in a function that returns (T, error),
// resolves once with its result, and defers an abandonment call in front of it:
//
//	go func() {
//		defer r.Resolve(0, errAbandoned)
//		r.Resolve(work(ctx))
//	}()
//
// Here errAbandoned is an error the caller defines, and the first argument of the deferred call
// is the zero value of T. The first Resolve wins, so the deferred call does nothing after the
// real one. On any path that skipped the real Resolve, such as a panic that a deferred recover in
// the producer goroutine stops, or a runtime.Goexit, waiters get errAbandoned instead of a false
// success. Every return inside work reaches the single real Resolve, so an early return added to
// work cannot skip it.
//
// Resolution is final, so the real Resolve must come after all work and cleanup that can fail.
// Put cleanup that can fail inside work, where its defers finish before work returns. Cleanup
// deferred in the producer goroutine, such as a defer w.Flush() next to the abandonment call,
// runs after Resolve, and if it fails, waiters are left with a success.
//
// A producer that cannot be split this way can keep the deferred abandonment call and call
// Resolve as the last action of each normal path. A path that forgets to resolve then reports
// errAbandoned instead of hanging, but it reports a coding mistake as an abandonment. The same
// limit on cleanup applies. [Resolver.Resolve] describes two patterns that look right and are
// not.
//
// A waiter's cancellation does not reach the producer. When every waiter gives up, the producer
// keeps working and its Resolve still succeeds. A producer that should stop needs its own
// context.
//
// NewPromise creates the promise's channel, so the [testing/synctest] bubble that runs
// NewPromise is the one the promise belongs to. Resolving a promise from outside the bubble that
// created it ends the process.
func NewPromise[T any]() (Future[T], Resolver[T]) {
	state := &promiseState[T]{done: make(chan struct{})}
	return Future[T]{state: state}, Resolver[T]{state: state}
}

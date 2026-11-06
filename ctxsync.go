// Package ctxsync provides synchronization primitives whose waits take a [context.Context]:
// [Event], [Gate], [Latch], [Cond], and [Barrier], and a promise, which [NewPromise] creates as a
// [Future] to read and a [Resolver] to write, and [Go] creates as a Future for the result of work
// it runs in a new goroutine.
//
// Every function that takes a context panics if the context is nil, including Go. It checks
// before anything else, so the panic does not depend on whether a wait would block. A caller that
// wants no cancellation passes [context.Background].
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
// Go starts one goroutine, which runs until its work returns or calls [runtime.Goexit]. No other
// function starts a goroutine or registers a [context.AfterFunc]. A blocked wait selects on
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
// use can tie one of them to a bubble. A promise is tied to the bubble that called NewPromise or
// Go, which creates its channel. Using a channel created inside a bubble from outside it ends the
// process. So from outside the bubble that called NewPromise or Go, a call to [Future.Wait],
// [Future.Value], [Future.IsResolved], [Future.Result], or [Resolver.Resolve], or any receive
// from the channel that [Future.Done] returns, ends the process. Blocking inside a bubble on a
// channel created outside it is not durable blocking, so synctest.Wait hangs and a leaked
// goroutine does not fail the bubble.
package ctxsync

import (
	"context"
	"errors"
	"sync"
)

// ErrBrokenBarrier is returned by [Barrier.Await] when the barrier is broken. Await returns it
// unwrapped, so errors.Is and == both match it.
var ErrBrokenBarrier = errors.New("ctxsync: barrier broken")

// ErrGoexit is the error a [Future] from [Go] holds when its work called [runtime.Goexit]. Go
// stores it unwrapped, so errors.Is and == both match it.
//
// The Future also holds ErrGoexit in two cases that come from the Go runtime. In both, recover
// returns nil, so Go cannot tell them from runtime.Goexit, and nothing is printed:
//
//   - A deferred call inside work calls runtime.Goexit while a panic unwinds. The runtime ends
//     the panic and discards it.
//   - Work calls panic(nil) in a program that sets GODEBUG=panicnil=1.
//
// Calling t.FailNow, or testify's require functions, inside work is misuse, because the testing
// package allows them only on the goroutine running the test. They end work through
// runtime.Goexit, so the Future usually holds ErrGoexit. Tests should use testify's assert
// functions inside work instead.
var ErrGoexit = errors.New("ctxsync: work called runtime.Goexit")

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
// For the common case, work that runs in a goroutine and hands its result to the code that waits
// for it, use [Go]. NewPromise is for producers that do not fit Go. Such a producer must call
// Resolve on every path where it returns, because a promise that is never resolved blocks every
// wait whose context never ends. A producer that may panic or call [runtime.Goexit] should use Go.
//
// A waiter's cancellation does not reach the producer. When every waiter gives up, the producer
// keeps working and its Resolve still succeeds. A producer that should stop needs its own
// context.
//
// NewPromise creates the promise's channel, so the promise belongs to the [testing/synctest]
// bubble that called NewPromise or Go. From outside that bubble, a call to [Future.Wait],
// [Future.Value], [Future.IsResolved], [Future.Result], or [Resolver.Resolve], or any receive from
// the channel that [Future.Done] returns, ends the process.
func NewPromise[T any]() (Future[T], Resolver[T]) {
	state := &promiseState[T]{done: make(chan struct{})}
	return Future[T]{state: state}, Resolver[T]{state: state}
}

// Go runs work in a new goroutine and returns a [Future] for its result. A panic in work ends the
// program, and Go does not resolve the Future before it does, so no wait observes the resolution
// before the runtime prints the panic.
//
// When work returns, Go resolves the Future with the value and error that work returned. When
// work calls [runtime.Goexit], Go resolves it with the zero value and [ErrGoexit]. The doc of
// ErrGoexit names two cases from the Go runtime where a panic in work prints nothing and the Future
// also holds ErrGoexit. The guarantee covers observing the resolution, not every way a wait can
// return. Work's own deferred calls run during the panic, so one that ends a context or signals a
// primitive that a waiter uses can release that waiter before the panic is printed, including a
// wait on this Future, which then returns ctx.Err().
//
// Go panics if ctx is nil, and otherwise if work is nil. Both panics happen in the caller's
// goroutine, and work does not run. Otherwise Go returns at once, without waiting for work to
// start or finish. It always starts work, even when ctx is already done, and passes ctx to work
// unchanged.
//
// The goroutine runs until work returns or calls runtime.Goexit, even after every waiter has
// given up. A wait that returns ctx.Err() does not stop it. To ask the work to stop, cancel ctx.
// That works only if work honors ctx, and canceling does not wait for the work to stop. A
// waiter's own context does not reach the work, unless the caller passes the same context to Go.
//
// To wait for the work to finish, wait on the Future. Once the Future is resolved, work has
// ended, and every deferred call inside work has run. The goroutine may still be finishing for a
// moment after that, but it touches nothing the caller owns. A strict count of goroutines right
// after a wait can still see it.
//
// Everything the caller did before it called Go happens before work starts. Everything work did
// before it returned or called runtime.Goexit, including its deferred calls, happens before any
// wait or query that observes the resolution: a [Future.Wait] that returns nil, a [Future.Value]
// that returns the stored outcome, a receive from the channel that [Future.Done] returns, an
// [Future.IsResolved] that returns true, and a [Future.Result] that reports ok. A wait that
// returns ctx.Err() gets no such edge.
//
// Go creates the promise in the caller's goroutine, so the Future and the goroutine that Go
// starts belong to the caller's [testing/synctest] bubble, as with NewPromise.
func Go[T any](ctx context.Context, work func(ctx context.Context) (T, error)) Future[T] {
	checkContext(ctx)
	if work == nil {
		panic("ctxsync: nil work")
	}
	future, resolver := NewPromise[T]()
	go runWork(ctx, work, resolver)
	return future
}

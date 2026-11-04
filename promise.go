package ctxsync

import (
	"context"
	"sync"
)

const (
	zeroFutureMessage   = "ctxsync: zero Future, use NewPromise"
	zeroResolverMessage = "ctxsync: zero Resolver, use NewPromise"
)

// promiseState is the state that the Future and the Resolver of one promise share.
type promiseState[T any] struct {
	mu sync.Mutex
	// value and err are written once, under mu, by the winning Resolve before it closes done, and
	// never change after.
	value T
	err   error
	// done is created by NewPromise, so Done never takes mu, and closed by the winning Resolve.
	// It is the only record that the promise is resolved, so a reader that sees it closed can
	// read value and err without mu, and every method agrees on whether the promise is resolved.
	done chan struct{}
}

// Future is the read end of a promise that [NewPromise] creates. A Future can wait for the
// promise, but it cannot resolve it.
//
// The zero Future is not usable, and every method on it panics. A zero Future can only come from
// a variable or field that was declared but never set, so the panic reports that bug at its
// first use.
//
// A Future is a handle that holds only a pointer to the promise. Unlike [Event], [Gate], [Latch],
// [Cond], and [Barrier], it may be copied, passed by value, and stored in maps, and every copy
// refers to the same promise. Two Futures are equal under == when they refer to the same promise.
//
// The value and error are returned by copy, as with any Go assignment. When T is a pointer,
// slice, map, or other reference type, every reader shares what it refers to, and the promise
// does not synchronize access to it beyond the happens-before edges the methods state.
//
// Every method is safe for concurrent use.
type Future[T any] struct {
	state *promiseState[T]
}

// Resolver is the write end of a promise that [NewPromise] creates. A Resolver can resolve the
// promise, but it cannot read it.
//
// The zero Resolver is not usable, and [Resolver.Resolve] on it panics. A zero Resolver can only
// come from a variable or field that was declared but never set, so the panic reports that bug at
// its first use.
//
// A Resolver is a handle that holds only a pointer to the promise. Unlike [Event], [Gate],
// [Latch], [Cond], and [Barrier], it may be copied, passed by value, and stored in maps, and every
// copy refers to the same promise. Two Resolvers are equal under == when they refer to the same
// promise.
//
// Resolve is safe for concurrent use.
type Resolver[T any] struct {
	state *promiseState[T]
}

// Resolve resolves the promise with value and err, wakes every waiter, and returns true. If the
// promise is already resolved, Resolve returns false and the stored value and error do not
// change, so the first call wins. Resolve panics if r is a zero Resolver.
//
// The promise stores value and err as given and does not interpret them. A nil err is a
// success, and a non-nil err is a failure that still carries value. Callers follow the usual Go
// convention of ignoring the value when the error is non-nil, but the promise does not enforce
// it.
//
// Every path through the producer must call Resolve, including early returns on errors, panics,
// and [runtime.Goexit], or every wait whose context never ends blocks forever. See [NewPromise]
// for the recommended pattern. Resolution is final, so call Resolve after all work and cleanup
// that can fail. Two patterns look right and are not:
//
//   - defer r.Resolve(v, err) evaluates v and err when the defer statement runs, so it resolves
//     with the values they held then, usually the zero value and nil, which reads as success.
//   - defer func() { r.Resolve(v, err) }() with named results v and err resolves with the real
//     outcome on a normal return. On a panic or runtime.Goexit, the named results keep whatever
//     they held, usually the zero value and nil, so waiters see success. Adding a deferred
//     abandonment call does not fix it, because the closure always calls Resolve. Depending on
//     the order of the two defers, either the closure wins with a false success on a panic, or
//     the abandonment call wins on every path.
//
// The Resolve call that returns true happens before any [Future.Wait] call that returns nil
// returns, before any receive from the channel that [Future.Done] returns completes, before any
// [Future.IsResolved] call that returns true returns, before any [Future.Result] call that
// reports ok returns, before any [Future.Value] call that returns the stored outcome returns,
// and before any Resolve call that returns false returns. Writes the producer made before
// Resolve, including writes to memory that value points to, are therefore visible to every
// reader that observed the resolution.
func (r Resolver[T]) Resolve(value T, err error) bool {
	if r.state == nil {
		panic(zeroResolverMessage)
	}
	state := r.state
	state.mu.Lock()
	defer state.mu.Unlock()
	// A losing call must observe the winner under mu, never through a fast path that skips mu,
	// so the winner's stores happen before any losing call returns.
	if isClosed(state.done) {
		return false
	}
	// Storing value and err before closing done means that a reader that sees done closed also
	// sees them.
	state.value = value
	state.err = err
	close(state.done)
	return true
}

// Done returns a channel that is closed once the promise is resolved. Every call returns the
// same channel. Done panics if f is a zero Future. The [Resolver.Resolve] call that resolved the
// promise happens before a receive from the channel completes.
func (f Future[T]) Done() <-chan struct{} {
	return f.mustState().done
}

// IsResolved reports whether the promise is resolved. It does not wait and does not allocate.
// IsResolved panics if f is a zero Future. If IsResolved returns true, the [Resolver.Resolve]
// call that resolved the promise happens before IsResolved returns.
func (f Future[T]) IsResolved() bool {
	return isClosed(f.mustState().done)
}

// Result returns the outcome of the promise without waiting. Before the promise is resolved, it
// returns the zero value, false, and a nil error. After, it returns the stored value, true, and
// the stored error. Result panics if f is a zero Future.
//
// Check ok. On an unresolved promise, v, _, err := f.Result() gives a zero value and a nil
// error, which looks like success. Discarding ok is safe once the caller has observed the
// resolution, for example right after [Future.Wait] returned nil.
//
// If Result reports ok, the [Resolver.Resolve] call that resolved the promise happens before
// Result returns.
func (f Future[T]) Result() (value T, ok bool, err error) {
	state := f.mustState()
	// Checking done before reading value and err orders those reads after the winning Resolve
	// stored them.
	if !isClosed(state.done) {
		var zero T
		return zero, false, nil
	}
	return state.value, true, state.err
}

// Wait blocks until the promise is resolved or ctx is done. It returns nil if the promise is
// resolved by the time Wait decides, even when ctx is done too, and ctx.Err() otherwise. If the
// promise is already resolved, Wait returns nil at once. Wait panics if ctx is nil. Otherwise it
// panics if f is a zero Future.
//
// Wait never returns the producer's error, so a non-nil result always means that ctx ended. A
// caller that must tell its own cancellation apart from a producer that failed with, for
// example, [context.Canceled] calls Wait and then [Future.Result].
//
// The end of ctx stops only this wait. It does not reach the producer, which keeps working, and
// its Resolve still succeeds. A producer that should stop needs its own context.
//
// When Wait returns nil, the [Resolver.Resolve] call that resolved the promise happens before
// Wait returns.
func (f Future[T]) Wait(ctx context.Context) error {
	checkContext(ctx)
	state := f.mustState()
	if isClosed(state.done) {
		return nil
	}
	return waitClosed(ctx, state.done)
}

// Value blocks until the promise is resolved or ctx is done, as [Future.Wait] does. If Wait
// returns an error, Value returns the zero value and that error. Otherwise it returns the stored
// value and error. Value panics if ctx is nil. Otherwise it panics if f is a zero Future.
//
// Unlike every other wait in this package, Value can also return the producer's error, because
// returning the producer's outcome is its purpose. A caller therefore cannot tell from Value
// alone whether a [context.Canceled] came from its own context or from the producer. A caller
// that needs the distinction calls Wait and then [Future.Result].
//
// The end of ctx stops only this wait. It does not reach the producer, which keeps working, and
// its Resolve still succeeds. A producer that should stop needs its own context.
//
// When Value returns the stored outcome, the [Resolver.Resolve] call that resolved the promise
// happens before Value returns.
func (f Future[T]) Value(ctx context.Context) (T, error) {
	if err := f.Wait(ctx); err != nil {
		var zero T
		return zero, err
	}
	// Wait returned nil, so the winning Resolve happens before this point, and value and err no
	// longer change.
	return f.state.value, f.state.err
}

// mustState returns the shared state of f, and panics if f is a zero Future.
func (f Future[T]) mustState() *promiseState[T] {
	if f.state == nil {
		panic(zeroFutureMessage)
	}
	return f.state
}

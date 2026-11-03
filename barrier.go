package ctxsync

import (
	"context"
	"fmt"
	"sync"
)

// generation is one round of arrivals. Its fields are guarded by Barrier.mu.
type generation struct {
	released chan struct{} // closed when the generation trips or breaks
	arrived  int
	tripped  bool
	broken   bool
}

// arrivalResult is the outcome of one step of Await. The zero value is not a real outcome.
type arrivalResult int

const (
	arrivalUnknown arrivalResult = iota
	arrivalBroken
	arrivalCanceled
	arrivalTripped
	arrivalWaiting
)

// Barrier is a reusable meeting point for a fixed number of parties. Each [Barrier.Await] call
// arrives at the barrier. When the Parties-th caller of a generation arrives, the generation
// trips and every party of it returns. The next arrival starts a new generation.
// Create a Barrier with [NewBarrier]. The zero value is not usable, and Await on it panics. A
// Barrier must not be copied after first use.
//
// A party whose context ends before its generation trips breaks the barrier, and a broken
// barrier stays broken. A caller that wants to continue after a break creates a new Barrier, and
// the parties need a way outside the barrier, such as a shared struct or a channel, to learn
// about the new one.
//
// The barrier does not track which goroutine is which. A caller beyond the Parties-th arrival of
// a generation starts the next generation.
type Barrier struct {
	// mu guards broken, current, and the fields of every generation.
	mu      sync.Mutex
	parties int // zero only in a zero Barrier, and never changed after construction
	broken  bool
	// current is the generation that is filling. It is nil until the first arrival of a
	// generation, and a trip or a break detaches it, so a waiter's later decisions read only the
	// generation it arrived in.
	current *generation
}

// Await arrives at the barrier and blocks until the last party of its generation arrives, the
// barrier breaks, or ctx is done. Await panics if ctx is nil. Otherwise it panics if b is a zero
// Barrier.
//
// Await returns nil when its generation trips. If the barrier is already broken, Await returns
// [ErrBrokenBarrier], even if ctx is done. If ctx is already done when Await is called, the
// caller does not arrive. It breaks the barrier and returns ctx.Err(). If ctx ends while Await
// waits, Await returns nil if its generation tripped anyway, ErrBrokenBarrier if another party
// broke the barrier first, and otherwise breaks the barrier and returns ctx.Err().
//
// Within the generation that broke, only the party whose cancellation broke the barrier returns
// ctx.Err(), and every other party of that generation returns ErrBrokenBarrier. When those
// parties share one context, as in an errgroup, the first of them to take the barrier's internal
// mutex after the cancellation returns ctx.Err(). Parties of an earlier generation that already
// tripped return nil, even if they return after the break. Later calls return ErrBrokenBarrier.
// Await returns ErrBrokenBarrier itself, not wrapped, so errors.Is and == both match it.
//
// With Parties equal to 1, every Await with a live ctx trips at once and returns nil, and an
// Await whose ctx is already done still breaks the barrier.
//
// Each party's arrival in a generation that trips happens before any Await of that generation
// returns nil. The break of the barrier happens before any Await returns ErrBrokenBarrier.
func (b *Barrier) Await(ctx context.Context) error {
	checkContext(ctx)
	if b.parties == 0 {
		panic("ctxsync: zero Barrier, use NewBarrier")
	}
	done := ctx.Done()
	gen, result := b.arrive(done)
	if result == arrivalWaiting {
		select {
		case <-gen.released:
			result = b.afterRelease(gen)
		case <-done:
			result = b.afterCancel(gen)
		}
	}
	switch result {
	case arrivalTripped:
		return nil
	case arrivalBroken:
		return ErrBrokenBarrier
	case arrivalCanceled:
		return ctx.Err()
	default:
		panic(fmt.Sprintf("ctxsync: Barrier.Await reached arrival result %d", result))
	}
}

// Parties returns the number of parties that [NewBarrier] was given. It returns 0 for a zero
// Barrier.
func (b *Barrier) Parties() int {
	return b.parties
}

// Waiting returns the number of parties that arrived in the current generation and are blocked.
// It returns 0 when no generation is filling, so it resets to 0 when a generation trips, and it
// returns 0 once the barrier is broken. A caller whose context was already done never arrives,
// so it is never counted. A party whose context ended while it waited stays counted until it
// takes the barrier's internal mutex to decide its result. The result is a snapshot, and another
// call can change the count before the caller reads it. It returns 0 for a zero Barrier.
func (b *Barrier) Waiting() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.broken || b.current == nil {
		return 0
	}
	return b.current.arrived
}

// IsBroken reports whether the barrier is broken. A broken barrier stays broken, so a true result
// stays true, and a false result is a snapshot. When IsBroken returns true, the break of the
// barrier happens before IsBroken returns. It returns false for a zero Barrier.
func (b *Barrier) IsBroken() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.broken
}

// arrive runs the steps of Await up to blocking. It returns the caller's generation and
// arrivalWaiting when the caller must wait for the generation to trip or break. Otherwise the
// result is final, and the generation is nil.
func (b *Barrier) arrive(done <-chan struct{}) (*generation, arrivalResult) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.broken {
		return nil, arrivalBroken
	}
	// A caller whose ctx is already done does not arrive, so it breaks the barrier even when it
	// would have been the last arrival.
	if isClosed(done) {
		b.breakLocked()
		return nil, arrivalCanceled
	}
	if b.current == nil {
		b.current = &generation{released: make(chan struct{})}
	}
	gen := b.current
	gen.arrived++
	if gen.arrived == b.parties {
		gen.tripped = true
		close(gen.released)
		b.current = nil
		return nil, arrivalTripped
	}
	return gen, arrivalWaiting
}

// afterRelease decides the result of a waiter whose generation's channel closed.
func (b *Barrier) afterRelease(gen *generation) arrivalResult {
	b.mu.Lock()
	defer b.mu.Unlock()
	if gen.tripped {
		return arrivalTripped
	}
	return arrivalBroken
}

// afterCancel decides the result of a waiter whose ctx ended while it waited. It reads only the
// waiter's own generation, never b.current, so a cancellation that lands after gen tripped cannot
// break the next generation.
func (b *Barrier) afterCancel(gen *generation) arrivalResult {
	b.mu.Lock()
	defer b.mu.Unlock()
	if gen.tripped {
		return arrivalTripped
	}
	if gen.broken {
		return arrivalBroken
	}
	b.breakLocked()
	return arrivalCanceled
}

// breakLocked marks the barrier broken and releases the waiters of the generation that is
// filling, if there is one. The caller must hold b.mu.
func (b *Barrier) breakLocked() {
	b.broken = true
	if b.current == nil {
		return
	}
	b.current.broken = true
	close(b.current.released)
	b.current = nil
}

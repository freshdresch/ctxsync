package ctxsync

import (
	"context"
	"sync"
	"sync/atomic"
)

// Event is a one-shot flag that stays set once [Event.Set] is called. The zero value is an unset
// Event that is ready to use. An Event must not be copied after first use.
type Event struct {
	mu sync.Mutex
	// set is stored only while holding mu, after done is closed. IsSet and Done read it under mu,
	// so they see all of a Set or none of it. The fast paths of Set and Wait read it without mu.
	set atomic.Bool
	// done is created lazily under mu. The first Set closes it, or Done creates it already closed
	// if Set ran first.
	done chan struct{}
}

// Set sets the event and wakes every waiter. Calls after the first do nothing.
//
// The first Set call happens before any [Event.Wait] call returns nil, before any receive from
// the channel that [Event.Done] returns completes, and before any [Event.IsSet] call that
// returns true returns.
func (e *Event) Set() {
	if e.set.Load() {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.set.Load() {
		return
	}
	// The fast paths of Set and Wait read set without mu, so set must be stored after done is
	// closed. Otherwise a second Set could return, or a Wait could return nil, while a waiter's
	// channel is still open.
	if e.done != nil {
		close(e.done)
	}
	e.set.Store(true)
}

// IsSet reports whether [Event.Set] has been called. It does not block on the state and does not
// allocate. IsSet holds the event's mutex, which the first Set holds until the event is set, so
// if IsSet returns true, the first Set call happens before IsSet returns.
func (e *Event) IsSet() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.set.Load()
}

// Done returns a channel that is closed once [Event.Set] has been called. Every call returns the
// same channel. If Set ran before the first Done call, Done returns a channel that is already
// closed. The first Set call happens before a receive from the channel completes.
func (e *Event) Done() <-chan struct{} {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.done == nil {
		e.done = make(chan struct{})
		if e.set.Load() {
			close(e.done)
		}
	}
	return e.done
}

// Wait blocks until the event is set or ctx is done. It returns nil if the event is set by the
// time Wait decides, even when ctx is done too, and ctx.Err() otherwise. If the event is already
// set, Wait returns nil at once. Wait panics if ctx is nil.
//
// When Wait returns nil, the first [Event.Set] call happens before Wait returns.
func (e *Event) Wait(ctx context.Context) error {
	checkContext(ctx)
	if e.set.Load() {
		return nil
	}
	return waitClosed(ctx, e.Done())
}

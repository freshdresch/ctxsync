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
	// set is read without mu but stored only while holding mu. Done relies on this to decide,
	// under mu, whether to create its channel already closed.
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
	// Closing done before storing set means that once IsSet returns true, any existing done
	// channel is already closed. A Wait that decides after that point then reports the event.
	if e.done != nil {
		close(e.done)
	}
	e.set.Store(true)
}

// IsSet reports whether [Event.Set] has been called. It does not wait and does not allocate.
// If IsSet returns true, the first Set call happens before IsSet returns.
func (e *Event) IsSet() bool {
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

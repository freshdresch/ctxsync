package ctxsync

import (
	"context"
	"sync"
	"sync/atomic"
)

// Gate is a flag that can open and shut any number of times. The zero value is a shut Gate that
// is ready to use. A Gate must not be copied after first use.
type Gate struct {
	mu sync.Mutex
	// open is stored only while holding mu, and stored as true after closing opened, if it exists.
	// IsOpen and Opened read it under mu, so they see all of an Open or none of it. The fast path
	// of Wait reads it without mu.
	open atomic.Bool
	// opened is created lazily under mu, and each opening has its own channel. While the gate is
	// shut, opened is nil or an unclosed channel for the next opening. While the gate is open,
	// opened is nil or the closed channel of the current opening.
	opened chan struct{}
}

// Open opens the gate and wakes every waiter. On an open gate, Open does nothing.
//
// The Open call that starts an opening happens before any [Gate.Wait] call that returns nil
// because of that opening returns, before any receive from that opening's channel completes, and
// before any [Gate.IsOpen] call that observes that opening returns true.
func (g *Gate) Open() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.open.Load() {
		return
	}
	// The fast path of Wait reads open without mu, so open must be stored after opened is closed.
	// Otherwise a Wait could return nil while a waiter's channel for this opening is still open.
	if g.opened != nil {
		close(g.opened)
	}
	g.open.Store(true)
}

// Shut shuts the gate. On a shut gate, Shut does nothing. A channel that [Gate.Opened] returned
// before Shut stays closed, and the next Opened call returns a new channel.
func (g *Gate) Shut() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.open.Load() {
		return
	}
	g.open.Store(false)
	g.opened = nil
}

// IsOpen reports whether the gate is open. It does not block on the state and does not allocate.
// IsOpen holds the gate's mutex, which [Gate.Open] holds until the gate is open, so if IsOpen
// returns true, the Open call that started the opening it observed happens before IsOpen returns.
func (g *Gate) IsOpen() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.open.Load()
}

// Opened returns the channel of one opening. While the gate is shut, it returns the channel for
// the next opening, which [Gate.Open] closes. While the gate is open, it returns the channel of
// the current opening, which is already closed. Calls during one opening, or during one shut
// period, return the same channel. The Open call that started an opening happens before a receive
// from that opening's channel completes.
//
// A channel returned before a [Gate.Shut] stays closed after it, because a closed channel cannot
// reopen, so callers call Opened again after each wakeup. A loop that selects only on Opened()
// spins for as long as the gate stays open, because every call returns a closed channel.
func (g *Gate) Opened() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.opened == nil {
		g.opened = make(chan struct{})
		if g.open.Load() {
			close(g.opened)
		}
	}
	return g.opened
}

// Wait blocks until the gate opens or ctx is done. The moment that counts is when Wait obtains
// its channel from [Gate.Opened], under the gate's mutex. Wait returns nil if the gate is open at
// some moment between then and when Wait decides, even when ctx is done too and even if the gate
// shuts again before Wait decides. Otherwise it returns ctx.Err(). If Wait finds the gate already
// open before it obtains a channel, it returns nil at once. The gate may be shut again by the
// time the caller acts on a nil result. Wait panics if ctx is nil.
//
// When Wait returns nil, the [Gate.Open] call that started the opening it observed happens before
// Wait returns.
func (g *Gate) Wait(ctx context.Context) error {
	checkContext(ctx)
	if g.open.Load() {
		return nil
	}
	return waitClosed(ctx, g.Opened())
}

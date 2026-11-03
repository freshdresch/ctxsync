package ctxsync

import (
	"context"
	"sync"
	"sync/atomic"
)

// Latch is a countdown that opens once its count reaches zero and then stays open. [NewLatch]
// fixes the count when it creates the Latch. The zero value is an open Latch with count 0. A
// Latch must not be copied after first use.
type Latch struct {
	mu sync.Mutex
	// count is the remaining count. The latch is open at zero, so the zero Latch is open. After
	// construction, count is read without mu but changed only while holding mu. Released relies
	// on this to decide, under mu, whether to create its channel already closed.
	count atomic.Int64
	// released is created lazily under mu. The CountDown that brings the count to zero closes it,
	// or Released creates it already closed if the count is already zero.
	released chan struct{}
}

// CountDown decrements the count. When the count reaches zero, the latch opens and wakes every
// waiter. At count zero, CountDown does nothing.
//
// The CountDown calls that bring the count to zero happen before any [Latch.Wait] call returns
// nil, before any receive from the channel that [Latch.Released] returns completes, and before
// any [Latch.Count] call that returns 0 returns.
func (l *Latch) CountDown() {
	l.mu.Lock()
	defer l.mu.Unlock()
	switch l.count.Load() {
	case 0:
		return
	case 1:
		// Closing released before storing 0 means that once Count returns 0, any existing
		// released channel is already closed. A Wait that decides after that point then reports
		// the release.
		if l.released != nil {
			close(l.released)
		}
		l.count.Store(0)
	default:
		l.count.Add(-1)
	}
}

// Count returns the remaining count. It does not wait and does not allocate. If Count returns 0,
// the [Latch.CountDown] calls that brought the count to zero happen before Count returns.
func (l *Latch) Count() int {
	return int(l.count.Load())
}

// Released returns a channel that is closed once the count reaches zero. Every call returns the
// same channel. If the count is already zero at the first Released call, including on a zero
// Latch, Released returns a channel that is already closed. The [Latch.CountDown] calls that
// brought the count to zero happen before a receive from the channel completes.
func (l *Latch) Released() <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released == nil {
		l.released = make(chan struct{})
		if l.count.Load() == 0 {
			close(l.released)
		}
	}
	return l.released
}

// Wait blocks until the count reaches zero or ctx is done. It returns nil if the count is zero
// by the time Wait decides, even when ctx is done too, and ctx.Err() otherwise. If the count is
// already zero, Wait returns nil at once. Wait panics if ctx is nil.
//
// When Wait returns nil, the [Latch.CountDown] calls that brought the count to zero happen before
// Wait returns.
func (l *Latch) Wait(ctx context.Context) error {
	checkContext(ctx)
	if l.count.Load() == 0 {
		return nil
	}
	return waitClosed(ctx, l.Released())
}

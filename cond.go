package ctxsync

import (
	"context"
	"sync"
)

// wakeKind records how a queued waiter was woken.
type wakeKind int

const (
	notWoken wakeKind = iota
	wokenBySignal
	wokenByBroadcast
)

// waiter is one blocked Wait call. Its fields are guarded by Cond.mu.
type waiter struct {
	prev, next *waiter
	queued     bool
	// woken is set only while the record is queued, so nothing overwrites it once set.
	woken wakeKind
	ready chan struct{} // closed when the waiter is woken
}

// Cond is a condition variable bound to the [sync.Locker] L, with the shape of [sync.Cond]. Its
// Wait takes a context, and [Cond.Changed] returns a channel for select. A Cond is usable once L
// is set, by [NewCond] or by a struct literal. A Cond must not be copied after first use.
//
// Wait reads L once per call. Assigning L while the Cond is in use is a data race. If L is nil,
// Wait panics with a nil-pointer dereference, as with sync.Cond. Signal, Broadcast, and Changed
// do not use L.
//
// Waiters form a FIFO queue in the order in which they join it inside Wait. [Cond.Signal] wakes
// the waiter at the front, and a signal that a canceled waiter passes on goes to the new front.
// With an RLocker, several goroutines can call Wait at once, and the Cond's internal mutex
// decides the order in which they join.
//
// Signal is correct only when every waiter waits for the same condition. When waiters on one
// Cond wait for different conditions, such as "not empty" and "not full", a Signal can wake a
// waiter whose condition is still false while the waiter it was meant for stays blocked. In that
// case, use [Cond.Broadcast], or a separate Cond for each condition.
type Cond struct {
	// L is held while the caller observes or changes the condition.
	L sync.Locker

	// mu guards head, tail, changed, and the fields of every waiter. The Cond never calls L while
	// holding mu.
	mu      sync.Mutex
	head    *waiter // front of the FIFO queue
	tail    *waiter
	changed chan struct{} // created lazily, closed and cleared by Broadcast
}

// Wait atomically unlocks c.L and blocks until a [Cond.Signal] or [Cond.Broadcast] wakes it or ctx
// is done, then locks c.L again before it returns. The caller must hold c.L, and Wait always
// returns holding c.L. Wait panics if ctx is nil, and panics with a nil-pointer dereference if
// c.L is nil.
//
// Wait returns nil when it was woken and ctx was live when Wait decided its result, after it
// locked c.L again. Otherwise it returns ctx.Err(). A Wait whose ctx is done when it decides
// returns ctx.Err() even if it was woken, and passes a Signal it received on to the next waiter.
// If ctx is already done when Wait is called, Wait unlocks c.L, locks it again, and returns
// ctx.Err() without joining the queue, so it cannot receive a signal. Locking c.L again cannot be
// interrupted, so a canceled Wait can block for as long as another goroutine holds c.L.
//
// A nil result does not prove that the condition changed after Wait began, because a passed-on
// signal can wake a waiter that began waiting after the original Signal. Callers recheck their
// condition in a loop, and the loop must end when Wait returns an error. A loop that ignores the
// error keeps calling Wait after ctx ends, and each call returns at once, so the loop spins. A
// caller that leaves without acting on its condition after a nil result, for example because
// ctx ended after Wait returned, calls Signal first so another waiter receives the wakeup:
//
//	c.L.Lock()
//	defer c.L.Unlock()
//	for !ready() {
//		if err := c.Wait(ctx); err != nil {
//			return err
//		}
//	}
//	if err := ctx.Err(); err != nil {
//		c.Signal()
//		return err
//	}
//	consume()
//
// Calling Wait without holding c.L is a bug, and its effect depends on the locker. With a
// [sync.Mutex] that no goroutine holds, the runtime stops the process with "unlock of unlocked
// mutex". With a sync.Mutex that another goroutine holds, Wait silently releases that goroutine's
// lock.
//
// Wait calls c.L.Unlock without holding the Cond's internal mutex, so a locker whose Unlock calls
// Signal on the same Cond does not deadlock. Wait joins the queue before it unlocks c.L, so when
// the waiter is at the front of the queue, that Signal wakes it, and Wait returns at once. A loop
// around such a Wait can spin.
//
// If c.L.Lock or c.L.Unlock panics, or calls [runtime.Goexit], inside Wait, the waiter leaves the
// queue and passes on any Signal it received, but the state of c.L afterward is unspecified.
//
// When Wait returns nil, the Signal or Broadcast call that woke it happens before Wait returns.
// When a canceled waiter passed the signal on, the original Signal call happens before Wait
// returns.
func (c *Cond) Wait(ctx context.Context) error {
	checkContext(ctx)
	done := ctx.Done()
	locker := c.L
	if isClosed(done) {
		locker.Unlock()
		locker.Lock()
		return ctx.Err()
	}

	record := &waiter{ready: make(chan struct{})}
	c.enqueue(record)
	decided := false
	defer func() {
		if !decided {
			c.abandon(record)
		}
	}()

	locker.Unlock()
	select {
	case <-record.ready:
	case <-done:
	}
	locker.Lock()
	if c.decide(record, done, &decided) {
		return nil
	}
	return ctx.Err()
}

// Signal wakes the waiter at the front of the queue, if there is one. A Signal with no waiters
// does nothing and is not remembered. Signal does not close the channel that [Cond.Changed]
// returns. Signal does not require holding c.L, but callers should change the condition under
// c.L before calling it.
//
// A Signal call happens before the [Cond.Wait] call that it woke returns nil.
func (c *Cond) Signal() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wakeFrontLocked(wokenBySignal)
}

// Broadcast wakes every queued waiter and closes the channel that [Cond.Changed] returned, if
// one exists. The next Changed call returns a new channel. Broadcast does not require holding
// c.L, but callers should change the condition under c.L before calling it.
//
// A Broadcast call happens before every [Cond.Wait] call that it woke returns nil, and before a
// receive from the Changed channel that it closed completes.
func (c *Cond) Broadcast() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.wakeFrontLocked(wokenByBroadcast) {
	}
	if c.changed != nil {
		close(c.changed)
		c.changed = nil
	}
}

// Changed returns a channel that the next [Cond.Broadcast] closes. Calls between two Broadcasts
// return the same channel. It is for callers that must wait for the condition alongside other
// channels:
//
//	c.L.Lock()
//	for !ready() {
//		changed := c.Changed()
//		c.L.Unlock()
//		select {
//		case <-changed:
//		case <-other:
//			return
//		}
//		c.L.Lock()
//	}
//	c.L.Unlock()
//
// Reading Changed while holding c.L, after checking the condition, means that any change made
// under c.L and followed by Broadcast closes the channel. Only Broadcast closes it. [Cond.Signal]
// does not, because a select can abandon the channel without telling the Cond, and a signal
// handed to it would be lost.
//
// The Broadcast call that closes the channel happens before a receive from it completes.
func (c *Cond) Changed() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.changed == nil {
		c.changed = make(chan struct{})
	}
	return c.changed
}

// enqueue adds record to the back of the queue.
func (c *Cond) enqueue(record *waiter) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.enqueueLocked(record)
}

// decide sets Wait's result after Wait locked c.L again, and reports whether Wait returns nil. A
// record that is still queued was not woken, so ctx is done, and the record leaves the queue. A
// woken record whose ctx is done passes a Signal on.
//
// decide sets *decided instead of leaving the flag to its caller, because the flag and the
// decision's effects must change in one hold of c.mu. A panic can then never separate them, so
// abandon runs exactly when no decision took effect.
func (c *Cond) decide(record *waiter, done <-chan struct{}, decided *bool) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	*decided = true
	if record.queued {
		c.removeLocked(record)
		return false
	}
	if !isClosed(done) {
		return true
	}
	if record.woken == wokenBySignal {
		c.passOnLocked()
	}
	return false
}

// abandon cleans up after a Wait that left between joining the queue and deciding, through a
// panic or runtime.Goexit. It acts as decide does when ctx is done, so the abandoned record never
// consumes a signal. It never touches c.L.
func (c *Cond) abandon(record *waiter) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if record.queued {
		c.removeLocked(record)
		return
	}
	if record.woken == wokenBySignal {
		c.passOnLocked()
	}
}

// enqueueLocked adds record to the back of the queue. The caller must hold c.mu.
func (c *Cond) enqueueLocked(record *waiter) {
	record.prev = c.tail
	record.next = nil
	if c.tail == nil {
		c.head = record
	} else {
		c.tail.next = record
	}
	c.tail = record
	record.queued = true
}

// removeLocked removes record, which must be queued, from the queue. The caller must hold c.mu.
func (c *Cond) removeLocked(record *waiter) {
	if record.prev == nil {
		c.head = record.next
	} else {
		record.prev.next = record.next
	}
	if record.next == nil {
		c.tail = record.prev
	} else {
		record.next.prev = record.prev
	}
	record.prev = nil
	record.next = nil
	record.queued = false
}

// wakeFrontLocked marks the record at the front of the queue as woken by kind, removes it, and
// closes its channel. It reports whether the queue held a record. The caller must hold c.mu.
func (c *Cond) wakeFrontLocked(kind wakeKind) bool {
	front := c.head
	if front == nil {
		return false
	}
	front.woken = kind
	c.removeLocked(front)
	close(front.ready)
	return true
}

// passOnLocked hands a Signal that a canceled waiter received to the waiter now at the front of
// the queue. A pass-on that finds an empty queue is dropped, as a Signal with no waiters is. The
// caller must hold c.mu.
func (c *Cond) passOnLocked() {
	c.wakeFrontLocked(wokenBySignal)
}

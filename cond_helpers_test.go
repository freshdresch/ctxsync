package ctxsync

import (
	"context"
	"fmt"
	"slices"
	"sync"
)

// chanLock is a sync.Locker built on a channel of capacity 1. Lock sends and Unlock receives. A
// goroutine blocked in Lock is durably blocked in a synctest bubble, so synctest.Wait can wait for
// a waiter that is locking L again inside Cond.Wait. Create one with make(chanLock, 1).
type chanLock chan struct{}

func (cl chanLock) Lock() {
	cl <- struct{}{}
}

func (cl chanLock) Unlock() {
	<-cl
}

// TryLock takes the lock without blocking and reports whether it did.
func (cl chanLock) TryLock() bool {
	select {
	case cl <- struct{}{}:
		return true
	default:
		return false
	}
}

// recordingLocker is a chanLock that records each Lock and Unlock call, in order, as "lock" or
// "unlock".
type recordingLocker struct {
	lock chanLock

	mu       sync.Mutex
	recorded []string
}

func newRecordingLocker() *recordingLocker {
	return &recordingLocker{lock: make(chanLock, 1)}
}

func (rl *recordingLocker) Lock() {
	rl.lock.Lock()
	rl.record("lock")
}

func (rl *recordingLocker) Unlock() {
	rl.record("unlock")
	rl.lock.Unlock()
}

// TryLock takes the lock without blocking and reports whether it did. It records nothing.
func (rl *recordingLocker) TryLock() bool {
	return rl.lock.TryLock()
}

// events returns a copy of the recorded calls.
func (rl *recordingLocker) events() []string {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return slices.Clone(rl.recorded)
}

func (rl *recordingLocker) record(event string) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.recorded = append(rl.recorded, event)
}

// queueLen returns the number of records in c's queue.
func queueLen(c *Cond) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return queueLenLocked(c)
}

// queueLenLocked returns the number of records in c's queue. The caller must hold c.mu.
func queueLenLocked(c *Cond) int {
	count := 0
	for record := c.head; record != nil; record = record.next {
		count++
	}
	return count
}

// tryLocker is a sync.Locker that can also take the lock without blocking, as chanLock and
// sync.Mutex can.
type tryLocker interface {
	sync.Locker
	TryLock() bool
}

// tookUnheldLock reports whether locker, which the caller's Wait should have returned holding,
// was free, so Wait returned without holding it. When it reports true, the caller holds locker,
// because TryLock took it, and must release it. Releasing a lock the caller never held instead
// would block forever on a chanLock and stop the process on a sync.Mutex. A locker without
// TryLock is assumed held. The check cannot detect a Wait that returned without locker while
// another goroutine holds it, because TryLock then fails, so the caller unlocks that goroutine's
// lock.
func tookUnheldLock(locker sync.Locker) bool {
	trier, ok := locker.(tryLocker)
	return ok && trier.TryLock()
}

// errReturnedUnheld is the error a test reports in place of waitErr when Wait returned without
// holding L. It formats waitErr with %v, not %w, so a check for Wait's expected error cannot match
// it.
func errReturnedUnheld(waitErr error) error {
	return fmt.Errorf("Wait returned %v without holding L", waitErr)
}

// startWaiter starts a goroutine that locks c.L, calls c.Wait(ctx), sends the result on the
// returned channel, and then unlocks c.L. The channel has capacity 1, so the goroutine never
// blocks on the send. If Wait returned without holding c.L, the goroutine sends an error in place
// of Wait's result, as tookUnheldLock describes.
func startWaiter(ctx context.Context, c *Cond) <-chan error {
	results := make(chan error, 1)
	go func() {
		c.L.Lock()
		err := c.Wait(ctx)
		if tookUnheldLock(c.L) {
			c.L.Unlock()
			results <- errReturnedUnheld(err)
			return
		}
		results <- err
		c.L.Unlock()
	}()
	return results
}

// newRecord returns a waiter record with a fresh ready channel, for tests that build records
// themselves.
func newRecord() *waiter {
	return &waiter{ready: make(chan struct{})}
}

// frontRecord returns the record at the front of c's queue, or nil if the queue is empty.
func frontRecord(c *Cond) *waiter {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.head
}

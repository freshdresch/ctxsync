package ctxsync

import (
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
	count := 0
	for record := c.head; record != nil; record = record.next {
		count++
	}
	return count
}

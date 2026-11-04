package ctxsync

import (
	"context"
	"runtime"
	"sync"
	"testing"
)

// The benchmarks in this file report the allocations of blocking waits. They assert nothing,
// because allocations on blocking paths are reported, not promised.

// BenchmarkEventWaitBlocking measures a Wait that a helper goroutine ends by calling Set. The
// helper yields before Set, so the waiter is likely blocked, but blocking is not guaranteed.
// Creating each Event and its channel happens with the timer stopped, so the reported time and
// allocations cover the handoff to the helper and Wait.
func BenchmarkEventWaitBlocking(b *testing.B) {
	events := make(chan *Event)
	var setter sync.WaitGroup
	setter.Go(func() {
		for event := range events {
			runtime.Gosched()
			event.Set()
		}
	})
	defer func() {
		close(events)
		setter.Wait()
	}()

	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		event := &Event{}
		event.Done()
		b.StartTimer()
		events <- event
		if err := event.Wait(b.Context()); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFutureWaitBlocking measures a Wait that a helper goroutine ends by calling Resolve.
// The helper yields before Resolve, so the waiter is likely blocked, but blocking is not
// guaranteed. Creating each promise happens with the timer stopped, so the reported time and
// allocations cover the handoff to the helper and Wait.
func BenchmarkFutureWaitBlocking(b *testing.B) {
	resolvers := make(chan Resolver[int])
	var resolving sync.WaitGroup
	resolving.Go(func() {
		for resolver := range resolvers {
			runtime.Gosched()
			resolver.Resolve(1, nil)
		}
	})
	defer func() {
		close(resolvers)
		resolving.Wait()
	}()

	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		future, resolver := NewPromise[int]()
		b.StartTimer()
		resolvers <- resolver
		if err := future.Wait(b.Context()); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCondWaitBlocking measures a Wait that the benchmark ends by calling Signal once the
// waiter is queued.
func BenchmarkCondWaitBlocking(b *testing.B) {
	helperCtx, cancelHelper := context.WithCancel(b.Context())
	var mu sync.Mutex
	cond := NewCond(&mu)
	var waiter sync.WaitGroup
	waiter.Go(func() {
		mu.Lock()
		defer mu.Unlock()
		for {
			if err := cond.Wait(helperCtx); err != nil {
				return
			}
		}
	})
	defer func() {
		cancelHelper()
		waiter.Wait()
	}()

	b.ReportAllocs()
	for b.Loop() {
		for queueLen(cond) != 1 {
			runtime.Gosched()
		}
		cond.Signal()
	}
}

// BenchmarkBarrierAwaitBlocking measures generations of a two-party Barrier, whose other party
// is a helper goroutine.
func BenchmarkBarrierAwaitBlocking(b *testing.B) {
	helperCtx, cancelHelper := context.WithCancel(b.Context())
	barrier := NewBarrier(2)
	var helper sync.WaitGroup
	helper.Go(func() {
		for {
			if err := barrier.Await(helperCtx); err != nil {
				return
			}
		}
	})
	defer func() {
		cancelHelper()
		helper.Wait()
	}()

	b.ReportAllocs()
	for b.Loop() {
		if err := barrier.Await(b.Context()); err != nil {
			b.Fatal(err)
		}
	}
}

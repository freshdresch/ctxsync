package ctxsync_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/freshdresch/ctxsync"
)

func ExampleEvent() {
	var ready ctxsync.Event
	go ready.Set()

	if err := ready.Wait(context.Background()); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("ready")
	// Output: ready
}

func ExampleGate() {
	var gate ctxsync.Gate
	gate.Open()
	if err := gate.Wait(context.Background()); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("open")

	gate.Shut()
	fmt.Println("IsOpen after Shut:", gate.IsOpen())
	// Output:
	// open
	// IsOpen after Shut: false
}

func ExampleLatch() {
	latch := ctxsync.NewLatch(3)
	for range 3 {
		go latch.CountDown()
	}

	if err := latch.Wait(context.Background()); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("all done")
	// Output: all done
}

func ExampleCond() {
	var mu sync.Mutex
	cond := ctxsync.NewCond(&mu)
	var tokens []int

	// take removes the oldest token. It waits until one is available or ctx ends.
	take := func(ctx context.Context) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		for len(tokens) == 0 {
			if err := cond.Wait(ctx); err != nil {
				return 0, err
			}
		}
		if err := ctx.Err(); err != nil {
			// This caller leaves without taking a token, so it hands the wakeup to another
			// waiter.
			cond.Signal()
			return 0, err
		}
		token := tokens[0]
		tokens = tokens[1:]
		return token, nil
	}

	put := func(token int) {
		mu.Lock()
		defer mu.Unlock()
		tokens = append(tokens, token)
		cond.Signal()
	}

	var producer sync.WaitGroup
	producer.Go(func() {
		for token := 1; token <= 3; token++ {
			put(token)
		}
	})
	for range 3 {
		token, err := take(context.Background())
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println("took", token)
	}
	producer.Wait()

	// With no tokens left, a canceled context ends the wait loop with an error.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := take(ctx)
	fmt.Println(err)
	// Output:
	// took 1
	// took 2
	// took 3
	// context canceled
}

func ExampleCond_Changed() {
	var mu sync.Mutex
	cond := ctxsync.NewCond(&mu)
	ready := false

	// waitReady waits until ready is true or stop closes, and reports which happened first.
	waitReady := func(stop <-chan struct{}) string {
		cond.L.Lock()
		for !ready {
			changed := cond.Changed()
			cond.L.Unlock()
			select {
			case <-changed:
			case <-stop:
				return "stopped"
			}
			cond.L.Lock()
		}
		cond.L.Unlock()
		return "ready"
	}

	stopped := make(chan struct{})
	close(stopped)
	fmt.Println(waitReady(stopped))

	results := make(chan string, 1)
	go func() {
		results <- waitReady(make(chan struct{}))
	}()
	cond.L.Lock()
	ready = true
	cond.L.Unlock()
	cond.Broadcast()
	fmt.Println(<-results)
	// Output:
	// stopped
	// ready
}

func ExampleBarrier() {
	barrier := ctxsync.NewBarrier(2)
	var passed atomic.Int32
	var parties sync.WaitGroup
	for range barrier.Parties() {
		parties.Go(func() {
			if err := barrier.Await(context.Background()); err != nil {
				fmt.Println(err)
				return
			}
			passed.Add(1)
		})
	}
	parties.Wait()
	fmt.Println("passed:", passed.Load())
	// Output: passed: 2
}

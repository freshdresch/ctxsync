package ctxsync_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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

func ExampleNewPromise() {
	// work returns its result through its return values, so every path through it reaches the
	// single Resolve below. A producer that may panic or call runtime.Goexit should use Go
	// instead.
	work := func(ctx context.Context) (int, error) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		return 42, nil
	}

	ctx := context.Background()
	future, resolver := ctxsync.NewPromise[int]()
	go func() {
		resolver.Resolve(work(ctx))
	}()

	value, err := future.Value(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(value)
	// Output: 42
}

func ExampleGo() {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/users/42":
			_, _ = io.WriteString(w, "Ada")
		case "/users/42/orders":
			_, _ = io.WriteString(w, "3 orders")
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	// get returns the body of a GET request for url.
	get := func(ctx context.Context, client *http.Client, url string) ([]byte, error) {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		response, err := client.Do(request)
		if err != nil {
			return nil, err
		}
		// The body is only read, so an error from Close reports nothing the caller can act on.
		defer func() { _ = response.Body.Close() }()
		if response.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("get %q: status %d", url, response.StatusCode)
		}
		return io.ReadAll(response.Body)
	}

	showUser := func(ctx context.Context, client *http.Client, baseURL string) error {
		// An early return cancels ctx, which asks the request still running to stop.
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Both requests start now and run at the same time.
		profile := ctxsync.Go(ctx, func(ctx context.Context) ([]byte, error) {
			return get(ctx, client, baseURL+"/users/42")
		})
		orders := ctxsync.Go(ctx, func(ctx context.Context) ([]byte, error) {
			return get(ctx, client, baseURL+"/users/42/orders")
		})

		profileBody, err := profile.Value(ctx)
		if err != nil {
			return fmt.Errorf("profile: %w", err)
		}
		ordersBody, err := orders.Value(ctx)
		if err != nil {
			return fmt.Errorf("orders: %w", err)
		}
		fmt.Printf("profile: %s\norders: %s\n", profileBody, ordersBody)
		return nil
	}

	if err := showUser(context.Background(), server.Client(), server.URL); err != nil {
		fmt.Println(err)
	}
	// Output:
	// profile: Ada
	// orders: 3 orders
}

func ExampleFuture_Wait() {
	future, resolver := ctxsync.NewPromise[string]()

	// describe tells a canceled wait apart from a producer that failed, even when both report
	// context.Canceled.
	describe := func(ctx context.Context) string {
		if err := future.Wait(ctx); err != nil {
			return "wait ended: " + err.Error()
		}
		// Wait returned nil, so the promise is resolved and ok is always true here.
		value, _, err := future.Result()
		if err != nil {
			return "producer failed: " + err.Error()
		}
		return "value: " + value
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	fmt.Println(describe(canceled))

	resolver.Resolve("", context.Canceled)
	fmt.Println(describe(context.Background()))
	// Output:
	// wait ended: context canceled
	// producer failed: context canceled
}

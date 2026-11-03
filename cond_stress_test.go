package ctxsync

import (
	"context"
	"math/rand/v2"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
)

// The counts are small on purpose. A consumer that a Signal reaches takes every token available,
// so any later Signal rescues a consumer that a lost wakeup left queued. One token per producer
// makes the lost wakeup likely to be among the last, and canceling most consumers makes it likely
// that a Signal reaches a canceled one.
const (
	stressProducers         = 3
	stressTokensPerProducer = 1
	stressConsumers         = 6
	// stressCanceledConsumers is fewer than stressConsumers, so one consumer stays live and a
	// lost wakeup leaves it queued while tokens remain.
	stressCanceledConsumers = 5
	// stressBroadcastOneIn is the chance, as one in this many, that a producer in
	// TestCondStressMixedBroadcast calls Broadcast instead of Signal.
	stressBroadcastOneIn = 4
)

func TestCondStressNoLostWakeups(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for i := range raceAwareIterations {
			runTokenStress(t, i, (*Cond).Signal)
		}
	})
}

func TestCondStressMixedBroadcast(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for i := range raceAwareIterations {
			runTokenStress(t, i, func(cond *Cond) {
				if rand.IntN(stressBroadcastOneIn) == 0 {
					cond.Broadcast()
					return
				}
				cond.Signal()
			})
		}
	})
}

// runTokenStress runs one iteration of a lost-wakeup stress test. Producers add tokens, each
// followed by a call to wake. Consumers take tokens, and a canceler cancels some consumers at
// random points. Once the producers finish and every goroutine is blocked or done, no consumer may
// be queued in Wait while tokens remain. It must run in a synctest bubble.
func runTokenStress(t *testing.T, iteration int, wake func(*Cond)) {
	cond := NewCond(make(chanLock, 1))
	tokens := 0  // guarded by cond.L
	waiting := 0 // consumers inside cond.Wait, guarded by cond.L
	var producers, consumersAndCanceler sync.WaitGroup
	for range stressProducers {
		producers.Go(func() {
			for range stressTokensPerProducer {
				cond.L.Lock()
				tokens++
				cond.L.Unlock()
				wake(cond)
			}
		})
	}

	cancels := make([]context.CancelFunc, 0, stressConsumers)
	for range stressConsumers {
		ctx, cancel := context.WithCancel(t.Context())
		cancels = append(cancels, cancel)
		consumersAndCanceler.Go(func() {
			err := consumeTokens(ctx, cond, &tokens, &waiting)
			if err != nil {
				assert.ErrorIs(t, err, context.Canceled, "iteration %d", iteration)
			}
		})
	}
	consumersAndCanceler.Go(func() {
		for _, consumer := range rand.Perm(stressConsumers)[:stressCanceledConsumers] {
			for range rand.IntN(51) {
				runtime.Gosched()
			}
			cancels[consumer]()
		}
	})

	producers.Wait()
	synctest.Wait()
	cond.L.Lock()
	remaining := tokens
	// After synctest.Wait, every consumer still inside Wait is blocked there.
	blockedInWait := waiting
	cond.L.Unlock()
	if remaining > 0 {
		assert.Zero(
			t,
			blockedInWait,
			"iteration %d: %d tokens remain while consumers wait in Wait",
			iteration, remaining,
		)
	}

	for _, cancel := range cancels {
		cancel()
	}
	consumersAndCanceler.Wait()
}

// consumeTokens takes tokens from *tokens, which cond.L guards, until Wait returns an error or ctx
// ends after Wait returns nil. It returns Wait's error, or nil when it left after a nil result.
// It keeps *waiting, which cond.L also guards, counting the consumers inside Wait.
//
// It checks ctx.Err() right after each nil Wait on purpose. A consumer that leaves at that point
// without acting calls Signal, as the Cond docs advise.
func consumeTokens(ctx context.Context, cond *Cond, tokens, waiting *int) error {
	for {
		cond.L.Lock()
		for *tokens == 0 {
			*waiting++
			err := cond.Wait(ctx)
			*waiting--
			if err != nil {
				// No Signal here. A canceled Wait passes on a Signal it received, and calling
				// Signal after an error would hide a missing pass-on from this test.
				cond.L.Unlock()
				return err
			}
			if ctx.Err() != nil {
				// Wait returned nil, so this consumer took a wakeup it will not act on.
				cond.Signal()
				cond.L.Unlock()
				return nil
			}
		}
		*tokens--
		cond.L.Unlock()
	}
}

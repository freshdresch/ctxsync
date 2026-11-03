package ctxsync

import (
	"context"
	"errors"
	"math/rand/v2"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	barrierStressParties     = 4
	barrierStressGenerations = 10
)

// TestBarrierHappensBefore hands plain ints between goroutines through the barrier, so the race
// detector reports a read if the barrier gives no happens-before edge. Each subtest closes a
// started channel in the goroutine that reads, before it writes anything or calls Await, and the
// writing goroutines wait for started and yield before they write. The started channel orders
// the reader before the writers, the opposite of the edge under test, so it cannot hide a missing
// edge through the barrier, and the reader usually blocks before the writers act.
//
// The test runs outside synctest, so every wait gives up after boundedWaitTimeout and reports a
// timeout as a failure instead of blocking the test forever.
func TestBarrierHappensBefore(t *testing.T) {
	t.Run("Trip", func(t *testing.T) {
		for i := range raceAwareIterations {
			runTripHandoff(t, i)
			if t.Failed() {
				return
			}
		}
	})
	t.Run("Broken", func(t *testing.T) {
		for i := range raceAwareIterations {
			runBrokenHandoff(t, i)
			if t.Failed() {
				return
			}
		}
	})
	t.Run("IsBroken", func(t *testing.T) {
		for i := range raceAwareIterations {
			runIsBrokenHandoff(t, i)
			if t.Failed() {
				return
			}
		}
	})
}

// runTripHandoff runs one iteration of the Trip subtest. Every party writes its own slot before
// Await, and after a nil result reads every slot. Party 0 closes started before it writes its
// slot, so only the barrier orders that write before the other parties' reads.
func runTripHandoff(t *testing.T, iteration int) {
	const parties = 4
	ctx, cancel := context.WithTimeout(t.Context(), boundedWaitTimeout)
	defer cancel()
	barrier := NewBarrier(parties)
	written := make([]int, parties)
	for slot := range parties {
		written[slot] = slot + 1
	}
	slots := make([]int, parties)
	observed := make([][]int, parties)
	awaitErrs := make([]error, parties)
	started := make(chan struct{})
	var goroutines sync.WaitGroup
	for party := range parties {
		goroutines.Go(func() {
			if party == 0 {
				close(started)
			} else {
				<-started
				runtime.Gosched()
			}
			slots[party] = written[party]
			awaitErrs[party] = barrier.Await(ctx)
			if awaitErrs[party] != nil {
				return
			}
			observed[party] = slices.Clone(slots)
		})
	}
	goroutines.Wait()

	for party := range parties {
		if awaitErrs[party] != nil {
			t.Errorf("iteration %d: party %d: Await: %v", iteration, party, awaitErrs[party])
			continue
		}
		assert.Equal(t, written, observed[party], "iteration %d: party %d", iteration, party)
	}
}

// runBrokenHandoff runs one iteration of the Broken subtest. The breaker writes the payload,
// cancels its context, and breaks the barrier. The waiter reads the payload after Await returns
// ErrBrokenBarrier.
func runBrokenHandoff(t *testing.T, iteration int) {
	const written = 42
	waiterCtx, cancelWaiter := context.WithTimeout(t.Context(), boundedWaitTimeout)
	defer cancelWaiter()
	barrier := NewBarrier(2)
	var payload int
	var observed int
	var waitErr, breakErr error
	started := make(chan struct{})
	var goroutines sync.WaitGroup
	goroutines.Go(func() {
		close(started)
		waitErr = barrier.Await(waiterCtx)
		if !errors.Is(waitErr, ErrBrokenBarrier) {
			return
		}
		observed = payload
	})
	goroutines.Go(func() {
		<-started
		runtime.Gosched()
		payload = written
		breakErr = awaitCanceled(t.Context(), barrier)
	})
	goroutines.Wait()

	assert.ErrorIs(t, breakErr, context.Canceled, "iteration %d: breaker", iteration)
	// The waiter read the payload only if it got ErrBrokenBarrier.
	if !assert.Same(t, ErrBrokenBarrier, waitErr, "iteration %d: waiter", iteration) {
		return
	}
	assert.Equal(t, written, observed, "iteration %d", iteration)
}

// runIsBrokenHandoff runs one iteration of the IsBroken subtest. The breaker writes the payload
// and breaks the barrier. The reader polls IsBroken until it is true and then reads the payload.
func runIsBrokenHandoff(t *testing.T, iteration int) {
	const written = 42
	barrier := NewBarrier(2)
	var payload int
	var observed int
	var sawBroken bool
	var breakErr error
	started := make(chan struct{})
	var goroutines sync.WaitGroup
	goroutines.Go(func() {
		close(started)
		sawBroken = pollUntil(barrier.IsBroken)
		if !sawBroken {
			return
		}
		observed = payload
	})
	goroutines.Go(func() {
		<-started
		runtime.Gosched()
		payload = written
		breakErr = awaitCanceled(t.Context(), barrier)
	})
	goroutines.Wait()

	assert.ErrorIs(t, breakErr, context.Canceled, "iteration %d: breaker", iteration)
	// The reader read the payload only if it saw the barrier broken.
	if !assert.True(t, sawBroken, "iteration %d: IsBroken still false at the timeout", iteration) {
		return
	}
	assert.Equal(t, written, observed, "iteration %d", iteration)
}

// awaitCanceled calls Await on barrier with a context derived from ctx and already canceled, so
// the call breaks the barrier unless it is already broken.
func awaitCanceled(ctx context.Context, barrier *Barrier) error {
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	return barrier.Await(canceledCtx)
}

func TestBarrierStressTripOnly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for i := range raceAwareIterations {
			runTripOnlyStress(t, i)
		}
	})
}

// runTripOnlyStress runs one iteration of TestBarrierStressTripOnly. Without cancellation, each
// party's k-th Await is in generation k, so a party counts its arrival in generation k before
// its k-th Await, and every party must see all of them counted when Await returns. A barrier that
// releases a generation early fails this check. It must run in a synctest bubble.
func runTripOnlyStress(t *testing.T, iteration int) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	barrier := NewBarrier(barrierStressParties)
	var arrivals [barrierStressGenerations]atomic.Int64
	var parties sync.WaitGroup
	for party := range barrierStressParties {
		parties.Go(func() {
			for generationIndex := range barrierStressGenerations {
				arrivals[generationIndex].Add(1)
				err := barrier.Await(ctx)
				if err != nil {
					t.Errorf(
						"iteration %d: party %d: generation %d: Await: %v",
						iteration, party, generationIndex, err,
					)
					return
				}
				assert.Equal(
					t,
					int64(barrierStressParties),
					arrivals[generationIndex].Load(),
					"iteration %d: party %d: generation %d released early",
					iteration, party, generationIndex,
				)
			}
		})
	}

	// On a correct barrier every party has returned by now. A party still blocked fails the
	// test through its Await error once ctx is canceled, instead of deadlocking the bubble.
	synctest.Wait()
	cancel()
	parties.Wait()
	assert.Equal(t, 0, barrier.Waiting(), "iteration %d", iteration)
	assert.False(t, barrier.IsBroken(), "iteration %d", iteration)
}

func TestBarrierStressCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for i := range raceAwareIterations {
			runCancelStress(t, i)
		}
	})
}

// runCancelStress runs one iteration of TestBarrierStressCancel. Each party calls Await in a loop
// and stops at its first error. One party, chosen at random, cancels its own context just before
// its Await for a generation chosen at random, so the cancellation always lands in that
// generation. It must run in a synctest bubble.
func runCancelStress(t *testing.T, iteration int) {
	breaker := rand.IntN(barrierStressParties)
	brokenGeneration := rand.IntN(barrierStressGenerations)
	barrier := NewBarrier(barrierStressParties)
	// results[party][k] is the result of that party's Await in generation k.
	results := make([][]error, barrierStressParties)
	var returned [barrierStressParties]atomic.Bool
	cancels := make([]context.CancelFunc, 0, barrierStressParties)
	var parties sync.WaitGroup
	for party := range barrierStressParties {
		ctx, cancel := context.WithCancel(t.Context())
		cancels = append(cancels, cancel)
		parties.Go(func() {
			defer returned[party].Store(true)
			for generationIndex := range barrierStressGenerations {
				if party == breaker && generationIndex == brokenGeneration {
					cancel()
				}
				err := barrier.Await(ctx)
				results[party] = append(results[party], err)
				if err != nil {
					return
				}
			}
		})
	}

	// On a correct barrier the break has released every party by now. A party still blocked
	// fails the check below, and canceling every context then releases it so the join returns.
	synctest.Wait()
	for party := range barrierStressParties {
		assert.True(
			t,
			returned[party].Load(),
			"iteration %d: party %d still blocked after generation %d broke",
			iteration, party, brokenGeneration,
		)
	}
	for _, cancel := range cancels {
		cancel()
	}
	parties.Wait()
	checkCancelStressResults(t, iteration, results, breaker, brokenGeneration)
}

// checkCancelStressResults checks that exactly one result is a context error, from the breaker
// in the broken generation, that every other party returned ErrBrokenBarrier in that generation
// and made no later call, and that every earlier call returned nil.
func checkCancelStressResults(
	t *testing.T,
	iteration int,
	results [][]error,
	breaker int,
	brokenGeneration int,
) {
	t.Helper()
	contextErrors := 0
	for party, partyResults := range results {
		for generationIndex, err := range partyResults {
			if errors.Is(err, context.Canceled) {
				contextErrors++
			}
			switch {
			case generationIndex < brokenGeneration:
				assert.NoError(
					t,
					err,
					"iteration %d: party %d: generation %d",
					iteration, party, generationIndex,
				)
			case generationIndex > brokenGeneration:
				t.Errorf(
					"iteration %d: party %d called Await in generation %d after generation %d broke",
					iteration, party, generationIndex, brokenGeneration,
				)
			case party == breaker:
				assert.ErrorIs(
					t,
					err,
					context.Canceled,
					"iteration %d: breaker %d: generation %d",
					iteration, party, generationIndex,
				)
			default:
				assert.Same(
					t,
					ErrBrokenBarrier,
					err,
					"iteration %d: party %d: generation %d",
					iteration, party, generationIndex,
				)
			}
		}
		assert.Len(
			t,
			partyResults,
			brokenGeneration+1,
			"iteration %d: party %d: results %v",
			iteration, party, partyResults,
		)
	}
	assert.Equal(t, 1, contextErrors, "iteration %d: results %v", iteration, results)
}

func TestBarrierAllocations(t *testing.T) {
	barrier := NewBarrier(2)
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		_ = barrier.IsBroken()
	}), "IsBroken")
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		_ = barrier.Waiting()
	}), "Waiting with no generation filling")

	live := make(chan struct{})
	_, result := barrier.arrive(live)
	require.Equal(t, arrivalWaiting, result, "first arrival")
	assert.Zero(t, testing.AllocsPerRun(100, func() {
		_ = barrier.Waiting()
	}), "Waiting with a generation filling")
}

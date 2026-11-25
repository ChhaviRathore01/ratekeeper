// Package storetest provides a conformance test suite for ratekeeper.Store
// implementations. Every store must pass it, which guarantees that limits
// behave identically regardless of backend or underlying algorithm.
//
// Usage in a store's tests:
//
//	func TestConformance(t *testing.T) {
//		storetest.Run(t, func(t *testing.T) ratekeeper.Store { return New(...) })
//	}
package storetest

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ChhaviRathore01/ratekeeper"
)

// T0 is the fixed start time used by the suite. Stores under test must use
// the `now` argument passed to Take rather than their own clock.
var T0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// tolerance absorbs backends that work at microsecond resolution.
const tolerance = 2 * time.Microsecond

// NewStore returns a fresh, empty store for a single subtest.
type NewStore func(t *testing.T) ratekeeper.Store

// Run executes the full conformance suite against stores built by newStore.
func Run(t *testing.T, newStore NewStore) {
	tests := []struct {
		name string
		fn   func(t *testing.T, s ratekeeper.Store)
	}{
		{"ConsumesUntilEmpty", testConsumesUntilEmpty},
		{"RetryAfterIsSufficient", testRetryAfterIsSufficient},
		{"RefillOverTime", testRefillOverTime},
		{"RefillCapsAtCapacity", testRefillCapsAtCapacity},
		{"Burst", testBurst},
		{"WeightedRequests", testWeightedRequests},
		{"DeniedRequestConsumesNothing", testDeniedRequestConsumesNothing},
		{"KeysAreIndependent", testKeysAreIndependent},
		{"ClockGoingBackwards", testClockGoingBackwards},
		{"ConcurrentNeverOverAdmits", testConcurrentNeverOverAdmits},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { tt.fn(t, newStore(t)) })
	}
}

func take(t *testing.T, s ratekeeper.Store, key string, l ratekeeper.Limit, n int, now time.Time) ratekeeper.Result {
	t.Helper()
	res, err := s.Take(context.Background(), key, l, n, now)
	if err != nil {
		t.Fatalf("Take(%q, n=%d): %v", key, n, err)
	}
	return res
}

func approx(t *testing.T, what string, got, want time.Duration) {
	t.Helper()
	if d := got - want; d < 0 || d > tolerance {
		t.Fatalf("%s = %s, want %s (+%s)", what, got, want, tolerance)
	}
}

func testConsumesUntilEmpty(t *testing.T, s ratekeeper.Store) {
	limit := ratekeeper.PerSecond(3)
	for i := 0; i < 3; i++ {
		res := take(t, s, "k", limit, 1, T0)
		if !res.Allowed {
			t.Fatalf("request %d: expected allowed", i)
		}
		if want := 2 - i; res.Remaining != want {
			t.Fatalf("request %d: Remaining = %d, want %d", i, res.Remaining, want)
		}
		if res.Limit != 3 {
			t.Fatalf("Limit = %d, want 3", res.Limit)
		}
		if res.RetryAfter != 0 {
			t.Fatalf("allowed request has RetryAfter %s", res.RetryAfter)
		}
	}

	res := take(t, s, "k", limit, 1, T0)
	if res.Allowed {
		t.Fatal("expected 4th request to be denied")
	}
	if res.Remaining != 0 {
		t.Fatalf("Remaining = %d, want 0", res.Remaining)
	}
	approx(t, "RetryAfter", res.RetryAfter, time.Second/3)
	approx(t, "ResetAt-now", res.ResetAt.Sub(T0), time.Second)
}

func testRetryAfterIsSufficient(t *testing.T, s ratekeeper.Store) {
	limit := ratekeeper.PerMinute(7) // interval does not divide evenly
	for i := 0; i < 7; i++ {
		take(t, s, "k", limit, 1, T0)
	}
	res := take(t, s, "k", limit, 1, T0)
	if res.Allowed {
		t.Fatal("expected denial")
	}
	if res = take(t, s, "k", limit, 1, T0.Add(res.RetryAfter)); !res.Allowed {
		t.Fatal("waiting RetryAfter should be enough to succeed")
	}
}

func testRefillOverTime(t *testing.T, s ratekeeper.Store) {
	limit := ratekeeper.PerSecond(10)
	for i := 0; i < 10; i++ {
		take(t, s, "k", limit, 1, T0)
	}
	if take(t, s, "k", limit, 1, T0).Allowed {
		t.Fatal("expected empty bucket")
	}

	now := T0.Add(500 * time.Millisecond) // 5 tokens back
	for i := 0; i < 5; i++ {
		if !take(t, s, "k", limit, 1, now).Allowed {
			t.Fatalf("refilled request %d denied", i)
		}
	}
	if take(t, s, "k", limit, 1, now).Allowed {
		t.Fatal("expected only 5 tokens after 500ms")
	}
}

func testRefillCapsAtCapacity(t *testing.T, s ratekeeper.Store) {
	limit := ratekeeper.PerSecond(2)
	take(t, s, "k", limit, 1, T0)
	res := take(t, s, "k", limit, 1, T0.Add(time.Hour))
	if res.Remaining != 1 {
		t.Fatalf("Remaining = %d, want 1 (capacity 2 minus 1)", res.Remaining)
	}
}

func testBurst(t *testing.T, s ratekeeper.Store) {
	limit := ratekeeper.PerSecond(1).WithBurst(5)
	res := take(t, s, "k", limit, 5, T0)
	if !res.Allowed || res.Remaining != 0 || res.Limit != 5 {
		t.Fatalf("unexpected result %+v", res)
	}
	// Burst drained; refills at 1/s, not 5/s.
	if take(t, s, "k", limit, 2, T0.Add(time.Second)).Allowed {
		t.Fatal("expected only 1 token after 1s")
	}
}

func testWeightedRequests(t *testing.T, s ratekeeper.Store) {
	limit := ratekeeper.PerSecond(10)
	res := take(t, s, "k", limit, 4, T0)
	if !res.Allowed || res.Remaining != 6 {
		t.Fatalf("after n=4: %+v", res)
	}
	res = take(t, s, "k", limit, 7, T0)
	if res.Allowed {
		t.Fatal("n=7 with 6 remaining should be denied")
	}
	approx(t, "RetryAfter", res.RetryAfter, 100*time.Millisecond)
}

func testDeniedRequestConsumesNothing(t *testing.T, s ratekeeper.Store) {
	limit := ratekeeper.PerSecond(5)
	take(t, s, "k", limit, 3, T0)
	if take(t, s, "k", limit, 3, T0).Allowed {
		t.Fatal("expected denial for 3 with 2 remaining")
	}
	if !take(t, s, "k", limit, 2, T0).Allowed {
		t.Fatal("denied request should not have consumed tokens")
	}
}

func testKeysAreIndependent(t *testing.T, s ratekeeper.Store) {
	limit := ratekeeper.PerSecond(1)
	take(t, s, "a", limit, 1, T0)
	if !take(t, s, "b", limit, 1, T0).Allowed {
		t.Fatal("key b should have its own bucket")
	}
}

func testClockGoingBackwards(t *testing.T, s ratekeeper.Store) {
	limit := ratekeeper.PerSecond(1)
	take(t, s, "k", limit, 1, T0)
	if take(t, s, "k", limit, 1, T0.Add(-time.Hour)).Allowed {
		t.Fatal("clock skew must not refill the bucket")
	}
}

func testConcurrentNeverOverAdmits(t *testing.T, s ratekeeper.Store) {
	limit := ratekeeper.PerHour(100)
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				res, err := s.Take(context.Background(), "k", limit, 1, T0)
				if err != nil {
					t.Error(err)
					return
				}
				if res.Allowed {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if got := allowed.Load(); got != 100 {
		t.Fatalf("allowed = %d, want exactly 100", got)
	}
}

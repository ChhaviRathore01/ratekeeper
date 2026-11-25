package memory

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ChhaviRathore01/ratekeeper"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func newStore(t *testing.T) *Store {
	t.Helper()
	s := New(WithCleanupInterval(0))
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestTakeConsumesUntilEmpty(t *testing.T) {
	s := newStore(t)
	limit := ratekeeper.PerSecond(3)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		res, err := s.Take(ctx, "k", limit, 1, t0)
		if err != nil {
			t.Fatal(err)
		}
		if !res.Allowed {
			t.Fatalf("request %d: expected allowed", i)
		}
		if want := 2 - i; res.Remaining != want {
			t.Fatalf("request %d: remaining = %d, want %d", i, res.Remaining, want)
		}
	}

	res, _ := s.Take(ctx, "k", limit, 1, t0)
	if res.Allowed {
		t.Fatal("expected 4th request to be denied")
	}
	// One token takes 1s/3 to refill.
	if want := time.Second / 3; res.RetryAfter < want || res.RetryAfter > want+time.Microsecond {
		t.Fatalf("RetryAfter = %s, want ~%s", res.RetryAfter, want)
	}
	if d := res.ResetAt.Sub(t0.Add(time.Second)); d < 0 || d > time.Microsecond {
		t.Fatalf("ResetAt = %s, want ~%s", res.ResetAt, t0.Add(time.Second))
	}
}

func TestRetryAfterIsSufficient(t *testing.T) {
	s := newStore(t)
	limit := ratekeeper.PerMinute(7) // interval does not divide evenly
	ctx := context.Background()

	for i := 0; i < 7; i++ {
		_, _ = s.Take(ctx, "k", limit, 1, t0)
	}
	res, _ := s.Take(ctx, "k", limit, 1, t0)
	if res.Allowed {
		t.Fatal("expected denial")
	}
	res, _ = s.Take(ctx, "k", limit, 1, t0.Add(res.RetryAfter))
	if !res.Allowed {
		t.Fatal("waiting RetryAfter should be enough to succeed")
	}
}

func TestRefillOverTime(t *testing.T) {
	s := newStore(t)
	limit := ratekeeper.PerSecond(10)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		_, _ = s.Take(ctx, "k", limit, 1, t0)
	}
	if res, _ := s.Take(ctx, "k", limit, 1, t0); res.Allowed {
		t.Fatal("expected empty bucket")
	}

	// After 500ms, 5 tokens are back.
	now := t0.Add(500 * time.Millisecond)
	for i := 0; i < 5; i++ {
		if res, _ := s.Take(ctx, "k", limit, 1, now); !res.Allowed {
			t.Fatalf("refilled request %d denied", i)
		}
	}
	if res, _ := s.Take(ctx, "k", limit, 1, now); res.Allowed {
		t.Fatal("expected only 5 tokens after 500ms")
	}
}

func TestRefillCapsAtCapacity(t *testing.T) {
	s := newStore(t)
	limit := ratekeeper.PerSecond(2)
	ctx := context.Background()

	_, _ = s.Take(ctx, "k", limit, 1, t0)
	res, _ := s.Take(ctx, "k", limit, 1, t0.Add(time.Hour))
	if res.Remaining != 1 {
		t.Fatalf("remaining = %d, want 1 (capacity 2 minus 1)", res.Remaining)
	}
}

func TestBurst(t *testing.T) {
	s := newStore(t)
	limit := ratekeeper.PerSecond(1).WithBurst(5)
	ctx := context.Background()

	res, _ := s.Take(ctx, "k", limit, 5, t0)
	if !res.Allowed || res.Remaining != 0 || res.Limit != 5 {
		t.Fatalf("unexpected result %+v", res)
	}
}

func TestDeniedRequestConsumesNothing(t *testing.T) {
	s := newStore(t)
	limit := ratekeeper.PerSecond(5)
	ctx := context.Background()

	_, _ = s.Take(ctx, "k", limit, 3, t0)
	if res, _ := s.Take(ctx, "k", limit, 3, t0); res.Allowed {
		t.Fatal("expected denial for 3 with 2 remaining")
	}
	if res, _ := s.Take(ctx, "k", limit, 2, t0); !res.Allowed {
		t.Fatal("denied request should not have consumed tokens")
	}
}

func TestKeysAreIndependent(t *testing.T) {
	s := newStore(t)
	limit := ratekeeper.PerSecond(1)
	ctx := context.Background()

	_, _ = s.Take(ctx, "a", limit, 1, t0)
	if res, _ := s.Take(ctx, "b", limit, 1, t0); !res.Allowed {
		t.Fatal("key b should have its own bucket")
	}
}

func TestClockGoingBackwards(t *testing.T) {
	s := newStore(t)
	limit := ratekeeper.PerSecond(1)
	ctx := context.Background()

	_, _ = s.Take(ctx, "k", limit, 1, t0)
	if res, _ := s.Take(ctx, "k", limit, 1, t0.Add(-time.Hour)); res.Allowed {
		t.Fatal("clock skew must not refill the bucket")
	}
}

func TestEvictFull(t *testing.T) {
	s := newStore(t)
	limit := ratekeeper.PerSecond(1)
	ctx := context.Background()

	_, _ = s.Take(ctx, "drained", limit, 1, t0)
	_, _ = s.Take(ctx, "idle", ratekeeper.PerSecond(100).WithBurst(1), 1, t0)

	s.evictFull(t0.Add(10 * time.Millisecond))
	if s.Len() != 1 {
		t.Fatalf("len = %d, want 1 (only the refilled bucket evicted)", s.Len())
	}
	s.evictFull(t0.Add(time.Second))
	if s.Len() != 0 {
		t.Fatalf("len = %d, want 0", s.Len())
	}
}

func TestConcurrentTakeNeverOverAdmits(t *testing.T) {
	s := newStore(t)
	limit := ratekeeper.PerHour(100)
	ctx := context.Background()

	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if res, _ := s.Take(ctx, "k", limit, 1, t0); res.Allowed {
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

func TestCloseIsIdempotent(t *testing.T) {
	s := New(WithCleanupInterval(time.Millisecond))
	_ = s.Close()
	_ = s.Close()
}

func BenchmarkTakeSingleKey(b *testing.B) {
	s := New(WithCleanupInterval(0))
	defer s.Close()
	limit := ratekeeper.PerSecond(1_000_000_000)
	ctx := context.Background()
	now := time.Now()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.Take(ctx, "k", limit, 1, now)
	}
}

func BenchmarkTakeParallel(b *testing.B) {
	s := New(WithCleanupInterval(0))
	defer s.Close()
	limit := ratekeeper.PerSecond(1_000_000_000)
	ctx := context.Background()
	now := time.Now()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = s.Take(ctx, "k", limit, 1, now)
		}
	})
}

package memory

import (
	"context"
	"testing"
	"time"

	"github.com/ChhaviRathore01/ratekeeper"
	"github.com/ChhaviRathore01/ratekeeper/store/storetest"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	s := New(WithCleanupInterval(0))
	t.Cleanup(s.Close)
	return s
}

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) ratekeeper.Store { return newStore(t) })
}

func TestEvictFull(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	t0 := storetest.T0

	_, _ = s.Take(ctx, "drained", ratekeeper.PerSecond(1), 1, t0)
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

func TestCloseIsIdempotent(t *testing.T) {
	s := New(WithCleanupInterval(time.Millisecond))
	s.Close()
	s.Close()
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

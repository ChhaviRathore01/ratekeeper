package hybrid

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/ChhaviRathore01/ratekeeper"
	"github.com/ChhaviRathore01/ratekeeper/store/memory"
	"github.com/ChhaviRathore01/ratekeeper/store/redisstore"
	"github.com/ChhaviRathore01/ratekeeper/store/storetest"
)

func newMemory(t testing.TB) *memory.Store {
	t.Helper()
	m := memory.New(memory.WithCleanupInterval(0))
	t.Cleanup(m.Close)
	return m
}

func newHybrid(t testing.TB, backend ratekeeper.Store, opts ...Option) *Store {
	t.Helper()
	s := New(backend, append([]Option{WithCleanupInterval(0)}, opts...)...)
	t.Cleanup(s.Close)
	return s
}

// A single hybrid instance must be indistinguishable from its backend.
func TestConformanceOverMemory(t *testing.T) {
	storetest.Run(t, func(t *testing.T) ratekeeper.Store {
		return newHybrid(t, newMemory(t))
	})
}

func TestConformanceOverRedis(t *testing.T) {
	storetest.Run(t, func(t *testing.T) ratekeeper.Store {
		mr := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = client.Close() })
		return newHybrid(t, redisstore.New(client, redisstore.WithClientTime()))
	})
}

func TestServesFromLeaseWithoutBackend(t *testing.T) {
	s := newHybrid(t, newMemory(t), WithBatchSize(10))
	ctx := context.Background()
	limit := ratekeeper.PerSecond(100)

	for i := 0; i < 10; i++ {
		res, err := s.Take(ctx, "k", limit, 1, storetest.T0)
		if err != nil || !res.Allowed {
			t.Fatalf("request %d: %+v %v", i, res, err)
		}
	}
	if st := s.Stats(); st.BackendCalls != 1 || st.LocalHits != 9 {
		t.Fatalf("stats = %+v, want 1 backend call and 9 local hits", st)
	}
	// 11th request needs a new batch.
	_, _ = s.Take(ctx, "k", limit, 1, storetest.T0)
	if st := s.Stats(); st.BackendCalls != 2 {
		t.Fatalf("stats = %+v, want 2 backend calls", st)
	}
}

func TestLeaseExpires(t *testing.T) {
	backend := newMemory(t)
	s := newHybrid(t, backend, WithBatchSize(10), WithLeaseTTL(100*time.Millisecond))
	ctx := context.Background()
	limit := ratekeeper.PerMinute(100)

	_, _ = s.Take(ctx, "k", limit, 1, storetest.T0)
	_, _ = s.Take(ctx, "k", limit, 1, storetest.T0.Add(100*time.Millisecond))
	if st := s.Stats(); st.BackendCalls != 2 {
		t.Fatalf("expired lease should force a backend call, stats = %+v", st)
	}
	// The 9 tokens of the first lease are gone from the backend for good.
	res, _ := backend.Take(ctx, "k", limit, 1, storetest.T0.Add(100*time.Millisecond))
	if res.Remaining != 79 {
		t.Fatalf("backend remaining = %d, want 79 (100 - 10 - 10 - 1)", res.Remaining)
	}
}

func TestAdaptiveStopsLeasingNearLimit(t *testing.T) {
	backend := newMemory(t)
	ctx := context.Background()
	limit := ratekeeper.PerMinute(25)

	a := newHybrid(t, backend, WithBatchSize(10))
	b := newHybrid(t, backend, WithBatchSize(10))

	_, _ = a.Take(ctx, "k", limit, 1, storetest.T0) // a leases 10, backend 15 left
	_, _ = b.Take(ctx, "k", limit, 1, storetest.T0) // b leases 10, backend 5 left
	// b now knows only 5 remain (< batch), so it must not lease any more:
	// it uses its 9 local tokens, then takes exactly one at a time.
	for i := 0; i < 9; i++ {
		_, _ = b.Take(ctx, "k", limit, 1, storetest.T0)
	}
	for i := 0; i < 5; i++ {
		res, _ := b.Take(ctx, "k", limit, 1, storetest.T0)
		if !res.Allowed {
			t.Fatalf("b request %d denied; the last 5 tokens should be usable", i)
		}
	}
	if res, _ := b.Take(ctx, "k", limit, 1, storetest.T0); res.Allowed {
		t.Fatal("limit exhausted, b must be denied")
	}
	// a still holds its own 9 tokens.
	for i := 0; i < 9; i++ {
		if res, _ := a.Take(ctx, "k", limit, 1, storetest.T0); !res.Allowed {
			t.Fatalf("a's leased token %d should be usable", i)
		}
	}
}

func TestBatchFallsBackToExactRequest(t *testing.T) {
	backend := newMemory(t)
	ctx := context.Background()
	limit := ratekeeper.PerMinute(10)
	_, _ = backend.Take(ctx, "k", limit, 7, storetest.T0) // only 3 left

	s := newHybrid(t, backend, WithBatchSize(5))
	res, _ := s.Take(ctx, "k", limit, 2, storetest.T0)
	if !res.Allowed {
		t.Fatal("batch of 5 fails, but the 2 actually needed are available")
	}
}

func TestNegativeCache(t *testing.T) {
	s := newHybrid(t, newMemory(t), WithBatchSize(1))
	ctx := context.Background()
	limit := ratekeeper.PerSecond(1)

	_, _ = s.Take(ctx, "k", limit, 1, storetest.T0)
	res, _ := s.Take(ctx, "k", limit, 1, storetest.T0)
	if res.Allowed {
		t.Fatal("expected denial")
	}
	calls := s.Stats().BackendCalls

	for i := 0; i < 100; i++ {
		res, _ := s.Take(ctx, "k", limit, 1, storetest.T0.Add(500*time.Millisecond))
		if res.Allowed || res.RetryAfter != 500*time.Millisecond {
			t.Fatalf("cached denial wrong: %+v", res)
		}
	}
	if st := s.Stats(); st.BackendCalls != calls || st.LocalDenials != 100 {
		t.Fatalf("denials should be served locally, stats = %+v", st)
	}
	if res, _ := s.Take(ctx, "k", limit, 1, storetest.T0.Add(time.Second)); !res.Allowed {
		t.Fatal("allowed again once RetryAfter has passed")
	}
}

type flakyStore struct {
	ratekeeper.Store
	down bool
}

func (f *flakyStore) Take(ctx context.Context, key string, l ratekeeper.Limit, n int, now time.Time) (ratekeeper.Result, error) {
	if f.down {
		return ratekeeper.Result{}, errors.New("backend down")
	}
	return f.Store.Take(ctx, key, l, n, now)
}

func TestLeaseAbsorbsBackendOutage(t *testing.T) {
	backend := &flakyStore{Store: newMemory(t)}
	s := newHybrid(t, backend, WithBatchSize(5))
	ctx := context.Background()
	limit := ratekeeper.PerSecond(100)

	_, _ = s.Take(ctx, "k", limit, 1, storetest.T0)
	backend.down = true
	for i := 0; i < 4; i++ {
		if res, err := s.Take(ctx, "k", limit, 1, storetest.T0); err != nil || !res.Allowed {
			t.Fatalf("leased token %d should work while backend is down: %v", i, err)
		}
	}
	if _, err := s.Take(ctx, "k", limit, 1, storetest.T0); err == nil {
		t.Fatal("expected backend error once the lease is used up")
	}
}

func TestEvictExpired(t *testing.T) {
	s := newHybrid(t, newMemory(t), WithLeaseTTL(time.Second))
	_, _ = s.Take(context.Background(), "k", ratekeeper.PerSecond(100), 1, storetest.T0)

	s.evictExpired(storetest.T0.Add(1500 * time.Millisecond))
	if len(s.leases) != 1 {
		t.Fatal("lease expired less than one TTL ago should be kept")
	}
	s.evictExpired(storetest.T0.Add(2001 * time.Millisecond))
	if len(s.leases) != 0 {
		t.Fatal("old lease should be evicted")
	}
}

// simulation runs `nodes` instances against one shared backend, each
// receiving `perNodePerMs` requests every simulated millisecond, and reports
// how many were admitted and how many backend calls were made.
type simResult struct {
	requests, admitted int
	backendCalls       int64
}

func simulate(t *testing.T, nodes, perNodePerMs int, limit ratekeeper.Limit, d time.Duration, useHybrid bool, opts ...Option) simResult {
	t.Helper()
	backend := newMemory(t)
	stores := make([]ratekeeper.Store, nodes)
	for i := range stores {
		if useHybrid {
			stores[i] = newHybrid(t, backend, opts...)
		} else {
			stores[i] = backend
		}
	}

	var r simResult
	ctx := context.Background()
	for ms := 0; ms < int(d/time.Millisecond); ms++ {
		now := storetest.T0.Add(time.Duration(ms) * time.Millisecond)
		for _, s := range stores {
			for k := 0; k < perNodePerMs; k++ {
				r.requests++
				res, err := s.Take(ctx, "k", limit, 1, now)
				if err != nil {
					t.Fatal(err)
				}
				if res.Allowed {
					r.admitted++
				}
			}
		}
	}
	if useHybrid {
		for _, s := range stores {
			r.backendCalls += s.(*Store).Stats().BackendCalls
		}
	} else {
		r.backendCalls = int64(r.requests)
	}
	return r
}

func TestSimulationUnderLimit(t *testing.T) {
	// 5 instances, 5,000 req/s total against a 10,000 req/s limit.
	limit := ratekeeper.PerSecond(10_000)
	r := simulate(t, 5, 1, limit, 10*time.Second, true, WithBatchSize(20), WithLeaseTTL(100*time.Millisecond))

	if r.admitted != r.requests {
		t.Fatalf("admitted %d of %d; nothing should be denied at 50%% load", r.admitted, r.requests)
	}
	if reduction := float64(r.requests) / float64(r.backendCalls); reduction < 15 {
		t.Fatalf("backend calls reduced only %.1fx, want >= 15x with batch 20", reduction)
	}
	t.Logf("under limit: %d requests, %d backend calls (%.1fx fewer)",
		r.requests, r.backendCalls, float64(r.requests)/float64(r.backendCalls))
}

func TestSimulationOverLimit(t *testing.T) {
	// 5 instances, 50,000 req/s total against a 10,000 req/s limit.
	limit := ratekeeper.PerSecond(10_000)
	d := 10 * time.Second
	exact := simulate(t, 5, 10, limit, d, false)
	hyb := simulate(t, 5, 10, limit, d, true, WithBatchSize(20), WithLeaseTTL(100*time.Millisecond))

	if hyb.admitted > exact.admitted {
		t.Fatalf("hybrid admitted %d > exact %d: must never over-admit", hyb.admitted, exact.admitted)
	}
	accuracy := float64(hyb.admitted) / float64(exact.admitted)
	if accuracy < 0.99 {
		t.Fatalf("hybrid admitted %.2f%% of exact, want >= 99%%", accuracy*100)
	}
	// Near the limit leasing switches off by design (exactness matters
	// most there); the saving comes from the negative cache.
	if reduction := float64(hyb.requests) / float64(hyb.backendCalls); reduction < 3 {
		t.Fatalf("backend calls reduced only %.1fx under overload", reduction)
	}
	t.Logf("over limit: exact admitted %d, hybrid admitted %d (%.2f%%), backend calls %d -> %d (%.1fx fewer)",
		exact.admitted, hyb.admitted, accuracy*100, exact.backendCalls, hyb.backendCalls,
		float64(exact.backendCalls)/float64(hyb.backendCalls))
}

func TestConcurrentNodesNeverOverAdmit(t *testing.T) {
	backend := newMemory(t)
	limit := ratekeeper.PerHour(1000)
	var mu sync.Mutex
	admitted := 0

	var wg sync.WaitGroup
	for n := 0; n < 4; n++ {
		s := newHybrid(t, backend, WithBatchSize(25))
		for g := 0; g < 10; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 100; i++ {
					res, err := s.Take(context.Background(), "k", limit, 1, storetest.T0)
					if err != nil {
						t.Error(err)
						return
					}
					if res.Allowed {
						mu.Lock()
						admitted++
						mu.Unlock()
					}
				}
			}()
		}
	}
	wg.Wait()
	if admitted > 1000 {
		t.Fatalf("admitted %d > limit 1000", admitted)
	}
	if admitted < 1000-4*25 {
		t.Fatalf("admitted %d, want at least %d (limit minus max stranded)", admitted, 1000-4*25)
	}
}

func BenchmarkRealRedis(b *testing.B) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		b.Skip("REDIS_ADDR not set")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = client.Close() }()
	// High enough that the benchmark never hits the limit, so it measures
	// the cost of an allowed request.
	limit := ratekeeper.PerSecond(100_000_000)
	ctx := context.Background()

	for _, batch := range []int{0, 10, 100} {
		name := "direct"
		if batch > 0 {
			name = fmt.Sprintf("hybrid-batch%d", batch)
		}
		b.Run(name, func(b *testing.B) {
			prefix := fmt.Sprintf("ratekeeper-bench:%d:", time.Now().UnixNano())
			var s ratekeeper.Store = redisstore.New(client, redisstore.WithKeyPrefix(prefix))
			var h *Store
			if batch > 0 {
				h = New(s, WithBatchSize(batch))
				defer h.Close()
				s = h
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err := s.Take(ctx, "k", limit, 1, time.Now()); err != nil {
						b.Error(err)
						return
					}
				}
			})
			if h != nil {
				b.ReportMetric(float64(h.Stats().BackendCalls)/float64(b.N), "redis-calls/op")
			} else {
				b.ReportMetric(1, "redis-calls/op")
			}
		})
	}
}

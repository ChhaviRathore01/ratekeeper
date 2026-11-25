package redisstore

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/ChhaviRathore01/ratekeeper"
	"github.com/ChhaviRathore01/ratekeeper/store/storetest"
)

// newMini starts an in-process Redis (miniredis) for fast unit tests.
func newMini(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return mr, client
}

// realRedis returns a client for the Redis at $REDIS_ADDR, skipping the test
// when it is not set. Each call gets a unique key prefix so tests don't
// interfere with each other or with existing data.
func realRedis(t testing.TB) (*redis.Client, string) {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set; skipping integration test")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("ping %s: %v", addr, err)
	}
	return client, fmt.Sprintf("ratekeeper-test:%d:", time.Now().UnixNano())
}

func TestConformanceMiniredis(t *testing.T) {
	storetest.Run(t, func(t *testing.T) ratekeeper.Store {
		_, client := newMini(t)
		return New(client, WithClientTime())
	})
}

func TestConformanceRealRedis(t *testing.T) {
	storetest.Run(t, func(t *testing.T) ratekeeper.Store {
		client, prefix := realRedis(t)
		return New(client, WithClientTime(), WithKeyPrefix(prefix))
	})
}

func TestServerTimeRealRedis(t *testing.T) {
	client, prefix := realRedis(t)
	s := New(client, WithKeyPrefix(prefix))
	limit := ratekeeper.PerMinute(2)
	ctx := context.Background()
	// The `now` argument must be ignored in server-time mode, so a wildly
	// wrong client clock must not matter.
	skewed := time.Now().Add(-24 * time.Hour)

	for i := 0; i < 2; i++ {
		res, err := s.Take(ctx, "k", limit, 1, skewed)
		if err != nil || !res.Allowed {
			t.Fatalf("request %d: res=%+v err=%v", i, res, err)
		}
	}
	res, err := s.Take(ctx, "k", limit, 1, skewed)
	if err != nil {
		t.Fatal(err)
	}
	if res.Allowed {
		t.Fatal("expected third request to be denied")
	}
	if res.RetryAfter <= 29*time.Second || res.RetryAfter > 30*time.Second {
		t.Fatalf("RetryAfter = %s, want ~30s", res.RetryAfter)
	}
}

func TestKeyExpiresWhenFull(t *testing.T) {
	mr, client := newMini(t)
	s := New(client, WithClientTime())
	limit := ratekeeper.PerSecond(10) // 100ms per token
	ctx := context.Background()

	if _, err := s.Take(ctx, "k", limit, 3, storetest.T0); err != nil {
		t.Fatal(err)
	}
	key := DefaultKeyPrefix + "k"
	if ttl := mr.TTL(key); ttl != 300*time.Millisecond {
		t.Fatalf("TTL = %s, want 300ms (time until 3 tokens refill)", ttl)
	}
	mr.FastForward(300 * time.Millisecond)
	if mr.Exists(key) {
		t.Fatal("key should expire once the bucket is full")
	}
}

func TestDeniedRequestDoesNotWrite(t *testing.T) {
	mr, client := newMini(t)
	s := New(client, WithClientTime())
	limit := ratekeeper.PerSecond(1)
	ctx := context.Background()

	_, _ = s.Take(ctx, "k", limit, 1, storetest.T0)
	before, _ := mr.Get(DefaultKeyPrefix + "k")
	ttlBefore := mr.TTL(DefaultKeyPrefix + "k")

	res, _ := s.Take(ctx, "k", limit, 1, storetest.T0)
	if res.Allowed {
		t.Fatal("expected denial")
	}
	after, _ := mr.Get(DefaultKeyPrefix + "k")
	if before != after || mr.TTL(DefaultKeyPrefix+"k") != ttlBefore {
		t.Fatal("denied request must not modify state")
	}
}

func TestKeyPrefix(t *testing.T) {
	mr, client := newMini(t)
	s := New(client, WithClientTime(), WithKeyPrefix("app1:"))
	_, _ = s.Take(context.Background(), "user:1", ratekeeper.PerSecond(1), 1, storetest.T0)
	if !mr.Exists("app1:user:1") {
		t.Fatalf("expected key app1:user:1, have %v", mr.Keys())
	}
}

func TestRedisDownReturnsError(t *testing.T) {
	mr, client := newMini(t)
	s := New(client, WithClientTime())
	mr.Close()

	if _, err := s.Take(context.Background(), "k", ratekeeper.PerSecond(1), 1, storetest.T0); err == nil {
		t.Fatal("expected error when Redis is unreachable")
	}
}

func TestLimiterFailOpenWhenRedisDown(t *testing.T) {
	mr, client := newMini(t)
	l, err := ratekeeper.New(New(client), ratekeeper.PerSecond(1),
		ratekeeper.WithFailurePolicy(ratekeeper.FailOpen))
	if err != nil {
		t.Fatal(err)
	}
	mr.Close()

	res, err := l.Allow(context.Background(), "k")
	if err != nil || !res.Allowed {
		t.Fatalf("fail-open limiter should allow when Redis is down: res=%+v err=%v", res, err)
	}
}

func BenchmarkTakeRealRedis(b *testing.B) {
	client, prefix := realRedis(b)
	s := New(client, WithKeyPrefix(prefix))
	limit := ratekeeper.PerSecond(1_000_000)
	ctx := context.Background()
	now := time.Now()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := s.Take(ctx, "k", limit, 1, now); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

package ratekeeper_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ChhaviRathore01/ratekeeper"
	"github.com/ChhaviRathore01/ratekeeper/store/memory"
)

type failingStore struct{ err error }

func (f failingStore) Take(context.Context, string, ratekeeper.Limit, int, time.Time) (ratekeeper.Result, error) {
	return ratekeeper.Result{}, f.err
}

func newLimiter(t *testing.T, limit ratekeeper.Limit, opts ...ratekeeper.Option) *ratekeeper.Limiter {
	t.Helper()
	s := memory.New(memory.WithCleanupInterval(0))
	t.Cleanup(func() { _ = s.Close() })
	l, err := ratekeeper.New(s, limit, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestNewValidatesLimit(t *testing.T) {
	s := memory.New(memory.WithCleanupInterval(0))
	defer s.Close()

	bad := []ratekeeper.Limit{
		{Rate: 0, Period: time.Second},
		{Rate: -1, Period: time.Second},
		{Rate: 1, Period: 0},
		{Rate: 1, Period: time.Second, Burst: -1},
		{Rate: 10, Period: time.Nanosecond},
	}
	for _, lim := range bad {
		if _, err := ratekeeper.New(s, lim); !errors.Is(err, ratekeeper.ErrInvalidLimit) {
			t.Errorf("New(%+v) err = %v, want ErrInvalidLimit", lim, err)
		}
	}
	if _, err := ratekeeper.New(nil, ratekeeper.PerSecond(1)); err == nil {
		t.Error("expected error for nil store")
	}
}

func TestAllowWithFakeClock(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	l := newLimiter(t, ratekeeper.PerSecond(2), ratekeeper.WithClock(func() time.Time { return now }))
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if res, err := l.Allow(ctx, "user:1"); err != nil || !res.Allowed {
			t.Fatalf("request %d: res=%+v err=%v", i, res, err)
		}
	}
	if res, _ := l.Allow(ctx, "user:1"); res.Allowed {
		t.Fatal("expected third request denied")
	}

	now = now.Add(500 * time.Millisecond)
	if res, _ := l.Allow(ctx, "user:1"); !res.Allowed {
		t.Fatal("expected a token after 500ms")
	}
}

func TestAllowNArgumentErrors(t *testing.T) {
	l := newLimiter(t, ratekeeper.PerSecond(5))
	ctx := context.Background()

	if _, err := l.Allow(ctx, ""); !errors.Is(err, ratekeeper.ErrEmptyKey) {
		t.Errorf("empty key err = %v", err)
	}
	if _, err := l.AllowN(ctx, "k", 0); err == nil {
		t.Error("expected error for n=0")
	}
	if _, err := l.AllowN(ctx, "k", 6); !errors.Is(err, ratekeeper.ErrExceedsBurst) {
		t.Errorf("n > capacity err = %v", err)
	}
}

func TestKeyPrefixIsolatesLimiters(t *testing.T) {
	s := memory.New(memory.WithCleanupInterval(0))
	defer s.Close()
	ctx := context.Background()

	a, _ := ratekeeper.New(s, ratekeeper.PerSecond(1), ratekeeper.WithKeyPrefix("login:"))
	b, _ := ratekeeper.New(s, ratekeeper.PerSecond(1), ratekeeper.WithKeyPrefix("api:"))

	_, _ = a.Allow(ctx, "user:1")
	if res, _ := b.Allow(ctx, "user:1"); !res.Allowed {
		t.Fatal("different prefixes must not share buckets")
	}
}

func TestFailurePolicies(t *testing.T) {
	storeErr := errors.New("backend down")
	ctx := context.Background()

	tests := []struct {
		name   string
		policy ratekeeper.FailurePolicy
		want   bool
	}{
		{"closed", ratekeeper.FailClosed, false},
		{"open", ratekeeper.FailOpen, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotErr error
			l, err := ratekeeper.New(failingStore{storeErr}, ratekeeper.PerSecond(1),
				ratekeeper.WithFailurePolicy(tt.policy),
				ratekeeper.WithErrorHandler(func(_ string, err error) { gotErr = err }),
			)
			if err != nil {
				t.Fatal(err)
			}
			res, err := l.Allow(ctx, "k")
			if err != nil {
				t.Fatalf("store failure should not surface as error, got %v", err)
			}
			if res.Allowed != tt.want {
				t.Fatalf("Allowed = %v, want %v", res.Allowed, tt.want)
			}
			if !errors.Is(gotErr, storeErr) {
				t.Fatalf("error handler got %v, want %v", gotErr, storeErr)
			}
		})
	}
}

func TestDefaultPolicyIsFailClosed(t *testing.T) {
	l, _ := ratekeeper.New(failingStore{errors.New("x")}, ratekeeper.PerSecond(1))
	if res, _ := l.Allow(context.Background(), "k"); res.Allowed {
		t.Fatal("default policy should fail closed")
	}
}

package httplimit_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ChhaviRathore01/ratekeeper"
	"github.com/ChhaviRathore01/ratekeeper/httplimit"
	"github.com/ChhaviRathore01/ratekeeper/store/memory"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

type failingStore struct{}

func (failingStore) Take(context.Context, string, ratekeeper.Limit, int, time.Time) (ratekeeper.Result, error) {
	return ratekeeper.Result{}, errors.New("backend down")
}

func newLimiter(t *testing.T, limit ratekeeper.Limit, now *time.Time) *ratekeeper.Limiter {
	t.Helper()
	s := memory.New(memory.WithCleanupInterval(0))
	t.Cleanup(s.Close)
	l, err := ratekeeper.New(s, limit, ratekeeper.WithClock(func() time.Time { return *now }))
	if err != nil {
		t.Fatal(err)
	}
	return l
}

var ok = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
})

func do(h http.Handler, remoteAddr string, headers map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = remoteAddr
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestAllowsThenDenies(t *testing.T) {
	now := t0
	h := httplimit.Middleware(newLimiter(t, ratekeeper.PerMinute(2), &now))(ok)

	for i, wantRemaining := range []string{"1", "0"} {
		w := do(h, "1.2.3.4:5678", nil)
		if w.Code != http.StatusOK {
			t.Fatalf("request %d: status %d", i, w.Code)
		}
		if got := w.Header().Get(httplimit.HeaderLimit); got != "2" {
			t.Errorf("Limit header = %q", got)
		}
		if got := w.Header().Get(httplimit.HeaderRemaining); got != wantRemaining {
			t.Errorf("request %d: Remaining = %q, want %q", i, got, wantRemaining)
		}
	}

	w := do(h, "1.2.3.4:5678", nil)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", w.Code)
	}
	if got := w.Header().Get(httplimit.HeaderRetryAfter); got != "30" {
		t.Errorf("Retry-After = %q, want 30", got)
	}
	// Bucket full again 60s after t0.
	if got, want := w.Header().Get(httplimit.HeaderReset), "1767225660"; got != want {
		t.Errorf("Reset = %q, want %q", got, want)
	}

	now = now.Add(30 * time.Second)
	if w := do(h, "1.2.3.4:5678", nil); w.Code != http.StatusOK {
		t.Fatalf("after Retry-After: status %d", w.Code)
	}
}

func TestRetryAfterRoundsUpToOneSecond(t *testing.T) {
	now := t0
	h := httplimit.Middleware(newLimiter(t, ratekeeper.PerSecond(10), &now))(ok)
	for i := 0; i < 10; i++ {
		do(h, "1.2.3.4:1", nil)
	}
	w := do(h, "1.2.3.4:1", nil)
	if got := w.Header().Get(httplimit.HeaderRetryAfter); got != "1" {
		t.Fatalf("Retry-After = %q, want 1 (100ms rounded up)", got)
	}
}

func TestByIPSeparatesClients(t *testing.T) {
	now := t0
	h := httplimit.Middleware(newLimiter(t, ratekeeper.PerMinute(1), &now))(ok)
	do(h, "1.1.1.1:1", nil)
	if w := do(h, "1.1.1.1:2", nil); w.Code != http.StatusTooManyRequests {
		t.Fatal("same IP, different port should share a limit")
	}
	if w := do(h, "2.2.2.2:1", nil); w.Code != http.StatusOK {
		t.Fatal("different IP should have its own limit")
	}
	if w := do(h, "[2001:db8::1]:443", nil); w.Code != http.StatusOK {
		t.Fatal("IPv6 client should be handled")
	}
}

func TestByForwardedFor(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "10.0.0.1:80"                            // the load balancer
	r.Header.Add("X-Forwarded-For", "6.6.6.6, 203.0.113.7") // 6.6.6.6 is spoofed by the client
	r.Header.Add("X-Forwarded-For", "10.0.0.2")

	tests := []struct {
		proxies int
		want    string
	}{
		{1, "10.0.0.2"},
		{2, "203.0.113.7"},
		{3, "6.6.6.6"},
		{4, "10.0.0.1"}, // not enough hops: fall back to RemoteAddr
	}
	for _, tt := range tests {
		got, err := httplimit.ByForwardedFor(tt.proxies)(r)
		if err != nil || got != tt.want {
			t.Errorf("trustedProxies=%d: got %q, %v; want %q", tt.proxies, got, err, tt.want)
		}
	}
}

func TestByHeader(t *testing.T) {
	now := t0
	h := httplimit.Middleware(newLimiter(t, ratekeeper.PerMinute(1), &now),
		httplimit.WithKeyFunc(httplimit.ByHeader("X-API-Key")))(ok)

	if w := do(h, "1.1.1.1:1", nil); w.Code != http.StatusBadRequest {
		t.Fatalf("missing key: status %d, want 400", w.Code)
	}
	do(h, "1.1.1.1:1", map[string]string{"X-API-Key": "a"})
	if w := do(h, "9.9.9.9:1", map[string]string{"X-API-Key": "a"}); w.Code != http.StatusTooManyRequests {
		t.Fatal("same API key from another IP should share a limit")
	}
	if w := do(h, "1.1.1.1:1", map[string]string{"X-API-Key": "b"}); w.Code != http.StatusOK {
		t.Fatal("different API key should have its own limit")
	}
}

func TestCost(t *testing.T) {
	now := t0
	h := httplimit.Middleware(newLimiter(t, ratekeeper.PerMinute(10), &now),
		httplimit.WithCost(func(r *http.Request) int {
			if r.Method == http.MethodPost {
				return 5
			}
			return 1
		}))(ok)

	post := func() int {
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}
	for i := 0; i < 2; i++ {
		if code := post(); code != http.StatusOK {
			t.Fatalf("POST %d: status %d; two POSTs of cost 5 should fit in 10", i, code)
		}
	}
	if post() != http.StatusTooManyRequests {
		t.Fatal("third POST should be denied")
	}
}

func TestCostAboveBurstIsServerError(t *testing.T) {
	now := t0
	h := httplimit.Middleware(newLimiter(t, ratekeeper.PerMinute(2), &now),
		httplimit.WithCost(func(*http.Request) int { return 3 }))(ok)
	if w := do(h, "1.1.1.1:1", nil); w.Code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500 for misconfigured cost", w.Code)
	}
}

func TestSkip(t *testing.T) {
	now := t0
	h := httplimit.Middleware(newLimiter(t, ratekeeper.PerMinute(1), &now),
		httplimit.WithSkip(func(r *http.Request) bool { return r.URL.Path == "/healthz" }))(ok)

	for i := 0; i < 5; i++ {
		r := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusOK || w.Header().Get(httplimit.HeaderLimit) != "" {
			t.Fatal("skipped requests must not be limited or get headers")
		}
	}
}

func TestCustomDeniedHandler(t *testing.T) {
	now := t0
	h := httplimit.Middleware(newLimiter(t, ratekeeper.PerMinute(1), &now),
		httplimit.WithDeniedHandler(func(w http.ResponseWriter, _ *http.Request, res ratekeeper.Result) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"slow down"}`))
		}))(ok)

	do(h, "1.1.1.1:1", nil)
	w := do(h, "1.1.1.1:1", nil)
	if w.Body.String() != `{"error":"slow down"}` || w.Header().Get(httplimit.HeaderRetryAfter) == "" {
		t.Fatalf("custom handler not used correctly: %q %v", w.Body.String(), w.Header())
	}
}

func TestWithoutHeaders(t *testing.T) {
	now := t0
	h := httplimit.Middleware(newLimiter(t, ratekeeper.PerMinute(1), &now), httplimit.WithoutHeaders())(ok)
	if w := do(h, "1.1.1.1:1", nil); w.Header().Get(httplimit.HeaderLimit) != "" {
		t.Fatal("headers should be disabled")
	}
	if w := do(h, "1.1.1.1:1", nil); w.Header().Get(httplimit.HeaderRetryAfter) == "" {
		t.Fatal("Retry-After should still be sent on 429")
	}
}

func TestFailOpenOmitsResetHeader(t *testing.T) {
	l, err := ratekeeper.New(failingStore{}, ratekeeper.PerMinute(1),
		ratekeeper.WithFailurePolicy(ratekeeper.FailOpen))
	if err != nil {
		t.Fatal(err)
	}
	w := do(httplimit.Middleware(l)(ok), "1.1.1.1:1", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("fail-open: status %d", w.Code)
	}
	if w.Header().Get(httplimit.HeaderReset) != "" {
		t.Fatal("Reset header should be omitted when the store failed")
	}
}

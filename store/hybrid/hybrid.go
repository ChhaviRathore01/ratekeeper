// Package hybrid provides a Store that cuts round-trips to a shared backend
// (such as Redis) by leasing tokens in batches and serving requests from a
// local cache.
//
// # How it works
//
// When a request for a key arrives and no local tokens are left, the store
// takes a batch of tokens from the backend in a single call and keeps the
// spare ones in memory. Following requests for that key on this instance are
// answered locally, without network I/O, until the batch runs out or the
// lease expires.
//
// # Accuracy
//
// Every token used locally was granted by the backend first, so the hybrid
// store never admits more than the limit allows. The trade-off goes the
// other way: tokens leased by one instance cannot be used by another, and
// tokens left in an expired lease are discarded. In the worst case the
// cluster admits up to (instances × (batch size − 1)) fewer requests than
// the limit, and leased tokens can be used up to the lease TTL after they
// were granted.
//
// To keep this cost low where it matters, leasing is adaptive: once the
// backend reports that fewer tokens remain than one batch, the store stops
// leasing and requests exactly what each call needs, so behaviour near the
// limit is exact.
//
// Use it for high-volume keys (per-tenant or global limits) where backend
// latency or load matters. For low-volume keys spread over many instances,
// such as a few requests per user per minute, use the backend store directly.
package hybrid

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ChhaviRathore01/ratekeeper"
)

// Store wraps a backend Store with local token leasing.
type Store struct {
	backend   ratekeeper.Store
	batchSize int
	leaseTTL  time.Duration
	cleanup   time.Duration // 0 = default (2 × leaseTTL), < 0 = disabled

	mu     sync.Mutex
	leases map[string]*lease

	localHits    atomic.Int64
	localDenials atomic.Int64
	backendCalls atomic.Int64

	stop      chan struct{}
	closeOnce sync.Once
}

// lease is the local state for one key. Its mutex is held during backend
// calls, so concurrent requests for the same key on this instance wait for
// a single round-trip and then share the leased tokens.
type lease struct {
	mu sync.Mutex

	tokens  int       // leased tokens not yet used
	expires time.Time // tokens are discarded after this

	known           bool // whether globalRemaining has been observed
	globalRemaining int  // tokens left in the backend at the last call
	resetAt         time.Time

	// Negative cache: the backend said a request for deniedNeed tokens
	// cannot succeed before deniedUntil. Other instances can only consume
	// tokens, never add them, so requests at least that large are denied
	// locally until then.
	deniedUntil time.Time
	deniedNeed  int
}

// Option configures a Store.
type Option func(*Store)

// WithBatchSize sets how many tokens are leased per backend call.
// Larger batches mean fewer backend calls but more potentially stranded
// tokens. It is capped at the limit's capacity. Defaults to 10.
func WithBatchSize(n int) Option {
	return func(s *Store) {
		if n >= 1 {
			s.batchSize = n
		}
	}
}

// WithLeaseTTL sets how long leased tokens may be used before they are
// discarded. Defaults to one second.
func WithLeaseTTL(d time.Duration) Option {
	return func(s *Store) {
		if d > 0 {
			s.leaseTTL = d
		}
	}
}

// WithCleanupInterval sets how often expired leases are removed to bound
// memory use. Defaults to twice the lease TTL. Zero or negative disables
// cleanup.
func WithCleanupInterval(d time.Duration) Option {
	return func(s *Store) {
		if d <= 0 {
			d = -1
		}
		s.cleanup = d
	}
}

// New creates a hybrid Store in front of backend. Call Close to stop the
// background cleanup of expired leases.
func New(backend ratekeeper.Store, opts ...Option) *Store {
	s := &Store{
		backend:   backend,
		batchSize: 10,
		leaseTTL:  time.Second,
		leases:    make(map[string]*lease),
		stop:      make(chan struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.cleanup == 0 {
		s.cleanup = 2 * s.leaseTTL
	}
	if s.cleanup > 0 {
		go s.janitor()
	}
	return s
}

var _ ratekeeper.Store = (*Store)(nil)

// Stats reports how requests were served.
type Stats struct {
	LocalHits    int64 // requests allowed from leased tokens
	LocalDenials int64 // requests denied from the negative cache
	BackendCalls int64 // round-trips to the backend
}

// Stats returns counters since the store was created.
func (s *Store) Stats() Stats {
	return Stats{
		LocalHits:    s.localHits.Load(),
		LocalDenials: s.localDenials.Load(),
		BackendCalls: s.backendCalls.Load(),
	}
}

func (s *Store) lease(key string) *lease {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.leases[key]
	if !ok {
		l = &lease{}
		s.leases[key] = l
	}
	return l
}

// Take implements ratekeeper.Store.
//
// For locally served requests, Result.Remaining is an estimate (backend
// tokens at the last call plus local tokens) and ResetAt is the value from
// the last backend call.
func (s *Store) Take(ctx context.Context, key string, limit ratekeeper.Limit, n int, now time.Time) (ratekeeper.Result, error) {
	l := s.lease(key)
	l.mu.Lock()
	defer l.mu.Unlock()

	local := 0
	if now.Before(l.expires) {
		local = l.tokens
	}

	// Fast path: enough leased tokens, no network.
	if local >= n {
		l.tokens = local - n
		s.localHits.Add(1)
		return ratekeeper.Result{
			Allowed:   true,
			Limit:     limit.Capacity(),
			Remaining: l.globalRemaining + l.tokens,
			ResetAt:   l.resetAt,
		}, nil
	}

	need := n - local

	// Known to be impossible right now: deny without network.
	if need >= l.deniedNeed && l.deniedNeed > 0 && now.Before(l.deniedUntil) {
		s.localDenials.Add(1)
		return ratekeeper.Result{
			Limit:      limit.Capacity(),
			Remaining:  local,
			RetryAfter: l.deniedUntil.Sub(now),
			ResetAt:    l.resetAt,
		}, nil
	}

	// Slow path: fetch the shortfall, plus spare tokens to lease unless the
	// backend is nearly empty.
	want := need
	batch := min(s.batchSize, limit.Capacity())
	if !l.known || l.globalRemaining >= batch {
		want = max(need, batch)
	}

	res, err := s.take(ctx, key, limit, want, now)
	if err != nil {
		return ratekeeper.Result{}, err
	}
	if !res.Allowed && want > need {
		// Not enough for a full batch; try for just what this call needs.
		if res, err = s.take(ctx, key, limit, need, now); err != nil {
			return ratekeeper.Result{}, err
		}
		want = need
	}

	l.known = true
	l.globalRemaining = res.Remaining
	l.resetAt = res.ResetAt

	if !res.Allowed {
		l.deniedUntil = now.Add(res.RetryAfter)
		l.deniedNeed = need
		// Keep any unexpired local tokens for smaller requests.
		res.Remaining += local
		return res, nil
	}
	l.deniedNeed = 0

	l.tokens = local + want - n
	l.expires = now.Add(s.leaseTTL)
	res.Remaining += l.tokens
	return res, nil
}

func (s *Store) take(ctx context.Context, key string, limit ratekeeper.Limit, n int, now time.Time) (ratekeeper.Result, error) {
	s.backendCalls.Add(1)
	return s.backend.Take(ctx, key, limit, n, now)
}

// Close stops the background cleanup goroutine. It does not close the
// backend. It is safe to call more than once.
func (s *Store) Close() {
	s.closeOnce.Do(func() { close(s.stop) })
}

func (s *Store) janitor() {
	t := time.NewTicker(s.cleanup)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case now := <-t.C:
			s.evictExpired(now)
		}
	}
}

// evictExpired drops leases that expired more than one TTL ago. Leases in
// use are skipped. Dropping a lease only discards tokens, so it can never
// cause over-admission.
func (s *Store) evictExpired(now time.Time) {
	cutoff := now.Add(-s.leaseTTL)
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, l := range s.leases {
		if !l.mu.TryLock() {
			continue
		}
		if l.expires.Before(cutoff) {
			delete(s.leases, k)
		}
		l.mu.Unlock()
	}
}

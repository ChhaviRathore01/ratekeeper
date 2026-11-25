// Package httplimit provides net/http middleware that rate limits requests
// with a ratekeeper.Limiter.
//
// Allowed responses carry X-RateLimit-Limit, X-RateLimit-Remaining and
// X-RateLimit-Reset headers. Denied requests get 429 Too Many Requests with
// a Retry-After header.
//
//	limiter, _ := ratekeeper.New(store, ratekeeper.PerMinute(60))
//	mux.Handle("/api/", httplimit.Middleware(limiter)(apiHandler))
package httplimit

import (
	"errors"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ChhaviRathore01/ratekeeper"
)

// Header names set by the middleware.
const (
	HeaderLimit      = "X-RateLimit-Limit"
	HeaderRemaining  = "X-RateLimit-Remaining"
	HeaderReset      = "X-RateLimit-Reset" // Unix time in seconds
	HeaderRetryAfter = "Retry-After"       // seconds
)

// ErrNoKey is returned by a KeyFunc when the request carries nothing to
// rate limit by (for example, a missing API key header). By default such
// requests are rejected with 400 Bad Request.
var ErrNoKey = errors.New("httplimit: no rate limit key in request")

// KeyFunc extracts the rate limit key from a request.
type KeyFunc func(r *http.Request) (string, error)

// ByIP keys requests by the client IP from r.RemoteAddr.
//
// Behind a reverse proxy or load balancer RemoteAddr is the proxy's
// address, so every client shares one limit; use ByForwardedFor instead.
func ByIP(r *http.Request) (string, error) {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if host == "" {
		return "", ErrNoKey
	}
	return host, nil
}

// ByForwardedFor keys requests by client IP taken from X-Forwarded-For,
// for services behind trustedProxies reverse proxies.
//
// Clients can put anything in X-Forwarded-For, so the leftmost entry cannot
// be trusted. Each trusted proxy appends the address it received the request
// from, so the real client IP is the entry trustedProxies positions from the
// right. With trustedProxies = 1 (a single load balancer) that is the last
// entry. If the header has too few entries, RemoteAddr is used.
func ByForwardedFor(trustedProxies int) KeyFunc {
	if trustedProxies < 1 {
		panic("httplimit: ByForwardedFor requires trustedProxies >= 1")
	}
	return func(r *http.Request) (string, error) {
		var hops []string
		for _, h := range r.Header.Values("X-Forwarded-For") {
			for _, part := range strings.Split(h, ",") {
				if p := strings.TrimSpace(part); p != "" {
					hops = append(hops, p)
				}
			}
		}
		if len(hops) < trustedProxies {
			return ByIP(r)
		}
		return hops[len(hops)-trustedProxies], nil
	}
}

// ByHeader keys requests by the value of a header, such as an API key.
// Requests without the header fail with ErrNoKey.
func ByHeader(name string) KeyFunc {
	return func(r *http.Request) (string, error) {
		if v := r.Header.Get(name); v != "" {
			return v, nil
		}
		return "", ErrNoKey
	}
}

type config struct {
	keyFunc      KeyFunc
	costFunc     func(*http.Request) int
	skip         func(*http.Request) bool
	onDenied     func(w http.ResponseWriter, r *http.Request, res ratekeeper.Result)
	onError      func(w http.ResponseWriter, r *http.Request, err error)
	writeHeaders bool
}

// Option configures the middleware.
type Option func(*config)

// WithKeyFunc sets how requests are grouped. Defaults to ByIP.
func WithKeyFunc(fn KeyFunc) Option {
	return func(c *config) { c.keyFunc = fn }
}

// WithCost sets how many tokens a request consumes, so expensive endpoints
// can count more than cheap ones. Defaults to 1.
func WithCost(fn func(*http.Request) int) Option {
	return func(c *config) { c.costFunc = fn }
}

// WithSkip exempts requests for which fn returns true (e.g. health checks).
func WithSkip(fn func(*http.Request) bool) Option {
	return func(c *config) { c.skip = fn }
}

// WithDeniedHandler replaces the default 429 response. Rate limit headers
// are already set when it is called.
func WithDeniedHandler(fn func(w http.ResponseWriter, r *http.Request, res ratekeeper.Result)) Option {
	return func(c *config) { c.onDenied = fn }
}

// WithErrorHandler replaces the default response when the key cannot be
// determined or the limiter rejects its arguments (400 for ErrNoKey,
// 500 otherwise).
func WithErrorHandler(fn func(w http.ResponseWriter, r *http.Request, err error)) Option {
	return func(c *config) { c.onError = fn }
}

// WithoutHeaders disables the X-RateLimit-* headers on responses.
// Retry-After is still sent on 429s.
func WithoutHeaders() Option {
	return func(c *config) { c.writeHeaders = false }
}

func defaultDenied(w http.ResponseWriter, _ *http.Request, _ ratekeeper.Result) {
	http.Error(w, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
}

func defaultError(w http.ResponseWriter, _ *http.Request, err error) {
	if errors.Is(err, ErrNoKey) {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
}

// Middleware returns middleware that rate limits requests using l.
func Middleware(l *ratekeeper.Limiter, opts ...Option) func(http.Handler) http.Handler {
	if l == nil {
		panic("httplimit: nil limiter")
	}
	cfg := config{
		keyFunc:      ByIP,
		costFunc:     func(*http.Request) int { return 1 },
		onDenied:     defaultDenied,
		onError:      defaultError,
		writeHeaders: true,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if cfg.skip != nil && cfg.skip(r) {
				next.ServeHTTP(w, r)
				return
			}

			key, err := cfg.keyFunc(r)
			if err != nil {
				cfg.onError(w, r, err)
				return
			}

			res, err := l.AllowN(r.Context(), key, cfg.costFunc(r))
			if err != nil {
				cfg.onError(w, r, err)
				return
			}

			if cfg.writeHeaders {
				setHeaders(w.Header(), res)
			}
			if !res.Allowed {
				w.Header().Set(HeaderRetryAfter, retryAfterSeconds(res.RetryAfter))
				cfg.onDenied(w, r, res)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func setHeaders(h http.Header, res ratekeeper.Result) {
	h.Set(HeaderLimit, strconv.Itoa(res.Limit))
	h.Set(HeaderRemaining, strconv.Itoa(res.Remaining))
	// ResetAt is zero when the store failed and the limiter failed open.
	if !res.ResetAt.IsZero() {
		h.Set(HeaderReset, strconv.FormatInt(int64(math.Ceil(float64(res.ResetAt.UnixNano())/1e9)), 10))
	}
}

// retryAfterSeconds rounds up, since Retry-After only supports whole
// seconds and rounding down would invite a retry that is still too early.
func retryAfterSeconds(d time.Duration) string {
	s := int64(math.Ceil(d.Seconds()))
	if s < 1 {
		s = 1
	}
	return strconv.FormatInt(s, 10)
}

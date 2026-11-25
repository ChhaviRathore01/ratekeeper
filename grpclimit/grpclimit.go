// Package grpclimit provides gRPC server interceptors that rate limit calls
// with a ratekeeper.Limiter.
//
// Denied calls fail with codes.ResourceExhausted. Response header metadata
// carries x-ratelimit-limit, x-ratelimit-remaining, x-ratelimit-reset and,
// on denial, retry-after.
//
//	srv := grpc.NewServer(
//		grpc.ChainUnaryInterceptor(grpclimit.UnaryServerInterceptor(limiter)),
//		grpc.ChainStreamInterceptor(grpclimit.StreamServerInterceptor(limiter)),
//	)
package grpclimit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/ChhaviRathore01/ratekeeper"
)

// Metadata keys set on responses. gRPC metadata keys are lowercase.
const (
	MDLimit      = "x-ratelimit-limit"
	MDRemaining  = "x-ratelimit-remaining"
	MDReset      = "x-ratelimit-reset" // Unix time in seconds
	MDRetryAfter = "retry-after"       // seconds
)

// ErrNoKey is returned by a KeyFunc when the call carries nothing to rate
// limit by. Such calls fail with codes.InvalidArgument.
var ErrNoKey = errors.New("grpclimit: no rate limit key in request")

// KeyFunc extracts the rate limit key from a call. fullMethod has the form
// "/package.Service/Method".
type KeyFunc func(ctx context.Context, fullMethod string) (string, error)

// ByPeer keys calls by the client's IP address (or the full peer address
// for non-IP transports).
func ByPeer(ctx context.Context, _ string) (string, error) {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return "", ErrNoKey
	}
	addr := p.Addr.String()
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host, nil
	}
	return addr, nil
}

// ByMetadata keys calls by the first value of an incoming metadata key,
// such as an API key or tenant ID. Calls without it fail with ErrNoKey.
func ByMetadata(name string) KeyFunc {
	return func(ctx context.Context, _ string) (string, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		if v := md.Get(name); len(v) > 0 && v[0] != "" {
			return v[0], nil
		}
		return "", ErrNoKey
	}
}

// PerMethod gives every method its own limit by combining the method name
// with the key from inner.
func PerMethod(inner KeyFunc) KeyFunc {
	return func(ctx context.Context, fullMethod string) (string, error) {
		k, err := inner(ctx, fullMethod)
		if err != nil {
			return "", err
		}
		return fullMethod + "|" + k, nil
	}
}

type config struct {
	keyFunc      KeyFunc
	costFunc     func(ctx context.Context, fullMethod string) int
	skip         func(ctx context.Context, fullMethod string) bool
	writeHeaders bool
}

// Option configures the interceptors.
type Option func(*config)

// WithKeyFunc sets how calls are grouped. Defaults to ByPeer.
func WithKeyFunc(fn KeyFunc) Option {
	return func(c *config) { c.keyFunc = fn }
}

// WithCost sets how many tokens a call consumes. Defaults to 1.
func WithCost(fn func(ctx context.Context, fullMethod string) int) Option {
	return func(c *config) { c.costFunc = fn }
}

// WithSkip exempts calls for which fn returns true (e.g. health checks).
func WithSkip(fn func(ctx context.Context, fullMethod string) bool) Option {
	return func(c *config) { c.skip = fn }
}

// WithoutHeaders disables the x-ratelimit-* response metadata. retry-after
// is still sent on denial.
func WithoutHeaders() Option {
	return func(c *config) { c.writeHeaders = false }
}

type limiter struct {
	l   *ratekeeper.Limiter
	cfg config
}

func newLimiter(l *ratekeeper.Limiter, opts []Option) *limiter {
	if l == nil {
		panic("grpclimit: nil limiter")
	}
	cfg := config{
		keyFunc:      ByPeer,
		costFunc:     func(context.Context, string) int { return 1 },
		writeHeaders: true,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &limiter{l: l, cfg: cfg}
}

// check applies the limit and returns header metadata to send (possibly
// nil) and a gRPC status error if the call must not proceed.
func (lm *limiter) check(ctx context.Context, fullMethod string) (metadata.MD, error) {
	if lm.cfg.skip != nil && lm.cfg.skip(ctx, fullMethod) {
		return nil, nil
	}

	key, err := lm.cfg.keyFunc(ctx, fullMethod)
	if err != nil {
		if errors.Is(err, ErrNoKey) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "grpclimit: %v", err)
	}

	res, err := lm.l.AllowN(ctx, key, lm.cfg.costFunc(ctx, fullMethod))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "grpclimit: %v", err)
	}

	md := metadata.MD{}
	if lm.cfg.writeHeaders {
		md.Set(MDLimit, strconv.Itoa(res.Limit))
		md.Set(MDRemaining, strconv.Itoa(res.Remaining))
		if !res.ResetAt.IsZero() {
			md.Set(MDReset, strconv.FormatInt(int64(math.Ceil(float64(res.ResetAt.UnixNano())/1e9)), 10))
		}
	}
	if !res.Allowed {
		secs := retryAfterSeconds(res.RetryAfter)
		md.Set(MDRetryAfter, strconv.FormatInt(secs, 10))
		return md, status.Error(codes.ResourceExhausted,
			fmt.Sprintf("rate limit exceeded, retry after %ds", secs))
	}
	return md, nil
}

// UnaryServerInterceptor rate limits unary calls.
func UnaryServerInterceptor(l *ratekeeper.Limiter, opts ...Option) grpc.UnaryServerInterceptor {
	lm := newLimiter(l, opts)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, err := lm.check(ctx, info.FullMethod)
		if len(md) > 0 {
			_ = grpc.SetHeader(ctx, md)
		}
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamServerInterceptor rate limits the opening of streams. Each stream
// counts once, regardless of how many messages it carries.
func StreamServerInterceptor(l *ratekeeper.Limiter, opts ...Option) grpc.StreamServerInterceptor {
	lm := newLimiter(l, opts)
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		md, err := lm.check(ss.Context(), info.FullMethod)
		if len(md) > 0 {
			_ = ss.SetHeader(md)
		}
		if err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

func retryAfterSeconds(d time.Duration) int64 {
	s := int64(math.Ceil(d.Seconds()))
	if s < 1 {
		s = 1
	}
	return s
}

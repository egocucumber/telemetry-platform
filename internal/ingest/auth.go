package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/egocucumber/telemetry-platform/internal/domain"
)

const APIKeyHeader = "x-api-key"

var ErrUnauthorized = errors.New("unknown api key")

type ctxKey struct{}

func GatewayFromContext(ctx context.Context) (domain.Gateway, bool) {
	gw, ok := ctx.Value(ctxKey{}).(domain.Gateway)
	return gw, ok
}

type Authenticator struct {
	pool *pgxpool.Pool
	ttl  time.Duration

	mu    sync.RWMutex
	cache map[string]cacheEntry
	sf    singleflight.Group
}

type cacheEntry struct {
	gw      domain.Gateway
	expires time.Time
}

func NewAuthenticator(pool *pgxpool.Pool, ttl time.Duration) *Authenticator {
	return &Authenticator{pool: pool, ttl: ttl, cache: map[string]cacheEntry{}}
}

func HashKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func (a *Authenticator) Lookup(ctx context.Context, apiKey string) (domain.Gateway, error) {
	hash := HashKey(apiKey)

	a.mu.RLock()
	e, ok := a.cache[hash]
	a.mu.RUnlock()
	if ok && time.Now().Before(e.expires) {
		return e.gw, nil
	}

	v, err, _ := a.sf.Do(hash, func() (any, error) {
		var gw domain.Gateway
		err := a.pool.QueryRow(ctx,
			`SELECT id, name FROM gateways WHERE api_key_hash = $1`, hash,
		).Scan(&gw.ID, &gw.Name)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrUnauthorized
		}
		if err != nil {
			return nil, fmt.Errorf("lookup gateway: %w", err)
		}
		a.mu.Lock()
		a.cache[hash] = cacheEntry{gw: gw, expires: time.Now().Add(a.ttl)}
		a.mu.Unlock()
		return gw, nil
	})
	if err != nil {
		return domain.Gateway{}, err
	}
	return v.(domain.Gateway), nil
}

func (a *Authenticator) authenticate(ctx context.Context) (context.Context, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	keys := md.Get(APIKeyHeader)
	if len(keys) == 0 || keys[0] == "" {
		return nil, status.Error(codes.Unauthenticated, "missing "+APIKeyHeader)
	}
	gw, err := a.Lookup(ctx, keys[0])
	switch {
	case errors.Is(err, ErrUnauthorized):
		return nil, status.Error(codes.Unauthenticated, "invalid api key")
	case err != nil:
		return nil, status.Error(codes.Unavailable, "auth backend unavailable")
	}
	return context.WithValue(ctx, ctxKey{}, gw), nil
}

func isPublic(fullMethod string) bool {
	return strings.HasPrefix(fullMethod, "/grpc.health.") || strings.HasPrefix(fullMethod, "/grpc.reflection.")
}

func (a *Authenticator) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		if isPublic(info.FullMethod) {
			return next(ctx, req)
		}
		ctx, err := a.authenticate(ctx)
		if err != nil {
			return nil, err
		}
		return next(ctx, req)
	}
}

func (a *Authenticator) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
		if isPublic(info.FullMethod) {
			return next(srv, ss)
		}
		ctx, err := a.authenticate(ss.Context())
		if err != nil {
			return err
		}
		return next(srv, &wrappedStream{ServerStream: ss, ctx: ctx})
	}
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }

package main

import (
	"context"
	"net"
	"os"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	telemetryv1 "github.com/egocucumber/telemetry-platform/gen/go/telemetry/v1"
	"github.com/egocucumber/telemetry-platform/internal/app"
	"github.com/egocucumber/telemetry-platform/internal/config"
	"github.com/egocucumber/telemetry-platform/internal/ingest"
	"github.com/egocucumber/telemetry-platform/internal/kafkax"
	"github.com/egocucumber/telemetry-platform/internal/postgres"
	"github.com/egocucumber/telemetry-platform/internal/redisx"
)

type Config struct {
	config.Common
	config.Kafka
	config.Redis
	config.Postgres

	GRPCAddr            string        `env:"GRPC_ADDR" envDefault:":8081"`
	DedupTTL            time.Duration `env:"DEDUP_TTL" envDefault:"1h"`
	AuthCacheTTL        time.Duration `env:"AUTH_CACHE_TTL" envDefault:"5m"`
	DeviceTouchInterval time.Duration `env:"DEVICE_TOUCH_INTERVAL" envDefault:"1m"`
}

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()
	cfg := config.MustLoad[Config]()
	rt, err := app.Bootstrap(ctx, "ingest", cfg.Common)
	if err != nil {
		return err
	}

	return rt.Run(ctx, func(ctx context.Context, g *errgroup.Group) error {
		pool, err := postgres.New(ctx, cfg.Postgres)
		if err != nil {
			return err
		}
		rdb, err := redisx.New(ctx, cfg.Redis)
		if err != nil {
			return err
		}
		producer, err := kafkax.NewProducer(cfg.Kafka, "ingest")
		if err != nil {
			return err
		}
		if err := kafkax.EnsureTopics(ctx, producer.Client, kafkax.DefaultTopics()); err != nil {
			return err
		}
		rt.Admin.AddCheck("postgres", postgres.Ping(pool))
		rt.Admin.AddCheck("redis", redisx.Ping(rdb))
		rt.Admin.AddCheck("kafka", kafkax.Ping(producer.Client))

		auth := ingest.NewAuthenticator(pool, cfg.AuthCacheTTL)
		devices := ingest.NewDeviceRegistry(pool, cfg.DeviceTouchInterval)
		server := ingest.NewServer(producer, rdb, devices, rt.Log, cfg.DedupTTL)

		srv := grpc.NewServer(
			grpc.StatsHandler(otelgrpc.NewServerHandler()),
			grpc.ChainUnaryInterceptor(auth.UnaryInterceptor()),
			grpc.ChainStreamInterceptor(auth.StreamInterceptor()),
			grpc.MaxRecvMsgSize(4<<20),
			grpc.KeepaliveParams(keepalive.ServerParameters{Time: 30 * time.Second, Timeout: 10 * time.Second}),
			grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
		)
		telemetryv1.RegisterIngestServiceServer(srv, server)
		hs := health.NewServer()
		hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
		grpc_health_v1.RegisterHealthServer(srv, hs)
		reflection.Register(srv)

		lis, err := net.Listen("tcp", cfg.GRPCAddr)
		if err != nil {
			return err
		}
		rt.Log.Info("grpc listening", "addr", cfg.GRPCAddr)

		g.Go(func() error { return srv.Serve(lis) })
		g.Go(func() error {
			<-ctx.Done()
			hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
			srv.GracefulStop()
			producer.Close()
			_ = rdb.Close()
			pool.Close()
			return nil
		})
		return nil
	})
}

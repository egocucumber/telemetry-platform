package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	queryv1 "github.com/egocucumber/telemetry-platform/gen/go/query/v1"
	"github.com/egocucumber/telemetry-platform/internal/app"
	"github.com/egocucumber/telemetry-platform/internal/config"
	"github.com/egocucumber/telemetry-platform/internal/postgres"
	"github.com/egocucumber/telemetry-platform/internal/query"
	"github.com/egocucumber/telemetry-platform/internal/redisx"
)

type Config struct {
	config.Common
	config.Redis
	config.Postgres

	HTTPAddr string `env:"HTTP_ADDR" envDefault:":8080"`
	GRPCAddr string `env:"GRPC_ADDR" envDefault:":8082"`
}

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()
	cfg := config.MustLoad[Config]()
	rt, err := app.Bootstrap(ctx, "query", cfg.Common)
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
		rt.Admin.AddCheck("postgres", postgres.Ping(pool))
		rt.Admin.AddCheck("redis", redisx.Ping(rdb))

		svc := query.NewService(query.NewRepo(pool), rdb, rt.Log)

		httpSrv := &http.Server{
			Addr:              cfg.HTTPAddr,
			Handler:           svc.Router(),
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       15 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       60 * time.Second,
		}
		g.Go(func() error {
			rt.Log.Info("http listening", "addr", cfg.HTTPAddr)
			if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return err
			}
			return nil
		})

		grpcSrv := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
		queryv1.RegisterQueryServiceServer(grpcSrv, query.NewGRPC(svc))
		hs := health.NewServer()
		hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
		grpc_health_v1.RegisterHealthServer(grpcSrv, hs)
		reflection.Register(grpcSrv)
		lis, err := net.Listen("tcp", cfg.GRPCAddr)
		if err != nil {
			return err
		}
		g.Go(func() error {
			rt.Log.Info("grpc listening", "addr", cfg.GRPCAddr)
			return grpcSrv.Serve(lis)
		})

		g.Go(func() error {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = httpSrv.Shutdown(shutdownCtx)
			grpcSrv.GracefulStop()
			_ = rdb.Close()
			pool.Close()
			return nil
		})
		return nil
	})
}

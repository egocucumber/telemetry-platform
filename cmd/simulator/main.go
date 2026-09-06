package main

import (
	"context"
	"os"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/egocucumber/telemetry-platform/internal/app"
	"github.com/egocucumber/telemetry-platform/internal/config"
	"github.com/egocucumber/telemetry-platform/internal/simulator"
)

type Config struct {
	config.Common

	IngestAddr   string        `env:"INGEST_ADDR" envDefault:"localhost:8081"`
	APIKey       string        `env:"API_KEY" envDefault:"demo-gateway-key"`
	DevicePrefix string        `env:"DEVICE_PREFIX" envDefault:"cnc"`
	Devices      int           `env:"DEVICES" envDefault:"20"`
	Interval     time.Duration `env:"INTERVAL" envDefault:"1s"`
	IncidentRate float64       `env:"INCIDENT_RATE" envDefault:"0.002"`
}

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()
	cfg := config.MustLoad[Config]()
	rt, err := app.Bootstrap(ctx, "simulator", cfg.Common)
	if err != nil {
		return err
	}

	return rt.Run(ctx, func(ctx context.Context, g *errgroup.Group) error {
		conn, err := grpc.NewClient(cfg.IngestAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 30 * time.Second, Timeout: 10 * time.Second, PermitWithoutStream: true}),
			grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`),
		)
		if err != nil {
			return err
		}
		fleet := simulator.New(conn, simulator.Config{
			APIKey:       cfg.APIKey,
			DevicePrefix: cfg.DevicePrefix,
			Devices:      cfg.Devices,
			Interval:     cfg.Interval,
			IncidentRate: cfg.IncidentRate,
		}, rt.Log)
		g.Go(func() error {
			defer func() { _ = conn.Close() }()
			return fleet.Run(ctx)
		})
		return nil
	})
}

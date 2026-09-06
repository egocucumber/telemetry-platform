package main

import (
	"context"
	"os"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/egocucumber/telemetry-platform/internal/app"
	"github.com/egocucumber/telemetry-platform/internal/config"
	"github.com/egocucumber/telemetry-platform/internal/kafkax"
	"github.com/egocucumber/telemetry-platform/internal/postgres"
	"github.com/egocucumber/telemetry-platform/internal/processor"
	"github.com/egocucumber/telemetry-platform/internal/redisx"
)

type Config struct {
	config.Common
	config.Kafka
	config.Redis
	config.Postgres

	ConsumerGroup   string        `env:"CONSUMER_GROUP" envDefault:"processor"`
	BatchSize       int           `env:"BATCH_SIZE" envDefault:"2000"`
	WindowSize      time.Duration `env:"WINDOW_SIZE" envDefault:"1m"`
	LatestTTL       time.Duration `env:"LATEST_TTL" envDefault:"24h"`
	RulesReloadEach time.Duration `env:"RULES_RELOAD_INTERVAL" envDefault:"1m"`
}

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()
	cfg := config.MustLoad[Config]()
	rt, err := app.Bootstrap(ctx, "processor", cfg.Common)
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
		cl, err := kafkax.NewConsumer(cfg.Kafka, "processor", cfg.ConsumerGroup, kafkax.TopicTelemetryRaw)
		if err != nil {
			return err
		}
		if err := kafkax.EnsureTopics(ctx, cl.Client, kafkax.DefaultTopics()); err != nil {
			return err
		}
		rt.Admin.AddCheck("postgres", postgres.Ping(pool))
		rt.Admin.AddCheck("redis", redisx.Ping(rdb))
		rt.Admin.AddCheck("kafka", kafkax.Ping(cl.Client))

		pipe := processor.NewPipeline(cl, pool, rdb, processor.PGRuleSource{Pool: pool}, rt.Log, cfg.WindowSize, cfg.LatestTTL)
		if err := pipe.ReloadRules(ctx); err != nil {
			return err
		}

		g.Go(func() error { return pipe.WatchRules(ctx, cfg.RulesReloadEach) })
		g.Go(func() error { pipe.EvictStale(ctx, 5*time.Minute, time.Hour); return nil })
		g.Go(func() error {
			defer func() {
				cl.Close()
				_ = rdb.Close()
				pool.Close()
			}()
			return kafkax.NewBatchConsumer(cl, rt.Log, cfg.BatchSize).Run(ctx, pipe.Handle)
		})
		return nil
	})
}

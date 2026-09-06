package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/egocucumber/telemetry-platform/internal/config"
	"github.com/egocucumber/telemetry-platform/internal/postgres"
	"github.com/egocucumber/telemetry-platform/migrations"
)

type Config struct {
	config.Postgres
	Timeout time.Duration `env:"MIGRATE_TIMEOUT" envDefault:"2m"`
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("migrate failed", "err", err)
		os.Exit(1)
	}
	log.Info("migrations applied")
}

func run(log *slog.Logger) error {
	cfg := config.MustLoad[Config]()
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	pool, err := connectWithRetry(ctx, log, cfg.Postgres)
	if err != nil {
		return err
	}
	defer pool.Close()

	return postgres.Migrate(ctx, pool, migrations.FS)
}

func connectWithRetry(ctx context.Context, log *slog.Logger, cfg config.Postgres) (*pgxpool.Pool, error) {
	for attempt := 1; ; attempt++ {
		pool, err := postgres.New(ctx, cfg)
		if err == nil {
			return pool, nil
		}
		log.Warn("database not ready", "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("gave up waiting for database: %w", err)
		case <-time.After(2 * time.Second):
		}
	}
}

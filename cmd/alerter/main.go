package main

import (
	"context"
	"net/http"
	"os"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/sync/errgroup"

	"github.com/egocucumber/telemetry-platform/internal/alerter"
	"github.com/egocucumber/telemetry-platform/internal/app"
	"github.com/egocucumber/telemetry-platform/internal/config"
	"github.com/egocucumber/telemetry-platform/internal/kafkax"
	"github.com/egocucumber/telemetry-platform/internal/postgres"
	"github.com/egocucumber/telemetry-platform/internal/redisx"
)

type Config struct {
	config.Common
	config.Kafka
	config.Redis
	config.Postgres

	ConsumerGroup string        `env:"CONSUMER_GROUP" envDefault:"alerter"`
	Cooldown      time.Duration `env:"ALERT_COOLDOWN" envDefault:"5m"`

	WebhookURL     string `env:"WEBHOOK_URL"`
	WebhookSecret  string `env:"WEBHOOK_SECRET"`
	TelegramToken  string `env:"TELEGRAM_BOT_TOKEN"`
	TelegramChatID string `env:"TELEGRAM_CHAT_ID"`
}

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()
	cfg := config.MustLoad[Config]()
	rt, err := app.Bootstrap(ctx, "alerter", cfg.Common)
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
		cl, err := kafkax.NewConsumer(cfg.Kafka, "alerter", cfg.ConsumerGroup, kafkax.TopicAlerts)
		if err != nil {
			return err
		}
		if err := kafkax.EnsureTopics(ctx, cl.Client, kafkax.DefaultTopics()); err != nil {
			return err
		}
		rt.Admin.AddCheck("postgres", postgres.Ping(pool))
		rt.Admin.AddCheck("redis", redisx.Ping(rdb))
		rt.Admin.AddCheck("kafka", kafkax.Ping(cl.Client))

		httpClient := &http.Client{Timeout: 10 * time.Second, Transport: otelhttp.NewTransport(http.DefaultTransport)}
		notifiers := []alerter.Notifier{alerter.LogNotifier{Log: rt.Log}}
		if cfg.WebhookURL != "" {
			notifiers = append(notifiers, alerter.WebhookNotifier{URL: cfg.WebhookURL, Secret: cfg.WebhookSecret, Client: httpClient})
		}
		if cfg.TelegramToken != "" && cfg.TelegramChatID != "" {
			notifiers = append(notifiers, alerter.TelegramNotifier{Token: cfg.TelegramToken, ChatID: cfg.TelegramChatID, Client: httpClient})
		}
		rt.Log.Info("notifiers configured", "count", len(notifiers))

		a := alerter.New(cl, pool, rdb, notifiers, cfg.Cooldown, rt.Log)
		g.Go(func() error {
			defer func() {
				cl.Close()
				_ = rdb.Close()
				pool.Close()
			}()
			return kafkax.NewBatchConsumer(cl, rt.Log, 500).Run(ctx, a.Handle)
		})
		return nil
	})
}

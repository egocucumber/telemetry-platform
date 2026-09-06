package app

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/egocucumber/telemetry-platform/internal/config"
	"github.com/egocucumber/telemetry-platform/internal/observability"
)

type Runtime struct {
	Log   *slog.Logger
	Admin *observability.AdminServer
	cfg   config.Common
	flush func(context.Context) error
}

func Bootstrap(ctx context.Context, name string, cfg config.Common) (*Runtime, error) {
	if cfg.ServiceName != "" {
		name = cfg.ServiceName
	}
	log := observability.NewLogger(name, cfg.Env, cfg.LogLevel, cfg.LogFormat)
	flush, err := observability.SetupTracing(ctx, name, cfg.Env, cfg.OTLPEndpoint, cfg.TraceSample)
	if err != nil {
		return nil, err
	}
	return &Runtime{
		Log:   log,
		Admin: observability.NewAdminServer(cfg.AdminAddr, log),
		cfg:   cfg,
		flush: flush,
	}, nil
}

func (r *Runtime) Run(ctx context.Context, body func(ctx context.Context, g *errgroup.Group) error) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return r.Admin.Run(gctx) })

	if err := body(gctx, g); err != nil {
		r.Log.Error("startup failed", "err", err)
		stop()
		_ = g.Wait()
		return err
	}
	r.Admin.SetReady(true)
	r.Log.Info("service ready")

	<-gctx.Done()
	r.Admin.SetReady(false)
	r.Log.Info("shutting down", "reason", context.Cause(gctx))

	done := make(chan error, 1)
	go func() { done <- g.Wait() }()
	select {
	case err := <-done:
		r.finish()
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		return nil
	case <-time.After(r.cfg.ShutdownTimeout):
		r.finish()
		return errors.New("shutdown timed out")
	}
}

func (r *Runtime) finish() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.flush(ctx); err != nil {
		r.Log.Warn("flush traces", "err", err)
	}
	r.Log.Info("bye")
}

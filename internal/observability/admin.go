package observability

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Checker func(ctx context.Context) error

type AdminServer struct {
	srv    *http.Server
	log    *slog.Logger
	ready  atomic.Bool
	mu     sync.RWMutex
	checks map[string]Checker
}

func NewAdminServer(addr string, log *slog.Logger) *AdminServer {
	a := &AdminServer{log: log, checks: map[string]Checker{}}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", a.handleReady)
	a.srv = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return a
}

func (a *AdminServer) AddCheck(name string, c Checker) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.checks[name] = c
}

func (a *AdminServer) SetReady(v bool) { a.ready.Store(v) }

func (a *AdminServer) handleReady(w http.ResponseWriter, r *http.Request) {
	if !a.ready.Load() {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	a.mu.RLock()
	defer a.mu.RUnlock()
	for name, c := range a.checks {
		if err := c(ctx); err != nil {
			a.log.WarnContext(ctx, "readiness check failed", "check", name, "err", err)
			http.Error(w, name+": "+err.Error(), http.StatusServiceUnavailable)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready"))
}

func (a *AdminServer) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		if err := a.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return a.srv.Shutdown(shutdownCtx)
	}
}

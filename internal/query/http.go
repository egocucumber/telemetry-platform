package query

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/egocucumber/telemetry-platform/internal/domain"
	"github.com/egocucumber/telemetry-platform/internal/observability"
	"github.com/egocucumber/telemetry-platform/internal/redisx"
)

var metricHTTP = observability.Histogram("query_http_seconds", "HTTP handler latency.", "route", "status")

type Service struct {
	repo *Repo
	rdb  *redis.Client
	log  *slog.Logger
}

func NewService(repo *Repo, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{repo: repo, rdb: rdb, log: log}
}

func (s *Service) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(10 * time.Second))
	r.Use(s.metrics)

	r.Route("/v1", func(r chi.Router) {
		r.Get("/devices", s.listDevices)
		r.Get("/devices/{id}", s.getDevice)
		r.Get("/devices/{id}/latest", s.getLatest)
		r.Get("/devices/{id}/history", s.getHistory)
		r.Get("/rules", s.listRules)
		r.Post("/rules", s.createRule)
		r.Delete("/rules/{id}", s.deleteRule)
		r.Get("/alerts", s.listAlerts)
	})
	return otelhttp.NewHandler(r, "query-api", otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
		return r.Method + " " + r.URL.Path
	}))
}

func (s *Service) metrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)
		route := chi.RouteContext(r.Context()).RoutePattern()
		metricHTTP.WithLabelValues(route, strconv.Itoa(ww.Status())).Observe(time.Since(start).Seconds())
	})
}

type errorResponse struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Service) writeError(ctx context.Context, w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "not found"})
	case errors.Is(err, errBadRequest):
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
	case errors.Is(err, context.DeadlineExceeded):
		writeJSON(w, http.StatusGatewayTimeout, errorResponse{Error: "timeout"})
	default:
		s.log.ErrorContext(ctx, "handler error", "err", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "internal error"})
	}
}

var errBadRequest = errors.New("bad request")

func badRequest(msg string) error { return errors.Join(errBadRequest, errors.New(msg)) }

type DeviceJSON struct {
	ID        string    `json:"id"`
	GatewayID string    `json:"gateway_id"`
	Name      string    `json:"name,omitempty"`
	LastSeen  time.Time `json:"last_seen"`
	CreatedAt time.Time `json:"created_at"`
}

func toDeviceJSON(d domain.Device) DeviceJSON {
	return DeviceJSON{ID: d.ID, GatewayID: d.GatewayID, Name: d.Name, LastSeen: d.LastSeen, CreatedAt: d.CreatedAt}
}

func (s *Service) listDevices(w http.ResponseWriter, r *http.Request) {
	limit := queryInt(r, "limit", 100, 1, 1000)
	devs, err := s.repo.ListDevices(r.Context(), limit)
	if err != nil {
		s.writeError(r.Context(), w, err)
		return
	}
	out := make([]DeviceJSON, 0, len(devs))
	for _, d := range devs {
		out = append(out, toDeviceJSON(d))
	}
	writeJSON(w, http.StatusOK, map[string]any{"devices": out})
}

func (s *Service) getDevice(w http.ResponseWriter, r *http.Request) {
	d, err := s.repo.GetDevice(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		s.writeError(r.Context(), w, err)
		return
	}
	writeJSON(w, http.StatusOK, toDeviceJSON(d))
}

type LatestResponse struct {
	DeviceID string                        `json:"device_id"`
	Latest   map[string]redisx.LatestValue `json:"latest"`
	Window   map[string]redisx.WindowValue `json:"last_window,omitempty"`
}

func (s *Service) getLatest(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	latest, err := redisx.GetLatest(r.Context(), s.rdb, id)
	if err != nil {
		s.writeError(r.Context(), w, err)
		return
	}
	if len(latest) == 0 {
		s.writeError(r.Context(), w, ErrNotFound)
		return
	}
	windows, err := redisx.GetWindows(r.Context(), s.rdb, id)
	if err != nil {
		s.writeError(r.Context(), w, err)
		return
	}
	writeJSON(w, http.StatusOK, LatestResponse{DeviceID: id, Latest: latest, Window: windows})
}

func (s *Service) getHistory(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	metric := q.Get("metric")
	if metric == "" {
		s.writeError(r.Context(), w, badRequest("metric is required"))
		return
	}
	to := time.Now()
	from := to.Add(-time.Hour)
	var err error
	if v := q.Get("from"); v != "" {
		if from, err = time.Parse(time.RFC3339, v); err != nil {
			s.writeError(r.Context(), w, badRequest("from must be RFC3339"))
			return
		}
	}
	if v := q.Get("to"); v != "" {
		if to, err = time.Parse(time.RFC3339, v); err != nil {
			s.writeError(r.Context(), w, badRequest("to must be RFC3339"))
			return
		}
	}
	step := time.Duration(queryInt(r, "step", 60, 1, 86400)) * time.Second

	points, err := s.repo.History(r.Context(), chi.URLParam(r, "id"), metric, from, to, step)
	if err != nil {
		s.writeError(r.Context(), w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"device_id": chi.URLParam(r, "id"), "metric": metric, "step_seconds": int(step.Seconds()), "points": points})
}

type RuleJSON struct {
	ID         int64   `json:"id,omitempty"`
	Name       string  `json:"name"`
	DeviceID   string  `json:"device_id,omitempty"`
	Metric     string  `json:"metric"`
	Op         string  `json:"op"`
	Threshold  float64 `json:"threshold"`
	ForSeconds int     `json:"for_seconds"`
	Severity   string  `json:"severity"`
	Enabled    bool    `json:"enabled"`
}

func toRuleJSON(r domain.Rule) RuleJSON {
	return RuleJSON{ID: r.ID, Name: r.Name, DeviceID: r.DeviceID, Metric: r.Metric, Op: string(r.Op),
		Threshold: r.Threshold, ForSeconds: int(r.For.Seconds()), Severity: string(r.Severity), Enabled: r.Enabled}
}

func (s *Service) listRules(w http.ResponseWriter, r *http.Request) {
	rules, err := s.repo.ListRules(r.Context())
	if err != nil {
		s.writeError(r.Context(), w, err)
		return
	}
	out := make([]RuleJSON, 0, len(rules))
	for _, rl := range rules {
		out = append(out, toRuleJSON(rl))
	}
	writeJSON(w, http.StatusOK, map[string]any{"rules": out})
}

func (s *Service) createRule(w http.ResponseWriter, r *http.Request) {
	var in RuleJSON
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&in); err != nil {
		s.writeError(r.Context(), w, badRequest("invalid json"))
		return
	}
	rl, err := validateRule(in)
	if err != nil {
		s.writeError(r.Context(), w, err)
		return
	}
	created, err := s.repo.CreateRule(r.Context(), rl)
	if err != nil {
		s.writeError(r.Context(), w, err)
		return
	}
	s.publishRulesChanged(r.Context())
	writeJSON(w, http.StatusCreated, toRuleJSON(created))
}

func (s *Service) deleteRule(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		s.writeError(r.Context(), w, badRequest("id must be an integer"))
		return
	}
	if err := s.repo.DeleteRule(r.Context(), id); err != nil {
		s.writeError(r.Context(), w, err)
		return
	}
	s.publishRulesChanged(r.Context())
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) listAlerts(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	alerts, err := s.repo.ListAlerts(r.Context(), q.Get("device"), q.Get("state"), queryInt(r, "limit", 50, 1, 500))
	if err != nil {
		s.writeError(r.Context(), w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"alerts": alerts})
}

func (s *Service) publishRulesChanged(ctx context.Context) {
	if err := s.rdb.Publish(ctx, redisx.ChannelRulesChanged, time.Now().Unix()).Err(); err != nil {
		s.log.WarnContext(ctx, "publish rules.changed", "err", err)
	}
}

func validateRule(in RuleJSON) (domain.Rule, error) {
	if in.Name == "" || in.Metric == "" {
		return domain.Rule{}, badRequest("name and metric are required")
	}
	op := domain.Op(in.Op)
	switch op {
	case domain.OpGT, domain.OpGE, domain.OpLT, domain.OpLE:
	default:
		return domain.Rule{}, badRequest("op must be one of gt, ge, lt, le")
	}
	sev := domain.Severity(in.Severity)
	switch sev {
	case domain.SeverityInfo, domain.SeverityWarning, domain.SeverityCritical:
	default:
		return domain.Rule{}, badRequest("severity must be one of info, warning, critical")
	}
	if in.ForSeconds < 0 {
		return domain.Rule{}, badRequest("for_seconds must be >= 0")
	}
	return domain.Rule{
		Name: in.Name, DeviceID: in.DeviceID, Metric: in.Metric, Op: op, Threshold: in.Threshold,
		For: time.Duration(in.ForSeconds) * time.Second, Severity: sev, Enabled: in.Enabled,
	}, nil
}

func queryInt(r *http.Request, key string, def, lo, hi int) int {
	v := r.URL.Query().Get(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return max(lo, min(hi, n))
}

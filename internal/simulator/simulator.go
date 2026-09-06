package simulator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	telemetryv1 "github.com/egocucumber/telemetry-platform/gen/go/telemetry/v1"
	"github.com/egocucumber/telemetry-platform/internal/ingest"
	"github.com/egocucumber/telemetry-platform/internal/observability"
)

var (
	metricSent = observability.Counter("simulator_measurements_total", "Measurements by ack status.", "status")
	metricRTT  = observability.Histogram("simulator_roundtrip_seconds", "Batch send → ack latency.")
)

type Config struct {
	APIKey       string
	DevicePrefix string
	Devices      int
	Interval     time.Duration
	IncidentRate float64
}

type Fleet struct {
	cfg     Config
	client  telemetryv1.IngestServiceClient
	log     *slog.Logger
	devices []*device
	rng     *rand.Rand
}

type device struct {
	id           string
	seq          uint64
	temp         float64
	vibration    float64
	load         float64
	rpm          float64
	coolant      float64
	incident     string
	incidentLeft int
}

func New(conn *grpc.ClientConn, cfg Config, log *slog.Logger) *Fleet {
	f := &Fleet{
		cfg:    cfg,
		client: telemetryv1.NewIngestServiceClient(conn),
		log:    log,
		rng:    rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 42)),
	}
	for i := range cfg.Devices {
		f.devices = append(f.devices, &device{
			id:        fmt.Sprintf("%s-%03d", cfg.DevicePrefix, i+1),
			temp:      55 + f.rng.Float64()*10,
			vibration: 2 + f.rng.Float64(),
			load:      40 + f.rng.Float64()*30,
			rpm:       8000 + f.rng.Float64()*2000,
			coolant:   3 + f.rng.Float64()*0.5,
		})
	}
	return f
}

func (f *Fleet) Run(ctx context.Context) error {
	ticker := time.NewTicker(f.cfg.Interval)
	defer ticker.Stop()

	var pending *telemetryv1.PublishRequest
	backoff := time.Second
	for {
		stream, err := f.open(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			f.log.WarnContext(ctx, "open stream", "err", err, "retry_in", backoff)
			if !sleep(ctx, backoff) {
				return nil
			}
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		backoff = time.Second
		f.log.InfoContext(ctx, "stream open", "devices", len(f.devices), "interval", f.cfg.Interval)

		err = f.loop(ctx, stream, ticker, &pending)
		if ctx.Err() != nil {
			_ = stream.CloseSend()
			return nil
		}
		f.log.WarnContext(ctx, "stream broken, reconnecting", "err", err)
	}
}

func (f *Fleet) open(ctx context.Context) (telemetryv1.IngestService_PublishClient, error) {
	ctx = metadata.AppendToOutgoingContext(ctx, ingest.APIKeyHeader, f.cfg.APIKey)
	return f.client.Publish(ctx)
}

func (f *Fleet) loop(ctx context.Context, stream telemetryv1.IngestService_PublishClient, ticker *time.Ticker, pending **telemetryv1.PublishRequest) error {
	for {
		if *pending == nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}
			*pending = f.tick(time.Now())
		}

		start := time.Now()
		if err := stream.Send(*pending); err != nil {
			return fmt.Errorf("send: %w", err)
		}
		resp, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("recv: %w", err)
		}
		metricRTT.WithLabelValues().Observe(time.Since(start).Seconds())

		var rejected int
		for _, ack := range resp.GetAcks() {
			switch ack.GetStatus() {
			case telemetryv1.AckStatus_ACK_STATUS_OK:
				metricSent.WithLabelValues("ok").Inc()
			case telemetryv1.AckStatus_ACK_STATUS_DUPLICATE:
				metricSent.WithLabelValues("duplicate").Inc()
			default:
				metricSent.WithLabelValues("rejected").Inc()
				rejected++
				if rejected == 1 {
					f.log.WarnContext(ctx, "measurement rejected", "device", ack.GetDeviceId(), "seq", ack.GetSeq(), "err", ack.GetError())
				}
			}
		}
		*pending = nil
	}
}

func (f *Fleet) tick(now time.Time) *telemetryv1.PublishRequest {
	req := &telemetryv1.PublishRequest{Measurements: make([]*telemetryv1.Measurement, 0, len(f.devices)*5)}
	ts := timestamppb.New(now)
	for _, d := range f.devices {
		f.step(d)
		for metric, v := range map[string]float64{
			"temperature":  d.temp,
			"vibration":    d.vibration,
			"spindle_load": d.load,
			"rpm":          d.rpm,
			"coolant_bar":  d.coolant,
		} {
			d.seq++
			req.Measurements = append(req.Measurements, &telemetryv1.Measurement{
				DeviceId: d.id, Metric: metric, Value: round(v), Ts: ts, Seq: d.seq,
				Tags: map[string]string{"line": "A"},
			})
		}
	}
	return req
}

func (f *Fleet) step(d *device) {
	n := f.rng.NormFloat64

	d.temp += (60-d.temp)*0.05 + n()*0.6
	d.vibration += (2.5-d.vibration)*0.1 + n()*0.2
	d.load += (55-d.load)*0.05 + n()*3
	d.rpm += (9000-d.rpm)*0.1 + n()*50
	d.coolant += (3.2-d.coolant)*0.1 + n()*0.05

	if d.incident == "" && f.rng.Float64() < f.cfg.IncidentRate {
		switch f.rng.IntN(3) {
		case 0:
			d.incident, d.incidentLeft = "overheat", 40+f.rng.IntN(30)
		case 1:
			d.incident, d.incidentLeft = "vibration", 10+f.rng.IntN(15)
		default:
			d.incident, d.incidentLeft = "coolant", 25+f.rng.IntN(20)
		}
	}
	switch d.incident {
	case "overheat":
		d.temp += 3 + n()
		d.load += 4
	case "vibration":
		d.vibration += 2.5 + math.Abs(n())
	case "coolant":
		d.coolant -= 0.35
	}
	if d.incident != "" {
		d.incidentLeft--
		if d.incidentLeft <= 0 {
			d.incident = ""
		}
	}
	d.load = max(0, min(120, d.load))
	d.coolant = max(0, d.coolant)
	d.vibration = max(0, d.vibration)
}

func round(v float64) float64 { return math.Round(v*100) / 100 }

func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

var ErrStopped = errors.New("simulator stopped")

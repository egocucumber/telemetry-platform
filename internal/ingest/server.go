package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	telemetryv1 "github.com/egocucumber/telemetry-platform/gen/go/telemetry/v1"
	"github.com/egocucumber/telemetry-platform/internal/domain"
	"github.com/egocucumber/telemetry-platform/internal/kafkax"
	"github.com/egocucumber/telemetry-platform/internal/observability"
	"github.com/egocucumber/telemetry-platform/internal/redisx"
)

const MaxBatch = 1000

var (
	metricAccepted = observability.Counter("ingest_measurements_total", "Measurements by outcome.", "outcome")
	metricLatency  = observability.Histogram("ingest_batch_seconds", "Batch processing latency.", "rpc")
	metricBatch    = observability.Gauge("ingest_batch_size", "Size of the last processed batch.", "rpc")
)

type Server struct {
	telemetryv1.UnimplementedIngestServiceServer

	producer *kafkax.Client
	rdb      *redis.Client
	devices  *DeviceRegistry
	log      *slog.Logger
	tracer   trace.Tracer
	dedupTTL time.Duration
	now      func() time.Time
}

func NewServer(producer *kafkax.Client, rdb *redis.Client, devices *DeviceRegistry, log *slog.Logger, dedupTTL time.Duration) *Server {
	return &Server{
		producer: producer,
		rdb:      rdb,
		devices:  devices,
		log:      log,
		tracer:   otel.Tracer("ingest"),
		dedupTTL: dedupTTL,
		now:      time.Now,
	}
}

func (s *Server) PublishBatch(ctx context.Context, req *telemetryv1.PublishBatchRequest) (*telemetryv1.PublishBatchResponse, error) {
	acks, err := s.process(ctx, "PublishBatch", req.GetMeasurements())
	if err != nil {
		return nil, err
	}
	return &telemetryv1.PublishBatchResponse{Acks: acks}, nil
}

func (s *Server) Publish(stream telemetryv1.IngestService_PublishServer) error {
	ctx := stream.Context()
	streamLink := trace.Link{SpanContext: trace.SpanContextFromContext(ctx)}
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}

		batchCtx, span := s.tracer.Start(ctx, "ingest.batch",
			trace.WithNewRoot(),
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithLinks(streamLink),
			trace.WithAttributes(attribute.Int("batch.size", len(req.GetMeasurements()))),
		)
		acks, err := s.process(batchCtx, "Publish", req.GetMeasurements())
		if err != nil {
			span.RecordError(err)
			span.SetStatus(otelcodes.Error, err.Error())
			span.End()
			return err
		}
		span.End()
		if err := stream.Send(&telemetryv1.PublishResponse{Acks: acks}); err != nil {
			return err
		}
	}
}

func (s *Server) process(ctx context.Context, rpc string, in []*telemetryv1.Measurement) ([]*telemetryv1.PublishAck, error) {
	start := time.Now()
	defer func() { metricLatency.WithLabelValues(rpc).Observe(time.Since(start).Seconds()) }()
	metricBatch.WithLabelValues(rpc).Set(float64(len(in)))

	gw, ok := GatewayFromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Internal, "gateway missing from context")
	}
	if len(in) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty batch")
	}
	if len(in) > MaxBatch {
		return nil, status.Errorf(codes.InvalidArgument, "batch exceeds %d measurements", MaxBatch)
	}

	now := s.now()
	acks := make([]*telemetryv1.PublishAck, len(in))

	candidates := make([]domain.Measurement, 0, len(in))
	idx := make([]int, 0, len(in))

	for i, pm := range in {
		m := fromProto(pm)
		acks[i] = &telemetryv1.PublishAck{DeviceId: m.DeviceID, Seq: m.Seq}
		if err := m.Validate(now); err != nil {
			s.reject(acks[i], err)
			continue
		}
		if err := s.devices.Ensure(ctx, gw.ID, m.DeviceID, now); err != nil {
			var owned ErrDeviceOwnedByOther
			if errors.As(err, &owned) {
				s.reject(acks[i], err)
				continue
			}
			return nil, status.Error(codes.Unavailable, "device registry unavailable")
		}
		candidates = append(candidates, m)
		idx = append(idx, i)
	}
	if len(candidates) == 0 {
		return acks, nil
	}

	keys := make([]string, len(candidates))
	for i, m := range candidates {
		keys[i] = m.IdempotencyKey()
	}
	fresh, err := redisx.MarkSeen(ctx, s.rdb, keys, s.dedupTTL)
	if err != nil {
		s.log.ErrorContext(ctx, "dedup", "err", err)
		return nil, status.Error(codes.Unavailable, "dedup store unavailable")
	}

	records := make([]*kgo.Record, 0, len(candidates))
	recIdx := make([]int, 0, len(candidates))
	for i, m := range candidates {
		if !fresh[i] {
			acks[idx[i]].Status = telemetryv1.AckStatus_ACK_STATUS_DUPLICATE
			metricAccepted.WithLabelValues("duplicate").Inc()
			continue
		}
		payload, err := proto.Marshal(&telemetryv1.TelemetryEvent{
			Measurement: in[idx[i]],
			GatewayId:   gw.ID,
			IngestedAt:  timestamppb.New(now),
		})
		if err != nil {
			return nil, status.Error(codes.Internal, "marshal event")
		}
		records = append(records, &kgo.Record{
			Topic:   kafkax.TopicTelemetryRaw,
			Key:     []byte(m.DeviceID),
			Value:   payload,
			Context: ctx,
		})
		recIdx = append(recIdx, idx[i])
	}
	if len(records) == 0 {
		return acks, nil
	}

	results := s.producer.ProduceSync(ctx, records...)
	for i, r := range results {
		ack := acks[recIdx[i]]
		if r.Err != nil {
			s.log.ErrorContext(ctx, "produce", "device", ack.DeviceId, "seq", ack.Seq, "err", r.Err)
			ack.Status = telemetryv1.AckStatus_ACK_STATUS_REJECTED
			ack.Error = "broker unavailable, retry"
			metricAccepted.WithLabelValues("produce_error").Inc()

			_ = s.rdb.Del(ctx, redisx.KeyDedup(fmt.Sprintf("%s:%d", ack.DeviceId, ack.Seq))).Err()
			continue
		}
		ack.Status = telemetryv1.AckStatus_ACK_STATUS_OK
		metricAccepted.WithLabelValues("ok").Inc()
	}
	return acks, nil
}

func (s *Server) reject(ack *telemetryv1.PublishAck, err error) {
	ack.Status = telemetryv1.AckStatus_ACK_STATUS_REJECTED
	ack.Error = err.Error()
	metricAccepted.WithLabelValues("rejected").Inc()
}

func fromProto(pm *telemetryv1.Measurement) domain.Measurement {
	m := domain.Measurement{
		DeviceID: pm.GetDeviceId(),
		Metric:   pm.GetMetric(),
		Value:    pm.GetValue(),
		Seq:      pm.GetSeq(),
		Tags:     pm.GetTags(),
	}
	if pm.GetTs() != nil {
		m.TS = pm.GetTs().AsTime()
	}
	return m
}

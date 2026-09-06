package kafkax

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel"

	"github.com/egocucumber/telemetry-platform/internal/config"
)

const (
	TopicTelemetryRaw = "telemetry.raw"
	TopicAlerts       = "telemetry.alerts"
	TopicDLQ          = "telemetry.dlq"
)

type TopicSpec struct {
	Name       string
	Partitions int32
	Config     map[string]*string
}

func DefaultTopics() []TopicSpec {
	retention := func(d time.Duration) *string {
		s := fmt.Sprintf("%d", d.Milliseconds())
		return &s
	}
	return []TopicSpec{
		{Name: TopicTelemetryRaw, Partitions: 6, Config: map[string]*string{"retention.ms": retention(24 * time.Hour)}},
		{Name: TopicAlerts, Partitions: 3, Config: map[string]*string{"retention.ms": retention(7 * 24 * time.Hour)}},
		{Name: TopicDLQ, Partitions: 1, Config: map[string]*string{"retention.ms": retention(7 * 24 * time.Hour)}},
	}
}

type Client struct {
	*kgo.Client
	Tracer *kotel.Tracer
}

func baseOpts(cfg config.Kafka, clientID string) ([]kgo.Opt, *kotel.Tracer) {
	tracer := kotel.NewTracer(kotel.TracerPropagator(otel.GetTextMapPropagator()))
	k := kotel.NewKotel(kotel.WithTracer(tracer))
	if cfg.ClientID != "" {
		clientID = cfg.ClientID
	}
	return []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(clientID),
		kgo.WithHooks(k.Hooks()...),
		kgo.DialTimeout(5 * time.Second),
	}, tracer
}

func NewProducer(cfg config.Kafka, clientID string) (*Client, error) {
	opts, tracer := baseOpts(cfg, clientID)
	opts = append(opts,
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.ProduceRequestTimeout(10*time.Second),
		kgo.RecordRetries(10),
	)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	return &Client{Client: cl, Tracer: tracer}, nil
}

func NewConsumer(cfg config.Kafka, clientID, group string, topics ...string) (*Client, error) {
	opts, tracer := baseOpts(cfg, clientID)
	opts = append(opts,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(500*time.Millisecond),
		kgo.SessionTimeout(30*time.Second),
		kgo.RebalanceTimeout(60*time.Second),

		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
	)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	return &Client{Client: cl, Tracer: tracer}, nil
}

func EnsureTopics(ctx context.Context, cl *kgo.Client, specs []TopicSpec) error {
	adm := kadm.NewClient(cl)
	for _, s := range specs {
		_, err := adm.CreateTopic(ctx, s.Partitions, 1, s.Config, s.Name)
		if err != nil && !isKafkaErr(err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", s.Name, err)
		}
	}
	return nil
}

func isKafkaErr(err error, want *kerr.Error) bool {
	var ke *kerr.Error
	return errors.As(err, &ke) && ke.Code == want.Code
}

func Ping(cl *kgo.Client) func(context.Context) error {
	return func(ctx context.Context) error {
		return cl.Ping(ctx)
	}
}

func Header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

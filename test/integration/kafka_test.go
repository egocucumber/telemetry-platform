//go:build integration

package integration

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/egocucumber/telemetry-platform/internal/config"
	"github.com/egocucumber/telemetry-platform/internal/kafkax"
)

func TestKafkaAtLeastOnce(t *testing.T) {
	ctx := context.Background()
	c, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.7.1", tckafka.WithClusterID("test-cluster"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Terminate(ctx) })

	brokers, err := c.Brokers(ctx)
	require.NoError(t, err)
	cfg := config.Kafka{Brokers: brokers}

	producer, err := kafkax.NewProducer(cfg, "test-producer")
	require.NoError(t, err)
	t.Cleanup(producer.Close)

	const (
		topic = "test.at-least-once"
		group = "test-group"
		n     = 5
	)
	require.NoError(t, kafkax.EnsureTopics(ctx, producer.Client, []kafkax.TopicSpec{{Name: topic, Partitions: 1}}))

	require.NoError(t, kafkax.EnsureTopics(ctx, producer.Client, []kafkax.TopicSpec{{Name: topic, Partitions: 1}}))

	for i := range n {
		require.NoError(t, producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte("k"), Value: []byte{byte(i)}}).FirstErr())
	}

	var attempts atomic.Int32
	seen := make(chan []byte, 100)
	processed := make(chan struct{}, 1)

	consumer, err := kafkax.NewConsumer(cfg, "test-consumer", group, topic)
	require.NoError(t, err)

	runCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- kafkax.NewBatchConsumer(consumer, slog.Default(), 100).Run(runCtx, func(_ context.Context, recs []*kgo.Record) error {
			if attempts.Add(1) == 1 {
				return errors.New("simulated sink outage")
			}
			for _, r := range recs {
				seen <- r.Value
			}
			processed <- struct{}{}
			return nil
		})
	}()

	select {
	case <-processed:
	case <-time.After(45 * time.Second):
		t.Fatal("batch was never processed")
	}
	require.GreaterOrEqual(t, attempts.Load(), int32(2), "failed batch must be retried")
	require.Len(t, seen, n, "all records delivered after the retry")

	adm := kadm.NewClient(producer.Client)
	require.Eventually(t, func() bool {
		offsets, err := adm.FetchOffsets(ctx, group)
		if err != nil {
			return false
		}
		o, ok := offsets.Lookup(topic, 0)
		return ok && o.At == n
	}, 15*time.Second, 200*time.Millisecond, "offsets must be committed after a successful batch")

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	consumer.Close()

	consumer2, err := kafkax.NewConsumer(cfg, "test-consumer-2", group, topic)
	require.NoError(t, err)
	pollCtx, cancel2 := context.WithTimeout(ctx, 5*time.Second)
	defer cancel2()
	fetches := consumer2.PollFetches(pollCtx)
	consumer2.AllowRebalance()
	consumer2.Close()
	require.Empty(t, fetches.Records(), "committed records must not be redelivered")
}

package kafkax

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type BatchHandler func(ctx context.Context, recs []*kgo.Record) error

const commitTimeout = 5 * time.Second

type Consumer struct {
	cl         *Client
	log        *slog.Logger
	maxRecords int
	minBackoff time.Duration
	maxBackoff time.Duration
}

func NewBatchConsumer(cl *Client, log *slog.Logger, maxRecords int) *Consumer {
	return &Consumer{
		cl:         cl,
		log:        log,
		maxRecords: maxRecords,
		minBackoff: 200 * time.Millisecond,
		maxBackoff: 10 * time.Second,
	}
}

func (c *Consumer) Run(ctx context.Context, h BatchHandler) error {
	for {
		fetches := c.cl.PollRecords(ctx, c.maxRecords)
		if fetches.IsClientClosed() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			c.cl.AllowRebalance()
			return err
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			c.log.ErrorContext(ctx, "fetch error", "topic", topic, "partition", partition, "err", err)
		})

		recs := fetches.Records()
		if len(recs) == 0 {
			c.cl.AllowRebalance()
			continue
		}

		if err := c.handleWithRetry(ctx, h, recs); err != nil {
			c.cl.AllowRebalance()
			return err
		}

		commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
		err := c.cl.CommitRecords(commitCtx, recs...)
		cancel()
		if err != nil {
			c.log.ErrorContext(ctx, "commit offsets", "err", err, "records", len(recs))
		}
		c.cl.AllowRebalance()
	}
}

func (c *Consumer) handleWithRetry(ctx context.Context, h BatchHandler, recs []*kgo.Record) error {
	backoff := c.minBackoff
	for attempt := 1; ; attempt++ {
		err := h(ctx, recs)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.log.WarnContext(ctx, "batch failed, retrying",
			"attempt", attempt, "records", len(recs), "backoff", backoff, "err", err)

		jitter := time.Duration(rand.Int64N(int64(backoff) / 2))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff + jitter):
		}
		backoff = min(backoff*2, c.maxBackoff)
	}
}

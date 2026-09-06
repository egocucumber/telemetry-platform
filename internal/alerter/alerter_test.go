package alerter

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryStopsOnSuccess(t *testing.T) {
	calls := 0
	err := retry(context.Background(), 3, time.Millisecond, func() error {
		calls++
		if calls < 2 {
			return errors.New("flaky")
		}
		return nil
	})
	if err != nil || calls != 2 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestRetryReturnsLastError(t *testing.T) {
	want := errors.New("down")
	calls := 0
	err := retry(context.Background(), 3, time.Millisecond, func() error { calls++; return want })
	if !errors.Is(err, want) || calls != 3 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestRetryHonoursContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := retry(ctx, 5, time.Second, func() error { return errors.New("x") })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context error, got %v", err)
	}
}

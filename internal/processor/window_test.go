package processor

import (
	"testing"
	"time"
)

func TestAggregatorTumblesOnBucketChange(t *testing.T) {
	a := NewAggregator(time.Minute)
	t0 := time.Date(2026, 1, 1, 10, 0, 5, 0, time.UTC)

	if _, closed := a.Add("d1", "temp", 10, t0); closed {
		t.Fatal("first sample must not close a window")
	}
	if _, closed := a.Add("d1", "temp", 30, t0.Add(20*time.Second)); closed {
		t.Fatal("same bucket must not close")
	}
	c, closed := a.Add("d1", "temp", 99, t0.Add(time.Minute))
	if !closed {
		t.Fatal("expected the 10:00 window to close")
	}
	if c.Bucket != t0.Truncate(time.Minute) || c.Stats.Count != 2 || c.Stats.Min != 10 || c.Stats.Max != 30 || c.Stats.Avg() != 20 {
		t.Errorf("closed window wrong: %+v", c)
	}
	snap, _ := a.Snapshot("d1", "temp")
	if snap.Count != 1 || snap.Last != 99 {
		t.Errorf("new window should hold only the rolling sample: %+v", snap)
	}
}

func TestAggregatorEvict(t *testing.T) {
	a := NewAggregator(time.Minute)
	old := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	a.Add("stale", "temp", 1, old)
	a.Add("fresh", "temp", 1, old.Add(time.Hour))
	if n := a.Evict(old.Add(30 * time.Minute)); n != 1 {
		t.Fatalf("expected 1 eviction, got %d", n)
	}
	if _, ok := a.Snapshot("stale", "temp"); ok {
		t.Error("stale series should be gone")
	}
	if _, ok := a.Snapshot("fresh", "temp"); !ok {
		t.Error("fresh series should remain")
	}
}

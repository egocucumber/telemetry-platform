package processor

import (
	"sync"
	"time"
)

type Stats struct {
	Count int64
	Sum   float64
	Min   float64
	Max   float64
	Last  float64
	Since time.Time
}

func (s Stats) Avg() float64 {
	if s.Count == 0 {
		return 0
	}
	return s.Sum / float64(s.Count)
}

type Aggregator struct {
	size time.Duration

	mu      sync.Mutex
	windows map[seriesKey]*Stats
}

type seriesKey struct{ device, metric string }

type Closed struct {
	DeviceID string
	Metric   string
	Bucket   time.Time
	Stats    Stats
}

func NewAggregator(size time.Duration) *Aggregator {
	return &Aggregator{size: size, windows: map[seriesKey]*Stats{}}
}

func (a *Aggregator) Add(device, metric string, v float64, ts time.Time) (Closed, bool) {
	bucket := ts.Truncate(a.size)
	key := seriesKey{device, metric}

	a.mu.Lock()
	defer a.mu.Unlock()

	w, ok := a.windows[key]
	if !ok {
		a.windows[key] = &Stats{Count: 1, Sum: v, Min: v, Max: v, Last: v, Since: bucket}
		return Closed{}, false
	}

	var closed Closed
	var hasClosed bool
	if bucket.After(w.Since) {
		closed = Closed{DeviceID: device, Metric: metric, Bucket: w.Since, Stats: *w}
		hasClosed = true
		*w = Stats{Since: bucket}
	}

	if w.Count == 0 {
		w.Min, w.Max = v, v
	} else {
		w.Min = min(w.Min, v)
		w.Max = max(w.Max, v)
	}
	w.Count++
	w.Sum += v
	w.Last = v
	return closed, hasClosed
}

func (a *Aggregator) Snapshot(device, metric string) (Stats, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	w, ok := a.windows[seriesKey{device, metric}]
	if !ok {
		return Stats{}, false
	}
	return *w, true
}

func (a *Aggregator) Evict(cutoff time.Time) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for k, w := range a.windows {
		if w.Since.Before(cutoff) {
			delete(a.windows, k)
			n++
		}
	}
	return n
}

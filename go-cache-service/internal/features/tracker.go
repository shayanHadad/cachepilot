// Package features maintains short, per-key windows of recent access
// timestamps and derives the time-based statistics the ML decision
// service uses.
package features

import (
	"sync"
	"time"
)

// oneMinuteWindow and fiveMinuteWindow are fixed, not configurable.
// They are tied directly to the two frequency features the ML model
// expects (frequency_1min, frequency_5min)
const (
	oneMinuteWindow  = 1 * time.Minute
	fiveMinuteWindow = 5 * time.Minute
)

// WindowStats holds the time-windowed access statistics for a single
// key, computed from its history immediately before the current
// access is recorded.
type WindowStats struct {
	Frequency1Min int

	Frequency5Min int

	RecencySec float64

	InterArrivalAvg float64
}

// keyHistory holds one key's retained access timestamps, oldest
// first. Pruned lazily to fiveMinuteWindow on every Observe/cleanup
// pass, so it never grows past the number of accesses in five
// minutes for that key.
type keyHistory struct {
	timestamps []time.Time
}

// Tracker maintains per-key sliding windows of access timestamps. It
// is safe for concurrent use.
type Tracker struct {
	mu      sync.Mutex
	history map[string]*keyHistory

	stopOnce sync.Once
	stopCh   chan struct{}
}

// NewTracker creates a Tracker and starts a background goroutine that
// purges stale per-key history every cleanupInterval, so keys that
// stop being accessed don't grow the tracker's memory forever. Call
// Stop when the tracker is no longer needed to end that goroutine.
func NewTracker(cleanupInterval time.Duration) *Tracker {
	t := &Tracker{
		history: make(map[string]*keyHistory),
		stopCh:  make(chan struct{}),
	}
	go t.cleanupLoop(cleanupInterval)
	return t
}

// Observe records an access to key at time ts and returns the window
// statistics derived from that key's history *prior* to this access
// (see DEV NOTE point 1 for why this ordering matters).
func (t *Tracker) Observe(key string, ts time.Time) WindowStats {
	t.mu.Lock()
	defer t.mu.Unlock()

	h, ok := t.history[key]
	if !ok {
		h = &keyHistory{}
		t.history[key] = h
	}

	stats := computeStats(h.timestamps, ts)

	h.timestamps = append(h.timestamps, ts)
	h.timestamps = prune(h.timestamps, ts)

	return stats
}

// computeStats derives WindowStats from prior history and the
// current access time now. prior is assumed already pruned to
// fiveMinuteWindow (Observe and purgeStale both maintain that
// invariant), but is pruned again defensively here.
func computeStats(prior []time.Time, now time.Time) WindowStats {
	pruned := prune(prior, now)
	if len(pruned) == 0 {
		return WindowStats{}
	}

	oneMinAgo := now.Add(-oneMinuteWindow)
	freq1 := 0
	for _, ts := range pruned {
		if ts.After(oneMinAgo) {
			freq1++
		}
	}

	last := pruned[len(pruned)-1]
	recency := now.Sub(last).Seconds()

	interArrival := 0.0
	if len(pruned) >= 2 {
		total := 0.0
		for i := 1; i < len(pruned); i++ {
			total += pruned[i].Sub(pruned[i-1]).Seconds()
		}
		interArrival = total / float64(len(pruned)-1)
	}

	return WindowStats{
		Frequency1Min:   freq1,
		Frequency5Min:   len(pruned),
		RecencySec:      recency,
		InterArrivalAvg: interArrival,
	}
}

// prune drops timestamps older than fiveMinuteWindow relative to now.
// timestamps must be sorted oldest-first, which Observe/purgeStale
// both guarantee by only ever appending to the end.
func prune(timestamps []time.Time, now time.Time) []time.Time {
	cutoff := now.Add(-fiveMinuteWindow)
	idx := 0
	for idx < len(timestamps) && timestamps[idx].Before(cutoff) {
		idx++
	}
	if idx == 0 {
		return timestamps
	}
	return timestamps[idx:]
}

// cleanupLoop periodically purges stale history until Stop is called.
func (t *Tracker) cleanupLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			t.purgeStale()
		case <-t.stopCh:
			return
		}
	}
}

// purgeStale prunes every key's history and removes keys whose
// history is now empty, so keys that are no longer being accessed
// don't hold memory in the tracker indefinitely.
func (t *Tracker) purgeStale() {
	now := time.Now()

	t.mu.Lock()
	defer t.mu.Unlock()

	for key, h := range t.history {
		h.timestamps = prune(h.timestamps, now)
		if len(h.timestamps) == 0 {
			delete(t.history, key)
		}
	}
}

// Stop terminates the background cleanup goroutine. Safe to call more
// than once; only the first call has any effect.
func (t *Tracker) Stop() {
	t.stopOnce.Do(func() {
		close(t.stopCh)
	})
}

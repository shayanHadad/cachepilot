package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shayanHadad/cachepilot/internal/features"
	"github.com/shayanHadad/cachepilot/internal/logger"
	"github.com/shayanHadad/cachepilot/internal/store"
	"github.com/shayanHadad/cachepilot/internal/types"
)

// Features is the set of time-windowed and document-derived inputs
// the ML decision service uses to make an admit/TTL decision for a
// single cache-miss.
type Features struct {
	Frequency1Min   int
	Frequency5Min   int
	RecencySec      float64
	InterArrivalAvg float64
	PayloadSizeKB   float64
	QueryType       string
}

// Decider is implemented by anything that can turn Features into an
// admit/TTL decision for a cache-miss.
type Decider interface {
	Decide(ctx context.Context, key string, features Features) (types.CacheDecision, error)
}

// Manager owns orchestration: it decides, per policy, whether a
// cache-miss should be admitted and for how long, applies fallback
// behavior if the ML service is slow or unavailable, and logs every
// request.
type Manager struct {
	cache  Cache
	store  *store.Store
	logger *logger.Logger
	policy string // "lru", "lfu", or "ml"

	// decider and tracker are only used when policy == "ml"
	decider   Decider
	tracker   *features.Tracker
	mlTimeout time.Duration

	// ttlExpiry tracks per-key expiry deadlines for the "ml" policy
	// only.
	ttlMu     sync.Mutex
	ttlExpiry map[string]time.Time

	// hits/misses are Manager's own logical counters, independent of
	// the underlying Cache's counters.
	hits, misses atomic.Int64

	stopCh chan struct{}
}

// NewManager builds a Manager.
func NewManager(
	c Cache,
	st *store.Store,
	log *logger.Logger,
	policy string,
	dec Decider,
	tr *features.Tracker,
	mlTimeout time.Duration,
	expiryCleanupInterval time.Duration,
) (*Manager, error) {
	switch policy {
	case "lru", "lfu":
	case "ml":
		if dec == nil {
			return nil, fmt.Errorf("cache: policy is \"ml\" but no Decider was provided")
		}
		if tr == nil {
			return nil, fmt.Errorf("cache: policy is \"ml\" but no features.Tracker was provided")
		}
	default:
		return nil, fmt.Errorf("cache: unknown policy %q (expected lru, lfu, or ml)", policy)
	}

	m := &Manager{
		cache:     c,
		store:     st,
		logger:    log,
		policy:    policy,
		decider:   dec,
		tracker:   tr,
		mlTimeout: mlTimeout,
		ttlExpiry: make(map[string]time.Time),
		stopCh:    make(chan struct{}),
	}

	if policy == "ml" {
		go m.expiryCleanupLoop(expiryCleanupInterval)
	}

	return m, nil
}

// Get returns the value for key, either from the cache or, on a
// miss, from MongoDB — applying the configured policy's admission
// rules along the way.
func (m *Manager) Get(ctx context.Context, key string) ([]byte, error) {
	start := time.Now()

	var stats features.WindowStats
	if m.policy == "ml" {
		stats = m.tracker.Observe(key, start)
	}

	value, hit := m.cache.Get(key)
	if hit && m.policy == "ml" && m.isExpired(key, start) {
		hit = false
		value = nil
	}

	status := types.StatusMiss
	var source string
	if hit {
		status = types.StatusHit
		m.hits.Add(1)
		source = "cache-hit"
	} else {
		fetched, err := m.store.GetPost(ctx, key)
		if err != nil {
			return nil, err
		}
		value = fetched
		m.misses.Add(1)
		source = m.admit(ctx, key, value, stats, start)
	}

	meta := decodeMeta(value)
	latencyMs := float64(time.Since(start).Microseconds()) / 1000.0
	m.logger.Log(types.NewLogEntry(start, key, status, latencyMs, len(value), meta.QueryType, source))

	return value, nil
}

// admit applies the configured policy's admission rule to a freshly
// fetched value. Only called on a miss. Returns a string identifying
// what made the decision — the policy name for lru/lfu, or the
// decider's own source for ml (see LogEntry.Source) — so the caller
// can log it.
func (m *Manager) admit(ctx context.Context, key string, value []byte, stats features.WindowStats, now time.Time) string {
	switch m.policy {
	case "lru", "lfu":
		m.cache.Put(key, value)
		return m.policy

	case "ml":
		meta := decodeMeta(value)
		feat := Features{
			Frequency1Min:   stats.Frequency1Min,
			Frequency5Min:   stats.Frequency5Min,
			RecencySec:      stats.RecencySec,
			InterArrivalAvg: stats.InterArrivalAvg,
			PayloadSizeKB:   meta.MediaSizeKB,
			QueryType:       meta.QueryType,
		}

		decision := m.decide(ctx, key, feat)
		if decision.Admit {
			m.cache.Put(key, value)
			if decision.TTL > 0 {
				m.setExpiry(key, now.Add(decision.TTL))
			}
		}
		return decision.Source

	default:
		return "unknown"
	}
}

// decide calls the Decider with the project's fixed timeout,
// enforced independently of whatever timeout ctx already carries.
// requests from the cache entirely.
func (m *Manager) decide(ctx context.Context, key string, feat Features) types.CacheDecision {
	dctx, cancel := context.WithTimeout(ctx, m.mlTimeout)
	defer cancel()

	decision, err := m.decider.Decide(dctx, key, feat)
	if err != nil {
		return types.CacheDecision{Admit: true, TTL: 0, Source: "fallback-lru"}
	}
	return decision
}

// setExpiry records that key should be treated as expired after at.
func (m *Manager) setExpiry(key string, at time.Time) {
	m.ttlMu.Lock()
	defer m.ttlMu.Unlock()
	m.ttlExpiry[key] = at
}

// isExpired reports whether key has a recorded expiry that has
// already passed.
func (m *Manager) isExpired(key string, now time.Time) bool {
	m.ttlMu.Lock()
	defer m.ttlMu.Unlock()
	exp, ok := m.ttlExpiry[key]
	if !ok {
		return false
	}
	return now.After(exp)
}

// expiryCleanupLoop periodically drops expiry bookkeeping for keys
// whose deadline has already passed.
func (m *Manager) expiryCleanupLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.purgeExpired()
		case <-m.stopCh:
			return
		}
	}
}

func (m *Manager) purgeExpired() {
	now := time.Now()
	m.ttlMu.Lock()
	defer m.ttlMu.Unlock()
	for key, exp := range m.ttlExpiry {
		if now.After(exp) {
			delete(m.ttlExpiry, key)
		}
	}
}

// Stats returns Manager's own hit/miss counters
func (m *Manager) Stats() CacheStats {
	return CacheStats{
		Hits:      m.hits.Load(),
		Misses:    m.misses.Load(),
		Evictions: m.cache.Stats().Evictions,
	}
}

// Close stops Manager's background expiry-cleanup goroutine.
func (m *Manager) Close() {
	close(m.stopCh)
}

// cachedMeta extracts just the fields Manager needs from a cached
// post's JSON bytes
type cachedMeta struct {
	QueryType   string  `json:"query_type"`
	MediaSizeKB float64 `json:"media_size_kb"`
}

// decodeMeta is best-effort: value is always bytes Manager itself
// produced via store.GetPost, so a decode failure would indicate a
// bug elsewhere, not a normal runtime condition.
func decodeMeta(value []byte) cachedMeta {
	var meta cachedMeta
	_ = json.Unmarshal(value, &meta)
	return meta
}

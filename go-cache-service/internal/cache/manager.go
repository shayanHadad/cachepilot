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
// single cache-miss. Defined here (not in mlclient) because this
// package is the consumer; the transport layer converts to/from this
// shape, not the other way around.
type Features struct {
	Frequency1Min   int
	Frequency5Min   int
	RecencySec      float64
	InterArrivalAvg float64
	PayloadSizeKB   float64
	QueryType       string
}

// Decider is implemented by anything that can turn Features into an
// admit/TTL decision for a cache-miss. The real implementation
// (internal/mlclient) calls the ML service over gRPC; tests can
// substitute a fake with no network involved.
type Decider interface {
	Decide(ctx context.Context, features Features) (types.CacheDecision, error)
}

// Manager owns orchestration: it decides, per policy, whether a
// cache-miss should be admitted and for how long, applies fallback
// behavior if the ML service is slow or unavailable, and logs every
// request. It does not implement eviction itself — that's delegated
// to the underlying Cache (LRU, LFU, or a capacity-bounded cache used
// under the "ml" policy).
type Manager struct {
	cache  Cache
	store  *store.Store
	logger *logger.Logger
	policy string // "lru", "lfu", or "ml" — validated in NewManager

	// decider and tracker are only used when policy == "ml"; both
	// are nil otherwise.
	decider   Decider
	tracker   *features.Tracker
	mlTimeout time.Duration

	// ttlExpiry tracks per-key expiry deadlines for the "ml" policy
	// only. See the DEV NOTE below for why this lives here instead
	// of inside the Cache interface.
	ttlMu     sync.Mutex
	ttlExpiry map[string]time.Time

	// hits/misses are Manager's own logical counters, independent of
	// the underlying Cache's counters. See DEV NOTE for why these
	// can diverge under the "ml" policy.
	hits, misses atomic.Int64

	stopCh chan struct{}
}


// NewManager builds a Manager. dec and tr are required (non-nil)
// when policy is "ml", and ignored otherwise. mlTimeout is the
// per-decision timeout enforced on every call to dec.Decide (should
// already be validated to the project's 5-10ms range by
// config.Validate before reaching here). expiryCleanupInterval
// controls how often stale TTL bookkeeping entries are purged; it is
// only used when policy == "ml".
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
		// no extra requirements
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
// rules along the way. Every call logs exactly one LogEntry, unless
// the underlying store lookup itself fails (see DEV NOTE point 3 in
// this file's package-level notes on why fetch errors aren't
// logged).
func (m *Manager) Get(ctx context.Context, key string) ([]byte, error) {
	start := time.Now()

	// Feed the access tracker first, for every access (hit or miss),
	// so frequency/recency features reflect the true request
	// pattern rather than just the miss pattern. Only meaningful
	// under the "ml" policy, where the returned stats get used below.
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
	if hit {
		status = types.StatusHit
		m.hits.Add(1)
	} else {
		fetched, err := m.store.GetPost(ctx, key)
		if err != nil {
			return nil, err
		}
		value = fetched
		m.misses.Add(1)
		m.admit(ctx, key, value, stats, start)
	}

	meta := decodeMeta(value)
	latencyMs := float64(time.Since(start).Microseconds()) / 1000.0
	m.logger.Log(types.NewLogEntry(start, key, status, latencyMs, len(value), meta.QueryType))

	return value, nil
}

// admit applies the configured policy's admission rule to a freshly
// fetched value. Only called on a miss.
func (m *Manager) admit(ctx context.Context, key string, value []byte, stats features.WindowStats, now time.Time) {
	switch m.policy {
	case "lru", "lfu":
		m.cache.Put(key, value)

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

		decision := m.decide(ctx, feat)
		if !decision.Admit {
			return
		}

		m.cache.Put(key, value)
		if decision.TTL > 0 {
			m.setExpiry(key, now.Add(decision.TTL))
		}
	}
}

// decide calls the Decider with the project's fixed timeout,
// enforced independently of whatever timeout ctx already carries.
// If the call fails or times out, it falls back to "behave like
// LRU": always admit, no TTL — so a struggling ML service degrades
// to the traditional baseline instead of stalling or dropping
// requests from the cache entirely.
func (m *Manager) decide(ctx context.Context, feat Features) types.CacheDecision {
	dctx, cancel := context.WithTimeout(ctx, m.mlTimeout)
	defer cancel()

	decision, err := m.decider.Decide(dctx, feat)
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
// already passed. A key with no recorded expiry is never considered
// expired (covers both fallback decisions with TTL == 0 and the
// lru/lfu policies, which never call setExpiry at all).
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
// whose deadline has already passed, so ttlExpiry doesn't grow
// forever over a long-running evaluation session.
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

// Stats returns Manager's own hit/miss counters (TTL-aware; see the
// package-level DEV NOTE) combined with the underlying Cache's
// eviction count.
func (m *Manager) Stats() CacheStats {
	return CacheStats{
		Hits:      m.hits.Load(),
		Misses:    m.misses.Load(),
		Evictions: m.cache.Stats().Evictions,
	}
}

// Close stops Manager's background expiry-cleanup goroutine. It does
// not close the injected Cache, Store, Logger, or Tracker — those
// are owned by whatever constructed them (main.go) and should be
// closed there.
func (m *Manager) Close() {
	close(m.stopCh)
}

// cachedMeta extracts just the fields Manager needs from a cached
// post's JSON bytes, without depending on store's unexported postJSON
// type. Manager and store agree on the wire shape ("query_type",
// "media_size_kb") rather than sharing a Go type, keeping the two
// packages decoupled — store could change its internal struct name
// or add fields freely without breaking Manager.
type cachedMeta struct {
	QueryType   string  `json:"query_type"`
	MediaSizeKB float64 `json:"media_size_kb"`
}

// decodeMeta is best-effort: value is always bytes Manager itself
// produced via store.GetPost, so a decode failure would indicate a
// bug elsewhere, not a normal runtime condition. On failure it
// returns a zero-value cachedMeta rather than an error, since losing
// query_type/payload_size on a single request isn't worth failing
// the whole Get() call over.
func decodeMeta(value []byte) cachedMeta {
	var meta cachedMeta
	_ = json.Unmarshal(value, &meta)
	return meta
}

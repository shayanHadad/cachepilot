package cache

// Cache is the common interface implemented by every policy
// (LRU, LFU, and the ML-driven policy)
type Cache interface {
	// Get returns the cached value for key, if present.
	Get(key string) (value []byte, ok bool)

	// Put stores value under key, applying the policy's own
	// admission/eviction rules.
	Put(key string, value []byte)

	// Stats returns a snapshot of this cache's hit/miss/eviction counters.
	Stats() CacheStats
}

// CacheStats is a snapshot of hit/miss/eviction counters for a cache.
type CacheStats struct {
	Hits      int64
	Misses    int64
	Evictions int64
}

// HitRate returns Hits / (Hits + Misses)
func (s CacheStats) HitRate() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total)
}

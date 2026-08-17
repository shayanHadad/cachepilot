package types

import "time"

// CacheDecision represents an admit/TTL decision for a given key,
// No matter what was the caching strategy.
type CacheDecision struct {
	// Admit reports whether the key should be stored in the cache.
	Admit bool

	// TTL is how long the key should stay in the cache if admitted.
	TTL time.Duration

	// Source identifies which policy produced this decision.
	Source string
}

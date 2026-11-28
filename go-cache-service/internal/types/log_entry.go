// Package types holds data structures shared across multiple internal
// packages (cache, logger, mlclient) to avoid circular imports between them.
package types

import "time"

// CacheStatus represents the result of the cache lookup
type CacheStatus string

const (
	// StatusHit means the key was found in the cache.
	StatusHit CacheStatus = "hit"
	// StatusMiss means the key was not found and was read from MongoDB.
	StatusMiss CacheStatus = "miss"
)

// LogEntry is a single request record written to the raw JSONL log.
type LogEntry struct {
	// Timestamp is stored as Unix milliseconds
	Timestamp int64 `json:"timestamp"`

	// Key is the string form of the MongoDB document's _id
	Key string `json:"key"`

	// CacheStatus is the outcome of the cache lookup
	CacheStatus CacheStatus `json:"cache_status"`

	// LatencyMs is the total response time in milliseconds.
	LatencyMs float64 `json:"latency_ms"`

	// ResponseSize is the size of the returned response, in bytes.
	ResponseSize int `json:"response_size"`

	// QueryType classifies the requested post: "text_post" or "media_post".
	QueryType string `json:"query_type"`

	// Source identifies what actually produced this outcome: "cache-hit"
	// for any hit, the policy name ("lru"/"lfu"/"none") for a baseline
	// miss, or the decider's own source ("heuristic-v1", "fallback-lru",
	// or a model name) for an "ml" policy miss. Lets evaluation code
	// count exactly how often decisions actually came from the ML
	// service vs. fell back.
	Source string `json:"source"`

	// FetchMs is the time spent reading the post from MongoDB, in
	// milliseconds. It is 0 for a hit.
	FetchMs float64 `json:"fetch_ms"`

	// DecisionMs is the time spent waiting for the admission decision,
	// in milliseconds. It is 0 for a hit and for the policies without a
	// decider, and it includes the wait for the timeout when the call
	// falls back.
	DecisionMs float64 `json:"decision_ms"`
}

// NewLogEntry builds a LogEntry, converting the given time.Time to
// Unix milliseconds. The timings are zero; add them with WithTimings.
func NewLogEntry(ts time.Time, key string, status CacheStatus, latencyMs float64, responseSize int, queryType string, source string) LogEntry {
	return LogEntry{
		Timestamp:    ts.UnixMilli(),
		Key:          key,
		CacheStatus:  status,
		LatencyMs:    latencyMs,
		ResponseSize: responseSize,
		QueryType:    queryType,
		Source:       source,
	}
}

// WithTimings returns a copy of e with the MongoDB read time and the
// decision time set, both in milliseconds.
func (e LogEntry) WithTimings(fetchMs, decisionMs float64) LogEntry {
	e.FetchMs = fetchMs
	e.DecisionMs = decisionMs
	return e
}

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
}

// NewLogEntry builds a LogEntry, converting the given time.Time to
// Unix milliseconds.
func NewLogEntry(ts time.Time, key string, status CacheStatus, latencyMs float64, responseSize int, queryType string) LogEntry {
	return LogEntry{
		Timestamp:    ts.UnixMilli(),
		Key:          key,
		CacheStatus:  status,
		LatencyMs:    latencyMs,
		ResponseSize: responseSize,
		QueryType:    queryType,
	}
}

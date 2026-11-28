package logger

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shayanHadad/cachepilot/internal/types"
)

// TestLoggedLineCarriesEveryLogEntryField checks that each LogEntry
// field reaches the JSONL file, in particular source, which
// evaluation uses to count how often decisions came from the model,
// and the fetch and decision timings.
func TestLoggedLineCarriesEveryLogEntryField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "service.jsonl")
	lg, err := NewLogger(path, 10)
	if err != nil {
		t.Fatalf("creating logger: %v", err)
	}

	entry := types.NewLogEntry(time.UnixMilli(1700000000000), "post1", types.StatusMiss, 1.5, 42, "text_post", "fallback-lru").
		WithTimings(2.5, 0.75)
	lg.Log(entry)
	if err := lg.Close(); err != nil {
		t.Fatalf("closing logger: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading log: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(data))), &got); err != nil {
		t.Fatalf("log line is not valid JSON: %v\n%s", err, data)
	}

	for _, field := range []string{"timestamp", "key", "cache_status", "latency_ms", "response_size", "query_type", "source", "fetch_ms", "decision_ms"} {
		if _, ok := got[field]; !ok {
			t.Errorf("field %q missing from logged line: %s", field, strings.TrimSpace(string(data)))
		}
	}
	if got["source"] != "fallback-lru" {
		t.Errorf("source = %v, want fallback-lru", got["source"])
	}
	if got["fetch_ms"] != 2.5 {
		t.Errorf("fetch_ms = %v, want 2.5", got["fetch_ms"])
	}
	if got["decision_ms"] != 0.75 {
		t.Errorf("decision_ms = %v, want 0.75", got["decision_ms"])
	}
}

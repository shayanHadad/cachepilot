package cache

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shayanHadad/cachepilot/internal/features"
	"github.com/shayanHadad/cachepilot/internal/logger"
	"github.com/shayanHadad/cachepilot/internal/types"
)

// slowPosts returns a small post body after a fixed delay.
type slowPosts struct{ delay time.Duration }

func (p slowPosts) GetPost(_ context.Context, _ string) ([]byte, error) {
	time.Sleep(p.delay)
	return []byte(`{"query_type":"text_post","media_size_kb":0}`), nil
}

// slowDecider admits every key after a fixed delay.
type slowDecider struct{ delay time.Duration }

func (d slowDecider) Decide(_ context.Context, _ string, _ Features) (types.CacheDecision, error) {
	time.Sleep(d.delay)
	return types.CacheDecision{Admit: true, TTL: 0, Source: "test-model"}, nil
}

// readLogLines parses every line of a JSONL log into a map.
func readLogLines(t *testing.T, path string) []map[string]any {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading log: %v", err)
	}
	var lines []map[string]any
	for _, raw := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var line map[string]any
		if err := json.Unmarshal([]byte(raw), &line); err != nil {
			t.Fatalf("log line is not valid JSON: %v\n%s", err, raw)
		}
		lines = append(lines, line)
	}
	return lines
}

// TestLogCarriesFetchAndDecisionTimings checks that a miss logs how
// long the MongoDB read and the admission decision took, and that a
// hit logs zero for both.
func TestLogCarriesFetchAndDecisionTimings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "service.jsonl")
	lg, err := logger.NewLogger(path, 100)
	if err != nil {
		t.Fatalf("creating logger: %v", err)
	}

	tr := features.NewTracker(time.Minute)
	defer tr.Stop()

	m, err := NewManager(NewLRU(10), slowPosts{delay: 20 * time.Millisecond}, lg, "ml",
		slowDecider{delay: 10 * time.Millisecond}, tr, 50*time.Millisecond, time.Minute)
	if err != nil {
		t.Fatalf("creating manager: %v", err)
	}

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := m.Get(ctx, "post1"); err != nil {
			t.Fatalf("get %d: %v", i+1, err)
		}
	}
	m.Close()
	if err := lg.Close(); err != nil {
		t.Fatalf("closing logger: %v", err)
	}

	lines := readLogLines(t, path)
	if len(lines) != 2 {
		t.Fatalf("got %d log lines, want 2", len(lines))
	}
	miss, hit := lines[0], lines[1]

	if miss["cache_status"] != "miss" {
		t.Fatalf("first request cache_status = %v, want miss", miss["cache_status"])
	}
	if fetch, _ := miss["fetch_ms"].(float64); fetch < 20 {
		t.Errorf("miss fetch_ms = %v, want at least 20", miss["fetch_ms"])
	}
	if decision, _ := miss["decision_ms"].(float64); decision < 10 {
		t.Errorf("miss decision_ms = %v, want at least 10", miss["decision_ms"])
	}

	if hit["cache_status"] != "hit" {
		t.Fatalf("second request cache_status = %v, want hit", hit["cache_status"])
	}
	if fetch, _ := hit["fetch_ms"].(float64); fetch != 0 {
		t.Errorf("hit fetch_ms = %v, want 0", hit["fetch_ms"])
	}
	if decision, _ := hit["decision_ms"].(float64); decision != 0 {
		t.Errorf("hit decision_ms = %v, want 0", hit["decision_ms"])
	}
}

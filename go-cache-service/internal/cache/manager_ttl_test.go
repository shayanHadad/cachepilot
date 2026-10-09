package cache

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/shayanHadad/cachepilot/internal/features"
	"github.com/shayanHadad/cachepilot/internal/logger"
	"github.com/shayanHadad/cachepilot/internal/types"
)

// fakePosts returns the same small post body for every key.
type fakePosts struct{}

func (fakePosts) GetPost(_ context.Context, _ string) ([]byte, error) {
	return []byte(`{"query_type":"text_post","media_size_kb":0}`), nil
}

// fixedDecider always returns the same decision.
type fixedDecider struct {
	decision types.CacheDecision
}

func (d fixedDecider) Decide(_ context.Context, _ string, _ Features) (types.CacheDecision, error) {
	return d.decision, nil
}

// newTestManager builds an "ml"-policy Manager whose decider admits
// every key with the given TTL and whose expiry cleanup runs every
// 10ms.
func newTestManager(t *testing.T, ttl time.Duration) *Manager {
	t.Helper()

	lg, err := logger.NewLogger(filepath.Join(t.TempDir(), "service.jsonl"), 100)
	if err != nil {
		t.Fatalf("creating logger: %v", err)
	}
	t.Cleanup(func() { _ = lg.Close() })

	tr := features.NewTracker(time.Minute)
	t.Cleanup(tr.Stop)

	dec := fixedDecider{decision: types.CacheDecision{Admit: true, TTL: ttl, Source: "test-model"}}
	m, err := NewManager(NewLRU(10), fakePosts{}, lg, "ml", dec, tr, 50*time.Millisecond, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("creating manager: %v", err)
	}
	t.Cleanup(m.Close)

	return m
}

// TestEntryIsHitBeforeTTLExpires is a control: a fresh entry must be
// served as a hit.
func TestEntryIsHitBeforeTTLExpires(t *testing.T) {
	m := newTestManager(t, time.Hour)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, err := m.Get(ctx, "post1"); err != nil {
			t.Fatalf("get %d: %v", i+1, err)
		}
	}

	if s := m.Stats(); s.Hits != 1 || s.Misses != 1 {
		t.Fatalf("hits=%d misses=%d, want hits=1 misses=1", s.Hits, s.Misses)
	}
}

// TestExpiredEntryIsNotServedAsHitAfterCleanup checks that an entry
// whose TTL has passed is a logical miss even after the expiry
// bookkeeping has been purged.
func TestExpiredEntryIsNotServedAsHitAfterCleanup(t *testing.T) {
	m := newTestManager(t, 50*time.Millisecond)
	ctx := context.Background()

	if _, err := m.Get(ctx, "post1"); err != nil {
		t.Fatalf("first get: %v", err)
	}

	// Long enough for the TTL to pass and several cleanup sweeps to run.
	time.Sleep(200 * time.Millisecond)

	if _, err := m.Get(ctx, "post1"); err != nil {
		t.Fatalf("second get: %v", err)
	}

	if s := m.Stats(); s.Hits != 0 {
		t.Fatalf("expired entry served as a hit: hits=%d misses=%d, want hits=0", s.Hits, s.Misses)
	}
}

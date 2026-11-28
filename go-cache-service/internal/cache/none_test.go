package cache

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/shayanHadad/cachepilot/internal/logger"
)

// TestNopCacheNeverStores checks that nothing put into the cache is
// ever served back.
func TestNopCacheNeverStores(t *testing.T) {
	c := NewNop()

	c.Put("a", []byte("x"))
	if _, ok := c.Get("a"); ok {
		t.Fatal("nop cache served a value")
	}
	c.Delete("a")

	if s := c.Stats(); s.Hits != 0 || s.Misses != 1 || s.Evictions != 0 {
		t.Fatalf("stats = %+v, want 0 hits, 1 miss, 0 evictions", s)
	}
}

// TestNonePolicyReadsEveryRequestFromTheStore checks that under the
// "none" policy a repeated key is never a hit.
func TestNonePolicyReadsEveryRequestFromTheStore(t *testing.T) {
	lg, err := logger.NewLogger(filepath.Join(t.TempDir(), "service.jsonl"), 100)
	if err != nil {
		t.Fatalf("creating logger: %v", err)
	}
	t.Cleanup(func() { _ = lg.Close() })

	m, err := NewManager(NewNop(), fakePosts{}, lg, "none", nil, nil, 50*time.Millisecond, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("creating manager: %v", err)
	}
	t.Cleanup(m.Close)

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := m.Get(ctx, "post1"); err != nil {
			t.Fatalf("get %d: %v", i+1, err)
		}
	}

	if s := m.Stats(); s.Hits != 0 || s.Misses != 2 {
		t.Fatalf("hits=%d misses=%d, want hits=0 misses=2", s.Hits, s.Misses)
	}
}

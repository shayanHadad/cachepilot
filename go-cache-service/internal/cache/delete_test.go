package cache

import "testing"

func TestLRUDelete(t *testing.T) {
	c := NewLRU(2)
	c.Put("a", []byte("1"))

	c.Delete("a")
	if _, ok := c.Get("a"); ok {
		t.Fatal("deleted key still present")
	}
	c.Delete("missing") // must not panic

	if e := c.Stats().Evictions; e != 0 {
		t.Fatalf("evictions=%d, want 0: Delete must not count as an eviction", e)
	}
}

func TestLFUDelete(t *testing.T) {
	c := NewLFU(2)
	c.Put("a", []byte("1"))
	c.Put("b", []byte("2"))
	c.Get("b") // b is now more frequent than a

	c.Delete("a")
	if _, ok := c.Get("a"); ok {
		t.Fatal("deleted key still present")
	}
	c.Delete("missing") // must not panic

	// The cache must still enforce its capacity after a delete.
	c.Put("c", []byte("3"))
	c.Put("d", []byte("4"))
	if n := len(c.items); n != 2 {
		t.Fatalf("items=%d, want 2", n)
	}

	if e := c.Stats().Evictions; e != 1 {
		t.Fatalf("evictions=%d, want 1 (only the capacity eviction)", e)
	}
}

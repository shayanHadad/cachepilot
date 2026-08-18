package cache

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// lruCache is a least-recently-used cache: the least recently accessed
// key is evicted first once capacity is exceeded.
type lruCache struct {
	mu       sync.Mutex
	capacity int
	ll       *list.List               // front = most recently used
	items    map[string]*list.Element // key -> node in ll

	hits, misses, evictions atomic.Int64
}

// lruEntry is the value stored inside each list.Element.
type lruEntry struct {
	key   string
	value []byte
}

// NewLRU creates an LRU cache with the given capacity (max number of keys).
func NewLRU(capacity int) *lruCache {
	return &lruCache{
		capacity: capacity,
		ll:       list.New(),
		items:    make(map[string]*list.Element),
	}
}

// compile-time check that *lruCache satisfies the Cache interface.
var _ Cache = (*lruCache)(nil)

// Get returns the cached value for key, moving it to the front of the
// LRU list (marking it as most recently used) if found.
func (c *lruCache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.items[key]
	if !ok {
		c.misses.Add(1)
		return nil, false
	}
	c.ll.MoveToFront(elem)
	c.hits.Add(1)
	return elem.Value.(*lruEntry).value, true
}

// Put stores value under key. If the key already exists, its value is
// updated and it's moved to the front. If adding a new key exceeds
// capacity, the least recently used key is evicted.
func (c *lruCache) Put(key string, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		elem.Value.(*lruEntry).value = value
		c.ll.MoveToFront(elem)
		return
	}

	elem := c.ll.PushFront(&lruEntry{key: key, value: value})
	c.items[key] = elem

	if c.ll.Len() > c.capacity {
		c.evictOldest()
	}
}

// evictOldest removes the least recently used entry. Caller must hold c.mu.
func (c *lruCache) evictOldest() {
	oldest := c.ll.Back()
	if oldest == nil {
		return
	}
	c.ll.Remove(oldest)
	delete(c.items, oldest.Value.(*lruEntry).key)
	c.evictions.Add(1)
}

// Stats returns a snapshot of hit/miss/eviction counts.
func (c *lruCache) Stats() CacheStats {
	return CacheStats{
		Hits:      c.hits.Load(),
		Misses:    c.misses.Load(),
		Evictions: c.evictions.Load(),
	}
}

package cache

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// lfuCache is a least-frequently-used cache: when full, the key with
// the lowest access frequency is evicted first. Among keys tied at
// the same frequency, the least recently used one is evicted
type lfuCache struct {
	mu        sync.Mutex
	capacity  int
	items     map[string]*list.Element // key -> node in its frequency list
	freqLists map[int]*list.List       // freq -> list of *lfuEntry, front = most recently touched at this freq
	minFreq   int

	hits, misses, evictions atomic.Int64
}

// lfuEntry is the value stored inside each list.Element.
type lfuEntry struct {
	key   string
	value []byte
	freq  int
}

// NewLFU creates an LFU cache with the given capacity
func NewLFU(capacity int) *lfuCache {
	return &lfuCache{
		capacity:  capacity,
		items:     make(map[string]*list.Element),
		freqLists: make(map[int]*list.List),
	}
}

var _ Cache = (*lfuCache)(nil)

// Get returns the cached value for key, bumping its access frequency
// by one if found.
func (c *lfuCache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.items[key]
	if !ok {
		c.misses.Add(1)
		return nil, false
	}
	entry := elem.Value.(*lfuEntry)
	c.bumpFrequency(elem, entry)
	c.hits.Add(1)
	return entry.value, true
}

// Put stores value under key, bumping its frequency if it already
// exists, or inserting it at frequency 1
func (c *lfuCache) Put(key string, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		entry := elem.Value.(*lfuEntry)
		entry.value = value
		c.bumpFrequency(elem, entry)
		return
	}

	if c.capacity > 0 && len(c.items) >= c.capacity {
		c.evictLFU()
	}

	entry := &lfuEntry{key: key, value: value, freq: 1}
	if c.freqLists[1] == nil {
		c.freqLists[1] = list.New()
	}
	elem := c.freqLists[1].PushFront(entry)
	c.items[key] = elem
	c.minFreq = 1
}

// bumpFrequency moves entry from its current frequency list to the
// next one up, updating minFreq if the old frequency's list becomes empty
func (c *lfuCache) bumpFrequency(elem *list.Element, entry *lfuEntry) {
	oldFreq := entry.freq
	c.freqLists[oldFreq].Remove(elem)
	if c.freqLists[oldFreq].Len() == 0 {
		delete(c.freqLists, oldFreq)
		if c.minFreq == oldFreq {
			c.minFreq++
		}
	}

	entry.freq++
	if c.freqLists[entry.freq] == nil {
		c.freqLists[entry.freq] = list.New()
	}
	newElem := c.freqLists[entry.freq].PushFront(entry)
	c.items[entry.key] = newElem
}

// evictLFU removes the least frequently used key
func (c *lfuCache) evictLFU() {
	lst := c.freqLists[c.minFreq]
	if lst == nil || lst.Len() == 0 {
		return
	}
	victim := lst.Back()
	lst.Remove(victim)
	if lst.Len() == 0 {
		delete(c.freqLists, c.minFreq)
	}
	delete(c.items, victim.Value.(*lfuEntry).key)
	c.evictions.Add(1)
}

// Stats returns a snapshot of hit/miss/eviction counts.
func (c *lfuCache) Stats() CacheStats {
	return CacheStats{
		Hits:      c.hits.Load(),
		Misses:    c.misses.Load(),
		Evictions: c.evictions.Load(),
	}
}

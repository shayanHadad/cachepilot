package cache

import "sync/atomic"

// nopCache never stores anything: every Get is a miss and Put does
// nothing. It backs the "none" policy, the no-cache baseline that
// reads every request from MongoDB.
type nopCache struct {
	misses atomic.Int64
}

// NewNop creates a cache that stores nothing.
func NewNop() *nopCache {
	return &nopCache{}
}

// compile-time check that *nopCache satisfies the Cache interface.
var _ Cache = (*nopCache)(nil)

// Get always reports a miss.
func (c *nopCache) Get(key string) ([]byte, bool) {
	c.misses.Add(1)
	return nil, false
}

// Put discards the value.
func (c *nopCache) Put(key string, value []byte) {}

// Delete does nothing, since nothing is ever stored.
func (c *nopCache) Delete(key string) {}

// Stats returns the number of lookups, all of them misses.
func (c *nopCache) Stats() CacheStats {
	return CacheStats{Misses: c.misses.Load()}
}

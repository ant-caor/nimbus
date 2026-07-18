// Copyright 2026 Antonio Cabezas Ordóñez
// SPDX-License-Identifier: Apache-2.0

// Package memory is nimbus's own in-process L1 store: a sharded LRU with TTL.
//
// It is intentionally hand-written (rather than wrapping Ristretto or Otter) to
// show the internals, while staying behind the store.Store interface so a
// higher-performance L1 can be dropped in later. Sharding by key hash reduces
// lock contention under Cloud Run's per-instance request concurrency.
package memory

import (
	"context"
	"hash/maphash"
	"sync"
	"sync/atomic"

	"github.com/ant-caor/nimbus/internal/clock"
	"github.com/ant-caor/nimbus/store"
)

// node is an intrusive doubly-linked-list element so LRU reordering needs no
// per-operation allocation.
type node[V any] struct {
	key        string
	entry      store.Entry[V]
	prev, next *node[V]
}

// shard is an independently-locked slice of the keyspace.
type shard[V any] struct {
	mu         sync.Mutex
	items      map[string]*node[V]
	head, tail *node[V] // head = most-recently-used, tail = least
	capacity   int
	clock      clock.Clock
	evictions  *atomic.Uint64
}

func (s *shard[V]) get(key string) (store.Entry[V], bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.items[key]
	if !ok {
		return store.Entry[V]{}, false
	}
	if n.entry.Expired(s.clock.Now()) {
		s.removeLocked(n)
		return store.Entry[V]{}, false
	}
	s.moveToFrontLocked(n)
	return n.entry, true
}

func (s *shard[V]) set(key string, e store.Entry[V]) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n, ok := s.items[key]; ok {
		n.entry = e
		s.moveToFrontLocked(n)
		return
	}
	s.insertLocked(key, e)
}

// setIfNewer installs e unless a live entry at an equal-or-greater version is
// already present, gating the L1-stomp race. It reports whether it installed.
func (s *shard[V]) setIfNewer(key string, e store.Entry[V]) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n, ok := s.items[key]; ok {
		// A live entry at an equal-or-newer version wins; do not stomp it with an
		// older (or equal, hence equivalent) value. An expired entry is dead and
		// is always replaced.
		if !n.entry.Expired(s.clock.Now()) && e.Version <= n.entry.Version {
			return false
		}
		n.entry = e
		s.moveToFrontLocked(n)
		return true
	}
	s.insertLocked(key, e)
	return true
}

// insertLocked adds a new node for key and evicts the LRU tail if over capacity.
func (s *shard[V]) insertLocked(key string, e store.Entry[V]) {
	n := &node[V]{key: key, entry: e}
	s.items[key] = n
	s.pushFrontLocked(n)
	if s.capacity > 0 && len(s.items) > s.capacity && s.tail != nil {
		s.removeLocked(s.tail)
		s.evictions.Add(1)
	}
}

func (s *shard[V]) delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n, ok := s.items[key]; ok {
		s.removeLocked(n)
	}
}

func (s *shard[V]) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.items)
}

// unlinkLocked detaches n from the list without touching the map.
func (s *shard[V]) unlinkLocked(n *node[V]) {
	if n.prev != nil {
		n.prev.next = n.next
	} else {
		s.head = n.next
	}
	if n.next != nil {
		n.next.prev = n.prev
	} else {
		s.tail = n.prev
	}
	n.prev, n.next = nil, nil
}

func (s *shard[V]) removeLocked(n *node[V]) {
	s.unlinkLocked(n)
	delete(s.items, n.key)
}

func (s *shard[V]) pushFrontLocked(n *node[V]) {
	n.prev = nil
	n.next = s.head
	if s.head != nil {
		s.head.prev = n
	}
	s.head = n
	if s.tail == nil {
		s.tail = n
	}
}

func (s *shard[V]) moveToFrontLocked(n *node[V]) {
	if s.head == n {
		return
	}
	s.unlinkLocked(n)
	s.pushFrontLocked(n)
}

// Cache is a sharded, in-process LRU+TTL store keyed by string.
type Cache[V any] struct {
	shards    []*shard[V]
	mask      uint64
	seed      maphash.Seed
	evictions atomic.Uint64
}

// New builds an in-process L1 store.
func New[V any](opts ...Option) *Cache[V] {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}
	n := roundUpPow2(cfg.shards)
	c := &Cache[V]{
		shards: make([]*shard[V], n),
		mask:   uint64(n - 1),
		seed:   maphash.MakeSeed(),
	}
	perShard := 0
	if cfg.capacity > 0 {
		perShard = (cfg.capacity + n - 1) / n
		if perShard < 1 {
			perShard = 1
		}
	}
	for i := range c.shards {
		c.shards[i] = &shard[V]{
			items:     make(map[string]*node[V]),
			capacity:  perShard,
			clock:     cfg.clock,
			evictions: &c.evictions,
		}
	}
	return c
}

func (c *Cache[V]) shardFor(key string) *shard[V] {
	return c.shards[maphash.String(c.seed, key)&c.mask]
}

// Get implements store.Store.
func (c *Cache[V]) Get(_ context.Context, key string) (store.Entry[V], bool, error) {
	e, ok := c.shardFor(key).get(key)
	return e, ok, nil
}

// Set implements store.Store.
func (c *Cache[V]) Set(_ context.Context, key string, e store.Entry[V]) error {
	c.shardFor(key).set(key, e)
	return nil
}

// SetIfNewer implements store.ConditionalStore: a version-gated install that
// will not stomp a live entry already holding an equal-or-greater version.
func (c *Cache[V]) SetIfNewer(_ context.Context, key string, e store.Entry[V]) (bool, error) {
	return c.shardFor(key).setIfNewer(key, e), nil
}

// Delete implements store.Store.
func (c *Cache[V]) Delete(_ context.Context, key string) error {
	c.shardFor(key).delete(key)
	return nil
}

// Close implements store.Store. The memory store holds no external resources.
func (c *Cache[V]) Close() error { return nil }

// Evictions reports the number of LRU evictions since creation.
func (c *Cache[V]) Evictions() uint64 { return c.evictions.Load() }

// Len reports the current number of entries across all shards.
func (c *Cache[V]) Len() int {
	n := 0
	for _, s := range c.shards {
		n += s.len()
	}
	return n
}

var (
	_ store.Store[int]            = (*Cache[int])(nil)
	_ store.ConditionalStore[int] = (*Cache[int])(nil)
)

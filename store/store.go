// Copyright 2026 Antonio Cabezas Ordóñez
// SPDX-License-Identifier: Apache-2.0

// Package store defines the storage contracts for nimbus tiers.
//
// All stores are keyed by string. The public nimbus.Cache is generic over a
// user key type K, but it maps K to a string exactly once at its boundary and
// then uses string keys uniformly across L1, L2, and the invalidation bus.
// That uniformity is what lets an instance evict an entry named by a key it
// received over the bus, where the original K is not recoverable.
//
// An L1 (in-process) tier satisfies Store: a best-effort accelerator with no
// knowledge of the coherence protocol. The shared L2 tier satisfies
// VersionedStore: it owns version minting and the tag index, and is the source
// of truth.
package store

import (
	"context"
	"errors"
	"math"
	"time"
)

// ErrVersionConflict is returned by SetCAS when the observed version does not
// match the expected version, meaning a concurrent writer won. The caller must
// discard its value and re-read rather than install a stale entry.
var ErrVersionConflict = errors.New("nimbus/store: version conflict")

// ForceVersion is the sentinel expected-version that skips the compare-and-swap
// guard and writes unconditionally. It is used for explicit writes (Set) and
// invalidations, which are last-writer-wins rather than fills.
const ForceVersion uint64 = math.MaxUint64

// Store is the minimal contract for an L1 (in-process) tier. Delete is
// unconditional; the L1 is a dumb accelerator and does not participate in the
// version-gated coherence protocol.
type Store[V any] interface {
	Get(ctx context.Context, key string) (Entry[V], bool, error)
	Set(ctx context.Context, key string, e Entry[V]) error
	Delete(ctx context.Context, key string) error
	Close() error
}

// ConditionalStore is an optional interface a Store may implement to gate an
// install on the entry version. nimbus uses it for versioned (L2-backed)
// installs so a slow fill cannot stomp a newer entry that the bus or a
// concurrent writer already placed in the tier with an older value (the
// L1-stomp race). A Store that does not implement it falls back to an
// unconditional Set, which is correct but unguarded.
type ConditionalStore[V any] interface {
	// SetIfNewer installs e and reports whether it installed. It installs when the
	// tier holds no entry for key, when the stored entry has expired (it is dead,
	// so it is replaced regardless of version), or when e carries a strictly
	// greater Version than a still-live stored entry. A same-version install over
	// a live entry is skipped: an equal version denotes the same authoritative
	// write, so the stored entry is already equivalent. The comparison and install
	// are atomic with respect to other operations on key.
	SetIfNewer(ctx context.Context, key string, e Entry[V]) (installed bool, err error)
}

// Metrics is an optional interface a Store may implement to report operational
// counters for observability. nimbus's Cache.Stats populates its Evictions and
// L1Len fields from it when the L1 tier implements it; a store that does not is
// simply not reported on. It carries no value type because the counters are
// independent of V.
type Metrics interface {
	// Evictions is the cumulative number of entries removed to respect a size or
	// TTL bound (not counting explicit Deletes).
	Evictions() uint64
	// Len is the current number of live entries held by the tier.
	Len() int
}

// VersionedStore is the contract for the authoritative, shared L2 tier. It is
// the single source of versions and the source of truth for values.
type VersionedStore[V any] interface {
	Store[V]

	// Load returns the authoritative entry for key. Entry.Version always carries
	// the current version, even when found is false (a tombstone or an absent
	// key reports found=false but a usable version), so callers can pass it as
	// the expected version to SetCAS. It is the "L2 touch" that lets an instance
	// converge after a missed invalidation broadcast.
	Load(ctx context.Context, key string) (Entry[V], bool, error)

	// SetCAS mints the next version for key and stores val with the given fresh
	// and stale deadlines, but only if the currently stored version equals
	// expect (or expect is ForceVersion). On success it returns the stored entry
	// carrying the freshly minted version. On a version mismatch it returns
	// ErrVersionConflict. tags, if any, associate the key for InvalidateByTag.
	// This is the write half of the fill invariant.
	SetCAS(ctx context.Context, key string, val V, expect uint64, freshUntil, staleUntil time.Time, tags []string) (Entry[V], error)

	// CompareAndDelete writes a versioned tombstone for key if version is at
	// least the current version (or version is ForceVersion). The tombstone
	// gates slower in-flight fills and must outlive the longest plausible
	// loader. It returns the new version and whether a tombstone was written.
	CompareAndDelete(ctx context.Context, key string, version uint64) (newVersion uint64, deleted bool, err error)

	// DeleteByTag invalidates every key associated with tag and returns the
	// affected keys so the caller can broadcast them. Implementations resolve
	// membership from their authoritative tag index. For very large tags this
	// may be chunked or replaced by tag-epoch invalidation; see DESIGN.md.
	DeleteByTag(ctx context.Context, tag string) ([]string, error)
}

// TombstoneTTLer is an optional interface a VersionedStore may implement to
// report how long its invalidation tombstones live. nimbus's Build uses it to
// verify the tombstone outlives the refresh window, which the
// fill-after-invalidate safety proof requires: a tombstone that expires before
// a slow in-flight fill completes lets that fill resurrect a deleted key. A
// store that does not implement this is simply not validated.
type TombstoneTTLer interface {
	TombstoneTTL() time.Duration
}

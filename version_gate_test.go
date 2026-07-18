// Copyright 2026 Antonio Cabezas Ordóñez
// SPDX-License-Identifier: Apache-2.0

package nimbus

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ant-caor/nimbus/store"
	"github.com/ant-caor/nimbus/store/memory"
)

// hookedL2 is a minimal in-memory VersionedStore test double. Its SetCAS runs an
// optional hook just after minting and before returning, which lets a test open
// the exact window where a newer entry lands in L1 between a fill's SetCAS and
// its install — the L1-stomp race. Only the methods the fill path exercises are
// meaningful; the rest are stubs.
type hookedL2[V any] struct {
	mu       sync.Mutex
	ver      uint64
	val      V
	live     bool
	onSetCAS func() // invoked inside SetCAS, after minting, before returning
}

func (s *hookedL2[V]) Load(_ context.Context, _ string) (store.Entry[V], bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.live {
		return store.Entry[V]{Version: s.ver}, false, nil
	}
	now := time.Now()
	return store.Entry[V]{Value: s.val, Version: s.ver, StoredAt: now, FreshUntil: now.Add(time.Hour), StaleUntil: now.Add(time.Hour)}, true, nil
}

func (s *hookedL2[V]) Get(ctx context.Context, key string) (store.Entry[V], bool, error) {
	return s.Load(ctx, key)
}

func (s *hookedL2[V]) SetCAS(_ context.Context, _ string, val V, expect uint64, freshUntil, staleUntil time.Time, _ []string) (store.Entry[V], error) {
	s.mu.Lock()
	if expect != store.ForceVersion && expect != s.ver {
		cur := s.ver
		s.mu.Unlock()
		return store.Entry[V]{Version: cur}, store.ErrVersionConflict
	}
	s.ver++
	s.val = val
	s.live = true
	e := store.Entry[V]{Value: val, Version: s.ver, StoredAt: time.Now(), FreshUntil: freshUntil, StaleUntil: staleUntil}
	hook := s.onSetCAS
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	return e, nil
}

func (s *hookedL2[V]) CompareAndDelete(_ context.Context, _ string, _ uint64) (uint64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ver++
	s.live = false
	return s.ver, true, nil
}

func (s *hookedL2[V]) Set(_ context.Context, _ string, _ store.Entry[V]) error { return nil }
func (s *hookedL2[V]) Delete(_ context.Context, _ string) error                { return nil }
func (s *hookedL2[V]) DeleteByTag(_ context.Context, _ string) ([]string, error) {
	return nil, nil
}
func (s *hookedL2[V]) Close() error { return nil }

var _ store.VersionedStore[string] = (*hookedL2[string])(nil)

// TestFillVersionGatesL1Install proves the cache routes its L2-minted L1 install
// through a version gate: if a newer entry lands in L1 between the fill's SetCAS
// and its install, the fill must not stomp it with its older value. The hookedL2
// reproduces that window deterministically. Without the gate (a plain L1 Set)
// the later, lower-version install would overwrite the newer entry.
func TestFillVersionGatesL1Install(t *testing.T) {
	ctx := t.Context()
	l1 := memory.New[string]()

	const newerVersion = 999
	l2 := &hookedL2[string]{}
	l2.onSetCAS = func() {
		// A concurrent writer / bus delivery lands a strictly newer entry in L1
		// while the fill is between minting (version 1) and installing.
		now := time.Now()
		newer := store.Entry[string]{
			Value: "newer", Version: newerVersion, StoredAt: now,
			FreshUntil: now.Add(time.Hour), StaleUntil: now.Add(time.Hour),
		}
		if installed, _ := l1.SetIfNewer(ctx, "k", newer); !installed {
			t.Error("seeding the newer L1 entry should have installed")
		}
	}

	loader := func(_ context.Context, _ string) (string, error) { return "filled", nil }
	c, err := NewBuilder[string, string](loader).L1(l1).L2(l2).TTL(time.Hour, 0).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	// The fill mints version 1 for "filled"; the hook then installs version 999.
	// The fill's gated install of version 1 must be rejected, leaving 999 in L1.
	if _, err := c.GetOrLoad(ctx, "k"); err != nil {
		t.Fatal(err)
	}

	got, ok, err := c.Get(ctx, "k")
	if err != nil || !ok {
		t.Fatalf("Get after fill: ok=%v err=%v", ok, err)
	}
	if got != "newer" {
		t.Fatalf("L1 holds %q: the fill stomped the newer entry — version gate did not hold", got)
	}
	if e, _, _ := l1.Get(ctx, "k"); e.Version != newerVersion {
		t.Fatalf("L1 version = %d, want %d (the newer entry must survive)", e.Version, newerVersion)
	}
}

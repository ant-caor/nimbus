// Copyright 2026 Antonio Cabezas Ordóñez
// SPDX-License-Identifier: Apache-2.0

package nimbus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ant-caor/nimbus/store"
	"github.com/ant-caor/nimbus/store/memory"
)

// unreachableL2 is a VersionedStore whose every operation fails with a
// non-conflict connectivity error, modelling an L2 (Redis/Memorystore) outage.
// It is the unit-level counterpart to the toxiproxy integration test.
type unreachableL2[V any] struct{ err error }

func (s unreachableL2[V]) Load(context.Context, string) (store.Entry[V], bool, error) {
	return store.Entry[V]{}, false, s.err
}
func (s unreachableL2[V]) Get(context.Context, string) (store.Entry[V], bool, error) {
	return store.Entry[V]{}, false, s.err
}
func (s unreachableL2[V]) SetCAS(context.Context, string, V, uint64, time.Time, time.Time, []string) (store.Entry[V], error) {
	return store.Entry[V]{}, s.err
}
func (s unreachableL2[V]) CompareAndDelete(context.Context, string, uint64) (uint64, bool, error) {
	return 0, false, s.err
}
func (s unreachableL2[V]) Set(context.Context, string, store.Entry[V]) error { return s.err }
func (s unreachableL2[V]) Delete(context.Context, string) error              { return s.err }
func (s unreachableL2[V]) DeleteByTag(context.Context, string) ([]string, error) {
	return nil, s.err
}
func (s unreachableL2[V]) Close() error { return nil }

var _ store.VersionedStore[string] = unreachableL2[string]{}

// TestL2OutageDegradedReadPath unit-tests the degraded-mode contract: when L2 is
// unreachable, a cold GetOrLoad returns the loader's result (value or
// ErrNotFound) without caching, a real loader error still propagates, and the
// degradation is counted in Stats.L2Errors. The integration suite proves the
// same against real Redis with a toxiproxy cut; this is the fast, deterministic
// version that also keeps the branches covered in the core module.
func TestL2OutageDegradedReadPath(t *testing.T) {
	ctx := t.Context()
	down := unreachableL2[string]{err: errors.New("dial tcp 10.0.0.1:6379: connect: connection refused")}

	build := func(l1 store.Store[string], loader Loader[string, string]) Cache[string, string] {
		c, err := NewBuilder[string, string](loader).L1(l1).L2(down).TTL(time.Hour, 0).Build()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}

	t.Run("successful load returns value, does not cache", func(t *testing.T) {
		l1 := memory.New[string]()
		c := build(l1, func(context.Context, string) (string, error) { return "from-origin", nil })

		v, err := c.GetOrLoad(ctx, "k")
		if err != nil {
			t.Fatalf("cold miss with L2 down must not fail: %v", err)
		}
		if v != "from-origin" {
			t.Fatalf("got %q, want the loader value", v)
		}
		// Nothing un-versioned was written to L1.
		if _, ok, _ := l1.Get(ctx, "k"); ok {
			t.Fatal("degraded fill must not write L1")
		}
		if got := c.Stats().L2Errors; got == 0 {
			t.Fatal("a degraded fill must be counted in Stats.L2Errors")
		}
	})

	t.Run("not-found load returns ErrNotFound, does not cache", func(t *testing.T) {
		l1 := memory.New[string]()
		c := build(l1, func(context.Context, string) (string, error) { return "", ErrNotFound })

		_, err := c.GetOrLoad(ctx, "k")
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("a not-found cold miss with L2 down must return ErrNotFound, got %v", err)
		}
		if _, ok, _ := l1.Get(ctx, "k"); ok {
			t.Fatal("degraded negative fill must not write L1")
		}
		if got := c.Stats().L2Errors; got == 0 {
			t.Fatal("a degraded negative fill must be counted in Stats.L2Errors")
		}
	})

	t.Run("real loader error still propagates", func(t *testing.T) {
		sentinel := errors.New("origin is down too")
		c := build(memory.New[string](), func(context.Context, string) (string, error) { return "", sentinel })

		_, err := c.GetOrLoad(ctx, "k")
		if !errors.Is(err, sentinel) {
			t.Fatalf("a genuine loader error must propagate even during an L2 outage, got %v", err)
		}
		if got := c.Stats().LoadErrors; got == 0 {
			t.Fatal("a real loader failure must be counted in Stats.LoadErrors")
		}
	})
}

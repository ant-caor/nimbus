// Copyright 2026 Antonio Cabezas Ordóñez
// SPDX-License-Identifier: Apache-2.0

package nimbus

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ant-caor/nimbus/invalidation"
	"github.com/ant-caor/nimbus/store/memory"
)

// TestCrossInstanceInvalidationViaBus proves that an Invalidate on one instance
// evicts the entry from another instance's L1, using the in-process fan-out bus
// (no Docker needed). The gcppubsub transport is covered in the integration
// module against the Pub/Sub emulator.
func TestCrossInstanceInvalidationViaBus(t *testing.T) {
	t.Parallel()
	bus := invalidation.NewMem()
	var loads atomic.Int64
	loader := func(_ context.Context, _ string) (int, error) {
		return int(loads.Add(1)), nil
	}
	mk := func() Cache[string, int] {
		c, err := NewBuilder[string, int](loader).
			L1(memory.New[int]()).
			Bus(bus).
			TTL(time.Hour, 0).
			Build()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}

	a, b := mk(), mk()
	ctx := t.Context()

	// Give both subscriber goroutines a moment to register on the bus.
	time.Sleep(50 * time.Millisecond)

	if _, err := a.GetOrLoad(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.GetOrLoad(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := b.Get(ctx, "k"); !ok {
		t.Fatal("b should hold k before invalidation")
	}

	// A invalidates -> bus broadcasts -> B evicts its L1 copy.
	if err := a.Invalidate(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if !eventually(time.Second, func() bool {
		_, ok, _ := b.Get(ctx, "k")
		return !ok
	}) {
		t.Fatal("B's L1 entry should have been evicted via the bus")
	}
	if s := b.Stats(); s.BusEvicts == 0 {
		t.Fatal("expected B to record a bus eviction")
	}
}

// TestBusSkipsOwnOrigin verifies an instance does not act on its own broadcasts
// (it already evicted locally), avoiding a redundant self-eviction loop.
func TestBusSkipsOwnOrigin(t *testing.T) {
	t.Parallel()
	bus := invalidation.NewMem()
	loader := func(_ context.Context, _ string) (int, error) { return 1, nil }
	c, err := NewBuilder[string, int](loader).L1(memory.New[int]()).Bus(bus).TTL(time.Hour, 0).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx := t.Context()
	time.Sleep(50 * time.Millisecond)

	_, _ = c.GetOrLoad(ctx, "k")
	_ = c.Set(ctx, "k", 5) // publishes from this instance's origin
	time.Sleep(50 * time.Millisecond)
	if s := c.Stats(); s.BusEvicts != 0 {
		t.Fatalf("instance should ignore its own broadcasts, got %d bus evicts", s.BusEvicts)
	}
}

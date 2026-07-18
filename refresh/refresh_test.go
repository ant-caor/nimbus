// Copyright 2026 Antonio Cabezas Ordóñez
// SPDX-License-Identifier: Apache-2.0

package refresh

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRequestBoundDedups(t *testing.T) {
	t.Parallel()
	r := NewRequestBound(time.Second, 0)
	defer func() { _ = r.Close() }()

	var runs atomic.Int64
	release := make(chan struct{})
	for range 10 {
		r.Schedule("k", func(_ context.Context) error {
			runs.Add(1)
			<-release // hold the slot so duplicates are suppressed
			return nil
		})
	}
	time.Sleep(50 * time.Millisecond)
	if got := runs.Load(); got != 1 {
		t.Fatalf("concurrent schedules for one key should run once, got %d", got)
	}
	close(release)
}

// TestRequestBoundCapsConcurrency proves the fan-out bound: with a cap of N and
// many more distinct stale keys scheduled at once (each loader blocking), at
// most N revalidations launch and run concurrently; the rest are dropped on
// saturation rather than spawning an unbounded burst of goroutines and loader
// calls. Deterministic: the cap tokens are acquired synchronously inside
// Schedule, and the launched loaders hold them until released.
func TestRequestBoundCapsConcurrency(t *testing.T) {
	t.Parallel()
	const maxConc, keys = 4, 64
	r := NewRequestBound(time.Second, maxConc)

	var concurrent, maxConcurrent atomic.Int64
	entered := make(chan struct{}, keys)
	release := make(chan struct{})
	loader := func(_ context.Context) error {
		c := concurrent.Add(1)
		for { // record the high-water mark
			m := maxConcurrent.Load()
			if c <= m || maxConcurrent.CompareAndSwap(m, c) {
				break
			}
		}
		entered <- struct{}{}
		<-release
		concurrent.Add(-1)
		return nil
	}

	launched := 0
	for i := 0; i < keys; i++ {
		if r.Schedule(fmt.Sprintf("k%d", i), loader) {
			launched++
		}
	}
	if launched != maxConc {
		t.Fatalf("launched %d refreshes, want exactly the cap %d (the rest must drop on saturation)", launched, maxConc)
	}

	// Wait until all maxConc launched loaders are actually inside (blocked on
	// release), then assert the cap was reached but never exceeded.
	for i := 0; i < maxConc; i++ {
		<-entered
	}
	if got := maxConcurrent.Load(); got != maxConc {
		t.Fatalf("max concurrent refreshes = %d, want exactly the cap %d", got, maxConc)
	}
	select {
	case <-entered:
		t.Fatal("more than cap loaders ran: fan-out was not bounded")
	default:
	}

	close(release)
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestRequestBoundReclaimsTokens proves a concurrency token is returned when a
// refresh finishes, so the cap measures *concurrent* refreshes, not a lifetime
// total. It guards the release path (the deferred <-r.sem) against a future
// defer-reordering regression: fill the cap, let those refreshes complete, then
// confirm a fresh key can still launch — which is only possible if the tokens
// were reclaimed.
func TestRequestBoundReclaimsTokens(t *testing.T) {
	t.Parallel()
	const maxConc = 4
	r := NewRequestBound(time.Second, maxConc)
	defer func() { _ = r.Close() }()

	// Saturate the cap with refreshes that complete immediately.
	var wg sync.WaitGroup
	wg.Add(maxConc)
	for i := 0; i < maxConc; i++ {
		if !r.Schedule(fmt.Sprintf("k%d", i), func(_ context.Context) error { wg.Done(); return nil }) {
			t.Fatalf("schedule %d within the cap should launch", i)
		}
	}
	wg.Wait() // all launched refreshes ran to completion

	// The token release happens in a defer just after the loader returns, so it
	// lands slightly after wg.Done — poll until a brand-new key can launch, which
	// requires at least one token to have been reclaimed.
	deadline := time.Now().Add(2 * time.Second)
	for !r.Schedule("fresh", func(_ context.Context) error { return nil }) {
		if time.Now().After(deadline) {
			t.Fatal("no token was reclaimed after the saturating wave completed")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestBackgroundRunsAndCloses(t *testing.T) {
	t.Parallel()
	b := NewBackground(2, 16, time.Second)

	var runs atomic.Int64
	var wg sync.WaitGroup
	wg.Add(5)
	for i := 0; i < 5; i++ {
		b.Schedule(fmt.Sprintf("k%d", i), func(_ context.Context) error {
			runs.Add(1)
			wg.Done()
			return nil
		})
	}
	wg.Wait()
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if got := runs.Load(); got != 5 {
		t.Fatalf("runs = %d, want 5", got)
	}
	// Schedule after Close is a no-op.
	b.Schedule("x", func(_ context.Context) error { runs.Add(1); return nil })
	if got := runs.Load(); got != 5 {
		t.Fatalf("schedule after close should not run, runs = %d", got)
	}
}

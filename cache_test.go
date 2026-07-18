// Copyright 2026 Antonio Cabezas Ordóñez
// SPDX-License-Identifier: Apache-2.0

package nimbus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ant-caor/nimbus/internal/clock"
)

func TestGetOrLoadStampedeCollapse(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	loader := func(_ context.Context, _ string) (int, error) {
		calls.Add(1)
		time.Sleep(20 * time.Millisecond) // widen the race window
		return 42, nil
	}
	c, err := NewBuilder[string, int](loader).TTL(time.Minute, 0).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	const n = 200
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := c.GetOrLoad(context.Background(), "k")
			if err != nil {
				errCh <- err
				return
			}
			if v != 42 {
				errCh <- fmt.Errorf("got %d, want 42", v)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		t.Fatal(e)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("loader called %d times, want exactly 1 (stampede not collapsed)", got)
	}
}

func TestGetOrLoadCachesValue(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	loader := func(_ context.Context, _ string) (int, error) {
		return int(calls.Add(1)), nil
	}
	c, err := NewBuilder[string, int](loader).TTL(time.Minute, 0).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx := t.Context()

	v1, err := c.GetOrLoad(ctx, "k")
	if err != nil || v1 != 1 {
		t.Fatalf("first load: v=%d err=%v", v1, err)
	}
	v2, err := c.GetOrLoad(ctx, "k")
	if err != nil || v2 != 1 {
		t.Fatalf("cached read should return 1, got v=%d err=%v", v2, err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("loader called %d times, want 1", got)
	}
}

func TestNegativeCaching(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	loader := func(_ context.Context, _ string) (int, error) {
		calls.Add(1)
		return 0, ErrNotFound
	}
	c, err := NewBuilder[string, int](loader).TTL(time.Minute, 0).NegativeTTL(time.Minute).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx := t.Context()

	for i := 0; i < 3; i++ {
		_, err := c.GetOrLoad(ctx, "missing")
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("call %d: want ErrNotFound, got %v", i, err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("negative cache should load once, loader called %d times", got)
	}
}

func TestFreshnessExpiryWithClock(t *testing.T) {
	t.Parallel()
	mc := clock.NewMock(time.Now())
	var calls atomic.Int64
	loader := func(_ context.Context, _ string) (int, error) {
		return int(calls.Add(1)), nil
	}
	c, err := NewBuilder[string, int](loader).TTL(time.Minute, 0).Clock(mc).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx := t.Context()

	if v, _ := c.GetOrLoad(ctx, "k"); v != 1 {
		t.Fatalf("first load got %d, want 1", v)
	}
	if v, _ := c.GetOrLoad(ctx, "k"); v != 1 {
		t.Fatalf("cached read got %d, want 1", v)
	}
	mc.Advance(2 * time.Minute) // past the fresh window
	if v, _ := c.GetOrLoad(ctx, "k"); v != 2 {
		t.Fatalf("after expiry got %d, want reload to 2", v)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("loader called %d times, want 2", got)
	}
}

func TestInvalidateTagEvicts(t *testing.T) {
	t.Parallel()
	loader := func(_ context.Context, _ string) (int, error) { return -1, nil }
	c, err := NewBuilder[string, int](loader).TTL(time.Hour, 0).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx := t.Context()

	if err := c.Set(ctx, "a", 100, WithTags("grp")); err != nil {
		t.Fatal(err)
	}
	if err := c.Set(ctx, "b", 200, WithTags("grp")); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := c.Get(ctx, "a"); !ok || v != 100 {
		t.Fatalf("get a before invalidation: v=%d ok=%v", v, ok)
	}
	if err := c.InvalidateTag(ctx, "grp"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := c.Get(ctx, "a"); ok {
		t.Fatal("a should be evicted after tag invalidation")
	}
	if _, ok, _ := c.Get(ctx, "b"); ok {
		t.Fatal("b should be evicted after tag invalidation")
	}
}

func TestClosedReturnsErrClosed(t *testing.T) {
	t.Parallel()
	loader := func(_ context.Context, _ string) (int, error) { return 1, nil }
	c, err := NewBuilder[string, int](loader).Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetOrLoad(context.Background(), "k"); !errors.Is(err, ErrClosed) {
		t.Fatalf("want ErrClosed after Close, got %v", err)
	}
}

func TestBuildRequiresLoader(t *testing.T) {
	t.Parallel()
	if _, err := (&Builder[string, int]{}).Build(); err == nil {
		t.Fatal("expected error when loader is nil")
	}
}

// TestRefreshCountedOncePerStaleWave verifies that many concurrent stale reads
// of one key dedupe to a single counted refresh (not one per reader).
func TestRefreshCountedOncePerStaleWave(t *testing.T) {
	t.Parallel()
	mc := clock.NewMock(time.Now())
	var loads atomic.Int64
	release := make(chan struct{})
	loader := func(_ context.Context, _ string) (int, error) {
		if loads.Add(1) >= 2 {
			<-release // hold the refresh load in flight so duplicates are suppressed
		}
		return 1, nil
	}
	c, err := NewBuilder[string, int](loader).TTL(time.Minute, time.Hour).Clock(mc).Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { close(release); _ = c.Close() })
	ctx := t.Context()

	if _, err := c.GetOrLoad(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	mc.Advance(2 * time.Minute) // now stale, still servable

	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = c.GetOrLoad(ctx, "k") // serves stale immediately, schedules a refresh
		}()
	}
	wg.Wait()

	if s := c.Stats(); s.Refreshes != 1 {
		t.Fatalf("Refreshes = %d, want 1 (concurrent stale reads should dedupe to one refresh)", s.Refreshes)
	}
}

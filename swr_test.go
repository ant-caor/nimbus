// Copyright 2026 Antonio Cabezas Ordóñez
// SPDX-License-Identifier: Apache-2.0

package nimbus

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ant-caor/nimbus/internal/clock"
)

func eventually(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

func TestStaleWhileRevalidate(t *testing.T) {
	t.Parallel()
	mc := clock.NewMock(time.Now())
	var calls atomic.Int64
	loader := func(_ context.Context, _ string) (int, error) {
		return int(calls.Add(1)), nil
	}
	c, err := NewBuilder[string, int](loader).
		TTL(time.Minute, 10*time.Minute). // fresh 1m, stale window +10m
		Clock(mc).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx := t.Context()

	if v, _ := c.GetOrLoad(ctx, "k"); v != 1 {
		t.Fatalf("first load got %d, want 1", v)
	}
	mc.Advance(2 * time.Minute) // now stale, still servable

	v, err := c.GetOrLoad(ctx, "k")
	if err != nil || v != 1 {
		t.Fatalf("stale read should return stale value immediately: v=%d err=%v", v, err)
	}
	// Revalidation happens out of band and updates the value to 2.
	if !eventually(2*time.Second, func() bool {
		got, ok, _ := c.Get(ctx, "k")
		return ok && got == 2
	}) {
		t.Fatalf("revalidation did not update value; calls=%d", calls.Load())
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("loader called %d times, want 2 (initial + one refresh)", got)
	}
	if s := c.Stats(); s.StaleHits != 1 || s.Refreshes != 1 {
		t.Fatalf("stats StaleHits=%d Refreshes=%d, want 1 and 1", s.StaleHits, s.Refreshes)
	}
}

func TestMaxTTLCapsLifetime(t *testing.T) {
	t.Parallel()
	mc := clock.NewMock(time.Now())
	var calls atomic.Int64
	loader := func(_ context.Context, _ string) (int, error) {
		return int(calls.Add(1)), nil
	}
	c, err := NewBuilder[string, int](loader).
		TTL(time.Hour, 0).        // fresh 1h on paper
		MaxTTL(10 * time.Second). // but capped to 10s absolute
		Clock(mc).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx := t.Context()

	if v, _ := c.GetOrLoad(ctx, "k"); v != 1 {
		t.Fatalf("first load got %d, want 1", v)
	}
	mc.Advance(20 * time.Second) // past the 10s cap, under the 1h fresh TTL
	if v, _ := c.GetOrLoad(ctx, "k"); v != 2 {
		t.Fatalf("MaxTTL should force a reload; got %d, want 2", v)
	}
}

func TestApplyJitterBounds(t *testing.T) {
	t.Parallel()
	base := time.Minute
	lo := time.Duration(float64(base) * 0.5)
	hi := time.Duration(float64(base) * 1.5)
	for range 1000 {
		got := applyJitter(base, 0.5)
		if got < lo || got > hi {
			t.Fatalf("jitter %v out of bounds [%v, %v]", got, lo, hi)
		}
	}
}

func TestBackgroundRefreshMode(t *testing.T) {
	t.Parallel()
	mc := clock.NewMock(time.Now())
	var calls atomic.Int64
	loader := func(_ context.Context, _ string) (int, error) {
		return int(calls.Add(1)), nil
	}
	c, err := NewBuilder[string, int](loader).
		TTL(time.Minute, 10*time.Minute).
		BackgroundRefresh(2).
		Clock(mc).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx := t.Context()

	_, _ = c.GetOrLoad(ctx, "k")
	mc.Advance(2 * time.Minute)
	if v, _ := c.GetOrLoad(ctx, "k"); v != 1 {
		t.Fatalf("stale serve got %d, want 1", v)
	}
	if !eventually(2*time.Second, func() bool {
		got, ok, _ := c.Get(ctx, "k")
		return ok && got == 2
	}) {
		t.Fatal("background refresh did not update the value")
	}
}

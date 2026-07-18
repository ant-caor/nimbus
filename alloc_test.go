// Copyright 2026 Antonio Cabezas Ordóñez
// SPDX-License-Identifier: Apache-2.0

package nimbus

import (
	"context"
	"testing"
	"time"
)

// TestZeroAllocReadHotPaths turns invariant #4 into a hard gate for the read hot
// paths: Get and GetOrLoad on an L1 hit must allocate zero times per op, or
// `go test` fails — instead of the regression only surfacing in a benchmark
// someone has to remember to read. See CLAUDE.md and DESIGN.md.
//
// Cache-level Set is deliberately NOT gated here: with a bus configured it must
// build and publish an invalidation Event (a real allocation), so it is not a
// zero-alloc path. The "L1 Set" that the README perf table reports as zero-alloc
// is the store-level Set, gated in store/memory (TestZeroAllocStore).
//
// (Note: cache Set currently also builds the Event even when no bus is
// configured, discarding it in publish() — a wasted allocation in the no-bus
// path that is a candidate micro-optimization, tracked separately.)
func TestZeroAllocReadHotPaths(t *testing.T) {
	loader := func(_ context.Context, _ string) (int, error) { return 42, nil }
	c, err := NewBuilder[string, int](loader).TTL(time.Hour, 0).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx := t.Context()

	// Prime L1 so every measured call is a fresh hot hit, never a miss/load.
	if _, err := c.GetOrLoad(ctx, "k"); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		fn   func()
	}{
		{"GetOrLoad", func() { _, _ = c.GetOrLoad(ctx, "k") }},
		{"Get", func() { _, _, _ = c.Get(ctx, "k") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := testing.AllocsPerRun(1000, tc.fn); got != 0 {
				t.Errorf("%s allocated %v alloc(s)/op on an L1 hit; invariant #4 requires 0", tc.name, got)
			}
		})
	}
}

// TestZeroAllocReadHotPathsIntKeys gates the integer-key fast path in
// defaultKeyString. The zero-alloc invariant is unconditional only for string
// keys, but integer keys must not regress to fmt.Sprint's two allocations:
//   - a small-magnitude key (|n| < 100) hits strconv's static table and stays
//     zero-alloc end to end, proving non-string keys *can* be zero-alloc;
//   - a large key costs at most the one unavoidable allocation for the rendered
//     key string, never more.
func TestZeroAllocReadHotPathsIntKeys(t *testing.T) {
	loader := func(_ context.Context, _ int) (int, error) { return 42, nil }
	c, err := NewBuilder[int, int](loader).TTL(time.Hour, 0).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx := t.Context()

	const smallKey, largeKey = 7, 1234567
	for _, k := range []int{smallKey, largeKey} {
		if _, err := c.GetOrLoad(ctx, k); err != nil { // prime L1
			t.Fatal(err)
		}
	}

	cases := []struct {
		name string
		fn   func()
		max  float64
	}{
		{"GetOrLoad/small", func() { _, _ = c.GetOrLoad(ctx, smallKey) }, 0},
		{"Get/small", func() { _, _, _ = c.Get(ctx, smallKey) }, 0},
		{"GetOrLoad/large", func() { _, _ = c.GetOrLoad(ctx, largeKey) }, 1},
		{"Get/large", func() { _, _, _ = c.Get(ctx, largeKey) }, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := testing.AllocsPerRun(1000, tc.fn); got > tc.max {
				t.Errorf("%s allocated %v alloc(s)/op on an L1 hit; want <= %v", tc.name, got, tc.max)
			}
		})
	}
}

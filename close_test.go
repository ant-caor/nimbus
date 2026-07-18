// Copyright 2026 Antonio Cabezas Ordóñez
// SPDX-License-Identifier: Apache-2.0

package nimbus

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeRefresher records how many times Close was called and returns a fixed
// error, so a test can assert both idempotency and error propagation.
type fakeRefresher struct {
	closeErr   error
	closeCalls int
}

func (f *fakeRefresher) Schedule(string, func(ctx context.Context) error) bool { return false }

func (f *fakeRefresher) Close() error {
	f.closeCalls++
	return f.closeErr
}

func TestCloseIdempotentAndPropagatesRefresherError(t *testing.T) {
	loader := func(context.Context, string) (int, error) { return 0, nil }
	c, err := NewBuilder[string, int](loader).TTL(time.Minute, 0).Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	cc, ok := c.(*cache[string, int])
	if !ok {
		t.Fatalf("expected *cache[string,int], got %T", c)
	}

	// Swap in a refresher whose Close fails, so we can observe the error and the
	// call count across repeated Close calls.
	sentinel := errors.New("refresher close failed")
	fr := &fakeRefresher{closeErr: sentinel}
	cc.refresher = fr

	// First Close must surface the refresher's error.
	if err := c.Close(); !errors.Is(err, sentinel) {
		t.Fatalf("first Close: got %v, want %v", err, sentinel)
	}
	// Repeat calls must return the same error without redoing the work.
	if err := c.Close(); !errors.Is(err, sentinel) {
		t.Fatalf("second Close: got %v, want %v", err, sentinel)
	}
	if fr.closeCalls != 1 {
		t.Fatalf("refresher Close called %d times, want 1 (Close must be idempotent)", fr.closeCalls)
	}
	if !cc.closed.Load() {
		t.Fatal("closed flag should be set after Close")
	}
}

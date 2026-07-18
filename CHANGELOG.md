# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the project is **pre-1.0, minor releases may contain breaking changes**;
patch releases never do.

## [Unreleased]

### Added

- `store.Metrics`, an optional interface a `store.Store` may implement to report
  operational counters (`Evictions() uint64`, `Len() int`). `Cache.Stats`
  populates its `Evictions` and `L1Len` fields from the L1 tier when it
  implements this interface (the bundled `store/memory` does), replacing a
  previously undiscoverable inline interface check.

### Changed

- `Cache.Close` is now idempotent — repeat calls return the same result without
  redoing the shutdown — and it now reports the refresher's `Close` error
  instead of silently discarding it.

## [0.1.0] - 2026-06-21

### Added

- Core cache: a generic `Cache[K, V]` with a fluent `Builder`, read-through
  `GetOrLoad`, `Get`, `Set`, `Invalidate`, `InvalidateTag`, `Stats`, and `Close`.
- Multi-tier storage: an in-process L1 (`store/memory`, a sharded LRU+TTL) behind
  the `store.Store` interface, and a versioned Redis L2 (`redisstore`) as the
  authoritative source of truth.
- Stampede protection via singleflight: concurrent misses collapse to one load.
- Stale-while-revalidate with request-bound (default) and background refresh
  modes, TTL jitter, `MaxTTL`, and negative caching.
- The versioned **fill invariant** (CAS on write) that closes the
  fill-after-invalidate stale-read race.
- Cross-instance invalidation bus (`invalidation.Bus`) with transports:
  in-process (`invalidation.Mem`) and Redis Pub/Sub (`invalidation/redispubsub`)
  in the core module, plus GCP Pub/Sub pull and push in a **separate module**
  (`github.com/ant-caor/nimbus/invalidation/gcppubsub`).
- Tag-based invalidation (`WithTags`, `InvalidateTag`).
- OpenTelemetry metrics in a **separate module**
  (`github.com/ant-caor/nimbus/metrics`) using asynchronous instruments, so the
  core module carries no OpenTelemetry dependency.
- **Cloud-agnostic dependency layout.** The core module
  `github.com/ant-caor/nimbus` requires only `rueidis` and `golang.org/x/sync`;
  GCP (`invalidation/gcppubsub`) and OpenTelemetry (`metrics`) are opt-in sibling
  modules, so a dependent never pulls the GCP/gRPC/protobuf or OTel trees unless
  it imports them. Import paths are unchanged; using GCP or OTel now adds a
  `go get` of the respective module. The Redis Pub/Sub bus + Redis L2 give a
  fully GCP-free coherence path that runs on any cloud or on-prem.
- Examples: an L1-only `examples/basic` (in the core module), a deployable
  `examples/cloudrun` (distroless Dockerfile + Terraform + OIDC push), a
  cloud-agnostic `examples/redisbus` (L1 + Redis L2 + Redis Pub/Sub bus, no GCP
  and no emulator), and a local `demo/local` (docker compose with Redis and the
  Pub/Sub emulator); the GCP-using and standalone examples are their own modules
  and are never published.
- `examples/redisbus`: a runnable demonstration of the GCP-free cross-instance
  coherence path — two in-process cache instances sharing one Redis show a `Set`
  on one evicting the other's L1 over the Redis Pub/Sub bus.
- `examples/cloudrun` now wires the `metrics` module after `Build()` (gated by
  `METRICS=1`), registering the OpenTelemetry adapter against a meter backed by a
  stdout exporter to show end-to-end observability without a heavy cloud client.
- Documentation: `README.md` (now including an **Alternatives / comparison**
  section against groupcache and Ristretto/Otter) and a design write-up in
  `DESIGN.md`.
- Integration test suite (separate module) running against real Redis and the
  Pub/Sub emulator via testcontainers.
- `store.TombstoneTTLer`, an optional interface a `VersionedStore` may implement
  to report its tombstone lifetime (implemented by `redisstore`). `Build` uses it
  to reject a configuration whose L2 tombstone TTL does not exceed the refresh
  timeout, which would reopen the fill-after-invalidate race.
- `store.ConditionalStore`, an optional interface a `Store` may implement for a
  version-gated install (`SetIfNewer`), implemented by `store/memory`.
- `Builder.MaxConcurrentRefresh(n)` to cap concurrent request-bound
  revalidations (default 16 per instance).
- **In-process OIDC verification on the GCP Pub/Sub push handler.**
  `gcppubsub.WithPushAuth(audience string, allowedServiceAccounts ...string)`,
  an opt-in option on `NewPush`, makes `PushBus.Handler()` verify the Pub/Sub
  OIDC token before dispatching: it validates the Google signature and expiry
  (via `idtoken`) and then enforces the claims `idtoken` does not check itself —
  a Google issuer and a verified (`email_verified`) email — plus the `aud` claim
  against the configured audience and the `email` claim against a service-account
  allowlist. The handler returns 401 for a missing/malformed `Authorization`
  header or a token that fails signature, issuer, or verified-email validation,
  403 for an audience mismatch or a non-allowlisted account, and 204 only after
  verification passes. This is defense-in-depth
  alongside the Cloud Run `run.invoker` IAM binding; the existing `PushHandler`
  stays available unauthenticated for advanced users. The `examples/cloudrun`
  service enables it by default (`PUSH_AUDIENCE` / `PUSH_SA_EMAIL`), and the
  Terraform now sets an explicit `audience` on the subscription's `oidc_token`.
  No new dependency: `idtoken` ships with the `google.golang.org/api` module the
  transport already requires, and the core module stays GCP-free.

### Changed

- The default key renderer now maps integer keys via `strconv` instead of
  `fmt.Sprint`, cutting a large-integer key on the read hot path from two
  allocations to at most one (small magnitudes stay zero-alloc). The
  zero-allocation guarantee is now documented as unconditional for `string` keys
  (and allocation-free `KeyString` codecs); non-string, non-integer keys should
  supply `KeyString` for a zero-allocation hot path.
- L2 per-key versions are now monotonic across **TTL expiry**. Previously the
  version was derived from the live Redis hash and restarted at 1 once the entry
  or its tombstone expired, so the "single monotonic version counter per key"
  guarantee held only within a key's lifetime. The Lua scripts now seed an
  absent key's version from the server clock (`(unixMillis << 10) | seq`) instead
  of zero, so a re-mint after expiry always exceeds any prior version — without a
  second key, a hash tag, or a cross-slot script. **Version values are now large,
  opaque, clock-seeded numbers rather than a 1-based count**; callers must already
  treat `Entry.Version` as opaque-and-monotonic, never assuming it starts at 1.
  Covered by `TestVersionFloorMonotonicAcrossExpiry` and
  `TestVersionFloorMonotonicAcrossTombstoneExpiry`.

### Fixed

- **Scale tag invalidation.** `DeleteByTag` resolved a tag with `SMEMBERS` (a
  whole-set reply) and tombstoned each member in a serial loop — N+1 round trips
  and an O(N) server-side reply per invalidation. It now iterates members with
  `SSCAN` (bounded replies) and pipelines the per-key tombstone scripts in
  batches (one round trip per 256 keys, cluster-safe — each stays a single-key
  script), and `addTags` pipelines its `SADD`/TTL refreshes in one round trip.
  `InvalidateTag` broadcasts the resolved keys in bounded chunks (each with its
  own event ID, so a receiver's dedupe ring does not drop all but the first)
  instead of one oversized message. Behavior is unchanged: per-key versioned
  tombstones, partial-but-resumable on error, and the set is never blind-deleted.
  Covered by `TestDeleteByTagLargeTagScans`,
  `TestDeleteByTagMintsMonotonicTombstones`, and
  `TestDeleteByTagDoesNotNukeNewMembers`.
- **Bound the request-bound refresh fan-out.** `RequestBound` previously spawned
  one detached goroutine and one loader call per distinct stale key, capped only
  by per-key dedup — so a synchronized stale wave (a deploy or a cold autoscaled
  fleet) could fan out into a thundering herd on the origin. Concurrent
  revalidations are now bounded by a semaphore (`MaxConcurrentRefresh`, default
  16 per instance) that drops on saturation, mirroring the background pool's
  queue-full drop; stale-while-revalidate keeps serving meanwhile. Covered by
  `TestRequestBoundCapsConcurrency` and `TestRequestBoundReclaimsTokens`.
- Define and enforce a **degraded-mode contract when L2 is unreachable**. A
  `GetOrLoad` cold miss whose loader succeeds but whose versioned L2 write-back
  fails with a non-conflict (connectivity) error previously surfaced the raw Redis
  error, failing a request the origin had already served. It now **fails open**:
  the loader's result is returned (the value, or `ErrNotFound`) without writing an
  uncoordinated entry to L1, so an L2 outage degrades to origin-load amplification
  (bounded by singleflight) rather than a failed read or a stale cache. Read-only
  paths (fresh/stale L1 hits, `Get`) keep serving; the write path
  (`Set`/`Invalidate`/`InvalidateTag`) still hard-fails since the write to the
  source of truth did not land. New `Stats.L2Errors` counts degraded fills.
  Proven against real Redis with a toxiproxy cut/heal (`TestL2OutageDegradedModeContract`).
- Close a narrow **L1-stomp** window: every L2-minted install into L1 is now
  version-gated (`SetIfNewer`), so a slow fill cannot overwrite a newer entry that
  a concurrent `Set` or a bus-delivered eviction placed in L1 between the fill's
  `SetCAS` and its install. The L1 stays best-effort — a `Store` without
  `ConditionalStore` falls back to an unconditional install, and bus eviction
  stays unconditional. Covered by `TestSetIfNewer` and `TestFillVersionGatesL1Install`.
- Enforce the fill invariant for **negative** entries: a known-absent result is
  now cached only through a version-gated CAS (a tombstone written iff L2 is still
  at the version read before the loader ran). Previously the negative install
  bypassed the CAS, so a value written while a not-found loader was in flight
  could be masked by a stale negative for the whole `NegativeTTL`. Covered by
  `TestNegativeFillInvariantUnderWrite`.

[Unreleased]: https://github.com/ant-caor/nimbus/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/ant-caor/nimbus/releases/tag/v0.1.0

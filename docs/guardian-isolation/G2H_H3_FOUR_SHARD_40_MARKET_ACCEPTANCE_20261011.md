# G2H H3 | Four-Shard / 40-Market Offline Acceptance Atom

Date: 2026-10-11 | base HEAD `d698c00`
Scope: H-04 from `G2H_REQUIREMENTS_FROZEN_20261011.md`.
Status: FOUR-SHARD FUNCTIONAL TESTS PASSED; H3 / G2H OVERALL NOT ACCEPTED.

## Existing production-source architecture, not a new transport

`packages/market-data/src/polymarket-sharded.ts` owns four independent
`SinglePolymarketMarketDataAdapter` instances, each with bounded
transport/ingress, token-sync epoch, reconnect and normalization. Its
`aggregateConnectionState()` reports `DISCONNECTED` if ANY active
shard is disconnected, and `CONNECTING` until ALL active shards are
connected. `handlers.onGap` receives the actual affected `tokenIds`
from the child adapter. `onError` carries NO shard identity.

G2H `GuardianStandaloneBookObserver` uses this existing adapter,
not EngineService or a new socket/parser. Previous H3 erroneously
invalidated ALL Guardian book channels on aggregate `DISCONNECTED`
or unscoped `onError`. Its sole `connected` flag also suppressed
verified normalized books from three already connected shards when
the fourth was slow to connect.

## Changes limited to required H-04 business correctness

- At startup `CONNECTING` permits observing verified NORMALIZED
  snapshots from any already connected child shard. This does NOT
  assert READY; the existing paired-book channel still needs fresh,
  correct market/token identity, COMPLETE UP and DOWN books, verified
  adapter sync/epoch and freshness/skew.
- At aggregate `DISCONNECTED` or `STALE`, the observer checks real
  adapter per-token sync and invalidates only affected complete market
  pairs, preserving unaffected healthy shards. Token-scoped `onGap`
  remains the first source of truth. If a failure is unscoped (no
  unsynced pair identifiable), it FAILS CLOSED across all markets,
  not guessing shard provenance.
- A generic `onError` now uses the same rule: attributed token sync
  gap blocks affected pairs; unattributable error blocks all.
  `STOPPED` continues to clear all pairs, as does aborted runner exit.
- On `observe`, per-token synchronization and stored snapshot epochs
  are rechecked against the adapter. Thus delayed or absent gap
  callbacks cannot keep a previously ACTIVE pair READY.
  An unpaired *newly synchronized* UP book remains bounded in
  the two-snapshot current-epoch cache while DOWN is not yet synced.
  This distinction avoids dropping valid first books during recovery.
- No change to adapter/source parsing, rest/min-size hydration, wallet,
  order execution, G3 admission, production process or DB.

## Reproducible offline acceptance tests

`tests/guardian-standalone-book-shards.test.ts` with FOUR real
`ScriptedBoundedMarketTransport` instances and the normal production
sharded adapter, NO INTERNET:

1. **H3-4S-01:** 40 markets / 80 token outcomes, 20 tokens per shard,
   all 40 independent normalized paired channels become READY.
2. **H3-4S-02:** shard 1 disconnects after 40 READY. The affected 10
   become BLOCKED while other 30 remain READY; reconnect has 10
   BLOCKED until both new UP and DOWN books. The other three
   physical streams are not restarted.
3. **H3-4S-03:** a different shard disconnects and formerly READY
   pairs cannot keep stale books.
4. **H3-4S-04:** a silent `markTokenGappedForTest` without callback
   blocks that pair immediately at `observe`, but does not blind other
   seven markets.
5. **H3-4S-05:** silent token-epoch rotation blocks cached READY;
   only new synchronized pair can recover, others remain READY.
6. **H3-4S-06:** fourth socket's subscribe is delayed; the
   first 30 pairs become READY while overall facade stays CONNECTING.
   Once fourth connects and publishes new full books all 40 READY.

Legacy standalone one-shard / eight-market tests remain unchanged:
`tests/guardian-standalone-book-observer.test.ts` (5 cases).

## H-04 limits not yet proved

- No live socket, real source timestamp distributions, process
  supervision, multiple-hour sustained four-shard failure cycles,
  40-market CPU/RSS/heap p95/p99, bounded-ingress overflow/replay
  measured workload, or latency of real reconnection was asserted.
- Aggregate unscoped errors must fail closed globally; this can
  decrease temporary availability versus a future shard-indexed
  observer callback, but does not impact book-trust correctness.
- H4 still owns representative capacity measurements; G2I owns
  one independently supervised Guardian process; G3 owns
  single-writer atomic order admission. None deployed here.

## Verification and operational boundaries

Verified on the local authorized laptop:
- **364/364 tests PASS in 27 test files**, including H3 four-shard
  `guardian-standalone-book-shards.test.ts` 6/6, original standalone
  observer 5/5, and disposable PostgreSQL integration 14/14.
- `pnpm --filter @pm/db typecheck` PASS.
- `pnpm exec tsc -p apps/engine/tsconfig.guardian-source.json --noEmit` PASS.
- `pnpm exec tsc -p apps/engine/tsconfig.forensic.json --noEmit` PASS.
- `pnpm --filter @pm/core typecheck` PASS.
- `git diff --check` PASS.
- `PM_FORENSIC_STAGE=08_current` was set in tests.
- NOTE: the 40-market/4-shard test is a deterministic scripted OFFLINE
  functional test. It is NOT a real socket 40-market throughput soak,
  and the old signed consumer synthetic 100Hz test does not measure
  live traffic rate, resource ceiling or PostgreSQL p99.
- Overall G2H is **NOT ACCEPTED**. H4 measured load and the named
  H2 source checkpoint/FS durability conditions remain.

No VPS/production database/production Guardian, shadow, wallet,
order submission, deploy, push or merge. Add optional improvements
to `G2H_DEFERRED_IMPROVEMENTS_BACKLOG.md` in this same commit.

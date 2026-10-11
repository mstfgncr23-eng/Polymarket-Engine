# G2H H2 | Temporary fail-closed Guardian recovery retention hold
Date: 2026-10-11 | Base commit `24f98c2`
**Status: temporary safety patch only. H-02/H-03/H-05 PARTIAL; G2H NOT ACCEPTED.**

## KNOWN (source-first root cause)

- `RetentionManager.runCycle()` previously pruned old
  `engine_checkpoints` in the regular batched drains BEFORE it
  considered `compactDurableTransitionsThrough`. Its compaction
  cutoff used the **second latest verified checkpoint** but did
  not inspect each Guardian's independently stored pinned receipt.
- The G2H v2 Guardian ACK is an on-disk **consumer-only**
  observation receipt, not an execution/order acknowledgement.
  The Engine's retention process has no authoritative
  per-consumer pin/lease handshake, so even two verified producer
  checkpoints cannot establish that a lagging reader has moved.
- The previous isolated real PostgreSQL test demonstrated that
  destructive compaction can strand a lagging reader at cursor 1
  while a healthy reader, already rebased, continues from 5 to 6.
  The lagging reader HALTs rather than fabricating skipped fills.
- A predicate checked before a separate pruning transaction
  would create TOCTOU races, and an unproven timeout-based
  assumption about reader liveness would not be an acceptable
  business correctness fence.

## Minimal implemented control

`apps/engine/src/retention-manager.ts` adds
`RetentionConfig.guardianRecoveryHold`:
- **Defaults TRUE** for `defaultRetentionConfig()`, including
  future manually constructed managers (fail-closed default).
- Set **TRUE explicitly** by `retentionConfigFromEngineEnv()`,
  which is used by the Engine runtime and cleanup CLI.
- When TRUE, removes the **checkpoint** drain and skips the
  **durable compaction** branch in `RetentionManager.runCycle()`.
  No destructive canonical recovery deletions are attempted
  through this automatically scheduled manager.
- Other independent retention lanes (telemetry, monitor/control,
  MCP idempotency, human local archive) retain their preexisting
  rules; this change does not grant economic rights to Guardian.
- Returns `guardianRecoveryHeld=true` in cycle results so
  operators and regression fixtures can tell a safe hold from
  an inactive workload.
- Older legacy maintenance **test** cases set
  `guardianRecoveryHold:false` deliberately to validate
  existing compaction/prune implementation. Neither production
  engine config nor CLI exposes an opt-out environment variable.
- Direct privileged `EventJournal.compactDurableTransitionsThrough`
  and `pruneOldCheckpoints` methods are **NOT** guarded by this
  manager policy. Operator/manual direct-maintenance procedures
  MUST not be used on a G2H recovery source until an atomic
  consumer-aware fence is independently proven.

This is intentionally an **all-consumer hold** rather than
pretending that a producer-only newer checkpoint certifies all
independent readers. No acknowledgement of a missing consumer
is inferred. It prevents the unsafe automatic operation, but
does NOT implement coordinated safe deletion or liveness.

## Evidence and isolated SQL

`tests/guardian-h2-retention-crash-pg.test.ts` now includes a
REAL `RetentionManager.runCycle` call against the disposable
PostgreSQL17 database `g2h_h2_retention_local` at
`127.0.0.1:55485` with all safeguards:
- Engine-derived env requests **retention enabled, compaction
  enabled and checkpoint keep=1**.
- Both engine-derived policy and generic default still return
  `guardianRecoveryHold=true`; no special test-only production
  shortcut.
- While a second consumer is pinned to old checkpoint 1,
  `runCycle` reports `guardianRecoveryHeld=true`,
  `checkpoint deletions=0`, `durable deletions=0`,
  `failures=0`: all **3 checkpoints and 5 durable rows remain**.
- After the safe automatic hold, the deliberately EXPLICIT direct
  privileged maintenance call deletes five rows (retains five
  durable replay identities) and prunes one old checkpoint,
  showing that the original stale consumer is HALTed by
  lost replay evidence rather than silently accepting newer
  state. This direct-call test is NOT permission for deployment.

Legacy automatic second-newest-checkpoint compaction is
validated separately with explicitly unprotected
`tests/pr18-hot-storage-lifecycle.test.ts`, scoped retention
compatibility test. The checkpoint budget tests in
`tests/stability-step4-reporting-retention.test.ts` also
explicitly opt out of G2H hold as an isolated legacy maintenance
test fixture. The latter file currently cannot import the
preexisting missing `apps/engine/src/source-seal.js`; this is
tracked independently from this safety gate, not treated PASS.

## Mandatory remaining work, NOT optional backlog

1. Design a real authoritative, durable *per-active-consumer*
   pinned checkpoint/receipt registry, registration/lease/
   decommission protocol, plus an **atomic fence** held during
   compaction and checkpoint pruning. Unknown/stale consumer
   must block cleanup or require explicit operator reconciliation.
2. Coordinate the future independent Guardian (G2I) and the
   current single economic writer; no second economic executor,
   ownership submit or trading authority introduced here.
3. Prove safe retention progress under a continuous realistic
   multi-hour replay (no unbounded disk/DB growth), actual
   replay key churn, missed checkpoint cadence, reader crash,
   writer failover, lease expiry, loss of operator connection,
   clock mismatch and compaction transaction races.
4. A global HOLD may increase hot database size. Measure it
   and keep cleanup explicitly disabled for recovery tables until
   a proper fence is implemented. No opt-out based on guess.
5. Native filesystem power failure durability and operational
   reconciliation remain UNVERIFIED; Windows simulated ACK
   fault injection does not prove target disk safety.

## Final verified tests

- `guardian-h2-retention-crash-pg.test.ts` **1/1 PASS** on
  disposable PostgreSQL 17 localhost; specifically tested the
  new EngineEnv-derived automatic manager run while a stale
  pinned consumer existed, and the explicit direct-admin
  invalidation scenario.
- Full G2H scoped regression: **369/369 PASS across 30 test files**
  on four **separate** disposable localhost PG instances, with
  `PM_FORENSIC_STAGE=08_current`. The earlier four-test H4
  capacity suite is separate and not counted in this total.
- `@pm/db` typecheck PASS, Guardian-source typecheck PASS,
  forensic Engine typecheck PASS, `@pm/core` typecheck PASS;
  `git diff --check` PASS.
- Historical `pr18-hot-storage-lifecycle.test.ts` targeted
  'compacts automatically only behind the older of two verified
  snapshots' test: **1/1 PASS** with explicit legacy opt-out;
  the remaining 29 tests skipped by that targeted selection.
- A separate non-gating run of the two broad legacy retention
  files is **NOT ALL GREEN**: `stability-step4-reporting-retention.test.ts`
  cannot import the preexisting missing
  `apps/engine/src/source-seal.js` (GI-010), while one
  `pr18-hot-storage-lifecycle.test.ts` case fails in existing
  `authoritativeDurablePayload(undefined)` test fixture.
  These two failures are not claimed fixed, nor are those full
  legacy suites included in the 369 passing scoped tests.

No production/VPS/database/wallet/order/deploy/merge/push.
Tested only on disposable localhost PostgreSQL and offline
fixtures. The default HOLD is a conservative temporary safety
measure, not evidence that ordinary infinite retention is solved.

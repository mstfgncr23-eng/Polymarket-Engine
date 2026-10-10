# G2H H2 | V2 non-signing consumer ACK + restart (PARTIAL)

Date: 2026-10-11 | Base commit `8b687ea` | Stage: H2 **IN PROGRESS**
Scope authority: `G2H_REQUIREMENTS_FROZEN_20261011.md`.
No G2I, G3, process, trading/execution or real wallet change.

## Implemented in this atom

1. `guardian-pg-ack-store.ts`: independent, strict `version:2`
   canonical PostgreSQL-observation receipt with source database/schema/role,
   experiment/authority, immutable checkpoint ID+hash, durable cursor, **last observed canonical database high-water** and
   complete economic-state digest. A restart rejects a database rollback
   below that recorded high-water even when the last ACK cursor remains visible. No Ed25519, writerEpoch, manifestSequence
   or signer key. Existing v1 receipts HALT with
   `LEGACY_FORMAT_REQUIRES_OPERATOR_RECONCILE`; no automatic migration.
2. Atomic fs `O_EXCL` commit, expected-prior CAS, explicit monotonic cursor,
   stale lock requires operator reconciliation, fsync file and POSIX dir
   (Windows power-loss rename durability **NOT** asserted).
3. `guardian-trusted-pg-session.ts`: read-only source, no wallet/submit API.
   On open: read pinned trusted CP, rehydrate full recovery state, and if a
   v2 ACK exists replay ALL transitions from CP up to acknowledged cursor
   again before comparing the full economic digest. Never adopt ACK
   as a substitute for real economic replay.
4. One bounded page per `poll()`. Page source returns one MVCC snapshot,
   verifies source identity + pinned CP each poll, applies contiguous rows,
   and commits observer ACK **only after full replay**. If commit is uncertain,
   session HALTs and does not advance its in-memory cursor.
5. Latched HALT on missing/altered CP, source regression/disconnect,
   corrupted payload, gap, repeated operation key, concurrent poll and
   receipt mismatch. Observation view is fresh-and-caught-up, COPY-ONLY
   and explicitly **NOT G3 executor admission authority**.
6. A newer database checkpoint can replace the previous ACK anchor
   only after the old session's economic state at a shared durable cursor
   exactly matches rehydration from the newer checkpoint plus all tail
   transitions. Checkpoint hash/seen-since-checkpoint keys are normalized
   *only for the parity proof*; the actual v2 receipt commits the new
   checkpoint hash and its own exact economic digest atomically.
   A checkpoint lying inside a read page causes a prefix-only ACK,
   rebase at that cursor, and processing the rest on the next poll.
   A missing/mismatched checkpoint never permits skipping the ACK.
7. Previous v1 `GuardianReadonlySession` stays unchanged for historical
   signature-based regression only. The new trusted PG session does not
   require any signing component in its data-source bootstrap or ACK.

## Evidence

- `tests/guardian-trusted-pg-session.test.ts`: 26/26 tests pass:
  multi-page ACK, process restart; crash before ACK; ACK persisted despite
  response loss; economic digest divergence; legacy format; authority
  mismatch; tampered/missing/duplicate rows; stale source; checkpoint
  mismatch; CAS/stale lock; backlog; concurrent poll prevents both commits;
  new rows after catch-up; fresh read-only view; persisted source high-water
  regression detected across restart; and a concurrent poll arriving
  while ACK fsync is awaiting, leaving volatile state untouched while
  restart reconciles the already persisted receipt. Additional tests
  prove correct checkpoint rebase and restart after old checkpoint
  retirement, fail-closed forged economic parity, rejection of unsupported
  rebase proofs, successful **8,200-row continuation** by rebase at
  cursor 4,000, and mandatory HALT at 8,193 rows without a new checkpoint.
- `tests/guardian-h1-postgres-consistent.test.ts`: 12/12 tests pass
  against a **disposable PostgreSQL 17 localhost container**, including
  actual restricted PostgreSQL/RLS reader consuming multi-page canonical
  durable transitions, then reopening to reconcile on-disk v2 receipt.
  A new real SQL checkpoint written at cursor 3 is replay-validated,
  atomically adopted under a v2 ACK, and recovered again after restart.
- **Final full scoped regression: 351/351 PASS across 25 files**
  including checkpoint rebase and restart.
  `@pm/db`, Guardian-source, forensic Engine and `@pm/core` TypeScript
  checks PASS. `git diff --check` PASS.
  The unrelated full Engine `./source-seal.js` import remains outside
  this scoped project gate; it was not silently changed.
- All source tests use local fixtures or loopback disposable database.
  `G2H_LOCAL_TEST_DATABASE_URL` is guarded to the one explicit
  `127.0.0.1:55481/g2h_local_20261011` test database; no production URL.

## H2 acceptance BLOCKERS, not optional polish

**B-H2-01 | Operational checkpoint production/retention contract, PARTIAL**

Local automatic parity-verified rebase and 8,200-row progression now PASS.
Without a newer proven checkpoint the 8,192-key budget still correctly
HALTS; it never evicts identity or skips history. Real PostgreSQL also
proves the rebase on a persisted SQL checkpoint and restart.

**Still required:** determine how the current Engine actually produces,
verifies, retains and prunes checkpoints under sustained load and whether
a newer *fully canonical* checkpoint is present before the replay key
budget. Prove compaction and crash interleavings, checkpoint presence
beyond the latest-64 selection window, and producer integrity states.
The consumer cannot fabricate or publish its own economic checkpoints.
This is a normal-operation dependency, not optional optimization.

**Source-code finding:** `apps/engine/src/engine.ts` calls
`checkpointInsideGate` under existing `durableGate`, with explicit
checkpoints at experiment lifecycle boundaries, startup and clean
shutdown. Its `commitAuthoritative` default `checkpoint?` is optional
and does not, by itself, prove a periodic checkpoint frequency;
`packages/core/src/retention.ts` defaults `checkpointKeep=8` and
retention disabled. **UNKNOWN**: actual durable transitions per second
and whether the user workload can reach 8,192 before a new checkpoint.
The 100Hz fixture is not a measured production durable-row rate. H4
must measure the real durable-rate distribution before introducing
periodic checkpoint writes or a new duplicate-key protocol.

**B-H2-02 | Actual target filesystem and uncertainty handling**

Windows local test file fsync does not prove power-loss rename durability
on the future operating volume. Need target-filesystem acceptance and
operator reconciliation procedure for ambiguous rename, stale lock and
unknown prior durable ACK; never auto-delete stale owner locks.

**B-H2-03 | Recovery and concurrency matrix under long runs**

Existing restart matrix has 26 deterministic fault tests plus two real
PostgreSQL H2 paths (multi-page restart and checkpoint rebase).
Need checkpoint compaction/failover interleavings, crash during source
transaction, retained checkpoint choice beyond latest-64 visibility,
durable propagation after unknown economic commit and realistic
sustained replay. H1 production DB ACL/TLS verification separately
requires operator authorization.

## Deferred improvements captured this same atom

See `G2H_DEFERRED_IMPROVEMENTS_BACKLOG.md`: reuse common v1/v2 file CAS
persistence mechanics (not optional correctness), and measure repeating
ACL catalog queries per page if CPU observations justify caching.
Neither idea expands H2 freeze.

**Gate: H2 is NOT ACCEPTED; G2H NOT DONE.** NO SHADOW, NO VPS, NO
PRODUCTION DB/DEPLOY, NO PUSH, NO MERGE, NO LIVE or wallet work.

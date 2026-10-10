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
6. Previous v1 `GuardianReadonlySession` stays unchanged for historical
   signature-based regression only. The new trusted PG session does not
   require any signing component in its data-source bootstrap or ACK.

## Evidence

- `tests/guardian-trusted-pg-session.test.ts`: 21/21 tests pass:
  multi-page ACK, process restart; crash before ACK; ACK persisted despite
  response loss; economic digest divergence; legacy format; authority
  mismatch; tampered/missing/duplicate rows; stale source; checkpoint
  mismatch; CAS/stale lock; backlog; concurrent poll prevents both commits;
  new rows after catch-up; fresh read-only view; persisted source high-water
  regression detected across restart; and a concurrent poll arriving
  while ACK fsync is awaiting, leaving volatile state untouched while
  restart reconciles the already persisted receipt.
- `tests/guardian-h1-postgres-consistent.test.ts`: 11/11 tests pass
  against a **disposable PostgreSQL 17 localhost container**, including
  actual restricted PostgreSQL/RLS reader consuming multi-page canonical
  durable transitions, then reopening to reconcile on-disk v2 receipt.
- Final full scoped regression: **345/345 PASS across 25 files**
  including the ACK-fsync concurrency race guard.
  `@pm/db` TS, Guardian-source TS, forensic Engine TS,
  `@pm/core` TS and `git diff --check` ALL PASS.
- All source tests use local fixtures or loopback disposable database.
  `G2H_LOCAL_TEST_DATABASE_URL` is guarded to the one explicit
  `127.0.0.1:55481/g2h_local_20261011` test database; no production URL.

## H2 acceptance BLOCKERS, not optional polish

**B-H2-01 | Automatic safe checkpoint rotation and ordinary 8,192+ rows**

Current `GuardianOfflineState.seenOperationKeys` halts at 8,192 unique
post-checkpoint operations. V2 receipts intentionally fence checkpoint
identity and CAS rejects a change without independent *economic parity*
proof. This is correct fail-closed for now but not acceptable for
continuous Guardian operations at practical sustained rates.
Requirement: prove deterministic old-CP-to-new-CP state parity at a
concrete durable cursor under a coherent PostgreSQL source, then
atomically migrate v2 receipt to the new checkpoint WITHOUT skipping
an ACK or silently evicting duplicate identity. Test CP created both
before and after a page; source compaction; CP loss and restart.
NO bypassing the limit for convenience.

**B-H2-02 | Actual target filesystem and uncertainty handling**

Windows local test file fsync does not prove power-loss rename durability
on the future operating volume. Need target-filesystem acceptance and
operator reconciliation procedure for ambiguous rename, stale lock and
unknown prior durable ACK; never auto-delete stale owner locks.

**B-H2-03 | Recovery and concurrency matrix under long runs**

Existing restart matrix is local + one real PostgreSQL multi-page test.
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

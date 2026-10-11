# G2H H2 | Atomic checkpoint-pin retention protocol: research and local SQL proof

Date: 2026-10-11 | Base: `21de06a`
**Status: PROTOCOL PROTOTYPE ONLY. H-02/H-03 PARTIAL, G2H NOT ACCEPTED.**

## Known source facts

- Current canonical `PostgresEventJournal.commitDurableTransition()`
  and `compactDurableTransitionsThrough()` both use PostgreSQL
  transaction-scoped advisory lock `DURABLE_MUTATION_LOCK_ID=872314001`.
- `PostgresEventJournal.pruneOldCheckpoints()` uses a separate
  `DELETE` statement **without** this same advisory lock.
  Therefore a separate external check followed by pruning is a
  TOCTOU risk, even if compaction itself is locked.
- The Engine/CLI `RetentionManager` currently holds both automatic
  checkpoint pruning and durable compaction via default-true
  `guardianRecoveryHold`, from `21de06a`.
- Guardian's v2 receipt is a local fsynced observation watermark,
  independent of the economic writer and of PostgreSQL maintenance.
  Guardian's active PG role is SELECT-only and may NOT be granted
  INSERT/UPDATE/DELETE merely to publish progress.
- Neither G2I independent supervisor lifecycle, authoritative
  membership roster nor cross-host receipt-loss policy yet exists.
  Under these unknowns, lifting the global hold would be unsafe.

## Checked production precedents (comparative, not reuse)

1. PostgreSQL 17 replication slots hold a persisted consumer
   `restart_lsn` and expose confirmed progress. An inactive slot
   can retain unlimited WAL under default `max_slot_wal_keep_size=-1`,
   or lose required WAL when a configured cap is breached.
   Reference:
   https://www.postgresql.org/docs/17/view-pg-replication-slots.html
   https://www.postgresql.org/docs/17/runtime-config-replication.html
2. Debezium PostgreSQL connector combines a consistent snapshot,
   an externally persisted offset, and retention of the required
   replication slot. A recreated/lost slot and stale offset can
   cause skipped changes. Reference:
   https://debezium.io/documentation/reference/stable/connectors/postgresql.html

**Inference:** economic journal tables are **not WAL**, so directly
adding logical replication slots will not prevent SQL DELETE of our
custom durable table. What transfers is the requirement for a
persisted recovery anchor, consumer membership, and an atomic fence.
Do not overgeneralize a WAL retention mechanism to application rows.

## Alternatives and failure analysis

| Alternative | Safety | Progress | Decision |
|---|---|---|---|
| Current default global HOLD | Protects automatic CP+journal history | Economic tables grow; no compaction | KEEP while coordinator absent |
| Only newest/second newest verified producer CP | No proof lagging consumer moved; demonstrated HALT | Deletes rows, strands old consumer | REJECT |
| Poll local Guardian ACK file before separate SQL DELETE | Crash/clock/TOCTOU and unknown-reader risk | Sometimes deletes | REJECT |
| Durable required-consumer roster + recorded restartable pin, short atomic deletion transaction using canonical lock | Can be fail-closed, if registration and proof share lock | Makes cleanup possible after all pins advance | PROTOTYPE VALIDATED, not integrated |
| Treat expired lease as permission to delete | Offline consumer might restart from old CP; may silently lose needed rows | Deletes sooner | REJECT without explicit operator decommission |

## Target protocol, authoritative boundaries (NOT YET IMPLEMENTED)

1. A **trusted registration service** owns a persistent, nonempty,
   sealed roster of all required consumers (e.g. Guardian primary
   deployment identity). The independent Guardian retains a
   SELECT-only journal role. Never auto-register by attempting
   reads and never interpret 'no reported consumer' as safety.
2. Enrollment occurs under the SAME canonical transaction lock
   that cleanup acquires; confirm that the chosen verified checkpoint
   and all required tail are retained. Unknown consumer, source
   identity, unsealed roster, invalid CP or uncertain DB availability:
   FAIL CLOSED.
3. The pin is keyed by experiment, consumer identity, source
   incarnation and **checkpoint ID/hash/cursor** with
   monotone revision and writer/supervisor fencing epoch. No
   advancement is permitted from heartbeat, expiry or
   non-durable in-process economic state.
4. A Guardian must first demonstrate **complete economic replay
   parity** to the newer verified CP. Its recovery anchor progress
   must then be committed in a durable authority, under a restricted
   coordinator principal. A local-only ACK followed by a separate
   DB pin advance is **insufficient under filesystem rollback**:
   a local receipt might disappear after the DB pin has advanced.
   Required design fork: make DB receipt authoritative (with
   independently verified economics and local receipt reconciliation),
   or prove stronger source/receipt guarantees before updating pin.
   Either requires explicit G2I supervisor/receipt protocol work.
5. Maintenance MUST hold the SAME canonical PostgreSQL
   transaction-scoped advisory lock while it:
   (a) reads sealed membership,
   (b) verifies every required consumer's retained checkpoint,
   (c) computes a deletion prefix no newer than the **oldest
   pinned CP durable cursor**,
   (d) deletes durable prefix into replay-key identities, and
   (e) prunes ONLY CPs not pinned by any required consumer.
   All checks and writes must be inside ONE transaction.
   Any exception rolls the entire operation back.
6. Registering/rebasing/decommissioning must serialize under
   the SAME lock and compare epoch/revision (CAS). Old instance
   cannot publish a new pin after fencing epoch changes.
   A new registration that loses the lock race MUST confirm the
   anchor is still retained before becoming active.
7. Crashed/disconnected consumer remains REQUIRED and pinned.
   Time-based lease expiry alone does not decommission. Operator
   must execute an explicit, audited decommission/reconciliation
   transaction after proving that it is safe to lose the old
   recovery anchor. A failed DB or unknown roster version BLOCKS
   cleanup, even if economic writer is healthy.
8. Future cleanup must be bounded: no long-sleep transactions or
   significant P99 durable-writer lock contention. Validate
   journal throughput, checkpoint emission cadence, replay-key
   preservation, disk/heap and race scheduling.
9. Exact deployment policy: **`guardianRecoveryHold=true` stays
   enabled** in Engine and CLI until DB migration, least-privilege
   identity, receipt semantics, supervisor enrollment and automated
   interleaving tests are accepted.

## Executed real PostgreSQL prototype

`tests/guardian-h2-atomic-retention-protocol-pg.test.ts` uses
a **disposable** PostgreSQL 17 instance only on
`127.0.0.1:55486`, database `g2h_h2_protocol_local`.
It creates only `h2_proto_*` test tables, no production migration,
and uses the repository's EXACT shared advisory lock key.

The test has an **isolated model** of roster/checkpoints/durable
prefix. It does **not** call actual
`PostgresEventJournal.compactDurableTransitionsThrough` or
`pruneOldCheckpoints` and cannot prove current production paths
have this protocol.

Cases:
- H2-PIN-01: unsealed roster denies, two required consumers
  individually pin old CP, first advancement cannot unlock deletion,
  second permits one atomic prefix cleanup (CP1 retained until safe).
- H2-PIN-02: stale epoch update rejected; an offline required
  consumer is not automatically ignored or retired by clock.
- H2-PIN-03: registration races cleanup under same advisory lock;
  old CP registration fails if cleanup wins; no phantom reader.
- H2-PIN-04: missing roster/unknown pin never unlocks cleanup.
- H2-PIN-05: explicit DB transaction abort after a prefix DELETE
  rolls back all row changes, and a later valid cleanup proceeds.

## Nonclaims and hard remaining blocks

- Tests use a synthetic pin table and an intentionally simplified
  delete; migration, actual journal compactor, checkpoint pruner,
  lease/epoch service, startup/first-registration, single-reader
  authority and real ACK durability integration are **NOT DONE**.
- SQL lock serialization in isolation does not prove the
  deployment-specific failure model or arbitrary manual privileged
  direct table DELETE protection.
- Future runtime mode G2I must decide the authoritative identity
  provider and where durable receipt advancement is executed.
- Real filesystem power loss, failover, stale writer and unknown
  consumer incident recovery remain unverified.
- There is no safe permission to remove the global HOLD, deploy,
  push/merge, connect a wallet or perform live/economic execution.

## Final test status

- Isolated real PostgreSQL 17 protocol case: **H2-PIN 5/5 PASS**.
  The test creates only scoped `h2_proto_*` scratch tables,
  rerunnable with `CREATE TABLE IF NOT EXISTS` and clean
  `TRUNCATE` per case; the initial all-suite rerun exposed
  a non-idempotent table-creation fixture and that harness
  problem was corrected before final acceptance.
- Final full G2H scoped regression, each PG-dependent group on
  a distinct disposable localhost PG 17 database at ports
  55481, 55483, 55484, 55485 and 55486:
  **374/374 PASS across 31 test files**.
- `@pm/db` TypeScript PASS; Guardian source TypeScript PASS;
  forensic Engine TypeScript PASS; `@pm/core` TypeScript PASS;
  `git diff --check` PASS.
- The existing H4 four-test throughput benchmark is independent
  and not counted in 374. Existing unrelated broad legacy
  `source-seal.js` import / payload-fixture failures were not
  changed or claimed fixed.
- No production code, migrations, retention parameters, live
  DB, VPS, deploy, push, merge or wallet changed. Default
  `guardianRecoveryHold=true` remains ENABLED.
- The experiment validates SQL *mechanics* under the existing
  lock key, not authenticated consumer receipt acceptance,
  actual deletion methods or end-to-end crash safety.
- All newly created disposable PostgreSQL containers will be
  stopped/removed after committing and verifying this atom.

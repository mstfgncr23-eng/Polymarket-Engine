# G2H H2 | Permanent checkpoint-pin registration scaffold
Date: 2026-10-11 | Parent commit: `eee19fb`
**Scope: local branch code and disposable PostgreSQL only. H2/H3/H5 PARTIAL, G2H NOT ACCEPTED.**

## Known facts from current implementation

- Canonical economic transitions and new verified checkpoints use
  `DURABLE_MUTATION_LOCK_ID`, but existing
  `PostgresEventJournal.pruneOldCheckpoints()` does **not**.
- Guardian receipt (`GuardianPgFileAckStore`) is currently a local
  consumer-only fsync/rename record, NOT durable PostgreSQL custody.
  Windows cannot guarantee parent-directory fsync after power loss.
- An independently running Guardian's enrolled deployment identity,
  restorable receipt and status are not yet authoritatively provisioned.
- From parent `21de06a`, `guardianRecoveryHold=true` remains
  the fail-closed default for Engine, cleanup CLI and any newly
  constructed RetentionManager.

## This atom actually implements (not prototype-only SQL)

1. Add `0010_guardian_recovery_registry.sql` to the Drizzle
   migration journal, without changing earlier migrations.
   It creates real `guardian_recovery_roster` and
   `guardian_recovery_pins` in the same database as
   `engine_checkpoints` and `durable_transitions`.
2. `guardian_recovery_pins.checkpoint_id` has a PostgreSQL
   `REFERENCES engine_checkpoints(id) ON DELETE RESTRICT`
   foreign key. This **independently prevents direct deletion of a
   PINNED checkpoint** even when a caller bypasses RetentionManager.
   It does NOT prevent deletion of *durable rows* needed to
   replay from that checkpoint; that is a remaining H2 blocker.
3. Both relations enable RLS. No public/anon/Guardian policies or
   write grants are created. Full table owner/maintenance privilege
   is still a powerful authority, to be separately provisioned.
4. New `PostgresGuardianRecoveryRegistry` exported from `@pm/db`:
   `enrollAndSeal` requires **maintenance** access mode and checks
   a nonempty bounded unique required-consumer roster, valid
   source identity, explicit experiment/anchor hash and positive
   epoch. In one SQL transaction under existing durable mutation
   advisory lock it checks verified canonical checkpoint hash,
   checkpoint semantic validity, persisted global highwater
   and exact experiment ID, then creates roster/pin records
   and seals the roster. The seal stores immutable required-member
   count and a SHA-256 digest of the sorted consumer/experiment IDs:
   a missing member is detected, not mistaken for full progress.
5. `inspect` produces `BLOCKED` for absent/unsealed/wrong
   source identity/empty or INCOMPLETE required roster/corrupt
   referenced checkpoint. It compares sealed member count and digest
   with the current rows. It is read-only and does not authorize cleanup.
6. Intentional one-way bootstrap: once sealed, this API never
   silently adds, retires or advances a required consumer. It has
   **NO** time-based liveness expiry, no decommission function,
   no ACK-to-DB shortcut and no mechanism for disabling
   `guardianRecoveryHold`. This is a safety scaffold, not a
   working automatic compaction coordinator.

## Real local PostgreSQL integration proof

`tests/guardian-h2-durable-roster-pg.test.ts` (5/5 PASS on a
fresh disposable PostgreSQL 17 at localhost:55487, DB
`g2h_h2_roster_local`):
- Actual `PostgresEventJournal.initialize()` applies migration 0010.
  First run caught a missing Drizzle `meta/_journal.json`
  manifest entry; corrected before counting results.
- A genuine canonical durable mark and a semantically valid
  verified `engine_checkpoints` snapshot are created using
  actual journal APIs.
- No roster => BLOCKED. Readonly `enrollAndSeal` forbidden.
  Wrong hash refuses enrollment and rolls back to no roster.
  Two operator-specified required consumers enroll and seal
  against the real checkpoint. Second enrollment fails. Wrong
  source identity gives BLOCKED.
- Two subsequent real checkpoints are saved. Direct
  `pruneOldCheckpoints(1)` attempting to remove the pinned
  checkpoint now fails at the PostgreSQL FK; the pin, checkpoint
  and roster are preserved.
- Separate PostgreSQL observer role granted SELECT only on
  normal recovery tables cannot SELECT/INSERT/DELETE the
  new registration relations. Migration RLS flags verified.
- Deliberately remove one of two registered required members using
  privileged TEST-ONLY SQL. Sealed member-count/hash mismatch
  returns `BLOCKED`; restoring the member returns SEALED_PINNED.
- Deliberately change the stored checkpoint payload under TEST-ONLY
  privileged SQL. Registry notices the canonical digest mismatch,
  `BLOCKED` until exact original restored.

## Failure modes, sharp remaining boundaries

- **Not yet a durable guardian receipt registry**: no trusted
  coordinator advances pins with verified economic parity and
  rollback-proof receipt. The authorized maintenance caller is
  still responsible for asserting the complete initial roster.
  A missing/unknown required consumer identity cannot be
  discovered automatically by this schema.
- Existing `compactDurableTransitionsThrough()` still has no
  consumer roster check; its direct privileged use can strand a
  pinned reader. The static FK does not guard durable rows.
  **Runtime HOLD stays mandatory.**
- `pruneOldCheckpoints` still lacks canonical mutation-lock
  coordination, so no fully atomic source compaction/pruning
  protocol exists yet. The FK is only a narrower last-line
  checkpoint guard.
- The migration adds a table and FK to the branch but has NOT
  been applied outside disposable local PostgreSQL. Any future
  rollout requires separate DDL/privilege, migration compatibility,
  lock and disk-impact approval.
- No independent G2I Guardian identity service, receipt
  transfer, incident reconciliation, operator decommission, or
  crash/power-loss proof. These remain mandatory H2/H3 blockers.
- A connected privileged DB owner can alter the roster or bypass
  RLS. No adversarial-root cryptographic guarantee is claimed.
- All tests in this atom are functional, not 2-vCPU load or
  multi-hour retention availability measurement.

## Next acceptance atom

Implement one independent trusted coordinator with a durable
economic receipt provenance model and explicit identity epochs;
couple its checkpoint-pin revision/CAS to an atomic maintenance
transaction shared with the real compactor **and** checkpoint
pruner; test at least two Guardian readers, lost local ACK,
credential separation and restart/registration/cleanup races.
Until accepted, never lift the default HOLD.

## Final verification

- 27-file unchanged core G2H suite: **364/364 PASS**.
- Separate PostgreSQL suites, each isolated by database:
  semantic economic PG 3/3; three-market PG 1/1;
  retention+crash PG 1/1; atomic SQL protocol PG 5/5;
  **new durable registry PG 5/5**.
- Aggregate across six separately executed, disjoint
  scoped test runs: **379/379 PASS across 32 test files**.
  This is NOT a single concurrent-capacity benchmark.
- `@pm/db`, Guardian source, forensic Engine, `@pm/core`
  TypeScript checks PASS; `git diff --check` PASS.
- Initial 3-test attempt failed due a missing migration
  manifest entry; it was repaired in
  `migrations/meta/_journal.json`, tested on a brand-new
  disposable PostgreSQL and the final 5/5 tests PASS.
- The production Engine/CLI default `guardianRecoveryHold=true`
  remains unchanged. The earlier H4 bounded capacity test
  is separate and not included in this count.
- No VPS, live database, wallet, network feed, deployment,
  merge or push. Dispose all six test PostgreSQL containers
  after committing and verifying this atom.

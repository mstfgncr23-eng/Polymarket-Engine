# G2H H2/H3 | Real PostgreSQL retention, pinned checkpoint and ambiguous ACK

Date: 2026-10-11 | Base HEAD `bf15ce2`
Authority: `G2H_REQUIREMENTS_FROZEN_20261011.md`.
Status: **H-02/H-03 PARTIAL; G2H NOT ACCEPTED**.

## Source-first observation

- `PostgresEventJournal.compactDurableTransitionsThrough()` uses
  a canonical PG advisory mutation lock, records operation identity
  in `durable_replay_keys`, then physically **DELETEs** durable
  transitions at/below the supplied cursor; it does NOT preserve
  replayable payloads in a compacted placeholder.
- `pruneOldCheckpoints()` deletes older checkpoints according to
  its `keepLatest` configuration. The two operations are
  authorized maintenance writes, not allowed to the Guardian.
- `RetentionManager.runCycle()` chooses the **second latest**
  verified checkpoint's cursor as its compaction boundary when
  compaction is enabled. This does **NOT** check the checkpoint
  pinned in the receipt of every lagging Guardian observer.
- `GuardianTrustedPgSession` explicitly pins the receipt's
  checkpoint, replays all intervening rows and uses economic-hash
  equality before parity-proven checkpoint rebase. It does not
  silently jump to a newer checkpoint after a gap.
- `GuardianPgFileAckStore.commit()` writes a temp file, syncs it,
  atomically renames and performs a directory fsync on non-Windows;
  on Windows, parent-directory fsync is NOT asserted durable
  across real power failure. A retry when the result is unknown
  demands operator reconciliation or a verified restart.
- This is a **coordination/liveness** limitation, not a proof of
  a silent lost-fill bug: correctness requires fail-closed, which
  the local test demonstrates.

## Executed isolated PostgreSQL 17 test

`tests/guardian-h2-retention-crash-pg.test.ts`, **1/1 PASS**.

Dedicated disposable DB `g2h_h2_retention_local` restricted
to **127.0.0.1:55485**, explicit guarded
`G2H_H2_RETENTION_LOCAL_DATABASE_URL` only.
An independent SELECT-only RLS Guardian reader role observes an
actual PostgreSQL canonical global durable sequence.

1. Verified checkpoint #1 at cursor **1**, starting with no
   inventory and $1,000 paper cash.
2. Real SQL canonical **PAPER_FILL BUY UP 5 @ $0.40**, then
   **PAPER_MAKER_RESERVED $0.60**, then
   **PAPER_ORDER_CANCELLED**, then real SQL canonical
   **PAPER_FILL SELL UP 1 @ $0.50** through cursor **5**.
3. A healthy and a lagging Guardian separately anchor to
   checkpoint #1, each with its own independent local ACK file.
4. Inject **crash before ACK rename** using a wrapper that refuses
   the post-bootstrap ACK. The underlying receipt stays at cursor 1
   and the volatile consumer HALTs. Unmodified Guardian restart
   replays from the last persisted receipt.
5. Inject **crash after actual ACK fsync+rename but before caller
   receives confirmation**. Actual receipt has advanced to 3
   while volatile cursor is still 1 and consumer HALTs.
   Restart reconciles at exact ACK 3 and consumes the remaining
   tail to 5. Neither case skips economics or double-applies.
6. Create two new canonical **verified checkpoints** at the
   committed economic cursor 5. The healthy consumer replays
   and **proves economic parity before checkpoint adoption**,
   then restarts pinned to one of the new checkpoints.
7. Execute actual SQL `compactDurableTransitionsThrough(5)`:
   **5 durable rows deleted, 5 durable replay-key identities
   retained**, global high-water still 5.
8. **Before deleting old checkpoint**, the lagging reader
   anchored at checkpoint #1 receives
   `G2H_PG_DURABLE_REPLAY_GAP`, HALTs, and leaves its receipt at 1.
9. Execute SQL `pruneOldCheckpoints(2)`: old checkpoint #1
   is pruned while the two new verified checkpoints remain.
   Stale reader restart rejects the now-missing pinned checkpoint.
   Its receipt does not move forward.
10. The healthy reader recovers from retained checkpoint 2/3
    despite the entire old durable prefix being compacted.
    New real SQL **SELL UP 1** at cursor 6 is processed.
    New restart confirms residual **3** and independent economic
    ledger realized PnL **+$0.20**.

Structured executable proof:
`G2H_H2_RETENTION_PROOF` shows compacted 5, keys 5,
pruned checkpoints 1, healthy cursor 6, lagging consumer HALT,
both crash injection paths and parity rebase.

## Explicitly unresolved mandatory blockers

1. **No consumer-pin-aware retention fence exists.** If a Guardian
   has not completed rebase to the chosen cleanup checkpoint,
   compaction can strand that observer. Failing closed is proven;
   guaranteeing ordinary continued service under enabled retention
   is **NOT** proven. Before G2H acceptance, establish a proven
   retention constraint/protocol tied to every authoritative
   consumer's pinned checkpoint or disable destructive
   compaction where continuous operation is required.
2. This atom invokes the *real journal's SQL compactor and prune
   methods*, not the entire `RetentionManager.runCycle()`
   production cadence. Need cross-component policy integration
   tests, especially with active/inactive lifecycle states.
3. Windows simulated before/after ACK wrappers are deterministic
   fault injection, **NOT** a real kernel kill or a filesystem
   power-cut test. Target OS/disk durability and lost receipt
   policy remain unresolved.
4. Multiple concurrently active legitimate observers or an
   unknown/disconnected observer require an explicit retained
   lease or operator reconciliation rule. Never infer its
   advancement from a healthy observer.
5. Must prove checkpoint producer cadence and compaction budget
   against actual archived durable rates. H-05 explicitly says
   **remote/production soak is not required for G2H**;
   isolated local tests remain the gate. Do not manufacture
   a VPS prerequisite or hard-coded lag SLO.

No changes to production retention code, no enabling compaction.
No VPS, production DB, wallet, external feed, orders, deploy,
push, merge or new economic actor. Compaction is **only** in the
dedicated disposable database. Optional observability belongs
in `G2H_DEFERRED_IMPROVEMENTS_BACKLOG.md`; the retention
coordination itself is NOT an optional improvement.

## Final regression

- Standalone local PostgreSQL retention/fault test **1/1 PASS**.
- Independent final scoped regression, 30 selected test files,
  including previous H1, H4 one-market and three-market economics:
  **369/369 PASS**. Each PostgreSQL test group had its own
  disposable database on ports 55481, 55483, 55484, 55485.
- `@pm/db` TypeScript PASS, Guardian-source Engine TS PASS,
  forensic Engine TS PASS, `@pm/core` TS PASS,
  `git diff --check` PASS.
- The separate four-test H4 capacity benchmark on port 55482 was
  NOT re-run here and is NOT included in these 369 tests.
- All four disposable database containers stopped and removed
  after the final commit, no live infrastructure touched.

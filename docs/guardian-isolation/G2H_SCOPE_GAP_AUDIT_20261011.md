# G2H Scope Gap Audit and FINITE Implementation Queue
Date: 2026-10-11 | repo baseline `refactor/guardian-isolation-atomic@aab85cb`
Authority: `G2H_REQUIREMENTS_FROZEN_20261011.md`.
Status: **Requirements frozen; code still NOT accepted.**
This is a code-level audit, NOT new user-requested features.
**Deferred improvements:** all useful nonblocking observations discovered
while performing H1-H4 must be recorded and committed in
`G2H_DEFERRED_IMPROVEMENTS_BACKLOG.md` in the same task. That separate file
is NEVER permission to expand H1-H4 or implement a deferred change.

## 1. Fact/assumption register

| Claim | Status | Evidence or limit |
|---|---|---|
| Current Engine is canonical economic writer | KNOWN in current process | `apps/engine/src/engine.ts` `durableGate`, `commitAuthoritative`, `applyCommitted`; does not prove multi-process failover |
| DB transitions have one serialized sequence | KNOWN for this implementation | `PostgresEventJournal.commitDurableTransition`: advisory txn lock + expected prior sequence + metadata update in same transaction |
| Guardian has a real SELECT-only PostgreSQL role | UNKNOWN | `access:"readonly"` is application capability; no actual DB grants/role verified |
| Current offline recovery covers positions + owners | KNOWN for tested fixtures | `guardian-offline-durable-source.ts`, G2G tests; not live PostgreSQL integration |
| Existing G2H consumer can run without Ed25519 | FALSE | `GuardianReadonlySession.open/poll` always require signed manifest, signed row pages, key/floors |
| Restart receipt is independent of signer epoch | FALSE | v1 `GuardianConsumerReceipt` contains writerEpoch/manifestSequence |
| G2H paired books work without Engine's running feed | KNOWN in offline adapter-owned tests; production process UNKNOWN | `GuardianStandaloneBookObserver` directly runs existing sharded `PolymarketMarketDataAdapter.run()` with scripted bounded transports; G2I process supervision not built |
| Guardian has economic write/submit access | FALSE in G2H modules | G2H adapters expose reads/views; G3 arbitration is currently inside Engine |
| "Position still open" is guaranteed during future order submission | NOT PROVEN, G3 | Snapshot is stale as soon as executor can admit a different intent |

## 2. Requirement -> current implementation -> exact gap

| Frozen requirement | Reusable source/tests | Proof gap | Status |
|---|---|---|---|
| H-01 trusted DB/role | `PostgresEventJournal.readGuardianConsistentPage`, isolated PostgreSQL 17 tests | Real isolated SELECT-only role + RLS proof PASS; production DB identity/credential/TLS and provisioning unverified | PARTIAL |
| H-02 consistent checkpoint/replay | Non-signed `guardian-trusted-pg-bootstrap.ts`; one-snapshot `readGuardianConsistentPage`, canonical reducer | First-page checkpoint+tail + real gap/tamper/concurrent-commit tested; repeated pages, checkpoint rotation and ACK resume not yet integrated | PARTIAL |
| H-03 restart/ACK | V2 no-sign ACK/replay, 26 offline tests; real PostgreSQL retained-CP lookup beyond newest 64, compaction/restart, pinned-deletion HALT | Safe parity rebase and 8,200-row progression PASS; **producer CP cadence/lease, target FS crash durability, source failover remain blockers** | PARTIAL |
| H-04 independent paired books | Standalone adapter H3 tests: 40/40 pairs in four bounded shards; 30 stay READY when one shard's 10 are BLOCKED, scoped reconnect, slow fourth-shard startup, silent sync/epoch loss | Offline four-shard functional tests PASS; **real sustained book age, overflow, paced traffic and latency under H4 load not yet proved**. Independent process supervisor belongs G2I, not H-04 | PARTIAL |
| H-05 bounded health/capacity | Historical JSONL 1,779 records size/rate counted; H4 2-CPU-capped PG with archive-shaped row size histograms (200 records), ACK p99 152-157ms; 40 markets, 32-level UP/DOWN, 80 books/s, 0 gaps; all four H4 tests PASS | **Economic semantics still only mark-to-market**, not real fills/settlements; short runs, synthetic book depth, 0 queue highwater, laptop CPU != target 2vCPU VPS, no sustained PG CPU/heap/failover or operator lag budget | PARTIAL |

## H4 two-CPU, archive-shaped bytes and 32-level books (2026-10-11)

- `G2H_H4_TWO_CPU_ARCHIVE_SHAPED_EVIDENCE_20261011.md`:
  2-CPU/1500MiB disposable PostgreSQL, local Node availableParallelism=2,
  full source JSONL row-size p50/p95/p99/max reproduced within
  tens of bytes, SELECT-only Guardian durable ACK under 20/s.
- 40 markets/four shards at 80 full books/s with 32 bid and ask
  levels per token, 40/40 READY, zero scripted ingress gap.
- H-05 remains partial: no real PAPER_FILL/settlement economics replay
  in this size-envelope profile, only ~4-6 second runs per repetition,
  no queue overflow workload or target-hardware long-run envelope.

## H4 measured historical rates + local paced capacity (2026-10-11)

- `G2H_H4_HISTORICAL_RATES_AND_PACED_LOCAL_BENCH_20261011.md`
  has checksum-confirmed aggregate durable rates from both actual soak
  exports, measured local 5/s and 20/s PostgreSQL ACK profiles (two
  repeats each), and 80/s four-shard full-book source-age profiles.
- The real observed durable average is just 0.026/0.035 rows/s,
  NOT the earlier 100/s synthetic stress scenario. The new H4 suite
  intentionally has a separate guarded local database/port from H1.
- H-05 remains PARTIAL: actual 5KB business-event shapes, true
  book depth/rate, queue saturation, target 2-vCPU limits and multi-hour
  resource/fault evidence remain unverified.

## H3 four-shard/40-market functional evidence (2026-10-11)

- `G2H_H3_FOUR_SHARD_40_MARKET_ACCEPTANCE_20261011.md`:
  source audit of aggregate connection semantics; six four-shard
  offline cases including 40/40 READY, one-shard 10 BLOCKED / 30 READY,
  delayed fourth-shard CONNECTING readiness, silent sync/epoch fences.
  H4 paced load and production target resource envelope remain OPEN.

## H2/H3 follow-on evidence (2026-10-11)

- `G2H_H2_RETENTION_H3_STANDALONE_EVIDENCE_20261011.md`: pinned CP
  lookup beyond newest 64 and real compacted PostgreSQL restart safety;
  standalone normalized adapter 8/8 pairs without any EngineService
  callback, and strict 7/8/reconnect/epoch invalidation.
- These are new local proofs only. Source lease under maintenance
  compaction, 4-shard availability and H4 measured capacity remain open.

## H2 local implementation evidence (2026-10-11)

- `G2H_H2_ACK_RESTART_PARTIAL_20261011.md` documents the v2
  signer-independent ACK, bounded multi-page poll, replay-before-ACK
  crash/restart recovery and named unresolved acceptance blockers.
- 26/26 closed fault tests plus two disposable PostgreSQL 17 end-to-end
  multi-page/restart test PASS. **H2 is still NOT ACCEPTED**, especially
  rotation/compaction and operation-key lifetime requirements.

## H1 local implementation evidence (2026-10-11)

- `G2H_H1_REAL_POSTGRES_READER_EVIDENCE_20261011.md`: disposable PostgreSQL 17,
  10/10 end-to-end tests; source identity, real least-privilege role, RLS
  SELECT policies, repeatable-read snapshot, page replay and no-sign bootstrap.
- Previously the signer-only `GuardianReadonlySession` was a required code
  dependency. New non-signed first-page path now exists separately; H2 still
  has to supply durable consumer ACK and continuous catch-up. Real production
  ACL/TLS and writer-scoped deployment are NOT authorized or verified.

## 3. Required implementation work. Exactly FOUR delivery packages.

**H1 | Trusted PostgreSQL reader and coherent source boundary**
1. Implement minimal read-only G2H source using the current PostgresEventJournal
   or a targeted read-only transaction API; no new message broker or signer.
2. Verify actual role/table grants and database source identity in isolated
   PostgreSQL. Prove a coherent checkpoint+high-water+ordered tail snapshot.
   Source may use read-only REPEATABLE READ transaction or demonstrably safe
   bounded fencing retries, depending on measured complexity; NO silent
   mixed-version acceptance.
3. Replace mandatory signed-manifest/page requirement in the actual G2H
   session path with this trusted PG model. Keep signed code as UNUSED
   experimental modules pending later deletion/retirement. Avoid both being
   simultaneously accepted as ambiguous default authority.
4. Concrete tests: concurrent commit, live checkpoint, gap/compaction,
   revoked privilege, wrong DB, stale source, mismatched experiment;
   zero economic writes.

**H2 | Single consumer restart contract**
1. Define new receipt version and deterministic migration/reconcile rule
   from signer-bound v1 (fail closed on unexplained existing receipt).
   Consumer progress stays distinct from economic idempotency.
2. Rebuild whole economic state from verified checkpoint + all missing rows
   on startup. ACK only fully applied batches, persist safely on target FS.
   Explicitly handle checkpoint rotation and 8,192-key bounded retention
   without evicting duplicate evidence silently.
3. Closed fault injection: kill before/after checkpoint/receipt, COMMIT_UNKNOWN,
   lost receipt, stale read, conflict, replay/race, re-opened episode. Compare
   ledger/residual/owner state to independent oracle.

**H3 | Market books usable independently of Engine**
1. Reuse `PolymarketMarketDataAdapter` COMPLETE normalized books, token
   synchronization and epoch logic behind the pure `GuardianPairedBookChannel`.
   Do NOT duplicate websocket parsing or order submission.
2. Prove an adapter-owned, standalone offline feed contract so the G2I
   process can run without `EngineService.onMarketSnapshot`. If there
   is an independent feed socket later, it belongs to the ONE Guardian;
   a second active trading Guardian remains prohibited.
3. 8/8 closure, asymmetric 7/8, reconnect, no-tick, epoch rotation,
   corrupted ladder, out-of-order, bounded queue and age/skew tests.

**H4 | Quantified local gate and signoff**
1. Isolated local PostgreSQL+normalized-book replay under recorded target
   event rates. Repeated runs: rows/s vs ticks/s, page latency p95/p99,
   source lag p95/p99, CPU, max heap/RSS, reconnect recovery.
2. Define resource ceiling using recorded deployment headroom and realistic
   40-market / 8-reference-close sample, not arbitrary percentages.
3. Full scoped tests and typechecks; evidence of no G3 executor code changes,
   no shadow, no remote operations. If all frozen H-01..H-05 pass, mark
   G2H DONE. Otherwise list ONLY failing acceptance IDs.

## 4. Explicitly parked: not on H1-H4 critical path

- Existing `guardian-signed-writer-manifest.ts`,
  `guardian-signed-durable-page.ts`,
  `guardian-writer-manifest-publisher.ts` and signature-based tests:
  **experimental/nonblocking**, NOT G2H prerequisites. Avoid adding code
  here; decide eventual removal only after new consumer parity succeeds.
- Separate Guardian OS process, scheduling and request IPC: **G2I**.
- Latest authoritative residual + open episode + maker/taker owner
  read-and-validate at order *admission*, and pre-submit/commit recheck:
  **G3**. Cannot be guaranteed by observation alone.
- Single durable executor / restart unknown order reconciliation: **G3**.
- No two active Guardians at cutover: **G4**.
- Real PAPER operational soak and realized PnL report: **G5**.
- Alternative event bus, cryptographic chain, generalized high availability
  platform, new policy threshold: **BACKLOG ONLY** unless evidenced.

## 5. Decision gates

- Stage H1 must be accepted before converting H2 receipt: no guessing
  what a 'trusted' checkpoint is.
- H2 and H3 may be independently tested offline, but H4 signoff uses their
  integrated state. If H1 requires modifying durable journal, characterize
  the existing transaction first and preserve single-writer source.
- NO automatic use of Docker/local PostgreSQL without verifying the test
  environment is isolated and does not attach to prod credentials.
- A broad suite passing means regression confidence only, not user approval
  for G2I, production cutover or connection to real wallet.
- Single source of truth for future work: requirements freeze + this audit.
  Other G2H atom documents are explanatory, not permission to expand scope.

## Source traces

- `packages/db/src/journal.ts` lines around 1051, 1479, 1500, 1563, 1597:
  PostgreSQL read and write sequence contracts.
- `packages/db/src/access.ts` journalMustVerifySchema excludes readonly:
  schema verification must be explicitly specified by H1's read adapter;
  trust cannot follow from `connectWithoutMigrate("readonly")` alone.
- `apps/engine/src/guardian-readonly-session.ts` signature-bound source
  and `apps/engine/src/guardian-consumer-ack-store.ts` v1 receipt.
- `apps/engine/src/guardian-normalized-book-bridge.ts` and
  `packages/market-data/src/polymarket.ts` adapter semantics.
- `apps/engine/src/engine.ts` order ownership, `durableGate`,
  `executeRiskReductionArbitration`, Guardian and profit-trailing paths.

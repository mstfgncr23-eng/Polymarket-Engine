# Guardian | Deferred Improvements and Observations
Created: 2026-10-11 | First evidence baseline: `45c5915`
Status: **DISCOVERY LOG, NOT an acceptance gate or implementation mandate.**

## Purpose and permanent working rule

When G2H/G2I/G3 work exposes an improvement, capture it **in this file as
part of the same scoped task**, then commit the updated file. The user
wants to revisit ideas **after the current gate finishes**, not keep
extending the gate with optional features.

- A discovered issue that violates a **frozen required invariant** is
  NOT "deferred": log it in the current gate's gap/acceptance document,
  fix or HALT that gate. May cross-reference it here, clearly as BLOCKER.
- An improvement that is useful but not required for the frozen gate goes
  HERE, with an immutable `GI-###` ID, stage, evidence, cost/risk,
  suggested experiment and decision status.
- `KNOWN` means directly verified by code or a captured measurement.
  `LIKELY` is a technical hypothesis; `UNKNOWN` requires evidence.
  No speculative claim should be reported as observed production failure.
- No new protection/risk threshold, dependency, optional rewrite,
  shadow Guardian, extra writer or LIVE execution is authorized here.
- Commit the backlog **with the scoped work or as one separate docs-only
  commit within the same task**. Do not interrupt the user per atom.
- After G2H acceptance, evaluate items once, explicitly ACCEPT / REJECT /
  DEFER with rationale; do not treat existence in this file as approval.
  Retain rejected entries for audit. Never silently upgrade an entry to
  a mandatory condition.
- Future entries: add evidence paths, source commit and whether the
  concern was reproduced under an actual measured workload.

## Open optional improvements

### GI-001 | Profile operation-key set copy before redesign
- Stage: G2H post-acceptance or measured H4 follow-up.
- Status: **OPEN / LIKELY CPU cost**, not independently isolated.
- Evidence: `apps/engine/src/guardian-offline-durable-source.ts`
  uses `new Set([...current.seenOperationKeys,row.operationKey])` for
  EVERY durable row. Current key budget is 8,192.
- Opportunity: measure cumulative copy/GC cost vs recovery/state hashing
  and compare persistent/structurally shared key sets **without weakening
  exact duplicate detection or crash parity**.
- Decision rule: change only with separately measured real bottleneck.
  Do not use lossy eviction as an optimization.
- Note: avoiding an ordinary-operation HALT at the 8,192 key limit
  is already MANDATORY under H-03/H-05, not this optional optimization.

### GI-002 | Economic digest hot-path profiling
- Stage: G2H measurements; optional tuning later.
- Status: **OPEN / LIKELY**, not an observed production bottleneck.
- Evidence: `guardian-readonly-session.ts:hashGuardianRecoverableState`
  serializes ledger, owners, orders, settlement maps and seen keys for
  consumer ACK and `readVerifiedView`.
- Opportunity: profile CPU/allocation under 40-market and high-journal
  rates; explore cached state digests only if cache invalidation can be
  proved exact for all economic mutation types.

### GI-003 | Avoid unnecessary complete-book cloning
- Stage: G2H book perf, revisit during G2I.
- Status: **OPEN / UNKNOWN actual cost**.
- Evidence: `guardian-paired-book-channel.ts` copies normalized
  depth on ingress and copies both outcomes again on `readGuardianPairedBook`.
  H3 added `guardian-standalone-book-observer.ts`, which also caches
  at most TWO epoch-checked normalized snapshots per market (necessary
  for both outcomes arriving with different token sync timing).
  This is bounded by the 40-market registration limit.
- Opportunity: measure depth/heap for realistic 40-market normalizer
  traffic. Consider immutable snapshot ownership or bounded transfer
  only if it preserves no mutable sharing and freshness/epoch fences.

### GI-004 | Separate observability from business readiness
- Stage: G2I/G5 operational dashboards.
- Status: **OPEN / DESIGN IDEA**.
- Evidence: source and book modules provide cursor/status/generation;
  G2H freeze H-05 already requires minimum distinct health.
- Opportunity beyond the mandatory minimum: operator-facing panels for
  recovered residual age, last *valid* UP/DOWN ticks, cursor lag histogram,
  rejected action reasons, epoch resets, time to reconcile and source
  disconnection. No new observability backend by default.

### GI-005 | Retire experimental Ed25519 implementation cleanly
- Stage: after H1/H2 non-signing-source parity passes.
- Status: **OPEN / CLEANUP**.
- Evidence: `guardian-signed-writer-manifest.ts`,
  `guardian-signed-durable-page.ts`,
  `guardian-writer-manifest-publisher.ts` and signer-only tests remain
  in the branch; `GuardianReadonlySession` currently depends on them.
- Opportunity: isolate/remove unused signer branches and tests, simplify
  tsconfig and API documentation, with regression proof that no default
  reader accidentally falls back to the cryptographic branch.
- Guardrail: **Removing mandatory signing from the active read path
  is H1 REQUIRED**, not a deferred improvement; only post-migration
  dead-code cleanup belongs here.

### GI-006 | Ack-store filesystem portability hardening
- Stage: G2I operating environment preparation.
- Status: **OPEN / UNKNOWN target volume durability**.
- Evidence: `guardian-consumer-ack-store.ts` notes that Windows rename
  directory fsync is not guaranteed; POSIX path explicitly syncs directory.
- Opportunity: target-FS power-loss model, durability probe and WAL/atomic
  receipt alternative **if** the intended deployment FS fails the H-03
  crash/restart requirement. It would then become a mandatory defect
  under H-03, not an optional polish task.

### GI-007 | Tune oversized replay page tail
- Stage: G2H H4 measurements and subsequent performance backlog.
- Status: **KNOWN local synthetic observation, UNKNOWN production impact**.
- Evidence: `G2H_LOCAL_REPLAY_CPU_AB_20261010.md`: replaying 8,192
  synthetic rows took 2,081.5ms before and 1,730.2ms after eliminating
  a redundant recovery-state clone. One 512-row page p95 was ~378ms
  in this particular small sample.
- Opportunity: repeat under controlled CPU and GC with sustained,
  paced real-shape journal/book load. Compare batch size, full-recovery
  clone and per-row invariant cost with correctness unchanged.
- Do not extrapolate single-run synthetic throughput to VPS p99.

### GI-008 | Operational checkpoint handoff runbook
- Stage: G2I/G4 operations, not an extra G2H primitive.
- Status: **OPEN / DOCUMENTATION**.
- Evidence: `guardian-consumer-ack-store.ts` leaves stale locks for
  operator reconciliation; `guardian-readonly-session.ts` blocks
  history-skipping checkpoint transitions.
- Opportunity: write a human-operated recovery decision tree for
  stale receipt lock, ambiguous checkpoint, expired producer observations,
  isolated reader failure and deliberate state rebootstrap.
- Guardrail: a safe checkpoint rotation mechanism is ALREADY REQUIRED
  in H-03. Runbook is not a substitute.

### GI-009 | Source transport simplicity and socket resource cost
- Stage: G2I.
- Status: **OPEN / ARCHITECTURE TRADEOFF, UNKNOWN measurements**.
- Evidence: the H3 `GuardianStandaloneBookObserver` now owns
  `PolymarketMarketDataAdapter.run()` without Engine callbacks.
  A scripted single-shard 8/8 reconnect test passes. Target
  four-shard behavior remains unmeasured and may over-invalidate
  all markets when a transport connection callback lacks shard identity.
  **Proving sustained required H-04 readiness is NOT deferred here**.
- Opportunity: evaluate the cost of the single Guardian-owned
  `PolymarketMarketDataAdapter` connection versus a genuinely
  independent market-data service. Prefer existing normalization.
  Never install a second *active Guardian* or independent order writer.
- Guardrail: actual independent book availability is H-04 mandatory;
  this entry covers topology cost/complexity beyond that criterion.

### GI-010 | Unrelated full Engine typecheck import blocker
- Stage: independent maintenance after G2H, unless it blocks required build.
- Status: **KNOWN in previous G2H baseline**.
- Evidence: full Engine TypeScript reported missing
  `apps/engine/src/operator-report.ts` import `./source-seal.js`;
  Guardian-source and Engine forensic tsconfigs passed.
- Opportunity: independently inspect source-seal provenance and fix
  through its owning reporting stage; do not quietly change report
  security/baselines as collateral Guardian work.
- Guardrail: if full project build is required for G2H acceptance,
  surface that as a gate blocker and explicitly decide scope.

### GI-011 | Examine position-centric telemetry costs
- Stage: G2I/G5.
- Status: **OPEN / HYPOTHESIS**.
- Evidence: engine's current risk-reduction ownership, guardian
  observation and market source paths share the runtime event loop
  (`apps/engine/src/engine.ts`).
- Opportunity: independently measure event-loop delay and residual
  observation-to-request age before suggesting future scheduling changes.
  Do not conflate transport CPU, DB throughput and trade latency.

### GI-012 | Automated, least-privilege reader provisioning and RLS audits
- Stage: G4 operator deployment checklist / later tooling polish.
- Status: **OPEN / KNOWN local proof, UNKNOWN production role setup**.
- Found during: H1 isolated PostgreSQL 17 integration tests (2026-10-11).
- Evidence: the existing migration enables RLS on
  `durable_sequence_metadata`, `engine_checkpoints`, and
  `durable_transitions` with no reader SELECT policy. An ordinary
  `GRANT SELECT` alone returns zero rows. The disposable H1 test
  demonstrated role-specific SELECT policies plus three-table grants
  unlock legitimate data and prevent INSERT/UPDATE/DELETE/DDL.
- Opportunity: create a separately reviewed, idempotent, operator-run
  role/RLS provisioning audit and permission drift check so reconnects,
  migrations and disaster recovery do not silently revoke the
  independent Guardian's observation ability.
- Cost/failure modes: policy drift, excessive cross-schema SELECT grants,
  privileged role inheritance and dangerously automated production DDL.
- Suggested validation: dedicated isolated PG role-grant/revoke matrix;
  compare restored backup/schema role grants. No automatic prod migration.
- Boundary: **working SELECT-only ACL and RLS visibility ARE H-01
  mandatory**. This optional entry is only later provisioning automation
  and runbook quality, NOT permission to defer H-01.

### GI-013 | Reuse one tested receipt-file CAS primitive across v1 and v2
- Stage: post-H2 acceptance cleanup; nonblocking unless persistence defect proven.
- Status: **OPEN / KNOWN duplicated implementation**.
- Found during: H2 commit, 2026-10-11.
- Evidence: `guardian-consumer-ack-store.ts` and new
  `guardian-pg-ack-store.ts` separately implement O_EXCL lock,
  fsync, atomic rename and CAS. Different receipt schemas are
  intentional and v1 MUST NOT silently convert to v2.
- Opportunity: share a generic, separately tested durable file-CAS
  primitive without merging authority formats. Measurable benefit is
  fewer independently maintained filesystem edge cases.
- Failure modes: accidental acceptance of signer receipt as PG v2,
  unsafe lock cleanup, changed on-disk encoding or ACK regression.
- Validation: existing signed and PG receipt fault suites green,
  power-loss/fsync tests on target FS if available.
- Boundary: H-03 durable correctness and stale lock remain REQUIRED;
  only reuse/refactor is deferred.

### GI-014 | Profile source ACL catalog validation on every journal page
- Stage: H4 measured capacity and later tuning, not a new feature.
- Status: **OPEN / UNKNOWN overhead**.
- Found during: H2 coherent PostgreSQL polling.
- Evidence: `PostgresEventJournal.readGuardianConsistentPage`
  checks live effective schema/table/sequence privileges via
  catalog queries inside each REPEATABLE READ READ ONLY transaction.
- Opportunity: measure DB wall/CPU and p95 at realistic 40-market
  journal rates before evaluating an independently fenced role-policy
  validation cache. No caching that silently masks privilege revocation,
  RLS changes or altered session identity.
- Boundary: Current fail-closed least privilege is H-01 mandatory.

### GI-015 | Profile verified checkpoint-rebase replay amplification
- Stage: H4 workload measurements, post-H2 optional tuning.
- Status: **OPEN / KNOWN extra replay, UNKNOWN deployment impact**.
- Found during: H2 parity-proven CP rotation (2026-10-11).
- Evidence: `GuardianTrustedPgSession.rebaseIfProven` verifies a newer
  checkpoint by reconstructing its economic state and replaying every
  post-checkpoint durable row through the already ACKed cursor, then
  compares the full recoverable economic digest. A one-run 8,200-row
  closed fixture with checkpoint at 4,000 completed and passed exact
  restart parity. These tests do NOT establish production CPU/heap p99.
- Opportunity: profile checkpoint frequency and rebase CPU/GC vs
  canonical journal throughput before considering a streaming parity
  digest or adjusting checkpoint cadence. Do not cache away economic
  state without proving every ledger/owner/reversal/trailing mutation.
- Boundary: A provably safe checkpoint rebase and continuous progress
  are H-03/H-05 REQUIRED. Only optional CPU improvements go here.

### GI-016 | Per-shard Guardian operator metrics instead of aggregate red status
- Stage: G2I/G5 dashboards, post-H3 nonblocking observability polish.
- Status: **OPEN / KNOWN aggregate ambiguity, UNKNOWN operator utility**.
- Found during: H3 four-shard/40-market functional tests, 2026-10-11.
- Evidence: `ShardedPolymarketMarketDataAdapter.aggregateConnectionState()`
  returns `DISCONNECTED` when any active shard disconnects; a Guardian
  may still safely have 30/40 market pairs READY. The source exposes
  `shardSnapshots()` and a read-only shard observer with per-shard
  connection state / reconnect counters / ingress queue stats.
- Opportunity: display per-shard healthy/failing market count, epoch
  reset and reconnect time next to aggregate status to avoid an operator
  mistaking a 10-market outage for a 40-market outage. Reuse existing
  metrics rather than inventing a new event stream or broker.
- Failure modes: telemetry and business readiness diverging, high
  label cardinality, accidental trust in a dashboard instead of the
  actual normalized token sync state.
- Validation: local simulation for 30 READY + 10 BLOCKED and bounded
  counts with 40 markets; no order authority or production side effect.
- Boundary: **correct shard-isolated book readiness is H-04 mandatory**
  and was fixed in the scoped H3 code, not deferred. This entry is
  only additional operator-facing observability.

### GI-017 | Reproducible performance baseline reports in CI and dashboards
- Stage: after H4 capacity acceptance, optional developer ergonomics.
- Status: **OPEN / KNOWN manual result collection**.
- Found during: H4 historical-rate and repeated paced benchmark,
  `G2H_H4_HISTORICAL_RATES_AND_PACED_LOCAL_BENCH_20261011.md`.
- Evidence: the H4 `vitest` harness emits structured
  `G2H_H4_PG_PROFILE`, `G2H_H4_BOOK_PROFILE`,
  `G2H_H4_SHAPED_PG_PROFILE`, `G2H_H4_SHAPED_ROW_JSON_SIZE`,
  and `G2H_H4_DEPTH_BOOK_PROFILE` lines. A two-CPU local
  controlled run was reproduced, but the 4-6 second windows are
  deliberately NOT a claim of sustainable multi-hour capacity.
  Readiness/parity assertions are automated but comparison of percentiles
  between different laptop runs remains a manual step.
- Opportunity: after target workload/resource gate is settled, archive
  machine-readable normalized run profiles and make opt-in trend
  comparisons. Normalize CPU core count, schema/fixtures and sampling
  windows rather than assuming raw p99 latency is portable.
- Failure modes: noisy CI machines cause false regressions, short 6s
  samples masquerade as multi-hour stability, benchmark fixture becomes
  an unintended production credential loader.
- Suggested validation: run same pinned disposable fixture on two
  isolated target-equivalent machines and quantify variance. No
  scheduled production monitoring, new service, or remote DB access.
- Boundary: H-05 still MANDATES real load-shape, duration and resource
  measurements. This entry defers only optional ongoing automation.

### GI-018 | Separate Guardian-only CPU from combined load-generator process
- Stage: post-H4 profiling refinement after correctness gate; if H4
  cannot identify a throughput bottleneck, this becomes blocking there.
- Status: **OPEN / KNOWN mixed CPU measurement**.
- Found during: H4 benchmark when `process.cpuUsage()` necessarily
  included synthetic PostgreSQL writer and Guardian reader/ACK work
  in the SAME Node/Vitest process. DB Docker CPU was only sampled
  once, separately. A subsequent H4 atom restricted Node's affinity to
  2 logical CPUs (availableParallelism=2) and PostgreSQL to 2 CPUs
  / 1.5GB with Docker limits, but still DID NOT isolate pure
  reader CPU from synthetic writer+test-framework CPU. See
  `G2H_H4_TWO_CPU_ARCHIVE_SHAPED_EVIDENCE_20261011.md`.
- Opportunity: run writer and read-only Guardian profiler in
  separate local processes or cgroups with controlled CPU quotas to
  isolate the actual replay/rebase CPU vs writer and Vitest overhead.
  Keep exact same PostgreSQL global ordering and read-only role.
- Failure modes: extra test-process complexity, timing artifacts and
  false bottleneck attribution. Do not introduce IPC into production
  merely to make benchmarks convenient.
- Suggested validation: compare idle/reader-only/write-only/combined
  CPU ms per 1,000 durable rows, at the same source load and byte
  distribution. No architecture decision without measurements.

### GI-019 | Reusable independent economic parity oracle fixture catalog
- Stage: optional testing ergonomics after H2/H4 acceptance.
- Status: **OPEN / KNOWN manual fixture duplication, UNKNOWN benefit**.
- Found during: H4 genuine PG replay of risk reduction admit/fail,
  partial SELL and settlement, 2026-10-11.
- Evidence: `guardian-h4-semantic-pg-oracle.test.ts` performs a
  genuine canonical SQL replay and compares every recovered ledger
  snapshot against an independently executed `PaperLedger` plus
  literal $999.04/$1002.04 cash and $0.24/$2.04 PnL assertions.
  Existing `guardian-offline-durable-source.test.ts` has related
  state/owner fixtures. Rewriting these by hand risks divergent
  prices, fees, dates and canonical operation keys.
- Opportunity: a reusable, deterministic fixture catalog of valid
  economic event sequences, with explicit externally calculated
  cash/PnL expectations and negative mutations. Shared test data
  can reduce maintenance duplication without sharing the actual
  production economic implementation with the oracle.
- Failure modes: a generated oracle calling the SAME reducer being
  tested creates false confidence; combinatorial cost masks root
  cause; fixtures can be accidentally treated as production traffic.
- Validation: compare independent hard-coded arithmetic oracle to
  canonical ledger for at least buy/sell, hedging, settlement,
  maker reservation and owner-failure sequences.
- Boundary: genuine economic parity and fail-closed ownership remain
  **mandatory H-02/H-05 requirements**, never deferred by this entry.
  Only refactoring common test fixture scaffolding is optional.

## NOT optional backlog items: current required work (cross-reference only)

Do **not** defer the following because they determine whether the planned
Guardian is trustworthy; track/close them only in the freeze and H1-H4 audit:

- H-01: real restricted PostgreSQL role/source identity and ACL proof.
- H-02: consistent committed checkpoint+cursor+tail, missing-row and
  integrity uncertainty detection.
- H-03: restart exact state parity, consumer receipt reconciliation and
  **safe continuation past replay-key budget without data loss**.
- H-04: fresh synchronized UP/DOWN observations independent of Engine's
  event loop, including disconnect/epoch safety.
- H-05: measured operational capacity and bounded queue/memory.
- G3 (later mandatory): atomic per-episode residual/owner validation
  at executor admission and execution; unknown-order reconciliation.

## Entry template

```md
### GI-0NN | Concise title
- Stage:
- Status: OPEN / ACCEPTED / REJECTED / DEFERRED; KNOWN / LIKELY / UNKNOWN.
- Found during: H#/commit/date.
- Evidence: code path, test name or measurement with units.
- Why useful:
- Cost/failure modes:
- Suggested validation:
- Final decision: pending; never silently add to a current freeze.
```

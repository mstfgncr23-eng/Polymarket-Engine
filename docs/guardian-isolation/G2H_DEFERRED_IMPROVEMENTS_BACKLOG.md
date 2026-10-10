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
- Evidence: current normalized book bridge is a pure callback consumer;
  the target Guardian must keep working when Engine stalls.
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

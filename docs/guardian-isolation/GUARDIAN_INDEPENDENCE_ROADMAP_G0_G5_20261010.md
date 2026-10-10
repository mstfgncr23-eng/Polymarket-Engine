# Guardian Independence | G0–G5 Execution Roadmap

Current date: 2026-10-10
Branch: `refactor/guardian-isolation-atomic`
Source authority: forensic stage 08_current / independent worktree; no merge-base assumption with historical `main`.
Operator rules: **NO SHADOW**, no second running live Guardian, no dual engine, no duplicate economic writers, PAPER only, no unapproved VPS/database changes, separate atomic commits, measured before design changes.

## Definition of done (THE END)

**G5 concludes the Guardian independence project**, not an automatic permission to trade LIVE.

Success means:
- Engine owns strategy, market qualification, 12/5/2 Profit-trailing and normal trade intents.
- Guardian is its own bounded, observable process and owns only protection monitoring/decisions for *actual open residual positions*.
- One execution authority admits and executes both normal and protective orders against CURRENT, durable episode / token / qty / cash / owner / book fences.
- No hidden / shadow second Guardian; no concurrent dual-writer cutover at any point.
- Integrity, DB uncertainty, 8/8 WS loss and restart have explicit fail-closed paths.
- PAPER validation has complete fill/fee/settlement/PnL reconciliation and a real final-report authority; objective CPU/WS reliability improves without business-regression evidence.
- Operator approves any cutover. LIVE wallet connection is a separate later decision and cannot be inferred from G5 acceptance.

## Completed, each atomic

| Gate | Result | Commit | Evidence |
| --- | --- | --- | --- |
| G0 | source pinning, NO-SHADOW & single-writer invariants | `17de09d` | `ATOMIC_MIGRATION_CONTRACT.md` |
| G1 | strict PAPER-only Guardian protection request contract and pure reject-only precheck | `84d37fe` | `G1_BOUNDARY_ACCEPTANCE.md` |
| G2A | Guardian durable asynchronous test fixture repairs; exact idempotency/crash keys | `b7cc1eb` | `G2A_DURABILITY_TEST_BASELINE_20261010.md` |
| G2B | transport-free paired-book pressure projection | `ddaf674` | `G2B_PURE_BOOK_SIGNAL_ACCEPTANCE_20261010.md` |
| G2C | duration/Profit-trailing action priority with veto fallback and 80s expiry fence | `e6446ef` | `G2C_DURATION_PAIR_PRIORITY_ACCEPTANCE_20261010.md` |
| G2D | lower Guardian state-history CPU work; frozen 720-step SHA-256 trace | `a0caab9` | `G2D_GUARDIAN_HISTORY_CPU_ACCEPTANCE_20261010.md` |
| G2E | pure read-only episode projection with durable cursor, duplicates and settlement fences | `80b61a3` | `G2E_POSITION_AUTHORITY_PROJECTION_ACCEPTANCE_20261010.md` |
| G2F | offline checkpoint + real durable-transition reducer adapter (limited clean checkpoint) | `397ab84` | `G2F_OFFLINE_DURABLE_REPLAY_ACCEPTANCE_20261010.md` |

Last G2F evidence: 9/9 targeted and 191/191 total tests PASS; scoped engine/core TypeScript PASS; NO running process, NO deploy.

## Remaining gates (not a fixed count of tiny commits)

### G2 remaining: Guardian actually out of engine

Likely next atomic slices:
- **G2G: Full legitimate checkpoint hydration.** Correctly model maker reservations, pending risk owner/failure, trailing, Guardian state, settlement lookup, integrity status. If any source evidence is untrusted, halt. Prove ledger/episode equality after every transition.
- **G2H: Read-only source ownership + bounded transfer.** Exact cursor from trusted Postgres/checkpoint source, authenticated source epoch, controlled backpressure, restart, DB-unavailable and incomplete-event modes. There must be no silent gap or contradictory open position.
- **G2I: Single isolated Guardian process, OFFLINE-only.** Pure observation/runtime relocated, rate/CPU benchmark, crash/replay and state transitions with saved timestamped fixtures. No real-market socket or live second Guardian, no shadow decision comparisons.
- Additional sub-atoms ONLY if test-proven blockers appear. **G2 complete** when a separate offline Guardian can reproduce correct deterministic state and safely stop/restart from trusted committed positions while respecting CPU budgets. No economic execution authority yet.

### G3: One shared execution authority, single writer

- Extract and fence order admission from EngineService while preserving current taker/maker lifecycle.
- Engine and Guardian emit typed requests, not independent trades; no speculative privilege for Guardian.
- Prove serialized economic execution and durable ownership under concurrent Profit-trailing, Guardian emergency, legacy 300s fallback and 80s market-expiry.
- Inject duplicate command, uncertain PostgreSQL commit, ACK loss, crash before/after fill, stale token and partial quantities. **G3 complete** only when the same PAIR/SELL residual cannot be consumed twice and all balances reconcile exactly.
- Do NOT assume a separate executor is necessary until isolating the current execution dependencies and failure isolation requirements.

### G4: Operator-authorized single-authority cutover, rollback

- Write exact deployment plan, baseline CPU/WS/DB and journal integrity gates.
- Stop the old active authority before starting its replacement. NO overlap, NO SHADOW.
- Verify no unresolved open orders/inventory/commit ambiguities. Require a documented safe maintenance window and rollback.
- Any refusal/gap means no cutover. G4 requires specific operator approval and must never be inferred from approval for local code fixes.

### G5: Production-like PAPER validation and exit criteria

- Start with clean basic smoke + deterministic fault harness. No LIVE or wallet credentials.
- Then staged soak (short sanity, long enough to observe market lifecycles; 24h acceptance after prior gates pass).
- Track valid fresh ticks, 8/8 reference clusters, event-loop p99, Guardian CPU, engine CPU, queue lag, DB commit latency and integrity.
- Reconcile every first leg, second leg, protective SELL, fees, settlement, realized PnL and experiment final canonical report. Missing final report is a blocker, not a success.
- If economics, crash recovery or reference continuity regresses, stop and revert via approved plan.

## Status shorthand

`G0 ✅ | G1 ✅ | G2A–F ✅ | G2G → | G3 ⏳ | G4 ⏳ | G5 ⏳`

*G2 letter suffixes may grow as specific independent tests reveal atomic blockers. The goal is **G5**, not an endless numbered series or a fixed estimate of elapsed time.*

## Non-authorizations

This document does not create a future task, does not restart anything, does not grant shell or VPS production mutation permission, does not switch PAPER/LIVE, does not touch money, and does not approve process cutover.

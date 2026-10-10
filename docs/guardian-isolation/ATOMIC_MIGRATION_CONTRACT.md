# Guardian Process Isolation: Atomic Migration Contract
Date: 2026-10-10
Status: Design/safety gate only. No runtime implementation, cutover or deployment approved by this document.

## 1. Non-negotiable operator constraints

1. **NO SHADOW**. Never start, deploy, schedule, or conceal a second live Guardian alongside the authoritative one. No background live-decision comparisons, mirrored real-market execution, shadow journal, or parallel live engine. Offline deterministic unit tests and saved fixtures are permitted; they are not a second operating Guardian.
2. Changes are atomic: one independently reviewable boundary per commit, narrow tests, a measured result, and an explicit stop/go decision. No bundled all-at-once rewrite.
3. Preserve the PAPER-only boundary. No wallet, LIVE credentials, production execution, or unapproved production changes.
4. No automatic restart, traffic switchover, database repair, cleanup, schema mutation, or container deployment without specific operator approval.
5. Exactly **one** authoritative order executor and ledger writer at all times. No dual submission ownership, even temporarily during migration.
6. Existing 12/5/2 Profit-trailing strategy behavior remains intact unless explicitly reviewed and separately approved.
7. Do not silently remove the 300-second legacy max-unhedged protection or the distinct 80-second pre-expiry safety mechanism. The old trigger and end-of-market safety must be analyzed as independent policies.
8. If an invariant cannot be proved, stop. Do not treat a successful test suite as permission to deploy.

## 2. Source authority and state of the world

- Clean source authority: branch `refactor/guardian-isolation-atomic`, initially at `4fd4ae56ea9ab27d81c57711dc533cf6d0387f4f`.
- Original forensic worktree: `C:\Users\acer\Desktop\Polymarket-Engine-reverse-regression`; contains untracked research/tests and must not be cleaned or overwritten.
- Verified: `python research/reverse_regression/stage.py --stage 08_current --verify` returned `managed=76 different=0 created=0 replaced=0 removed=0 mode=READ_ONLY` on the parent forensic worktree.
- Relevant compiled-image source map authority: `research/reverse_regression/evidence/08_current.manifest.json`. Source maps are stronger evidence for the running image than the compatibility `gitSha` manifest label.
- Historical `main` branch has **no merge base** with the forensic lineage; do not rebase/merge blindly into main.
- 2026-10-10 V2 2h PAPER soak: 19 completed positions, 13 wins/6 losses, net trading PnL −$4.89629098. Recorded 386 reference closes, 14 8/8 close clusters, and a `REPORT_SOURCE_INTEGRITY_FAULT` with PostgreSQL write outcome unknown. These are observed facts, not proof of every causal edge.
- Current engine's journal/ledger and order path are a tightly coupled authority. Refactoring them is an economic state migration, not just code movement.

## 3. Code-level authority map (confirmed against source)

| Concern | Current owner / entry point | Extraction implication |
| --- | --- | --- |
| Market snapshots | `apps/engine/src/engine.ts:onMarketSnapshot` | Calls `observeGuardianSnapshot` on the main engine event-loop |
| Guardian observation | `guardianObservation`, `evaluateGuardianRuntime` | Depends on synchronized, fresh UP/DOWN CLOB books and the current open episode |
| Guardian state | `guardianRuntimeStates`, `guardianPersistenceQueues`, `reversalProtectionStates` | In-memory and durable versions must agree before action |
| Guardian decisions | `maybeReversalProtection`, `reversalEligibility` | The decision is NOT itself permission to submit |
| Order arbitration | `executeRiskReductionArbitration`, `riskReductionOwners` | Currently in engine; shared with legacy deadlines |
| Durable economic mutation | `DurableMutationGate`, `commitAuthoritative`, `PAPER_FILL` | Source of truth; must have one writer and a stable replay identity |
| Profit-trailing | `kickProfitTrailingPair`, normal hedge executor | Remains independent of Guardian's observation logic |
| Pre-expiry | `protectiveDeRiskEligibility` | Effective deadline is min(open age + 300s, market end − 80s), when market metadata exists |

## 4. Intended process boundaries

**ENGINE** owns market discovery, strategy, first-leg intent, 12/5/2 logic and normal trading intents.

**GUARDIAN** runs as its own bounded process. It consumes committed positions and trustworthy market data; evaluates only open residual directional risk and emits protection requests. It MUST NOT initiate new speculative entry positions or mutate authoritative balances/positions. It cannot assume the engine event-loop remains responsive.

**EXECUTION AUTHORITY** is the only component that can admit, cancel and execute economic orders or change the authoritative ledger. It arbitrates Engine and Guardian requests against the latest episode and current synchronized market books. A separately running executor is the target for engine-stall tolerance; its actual implementation requires extraction of the existing economic commit boundary, not duplication.

**POSTGRESQL** holds authoritative durable events and checkpoints. A second consumer may read snapshots/events but may not independently commit fills; transport and recovery must be explicitly versioned and fenced.

This is a **target**, not a claim that the current code already provides these process interfaces.

## 5. Protection request contract: mandatory fields

Proposed minimum typed fields, to be proved and refined in a future atomic change:
- `requestId`: globally unique, idempotent across retries.
- `experimentId` and PAPER policy gate.
- `marketId`, current `positionId`, `unhedgedStartedAt`, `heldOutcome`.
- `expectedResidualQty`, current authority sequence or durable fence; never an unfenced cached amount.
- `decisionReason`: one of existing locked Guardian reasons, NOT arbitrary strategy actions.
- `observedAt`, reference event IDs, required freshness/synchronization limits, expiration.
- `requestedKind`: `PROTECTIVE_SELL` or `HEDGE_SECOND_LEG` as *request*, not a pre-approved order.
- `policyGeneration` for schema/config compatibility.

The executor must independently recompute executable side, qty, depth, fee-inclusive salvage, token identity, current episode, market end and ownership **at admission and again at commit**. Treat all Guardian-provided prices as evidence, never as final execution authority.

## 6. Non-negotiable correctness invariants

- **Single-writer invariant**: exactly one component commits each `PAPER_FILL` and derived position transition.
- **Episode fencing**: an old request must never affect a re-opened position in the same market.
- **Residual conservation**: no risk-reduction order may exceed the still-open residual; a PAIR and SELL must not both consume the same shares.
- **Idempotency**: duplicate request, ACK loss, executor restart and uncertain write outcome never cause duplicate fills.
- **Ordered evaluation**: ignore stale/future/out-of-order book or Guardian inputs; a late confirmation cannot cancel newer-order ownership.
- **Safety first**: no protective execution with stale/unsynchronized CLOB, unknown DB commit outcome, unresolved integrity fault, unavailable authoritative position, or expired market.
- **Bounded resources**: Guardian cannot congest engine's network ingress, CPU/event-loop or journal admission; overload is surfaced, bounded and fail-closed.
- **Visibility**: distinct health for Guardian, engine and executor, with queue depth, age of last *valid tick*, action age, rejection reason, reconnect duration and event-loop delay.
- **Recovery**: process restarts rehydrate from authoritative durable evidence and reconcile in-flight orders before accepting new work.
- **Accounting**: same fills/settlements must produce identical gross, fees, cash, final PnL; no normalization of earlier defects to hide mismatches.

## 7. Atomic gates and stop conditions

| Gate | Deliverable | Required proof | Stop condition |
| --- | --- | --- | --- |
| G0 | Source, risk and ownership map (this document) | Stage 08 source-map verify; clean isolated branch; known uncertainties | Source not authoritative |
| G1 | Pure versioned observation/action schemas | Unit tests: invalid episode, malformed qty, stale timestamps, duplicate request; zero runtime wiring | Incompatible event contract |
| G2 | Guardian observation runtime isolated as a library and process, **offline only** | Deterministic fixtures, restart, CPU and ordering tests; never attach live feeds beside deployed Guardian | Behavioral mismatch or unbounded CPU |
| G3 | Shared execution authority extraction, one-writer proof | Fault injection for concurrent PAIR/SELL, partial fills, idempotent retry, pre-expiry arbitration and database ambiguity | Two writers or unprovable cash/order fence |
| G4 | Production-grade single-writer cutover plan and rollback rehearsal | Explicit operation checklist; only one active Guardian and one executor after cutover; no live parallelism | Unfinished positions, unresolved DB integrity, no rollback proof |
| G5 | Operator-authorized isolated PAPER validation | Real feed continuity, 0/8 recovery, economic ledger parity, CPU/event-loop, guardian trigger outcomes and complete final-report integrity | Any fail-closed, missing final report, or unattributed economic variance |

No step implicitly authorizes later gates. G4/G5 require a separate explicit approval before modifying running processes.

## 8. Required local deterministic tests (not shadow)

- Pure Guardian EARLY/LATE arm/recover/emergency/timeout with saved timestamped observations; replay determinism.
- Market has two unrelated positions at different epochs; stale request from epoch A must fail.
- Guardian and Profit-trailing concurrently target one residual; exactly one admissible owner.
- Guardian emergency and 300-second/80-second fallback collide; prove deterministic priority and only one economic exit.
- EndDate unknown, within 80s, already expired, delayed callback and out-of-order market snapshot.
- Crash before order creation, after admission, after fill, before ACK, during checkpoint, during retry.
- PostgreSQL slow, connection timeout and uncertain commit; no guessed success or duplicates; final report integrity gate.
- 8/8 close, asymmetric 7/8, reopen, connected-but-no-ticks, stale CLOB book and heartbeat recovery.
- Repeated market reopen and restart from durable transitions with bounded Guardian state.
- 19-trade 2026-10-10 PAPER arithmetic independent oracle: 13/6, net −$4.89629098, no silently changed fill economics.

## 9. Today / not today

Done in G0: read-only source and architecture audit, safe isolated Git worktree, zero shadow, zero production change.

**NOT DONE**: working independent Guardian process, extracted executor, integrated IPC, migration, runtime tests, cutover, CPU fix, financial-policy changes, production deployment.

Only advance to G1 after confirming the G0 document and source authority. Each later gate must be independently tested, reviewed and committed. If the test setup cannot be isolated without impacting the running VPS, do not run it.

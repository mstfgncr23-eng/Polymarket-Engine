# Guardian Independence Roadmap, Rev 2: Outcome-based gates

Updated: 2026-10-10 | Branch: `refactor/guardian-isolation-atomic`
Current HEAD after G2G-2: `06c446f`
This document supersedes the older atom-oriented G0–G5 roadmap **for scheduling and completion criteria**. Historical acceptance notes and commits remain immutable.

## Business goal and architecture

Original goal: Engine remains the trading product; Guardian becomes a separate **continuously operating protective process** for actual open, unsettled, directional residual exposures.

- ENGINE: discovery, qualification, 12/5/2 strategy and normal trade intent.
- GUARDIAN: independently observes confirmed open episodes + synchronized fresh market books; issues narrowly typed **protection requests**, never arbitrary speculative orders.
- ONE ECONOMIC EXECUTOR: authenticates both request sources and rechecks durable residual, episode, order owners, exact quantity, executable depth/fees, token epoch, outstanding maker/taker state, DB uncertainty and idempotent fill result.
- SOURCE OF TRUTH: authoritative durable transitions + independently verified checkpoint, not UI state, journal mirror, fuzzy request timestamps or stale in-memory reads.
- An independent Guardian process does **not** imply a second active Guardian or separate independent order writer. **SHADOW IS PROHIBITED**.

## What we know / what is not proven

KNOWN:
- G0 `17de09d`, G1 `84d37fe`, G2A `b7cc1eb`, G2B `ddaf674`, G2C `e6446ef`, G2D `a0caab9`, G2E `80b61a3`, G2F `397ab84`, G2G-1 `cd7c81a`, G2G-2 `06c446f` are committed to this branch.
- Guardian's hot-path repeated deep history clone caused measured CPU cost; G2D reduced single local observation duration, NOT proven full engine CPU or WS stabilization.
- Existing economic recovery can replay real positions, reservations, admitted taker ownership, Guardian V2 and trailing states. Offline G2G-2 verifies 18 tests; its broad regression recorded 216/216.
- These G2 parts are **contracts/tests/offline adapter inside the repo**. Guardian is NOT yet a separate running process.

UNKNOWN, cannot be asserted as done:
- Trusted live producer epoch, durable cursor + retention/compaction handoff, authenticated delivery and durable consumer ACK.
- Accurate independent UP/DOWN snapshot synchronization under 8/8 reference resets, book freshness, stale token epoch and WS reconnection.
- Bounded CPU/heap/queue lag and graceful reconnect/replay at realistic runtime throughput.
- One-writer execution under cross-process concurrent Guardian/Profit-trailing requests, unknown commit result, dropped ACK, crash/replay.
- Zero unexplained open residual/PnL/report reconciliation during a sufficiently long PAPER soak.

## Five end-to-end delivery gates (G0/G1 foundations already met)

| Stage | Real deliverable | Completion check | Status |
| --- | --- | --- | --- |
| G0–G1 | Safety rules and PAPER-only request contracts | Source/authority fixed, invalid actions and stale identities rejected | DONE |
| G2 | **One independent Guardian process** consuming trustworthy positions + market books, surviving restart, producing only protection requests | Closed fixture test: active episode -> valid request -> episode close, restart, fault injection, CPU/heap/queue targets | IN PROGRESS |
| G3 | **One economic executor** for Engine/Guardian intent with durable idempotency | Concurrent/duplicate/stale requests, fill before/after crash and DB unknown result yield at most one economic consumption | NOT STARTED |
| G4 | **One-time operator-authorized cutover** to isolated Guardian without overlapping old Guardian | Preflight, old owner stopped, full authoritative state recovered, new owner starts, rollback and monitoring established | NOT STARTED |
| G5 | **PAPER operating acceptance**, then project closed | Short smoke, staged soak incl 24h, complete canonical experiment/PnL/fill/fee/settlement reconciliation, CPU/WS/lag/DB baselines | NOT STARTED |

### G2 finish line: exactly three remaining deliverables, not an endless alphabet

**G2G (finishing state correctness)**: DONE in the offline read-only adapter. G2G-1 `cd7c81a` (maker/taker ownership), G2G-2 `06c446f` (Guardian/trailing/settlement), plus final `G2G_STATE_COVERAGE_CLOSEOUT_20261010.md` (naked diagnostics and explicit integrity/quarantine halt). This proves schema/episode/invariant coverage for eligible verified fixtures, **not** a trusted real-source feed or a resolution of an unknown DB commit. Those are required G2H producer checks.

**G2H (trusted data path)**: Choose the actual producer/consumer mechanism based on current code and production implementation evidence, then implement a narrow source protocol. Require verified checkpoint hash against trusted store, writer epoch, global durable sequence including unrelated transitions, compacted retention gap detection, heartbeat, backpressure, bounded queue, strictly fresh paired market books, feed reconnect epoch and consumer fail-closed on unknown state. Test partial packet, duplicated record, gap, source failover, DB latency and disconnection. **No hidden shadow feed or second live Guardian**.

**G2I (separate process OFFLINE harness)**: Move the Guardian observer/decision loop into one supervised isolated process with limits, request-only output, no wallet or economic submitter. Replay saved fixtures as the **only** active Guardian in a closed harness. Measure CPU/event-loop and the restarts/lag; prove all open positions eventually tracked, closed episodes cannot trigger commands, bounded memory under 100Hz test and monotonic resumption. G2 completes **only when this process actually runs and passes**, not when a pure library compiles.

Sub-atoms are implementation commits, not new milestones. If another correctness issue appears, fix it in the active gate with a focused test and journal entry. No new G letters by default.

### G3: execution authority

Retain the existing correct single-writer mechanism until its actual boundaries are mapped. Extract only what is needed. The Guardian may suggest protective SELL / HEDGE; it never bypasses one canonical engine-owned risk gate. Fault-test durable per-episode ownership and residual qty, cancelled or adopted makers, simultaneous profit-trailing and Guardian, DB commit-unknown, duplicate/delayed request and retry after restart. Any uncertain economic result means stop/reconcile, not submit another fill.

### G4: controlled operational change

Requires separate explicit operator permission. PAPER only. Before cutover: complete manifest, current positions/makers/owners, report history, backups and rollback image, single-writer fencing and live health. Stop old Guardian first; recover current durable cursor and active episodes; start one new Guardian, zero overlap. Abort on unknown commit or data gap; rollback only under proven ownership fencing. **No shadow run**, even briefly.

### G5: measurable acceptance

Short smoke -> lifecycle fault tests -> longer controlled PAPER soak -> **24h final soak** after the previous gates are green. Measure tick freshness (not just socket CONNECTED), clustered 8/8 reference closes, event-loop p95/p99, per-service CPU/heap, durable lag, commit latency and all admitted/filled orders, fees and settlement realized PnL. Require a complete canonical experiment report; a missing or truncated report is a blocker. Compare with a measured same-workload baseline and investigate every unexplained loss/state divergence.

No automatic wallet link or LIVE activation after G5. That is a separate decision.

## Immediate next work

1. Close remaining G2G data-integrity exclusions, without inventing policy changes.
2. Implement and test G2H trusted source and fresh paired-book path.
3. G2I single isolated process in a closed harness.
4. Only then G3, G4, G5.

## Operating rules (nonnegotiable)

NO SHADOW. NO parallel production Guardian. NO VPS/deploy or LIVE without explicit cutover approval. Atomic scoped commits + named acceptance note + clean working tree. Run test and typecheck per gate. Distinguish known, likely, unknown. Failure => remain on that gate rather than declaring progress from task count.

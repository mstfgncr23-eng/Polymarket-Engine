# G2B | Pure Guardian Book Projection Extraction

Date: 2026-10-10
Branch: refactor/guardian-isolation-atomic
Purpose: Isolate one deterministic market-observation computation from the engine without splitting runtime ownership.

## What changed

- Extracted the exact two-sided, top-of-book Guardian projection from `EngineService.guardianObservation` into the pure, transport-free `packages/core/src/guardian-book-signal.ts:projectGuardianBookSignal`.
- Preserved the current engine's upstream freshness, token epoch, sync state and token identity checks, as well as UP-positive pressure normalization and ordered event-ID provenance.
- Did not alter `evaluateGuardianV2`, any Guardian state machine, durable queues, position bookkeeping, risk/deadline policy, 12/5/2 strategy, or execution authority.
- Added tests comparing the extracted projection with a separate arithmetic expression of the original logic, including both held outcomes and invalid books.
- A module that can run without EngineService is an **extraction step**; it does not imply independent live Guardian or service has been implemented.

## Proof

- `pnpm exec vitest run tests/guardian-book-signal.test.ts tests/reversal-protection-paper.test.ts tests/guardian-boundary-contract.test.ts tests/profit-trailing-pair.test.ts tests/reversal-protection.test.ts`: **73/73 PASS**.
- `pnpm --filter @pm/core typecheck`: PASS.
- `pnpm exec tsc -p apps/engine/tsconfig.forensic.json --noEmit`: PASS.
- `git diff --check`: PASS.
- Direct evaluation is pure and does not create a process, a socket, a thread, a timer, a journal or an order. **NO SHADOW**.

## Still blocked / separate economic defect

The existing profit-trailing PAPER integration test `persists high-water across compacted restart and trails profit after legacy deadline` still fails on its unchanged baseline: at 303000ms the current policy hands execution from profit trailing to `MAX_UNHEDGED_DURATION`, which can act upon the first updated held-side book before the opposite-side book arrives. This produces a `PROTECTIVE_DE_RISK` SELL instead of the expected profitable second-leg BUY.

This is NOT fixed in G2B. It is a **policy/priority and paired-observation atomicity** issue rather than book-projection arithmetic. Required next step: separately specify priority under stale/missing opposite book, 300s duration threshold, 80s pre-expiry protection, positive executable PAIR, Guardian-confirmed risk, and one-writer admission. Do not silently erase the protective fallback or permit stale book execution merely to satisfy a test.

## Guardrails

- No VPS, deployment, wallet, LIVE orders, schema migration or source cleanup.
- No shadow process, no dual Guardian or dual executor.
- Test sources only on local single-engine PAPER fixtures; no live market sockets.

# G2C | Deadline-versus-PAIR Authority, Atomic Economic Correction

Date: 2026-10-10
Branch: `refactor/guardian-isolation-atomic`
Parent: `ddaf674680f04e09ae4c03a1edaf520d5c371995`
Scope: LOCAL PAPER source and deterministic integration fixtures ONLY. NO SHADOW.

## 1. Root cause proven before change

- `profitTrailingRetainsAuthority` returned false on **any** protective eligibility after the first unhedged 300s, regardless of whether a fresh full, fee-inclusive profitable and actionable PAIR existed.
- After a compacted checkpoint restart, synchronized UP and DOWN snapshots arrive via distinct callbacks. The first UP could immediately launch legacy A/B deadline rescue before the opposite DOWN callback.
- Baseline single test `persists high-water across compacted restart and trails profit after legacy deadline` failed after 303000ms: `PAPER_FILL` intent `PROTECTIVE_DE_RISK`, SELL 5 UP @0.80, rather than expected positive `HEDGE_SECOND_LEG`.
- This is a real arbitration/restart integration defect. It does not establish that every old-soak loss was caused by it.

## 2. Narrow operational change

File changed: `apps/engine/src/engine.ts`.

1. Existing 300s max-unhedged deadline **remains enabled**. It may still select a protective SELL or opposite BUY rescue, based on current A/B economics.
2. **Only when** 12/5/2 Profit-trailing is enabled, a synchronized, fresh, fully executable pair of books yields fee-positive locked PnL **and** the exact current `evaluateProfitTrailingPair` has an actionable `TARGET` or `TRAIL`, that specific HEDGE SECOND LEG may retain priority **past** the 300s duration deadline.
3. This exception is recomputed at the existing order admission and durable mutation fences using current position/episode and books. No unfenced blanket exemption, no stale-book bypass, no speculative first-leg authority.
4. From `marketEnd - 80,000ms`, Profit-trailing may **not** retain priority, even if a gross/fee-positive PAIR is present. The pre-expiry safety controller remains authoritative.
5. After the duration deadline, snapshot/hydration-triggered fallback gets **one event-loop turn** of coalescing, allowing a paired UP/DOWN update to reach the trailing evaluator. This is NOT a time-based grace interval, new timer, persistent waiver, or guarantee that separate network packets will land in the same turn.
6. If the trailing candidate fails canonical risk admission, full-quantity eligibility, or final order submission, its **exact current episode + UP/DOWN event IDs** are marked rejected. Rejected evidence cannot keep suppressing the legacy protective A/B fallback. The mark expires when source evidence changes, and is cleared when the residual closes.
7. The explicit maintenance/deadline fallback remains unmodified. If no synchronized positive actionable pair is available, or its risk admission rejects, the existing protective A/B controller remains available. One-turn coalescing does not suppress fail-closed conditions.

No new daemon, queue service, polling feed, Guardian process, executor, dependency, schema migration, or LIVE action is created.

## 3. Guarding against overreach

- "Positive gross edge" alone is NOT enough: fee-inclusive PnL must be positive and `TARGET` or `TRAIL` must fire. A merely armed / new-high-water position gets normal post-duration protective fallback.
- If the opposite book remains absent/stale beyond that turn, A-only protective SELL remains possible.
- Fallback A/B may choose `RESCUE_SECOND_LEG` rather than `PROTECTIVE_DE_RISK` when a B rescue is economically preferable. Neither is a Profit-trailing `HEDGE_SECOND_LEG`.
- Guardian's independent confirmation route and durable one-writer risk reduction ownership remain untouched, but simultaneous Guardian/emergency events at the exact duration boundary still warrant explicit fault/race tests before cutover.
- The source remains inside the EngineService. Independent Guardian process remains NOT DONE.

## 4. Evidence: before/after and regression

BEFORE:
- `tests/profit-trailing-pair-engine.test.ts`: 1 fail, 2 pass. Failure SELL via `MAX_UNHEDGED_DURATION`.

AFTER:
- Same restart/compaction test now reaches the expected `HEDGE_SECOND_LEG`, with no concurrent protective/rescue/Guardian fill.
- Five additional adverse-path tests:
  - negative/fee-unprofitable pair => deadline SELL remains;
  - positive but non-TARGET/non-TRAIL pair => A/B fallback, may be SELL or rescue BUY, never Profit-trailing;
  - explicitly injected canonical trailing risk veto => bounded handoff to A/B, no stalled residual;
  - stale/absent opposite book => a single fresh held book does not indefinitely block A-only fallback;
  - exact 80s pre-expiry boundary => Profit-trailing may not outrank protective A/B.
- `pnpm exec vitest run tests/profit-trailing-pair-engine.test.ts tests/engine-guardian-paper.test.ts tests/reversal-protection-paper.test.ts tests/guardian-boundary-contract.test.ts tests/guardian-book-signal.test.ts`: **112/112 PASS**.
- `PM_FORENSIC_STAGE=08_current pnpm exec vitest run tests/guardian-v2.test.ts tests/profit-trailing-pair.test.ts tests/reversal-protection.test.ts`: **55/55 PASS**.
- `pnpm --filter @pm/core typecheck`: PASS.
- `pnpm exec tsc -p apps/engine/tsconfig.forensic.json --noEmit`: PASS.
- `git diff --check`: PASS.
- The forensic fixed-rate overhead test previously executed: stage-specific metrics show Guardian observation still dominates CPU; it is NOT a performance victory. An unrelated run of `tests/guardian-v2.test.ts` without its required `PM_FORENSIC_STAGE` env failed with `UNKNOWN_OR_MISSING_FORENSIC_STAGE`; rerun with the correct env passed 55/55.

## 5. Known limitations and next atom

- One event-loop turn does NOT guarantee a network-delayed second-side book arrives before a protective SELL. We refuse an indefinite delay that would hide a dangerous open residual.
- The 300s duration and 80s pre-expiry policies still exist; removing either requires explicit design/proof of replacement failure-mode protection, not test expectation manipulation.
- The source-map forensic baseline contains an unrelated missing `source-seal.js` full-repository typecheck dependency. The scoped forensic engine TS target is green; this change does not fabricate that file.
- Need atomic Guardian process isolation and durable position-event ownership next; no cutover without fault and fill-idempotency proof.
- VPS and running PAPER environment were not changed.

## 6. Commit discipline

All modified files are in this atom only:
- `apps/engine/src/engine.ts`
- `tests/profit-trailing-pair-engine.test.ts`
- this acceptance document

No changes to databases, migrations, wallets, manifests, 12/5/2 numeric thresholds, Guardian threshold values, or VPS.

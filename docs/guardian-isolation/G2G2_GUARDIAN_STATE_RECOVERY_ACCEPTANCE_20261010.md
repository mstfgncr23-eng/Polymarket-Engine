# G2G-2 | Guardian / Profit-trailing / Settlement State Recovery

Date: 2026-10-10
Branch: refactor/guardian-isolation-atomic
Parent: cd7c81a (G2G-1)
Stage: 08_current forensic source, offline-only.

## What was fixed

Real stage-08 `hydrateCheckpointPayload` already parses Guardian V2 state, Profit-trailing high-water, and economic settlement lookups. The G2F/G2G-1 offline read-only adapter formerly rejected legitimate checkpoints holding them, so it could not truthfully follow a busy open position across restart.

`apps/engine/src/guardian-offline-durable-source.ts` now restores these states with the original `reversalProtectionStateSchema`, `profitTrailingPairStateSchema`, and `settlementLookupSchema`, with duplicate-market rejection and a settlement lookup that must correspond to actual nonsettled inventory. It runs `validatePostReplayInvariants` after the checkpoint AND every durable transition.

This does **not** grant the recovered Guardian state the right to trade. The projection is still offline, read-only and dependent on an externally trusted durable checkpoint.

## Explicit conditions

- Guardian state MUST match the exact same market, position, `unhedgedStartedAt`, held outcome, active risk configuration, locked Guardian version/generation and positivity of residual. Current recovery invariant validates these fields.
- Profit-trailing state MUST match the same position and episode.
- Duplicate Guardian/trailing/settlement records for one market fail closed, rather than last-write-wins.
- Settlement lookups MUST describe a currently economically held, nonsettled market.
- Durable `PAPER_FILL` ending the residual clears stale Guardian, Profit-trailing and settlement mapping via the real recovery reducer.
- `nakedDirectionalDiagnostics`, unresolved source-integrity/quarantine and other unmodeled state remain fail-closed. Never claim full service recovery while these are unsupported.
- Entire `RecoverableEngineState` is cloned per row. This is a correctness reference, **not** a production-speed transport implementation.

## Tests

- `tests/guardian-offline-durable-source.test.ts` now has **18 tests**, including four new G2G-2 tests.
- Tests use actual `evaluateGuardianV2` generation BAD_REGION_V3 and actual `evaluateProfitTrailingPair`, with 12/5/2 / Guardian risk config and real `PaperLedger`.
- First test run exposed a stale-episode fixture rejected earlier by existing semantic checkpoint verification. The test was corrected to require the actual fail-closed rejection, not to loosen code.
- Guard state restore, MTM retention, mismatched generation, duplicate market state, invalid episode, complete SELL clearing policy and lookups are tested.
- Core TypeScript: PASS; forensic engine TypeScript: PASS; `git diff --check`: PASS.
- Broad regression: **216/216 PASS in 12 test files**, PM_FORENSIC_STAGE=08_current. Full exit code 0, terminal record `%TEMP%\\guardian_G2G2_acceptance_20261010.log`.
- No second process, live market socket, DB schema, VPS, wallet, LIVE trading, or shadow mode was used.

## Still needed for G2 completion

G2H: authenticated external source/epoch, exact global durable cursor including compacted/unknown rows, bounded delivery and latest valid per-side executable book with reset on feed disconnect, restart/replay after unknown commits, halt on lag or hash ambiguity.

G2I: one isolated Guardian process in an OFFLINE closed harness with deterministic market and open-position stream, memory/CPU limits, journal replay, idempotent outgoing **requests only** and crash/restart tests. No simultaneous production Guardian; any real process cutover belongs to G4 approval.

G3: a single economic executor accepts and serializes both engine and Guardian requests against durable residual/episode and current book; no autonomous Guardian orders.

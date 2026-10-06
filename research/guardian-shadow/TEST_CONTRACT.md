# TEST_CONTRACT.md — Two-Stage Guardian Historical Screen

## Authority
- Source soak: `FINAL_SOAK_BUNDLE.tar.gz`
- Expected soak SHA256: `8838cf6e857c0ad17384e1f343fff4a09e0c23fefc4789724f96d71ba6676315`
- Historical first-leg cohort: exactly 166 trades
- Historical PnL: `-36.6570110804`
- Historical paths:
  - NORMAL_HEDGE: 92 / +13.704592266
  - PR3_RESCUE: 51 / -18.192022564
  - PROTECTIVE: 21 / -34.689811831
  - UNPAIRED_SETTLEMENT: 2 / +2.520231049
- Historical meteors <= -$4: 6

The script MUST abort if these baseline anchors fail.

## Policy under test
This screen tests only the requested two-stage Guardian design.

### Stage 0 — NORMAL priority
Existing profitable NORMAL_HEDGE behavior is preserved. The helper must not modify a trade unless a Stage-2 failure condition is actually observed before the historical exit.

### Stage 1 — existing PR3 remains
Historical successful PR3 rescue cycles are preserved EXACTLY.
The test does NOT remove PR3 and does NOT reinterpret successful PR3 fills.

### Stage 2 — Protective Helper
For still-directional positions, on each available contemporaneous replay state:

- PR3 mark-loss trigger remains `$0.35`.
- PR3 pair-lock cap remains `$0.60`.
- If mark loss is below `$0.35`, helper does nothing.
- If the PR3 lock is still full-size executable and within `$0.60`, helper does nothing.
- If PR3 is over-cap or full opposite depth is unavailable, and a full held-side SELL is executable, helper SELLs the full held position.
- If a recorded PR3 rescue execution error has occurred, and a full held-side SELL is executable, helper SELLs the full held position.
- The SELL must be full-size. No partial-fill success is invented.
- If full SELL is not evidenced, the helper waits for a later evidenced state. It does not fabricate a fill.

A sensitivity grid may additionally require a minimum direct-SELL loss before helper activation, but this is an explicit parameter and is never silently changed.

## Evidence classes
Every changed action retains the source evidence label:
- `COUNTERFACTUAL_FROM_RECORDED_STATE`
- `SHADOW_APPROXIMATE`

Historical untouched paths remain `EXACT`.

No missing delayed state may be converted into a meteor or a fill.

## Latency audit
The primary historical replay acts only on an actually observed state. It does NOT invent 25–500 ms states.

For latency checks at 25/50/100/150/250/500 ms:
- target = decision timestamp + requested latency
- a delayed action is considered observed only if a full executable SELL state exists at or after target within **25 ms**
- otherwise that delayed action is `UNKNOWN`
- if any helper action is UNKNOWN at a latency, full-policy delayed PnL is reported `UNKNOWN`
- UNKNOWN is never counted as a meteor

## Primary GO / NO-GO gate before a 24h forward shadow soak
A policy must satisfy all of:
- known full-sample PnL > -$5, preferably >= $0
- 0 known meteors
- 0 new meteors
- worst known trade > -$1
- original NORMAL_HEDGE retained >= 90/92
- TRAIN and TEST both improve versus historical
- delayed-state UNKNOWNs must not be used to claim latency robustness

If no policy passes, verdict = `NO-GO`.

## Failure behavior
Any baseline mismatch, exception, timeout, missing authority file, or incomplete output means:
`TEST INCOMPLETE`
No numeric conclusion may be produced from a failed run.

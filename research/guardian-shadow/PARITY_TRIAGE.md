# PR88 parity triage — 2026-10-03

BASE: `8f0abacc936e6195f10f418867475a99fc8049ca`.
Reviewed HEAD: `d25fb4f738f4ce3f00e9c6a56973b4997c636011`.
The review correction changes only Guardian's global execution-error exemption;
the parity fixture has no execution errors and never admits Guardian.

## Method

Run the exact existing PARITY test 30 times per revision, alternating BASE/HEAD
order between pairs, in separate archived checkouts with the same dependencies.
Both use Node 26.7.0, Vitest 4.1.11 and the following identical command:

```powershell
node ../Polymarket-Engine/node_modules/vitest/vitest.mjs run tests/pr3-premerge-correctness.test.ts -t 'PARITY: same state yields equal canonical RESCUE decision' --config parity-triage.config.ts
```

Worker configuration is unchanged/default: no worker-count, pool, sequencing or
timeout override. Both setup/config files are identical. Test working-file
SHA256: `8fd16a6b7c979a0d0c7efcd539603495235b22af45e4d20e5d81347e0bcbae50`.
Instrumentation SHA256:
`b7aa3a74989f523e462e999740b52ac19ae9e297899f8294c8dc28091ee0360c`.

Separate measurement setup wraps synchronous runtime recording and clock reads,
buffers evidence, and snapshots journal events/durable transitions after the
test. It does not await/log to disk during execution or change authority,
liquidity, assertions, timeouts or delays. Failed engines are stopped only after
trace/timing capture. Measurement has some instrumentation overhead, identical
on both revisions; rates describe this controlled sample, not an exact
unobserved production probability. Preliminary selector probes/skipped runs
were discarded and are excluded from the 60 measurements.

Each measured run retains stdout, elapsed command/test wall time, both engines'
PAPER_ORDER_CREATED intent/order IDs, RISK_REDUCTION_ADMITTED/FAILED ownership,
PAPER_FILL, phase/clock changes, position/owner at deadline advancement, deadline
fallback selection, and terminal ledger state. Raw traces, setup/config,
CSV/index and analysis script are included in the handoff evidence archive.

## Results and demonstrated cause

| Revision | Pass | Timeout | Observed timeout rate | Median successful command | Median failed command | Median successful test | Median failed test |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| BASE | 16 | 14 | 46.7% | 4065 ms | 10096 ms | 177.28 ms | 6177.96 ms |
| Reviewed HEAD | 19 | 11 | 36.7% | 4066 ms | 10110 ms | 176.67 ms | 6180.36 ms |

All **25 failures** have the same captured mechanism on BASE and HEAD:

1. Both instances receive pre-deadline books at age 20s. Held UP bid $0.70,
   opposite DOWN ask $0.30, quantity 5 and zero fees make PR3 admissible.
2. The early instance fills RESCUE. The deadline instance's own EARLY PR3 order
   is admitted but still pending when the clock advances; it has not paired.
3. The existing `EARLY_CATASTROPHIC_DEADLINE_OWNERSHIP` fill fence rejects that
   pending order and durably releases it. This is a lifecycle release, not an
   actual execution error that should authorize Guardian.
4. Existing deadline fallback computes both guaranteed salvages as $3.50:
   SELL = 5 × $0.70; opposite BUY = 5 − 5 × $0.30. Its existing
   `A_PREFERRED_TIE` rule selects `PROTECTIVE_DE_RISK` SELL.
5. That full SELL really fills and the position is removed: residual zero.
   The unchanged test waits for a RESCUE fill instead, then hits its 6000ms
   wait timeout. Guardian created no order and consumed no residual.

All **35 passes** also have the same mechanism: the deadline instance already
completed its EARLY PR3 BUY and had paired quantity 5 / residual zero before
clock advancement. No deadline fallback runs. Consequently a passing execution
does not prove the test's claimed comparison with PR82 B fallback.

This demonstrates pre-existing scheduling sensitivity and an incorrect assumed
deadline action in the test, rather than a Guardian policy failure. HEAD's
observed timeout rate is 10 percentage points lower; the finite sample does not
prove population-rate equivalence or rule out every timing difference. There is
no evidence here of a PR88 increase, changed tie semantics, missed protection,
duplicate fill or remaining exposure. No production change to the deadline
tie rule or test modification is justified to force this RESCUE assertion.
The separate deterministic Guardian deadline-handoff test verifies the correct
SELL behavior. The existing PARITY test remains unchanged, including its
liquidity, assertions and timeout; its redesign requires separate review.

## Every measured run

Order IDs and admissions/failures/fills for every row are in
[PARITY_TRIAGE_RUNS.csv](PARITY_TRIAGE_RUNS.csv); full phase evidence is also
in the handoff trace/index archive. `NONE` below means the instance had already
paired before advancement, not that protection was omitted.

| Run | Result | Wall ms | Test ms | PR3 complete before advance | Deadline fallback |
| --- | --- | ---: | ---: | --- | --- |
| base-01 | FAIL | 10095 | 6180.85 | False | PROTECTIVE_DE_RISK |
| head-01 | PASS | 4062 | 169.43 | True | NONE (already paired) |
| head-02 | PASS | 4021 | 170.25 | True | NONE (already paired) |
| base-02 | PASS | 4016 | 163.09 | True | NONE (already paired) |
| base-03 | FAIL | 10144 | 6167.94 | False | PROTECTIVE_DE_RISK |
| head-03 | PASS | 4077 | 188.51 | True | NONE (already paired) |
| head-04 | PASS | 4078 | 174.11 | True | NONE (already paired) |
| base-04 | PASS | 4067 | 180.07 | True | NONE (already paired) |
| base-05 | PASS | 4038 | 183.74 | True | NONE (already paired) |
| head-05 | PASS | 4114 | 179.88 | True | NONE (already paired) |
| head-06 | FAIL | 10070 | 6179.87 | False | PROTECTIVE_DE_RISK |
| base-06 | FAIL | 10134 | 6179.37 | False | PROTECTIVE_DE_RISK |
| base-07 | FAIL | 10029 | 6178.87 | False | PROTECTIVE_DE_RISK |
| head-07 | PASS | 4168 | 166.24 | True | NONE (already paired) |
| head-08 | FAIL | 10074 | 6167.79 | False | PROTECTIVE_DE_RISK |
| base-08 | FAIL | 10098 | 6173.03 | False | PROTECTIVE_DE_RISK |
| base-09 | PASS | 4211 | 176.78 | True | NONE (already paired) |
| head-09 | PASS | 4050 | 176.67 | True | NONE (already paired) |
| head-10 | PASS | 4131 | 181.71 | True | NONE (already paired) |
| base-10 | PASS | 4063 | 179.38 | True | NONE (already paired) |
| base-11 | PASS | 4088 | 177.16 | True | NONE (already paired) |
| head-11 | PASS | 4036 | 180.52 | True | NONE (already paired) |
| head-12 | PASS | 4020 | 169.67 | True | NONE (already paired) |
| base-12 | FAIL | 10088 | 6166.65 | False | PROTECTIVE_DE_RISK |
| base-13 | PASS | 4099 | 180.09 | True | NONE (already paired) |
| head-13 | FAIL | 10110 | 6171.6 | False | PROTECTIVE_DE_RISK |
| head-14 | PASS | 4066 | 178.21 | True | NONE (already paired) |
| base-14 | PASS | 4063 | 190.27 | True | NONE (already paired) |
| base-15 | FAIL | 10153 | 6196.01 | False | PROTECTIVE_DE_RISK |
| head-15 | PASS | 4057 | 168.6 | True | NONE (already paired) |
| head-16 | PASS | 4122 | 175.53 | True | NONE (already paired) |
| base-16 | FAIL | 10079 | 6161.46 | False | PROTECTIVE_DE_RISK |
| base-17 | FAIL | 10097 | 6195.79 | False | PROTECTIVE_DE_RISK |
| head-17 | PASS | 4094 | 184.78 | True | NONE (already paired) |
| head-18 | FAIL | 10161 | 6180.36 | False | PROTECTIVE_DE_RISK |
| base-18 | PASS | 4017 | 182.44 | True | NONE (already paired) |
| base-19 | PASS | 4141 | 188.47 | True | NONE (already paired) |
| head-19 | FAIL | 10156 | 6180.97 | False | PROTECTIVE_DE_RISK |
| head-20 | FAIL | 10065 | 6162.13 | False | PROTECTIVE_DE_RISK |
| base-20 | FAIL | 10124 | 6190.43 | False | PROTECTIVE_DE_RISK |
| base-21 | PASS | 4235 | 173.83 | True | NONE (already paired) |
| head-21 | FAIL | 10112 | 6175.13 | False | PROTECTIVE_DE_RISK |
| head-22 | PASS | 4048 | 159.32 | True | NONE (already paired) |
| base-22 | PASS | 4015 | 176.0 | True | NONE (already paired) |
| base-23 | PASS | 4034 | 161.95 | True | NONE (already paired) |
| head-23 | FAIL | 10059 | 6184.12 | False | PROTECTIVE_DE_RISK |
| head-24 | PASS | 4081 | 171.68 | True | NONE (already paired) |
| base-24 | FAIL | 10146 | 6196.45 | False | PROTECTIVE_DE_RISK |
| base-25 | FAIL | 10060 | 6176.61 | False | PROTECTIVE_DE_RISK |
| head-25 | PASS | 4292 | 180.24 | True | NONE (already paired) |
| head-26 | FAIL | 10088 | 6189.71 | False | PROTECTIVE_DE_RISK |
| base-26 | FAIL | 10063 | 6177.05 | False | PROTECTIVE_DE_RISK |
| base-27 | PASS | 4031 | 165.8 | True | NONE (already paired) |
| head-27 | PASS | 4020 | 183.0 | True | NONE (already paired) |
| head-28 | FAIL | 10118 | 6191.13 | False | PROTECTIVE_DE_RISK |
| base-28 | PASS | 4096 | 177.4 | True | NONE (already paired) |
| base-29 | PASS | 4266 | 175.57 | True | NONE (already paired) |
| head-29 | FAIL | 10282 | 6201.35 | False | PROTECTIVE_DE_RISK |
| head-30 | PASS | 4029 | 177.61 | True | NONE (already paired) |
| base-30 | FAIL | 10073 | 6169.13 | False | PROTECTIVE_DE_RISK |

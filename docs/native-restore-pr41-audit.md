# Native Restore: PR41 Keep/Drop Audit

Restore baseline: `0a148e2c7416dc5afc5700bb90c261d5d13902bc`

Source PR: #41, the first Astra PR.

Goal: recover the pre-Astra `NATIVE_DETERMINISTIC_V1` product while retaining only fixes that are independently valid for the native architecture. No Astra/OpenAI code is allowed to become economic authority again.

## KEEP — independently valid native fixes

### 1. Running archive source-window correctness

Files:
- `apps/engine/src/archive.ts`
- regression test extracted into `tests/native-restore-pr41-regressions.test.ts`

Keep behavior:
- derive the historical experiment lifecycle before an archive scan
- use the durable `startedAt` / `completedAt` bounds
- a RUNNING archive is `interim`, `rolling`, `complete=false`, `truncated=true`

Reason: without the durable experiment start boundary, a running archive can be empty or mis-scoped. This is unrelated to Astra.

### 2. Strict time-aware first-leg admission

Files:
- `packages/core/src/experiment.ts`
- regression test extracted into `tests/native-restore-pr41-regressions.test.ts`

Keep behavior:
- `RUNNING` alone is not sufficient
- `startValidationPending` must be false
- `entryClosesAt` must exist and parse
- `now < entryClosesAt` is required

Reason: the experiment deadline is an economic boundary for native trades too.

### 3. Native close-vs-entry race fence — KEEP, extract manually

Source hunks are mixed into `apps/engine/src/engine.ts` and must not be cherry-picked wholesale.

Keep only these semantics:
- serialize native first-leg admission against experiment close/status transition using the existing `experimentBoundaryGate`
- re-check the entry window immediately before `PAPER_ORDER_CREATED`
- if the deadline has elapsed, durably move RUNNING to ENTRY_CLOSED before returning
- serialize maintenance-driven RUNNING -> ENTRY_CLOSED / COMPLETED transition with the same boundary gate

Do not carry any Astra admission, provider, model, budget, memory, correlation, or learning code while extracting this fence.

### 4. Polymarket close code 1013 diagnostics — KEEP OPTIONAL

Files:
- `packages/market-data/src/bounded-clob-market.ts`
- `tests/polymarket-clob-close-diagnostic.test.ts`
- optional metric-only pieces from `apps/engine/src/metrics.ts` / `engine.ts`

Keep behavior:
- classify WebSocket close code 1013 as `slow_consumer`

Reason: diagnostic-only and independent from decision authority. It does not change economic behavior.

## DROP — Astra-specific or policy changes not justified by the native baseline

### Astra implementation and dependencies

Drop entirely:
- `apps/engine/src/astra/client.ts`
- `apps/engine/src/astra/service.ts`
- `docs/astra-brain-v1.md`
- `packages/core/src/astra-feature-engine.ts`
- `packages/core/src/astra-gate.ts`
- `packages/core/src/astra-learner.ts`
- `packages/core/src/astra-learning.ts`
- `packages/core/src/astra-memory.ts`
- `packages/core/src/astra.ts`
- OpenAI dependency / Astra smoke command / package-lock changes caused by Astra

### Astra authority plumbing

Drop Astra-specific changes from:
- `apps/engine/src/runtime.ts`
- `apps/engine/src/preflight.ts`
- `apps/engine/src/recovery.ts`
- `apps/engine/src/report.ts`
- `apps/engine/src/journal-writer.ts`
- `packages/core/src/domain.ts`
- `packages/core/src/index.ts`
- `packages/core/src/journal-types.ts`
- `packages/core/src/retention.ts`

Native authority remains `NATIVE_DETERMINISTIC_V1`.

### Separate first-leg size cap

Do not carry by default:
- `maxFirstLegOrderSizeUsd=12.5`
- `MAX_FIRST_LEG_ORDER_SIZE`
- strategy/risk plumbing for that cap

Affected PR41 files:
- `packages/core/src/config.ts`
- `packages/core/src/domain.ts`
- `packages/core/src/risk.ts`
- `packages/core/src/strategy.ts`
- related `apps/engine/src/engine.ts` hunk

Reason: this is a risk-policy change introduced with Astra, not a correction required to restore native correctness. Re-evaluate separately with evidence if desired.

## MIXED FILES — never cherry-pick whole-file PR41 versions

The following files contain both Astra work and independent native fixes. They require manual extraction:
- `apps/engine/src/engine.ts`
- `apps/engine/src/metrics.ts`
- `packages/core/src/experiment.ts`

`packages/core/src/experiment.ts` has already been extracted narrowly: only the strict first-leg deadline helper was kept; Astra report wording was not carried.

## Test handling

Drop Astra suites as restore dependencies:
- `tests/astra-brain-v1.test.ts`
- `tests/astra-final-correctness.test.ts`

But extract independent regression concepts into native tests:
- strict first-leg deadline
- no first leg without `entryClosesAt`
- running archive uses durable start and is non-empty interim output
- close-vs-entry race cannot create a paper order after deadline
- WS 1013 classification if diagnostic is retained

Keep the existing native test suite intact.

## Storage / retention note

PR41 did not invent retention. Pre-Astra already had retention/rollup code, but all three operational flags defaulted false. Do not mix the later PR45 production gate into the PR41 restore.

First clean-DB native canary policy:
- `RETENTION_ENABLED=true`
- `RETENTION_ANALYTICS_ROLLUP_ENABLED=false`
- `RETENTION_COMPACTION_ENABLED=false`
- external DB-size/read-amplification watchdog active

Long-duration analytics rollup/compaction is a later, separately proven subsystem.

## Current extraction status

Applied on `restore/native-baseline`:
- [x] running archive source-window/interim fix
- [x] strict time-aware first-leg helper
- [x] dedicated native regression tests for both
- [ ] native close-vs-entry race fence in EngineService
- [ ] optional WS 1013 diagnostic
- [ ] PR53 beginExperiment residue gate
- [ ] PR50 market-data optimization
- [ ] current dashboard visual layer / native selector adaptation
- [ ] storage watchdog

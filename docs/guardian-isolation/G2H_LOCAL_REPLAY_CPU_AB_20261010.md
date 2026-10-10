# G2H | Local CPU A/B, Only Necessary Economic Clone

Date: 2026-10-10. Parent: `bc557de`.
Gate: **G2H IN PROGRESS / NOT ACCEPTED**. Offline synthetic only.

## Measured symptom and minimal source fix

Real source code in `advanceGuardianOfflineSource` previously called
`cloneRecoverableState(current.recovery)` **twice** per authoritative
row: once for the previous frame state, and once for the next state.
Only `next` receives `applyDurableTransition`; `previous` is
read for the already committed position episode and settlement existence.
Copying `previous` was unnecessary. This atom uses a readonly previous
reference and retains the deep copy of `next`, fail-closed state
validation, operation-key set cloning, and frame/ledger parity proof.

## Reproducible local synthetic workload

Test `G2H-LOAD-01`:
`PM_FORENSIC_STAGE=08_current`, optionally
`PM_G2H_STRESS=1`, Vitest `tests/guardian-readonly-session.test.ts`
with `-t G2H-LOAD-01`. In memory, one paper market with a canonical
checkpoint followed by real durable MTM reducer rows generated every
10ms *in payload timestamps only*. All rows are delivered and
cryptographically certified as fast as possible. No real-time pacing,
PostgreSQL, market WebSocket, wallet, shadow, or VPS.

| Measurement | Before single-clone (8,192 rows) | After single-clone (8,192 rows) |
|---|---:|---:|
| End-to-end local replay wall time | 2,081.5 ms | 1,730.2 ms |
| Process CPU time during replay | 2,142 ms | 1,750 ms |
| Replay throughput, rows/s | 3,935.6 | 4,734.7 |
| p95 of 16 poll/page calls, 512 rows each | 381.2 ms | 377.5 ms |
| Test status and cursor | PASS, 8,212 | PASS, 8,212 |

The wall/CPU differences are one-run **observations**, not a proven
production speed-up. For the usual 1,000 rows at 64/page, observations
were 136.3 ms before and 122.6 ms after; these runs had different
process contention and CPU time, so extrapolation is unsafe.
`process.memoryUsage().heapUsed` delta can be negative after GC,
thus it is explicitly NOT a measured peak memory ceiling.

## Remaining bottleneck and safety gates

The high per-page tail is not solved. `seenOperationKeys` still clones
a growing Set at every row; this is candidate O(N^2) allocation work.
Before replacing it, validate sampling and persistent immutable
set alternatives against replay/idempotency correctness and
actual 40-market throughput. No lossy key eviction is permitted.
This local benchmark cannot prove actual 8/8 market reconnect, real
p95/p99 source lag, PostgreSQL lock load or Engine CPU impact.

One new regression test `G2H-14` checks old economic state,
operation-key receipts and maker/fill episode parity remain unchanged.
Focused **67/67 PASS** across four files, Guardian-source TypeScript PASS.

NO SHADOW / PAPER ONLY / NO VPS / NO PUSH / NO DEPLOY / NO MERGE.

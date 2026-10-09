# Reverse Regression Round 4 | 2026-10-09

## Authorized scope
Reverse removal from current engine to Guardian65, no production rollback or edits. Production remained untouched. Test results are not a claim that the original 7+ reference failure is fixed.

## Eight-stage source reversal
Order: current -> CPU V3 -> CPU V2 -> CPU V1 -> identity -> Step3 -> Step2 -> Guardian65.
All eight source snapshots built successfully, passed the dedicated engine-main TypeScript check, and passed the same 47 tests: 376 / 376 PASS.

## Active PAPER Guardian synthetic fixed-rate input
Each stage used 100 book snapshots/sec with one open PAPER position and the same test harness. All completed 200/200 events.

| Stage | Event-loop p99 ms | Callback p99 ms |
|---|---:|---:|
| Current | 16.88 | 5.177 |
| CPU V3 | 16.45 | 5.131 |
| CPU V2 | 16.49 | 4.984 |
| CPU V1 | 16.66 | 4.857 |
| Identity | 16.24 | 5.926 |
| Step3 | 16.52 | 4.685 |
| Step2 | 16.64 | 3.991 |
| Guardian65 | 27.71 | 13.447 |

These are Windows synthetic measurements and do not prove a VPS root cause.

## Last-change packet single-file ablation
Current vs CPU V3 differed in 6 project source files:
- apps/engine/src/engine.ts: standalone revert FAILS typecheck (old source-seal import and type dependency).
- apps/engine/src/final-report.ts: standalone revert FAILS typecheck (old source-seal import).
- apps/engine/src/report-source-integrity.ts: standalone revert FAILS typecheck (missing current export and source-seal import).
- apps/engine/src/runtime-supervisor.ts: standalone revert compiles and tests PASS.
- apps/engine/src/source-seal.ts: standalone revert compiles and tests PASS.
- packages/core/src/retention.ts: standalone revert compiles and tests PASS.

Coherent group rollback (final-report + source-seal) PASS; full six-file previous-version group PASS. Mixed groups between these cases failed compile because of inter-file API dependencies. This does NOT demonstrate a runtime fault.

## Real VPS isolated PAPER image walk
Identical config: one engine test container at a time, 40 discovered markets, CPU1 affinity, soft scheduler shares 128, no hard CPU quota, 512MiB RAM, memory journal, private Docker network and no production credentials.

| Image | Result | Wall-clock seconds | Reference error count |
|---|---|---:|---:|
| current | INCONCLUSIVE_NO_TARGET | 101.87 | 3 |
| cpu3 | INCONCLUSIVE_NO_TARGET | 101.49 | 0 |
| cpu2 | INCONCLUSIVE_NO_TARGET | 101.11 | 0 |
| cpu1 | HOST_LOAD_GUARD | 90.87 | 2 |
| identity | HOST_LOAD_GUARD | 15.15 | 0 |
| step3 | HOST_LOAD_GUARD | 15.17 | 0 |
| step2 | HOST_LOAD_GUARD | 15.21 | 0 |
| safe | CONTAINER_EXITED (operator stopped) | 19.71 | 0 |

Only the first three VPS stages yielded comparable full-duration observation windows; none had the target mass reference close. The host-load guard invalidated later comparisons. Never label a non-trigger as a proven PASS.

## Forensic code observation
packages/market-data/src/reference-feed.ts source is identical across all eight compiled-source snapshots (SHA-256 prefix c7b76f36). Step2 changed packages/market-data/src/polymarket-sharded.ts from global callback ordering to per-shard callback tails and ingress-bounds.ts maxEvents from 4096 to 6144. These are factual differences, NOT established causes.

## Safety fix
Earlier runner wrongly returned process exit 0 after HOST_LOAD_GUARD, allowing the parent walk to proceed. Detected and STOPPED the Windows orchestrator and remaining forensic container. The VPS runner and Windows walk now treat HOST_LOAD_GUARD and CONTAINER_EXITED as hard stops. Added 2 unit tests. Total Python observer/runner tests: 18/18 PASS.

## Production safety
Final read-only check: only polymarket-engine-no-seal and polymarket-postgres running; database UP, polymarketWs CONNECTED, 40 qualified markets, overloadState FAIL_CLOSED unchanged. No production reset, restart, deployment, or database write.

## Honest conclusion
NO code regression root cause isolated. None of the reverse-image tests reliably reproduced the original synchronized failure under realistic CPU limits. A different workload/capture plan cannot be substituted without discussing it with the user, per the locked workflow rule.

## Evidence
Local ignored records: research/reverse_regression/runs. Private raw logs: research/reverse_regression/evidence/private and /home/ubuntu/pm-forensic-20261009/pmf-reg-*.json. Reusable orchestrators: run_reverse_stage_tests.py, run_reverse_pace.py, run_single_file_ablations.py, run_coherent_ablations.py, run_vps_reverse_walk.py.

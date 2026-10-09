# PolyMarket reverse regression | LOCAL ONLY

## Explicit safety boundary
No rollback, production DB writes, production service changes, trading-mode changes,
VPS instrumentation or running test are approved. This preparation uses a persistent Windows
Desktop git worktree. Stage maps were copied READ-ONLY from preexisting image source maps.

Git base: 21a195a6809e6d7eaf8b691dba65cf8ce3edda6f
Safe compiled image: sha256:1206d8352a36a361bb07451904225dc4c44c6501243b67a182e2bb9ccea589c5
Current compiled image: sha256:dbd6ee9e3e4af56575bd39de9b9572e99d8058f7525120fa23dbce15a00d5cd3

NOTE: Git base and compiled safe image are not interchangeable. Nine actual
image source maps are locally preserved in maps/ and NOT committed/pushed.

## Stage sequence
python research/reverse_regression/stage.py --list
python research/reverse_regression/stage.py --stage 08_current --verify
python research/reverse_regression/stage.py --stage 07_cpu_v3 --apply
python research/reverse_regression/stage.py --stage 07_cpu_v3 --verify

Default walk:
08_current -> 07_cpu_v3 -> 06_cpu_v2 -> 05_cpu_v1 ->
03_identity -> 02_step3 -> 01_step2 -> 00_safe

SKIP 04_mapopt by default. It briefly appeared, was rejected, and was later
removed. Restoring it while 'reversing' later work would confound isolation.

Each snapshot reconstitutes 74-75 image-included project source files, changing
only that known source set. It does not alter DB, configuration or tests.

## Failure-trigger observer
Offline regression proof, no engine launched:
cd research/reverse_regression
python -m unittest -v test_fault_watch
python fault_watch.py --fixture fixtures/reference_mass.jsonl --out runs/smoke

Conditions:
- >=7 distinct reference closes within 1s, abort=false;
- CLOB stale within 20ms of that mass close, if received before stopping;
- PostgreSQL journal CONNECT_TIMEOUT;
- a critically logged FAIL_CLOSED.

Output: incident.json and summary.txt, plus bounded preceding 120s event excerpts.
Exit code 10: fault detected. Exit code 2: no target fault, INCONCLUSIVE.
NO FAILURE does not imply regression fixed. Synthetic detector tests do not prove
real market/WebSocket/DB fault reproduction.

Live tests require independent PAPER mode, isolated DB/volumes, no wallet,
explicit run command and CPU/RAM/time limits; user approval is required before
starting. Example syntax after all gates:
PM_FORENSIC_ENV=isolated-paper
python fault_watch.py --confirm-isolated-paper --max-seconds 7200 -- <LOCAL_PAPER_COMMAND>

## Experimental gates
1. Reproduce the specific fault in current stage under controlled conditions.
2. Change one minimal independent component, retain identical input/seed/rate.
3. If fault vanishes, reintroduce the removed component and prove it returns.
4. Test Guardian ownership, async ordering, durable idempotency, fail-closed and
   shutdown/recovery semantics at each stage.
5. If historical real feed raw messages are missing, do not assert that a clean
   run proves the bug fixed. Capture new input only with permission.

Not to be included in PR: maps/*.json (10MB each), private logs, DB exports,
tokens or runtime run output. Current branch remains local, not pushed.


## VPS real-market tests (2026-10-09 update)

Result summary: `evidence/ROUND_3_VPS_REAL_FEED_20261009.md`.
The real VPS regression experiments produced a crucial test-harness constraint:
**0.30 vCPU hard quota triggered 7+/8 synchronous reference closures in BOTH current
and Guardian65 image**. These deliberately CPU-starved staging incidents are NOT
evidence of a post-Guardian65 code regression. Under CPU1 pin + low relative
scheduler weight (no hard quota), current ran about 241s with only 3 individual
reference errors and no 7+ synchronized cluster; this remains INCONCLUSIVE,
not a proven fix.

Production's pre-existing critical `PERSISTENCE_OUTCOME_UNKNOWN` / Postgres
`CONNECT_TIMEOUT` was logged at 2026-10-09 07:30:31 UTC, roughly 11 hours
BEFORE the isolated VPS tests. `overloadState=FAIL_CLOSED` must not be
cleared/restarted by this forensic work.

VPS-only bounded runner copied to:
`/home/ubuntu/pm-forensic-20261009/vps_reverse_runner.py`

Commands (dry-run by default; no production DB):
```bash
python3 /home/ubuntu/pm-forensic-20261009/vps_reverse_runner.py --stage current --seconds 240
# Deliberate PAPER-only launch:
python3 /home/ubuntu/pm-forensic-20261009/vps_reverse_runner.py --stage current --seconds 240 --execute-isolated-paper
```

For stage comparisons, choose `safe`, `step2`, `step3`, `identity`,
`cpu1`, `cpu2`, `cpu3`, or `current`. Only attempt A/B when
the target fault reproduces with realistic CPU policy and comparable input.
The runner uses `--cpuset-cpus 1 --cpu-shares 128`, no hard CPU quota,
private forensic Docker network, fixed 512MiB RAM, `JOURNAL_MODE=memory`,
PAPER-only authority, and no production mounts or host ports.
It stops the forensic container on 7+ reference mass-close, host overload,
explicit timeout or unexpected exit, and writes its evidence file.

Tests:
```bash
python3 -m unittest -q test_fault_watch test_vps_reverse_runner
```
16 tests passed on 2026-10-09. Full real production equivalence is NOT
achieved until isolated PostgreSQL, active PAPER Guardian state and matching
traffic are included. The staging PostgreSQL container remains STOPPED.

## Recorded public WebSocket replay (Round 5, 2026-10-09)

Approval scope: same captured public messages fed into eight historical engine
source snapshots. NO production modifications. Real captured WS frames are stored
in fixtures/public_market_capture_four_markets_20261009.jsonl and are public
market data, not private conversation/production data.

Dataset SHA256:
7ea27f0c585d2da70ee7f9a64b2203c7b5df3e741b905d1faf4818766466ed19

- Eight Binance reference assets; four Polymarket market pairs, eight tokens.
- 231 reference and 63 CLOB wire frames; the CLOB adapter emits 132 snapshots.
- Replay is deterministic about input bytes, ordering, and relative capture
  offsets. Scheduling latency may still vary between independent OS runs.
- Test waits for all bounded input queues to drain before evaluating.
- One callback lane on Guardian65, four simultaneous lanes after Step2.
- The capture had NO synchronized 7/8 reference close. Replay did not isolate
  the historical production incident; no causal claim is justified.
- The test covers feed adapters, not full Guardian/durable PostgreSQL.

Run from the repository root (Windows PowerShell):
    python research/reverse_regression/run_public_replay.py

The runner opts in to the long test, swaps compiled-source stages one by one,
and restores 08_current even on failure. Test outputs remain ignored under
runs/public_capture_replay. A normal Vitest run skips the 20-second replay.

Evidence summary:
    evidence/ROUND_5_PUBLIC_RECORDED_REPLAY_20261009.md

Captured with capture_public_market.mjs on the VPS in a temporary isolated
unprivileged container; capture terminates by a strict time/size/rows bound.
New capture may be substituted ONLY after recording and updating the fixture
digest and clearly distinguishing it from the original historical incident.

# G2H H4 | Archive-shaped durable rows and two-CPU local capacity atom

Date: 2026-10-11 | Baseline `e90bd5e` |
Requirement: frozen H-05 under `G2H_REQUIREMENTS_FROZEN_20261011.md`.
**Status: H-05 PARTIAL; G2H NOT DONE. No live source or production test.**

## Source provenance and shape, independently verified
Two provided local soak tarballs were inspected read-only. Their SHA-256s
match their supplied manifests. Only the JSONL aggregate type and UTF-8
row byte length were used, never wallet identifiers, orders or secrets.

Combined `durable_transitions.jsonl` (1,779 rows):
- `INFRASTRUCTURE_COST_ACCRUED`: 1,051 / 59.08%;
- `PAPER_FILL`: 456 / 25.63%;
- `POSITION_SETTLED`: 199 / 11.19%;
- `RISK_REDUCTION_ADMITTED`: 64 / 3.60%;
- other lifecycle/failure: 9 / 0.51%.
- Actual JSONL aggregate sizes: p50 1,100 B; p95 5,401 B;
  p99 7,483 B; max 8,311 B.

## Harness change

Extended `tests/guardian-h4-local-capacity.test.ts` with two
explicitly labeled offline workloads:

**H4-SHAPE-PG:** 20 durable rows/s target, two paced repeats of
100 rows each. 59% infrastructure-size, 26% fill-size, 11%
settlement-size and 4% admission-size synthetic **envelopes**.
The fill tail includes 5.4k, 7.48k and 8.31k target rows. This
fixture uses real `PostgresEventJournal.commitDurableTransition`,
real single global DB sequence, SELECT-only RLS reader,
`GuardianTrustedPgSession` and fsynced v2 ACK.

**CRITICAL limitation:** for safety, test events still have
`LEDGER_MARK_TO_MARKET` semantics and an opaque bench-only pad,
not actual `PAPER_FILL` / ownership economic state mutations.
The bytes and frequency *distribution* approximate exports, NOT
the real state-transition mix. Semantic-equivalence/replay oracle
with complex open positions remains a mandatory H-02/H-05 task;
never claim this measured full paper-trade correctness.

Postgres itself was verified `NanoCpus=2, Memory=1572864000 bytes`
using `docker inspect` (disposable PG 17, localhost 55482).
PowerShell parent affinity was set to two logical processors
(mask 3); the Node/Vitest worker reported
`os.availableParallelism()=2`. Physical processor is still
a different laptop-generation CPU than the 2 vCPU Haswell VPS,
and Windows Docker virtualized host differs from Linux.
No actual VPS resource or production DB was touched.

PostgreSQL SQL `octet_length(row_to_json(t)::text)` after
the final 200-row shaped case verified:
- generated p50 **1,109 B**; actual archive p50 **1,100 B**;
- generated p95 **5,426 B**; archive p95 **5,401 B**;
- generated p99 **7,516 B**; archive p99 **7,483 B**;
- generated max **8,336 B**; archive max **8,311 B**.
This direct SQL measurement is preferable to a JS payload-only
estimate, because hashes, sequence IDs and timestamps add bytes.

Standalone **H4-DEPTH** benchmark uses 40 markets across 4
bounded scripted transport shards. For every market both UP and
DOWN full books include 32 sorted bid and 32 sorted ask levels
(64 levels per token). 80 full books/s aggregate, two 4-second
repetitions. Each tick is accepted only after the **NEW** source
timestamp for both sides has been processed. 40/40 READY required,
with 0 ingress gaps. The level-32 fixture is an *engineering depth
scenario*, not a claimed historical venue depth distribution.

## Accepted independent 2-CPU runs, plus final full-suite confirmation

- Independent shaped-run #1/#2: 19.02 / 19.21 rows/s, ACK p99
  156.36 / 157.85ms, combined Node CPU 766 / 625ms.
- Full-suite shaped-run #1/#2: **18.97 / 19.25 rows/s**, ACK p99
  **157.46 / 151.56ms**; committed cursor reached 501.
  Derived full-row size distribution rechecked:
  p50 1,109 B; p95 5,426 B; p99 7,516 B; max 8,336 B.
- Independent 32-level books #1/#2: 79.72 / 79.91 books/s,
  all 40 READY, source-age p99 73 / 54ms, 0 gaps.
- Full-suite 32-level books #1/#2: **79.93 / 79.78 books/s**,
  source-age p99 **64 / 63ms**, settle/read p99
  **62.14 / 61.1ms**, 40/40 READY and 0 gaps.
- Full-suite original small-event 5/s x2 and 20/s x2
  were also completed in the same 2-logical-CPU affinity envelope.
  All scoped row sessions reached persisted ACK with no sampled backlog.

Combined Node CPU includes Vitest, writer, reader and transport
fixture overhead, not pure Guardian CPU. A one-time post-run
Docker metric (~0.09% CPU, 69MiB RSS) is NOT peak or p95 PG
utilization. Sampling heap deltas do not prove long-run stable
memory, and zero queue-high-water under scripted injection does
not prove actual bounded network overflow behavior.

## Remaining nondeferable H-05 blockers

1. Truly semantic representative PAPER_FILL/settlement/ownership
   replay with production-like checkpoint evolution and independent
   state oracle, not just byte-matched marks.
2. Multi-hour workload and repeat restarts with heap trend,
   PostgreSQL CPU p95/p99, disk and queue peak/overflow traces.
3. Compare against genuinely target-equivalent CPU resources and
   agreed explicit book-age/cursor-lag limits; laptop two-logical
   processor affinity and Docker quota are necessary test controls,
   not a substitute for VPS performance certainty.
4. Unknown real full-book depth/rate distribution and four-shard
   outage frequency; archival durable JSONL is not a book feed.
5. H-02/H-03 pinned-checkpoint retention, operator reconciliation
   after power loss, and eventual single economic authority remain.

Read-only test behavior and boundedness remain. Optional follow-on
perf automation is tracked in the separate
`G2H_DEFERRED_IMPROVEMENTS_BACKLOG.md`. No architecture rewrite
or external system action was justified by these results.

## Final local test evidence
Final full four-test capped H4 run: **4/4 PASS**, one test file,
with `os.availableParallelism()=2` and the independent PostgreSQL
cgroup quota of 2 CPU / 1.5 GB.
Separate FINAL scoped Guardian regression, using a DISTINCT
isolated localhost PostgreSQL 17 on port 55481: **364/364 PASS
across 27 files**. The tests were run after H4 changes, with
`PM_FORENSIC_STAGE=08_current`.
`@pm/db`, Guardian source, forensic Engine and `@pm/core`
TypeScript checks all PASS; `git diff --check` PASS.
These are separate sequential test runs, not proof of measured
combined production traffic. The full Engine typecheck still
has its separately recorded `source-seal.js` issue outside
this scoped gate; no unrelated production code was edited.
G2H must remain NOT DONE until required gates pass.

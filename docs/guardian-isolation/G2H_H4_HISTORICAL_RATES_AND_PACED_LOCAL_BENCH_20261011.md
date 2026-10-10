# G2H H4 | Verified historical durable rates and measured paced local benchmark

Date: 2026-10-11 | base `662e303` | **H4 PARTIAL, G2H NOT ACCEPTED**.
Authority: `G2H_REQUIREMENTS_FROZEN_20261011.md`.
No VPS, production DB, real external WS, order, shadow, wallet, deploy,
push, merge, or H2 target-filesystem mutation.

## 1. INPUT DATA PROVENANCE: two attached real soak artifacts

**Source only: users' attached exports, NOT synthetic replay measurements.**
`FINAL_SOAK_BUNDLE.tar.gz`: SHA-256
`8838cf6e857c0ad17384e1f343fff4a09e0c23fefc4789724f96d71ba6676315`.
`GUARDIAN_SOAK_FINAL_20261003.tar.gz`: SHA-256
`6a460b1f0d6656a441b3e2ab2a51479259821fb064602f4c36ab6569d6952b88`.
Both calculated archive hashes match their provided `.sha256` manifests.
Read **only** `durable_transitions.jsonl` for the historical durable-rate
statistics. Neither historical archive was loaded into any database.
Aggregated timestamps/row sizes/types only, no payload secrets copied.

| Metric | 2026-09-29 to 09-30 final soak | 2026-10-03 guardian soak |
|---|---:|---:|
| Durable rows | 1,302 | 477 |
| First global sequence | 853 | 2,155 |
| Last global sequence | 2,154 | 2,631 |
| Missing global sequence in export | 0 | 0 |
| Window seconds (first to last recorded_at) | 49,998.06 | 13,653.45 |
| Average durable rows/s | **0.02604** | **0.03494** |
| Peak observed 1-second bucket, rows | **4** | **5** |
| Peak observed 5-second bucket, rows | 6 | 5 |
| Peak observed 10-second bucket, rows | 6 | 7 |
| Peak observed 60-second bucket, rows | 7 | 7 |
| p95 individual JSONL row length, bytes | 5,010 | 5,406 |
| Max individual JSONL row length, bytes | 7,516 | 8,312 |
| Most common durable type | INFRASTRUCTURE_COST_ACCRUED | INFRASTRUCTURE_COST_ACCRUED |

These windows do NOT prove the deployment's future peak write rate or
any external market-data event rate. The prior **100 durable rows/s**
was a SYNTHETIC replay stress assumption, not an observed business rate.
At a hypothetical *constant* 0.035 rows/s, 8,192 new operation keys
would take about 65 hours; at 100/s it takes 81.92 seconds. Neither
extrapolation is a measured checkpoint rotation interval.
Business correctness still requires parity-proven rotation.

## 2. New reproducible benchmark

`tests/guardian-h4-local-capacity.test.ts`:
- One isolated PostgreSQL 17 on **127.0.0.1:55482**, DB
  `g2h_h4_local_20261011`, only from an explicitly guarded
  `G2H_H4_LOCAL_DATABASE_URL`; distinct from H1's 55481 and
  from every production/remote database.
- Real `PostgresEventJournal.commitDurableTransition()` writer,
  real SELECT-only RLS-policy reader and
  `GuardianTrustedPgSession` with true v2 fsynced observation ACK,
  canonical global sequence, `READ ONLY REPEATABLE READ` snapshots.
- H4 writer events use the actual recognized
  `LEDGER_MARK_TO_MARKET` type and semantic reducer, but with SMALL
  synthetic `prices:{}` payload. They are NOT representative 5KB
  PAPER_FILL rows; use this as a protocol/CPU baseline, NOT proof of
  realistic high-byte business traffic throughput.
- Two 6-second paced repetitions at 5 rows/s, the maximum observed
  historical one-second bucket as a **sustained test load**. Two more
  at 20 rows/s, **4 times** that short-burst reference.
  Poll schedule approximately 150ms. Verified every durable row reached
  on-disk ACK without cursor regression or missing operation keys.
- Separate 40-market/4-shard SCRIPTED bounded transport benchmark sends
  one current-timestamp UP and DOWN full book per market each second
  (80 books/s), repeated twice for full six-second intervals. A tick
  only counts when all **new** UP/DOWN timestamps have actually been
  normalized and 40/40 reads are READY. Each book has ONE level per
  ladder side, so book depth/bytes are NOT representative of the real
  venue. No Engine callback is used and no external network opens.

## 3. Actual measured numbers (final run on local laptop, not VPS)

Local host CPU: Intel i7-13620H, **16 logical processors**.
PostgreSQL Docker stats was sampled, not continuously profiled:
**0.05% CPU, 37.82 MiB resident / 7.601 GiB container visible**
at one *post-stress* snapshot. This is **NOT** a PostgreSQL p95 CPU
sample and does not establish target VPS capacity.

| Local PostgreSQL run | Actual rows/s | ACK lag p95 ms | ACK lag p99 ms | Poll p99 ms | Max sampled heap delta MiB | Max observed backlog rows |
|---|---:|---:|---:|---:|---:|---:|
| 5/s #1 | 5.00 | 176.95 | 189.57 | 54.32 | 10.65 | 0 |
| 5/s #2 | 4.87 | 181.36 | 183.02 | 49.97 | 16.64 | 0 |
| 20/s #1 | 19.15 | 173.21 | 194.60 | 37.47 | 23.77 | 0 |
| 20/s #2 | 18.99 | 182.46 | 204.48 | 39.99 | 15.62 | 0 |

CPU (combined Node benchmark writer, reader, ACK, runner overhead,
NOT pure Guardian CPU) per six-second run:
**516ms, 406ms, 812ms, 938ms**, respectively.
All four runs ended with fully confirmed local on-disk observation ACK
at global cursor **301**, no backlog, source-role read-only.
`maxBacklogRows=0` is per **sampled post-poll** state, not a statement
that there is never an in-flight row.

| Local 40-market books | Actual books/s | READY minimum | Source age p95/p99 ms | Full tick settle/read p95 ms | Gap count |
|---|---:|---:|---:|---:|---:|
| repeat 1 | 79.86 | 40/40 | 27 / 27 | 26.28 | 0 |
| repeat 2 | 79.89 | 40/40 | 23 / 23 | 21.34 | 0 |

Combined Node writer/observer CPU:
**93ms and 437ms** per full six-second book run.
Sampled heap deltas: **8.28 MiB and 2.77 MiB**.
Post-tick bounded ingress `depth/bytes` and exposed
`highWaterEvents/highWaterBytes` were all zero under this *scripted*
transport seam. That **DOES NOT** prove queue-headroom/overflow safety
under actual network ingress. No queue-loss event was observed.

## 4. Explicit unresolved H-05 acceptance conditions

- There is no production-history market BOOK frame rate/depth sample
  in these counted durable JSONL exports; current 80/s one-level book
  load is an explicitly constructed scenario, NOT a proven live maximum.
- The 5KB historical durable payloads are not yet faithfully replayed
  through this benchmark's small mark-to-market writes. Need a
  representative semantic event/byte distribution before capacity
  extrapolation or any changed architecture claim.
- Current run lengths (6 seconds) and two repetitions cannot
  establish hours-long heap stability, 2-vCPU target CPU envelope,
  p99 of real network reconnect or PostgreSQL failover.
- PostgreSQL's observed Docker CPU reading was ONE instantaneous sample
  taken after stress; continuous PG CPU/heap/disk sampler at a
  representative schedule and strict resource limits is still required.
- The scripted transport returned zero queue high-water counters;
  therefore queue occupancy saturation and event-overflow behavior
  require a separate realistic queue-traffic fixture.
- Need explicit operational **limits** derived from target hardware
  and acceptable observation lag; never invent thresholds from these
  laptop numbers. Business correctness and H2 checkpoint cadence/
  retention/power-loss proof remain blocking.

## 5. Safe reproducibility

1. Start a **new disposable** local PostgreSQL 17 container named
   `pm-g2h-h4-local-20261011`, DB `g2h_h4_local_20261011`,
   mapping **127.0.0.1:55482:5432**. Do NOT use your VPS,
   production DB, Supabase or existing containers.
2. Set `G2H_H4_LOCAL_DATABASE_URL` to the guarded local-only
   test database. The test rejects any other host, port, database
   or username and does not consult `DATABASE_URL`.
3. Run `pnpm exec vitest run tests/guardian-h4-local-capacity.test.ts`.
4. Review `G2H_H4_PG_PROFILE` and `G2H_H4_BOOK_PROFILE` console
   lines. They include rates, CPU milliseconds, heap deltas, lag
   percentiles, source age, READY and gap counters.
5. Stop the disposable container. The test reader is DROPped and
   the v2 ACK file is confined to a temporary directory. No deploy.

## 6. Scoped baseline verification

- H4 isolated paced benchmark itself: **2/2 PASS**, with two PG
  5/s repetitions, two 20/s repetitions and two 40-market book
  repetitions nested in the two tests.
- Unchanged Guardian baseline suite run separately with its own
  `127.0.0.1:55481` disposable PostgreSQL fixture:
  **364/364 PASS across 27 test files**.
- All four scoped TypeScript checks PASS: `@pm/db`,
  `apps/engine/tsconfig.guardian-source.json`,
  `apps/engine/tsconfig.forensic.json` and `@pm/core`.
- `git diff --check` PASS. The H4 standalone benchmark is intentionally
  *not* run at the same time or against the same database as H1. Do
  not add the counts as though a single, concurrent run was measured.

**G2H H4 = PARTIAL, NOT DONE.** This is measured local evidence,
not permission to migrate the architecture or move to live orders.

# Step 5 capacity evidence

This repository contains two different performance tools. `scripts/stability-hot-path-bench.ts` remains the isolated trigger, reference, normalization, and ingress microbenchmark. `scripts/sustained-capacity-harness.ts` drives the real `EngineService` through a bounded scripted market transport, discovery reconciliation, reference ingress, runtime supervisor, journal admission, and `SseHub`.

Run a local probe with:

```text
pnpm bench:sustained -- --duration-ms 10000 --markets 6 --market-rate 30 --reference-rate 12 --dashboard slow --database delayed
```

The default deterministic profile uses four markets across multiple assets, 30 market source events per market-second, 12 reference ticks per second, a reconnect/gap burst, one discovery refresh, and a 750 ms warm-up. It emits full books on first observation and multi-entry `price_change` frames thereafter. The dashboard profile can be disconnected, connected, or slow; the database profile can be healthy, delayed, or temporarily unavailable. Tests use shorter durations while retaining the same phases.

Each sample records process memory (`rss`, `heapUsed`, `heapTotal`, `external`, `arrayBuffers`), event-loop delay and utilization, process CPU, supervisor and journal admission, bounded market ingress, reference-window entries and retry timers, SSE subscribers and buffered bytes, active triggers/provenance/dedupe, market snapshots and caches, activation bookkeeping, settled-market tracking, and idle Grok tombstones. The report also counts source events, reconnect requests, reference ticks, burst events, raw observation rows, and durable transitions.

Plateau classification discards the warm-up window, evaluates the final half of the steady/recovery samples, and uses linear regression slope plus high-water marks. A queue is `monotonic_growth` only when its final slope exceeds both an absolute and relative threshold; a V8 heap expansion by itself is therefore not treated as a leak. The harness reports raw samples so reviewers can inspect the warm-up, burst, recovery, and plateau windows.

The current local evidence keeps these structures inside their architectural bounds:

| Collection | Domain and bound | Cleanup or saturation behavior |
| --- | --- | --- |
| `RuntimeSupervisor` lane queues | p0 256, p3 1024 events; bounded bytes and concurrency | admission rejects at the cap; failures retained at 32 |
| `DirectJournalAdmission` | 4 active, 32 critical waiters, 96 noncritical waiters | critical/noncritical rejection follows existing fail-closed policy |
| market ingress queue | bounded by event count and 16 MB | gap + resync; no silent coalescing |
| trigger/reference windows | trigger dedupe 8192; reference window 60 seconds/16384 | TTL/rolling eviction; overflow invalidates context and requests resync |
| Engine snapshots and recent state | per discovered/managed market; recent triggers 500 | discovery reconciliation removes vanished state; open-position obligations are retained |
| activation coordinator | configurable memory limit (500 in production defaults) | TTL/LRU-style ordered eviction and cooldown pruning |
| reference registry sessions/chains | one session per discovered asset | serialized start/stop; assets leave when no market requires them |
| SSE subscribers | bounded subscriber count policy and 256 KiB backpressure threshold | slow clients are closed after the byte or persistent-backpressure threshold |
| idle Grok tombstones | process-local exact retry map, bounded count and TTL | oldest entries evicted; no idle canonical journal row is written |
| settled markets | business-lifecycle set for markets with settlement obligations | cleared only when recovery/settlement lifecycle no longer needs the obligation |
| checkpoints/reports | retention policy and report identity keys | bounded retention; durable transitions are protected from deletion |

`settledMarkets` is intentionally business-lifecycle bounded by open position obligations, rather than an arbitrary timer. A production soak must confirm that obligation closure matches expected market turnover.

The real Polymarket adapter also has an experiment-start gate. A 48-hour start requires a connected feed, a synchronized trusted book for at least one entry-qualified candidate, and a fresh required reference. The same checks run immediately before the durable transition; a feed gap observed while preflight is in flight returns a deterministic blocker and does not create `EXPERIMENT_STARTED`. Test doubles that do not model transport trust retain their existing lifecycle semantics.

CPU numbers in the harness are measured on the local host. The rate-to-demand text is synthetic/estimated and must not be read as a Fly utilization percentage. The final `shared-cpu-2x` verdict remains unknown until a controlled paper-only Fly soak measures CPU utilization and steal, RSS/heap, event-loop delay, ingress rates, supervisor/journal waiters, database latency, reconnects, SSE clients/buffers, and storage over the agreed soak interval.

The Step 5 code-level verdict is `READY_FOR_PRODUCTION_SOAK` when the focused capacity/fault tests, the existing crash/recovery suites, and the full CI-equivalent commands pass. This does not authorize a deploy, production database mutation, experiment start, or live trading.

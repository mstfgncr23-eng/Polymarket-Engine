# Step 5 capacity and fault evidence

Step 5 has two separate local tools. `scripts/stability-hot-path-bench.ts` is an isolated trigger, reference, normalization, and ingress microbenchmark. `scripts/sustained-capacity-harness.ts` drives the real `EngineService` through bounded market ingress, discovery reconciliation, reference ingress, runtime supervision, journal admission, and `SseHub`.

Run the evidence profile with:

```text
pnpm bench:sustained -- --duration-ms 10000 --warmup-ms 1500 --markets 6 --market-rate 30 --reference-rate 12 --reconnect-storm true --reconnect-cycles 2 --dashboard slow --database delayed
```

The default profile uses four markets, synthetic market and reference rates, a burst, two reconnect cycles, one discovery refresh, and a warm-up. `--ingress-overflow true` is a separate scenario: it intentionally fills the bounded source queue and must observe overflow, a gap, full-book resynchronization, and a trusted final market state. The normal profile may have zero gaps and zero rejections.

Every sample records process memory (`rss`, `heapUsed`, `heapTotal`, `external`, `arrayBuffers`), event-loop delay, event-loop utilization, process CPU, supervisor and journal admission, bounded ingress, reference-window entries and retry timers, SSE subscribers and buffered bytes, active triggers/provenance/dedupe, market snapshots and caches, activation bookkeeping, settled-market tracking, and idle Grok tombstones. The report also counts source events, rejected source events, reconnect requests and completed cycles, reference ticks, burst events, raw observation rows, and durable transitions.

The event-loop sampler takes two absolute `performance.eventLoopUtilization()` snapshots and computes a delta from those snapshots; a delta is never reused as the next baseline. `monitorEventLoopDelay` is reset after each sample, so `delayP99Ms` and `delayMaxMs` describe that window. `delayP99HighWaterMs` and `delayMaxHighWaterMs` preserve whole-run high-water values. This exposes warm-up, steady, burst, and recovery windows separately.

## Capacity verdict

The harness emits a machine-readable `verdict`:

* `READY_FOR_PRODUCTION_SOAK` requires every structural invariant, storage-admission invariant, required queue/lifecycle-retained-state plateau, bounded hard-limit registries, bounded slow-client behavior, and acceptable local memory evidence.
* `NOT_READY` includes blocker names such as `INVARIANT_FAILED:<name>`, `REQUIRED_QUEUE_MONOTONIC_GROWTH:<name>`, `REQUIRED_MEMORY_MONOTONIC_GROWTH`, `STORAGE_ADMISSION_FAILED:RAW_OBSERVATIONS_PERSISTED`, or `INSUFFICIENT_LOCAL_EVIDENCE:memory`.
* Runs shorter than 3 seconds or with fewer than ten final-window samples report `localMemory: INSUFFICIENT_LOCAL_EVIDENCE`; this is not a pass. Structural boundedness remains independently visible as `structural: PASS` or `FAIL`.

Plateau classification discards warm-up and evaluates the final half of the remaining samples. Hard-capped stores use their explicit high-water invariants, transient queues use recovery/high-water evidence, and lifecycle-retained maps/sets use a final-window absolute sustained-growth policy. A retained metric is `monotonic_growth` only when its sustained absolute growth and positive-slope evidence clear the material floor; baseline-relative thresholds cannot hide a slow leak in a large map. Required queues and retained-state metrics fail the verdict when monotonic; short local memory evidence remains insufficient rather than being inferred from V8 expansion noise.

## Feed-start boundary

For the real Polymarket adapter, an experiment start requires a connected feed, a synchronized trusted book for an entry-qualified candidate, and a fresh required reference. Those checks run before the durable transition and again in `commitAuthoritative` immediately before it enters the journal. `experimentFeedGeneration` is incremented by gaps, disconnects, stale references, and discovery/token changes.

The boundary gate serializes lifecycle start and idle Grok acceptance. `experimentStartPending` keeps first-leg execution out while the start is in flight. New starts first persist `EXPERIMENT_STARTED` with `startValidationPending=true`; successful post-commit feed validation then persists `EXPERIMENT_START_VALIDATED` with the same experiment identity and `startValidationPending=false` before `beginExperiment` returns `ok:true`. If feed truth changes after the start row becomes authoritative, the engine records `EXPERIMENT_START_ABORTED` while the gate is held and returns `EXPERIMENT_FEED_INVALIDATED_DURING_START`. Recovery deterministically aborts a STARTED identity that has no VALIDATED or ABORTED transition; STARTED+VALIDATED remains a valid RUNNING experiment even when checkpoints are lost. Aborted starts cannot produce a normal final report. Test doubles that do not model transport trust retain their existing lifecycle behavior.

## Long-lived state inventory

The following inventory covers mutable collections owned by the touched runtime lifecycle. Bounds are architectural limits or business-lifecycle limits; no correctness-bearing state is evicted solely to make a capacity report pass.

| Owner / state | Purpose and lifecycle bound | Cleanup / saturation | Evidence |
| --- | --- | --- | --- |
| `EngineService.snapshots`, `restingOrders`, `makerReservations`, `recentTrades` | Current market books; open paper orders; maker ownership; recent trade timestamps. One entry per managed market/order/token and open obligation. | Discovery reconciliation removes vanished markets after obligations close; fills/cancels remove orders and reservations; recent trades follow existing time window. | MEASURED_LOCAL + STRUCTURALLY_PROVEN |
| `activeTriggers`, `triggerProvenance`, `provenanceInvalid`, `signalsByTrigger`, `lastEvaluatedSignalCount`, `signalReceivedTimes` | Actionable trigger lifecycle, provenance, invalidation, per-role signals, consensus dedupe, rate window. Trigger dedupe is bounded (8192) and signal rate uses the configured rolling limit. | Trigger expiry/settlement and dedupe eviction prune entries; admission rejects at supervisor/journal caps. | DETERMINISTIC_TEST + MEASURED_LOCAL |
| `marketsById`, `entryQualified`, `managedMarketIds`, `assetByMarket`, `assetIndex` | Discovery truth and managed/entry indexes. Cardinality follows discovered markets and open obligations. | Reconciliation removes vanished non-obligation markets and their index entries; obligation markets remain until settlement. | MEASURED_LOCAL + STRUCTURALLY_PROVEN |
| `settledMarkets`, `settlementBackoff` | In-memory duplicate suppression while a settled market remains in the live discovery/obligation set, plus retry backoff. Durable `POSITION_SETTLED` operation keys remain the recovery authority. | Settled ids are removed when discovery removes the market; open obligations keep a vanished market until settlement. Backoff is removed on success or market removal. | DETERMINISTIC_TEST + MEASURED_LOCAL; Fly soak required for turnover |
| `snapshotEpoch`, `tokenIds`, `tokenIdSet` | Source generation and subscribed token identity. | Replaced on discovery; gaps invalidate projections and require a new full-book epoch. | DETERMINISTIC_TEST + MEASURED_LOCAL |
| `referenceUnreliable`, `referenceEpoch`, `referenceLogicalGapped`, `referenceResyncInFlight`, `referenceResyncRequested`, `referenceRequired`, `referenceBlocked` | Reference trust, generation, gap and resync state per asset/market. | Reconciliation and feed recovery clear/advance state; duplicate requests coalesce; overflow marks the asset unreliable. | DETERMINISTIC_TEST + MEASURED_LOCAL |
| `referenceRetryTimers`, `referenceRetryDeadlines` | One retry timer/deadline per reference asset. | Timer is cleared on success, stop, or asset removal; retries are coalesced. | MEASURED_LOCAL |
| `idleGrokSubmissions` | Process-local exact retry and concurrent duplicate suppression while IDLE; no durable idle claim or canonical accepted row. | TTL and count bound (`IDLE_GROK_IDEMPOTENCY_MAX`); active promotion removes the reservation before durable claim. | DETERMINISTIC_TEST |
| `reportSourceIntegrity`, source-seal fields (`sourceSealing`, `sourceSealed`, generation, deadline, attempt, flight, abort) | Integrity faults and completion/source-seal coordination. | Faults are sticky and fail closed; seal attempts are cancelled by generation/deadline and persisted once. | DETERMINISTIC_TEST; Fly soak required for outage duration |
| `RuntimeSupervisor` queues, busy keys, failure ring | Bounded p0/p3 work and per-key serialization. | p0=256 and p3=1024 event caps; failures retained at 32; admission rejects at cap. | MEASURED_LOCAL + DETERMINISTIC_TEST |
| `DirectJournalAdmission` active/waiting queues | Bounded append admission with critical reservation. | Four active slots, 32 critical waiters, 96 noncritical waiters; deterministic rejection/fail-closed policy. | MEASURED_LOCAL + DETERMINISTIC_TEST |
| `TriggerEngine` books, pairs, reference windows, market windows, emitted dedupe/order | Trigger calculation and event dedupe. | Reference window is 60 seconds/16384 entries; trigger dedupe is 8192; market-window expiry prunes state. | DETERMINISTIC_TEST + MEASURED_LOCAL |
| `PolymarketMarketDataAdapter` token sync, books, timestamp map, token waiters | Per-token source synchronization and full-book generations. | Discovery replaces token set; gaps clear books and wake resync; waiters are resolved/rejected on stop. | DETERMINISTIC_TEST + MEASURED_LOCAL |
| `ReferenceFeedRegistry` adapters, resolution cache, sessions, chains, stop epochs, backoff | One feed session per required asset and serialized lifecycle. | Assets leave when no market requires them; chains coalesce and stop epochs prevent stale sessions. | DETERMINISTIC_TEST; Fly soak required for reconnect turnover |
| activation coordinator sent/order/recent/latency/retry maps | MCP activation dedupe, cooldown and latency history. | Configured memory limit (500 production default), cooldown pruning, ordered eviction. | MEASURED_LOCAL |
| `SseHub` subscribers/tracked clients/timer | Dashboard fan-out and backpressure tracking. | Subscriber policy and 256 KiB buffer threshold close slow clients; timer stops when empty. | MEASURED_LOCAL + DETERMINISTIC_TEST |
| checkpoints, durable transitions, report identities | Recovery and canonical lifecycle facts. | Existing retention and operation keys; durable rows are never removed by the harness. | DETERMINISTIC_TEST; Fly storage soak required |

`settledMarkets` is lifecycle-bound to currently retained discovery/obligation state. Historical settlement identity remains in durable `POSITION_SETTLED` rows and operation keys; replay applies the ledger transition but does not repopulate every historical marker. Current discovered markets may rehydrate a bounded suppression marker from their durable operation key, and rediscovery of an old market therefore cannot settle twice. The harness never prunes durable settlement rows. Production soak must confirm that discovery turnover and durable storage retention match the expected workload.

## Database profiles and evidence labels

`database=delayed|unavailable` in the sustained harness exercises monitor-only `APPEND` admission while raw market/reference observations remain non-durable. It is reported as `MEASURED_LOCAL` for that path and is not a durable-DB stress claim. `tests/step5-durable-fault.test.ts` covers direct journal uniqueness and an actual EngineService `PAPER_FILL` mutation through `commitAuthoritative`, including delay, outage, recovery, idempotent retry, and no duplicate ledger/cash mutation. `CHECKPOINT` and `IDEMPOTENCY` crash/replay behavior is covered by the existing Step 4/PR #13-15 suites and the focused persistence tests.

Evidence labels in review are explicit:

* `MEASURED_LOCAL`: observed by the local harness or test runtime.
* `DETERMINISTIC_TEST`: asserted with a controlled clock, transport, hold, or fault.
* `STRUCTURALLY_PROVEN`: follows from bounded queues, generation fences, and lifecycle code.
* `REQUIRES_FLY_SOAK`: CPU/steal, RSS over 48 hours, real reconnect rates, database latency/pool waiters, storage growth, and real SSE population.
* `NOT_EXERCISED`: a scenario not run by a particular profile (for example, overflow in the normal profile).

Local CPU values are process measurements on the host. Synthetic rates and payloads are not Fly CPU percentages. The final `shared-cpu-2x 512MB FRA singleton` capacity decision requires a controlled paper-only Fly soak; this code-level verdict does not authorize deployment, production database mutation, a 48-hour experiment, or live trading.

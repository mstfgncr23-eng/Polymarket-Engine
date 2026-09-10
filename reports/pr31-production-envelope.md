# PR31 production-envelope falsification

This is an experimental falsification report for the sole PR31 candidate: `maxDeliveryEventsPerSlice=128`, `maxDeliverySliceMs=0.5`. It is not a production scheduler selection, an OVH soak, a 1013 fix claim, or a live-trading validation.

Decision: **PASS_TO_OVH_COMPARISON**

| Scenario | Workload | Repeats passed | Required result |
|---|---:|---:|---|
| A | 51420 source events | 3/3 | PASS |
| B | 56180 source events | 3/3 | PASS |
| C | 45000 source events | 3/3 | PASS |

Bottleneck attribution: No local bottleneck was exposed by the required production-shaped scenarios; this does not establish production safety.

## Scope classification

- FULL REAL: BoundedClobMarketTransport, BoundedIngressQueue, candidate delivery scheduler, PolymarketMarketDataAdapter parsing/normalization, EngineService.onMarketSnapshot implementation, and TriggerEngine.ingestMarket implementation.
- PARTIAL REAL: Engine lifecycle uses a memory journal and deterministic configured token metadata; the harness wraps the real snapshot handler only to measure completion and source correlation.
- STUBBED: external reference/economic side effects are not required by the neutral, IDLE paper-mode workload; activation is the production no-op bridge.
- NOT EXERCISED: live OVH network behavior, actual server-side WebSocket send-buffer thresholds, reconnect/initial_dump recovery, dynamic subscription, active triggers, Grok/network calls, resting orders, fills, risk, and execution.

The source workload preserves approximately 98.1% `price_change`, 1.3% `book`, and 0.6% `last_trade_price`; every price-change source event carries two entries. Scenario C is explicitly synthetic safety margin above the observed Stage0 peak, not an observed production rate. Source events/sec and normalized snapshots/sec remain distinct units.

### KNOWN FROM STAGE0

Stage0 observed 1,713.75 expanded source events/sec average, approximately 3,641.33 source events/sec at the maximum sample interval, approximately 3,384.91 normalized snapshots/sec average, and 7,190.33 normalized snapshots/sec at that interval. The queue lifetime HWM was 104 with no ingress overflow, while 21 of 22 provider closes were 1013 slow-consumer closes. The server-side threshold is undocumented.

### WHAT THIS HARNESS ACTUALLY EXERCISES

A real in-process transport-to-Engine path with the PR31 candidate override, deterministic 40-market/80-token ordering, trusted initial books, the Stage0 event mix, two-entry price-change expansion, bounded real-time producer catch-up, exact source admission/delivery accounting, FIFO/per-token checks, queue bounds, event-loop/fairness/CPU measurements, and the real Engine/Trigger hot path.

### WHAT IT DOES NOT EXERCISE

It does not reproduce the remote Polymarket server, prove the cause of close code 1013, exercise reconnect initial dumps, or validate sharding, production persistence, reference feeds, triggers, risk, execution, or live trading.

### RESULTS A / B / C

#### A

- repeat 1: **PASS**; attempted/admitted/delivered/processed=51420/51420/51420/51420; normalized=101565/101565; queue HWM=80 events / 16391 bytes; producer lag max=15.02933395565924 ms; drain=1.1941999999980908 ms.
- repeat 2: **PASS**; attempted/admitted/delivered/processed=51420/51420/51420/51420; normalized=101565/101565; queue HWM=80 events / 16471 bytes; producer lag max=15.851429171525524 ms; drain=15.507799999999406 ms.
- repeat 3: **PASS**; attempted/admitted/delivered/processed=51420/51420/51420/51420; normalized=101565/101565; queue HWM=80 events / 16471 bytes; producer lag max=15.108919603284448 ms; drain=0.2865999999921769 ms.

Variance: {"workloadEventsAttempted":{"min":51420,"max":51420,"range":0},"workloadEventsDelivered":{"min":51420,"max":51420,"range":0},"producerSchedulingLagMaxMs":{"min":15.02933395565924,"max":15.851429171525524,"range":0.822095215866284},"queueDepthAtProducerStop":{"min":0,"max":12,"range":12},"queueHighWaterEvents":{"min":80,"max":80,"range":0},"queueHighWaterBytes":{"min":16391,"max":16471,"range":80},"transportDeliveryP99Ms":{"min":2.889600000002247,"max":2.907600000005914,"range":0.018000000003667083},"snapshotCompletionP99Ms":{"min":2.885099999999511,"max":2.912000000011176,"range":0.026900000011664815},"eventLoopUtilization":{"min":0.32708047169204796,"max":0.3334499354275869,"range":0.006369463735538927},"eventLoopDelayP99Ms":{"min":21.790719,"max":22.102015,"range":0.31129600000000224},"fairnessMaxDriftMs":{"min":12.875499999998283,"max":14.001299999996263,"range":1.1257999999979802},"processCpuMs":{"min":8578,"max":10984,"range":2406},"wallMs":{"min":30001.921000000002,"max":30019.332799999996,"range":17.4117999999944}}.

#### B

- repeat 1: **PASS**; attempted/admitted/delivered/processed=56180/56180/56180/56180; normalized=110960/110960; queue HWM=80 events / 16471 bytes; producer lag max=15.663275962651824 ms; drain=1.0856000000057975 ms.
- repeat 2: **PASS**; attempted/admitted/delivered/processed=56180/56180/56180/56180; normalized=110960/110960; queue HWM=80 events / 16551 bytes; producer lag max=15.03000490082195 ms; drain=60.577799999999115 ms.
- repeat 3: **PASS**; attempted/admitted/delivered/processed=56180/56180/56180/56180; normalized=110960/110960; queue HWM=80 events / 16551 bytes; producer lag max=15.321224504092243 ms; drain=0.28279999998630956 ms.

Variance: {"workloadEventsAttempted":{"min":56180,"max":56180,"range":0},"workloadEventsDelivered":{"min":56180,"max":56180,"range":0},"producerSchedulingLagMaxMs":{"min":15.03000490082195,"max":15.663275962651824,"range":0.6332710618298734},"queueDepthAtProducerStop":{"min":0,"max":9,"range":9},"queueHighWaterEvents":{"min":80,"max":80,"range":0},"queueHighWaterBytes":{"min":16471,"max":16551,"range":80},"transportDeliveryP99Ms":{"min":2.493499999996857,"max":2.6178000000072643,"range":0.12430000001040753},"snapshotCompletionP99Ms":{"min":2.57390000000305,"max":2.6319000000075903,"range":0.0580000000045402},"eventLoopUtilization":{"min":0.4753199240332429,"max":0.4820948056490006,"range":0.0067748816157576774},"eventLoopDelayP99Ms":{"min":21.626879,"max":21.987327,"range":0.36044800000000166},"fairnessMaxDriftMs":{"min":12.897299999996903,"max":14.00989999997546,"range":1.1125999999785563},"processCpuMs":{"min":11938,"max":14469,"range":2531},"wallMs":{"min":26001.291100000002,"max":26065.24859999999,"range":63.95749999998952}}.

#### C

- repeat 1: **PASS**; attempted/admitted/delivered/processed=45000/45000/45000/45000; normalized=88875/88875; queue HWM=80 events / 16551 bytes; producer lag max=1.2200555555464234 ms; drain=0.06680000000051223 ms.
- repeat 2: **PASS**; attempted/admitted/delivered/processed=45000/45000/45000/45000; normalized=88875/88875; queue HWM=80 events / 16551 bytes; producer lag max=2.221966666635126 ms; drain=0.0687000000034459 ms.
- repeat 3: **PASS**; attempted/admitted/delivered/processed=45000/45000/45000/45000; normalized=88875/88875; queue HWM=80 events / 16551 bytes; producer lag max=7.1416555555479135 ms; drain=0.09299999999348074 ms.

Variance: {"workloadEventsAttempted":{"min":45000,"max":45000,"range":0},"workloadEventsDelivered":{"min":45000,"max":45000,"range":0},"producerSchedulingLagMaxMs":{"min":1.2200555555464234,"max":7.1416555555479135,"range":5.92160000000149},"queueDepthAtProducerStop":{"min":0,"max":0,"range":0},"queueHighWaterEvents":{"min":80,"max":80,"range":0},"queueHighWaterBytes":{"min":16551,"max":16551,"range":0},"transportDeliveryP99Ms":{"min":0.07019999998738058,"max":0.07950000002165325,"range":0.00930000003427267},"snapshotCompletionP99Ms":{"min":0.12739999999757856,"max":0.13980000000447035,"range":0.012400000006891787},"eventLoopUtilization":{"min":1,"max":1,"range":0},"eventLoopDelayP99Ms":{"min":10.076159,"max":10.223615,"range":0.14745600000000003},"fairnessMaxDriftMs":{"min":0.40030000000842847,"max":0.9552000000257976,"range":0.5549000000173692},"processCpuMs":{"min":9844,"max":9984,"range":140},"wallMs":{"min":10000.11659999998,"max":10000.14969999998,"range":0.033100000000558794}}.

### BOTTLENECK ATTRIBUTION

No local bottleneck was exposed by the required production-shaped scenarios; this does not establish production safety.

### FALSIFIED HYPOTHESES

Within this local production-shaped envelope, the candidate did not fail through intended producer under-injection, bounded ingress overflow, transport reordering, normalization loss, or the measured serial Engine/Trigger path. This falsifies only those local failure modes for these scenarios; it does not falsify remote server send-buffer pressure.

### REMAINING HYPOTHESES

Remote per-connection burst/send-buffer pressure, reconnect initial dumps, differences between the synthetic transport and the Polymarket server, and any production-only scheduling or persistence interaction remain unresolved.

### DECISION

**PASS_TO_OVH_COMPARISON** — this result is local falsification evidence only. It does not select a production scheduler, claim to fix 1013, approve merge, approve deploy, or approve live trading.

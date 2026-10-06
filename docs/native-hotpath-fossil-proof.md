# Native V1 hot-path dependency proof

Audit performed before implementation against `92b1cf600ec948d5acdff477aab3dd8baa66c325` on isolated branch `perf/native-only-hotpath-fossil-purge` (clean status).

PR target is `analysis/guardian-shadow-24h`: PR #96 metadata identifies that branch, and GitHub comparison confirms its head equals the exact authorized base. Repository default `main` is a divergent Astra lineage and must not be used as the target for this Native-only patch.

| Candidate | Classification | Removal boundary |
| --- | --- | --- |
| A: `ensureLatency` / `snapshot` / `setActivationLatency` | PROVEN_SAFE_REMOVE | Remove from `processTrigger`; retain coordinator, recovery parser and attribution for actual inbound MCP/research activity. |
| B: Native `GROK_ACTIVATION_DECISION` | PROVEN_SAFE_REMOVE | Stop producing the Native non-wake event; retain historical journal types, retention classification and readers. |
| C: Native `addActivation` | PROVEN_SAFE_REMOVE | Remove the only producer, dead state list/method/type and unused dashboard field. |
| D: scout-refresh callsites | KEEP_CURRENT_AUTHORITY | Keep optional xAI/scout warming. Configuration, role runtime, inbound research and legacy manifests still support this path; the task requires proof that no authorized optional mode expects it, which the repository does not establish. |

## Producers and consumers

`EngineService.processTrigger` unconditionally evaluates Native V1 and produces A/B/C inside `experimentBoundaryGate`. A copies up to 500 attribution records and clones them into `EngineStateStore`; C unshifts up to 200 non-wake records. Neither enters `evaluateNativeDecisionForTrigger`, `nativeCompatibilityDecision`, `evaluateTrade`, risk, the ledger, profit trailing, settlement, or durable transition admission. Preserve the boundary gate and all canonical trigger/consensus writes.

A has real, optional diagnostic consumers: `noteMcpRequest`, accepted `submitGrokSignal`, the snapshot API, checkpoint writing and `restoreCheckpointPayload`. Preserve these. Create attribution lazily only for a known trigger when an explicit MCP request or accepted external signal arrives, rather than for every Native trigger. Generic polls must not create attribution; unknown trigger IDs must not allocate records. Historical latency remains recoverable. No external signal gains economic authority.

B has no semantic handler in `ExperimentSummaryAccumulator` (`report.ts`) or the critical recovery reducer (`recovery.ts`). It is an ordinary canonical row whose presence affects source counts, not trading/report outcomes. Final-report integrity derives its count/digest from the actual persisted stream, so omitting the producer does not require forged counts or a migration. Keep `TRIGGER_EVENT`, `CONSENSUS_DECISION`, source sealing and persistence ordering unchanged. Keep the old event name in `journal-types.ts` and retention allowlists so historical rows remain readable.

C has no reader outside snapshot construction, optional dashboard typing and fixture literals (`rg` across engine/dashboard/tests). The dashboard components and selectors do not render/use it. It is not a checkpoint producer or restored field. Extra `activations` fields in old checkpoint payload objects are already ignored. Remove the unused API exposure and fixture literals; retain health/trading/position/risk/feed/experiment fields. `activationLatency` remains an optional diagnostic API field for real external activity and historical checkpoint compatibility.

D has market and per-asset reference callsites. `requestScoutRefresh` returns early for Native authority; otherwise it can dispatch bounded roles using HotResearchCache, xAI configuration and the existing budget. Its reference loop is not the primary measured bottleneck. Retain it unchanged.

## Tests encoding historical behavior

- `native-close-entry-race` and `persistence-admission` require the obsolete Native activation event: change only that expectation and retain economic/lifecycle assertions.
- `hardening-engine` expects Native-created latency after restart: replace with absence; add a separate old-checkpoint recovery fixture with nonempty historical latency and an old activation journal row.
- `discovery-reconciliation` MCP attribution cases and `multi-market-activation` generic-poll case expect a prepopulated Native latency record: assert lazy allocation and preserve actual per-trigger MCP/signal attribution isolation.
- `hot-path-instrumentation` requires the obsolete bookkeeping histogram observation: remove the dead histogram and assert its absence while preserving process-trigger accounting checks.
- Keep coordinator, dormant optional mode, profit-trailing, risk, recovery, supervisor and report regression coverage.

No checkpoint schema break, DB migration, history rewrite, concurrency/capacity change, env change, new queue/framework, merge, deployment or production operation is authorized by this patch.

## Reproducible comparison

Copy `scripts/native-hotpath-fixture.ts` and `scripts/native-hotpath-bench.ts` unchanged into a detached exact-base worktree. Install the same frozen lockfile, then run each worktree with:

```text
node node_modules/tsx/dist/cli.mjs --tsconfig tsconfig.tests.json scripts/native-hotpath-bench.ts result.json 2048 0
```

The patch command can additionally take the base result path as its fifth argument. It asserts identical harness bytes, Node/workload/backend parameters and normalized economic outputs. Only generated identities and wall-clock audit metadata are stripped; Native probabilities/features, risk verdicts, intent, side, price, size, fees/slippage, ledger/positions and durable trailing payloads remain in the comparison. The economic fixture requires an actual Native first-leg fill and a BUY/HEDGE_SECOND_LEG after arm/high-water/retrace; existing tests separately cover .12 target, recovery and settlement hold.

The storm uses 8 markets, a fixed clock, 512 warmup triggers (filling the base's 500-entry history) and 2,048 measured triggers in bursts of 64. Flat books prevent trading during timing while still requiring one canonical trigger and one Native consensus per measured trigger. The runtime bounds are unchanged. It measures wall time, process CPU time, throughput, sampled queue peak/RSS, activation-only call duration and journal append counts/wall time by event type. A second workload (`512 1`) injects the same 1ms-per-append test-double delay into both versions to isolate synchronous I/O occupancy. Neither workload measures live PostgreSQL, ingress slow-consumer behavior or production CPU saturation.

Historical activation rows intentionally affect `decisionAccountingEventCount` by their presence; regression coverage verifies that all other final-report semantics remain identical and that the retained historical row is still readable.

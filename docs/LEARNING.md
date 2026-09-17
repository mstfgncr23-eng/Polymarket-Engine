# Native V2 Learning Memory (PR1 foundation)

Paper-only durable learning samples for Native V2 shadow features and fair
probability. This does **not** change V1 authority, V2 decision/ranking/
execution outputs, risk, or the economic ledger.

## Enablement

| Env | Default | Meaning |
| --- | --- | --- |
| `LEARNING_ENABLED` | `false` | Opt-in. Production unchanged until enabled. |
| `LEARNING_DATA_DIR` | `/data/learning` | Durable directory inside the engine container. |
| `LEARNING_MAX_BYTES` | `256000000` | Operational **default** byte cap (not a measured capacity requirement). |
| `LEARNING_CHECKPOINT_SECONDS` | `240,180,120,60,30,15,5` | Comma-separated seconds before expiry. |
| `LEARNING_SETTLEMENT_POLL_MS` | (falls back to `SETTLEMENT_POLL_MS`) | Non-economic learning label poll interval. |

Failures degrade learning only (`summary().degraded` / manifest `lastError`).
Trading continues.

## Container path + host mount contract

The engine writes append-only segmented JSONL under `LEARNING_DATA_DIR`
(default `/data/learning`). On OVH/Fly (or local Docker), mount a persistent
host/volume path to that container path before enabling learning:

```text
host:  <persistent-volume>/learning
guest: /data/learning
```

Example compose fragment (do not treat as a deploy change in this PR):

```yaml
services:
  engine:
    environment:
      LEARNING_ENABLED: "true"
      LEARNING_DATA_DIR: /data/learning
    volumes:
      - learning-data:/data/learning

volumes:
  learning-data:
```

Without the mount, process restart loses samples when the container filesystem
is ephemeral.

## Layout

```text
/data/learning/
  manifest.json          # schema versions, git SHA, counts, bytes, degradation
  segments/
    segment-00001.jsonl  # append-only NDJSON rows (sample | label)
```

## Sample identity + checkpoints

- Identity: `${marketId}::${checkpointId}` (e.g. `mkt-1::T-60`)
- Crossing: first observation where `feature.timing.timeToExpiryMs <= checkpointMs`
- Exactly one logical sample per market+checkpoint
- `timeToExpiryMs === null` → skip sampling (no invented timing)
- Invalid checkpoint seconds (non-positive / non-integer) are skipped
- Checkpoints larger than an optional known market horizon are skipped

## Settlement labeling (non-economic)

`pollLearningSettlements` reuses `resolutionFetcher` + `parsePublicBinaryResolution`
and calls `LearningSampleStore.labelMarket` only. It does **not** call
`settleMarket`, `commitAuthoritative(POSITION_SETTLED)`, or mutate the ledger.
Unresolved samples stay explicitly unresolved until authoritative resolution.

## Counterfactual V2 portfolio state (PR2)

Phase-4 shadow execution evaluates risk/cash/positions against an **isolated
Native V2 counterfactual portfolio**, not the authoritative V1 paper ledger.

| Concern | Behavior |
| --- | --- |
| State basis | `DECISION_TIME_V2_COUNTERFACTUAL_STATE` |
| Storage | `${LEARNING_DATA_DIR}/counterfactual/` (manifest + segmented JSONL) |
| Accounting | Reuses `PaperLedger.applyFill`, `executePaperTaker`, settlement helpers |
| Advance rule | State advances **only** when shadow eval `wouldSubmitOrder === true` |
| Idempotency | Action key `${marketId}:${outcome}:${intent}` — reevaluations do not stack fills |
| Settlement | Uses authoritative public/learning resolution labels only — never invents winners |
| Isolation | Never mutates V1 ledger, real PAPER cash, V1 positions, V1 reserved cash, execution authority, or deterministic risk authority |

### Layout

```text
/data/learning/
  manifest.json              # PR1 learning samples
  segments/...
  counterfactual/
    manifest.json            # CF fill/settlement counts
    segments/
      segment-00001.jsonl    # append-only fill | settlement events
```

### Recovery

On process start, `NativeV2CounterfactualStore.init()` replays durable
counterfactual events into a fresh isolated `PaperLedger`. Failures degrade
counterfactual durability only (`summary().degraded`); V1 trading continues.

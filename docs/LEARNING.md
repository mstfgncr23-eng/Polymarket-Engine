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


## Learnable feature schema (PR3)

Feature schema version: `native-v2-shadow-features-v2` (was `native-v2-shadow-features-v1`).

PR3 enriches Native V2 **learnable information only**. Learned models still have
**no authority** over V1 decisions, deterministic risk, or execution.

### Part A — event-flow / OFI: BLOCKED

CLOB transport evidence:

- Wire `event_type`s normalized by `normalizeRawClobMarketEvent` in
  `packages/market-data/src/bounded-clob-market.ts`: `book`, `price_change`,
  `last_trade_price`
- `price_change` applies absolute post-state size via `applyLevelChange` in
  `packages/market-data/src/polymarket.ts` (not typed add/cancel deltas)
- `last_trade_price` only updates in-memory `state.lastTradePrice` and returns
  **no** `MarketSnapshot`s to the engine feature path
- `NativeV2ShadowFeatureExtractor.ingestMarket` accepts snapshots only — never
  raw CLOB events

Therefore add/cancel/trade cannot be discriminated reliably. PR3 does **not**
fabricate OFI from snapshot diffs. `featureVector.eventFlow` is always:

- `available: false`
- `reason: "CLOB_TRANSPORT_LACKS_ADD_CANCEL_TRADE_DISCRIMINATION"`
- all numeric OFI fields explicitly `null`

Follow-up: wire a bounded event-flow buffer at the CLOB normalize boundary if
Polymarket ever exposes typed add/cancel/trade deltas.

### Part B — resolution-aligned features: LANDED

From already-supported market + Binance reference inputs:

- `resolutionAligned.timeToExpiryMs` + `invSqrtTimeToExpiryMinutes`
- TTE interactions: book pressure / short momentum / vol-normalized movement × invSqrt(TTE)
- Mid vs market-prior distances + binary complement basis
- Reference venue/symbol/age + market↔reference source skew
- Synchronization / freshness passthrough

Explicitly unavailable (no invented strike):

- `referenceStrikeDistanceAvailable: false`
- `referenceStrikeDistanceBps: null`

(`DiscoveredMarket` has `endDate` but no price-to-beat / strike field.)

### Part C — cross-venue / oracle: partial

| Source | Status |
| --- | --- |
| Binance reference (existing `ReferenceTick` / reference feed) | Exposed under `crossVenueOracle.binance` when ticks present |
| Chainlink | `available: false`, `reason: "NO_EXISTING_INTEGRATION"` |
| Coinbase | `available: false`, `reason: "NO_EXISTING_INTEGRATION"` |

No new network/feed architecture was added.

### Authority

Unchanged: V1 Native Math → Strategy → Risk → Execution. PR3 only widens the
shadow/learning feature vector.

## Offline training / validation (PR4)

Paper-only offline forecasting pipeline. **No runtime trading authority**, no auto
promotion, no live order submission.

### Goal

Turn finalized (resolved) learning observations into a reproducible offline
candidate: dataset → market-grouped walk-forward → residual logistic → baselines
+ calibration + economic eval → versioned artifact.

### Format choice: JSONL (not Parquet)

**Chosen:** compact JSONL (`dataset.jsonl` + `dataset.manifest.json`).

**Rationale:** the stack already persists learning samples as segmented JSONL
under `LearningSampleStore`. Adding Parquet would introduce an Arrow/Parquet
dependency solely for aesthetics. JSONL stays dependency-free, diff-friendly,
and checksum-stable.

### Model choice: inspectable residual logistic

```
logit(p_learned) = logit(p_market) + w·x + b
```

L2-regularized logistic residual around the market prior. Pure TypeScript — no
GBDT/ML stack. This is **not** a BUY/SELL classifier.

### Always-scored baselines

1. Market prior (`pair.marketPriorUp`)
2. Deterministic V2 shadow fair probability
3. Learned residual model

### Leakage prevention

- Rows are grouped by `marketId`
- Walk-forward assigns whole markets to train or validation (never both)
- Optional `trainCutoffObservedAt` for explicit time cutoffs
- Same-market random splits are rejected

### Calibration

Brier, log loss, and calibration buckets. If an isotonic calibrator is fit, it
uses a held-out slice of validation only — never the same rows that are scored.

### Economic evaluation (available vs unavailable)

| Available from sample fields | Unavailable without new joins |
| --- | --- |
| Approximate executable ask/bid via mid±spread/2 | True VWAP depth-walk (asks not stored) |
| Expected gross edge (p − ask) | Fee schedule on sample |
| Optional assumed fee → net edge | Counterfactual ledger realized PnL join |
| Binary settlement PnL for hypothetical BUY UP | Live fill prices |
| Drawdown / trade count / asset / checkpoint breakdown | |

### CLI

```bash
pnpm learning:train-validate -- --data-dir /data/learning --out ./artifacts/learning-pr4
```

### Candidate artifact

`candidate.json` includes: model type/version, feature schema, dataset version,
training cutoff/window, metrics, economic eval, git SHA, checksumSha256.

### Authority

Unchanged: V1 Native Math → Strategy → Risk → Execution. PR4 is offline tooling
only.


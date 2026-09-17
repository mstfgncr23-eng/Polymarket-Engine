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
- Checkpoint selection: deterministic nearest-eligible band from durable sample IDs + current TTE
  (each checkpoint C owns `(midpoint(S, C), C]` vs next-smaller neighbor S; at most one sample per observation)
- Restart-safe: does not depend on volatile previous-TTE cursors or hot-path cursor writes
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

### Experiment lifetime / scoping (KNOWN)

V1 PAPER portfolio is **continuous across experiment windows**, not reset per experiment:

- `PaperLedger` is constructed once from `PAPER_START_BALANCE` (`apps/engine/src/engine.ts`).
- `beginExperiment` stamps `startingBalance: this.ledger.snapshot().startingBalance` (carries cash/PnL forward).
- `experimentEconomicResidueBlockers()` refuses a new experiment when open positions / resting orders / reservations remain — it does **not** wipe the ledger (`docs/native-restore-pr53-audit.md`).
- On `START_VALIDATED`, only `nativeV2ShadowStore.reset()` runs (shadow feature cache), **not** a CF/V1 ledger reset.

CF therefore keeps process-lifetime + durable continuous portfolio semantics (durable `startingBalance` authoritative, no experimentId wipe). Action keys stay `${marketId}:${outcome}:${intent}` and clear on settlement, but settled market IDs remain terminal tombstones (never reopen). Learning samples may stamp `experimentId` for attribution, but the learning store is also continuous.

### Bounded CF storage

| Env / option | Default | Meaning |
| --- | --- | --- |
| `maxBytes` (store) / `LEARNING_MAX_BYTES` (engine wiring) | `256000000` | Total CF event-log byte cap under `.../counterfactual/` |
| `segmentMaxBytes` | `8000000` | Per-segment rotation only (insufficient alone) |

Fill admission is obligation-aware: a fill is accepted only when `currentBytes + fillRowBytes + sum(exactSettlementObligation(openMarketId)) <= maxBytes`. Each open market's obligation is the **exact serialized JSON identity byte cost** of that `marketId` (including JSON escapes) plus bounded non-identity overhead (clamped `source` / `reason` / deltas + fixed `at`). Settlement `marketId` is preserved exactly (legal max 160 per `idSchema`; never truncated) so restart replay matches the fill identity — even when JSON escaping expands beyond 1 byte/char. A fixed 1024-byte-per-open constant is **not** sufficient for worst-case legal ids. When the cap is hit, the write is rejected (`CAPACITY_EXCEEDED`), in-memory state is **not** advanced, and `summary().degraded` is set — V1 authority unchanged. On restart, open positions are reconstructed from the durable log and obligations are recomputed from those exact market IDs.

### Durable commit atomicity

Fill and settlement transitions stage on a temporary `PaperLedger`, append the durable segment row first, then apply the live in-memory transition. A manifest write failure **after** a successful segment append does **not** roll back the economic event.

### Recovery / fail-closed CF

On process start, `NativeV2CounterfactualStore.init()` replays durable
counterfactual events into a fresh isolated `PaperLedger`.

- **V1** remains fail-soft elsewhere.
- **CF is fail-closed** when durable recovery is untrustworthy (corrupt/unreadable
  economic rows, startingBalance mismatch, unsafe maxBytes decrease).
- Exposes `recoveryStatus` / `recoveryReady`; Phase-4 must not use CF as
  stateful shadow authority while unavailable (`RECOVERY_UNAVAILABLE`).
- Torn/truncated last-segment tail (no trailing newline) is skipped deterministically;
  any other corrupt economic row fails CF closed (never silently skipped).
- Persisted `startingBalance` is authoritative; constructor mismatch → CF unavailable.
- `maxBytes`: safe increase allowed; unsafe decrease that cannot cover committed
  bytes + settlement obligations → CF unavailable.
- After official settlement, the market identity is a **terminal tombstone**
  (reconstructed from durable settlement rows) — same market never reopens.

### Ops notes (non-blocking)

- OVH `/data/learning` mount is an ops prerequisite for durable CF/learning recovery.
- Combined PR1 learning + CF disk usage shares `LEARNING_MAX_BYTES` defaults per store
  layout under `LEARNING_DATA_DIR` (document capacity jointly in ops).
- Compaction of CF segments is **not** a PR2 merge blocker.

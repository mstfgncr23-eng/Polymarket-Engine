# Naked directional behavior diagnostic (PAPER only)

`NAKED_DIRECTIONAL_SOAK_ENABLED=false` is the default. Enabling it opts the next paper experiment into observing its actual first-leg positions through public binary resolution. This document describes the implementation; it does not authorize starting an experiment.

## Economic authority

Discovery, native decisions, qualification, entry sizing, the first-leg price regime, admission, fees, latency, maker evolution and first-leg revalidation use their existing production paths. No threshold is changed. The engine remains restricted to `TRADING_MODE=paper`.

The explicit flag is recorded as `riskConfiguration.nakedDirectionalSoakEnabled` in the durable experiment manifest. A diagnostic experiment uses `observe_until_settlement` even if the runtime requested `mark_to_market`. Its entry deadline still stops new first legs. Already-open positions keep their normal settlement obligations and the experiment cannot complete until every position settles.

The following economic authorities return before admitting an action:

- Normal `HEDGE_SECOND_LEG` in `evaluateTrade`.
- PR3 in `maybeEarlyCatastrophicPairLock`.
- Guardian in `maybeGuardianProtectiveSell`.
- PR82/deadline fallback, including recovered exit execution, in `maybeProtectiveDeRisk`.

`admitRiskReduction`, `submitPaperOrder`, `reserveMaker` and `applyFill` also fence disabled intents. An opposite BUY or held SELL cannot bypass the gate by being labeled as a first leg. Same-side partial first-leg maker fills retain their existing behavior. `resumeRiskReduction` refuses economic execution and releases any legacy recovered exit if called. `markToMarketClose` cannot finalize a diagnostic experiment with unresolved inventory.

The mode is sticky for an active durable experiment and for its diagnostic positions. Restarting with the runtime flag false does not restore intervention on those positions. Restarting with the flag true does not enable diagnostics for an existing non-soak experiment, including older manifests without the flag. The environment flag is used only when constructing the next experiment. A subsequent clean experiment uses its new runtime configuration. Database, journal, integrity, cash and entry lifecycle gates remain authoritative. Observation persistence uses the existing durable mutation gate and therefore a persistence failure retains the existing fail-closed behavior.

## Observations

Only a real authoritative paper first-leg fill creates a diagnostic record. The observer reads the actual position and cached, synchronized books. It creates no order, candidate, owner, admission, synthetic close, shadow position or independent strategy, and performs no database reads or writes in the book callback.

- Directional mark is the held book's best bid. Directional PnL is `quantity * (mark - average entry cost) - actual entry fees`. This is a mark, not a hypothetical SELL execution; no unincurred exit fee is charged.
- Opposite execution walks the actual ask depth for the position's entire residual, using the canonical paper VWAP and taker fee calculation. Partial depth and its VWAP/fee are visible, but a full executable pair opportunity requires synchronized fresh depth, a connected feed and a known fee schedule.
- Gross pair edge per share is `1 - average entry cost - opposite VWAP`. Locked pair PnL also subtracts actual entry fees and executable opposite fees; net pair edge divides that PnL by residual quantity.
- PnL minima/maxima and gross/net pair-edge maxima update on every accepted observed book, before sampling. Decimal values, timestamps, elapsed time and the contemporaneous PnL/pair-edge context remain separate from the bounded path. No fixed `.04/.05/.06` threshold-crossing ladder is used.
- The path includes time samples, arbitrary pair-edge changes of at least `.001` per share, meaningful mark movement, and availability/freshness transitions. Each point includes time since entry and time since the previous meaningful pair-edge change.
- Source/receive timestamps, freshness, depth, fees and feed status travel with observations. Ingress overflow/slow-consumer, reconnect/connection, stale/future/malformed timestamp and feed error events correlate to open positions by token/market or reference asset. All delivered timestamp anomalies are counted before the existing log sampling gate.

No observations are fabricated for stale/future/unsynchronized books or downtime. Freshness and missing-book transitions remain visible. Opposite pair economics can still be observed when the held mark is unavailable.

## Storage, recovery and reports

Memory retains at most 128 path points and 64 anomaly details per open position. Path thinning preserves coverage across the lifetime; independent extrema and anomaly counters are not thinned. Anomaly details preserve the first event and the most recent events.

A dedicated five-second timer persists dirty-position batches through `NAKED_DIRECTIONAL_OBSERVATION` in the existing `durable_transitions` table, independently of the one-minute maintenance timer. Timer and maintenance calls coalesce into one write with the same minimum five-second interval. The timer is cleared during shutdown and journal admission joins pending persistence. Each position carries at most eight **new** path points in a batch, plus cumulative extrema/current state/anomaly metadata; previous path points are not repeatedly copied. Replay merges incremental paths and never overwrites newer in-memory observations. Existing checkpoints preserve the full bounded in-memory diagnostic state. State and write volume depend on currently open positions and configured bounds.

Canonical `POSITION_SETTLED` economics and idempotency are unchanged. Its durable derived audit atomically includes one `NAKED_DIRECTIONAL_SUMMARY` with entry, PnL/pair paths, extrema, anomalies, winner, actual gross/net paper result and lifetime. The record is a canonical report input and follows existing report-source integrity, sealing, analytics rollup and retention rules. Final report JSON and Markdown include those per-position summaries. Diagnostic state and persistence caches are removed when the position settles.

There is no new table, migration, live/wallet authority or external service.

## Limits

Exact extrema refer to books actually accepted by the running engine and the existing canonical execution arithmetic. The bounded time series is intentionally lossy; it cannot reproduce every book tick. Restart marks an observation gap, restores all committed extrema/checkpoint state, and resumes observation from fresh books. An abrupt process loss can lose observations since the last successful diagnostic batch/checkpoint, including extrema in that uncommitted interval. Disconnected periods have no invented market path.

The diagnostic transition type requires this engine version for replay; rolling back to an older binary while diagnostic obligations remain is unsupported and fails closed. PostgreSQL integration cases require the existing test database environment. Validation runs use scripted paper adapters and do not start a soak.

# Astra Brain V1

Astra is an opt-in economic decision authority for paper experiments. The
default remains `NATIVE_DETERMINISTIC_V1`; setting `ASTRA_ENABLED=true` makes
the experiment manifest record `ASTRA_BRAIN_V1` and routes first-leg economic
decisions through the deterministic feature/gate boundary and the dedicated
OpenAI Responses client.

## Safety boundary

- No Native fallback exists when Astra is enabled. Missing structured output,
  timeout, governor denial, stale data, journal failure, and failed current
  book revalidation fail closed.
- Existing unpaired positions use the deterministic hedge path directly. Astra
  is not called for a hedge, and the existing `$25` hedge cap remains intact.
- New first legs use a separate `$12.50` cap. This is enforced in strategy
  sizing and in deterministic risk evaluation.
- The existing journal and durable mutation gate remain the economic authority.
  Per-trigger Astra facts are bounded and appended in a single canonical batch
  before an order can be submitted. Astra usage cost is recorded through the
  existing authoritative operating-cost transition.
- Memory, bounded rolling FAST-dispatch governor state, and the bounded learner
  checkpoint are included in the existing verified engine checkpoint. No raw
  history, secrets, or chain-of-thought are persisted.

## Configuration

Astra is disabled by default. When enabled in production, `ASTRA_API_KEY` is
required by configuration validation and must be supplied through the runtime
environment only. The application does not read keys from files or log them.

There is no calendar-day monetary budget or UTC reset. FAST dispatch is bounded
by a rolling one-hour governor (default 12 dispatches), a 5-second global
minimum interval, and a 60-second per-market cooldown. Fast and Deep calls have
separate bounded concurrency, timeout, output-token, and uncertainty controls.
Only explicit economic candidates, or severity-2+ strong informational and
microstructure candidates, may enter paid first-leg inference; maintenance,
risk, settlement, and unclassified triggers are rejected before provider work.

The tradability/data-quality gate remains a separate layer and still requires
synchronized, provenance-valid, fresh, sufficiently liquid, narrow-spread
market truth. A canonical `ASTRA_PROVIDER_DISPATCH_STARTED` event is written
before each authorized provider request. The dedicated Astra OpenAI client sets
`maxRetries: 0`; `ASTRA_FAST_TIMEOUT_MS` defaults to 10000 ms and starts only
when the SDK request begins, so faster responses are processed immediately.

Fast is the ordinary first-leg economic decision. Deep is a deterministic,
selective challenge pass; it is never called after Fast returns `NO_TRADE`, and
its failure produces `NO_TRADE` rather than falling back to Fast. Batch runs
after the configured settlement threshold, uses only compact calibration and
lesson state, returns at most eight strict lessons, and is single-flight and
non-blocking for trading. Batch failure cannot stop the engine.

## Smoke test

`pnpm smoke:astra` is an explicit live, single-call smoke command. It requires
both `ASTRA_ENABLED=true` and `ASTRA_API_KEY` in the environment, sends only a
synthetic feature packet, validates strict structured output, and does not start
an experiment or submit an order. It is intentionally not run by CI or by the
implementation workflow without a securely injected key.

## Learning and reporting

Settlement observations feed a bounded batch learner for calibration and
counterfactual evidence. Reproducible counterfactuals are computed only from
recorded execution facts; otherwise they are marked unavailable. Astra
invocation counts and costs are reported separately for `ASTRA_FAST`,
`ASTRA_DEEP`, and `ASTRA_BATCH`. Usage, failures, dispatch-start evidence, gate
outcomes, deep reviews, reconnect timing, and learning updates are observable through
bounded metrics and canonical report inputs. The existing completion and
final-report flow remains unchanged.

Recovery restores the experiment identity, authority, bounded memory,
calibration/lesson state, and rolling governor evidence from the existing
checkpoint and canonical journal. Cost reporting retains exact known usage and
marks provider cost completeness `COMPLETE`, `PARTIAL`, or `NONE` when a
dispatch outcome is unknown. Historical daily-budget checkpoints/events remain
readable but are not runtime authority. Native V1 remains available for the
default mode and historical compatibility, but it is not an economic fallback
when Astra is enabled.

For a later deployment, production must explicitly set
`ASTRA_FAST_TIMEOUT_MS=10000` after this change is approved. This task does not
change production configuration.

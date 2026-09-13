# Astra Brain V1

Astra is an opt-in economic decision authority for paper experiments. The
default remains `NATIVE_DETERMINISTIC_V1`; setting `ASTRA_ENABLED=true` makes
the experiment manifest record `ASTRA_BRAIN_V1` and routes first-leg economic
decisions through the deterministic feature/gate boundary and the dedicated
OpenAI Responses client.

## Safety boundary

- No Native fallback exists when Astra is enabled. Missing structured output,
  timeout, budget exhaustion, stale data, journal failure, and failed current
  book revalidation fail closed.
- Existing unpaired positions use the deterministic hedge path directly. Astra
  is not called for a hedge, and the existing `$25` hedge cap remains intact.
- New first legs use a separate `$12.50` cap. This is enforced in strategy
  sizing and in deterministic risk evaluation.
- The existing journal and durable mutation gate remain the economic authority.
  Per-trigger Astra facts are bounded and appended in a single canonical batch
  before an order can be submitted. Astra usage cost is recorded through the
  existing authoritative operating-cost transition.
- Memory, daily budget state, and the bounded learner checkpoint are included
  in the existing verified engine checkpoint. No raw history, secrets, or
  chain-of-thought are persisted.

## Configuration

Astra is disabled by default. When enabled in production, `ASTRA_API_KEY` is
required by configuration validation and must be supplied through the runtime
environment only. The application does not read keys from files or log them.

The default daily inference budget is `$5`. Fast and Deep calls have separate
bounded concurrency, timeout, output-token, and uncertainty controls. Deep
review is selective and triggered by the strict model output or the configured
uncertainty threshold.

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
`ASTRA_DEEP`, and `ASTRA_BATCH`. Usage, failures, budget blocks, gate outcomes,
deep reviews, reconnect timing, and learning updates are observable through
bounded metrics and canonical report inputs. The existing completion and
final-report flow remains unchanged.

Recovery restores the experiment identity, authority, bounded memory,
calibration/lesson state, and daily cost state from the existing checkpoint;
old Native checkpoints remain readable. Native V1 remains available for the
default mode and historical compatibility, but it is not an economic fallback
when Astra is enabled.

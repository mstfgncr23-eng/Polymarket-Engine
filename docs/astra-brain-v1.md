# Astra Brain V1

Astra is an opt-in economic decision authority for paper experiments. The
default remains `NATIVE_DETERMINISTIC_V1`; setting `ASTRA_ENABLED=true` makes
the experiment manifest record `ASTRA_BRAIN_V1` and routes first-leg economic
decisions through the deterministic feature/gate boundary and the dedicated
OpenAI Responses client.

## Safety boundary

- No Native fallback exists when Astra is enabled. Missing structured output,
  timeout, stale data, journal failure, and failed current-book revalidation
  fail closed.
- Existing unpaired positions use the deterministic hedge path directly. Astra
  is not called for a hedge, and the existing `$25` hedge cap remains intact.
- New first legs use a separate `$12.50` cap. This is enforced in strategy
  sizing and in deterministic risk evaluation.
- The existing journal and durable mutation gate remain the economic authority.
  Per-trigger Astra facts are bounded and appended in a single canonical batch
  before an order can be submitted. Successful provider usage is recorded
  through the existing authoritative operating-cost transition.
- Memory and the bounded learner checkpoint are included in the existing
  verified engine checkpoint. No raw history, secrets, or chain-of-thought are
  persisted.
- Astra has no calendar-day monetary cutoff and no UTC reset. Cost is observed
  and durably accounted from provider usage. Dispatch remains bounded by
  deterministic opportunity admission, first-leg windows, a hard rolling
  maximum of 12 FAST dispatches per hour, a 5-second global interval, a
  60-second per-market cooldown, stage concurrency, timeouts, output-token
  caps, and selective Deep review. Governor evidence is restored from the
  durable journal after restart, so recovery cannot reset the rolling limit.

## Configuration

Astra is disabled by default. When enabled in production, `ASTRA_API_KEY` is
required by configuration validation and must be supplied through the runtime
environment only. The application does not read keys from files or log them.

`ASTRA_MAX_DAILY_COST_USD` is accepted only as a deprecated deployment-compatibility
input and is ignored. It has no default and does not gate or reset inference.
Fast and Deep calls retain separate bounded concurrency, timeout, output-token,
and uncertainty controls. Deep review is selective and triggered by the strict
model output or the configured uncertainty threshold.

The canonical `ASTRA_PROVIDER_DISPATCH_AUTHORIZED` fact is durably committed
before the SDK request may begin. `ASTRA_PROVIDER_DISPATCH_STARTED` is recorded
only after the SDK request operation has actually been created; if request
construction fails, the attempt remains `NOT_DISPATCHED`. The dedicated OpenAI
client uses `maxRetries=0`, so one admitted dispatch produces at most one
provider attempt. `ASTRA_FAST_TIMEOUT_MS` is a maximum provider-operation
deadline: its timer starts when the SDK request begins, successful responses are
processed immediately, and no artificial sleep is introduced. Admission/gating
time is outside that provider-response deadline.

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
`ASTRA_DEEP`, and `ASTRA_BATCH`. Usage, failures, gate outcomes, deep reviews,
reconnect timing, and learning updates are observable through bounded metrics
and canonical report inputs. Historical `ASTRA_BUDGET_BLOCK` facts remain
readable for old reports but new Astra execution does not emit budget blocks.
Provider failures preserve known usage when a response was received; timeout or
abort races are accounted once, and `DISPATCHED_UNKNOWN` remains incomplete
until exact provider completion evidence exists. The final-report flow waits for
in-flight provider calls before close/report/source sealing.

Recovery restores the experiment identity, authority, bounded memory, and
calibration/lesson state from the existing checkpoint. Legacy checkpoints that
contain `astraBudget` remain recoverable, but that field is ignored and never
controls dispatch. Old Native checkpoints remain readable. Native V1 remains
available for the default mode and historical compatibility, but it is not an
economic fallback when Astra is enabled.

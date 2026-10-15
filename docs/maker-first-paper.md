# Maker-First PAPER hybrid

`PAPER_MAKER_FIRST_ENABLED=false` is the default. The base style stays TAKER.
The feature must not be combined with a legacy MAKER base style. Execution
latency stays 150 ms in deployment; configuration requires each quote TTL to
exceed that latency. First-leg TTL defaults to 2,000 ms (500–5,000); the hedge
TTL defaults to 750 ms (250–2,000). The experiment stores these settings.
An active experiment cannot silently change them across process recovery.

## Directional entry

The existing candidate, risk and first-leg admission run first. An accepted
TAKER opportunity may join the actual best bid only if the bid is passive,
on the known tick, meets minimum size and the unchanged $0.65 floor, and has
better fee-adjusted expected economics. Unknown metadata falls back to the
existing path. ROI remains at least 0.015. Limit price is never widened.
Outstanding entry quote remainders count as potential exposure; reservation
and fill revalidate limits inside the durable gate. Cash is actually reserved.
Only one directional first-leg lifecycle can own a market. With the separate
`PAPER_DUAL_SIDED_MAKER_ENABLED=true` flag, the conditional pair planner below
may instead own two fresh quotes. They are not relabeled as already-filled hedges.

## Dual Maker ownership and diagnostics

Dual planning currently runs AFTER directional strategy/risk acceptance. A
directional rejection therefore prevents dual planning too; the feature does
not scan every book for independent market-making opportunities. Both passive
prices and sizes must pass the conditional pair economics planner. The cheap
side uses the explicit dual strategy policy, not the directional $0.65 floor.
Conditional pair profit assumes both fills and is never booked as realized PnL.
The directional `entryAdmission` field is omitted from dual orders rather than
set to `undefined`; JSONB drops undefined object keys, so assigning one would
make the pre-insert hash disagree with the persisted authoritative payload.
Real PostgreSQL lifecycle tests independently re-hash every persisted transition.

The first and second durable reservations atomically carry experiment-scoped
`DUAL_MAKER_PLAN_STARTED` and `DUAL_MAKER_QUOTES_ADMITTED` audits. A known
second-side rejection cancels/release the first reservation, records
`DUAL_MAKER_PLAN_NOT_ADMITTED`, and permits ordinary directional fallback only
if no position exists. A fatal/unknown commit fails closed without further
rollback mutations; recovery repairs committed audit before cancelling quotes.
Entry-close, expiry, current position and ownership are fenced again at commit.

Opposite unfilled quotes receive no directional hedge credit: admission and
serialized entry reservation/fill budget the worst one-sided pending cost per
market. After an actual partial first fill, the opposite allocation is capped
to the true residual while preserving the original public trade evidence.
Remainders are cancelled, not turned into a new directional position. Both
orders get scoped terminal results, including the sibling that becomes a hedge.
All cancellation authorities share the existing durable gate. The first
committed cancel wins; a different later reason cannot create an identity fault.
An entry cancellation preserves alpha cooldown; hedge retries retain their
separate risk-reduction handoff. Guardian/PT policies and 150 ms latency remain.

`dual_maker_plans_total{outcome}` is a process-scoped bounded funnel, with
ATTEMPT as its denominator. Reject evidence snapshots the actual reservation
fence and distinguishes a changed best-bid join from a crossing limit without
relaxing the join policy. Cancellation audit retains bounded per-quote counts
of validated public flow, exact-price SELL flow, eligible volume, activation,
queue and current book. This uses the same owned consumer and writes only at
quote lifecycle boundaries. Counts do not reconstruct previously unretained
trade history and are not authenticated account fills. Restart cancels quotes
whose intervening flow is unknown. TTL/participation are unchanged by diagnostics.

## Maker hedge and emergency priority

During the existing Profit-Trailing experiment, its controller still owns all
second-leg timing: 0.12 target, 0.05 arm, 0.02 retrace. Once its existing
executable taker opportunity is valid, it may try one passive maker hedge per
episode. `makerAttemptedAt` is durable controller state, so a retry/restart
cannot create an endless maker loop. Expiry cancels and releases the quote;
the controller then re-evaluates the normal taker fallback on the current
book. No profit is promised while waiting. Market expiry and safety deadlines
bound quote lifetime further.

Confirmed Guardian protection cancels pending makers before acquiring the
canonical taker owner. Maker fill commit checks current ownership, current
remaining quantity, token epoch, cancellation and settlement. A delayed fill
cannot resurrect a cancelled quote. Actual protective execution is still
TAKER, with its existing safety checks. Partial hedge fills reduce only the
actual residual. Shutdown joins in-flight maker fills within the existing
shutdown budget; unresolved work prevents a clean checkpoint claim.

## Fill evidence and limitations

Hybrid quotes never use book-quantity reductions as fills. The existing owned
WebSocket stream also delivers `last_trade_price` events through its same
ordered shard lane. Only fresh, exact-price, matching-token, matching-epoch
SELL aggressor flow after durable reservation acknowledgement plus simulated
submission latency can fill a BUY quote. Trade size, timestamp and side are
mandatory. A stable fingerprint deduplicates evidence; equal/old source
timestamps are conservatively ignored. Missing optional transaction hashes
remain visible in each fill's `makerEvidence`.

Executed volume first consumes queue ahead, then only 50% of excess can fill
the quote, up to its remainder. Displayed queue growth before the first
eligible flow cannot be put behind our quote. Requotes start a fresh queue.
Public flow supports a conservative PAPER allocation; it does not prove an
authenticated exchange fill or an exact counterfactual queue position.

The normal ledger, durable PAPER_FILL, remaining reservation and order state
are committed atomically. The fill persists the complete public trade proof,
activation time and queue before/after. Maker fees use the actual schedule;
unreceived rebates/rewards are never booked. Fee changes cancel the quote.
The validated execution book is frozen before PostgreSQL awaits, while current
cash/inventory/owner/cancel/settlement fences remain authoritative.

Recovery replays durable fills and repairs their audit first, then cancels
hybrid quote remainders: intervening queue/trade flow cannot be proved across
a disconnect. An orphan CREATED entry becomes a visible terminal rejection.
The previous legacy maker model is retained only for feature-OFF compatibility.

## Evidence

Tests `maker-first-model`, `maker-first-feed`, `maker-first-engine` and the
hybrid Guardian handoff case cover source ordering, latency, partial fills,
cancel races, exposure reservations, expiry, fallback and crash boundaries.
Use the explicit regression whitelist; do not run SHADOW suites. Real PG
fixtures require a dedicated disposable database, never production credentials.

Public field reference: https://docs.polymarket.com/market-data/realtime-data
Fee reference: https://docs.polymarket.com/trading/fees
Historical fee savings are conditional modeled bounds; real profitability
requires all fills, missed opportunities, losses, fees and settlement from the
new PAPER experiment. Do not count a quoted order or ADMITTED as a fill.

## Historical archive failures

Historical dual reservations written with the undefined proof field remain
unchanged. Their stored JSON cannot independently reproduce the producer hash;
archive verification must refuse deletion rather than silently accepting them.
Archive output streams observe errors from creation through completed teardown.
A hash/source failure while writes are outstanding returns the original failure,
removes the incomplete temporary file and cannot crash the Engine with a late
unhandled `ERR_STREAM_DESTROYED`. Separate child-process regressions exercise
both recovery and journal writers after real filesystem backpressure.

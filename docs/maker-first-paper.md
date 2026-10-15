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
Only one directional first-leg lifecycle can own a market. Simultaneous UP
and DOWN fresh quotes are not enabled or relabeled as hedges.

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

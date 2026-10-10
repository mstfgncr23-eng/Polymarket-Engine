# G2H | Paired Market Book Freshness and Reconnect Fence

Date: 2026-10-10
Branch: `refactor/guardian-isolation-atomic`
Parent: `64e57f3`
Status: **G2H IN PROGRESS**, offline source-side book contract only.

## Reason and inspected evidence

The existing Engine `guardianObservation()` requires market synchronization,
fresh paired UP/DOWN books and current token identity before projecting the
Guardian pressure signal. This work preserves that requirement across a future
process boundary without starting any process.

Polymarket's market WebSocket reports `book` as an order-book snapshot and
`price_change` as updates; a socket that is CONNECTED does not prove
a current normalized book. Official schema example:
https://github.com/Polymarket/agent-skills/blob/main/websocket.md
The local `MarketSnapshot.sequence` / G2H `ingressSequence` represents our
normalized intake, **NOT a native globally monotonic Polymarket sequence**.
The upstream hash does not serve as a durable cursor.

## Narrow implementation

- Added `apps/engine/src/guardian-paired-book-channel.ts`: standalone,
  pure, closed input/output state with one active generation and no network,
  database, executor, process or wallet access.
- Caller must supply correctly normalized **complete** books for pinned
  marketId + UP/DOWN token identities and an externally synchronized feed.
  Delta messages cannot be passed as full books without breaking contract.
- A new local generation flushes both previous sides. A disconnect, source
  gap, token rotation, unsync or overload invalidates both and blocks reads.
  Late old-generation frames, duplicate/out-of-order/gap in G2H envelope,
  invalid depth or incorrect identity halt this generation.
- READY is allowed only with BOTH complete books in the current generation,
  valid source/receive timestamps, configured max age and cross-side skew.
  Only latest two books retained, up to 512 levels per side. Input and
  successful read output are defensively copied.
- READY is **observational only**. No hedge/SELL/BID is submitted. Executor
  must recheck current depth/fee/token/position/risk owner before a fill.

## Tests and acceptance

- Seven new closed Vitest tests: pair gating; disconnect/reconnect;
  stale late generation + IPC envelope ordering; malformed/crossed/wrong
  token; source and receive age/skew; 1000-frame bounded retention and
  caller-mutation isolation; distinct eight-market reset.
- `PM_FORENSIC_STAGE=08_current`, focused **7/7 PASS**.
- Related broad regression: **239/239 PASS** across **13 files**.
- Core TypeScript PASS; forensic Engine TypeScript PASS; diff check PASS.

## Known limits and remaining G2H work

- The authenticated producer/transport that stamps connection generations,
  synchronizes both tokens and normalizes full books **does not exist yet**.
  A caller-supplied synchronization bit is not proof of upstream truth.
- No socket adapter, WS heartbeat, actual 8/8 disconnection reproduction,
  real source-driven packet delivery, lag p95/p99, DB cursor pairing or
  independent Guardian process test here. G2I remains not done.
- Any unknown publisher epoch, queued frame provenance, silent source freeze,
  or uncertain durable commit must HALT/reconcile. This module cannot
  authenticate producer identity nor certify executable full-size fill.
- G2H still needs trusted writer incarnation/lease, durable ACK and recovery
  supervisor, strict cross-channel source freshness/lag, bounded IPC and
  source-backpressure under realistic rate. No production cutover authorized.

## Rollback

Revert this single commit if needed; keep prior G2H checkpoint and cursor
commits. No production rollback applies because this change is local only.

NO SHADOW / NO DUAL WRITER / PAPER ONLY / NO VPS / NO PUSH / NO DEPLOY.

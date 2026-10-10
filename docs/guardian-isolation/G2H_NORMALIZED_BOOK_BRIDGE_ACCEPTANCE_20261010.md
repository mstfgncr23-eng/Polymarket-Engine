# G2H | Bridge to Existing Market Normalizer Sync Epoch

Date: 2026-10-10. Parent: `764e865`.
Gate: G2H remains **IN PROGRESS**, no market feed connected in tests.

## Proven existing producer architecture

The checked-in `packages/market-data/src/polymarket.ts` already maintains
per-token sync state and epoch through `isTokenSynchronized()`,
`tokenEpoch()`, and canonical complete `MarketSnapshot` emission.
`commitFullBook()` parses/stages full books. `commitPriceChange()`
stages incremental changes and publishes complete normalized snapshots.
`onGap()` and reconnect reset token trust. These are real production
source-code interfaces; G2H does NOT invent a second raw WS normalizer.

Official Polymarket market WS event types:
https://github.com/Polymarket/agent-skills/blob/main/websocket.md

## Narrow design

- `guardian-normalized-book-bridge.ts` receives ONLY already-trusted
  normalized complete `MarketSnapshot` objects from the existing adapter.
- Before admitting a frame, independently probes current synchronization
  and epoch for UP and DOWN canonical token IDs. Token identity drift,
  either token epoch advancing, disconnect, gap or overload invalidates
  BOTH cached books and forces a fresh, bounded pair.
- Local trusted ingress sequence and generation are assigned by the bridge,
  not inferred from Polymarket timestamps/hash.
- Does not open a second market socket, start a Guardian process, enqueue
  book history or submit any order.

## Test evidence and gaps

Six new deterministic local tests (pair, disconnect/reconnect,
one-sided epoch rotation, unsync/token mismatch, overload and malformed
normalized depth): **6/6 PASS**.

The bridge is NOT YET WIRED into Engine's real adapter callbacks; this
would require proving the callback sequence and ownership contract before
cutover. There is no actual live-source network test or WS reconnect
reproduction here. Real producer's signed epoch/manifest publication and
independent trust issuance also remain unresolved. G2H is not declared
complete on the strength of unit tests.

NO SHADOW / PAPER ONLY / NO VPS / NO PUSH / NO DEPLOY / NO MERGE.

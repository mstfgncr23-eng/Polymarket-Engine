# Recorded Public Market Replay: Reverse Regression Round 5

Date: 2026-10-09. Agreed scope: replay precisely the same newly captured public reference and market WebSocket traffic against each deployed-source historical stage, latest to oldest.

## Capture provenance
- Captured using a bounded unprivileged Node container on the authorized VPS: Binance public aggTrade, Polymarket public Gamma API and CLOB market WebSocket.
- Production engine/database/credentials were not accessed or changed. Capture container exited.
- Dataset: research/reverse_regression/fixtures/public_market_capture_four_markets_20261009.jsonl
- SHA-256: 7ea27f0c585d2da70ee7f9a64b2203c7b5df3e741b905d1faf4818766466ed19
- Rows: 304; reference frames: 231; CLOB frames: 63.
- Reference assets: BTC, ETH, SOL, XRP, DOGE, BNB, HYPE, ZEC. Four Polymarket market pairs / eight public tokens.
- Message ordering and capture offsets preserved. Replay uses relative offset timing at 1x speed and opens ZERO real network connections.
- Capture bounded to <=60 seconds, <=2500 records and <=3MiB.

## Implementation
The real ReferencePriceFeedAdapter, BoundedClobMarketTransport and PolymarketMarketDataAdapter were exercised with PlaybackSocket and four independent CLOB transports. Each stage uses the same raw frame bytes and digest. It waits for all reference callbacks and bounded CLOB queues to finish before comparing.
This is ADAPTER-LEVEL replay only. It does not run a production-equivalent EngineService with Guardian risk arbitration and durable journal.

## Reverse test results
| Stage | Reference ticks | CLOB snapshots | Markets | Max concurrent snapshot callbacks | Callback errors | Event-loop p99 ms |
|---|---:|---:|---:|---:|---:|---:|
| 08_current | 231/231 | 132 | 4 | 4 | 0 | 44.2 |
| 07_cpu_v3 | 231/231 | 132 | 4 | 4 | 0 | 37.55 |
| 06_cpu_v2 | 231/231 | 132 | 4 | 4 | 0 | 40.11 |
| 05_cpu_v1 | 231/231 | 132 | 4 | 4 | 0 | 41.94 |
| 03_identity | 231/231 | 132 | 4 | 4 | 0 | 17.32 |
| 02_step3 | 231/231 | 132 | 4 | 4 | 0 | 18.51 |
| 01_step2 | 231/231 | 132 | 4 | 4 | 0 | 16.97 |
| 00_safe | 231/231 | 132 | 4 | 1 | 0 | 17.19 |

All 8 versions PASS this recorded input. Reconstructed source snapshots verified, then current snapshot restored with zero mismatches.

## Artifact corrected
An initial 15x accelerated replay seemed to show fewer Guardian65 callbacks than current, but it had too little time to drain the old globally serial callback queue. After switching to 1x and requiring bounded queues to drain, both variants delivered 132 CLOB callbacks, as did the other six versions. The initial count difference was a TEST ARTIFACT, not an established code regression.

## Results and limitations
- All eight stages accepted 231 reference ticks and delivered 132 CLOB snapshot callbacks across four market pairs.
- Guardian65 used one active callback lane; post-Step2 versions had up to four, as designed.
- The differences in single-run event-loop p99 should not be interpreted as code-caused CPU performance changes without repeated and controlled experiments.
- Original incident raw WS frames are unavailable; newly captured sample did not include any synchronized reference closure.
- Therefore NO software update has been identified as the cause of the historical mass reference disconnect by this replay.

## Reproducible command
From Windows reverse regression worktree:
    python research/reverse_regression/run_public_replay.py

This runner verifies input byte hash, applies source snapshot stages in reverse order and restores 08_current in finally. Its stage JSON logs are ignored under research/reverse_regression/runs/public_capture_replay.
Opt-in test runs use PM_REPLAY_ENABLE=1 to avoid slowing down normal CI test execution.

## Agreed plan boundary
The approved recorded-input replay is implemented and documented. Historical fault-injection or full Guardian+Postgres replay would be a distinct extension and will not be performed before informing the user and obtaining approval.

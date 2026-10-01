# TEST_CONTRACT traceability for 24h forward shadow

Source authority: user-supplied Two-Stage Guardian TEST_CONTRACT.

| Contract clause | Forward implementation | Test / evidence | Status |
|---|---|---|---|
| NORMAL_HEDGE stays first priority | `NORMAL_HEDGE_PRIORITY` guard using the existing 0.015 locked-edge boundary before any helper SELL | harness pins constant and marker | PASS |
| Existing PR3 remains | PAPER engine is unchanged; sidecar never submits/cancels orders | read-only harness forbids order client/operator endpoints | PASS |
| Trigger loss = $0.35 | `TRIGGER_LOSS = Decimal("0.35")` | pinned assertion | PASS |
| PR3 pair-lock cap = $0.60 | `PAIR_LOCK_CAP = Decimal("0.60")` | pinned assertion | PASS |
| Full-size opposite lock within cap suppresses helper | full opposite book walk + fee/min-lot + cap test | observer logic | PASS |
| Over-cap / unavailable full opposite depth hands to Protective | failure route `PR3_CAP_OR_DEPTH_FAIL` | observer logic | PASS |
| Rescue execution error hands to Protective | same-episode `RESCUE_EXECUTION_ERROR` tracking | observer logic | PASS |
| Protective action is full held-side SELL only | full bid-ladder walk, max-order-size cap, min-lot + fee metadata; action requires `full` | observer logic | PASS |
| No partial-fill success invented | partial or missing executable SELL returns `HELPER_NOT_EXECUTABLE` and waits | observer logic | PASS |
| Missing latency state is UNKNOWN | delayed book must be locally received at/after target and within +25 ms; full SELL must be executable | observer latency logic | PASS |
| Latencies 25/50/100/150/250/500 ms | fixed `LATENCY_MS` tuple | observer output | PASS |
| PAPER only / no LIVE mutation | read-only engine endpoint + public CLOB WS observer | CI read-only test | PASS |

Important scope note: the historical contract's GO/NO-GO gate did not pass (`PnL > -5` and `worst > -1` failed). The 24h forward soak is therefore an explicit operator override for evidence collection, not a claim that the historical GO gate passed.

# Round 6: Isolated real-feed soak attempt

## Approved boundaries
- Test container: pmf-reg-current-1791577490, exact current deployed image no-source-seal-v1-20261008.
- Real Gamma, CLOB and Binance public feeds; 40 qualified markets; PAPER; memory journal.
- Separate forensic Docker network, no production credentials or database, no wallet, no host ports.
- CPU1 affinity + soft CPU shares 128, no hard CPU quota; 512MiB memory.
- Start 2026-10-09 20:24:50 UTC, intended 900sec max, stop on >=7 unique reference closes within 1000ms or host safety.

## Observed outcome
- HOST_LOAD_GUARD after 152.83s. Container was stopped successfully.
- Three single reference closes, zero 7+ mass closes and zero CLOB stale indications.
- First five minutes NOT completed, so absence of fault is inconclusive.

### Reference-close events
- at_ms=1791577502250 asset=DOGE message=REFERENCE_CLOSE:kind=websocket_error:code=1011:wasClean=false:abort=false:reason=websocket error
- at_ms=1791577528527 asset=HYPE message=REFERENCE_CLOSE:kind=stale_feed:code=4000:wasClean=false:abort=false:reason=stale feed
- at_ms=1791577622398 asset=HYPE message=REFERENCE_CLOSE:kind=stale_feed:code=4000:wasClean=false:abort=false:reason=stale feed

## Telemetry samples immediately before stop
- Only 7 samples, 2026-10-09T20:26:59.378811+00:00 to 2026-10-09T20:27:27.642044+00:00 UTC. Not full-session event-loop evidence.
- Event-loop lag p99 range 129.37 to 179.96 milliseconds.
- Event-loop utilization range 0.713 to 0.764.
- Highest sampled host one-minute load average 2.78 on 2 vCPUs.
- Cgroup CPU quota disabled; CPU throttled periods=0. Final CPU use 95.976s over 152.83s wall time.
- The runner stops when host load exceeds 2.2 for three consecutive observations.
- Isolated engine health early on: connected WebSocket, 40 markets, NORMAL, memory DB UP. HTTP scrapes began failing around load guard.

## Same-window production cross-check
- In 20:24:40-20:27:40 UTC, five single reference errors (DOGE, HYPE); zero synchronized 7+ reference-close clusters.
- HYPE production stale close at 20:27:02.264Z and forensic stale close at 20:27:02.398Z: 134ms separation. This does NOT establish root cause.
- Production last read-only ready: database UP, Polymarket WS CONNECTED, qualified 40, pre-existing FAIL_CLOSED.
- No production container restart, database write, reset or strategy change.

## Private raw data (excluded from Git)
- VPS: /home/ubuntu/pm-forensic-20261009/pmf-reg-current-1791577490.json
- VPS: /home/ubuntu/pm-forensic-20261009/pmf-reg-current-1791577490-metrics-samples.jsonl
- Local: research/reverse_regression/evidence/private/ matching files.

## Honest conclusion and next choice
- Synchronized 7/8 reference disconnect NOT reproduced in the 153sec window.
- The shared VPS could not sustain a second full engine for five minutes without hitting the agreed host load safety guard.
- No safety guard was disabled. No alternate experiment was silently launched.
- Proposed only, not executed: low-overhead read-only observation of existing production reference/CLOB logs and metrics, alongside a separate 8-asset public-only reference listener. This changes test design and requires user approval.

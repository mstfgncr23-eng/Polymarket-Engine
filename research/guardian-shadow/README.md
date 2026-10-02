# Guardian 24-hour forward shadow

This harness observes the locked two-stage Guardian policy without changing trading authority.

## Safety

- PAPER only.
- No private key.
- No live order client.
- Engine access is read-only through /events, /api/dashboard and /api/shadow/guardian-state.
- Market data comes from the public Polymarket CLOB WebSocket.
- Raw CLOB frames are persisted only while the corresponding market has an open unpaired PAPER position.
- Shadow files live outside Postgres under /data/guardian-shadow.

## Files

- LOCKED_POLICY.md: frozen policy contract.
- guardian_shadow_24h.py: real-time observer and latency sampler.
- analyze_shadow.py: post-run observation summary.
- requirements.txt: Python dependency pin.

## Runtime prerequisites

The PAPER engine must be built from this analysis branch, ready on 127.0.0.1:8080, and started with EXPERIMENT_DURATION_HOURS=24. The observer should start before the experiment so its direct CLOB books are warm before the first PAPER leg.

Final strategy-level PnL remains an offline proof step after the soak. It must combine the canonical experiment archive with the raw shadow files. Missing execution state must remain UNKNOWN.

Evidence integrity is fail-closed: active-experiment authority/SSE gaps, a position that activates before trusted direct books are available, an unbound rescue-failure episode, or non-zero final economic residue prevents a `COMPLETE` seal.

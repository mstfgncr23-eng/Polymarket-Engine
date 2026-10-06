# Engine Guardian and passive recording

Engine Guardian is the sole production Guardian policy and executes real PAPER
taker SELL orders through shared durable risk-reduction ownership. The Python
`guardian_shadow_24h.py` filename is retained for tooling compatibility; its former
counterfactual policy, eligibility, execution economics, failure tracking, and
shadow-close state have been deleted.

The Python script now records unmodified read-only engine runtime responses, SSE
lines, public CLOB frames for engine-reported unpaired markets, request latency,
and runtime diagnostics. It never evaluates Guardian eligibility, submits engine
mutations/orders, or computes counterfactual fills or PnL. It requires a read token
and a PAPER runtime. The prior command's engine URL, read token, source SHA,
output directory, and duration arguments remain; settlement-grace and historical
shadow completeness semantics are retired. No recorder has been started by this PR.

`LOCKED_POLICY.md`, the exact `TEST_CONTRACT.md` bytes, and `analyze_shadow.py`
remain historical research artifacts for already-recorded shadow runs. The old
analyzer is not an analyzer for the new passive recording format and does not
constitute a runtime policy authority. Actual executions and PnL must be read from
the engine journal and experiment archive.

See `ENGINE_GUARDIAN.md` for the production flow, lifecycle, test-scope changes,
and operating limits.

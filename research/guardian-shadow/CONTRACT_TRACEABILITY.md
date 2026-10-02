# TEST_CONTRACT traceability for 24h forward shadow

Source authority: frozen Two-Stage Guardian TEST_CONTRACT.

This table is a **coverage map**, not a CI/soak acceptance verdict. A row marked `COVERED` means the clause has an implementation path and a named test/evidence path. It becomes accepted only when the exact immutable SHA passes the full required checks.

| Contract clause | Forward implementation | Test / evidence | Coverage |
|---|---|---|---|
| NORMAL_HEDGE stays first priority | canonical engine `normalHedgeAdmissible` is recomputed from the same runtime decision basis immediately before any helper SELL | TS authority test compares read-only result with canonical normal decision and proves observation does not mutate ledger/orders/PR3 counters | COVERED |
| Existing PR3 remains | PAPER engine trading path is unchanged; sidecar never submits/cancels orders | read-only endpoint test + harness source-safety assertions | COVERED |
| Trigger loss = $0.35 | `TRIGGER_LOSS = Decimal("0.35")` and canonical-equivalent realizable mark-loss math | Python policy tests + frozen config preflight | COVERED |
| PR3 pair-lock cap = $0.60 | `PAIR_LOCK_CAP = Decimal("0.60")`; full opposite ladder walk + fee/min-lot metadata | Python policy tests + frozen config preflight | COVERED |
| Full-size opposite lock within cap suppresses helper | full opposite book walk + fee/min-lot + cap test | `test_affordable_full_pr3_blocks_helper` | COVERED |
| Over-cap / unavailable full opposite depth hands to Protective | failure route `PR3_CAP_OR_DEPTH_FAIL` | `test_over_cap_full_sell_records_helper` | COVERED |
| Rescue execution failure hands to Protective | rescue failure is bound to the exact `market + unhedgedStartedAt` episode; no cross-episode carryover | rescue-error helper test + cross-episode negative test | COVERED |
| Protective action is full held-side SELL only | full bid-ladder walk, max-order-size cap, min-lot + fee metadata; action requires `full` | Python policy tests | COVERED |
| No partial-fill success invented | partial or missing executable SELL returns `HELPER_NOT_EXECUTABLE` and waits | `test_partial_held_depth_never_invents_full_sell` | COVERED |
| Missing latency state is UNKNOWN | delayed book must be newly received at/after target and within +25 ms; full SELL must be executable | latency KNOWN/UNKNOWN unit test | COVERED |
| Latencies 25/50/100/150/250/500 ms | fixed `LATENCY_MS` tuple | observer output cardinality + analyzer | COVERED |
| PAPER only / no LIVE mutation | authenticated read-only engine endpoint + public CLOB WS observer | TS side-effect test + source-safety assertions | COVERED |
| Evidence gap during relevant exposure is surfaced | active experiment state/SSE failures are critical; a newly activated position without trusted direct books invalidates the run | Python activation-integrity test + sealed integrity events | COVERED |
| COMPLETE requires zero economic residue | final experiment unresolved count, dashboard positions and reserved cash must all prove zero | pure completion-residue unit test + analyzer defense-in-depth | COVERED |

Historical GO/NO-GO remains unchanged: the historical policy did **not** pass the original economic gate. The user-approved 24h forward run is an evidence-collection override, not a claim that the historical gate passed.

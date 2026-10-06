# Engine Guardian PAPER execution

The required base is `8f0abacc936e6195f10f418867475a99fc8049ca` (merged PR87).
No threshold changes, migrations, live execution, deployment, or soak startup
are included.

## Authority and priority

Before the existing duration/pre-expiry deadline, `maybeProtectiveDeRisk` runs
normal profitable hedge priority through the existing PR3 path, then the full
catastrophic opposite BUY pair lock, then Engine Guardian's full held-side SELL,
then waits. A normal hedge or healthy hedge maker always prevents Guardian.
A full executable opposite lock within the existing loss cap also prevents it,
unless an actual rescue execution failed for this exact residual episode.
The age/loss/cap configuration remains 10000 ms / $0.35 / $0.60.

Guardian alone is exempt from the global `ABNORMAL_EXECUTION_ERRORS` hourly
counter veto: unrelated historical errors cannot trap an emergency full SELL.
Normal, PR3 and old protective intents keep that veto. Database/journal/integrity,
fresh held book, exact episode/token/residual, settlement and full execution
checks remain; an actual Guardian simulator failure still records durable
`RISK_REDUCTION_FAILED` with `executionFailure: true` and PAPER_EXECUTION_ERROR,
releases ownership, and leaves residual open without a fabricated fill.

`evaluateGuardian` is the only Guardian implementation. It checks current
position, episode, held token, synchronized/nonfuture/fresh books, age/loss,
canonical normal admissibility, maker/taker ownership, full opposite depth and
fee-inclusive cap, and the canonical protective risk admission for a full SELL.
An absent/stale opposite book is unknown, not proof of insufficient depth.
Full SELL must meet depth, minimum size, metadata, and existing order-size/hard
risk bounds; quantity is never reduced to pass admission.

`admitRiskReduction` repeats those checks inside PR87's serialized gate against
the original order's quantity, token and SELL limit. A callback that changed the
priority while Guardian awaited admission gets one same-pass PR3/normal recheck,
so yielding callbacks do not lose the state update.
If an admitted/resumed order is released by an execution or deadline fence
without consuming the residual, the shared call chain revalidates current
Guardian/deadline authority in the same pass. A partial successful reduction
does not run that failure handoff. This preserves the old deadline fallback
without requiring an additional book update after callbacks yielded to an owner.

## Shared durable lifecycle

The distinct `GUARDIAN_PROTECTIVE_SELL` intent uses the existing ownership
schema and `RISK_REDUCTION_ADMITTED` transition. Its admission includes the
stable `PAPER_ORDER_CREATED` audit. Existing `submitPaperOrder`, latency,
`executePaperTaker`, and `applyFill` produce the real execution. The fill uses
`risk-reduction-fill:<order-id>`; its ledger mutation and owner release are
atomic. Hard safety, settlement, token, episode, exact residual and full quantity
are rechecked before the fill commit. A pending durable owner keeps authority
through the later deadline phase; a normal opportunity that appears after
durable admission does not acquire that same residual.

Execution failures use `RISK_REDUCTION_FAILED`. Shared lifecycle evidence stores
the latest actual failed taker ownership/reason per open episode, including
market, `unhedgedStartedAt`, intent, order identity, and admitted residual.
Only actual simulator/execution failures are marked `executionFailure: true`;
state/cash fences, supersession, settlement, and recovery aborts are not failures
that authorize Guardian. Evidence is checkpointed/cloned with shared state and
survives transition compaction. Fills/residual changes, a new episode admission,
and settlement clear obsolete evidence. Older PR87 records without this explicit
marker remain fail-closed and cannot authorize the failure override.

Guardian has no separate pending-order or restart state. Restart recovers the
original shared owner/order, waits for fresh synchronized books, and executes
that identity once. A committed fill is replayed and missing derived audits are
repaired without another SELL. Delayed fills after owner release are rejected.

## Deadline safety and observability

The old PR82 duration/pre-expiry A/B salvage fallback remains. It may still
choose a partial protective SELL or opposite BUY in that different phase.
Guardian supplies the previously missing pre-deadline full SELL after PR3
cap/depth failure or exact-episode execution failure. The paths share one
durable owner, per-market flight, residual/episode checks, and settlement
authority, so they cannot double consume the episode.

Existing journal/runtime events identify Guardian evaluation, eligibility,
cap/depth/failure reason, held token, full quantity, order identity, admission,
fill, completion/failure and blocked races. Fill-linked POSITION_UPDATED and
PNL_SNAPSHOT audit the actual result, including the terminal zero position when
PaperLedger removes an empty position. Dashboard changes add only the intent
label in the existing event summary. Python is a passive recorder; the historical
shadow contract and analyzer remain archives for old recordings.

## Existing test scope changes

All three modified PR3 tests are **integrated runtime behavior tests**, because
they emit adapter callbacks into EngineService, execute PAPER orders, and assert
ledger state. No isolated PR3 unit fixture was changed. No original held-side
depth, price, opposite liquidity, fee schedule, or timeout was reduced.

| Original test name | Original purpose | Exact fixture change / expected behavior | Coverage preservation |
| --- | --- | --- | --- |
| Test C (4962594 false-rescue control): locked > cap -> NO rescue, then normal hedge path | Over-cap PR3 must not execute; a later profitable normal hedge can execute | Original cap-failure fixture is unchanged. It now asserts one real full Guardian SELL and zero residual. A second observed first-leg episode with the same original entry price, quantity and fees is added at 21s for the unchanged normal-hedge book/reference phase. | Keeps the no-rescue cap assertion and original normal fill quantity/price assertions; adds full SELL identity, terminal state and no duplicate Guardian execution. |
| Test E: partial opposite depth -> no early rescue; next fresh snapshot retries full | Partial opposite depth cannot fabricate PR3; later full depth admits the whole residual | Original partial-depth and full held-SELL fixture is unchanged and now asserts one full Guardian SELL. The existing improved opposite ladder is emitted after closure and must not rescue the old episode. A second first-leg episode with the same entry/quantity/fees is added at 21s, then fresh original full-depth ladders at 31s satisfy the unchanged 10s age gate. | Keeps zero rescue on partial depth and successful full PR3 quantity for a later real episode; adds actual full SELL, closure, and rejection of rescue against the already closed episode. |
| Test G: latency deterioration -> final-fence rejects the actual fill | PR3's actual fee-inclusive loss cap must reject a deteriorated latency fill; residual remains available for later protection | All original ladders, callback ordering, price, depth, fee, latency and timeout remain. Adds an explicit wait for durable Guardian PAPER_ORDER_CREATED before the later improved opposite callback, anchoring that race during Guardian latency. Expected later action changes from PR3 retry to one full real Guardian SELL. | All original final-fence/cap rejection, no-rescue-fill, durable fill-count and still-open-residual assertions remain. Adds full Guardian identity/quantity/price, zero terminal exposure, and no later PR3 consumption. PR3 improvement before Guardian admission is separately rejected by canonical admission race tests; after-admission ownership is now deterministic. |

The 20-test PR3 pre-merge correctness suite remains unchanged, including its
hot-path counts and normal-admission races. The new 46-test Engine Guardian suite covers
full UP/DOWN SELL economics, cap/depth/exact-failure integration, normal/maker/PR3
priority, serialized races, no partial fabrication, duplication, latency changes,
recovery crash boundaries, settlement and old deadline ownership.
The review correction adds 10 cases: cap/depth SELL despite excess generic
errors, three hard authority faults, actual durable Guardian execution failure,
and four other intents retaining their global error veto. The actual failure
case deterministically holds the post-admission order audit, changes full depth,
then releases execution; it does not rely on winning a wall-time race.

Python policy tests are intentionally replaced by passive recording tests. The
canonical policy and real execution assertions now live in the engine suite;
the archived TEST_CONTRACT byte hash and historical analyzer checks stay intact.

### Other existing test changes

These are isolated recorder/harness tests, not PR3 runtime fixtures. The old
Python implementation and its mock policy fixture are deleted, as requested;
none is changed to make Guardian ineligible. Python policy assertions are
replaced by real engine policy/execution coverage, rather than retained against
a second authority. The following inventory includes every retired Python test.

| Original test name | Original purpose | Exact fixture/change | Coverage consequence |
| --- | --- | --- | --- |
| `python policy/unit tests pass without network access` | Run Python tests without external services | Same local subprocess command; renamed to `passive recorder unit tests pass without network access` and runs the four recorder tests | Retains offline execution and exit-status assertion; policy tests move to engine execution coverage below |
| `observer stays read-only` | Assert observer read-only access and duplicated locked policy | No runtime fixture; replaces static duplicated-policy strings with passive format/authority/GET allowlist and absence of eligibility/math/order APIs | Keeps no order client, create/post/operator access checks; adds removal of independent policy; exact contract hash and analyzer UNKNOWN assertions are unchanged separate tests |
| `test_over_cap_full_sell_records_helper` | Over-cap lock records a counterfactual full SELL | Deletes old `GuardianShadow` mock fixture/test | UP/DOWN meteor engine tests retain cap failure and full depth, now assert real SELL, cash, fees, PnL and zero residual |
| `test_canonical_normal_priority_blocks_helper` | Canonical normal hedge suppresses helper | Deletes old mock fixture/test | `yields to canonical normal profitable hedge` and normal admission race assert production priority |
| `test_affordable_full_pr3_blocks_helper` | Full affordable PR3 suppresses helper | Deletes old mock fixture/test | `yields to full affordable PR3 pair lock` and PR3 economics/owner races assert production priority |
| `test_partial_held_depth_never_invents_full_sell` | Partial held depth cannot create full helper SELL | Deletes old mock fixture/test | Engine partial held-book test retains insufficient depth and asserts no Guardian order/fill |
| `test_recorded_rescue_error_hands_to_sell_even_if_lock_affordable` | Same-episode rescue error permits counterfactual SELL | Deletes old mock fixture/test | Real rescue execution failure test retains affordable full PR3 and full SELL, adds durable evidence/checkpoint recovery and actual zero residual |
| `test_rescue_failure_never_carries_into_a_new_episode` | Old episode error cannot authorize helper | Deletes old mock fixture/test | Engine old-episode failure test and episode-change admission race verify exact durable identity |
| `test_authority_poll_replays_newest_first_risk_feed_chronologically` | Reconstruct failure binding from observer feed order | Deletes old mock event-feed fixture/test | Observer inference is intentionally retired; canonical failure binding is tested from actual durable rescue failure and compacted recovery. Raw runtime evidence fidelity is tested without reordering/inference |
| `test_position_activation_without_trusted_books_invalidates_evidence` | Invalidate shadow evidence after untrusted activation | Deletes old mock fixture/test | Shadow completeness policy is retired; production stale/unsynchronized held-book tests prevent execution; passive recording scope test follows engine positions |
| `test_latency_known_requires_new_full_sell_state_inside_25ms_window` | Counterfactual latency samples require contemporaneous book evidence | Deletes old mock monotonic-time/latency fixture/test | Counterfactual 25ms KNOWN/UNKNOWN sampling is intentionally removed, not claimed equivalent. Engine tests instead fence actual latency depth/residual/settlement changes; passive recorder preserves raw timestamps/latency, historical analyzer remains unchanged |
| `test_completion_residue_gate_requires_zero_positions_and_reserved_cash` | Shadow completion rejects remaining positions/reservations | Deletes old shadow completion fixture/test | Shadow run-completeness gate is intentionally removed; production ledger zero-residual and pending-owner/restart/settlement assertions remain in engine tests. Historical analyzer still rejects incomplete recordings |
| `test_walk_skips_zero_price_and_enforces_notional_cap` | Test Python duplicate depth walk/notional bound | Deletes Python book/math fixture/test | Python execution math is removed; production full-depth and no-partial Guardian tests use canonical execution, plus unchanged core depth/VWAP and risk-limit tests |
| `test_fee_matches_taker_formula_and_rounding` | Test Python duplicate fee rounding | Deletes Python fee fixture/test | Python fee math is removed; UP/DOWN engine meteor tests assert actual executed fees/cash/PnL; unchanged `uses the market fee curve and taker-only maker rule` covers canonical fees |

The new passive tests cover raw JSONL fidelity, engine-derived subscription
scope, allowlisted GET/auth and manifest authority, and rejection of non-PAPER
runtime. They do not claim to preserve retired counterfactual business logic.

## Validation

Final focused run: `pnpm test -- tests/engine-guardian-paper.test.ts
tests/durable-risk-reduction-ownership.test.ts tests/early-catastrophic-pair-lock.test.ts
tests/pr3-premerge-correctness.test.ts`: all 95 tests pass
(Guardian 46, existing early PR3 13, PR87 16, PR3 pre-merge 20).
Engine typecheck, engine build and synthetic CLI smoke pass.
Final full `pnpm test`, default worker settings: 90 files pass, 1 fails, 2 skipped;
1082 tests pass, 1 fails, 10 skipped. Only the unchanged PARITY test times out.
No worker override or rerun was used to hide this failure. Repeated exact-test
BASE/HEAD measurements and captured root cause are documented in
[PARITY_TRIAGE.md](PARITY_TRIAGE.md) with every run's IDs in
[PARITY_TRIAGE_RUNS.csv](PARITY_TRIAGE_RUNS.csv). No existing test timeout, skip,
assertion or liquidity fixture was adjusted to obtain a passing suite.

## Limits

This validates PAPER mechanics, not future market profitability. Insufficient
full SELL depth, unknown metadata, order-size bounds, unhealthy journal/database,
hard safety vetoes or expiry leave exposure open for retry/settlement. No live
order path or new execution simulator exists. A real 24h PAPER run still requires
review, deployment of this engine revision and operator startup; none is done here.

The unchanged PARITY test has now been triaged with identical commands and
instrumentation on the required BASE and reviewed HEAD: 14/30 BASE timeouts and
11/30 HEAD timeouts. Every failure captures pending early PR3 at deadline
advancement, its existing deadline-fence release, then the old equal-salvage
protective SELL with zero residual while the test waits for RESCUE. Every pass
completed PR3 before advancement and did not test deadline fallback at all.
This establishes pre-existing scheduling sensitivity and the test's incorrect
deadline-action assumption, with no observed HEAD failure-rate increase in this
finite sample. It does not prove population-rate equivalence. Guardian never
executes in these parity runs. No fixture, assertion, timeout, skip or deadline
tie rule was changed. The full suite still has this known test blocker; its
separate redesign/review remains before an unconditional green validation claim
or soak readiness. The deterministic handoff test verifies the correct existing
SELL behavior without duplicates.

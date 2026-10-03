# Deterministic PR3 deadline lifecycle coverage

Required base: `analysis/guardian-shadow-24h` at
`ffc8dc9cc70158d4c4fa123114d1137962212f96`, verified with a clean worktree
before implementation. This is a test-only correction; no production files,
configuration, synchronization mechanism or execution policy change.

## Original test and preserved economics

Replaces `PARITY: same state yields equal canonical RESCUE decision pre-deadline
(early) and at-deadline (PR82 B fallback)` in the PR3 pre-merge suite.
Its stated purpose was comparing full-residual RESCUE behavior across phases,
but it initialized both engines before deadline and waited for only one engine's
fill before advancing the other clock. It neither forced a pending action nor
exercised deadline fallback on successful runs.

The committed [triage report](PARITY_TRIAGE.md) and
[per-run measurements](PARITY_TRIAGE_RUNS.csv) remain unchanged: 14/30 BASE
and 11/30 reviewed HEAD timeouts. Failed runs correctly released pending EARLY
PR3, chose existing tie-preferred protective SELL and reached zero residual;
the test incorrectly kept waiting for RESCUE. Passing runs had already completed
PR3 before advancing. The new tests explicitly replace that unsupported
cross-phase RESCUE-equivalence assertion with coverage of both real states.

Both tests retain the original economic fixture:

| Fact | Preserved value |
| --- | --- |
| PAPER balance | $12 |
| First held position | 5 UP at $0.80; zero fees |
| Pre-deadline age | 20 seconds |
| Held UP book | $0.70 bid, depth 1000 |
| Opposite DOWN book | $0.24 bid / $0.30 ask, depth 1000 |
| Fee schedule | Original `ERROR_SCHEDULE`, rate zero |
| Window/deadline | Original one-hour window and `eligibilityMs(END_OFFSET_MS) + 1` |
| Full residual | 5 shares |
| Deadline guaranteed salvage | SELL $3.50 = opposite BUY $3.50 |
| Tie policy | Existing `A_PREFERRED_TIE`, held SELL |

No liquidity, price, fee, threshold, latency, timeout or worker setting changed.
The existing 6000ms predicate-wait timeout and suite timeout remain intact.
No arbitrary sleep was added. Local helpers only construct these same books
and inspect existing owner/flight state; no production hook was added.

## Test A: completed before deadline

`PARITY-LIFECYCLE-A: completed EARLY PR3 leaves no residual for deadline protection`

Real adapter callbacks start EARLY PR3. The test explicitly waits for its real
RESCUE fill and for owner/flight release before advancing the fake clock.
It asserts one order matched to the fill ID, BUY DOWN on the canonical token,
full quantity 5 at $0.30 with zero fee, and the EARLY authority reason.
Ledger UP=5/DOWN=5/paired=5/residual=0 proves economic neutralization.
The paired position can remain in the ledger for later settlement; closing
the residual does not fabricate an inventory removal.

After advancing beyond deadline and emitting fresh unchanged ladders, it asserts
residual remains zero, still exactly one risk-reduction order/fill, no protective
or Guardian fill/order, no deadline risk evaluation, one durable admission and
no failure. This replaces scheduler luck with completion as a prerequisite.

## Test B: durable pending boundary before deadline

`PARITY-LIFECYCLE-B: durably pending EARLY PR3 hands the deadline tie to one held SELL`

The existing `MemoryEventJournal.setAppendInFlightHoldForTest` holds only
`PAPER_ORDER_CREATED`. PR87 commits/applies admission and installs its owner
before appending that derived order audit. Holding the audit prevents
`submitPaperOrder` from entering execution, without a new production delay.
The test waits until the durable admission, current owner and in-flight append
are all observable. It asserts the exact admitted order ID, PENDING/ADMITTED
state, full quantity 5, opposite canonical token, EARLY reason, untouched
residual 5 and no risk-reduction fill or visible held order audit.

While still held, it advances the same fake deadline and provides both fresh
original ladders. The same owner and no fill are rechecked before release.
Then it releases the append; no new tick/book is emitted before the expected
fallback fill. The original engine lifecycle must perform the handoff.

Assertions prove:

- Exactly one matching durable EARLY failure/release with
  `EARLY_CATASTROPHIC_DEADLINE_OWNERSHIP` and `executionFailure: false`.
- State-change error evidence and zero generic execution errors; no RESCUE fill.
- Exactly one real protective SELL on held `btc-mkt-up`, all 5 shares at $0.70,
  zero fee/slippage, deadline authority, both available $3.50 salvages and
  unchanged `A_PREFERRED_TIE` selection.
- Exactly two risk-reduction orders (released PR3 + protective), one
  risk-reduction fill, two durable admissions and two durable economic fills
  overall (original first leg + SELL), with stable matching order IDs.
- No Guardian order, no remaining owner and no ledger position/residual.
- Actual cash $11.50, SELL cash increase $3.50, realized/net trading PnL −$0.50,
  zero fees/slippage, consistent with 5 × ($0.70 − $0.80).
- Further unchanged fresh books cannot create another order/fill or residual.

Failure cleanup always releases/removes the hold and stops the engine. Existing
crash hooks were inspected but are unnecessary: this case exercises real
admission and deadline fencing, not a simulated restart.

## Validation

The first development run rejected both new tests solely because their zero-fee
string assertion expected `0.00000`; canonical execution serializes zero as `0`.
Both lifecycle actions were correct. The new expectations were corrected to
the existing representation without changing fee economics or synchronization.

After correction, 30 consecutive default-worker targeted commands passed both
tests: 60/60 passes, zero timeouts and zero wrong branches. Each pass checks
action/token/quantity/order identity and terminal residual in the assertions
above. No instrumentation, worker reduction, timeout increase or skipped target
was used. Command:

```powershell
pnpm test -- tests/pr3-premerge-correctness.test.ts -t PARITY-LIFECYCLE
```

Command wall times ranged from 2922 to 4453ms (mean 3209.5ms). Test file SHA256
for this repeat series:
`b0931fe20728a2a936f4ecb2dedd8864dd2852417db3feb05e668fd46f5c59c5`.
The handoff includes all 30 command logs and the machine-readable result index.

Default-worker correctness suites: 96 passed (PR3 pre-merge 21, early PR3 13,
PR87 ownership 16, Guardian 46). Engine typecheck/build and synthetic CLI smoke
passed. Full unmodified `pnpm test`, default workers: 91 files passed / 2 skipped;
1084 tests passed / 10 skipped (1094 total). No full-suite failure or reduced-worker
rerun occurred on this final revision. The skips are existing suite skips; neither
new lifecycle test is skipped.

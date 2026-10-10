# G2G | State Coverage Closeout: Offline Recovery

Date: 2026-10-10
Branch: `refactor/guardian-isolation-atomic`
Source: Stage-08 forensic code, previous HEAD `cfef6f4`.
Operator: NO SHADOW, no live process, no VPS or economic execution changes.

## Narrow final change

`apps/engine/src/guardian-offline-durable-source.ts` can now hydrate the existing `nakedDirectionalDiagnostics` from verified schema-v2 checkpoint payloads. It uses the original `nakedDirectionalDiagnosticSchema`, keys records by market with duplicate rejection, and runs the existing `validatePostReplayInvariants`. Source replay already knows how to merge `NAKED_DIRECTIONAL_OBSERVATION` and clear a diagnostic on durable `POSITION_SETTLED`.

This change does not alter Guardian V2 policy, profit trailing, journal/DB tables, fill admission or the paper engine runtime.

## Coverage matrix

| Authority field | G2G handling |
| --- | --- |
| Ledger, positions, cash and fees | Source PaperLedger + result accounting and post-replay invariants |
| Maker resting orders and reserved cash | Restored and reserve totals checked |
| Pending risk-reduction owners and execution failures | Restored, episode/quantity fenced, conflicting owner rejected |
| Guardian V2 and profit-trailing states | Original runtime schemas and checkpoint/replay invariants |
| Economic settlement lookups | Existing schema and nonsettled inventory constraint |
| Naked-directional soak diagnostics | Original schema, unique market, valid position ID and recovery invariants |
| Report source integrity faults / quarantine | **HALT**; not ignored, not silently treated as clean |
| Missing, invalid, compacted, out-of-sequence durable payload | **HALT**, never synthesize a no-position-change event |
| Unknown/uncertain DB commit result | **NOT DETECTABLE FROM AN UNTRUSTED SNAPSHOT**; G2H producer must verify and fail closed, no subsequent protection admission on ambiguity |

## Proof

- Four new G2G tests: valid diagnostic restore/MTM, duplicate/wrong position ID rejection, committed settlement cleanup, explicit refusal of unresolved integrity/quarantine.
- Test fixtures were updated to include `nakedDirectionalDiagnostics: []` because the new strict coverage rejects omissions; no runtime behavior changed to suit the old fixture.
- `PM_FORENSIC_STAGE=08_current`: 12 test files / **220/220 PASS**.
- Core TypeScript, forensic engine TypeScript and `git diff --check`: PASS.

## Completion meaning

G2G's **offline read-only checkpoint field-coverage gate** is complete for the modeled state above, with explicit fail-closed exclusions. It is not proof that a real independent Guardian can securely consume source data. G2 as a whole remains **IN PROGRESS**.

Next gate **G2H** must supply a trustworthy source-to-consumer handoff, producer epoch, independently verified content hash, contiguous cursor (including unrelated rows), correct handling of retention/compaction, lag bound, fresh paired UP/DOWN books and no-ACK/unknown-commit behavior. Only after G2H passes should G2I run a single independent Guardian in a CLOSED harness. Nothing here authorizes shadow or deploy.

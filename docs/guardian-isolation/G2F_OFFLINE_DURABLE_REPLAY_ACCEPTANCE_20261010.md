# G2F | Offline Durable Source / Checkpoint Verification

Date: 2026-10-10
Authority: `refactor/guardian-isolation-atomic` parent `80b61a31aca8a7b4cf8ce7168a7cf0c669ab0cdf`.
Status: OFFLINE source translator only. NO SHADOW, NO VPS, NO deployment, NO wallet, NO LIVE.

## Source evidence and problem

- `apps/engine/src/engine.ts` records PAPER_FILL via `commitAuthoritative` using `fill:<id>` and the authoritative applied ledger. `journal_events` mirrors/derived audit do not replace the durable transition stream.
- `apps/engine/src/recovery.ts:applyDurableTransition` reconstructs the true residual with `PaperLedger.applyFill`. The durable PAPER_FILL payload is NOT already a complete read-model update.
- `POSITION_SETTLED` replay removes the ledger position. The live engine additionally adds the market to `settledMarkets` in `afterApply`. An offline adapter must derive this terminal flag only from a committed and verified settlement row.
- Durable sequences are global. Every row from the cursor must be examined, including non-position events, to detect gaps.
- A compacted durable operation may retain only an identity + hash and no original payload. Such a record is *not* enough to reconstruct economics.
- The live checkpoint contains schema-v2 ledger, positions, durableCursor, settledMarkets, experiment, economic owners/resting orders and Guardian/trailing state. Incomplete restoration of any active authority is dangerous.

## One small implementation atom

`apps/engine/src/guardian-offline-durable-source.ts` adds two **synchronous, read-only** functions:
- `bootstrapGuardianOfflineSource` takes a caller-provided **trusted checkpoint**, checks v2 and recovery validation, matches the externally trusted checkpoint content hash via `hashCheckpointPayload`, reconstructs a PaperLedger from its ledger/positions and creates the G2E read-only projection at the exact cursor.
- `advanceGuardianOfflineSource` requires row.sequence = cursor + 1, verifies `hashDurablePayload(row.payload)` matches the stored hash, refuses compacted/empty payload and an operation key repeated since bootstrap, clones the recovery state, invokes the real `applyDurableTransition`, emits exactly one G2E position frame and compares every residual/settled market with the replayed authoritative ledger.
- A durable `POSITION_SETTLED` derives the terminal settled bit explicitly; normal research journal entries are NEVER used as terminal authority.
- Unknown rows/integrity faults, source gaps, mismatched hashes, unmodeled authority and divergent ledger projections **throw fail-closed**. No retry, order, action, write, feed, thread or process is scheduled by this module.

## Deliberate fail-closed limitations, NOT finished architecture

- This first adapter rejects checkpoints with outstanding maker reservations/resting orders, risk-reduction owners/failures, Guardian states, profit-trailing states, naked diagnostic states, settlement lookup state, unresolved report-source integrity or quarantine. It does NOT claim to support a normal busy engine checkpoint. These need full typed hydration and ordering proof in G2G.
- A checksum alone cannot authenticate its own source. `externallyVerifiedContentHash` must originate from a **separately trusted verified checkpoint lookup** and source identity/epoch must be externally fenced. Caller-supplied identity strings provide no authentication.
- The operation-key set is only for replayed rows since bootstrap. Durable key uniqueness **before checkpoint** depends on the trusted storage/history and remains unimplemented as a cross-process guarantee.
- Frames are produced from offline rows supplied by a test, not a live Postgres read. There is no source subscription, true consumer watermark persistence, ACK protocol, disconnect recovery, IPC, O(1) Guardian manager or separate process.
- `hashDurablePayload` verifies each payload against the field in its supplied row, NOT against an authenticated external manifest by itself. Tampering with both payload and hash requires a trusted source/transport to reject.
- Existing `G2E` operation identifiers are provisional request/publisher contracts; cannot be treated as execution permission.
- The live `applyDurableTransition` has replay-only branches which differ from live `afterApply`, notably settlement terminal markers. These must be reconciled in full service extraction before cutover.

## Tests run

- `pnpm exec vitest run tests/guardian-offline-durable-source.test.ts`: **9/9 PASS**. Includes real `PaperLedger` replay, partial SELL, settlement, MTM global cursor progress, tampered checkpoint/payload, missing/duplicated sequences, compacted empty payload, duplicate operation key and unknown integrity fault.
- `pnpm exec tsc -p apps/engine/tsconfig.forensic.json --noEmit`: PASS.
- `PM_FORENSIC_STAGE=08_current pnpm exec vitest run tests/guardian-offline-durable-source.test.ts tests/guardian-position-projection.test.ts tests/guardian-boundary-contract.test.ts tests/guardian-v2-state-equivalence.test.ts tests/guardian-v2.test.ts tests/reversal-protection-paper.test.ts tests/profit-trailing-pair-engine.test.ts tests/engine-guardian-paper.test.ts tests/profit-trailing-pair.test.ts tests/guardian-book-signal.test.ts tests/reversal-protection.test.ts`: **191/191 PASS**, 11 test files.
- `pnpm --filter @pm/core typecheck`: PASS.
- `pnpm exec tsc -p apps/engine/tsconfig.forensic.json --noEmit`: PASS.
- `git diff --check`: PASS (verify at stage).

## Files

- New: `apps/engine/src/guardian-offline-durable-source.ts`
- New: `tests/guardian-offline-durable-source.test.ts`
- New: this report.
- No change to `apps/engine/src/engine.ts`, `packages/core`, journal, DB schema, risk policy, execution authority or production process.

## Next G2G contract

Hydrate **all** currently legitimate checkpoint/recovery authorities without skipping settled markets, pending maker/rescue takers, trailing state, Guardian state, or uncertain DB commits. Require externally verifiable producer epoch, exact cursor handoff, fail-closed source integrity and bounded transport buffering. Only then consider a single separate offline Guardian process with no second live system.

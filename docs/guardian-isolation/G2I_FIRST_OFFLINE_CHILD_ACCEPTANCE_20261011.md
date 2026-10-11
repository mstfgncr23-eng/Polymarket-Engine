# G2I | First actual separate Guardian process (offline vertical slice)
Date: 2026-10-11 | branch: `refactor/guardian-isolation-atomic`

## Exact scope
- Added `apps/engine/src/guardian-offline-child.ts`: Node.js **separate child PID**, accepts closed-fixture IPC only when `PM_GUARDIAN_OFFLINE_CHILD=1`; no DB/CLOB/WS/wallet/executor imports, no economic writes.
- Uses existing `@pm/core` `bootstrapGuardianPositionProjection`, `evaluateGuardianV2`, `projectGuardianBookSignal` and `guardianProtectionRequestSchema`. Reuses G2H `guardian-paired-book-channel` to check both book ladders, current ages and skew.
- Emits `NO_ACTION`, `REQUEST` or sticky `HALTED` IPC responses; validation rejects duplicate/reversed generations, token rotation and source cursor/identity rollback. Result is a **typed risk request**, never an order or executor admission.
- Production `engine.ts` is intentionally unchanged. Its in-process Guardian still owns PAPER production logic until future approved G4 cutover. No shadow consumer exists.

## Local acceptance evidence
- `tests/guardian-g2i-offline-process.test.ts`: 5/5 PASS. Separate OS PID; two observations -> V2 EARLY emergency -> typed PAPER request; no re-request on following tick; stale book -> NO_ACTION; repeated ingress -> sticky HALT; empty position -> NO_ACTION; kill/new child -> clean standalone start.
- Isolated child TypeScript typecheck PASS using `tsc --noEmit --target es2022 --module nodenext --moduleResolution nodenext --skipLibCheck --types node apps/engine/src/guardian-offline-child.ts`.
- `@pm/core` typecheck PASS. The unconfigured standalone `tsc` command omitted `--types node` and produced environment-type errors; this is NOT a product regression.
- No network connection, Postgres, actual CLOB market data, Wallet or economic executor was used.

## Known gaps, NOT silently called DONE
- This is an **offline process seam**, not full G2I or G2. Its bootstrap is only structurally checked by this child: external checkpoint signature/source authentication, durable cursor/ACK replay and source freshness/epoch are still mandatory when a trusted producer is attached.
- IPC has no bounded production supervisor/queue, kill/restart recovery receipt, cross-process durable state persistence, multi-market throughput or sustained 2-vCPU p99 measurement yet.
- Trading Engine has NOT been detached or wired to IPC; request delivery to the existing single economic executor and its ownership fencing are G3 work.
- G2H frozen H2/H3/H4 requirements remain PARTIAL. `guardianRecoveryHold=true` remains unchanged. No retention protocol extension or database mutation was introduced.
- Next step: adapt this real separate process seam to the **existing trusted read-only PG session and standalone paired-book observer** in an isolated offline harness, with bounded delivery/ACK and deterministic parity; only proven integration blockers may be fixed. Do not start a retention redesign.

## Security and permission
PAPER ONLY. NO SHADOW. SINGLE ECONOMIC WRITER. NO PRODUCTION DEPLOY, PUSH, MERGE, DB MUTATION OR WALLET.

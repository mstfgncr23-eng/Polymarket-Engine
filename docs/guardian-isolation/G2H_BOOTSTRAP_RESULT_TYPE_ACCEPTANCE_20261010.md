# G2H | Bootstrap result typed fail-closed branch

Date: 2026-10-10. Parent `06b4f02`.
Gate: G2H IN PROGRESS.

The G2F offline source handled `bootstrapGuardianPositionProjection()`
with one not-APPLIED branch that erroneously read `.reason` from
the DUPLICATE result variant. The runtime DUPLICATE case was impossible
on a new bootstrap, but TypeScript correctly rejected the union.

This tiny atom distinguishes REJECTED (with reason) from the
unexpected DUPLICATE (separate halt). It does not change the success
path or any economic/runtime order semantics.

Closed source/session/issuer tests: **46/46 PASS**.
Engine full TS now has just the known out-of-scope failure:
`apps/engine/src/operator-report.ts` refers to missing `./source-seal.js`.
No "full engine typecheck PASS" is claimed. Diff check PASS.

NO SHADOW / NO VPS / PAPER ONLY / NO PUSH / NO DEPLOY / NO MERGE.

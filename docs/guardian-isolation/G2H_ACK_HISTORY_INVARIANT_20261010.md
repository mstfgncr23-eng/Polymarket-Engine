# G2H | Prevent signed checkpoint from rewriting ACKed history

Date: 2026-10-10. Parent `3fa5868`.
Gate: G2H IN PROGRESS. Zero production changes.

## Safety defect and resolution

The consumer read-only session previously allowed a newer signed manifest
sequence to replace the read model at an already-ACKed cursor even when
its projection digest disagreed with the previous persisted ACK. The same
loophole allowed a newer checkpoint to start AHEAD of the ACK without
replaying through the ACK's historical position. Signature freshness is
NOT authorization to rewrite previously observed economic history.

The session now HALTS on ANY same-cursor projection mismatch and HALTS
when its newly adopted checkpoint cursor exceeds an existing ACK,
regardless of manifestSequence or writerEpoch. It must reconcile using
a prior trusted checkpoint through the recorded cursor, not skip events.

Two new tests confirm:
- newer signed sequence + forged same-cursor projection => HALT;
- checkpoint starting ahead of ACK => HALT, zero receipt writes.

Added `apps/engine/tsconfig.guardian-source.json` to typecheck all
`guardian-*.ts` modules explicitly, not only `main.ts` as forensic
tsconfig does. Guardian-source TypeScript PASS; Engine forensic TS PASS;
session tests **11/11 PASS**.

Known non-G2H full Engine TS blocker:
`apps/engine/src/operator-report.ts` imports missing `./source-seal.js`.
Intentionally no unrelated repair or report rewrite.

NO SHADOW / PAPER ONLY / NO VPS / NO DEPLOY / NO PUSH / NO MERGE.

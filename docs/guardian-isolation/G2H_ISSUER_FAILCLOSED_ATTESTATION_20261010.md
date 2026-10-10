# G2H | Fail-Closed Writer Signer Prechecks

Date: 2026-10-10. Parent: `247573b`.
Status: **G2H IN PROGRESS, NOT ACCEPTED**.

## Narrow safety problem

The offline manifest signer only required caller-supplied `economicState:
"VERIFIED"`, equality of canonical cursor and DB high-water and a
self-consistent checkpoint digest. That did NOT reject a checkpoint
bearing unresolved `reportSourceIntegrity` or `reportQuarantine`;
nor did it disambiguate two otherwise eligible self-hashed checkpoint
variants at the **same durable cursor**. A concurrent invocation of
`issue()` could also choose the same in-memory manifest sequence.

## Local corrections

- The issuer now refuses to sign a selected checkpoint containing
  unresolved report source faults or a quarantine marker.
- If two eligible, verified checkpoints have the same durable cursor
  but incompatible hashes, issuance HALTS rather than selecting one
  arbitrarily by array ordering. Divergent economic state at the same
  durable cursor cannot be dismissed as checkpoint metadata churn.
- Signing the next manifest sequence is serial within one issuer
  instance. Concurrent calls HALT before any signature or sequence
  increment. The next accepted call gets the next monotonic sequence.
- The checkpoint digest code path explicitly handles malformed
  payloads and refuses publication on failures.
- All of this remains PAPER-only, offline, local.

Tests: three new issuer cases for unresolved source fault/quarantine,
same-cursor conflicting checkpoint and concurrent issuer reentry.
Relevant focused **30/30 PASS**, Guardian source TS PASS.

## Still unproved, blocking G2H

The `economicState` witness is still a *caller claim*, NOT canonical
EngineService readiness/unknown-commit proof. Signing keys are not in
a provisioned exclusive producer; the in-memory issuer sequence is
NOT persisted across process failover; no real writer epoch lease exists.
These contracts must be owned by the canonical Engine inside its
durable gate and independently verified before deployment. This is
not an operational signing authority.

NO SHADOW / NO VPS / NO LIVE / NO PUSH / NO MERGE / NO DEPLOY.

# G2H | ACK state digest must bind all economic authorities

Date: 2026-10-10. Parent: `5eeb397`.
Gate: G2H IN PROGRESS, offline reader and recovery only.

## Correctness risk

A read-only consumer receipt originally hashed only position projection and
settlement list. Those values can remain unchanged even if restored resting
maker ownership, Guardian protection state, trailing state, outstanding risk
admission or accounting diverges at exactly the same durable cursor.
A stale or forged ACK could falsely validate a different economic state.

## Narrow mitigation

`hashGuardianRecoverableState` now SHA-256 hashes a deterministic, sorted
full recovered authority envelope: position projection, ledger snapshot,
all ledger positions, experiment, result accounting, resting orders, maker
reservations, pending owners, execution failures, reversal states, trailing
states, settlement lookups, naked diagnostics, settled markets, infrastructure
accrual, and consumed operation-key set. The receipt retains the existing
field name `projectionHash` but uses the domain-separated
`pm-guardian-consumer-economics-v2` digest. This format is not deployed;
any former local receipt MUST fail closed and be rebuilt after independently
pinned checkpoint/replay rather than mechanically migrated.

The new deterministic test mutates maker reservations, trailing state and
consumed keys while leaving open positions unchanged, and verifies digest
difference. Session/source targeted tests: **15/15 PASS**; dedicated Guardian
source TypeScript check PASS. Economic decisions remain request-only.

## Known limitation

This proves restart parity against a prior consumer receipt, **not** that the
underlying independently unsigned durable row pages have producer signatures.
The source still requires a trusted PostgreSQL ACL/transport, durable
exclusive producer epoch and operational signer integration.

NO SHADOW / PAPER ONLY / NO VPS / NO PUSH / NO DEPLOY / NO MERGE.

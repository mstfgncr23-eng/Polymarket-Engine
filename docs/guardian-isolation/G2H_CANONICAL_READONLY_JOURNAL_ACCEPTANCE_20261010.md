# G2H | Actual Canonical Journal Reader Capability

Date: 2026-10-10. Parent: `bc21648`.
Gate: **G2H IN PROGRESS**.

## Why

The real `EventJournal` already exposes `listVerifiedCheckpoints()`,
`latestDurableSequence()` and ordered `readDurableTransitionsAfter()`.
Its `JournalAccessMode="readonly"` denies all runtime/maintenance writes
and does not run migrations. A separate new database feed stack is
unjustified at the present measured workload.

## Minimal adapter

`apps/engine/src/guardian-real-journal-source.ts` restricts the
journal capability to exactly the three read methods consumed by G2H.
It fails closed if `access` is not `readonly`, if the verified-checkpoint
API is absent, or if the page bounds are invalid; the returned adapter
exposes no write, migration, retention or economic execution API.

Closed tests exercise **real MemoryEventJournal implementation**, not a
hand-rolled imitation, including saved verified checkpoint and explicit
write-denial after switching to readonly. **2/2 PASS**.

## Remaining production-trust gate

This wrapper does not connect to production nor provision a DB user.
The operational consumer must open a PostgreSQL connection with a
genuine SELECT-only database role and `connectWithoutMigrate("readonly")`.
Local mode enforcement does NOT replace DB ACL/TLS verification.
No runtime operator permission to do that exists here; do not try it.

NO SHADOW / PAPER ONLY / NO VPS / NO PUSH / NO DEPLOY / NO MERGE.

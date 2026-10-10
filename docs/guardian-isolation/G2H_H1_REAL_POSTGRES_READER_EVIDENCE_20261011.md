# G2H H1 | Restricted PostgreSQL coherent reader | first local atom

Date: 2026-10-11. Parent branch `refactor/guardian-isolation-atomic`;
base `30258d2`. **H1 partially implemented, NOT ACCEPTED.**
Binding scope: `G2H_REQUIREMENTS_FROZEN_20261011.md`.
Follow-on: H2 is durable consumer receipt and multi-page checkpoint rotation.

## What was directly implemented (no signing, no runtime Guardian)

- `packages/db/src/journal.ts:PostgresEventJournal.readGuardianConsistentPage`:
  one PostgreSQL `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY`
  transaction for role identity, privilege inspection, global committed
  high-water, up to 64 verified checkpoints and up to 512 ordered durable
  rows. Each page sees ONE MVCC snapshot; sequence is global, not
  filtered by experiment or event category. Caller can explicitly supply
  `afterSequence`, or `null` to begin from the latest verified
  checkpoint cursor in **the same transaction**.
- Security boundary: existing `access:"readonly"` app capability is
  mandatory but not enough. Check `current_database()`, `current_schema()`,
  `current_user`, `session_user`, transaction read-only/isolation,
  named role, explicit three-table SELECT ability, schema USAGE, and
  absence of table/sequence mutation/DDL/admin rights in the trusted
  schema. Reject mismatch; no silent fallback.
- Checkpoint digest and semantic content integrity are verified from
  the transaction snapshot. Any malformed checkpoint, high-water
  regression, payload hash mismatch, absent/missing contiguous row
  or absent source cursor metadata HALTS.
- `apps/engine/src/guardian-trusted-pg-bootstrap.ts` builds
  recovered PAPER position, ledger, owners, reservation and protection
  state directly from the trusted checkpoint + bounded first-page
  transitions. **No Ed25519, signed manifest, per-page issuer
  certificate or new process**. This is NOT the old signer-bound
  `GuardianReadonlySession`; no order action/consumer ACK is exposed.

## Real disposable PostgreSQL 17 results (10/10 PASS)

The tests created a disposable container named
`pm-g2h-local-20261011` with loopback `127.0.0.1:55481`,
a test-only DB, and a dedicated non-admin reader principal.
`G2H_LOCAL_TEST_DATABASE_URL` accepts only this exact localhost DB,
port and admin username; tests DO NOT use DATABASE_URL, credentials
of VPS or Supabase, nor any production database.

`tests/guardian-h1-postgres-consistent.test.ts`:
1. Correct restricted source identity, checkpoint, high-water, global
   contiguous page.
2. Wrong database/schema/role, unsafe bounds, writer-mode journal
   rejected.
3. **Actual** reader INSERT/UPDATE/DELETE/CREATE TABLE rejected
   by PostgreSQL SQLSTATE `42501` with only SELECT grants.
4. Removed SELECT privilege, or added UPDATE privilege, HALTS.
5. Concurrent writer commits durable row while a separate reader
   transaction remains on the former repeatable-read snapshot. New
   reader transaction sees the new high-water.
6. Cursor ahead of committed source HALTS.
7. RLS denies the global cursor despite SELECT GRANT when a SELECT
   policy is removed; reader HALTS.
8. Persisted checkpoint corrupted hash rejected.
9. Trusted non-signer bootstrap reconstructs economics and a 1-page
   bounded replay to full high-water; small maxBacklogRows HALTS.
10. Physically missing journal sequence rejected.

**Critical confirmed real-world schema issue:** migration enables RLS on
`durable_sequence_metadata`, `engine_checkpoints` and
`durable_transitions` but has no general reader policies.
`GRANT SELECT` ALONE RETURNS NO ROWS. In the disposable DB,
role-specific `FOR SELECT ... USING (true)` policies on precisely
these three tables plus their SELECT grants made data visible.
No policy/role/migration was changed in any production DB.
This permission setup is a required H-01 deployment proof,
not an optional backlog improvement. Provisioning automation beyond
the proof is parked in `G2H_DEFERRED_IMPROVEMENTS_BACKLOG.md` as GI-012.

Official PostgreSQL docs support one snapshot across RR queries and
separate table privileges/RLS:
https://www.postgresql.org/docs/17/transaction-iso.html
https://www.postgresql.org/docs/17/ddl-rowsecurity.html

## Still blocking full H1/H2 acceptance

- The existing signer-only `GuardianReadonlySession` remains as an
  experimental path, and is NOT reconnected to the new trusted PG
  reader. The new bootstrap proves an independent no-signature first
  page, not a continuously polling consumer with durable ACK.
- A new `readGuardianConsistentPage` call is a NEW snapshot. The old
  checkpoint must remain accessible and identical for multi-page
  recovery; consumer cross-snapshot state, compaction, checkpoint
  handoff, old receipt migration and restart are **H2**.
- The local test role/RLS policies are temporary fixture setup.
  No real prod role, TLS identity, migration/RLS drift or production
  privilege grants have been inspected or changed. Do not mark H-01
  deployed or connect to a VPS merely because disposable tests pass.
- The separate, exclusive economic order writer and protection
  admission remain **G3**, not a property this reader can prove.

NO SHADOW / NO LIVE / NO PROD DB / NO VPS / NO DEPLOY / NO PUSH / NO MERGE.

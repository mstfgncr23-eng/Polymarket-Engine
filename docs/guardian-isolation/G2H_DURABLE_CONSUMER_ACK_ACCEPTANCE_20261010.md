# G2H | Local Durable Consumer Progress ACK

Date: 2026-10-10. Parent: `e95b003`. Gate: **G2H IN PROGRESS**.

## Evidence and mechanism

NATS JetStream durable consumers use explicit acknowledgments and redelivery;
PostgreSQL logical replication documentation also makes clear that crash/restart
can replay already delivered changes. We do not add either service to this
existing ordered PostgreSQL durable-log architecture. Instead use a narrow,
consumer-owned progress receipt, never a new economic event.

Sources:
- https://docs.nats.io/concepts/jetstream
- https://www.postgresql.org/docs/16/logicaldecoding-explanation.html

## Changes

- Added `guardian-consumer-ack-store.ts`: a separate **consumer-only** local
  cursor watermark, not part of the canonical ledger or engine journal.
  Receipt names authority, experiment, issuer epoch, signed manifest sequence,
  pinned checkpoint, cursor and observational projection digest.
- Compare-and-swap reload under O_EXCL single-writer file lock; reject another
  writer and stale compare; on-disk file flush, rename replacement, POSIX
  directory flush when available. Corrupt/truncated records and regression
  halt. Stale lock after crash requires operator investigation.
- Windows portability limitation: directory fsync cannot be promised; an
  ACK after sudden power loss may be lost despite file flush + rename.
  Therefore receipt is a **progress hint / replay fence**, not permission to
  skip historical ledger replay. Power-loss durability must be validated
  before G4; an ambiguous commit is HALT + reconcile.
- G2H source/recovery supervisor still must bind a receipt to independently
  signed fresh checkpoint and verify actual replayed projection digest on
  restart. No single live process yet; the store alone cannot do so.

## Test evidence

Six offline filesystem tests: restart, CAS concurrency, regression/authority
swap, corruption, stale lock and retry after refused write.
Focused **6/6 PASS**, forensic Engine TS PASS.

NO SHADOW / NO VPS / PAPER ONLY / NO REMOTE PUSH / NO LIVE / NO MERGE.

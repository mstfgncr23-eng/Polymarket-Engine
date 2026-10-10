# G2H | Bounded Read-Only Durable Page Transport

Date: 2026-10-10
Branch: `refactor/guardian-isolation-atomic`
Parent: `38f91f8`.
Status: **G2H IN PROGRESS**, first read-only page atom only. No separate Guardian process, no live DB connection, no shadow or production deploy.

## Architecture decision from current source and official DB semantics

Use the current PostgreSQL `durable_transitions` ordered source through the existing `readDurableTransitionsAfter(cursor,limit)` and `latestDurableSequence()` APIs. This avoids a new Kafka/Redis/CDC stack before there is measured evidence of necessity.

- The real PostgreSQL journal reader orders by `sequence ASC`; payloads may be compacted, and the global sequence contains unrelated transitions.
- PostgreSQL NOTIFY is transaction-delivered notification/signaling, not a durable history source. A future optional wake-up hint cannot replace the durable cursor scan. PostgreSQL docs: https://www.postgresql.org/docs/15/sql-notify.html
- Logical replication slots are not default: operational WAL retention and lag must be managed. PostgreSQL reference: https://www.postgresql.org/docs/current/logicaldecoding-explanation.html

## Implemented

`apps/engine/src/guardian-readonly-durable-feed.ts`:

- Accepts only two read methods via a minimal `GuardianDurableReadSource` interface. No credentials, DB connection establishment, timer, thread, socket, external executor or outgoing request is created by this module.
- Retrieves global high-water; bounds each pull to `pageSize <= 512` rows and maximum tolerated backlog `<= 100000` (both caller configured and validated).
- Requires strict monotonic `sequence = cursor + 1`. Refuses empty page when behind high-water, impossible page lengths, duplicate/out-of-order/future sequence, regressed high-water and large backlog.
- Uses the G2F real recovery/ledger/position projection translator on EACH row. Compacted empty payload, wrong hash, unknown durable types and accounting/episode disagreement are fatal, not treated as NOOP.
- Maps DB read failures to an explicit `GuardianFeedHalted` reason. A halted caller must stop protection decisions and reconcile a new verified checkpoint before serving any requests. Old input state is never returned as fresh after a failed read.
- Reads one bounded page and returns its new immutable state; it does not auto-loop or silently backoff into obsolete books.

## Proof

New deterministic tests appended to `tests/guardian-offline-durable-source.test.ts` for two-page partial SELL -> settlement replay, empty/upstream gap, retention loss, malformed page, backlog, high-water regression, timeout, future/out-of-order sequence and altered/compacted payload.

- Targeted offline source tests: **28/28 PASS**.
- Broad regression: **226/226 PASS across 12 test files** with `PM_FORENSIC_STAGE=08_current`, process exit code 0, log `%TEMP%\\guardian_G2H_readonly_source_regression_20261010.log`.
- Forensic engine/core TypeScript and git diff --check required.
- NO SHADOW. No live sockets, VPS, DB write, production engine changes or real orders.

## Unfinished G2H acceptance

1. Independently read verified checkpoint from the trusted storage source and fence its commit cursor, epoch/experiment, hash and retention. `externallyVerifiedContentHash` alone is not authentication.
2. Compose a pull/restart supervisor with explicit **halt/reconcile** lifecycle and no stale decisions after DB disconnect, source failover, consumer ACK loss or uncertain commit.
3. Fresh paired UP/DOWN executable book source: correct token and sync epoch, feed-connected-and-actually-ticking, sourceTimestamp/receivedAt, one-sided disconnect and 8/8 restart handling.
4. Bounded CPU, database query demand, process queue lag and backpressure under actual workload.
5. No second live Guardian and no shadow process. G2I runs only one Guardian in an **offline closed harness** after G2H is complete.

This commit is a narrow incremental deliverable inside G2H, NOT completion of the stage.

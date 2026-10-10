# G2H H4 / H2 | Three-market economic parity and checkpoint rebase on local PostgreSQL

Date: 2026-10-11 | Base HEAD `b2ef51b`
Evidence only, H-02 / H-03 / H-05 **PARTIAL**, G2H **NOT ACCEPTED**.
No source-code changes to production execution, book adapters, risk
parameters, engine, database schema or order authority.

## Relevant actual implementation

The canonical `PostgresEventJournal.commitDurableTransition` assigns
ONE global ordered sequence under a SQL transaction and writer lock.
It is **not** correct to test multi-writer order submission; this
scenario tests interleaved economics of multiple open markets under
the actual single-writer sequence. The Guardian consumer reads the
verified checkpoint and contiguous rows through a real independently
restricted PostgreSQL SELECT role. A v2 fsynced ACK stores the
cursor and all-state economic hash, not an order-admission receipt.

## Reproducible fixture and proof

`tests/guardian-h4-multimarket-pg-parity.test.ts` tests an isolated
disposable PostgreSQL 17 bound only to `127.0.0.1:55484`,
database `g2h_h4_multimarket_local`, checked explicit
`G2H_H4_MULTIMARKET_DATABASE_URL`; no default `DATABASE_URL`
or production/VPS credentials. Three separate UP positions
(BTC, ETH, SOL) are created by real canonical SQL PAPER_FILL rows
from an economically empty verified checkpoint at cursor 1.

| Cursor | Canonical durable transition | Verified economics / ownership |
|---|---|---|
| 1 | Empty verified baseline checkpoint | $1,000 cash, no inventory |
| 2 | PAPER_FILL BTC UP BUY 5 @ $0.40 | BTC residual 5 |
| 3 | PAPER_FILL ETH UP BUY 4 @ $0.30 | ETH residual 4 |
| 4 | PAPER_FILL SOL UP BUY 3 @ $0.44 | SOL residual 3 |
| 5 | RISK_REDUCTION_ADMITTED BTC | One active owner, BTC residual 5 |
| 6 | RISK_REDUCTION_ADMITTED ETH | Two simultaneous active owner records on different markets |
| 7 | PAPER_MAKER_RESERVED SOL | $0.60 maker reservation, one resting hedge order |
| 8 | RISK_REDUCTION_FAILED ETH | ETH owner released; failure recorded, BTC owner still active |
| 9 | PAPER_ORDER_CANCELLED SOL | $0.60 reservation released, resting order removed |
| 9 | VERIFIED NEW CHECKPOINT and parity-rebase | Consumer receipt checkpoint changed only after exact economic parity |
| 10 | PAPER_FILL BTC UP SELL 5 @ $0.52 | BTC fully closed, owner released |
| 11 | PAPER_FILL ETH UP SELL 1 @ $0.42 | ETH residual 3, earlier failed owner invalidated |
| 12 | POSITION_SETTLED SOL, UP payoff 1 | SOL closed/settled; ETH residual 3 persists |

The real journal source is queried again after EACH cursor and compared
against an independently operated `PaperLedger` calculator. The
Guardian `readVerifiedView` open residuals, maker reservation,
resting order, active owner IDs, failed owner IDs, settled markets,
durable cursor and consumer receipt are checked at every relevant
stage. A new Guardian consumer is constructed from the exact same
on-disk receipt after every stage; its full economic hash matches.

When writing the new verified checkpoint at cursor 9, all canonical
ledger positions, maker reservations, owners, failures, settlement
lookups and result accounting are included; rebase must prove
full-state equality before adopting its hash/ID. The test verifies
checkpoint ID actually changes, survives restart, and rows at
cursor 10+ continue in order. Old source checkpoint is still
retained, not compacted in this test.

**Independent hand-calculated result at cursor 12:**
- Initial BUY cash: $1,000 - $2.00 BTC - $1.20 ETH -
  $1.32 SOL = **$995.48**.
- BTC SELL 5 x $0.52 = +$2.60; ETH SELL 1 x $0.42 = +$0.42;
  SOL settlement 3 x $1 = +$3.00; cash **$1,001.50**.
- Realized PnL BTC +$0.60, ETH +$0.12,
  SOL +$1.68 = **+$2.40**, no synthetic fees.
- ETH remaining open quantity **3**, not settled or spuriously owned.

## Limits and strict nonclaims

- **KNOWN PASS:** three genuine SQL market buys, two overlapping
  owners across *different* markets, maker reserve/cancel, full and
  partial SELL, settlement, checkpoint rebase, ACK/restart parity.
- **KNOWN NOT TESTED:** simultaneous competing economic writers,
  which are outside the frozen single-writer boundary and belong
  to later G3 admission/race testing, not this H2 observer.
- **UNKNOWN:** large 40-market concurrent economic owner load,
  live venue/network rates, several-hour CPU/heap/ACK durability
  under power loss, realistic full market book distribution,
  queue overflow and checkpoint/compaction after actual deployment.
- The test proves an **11-row canonical journal tail** after
  checkpoint 1, including rebase at cursor 9 and continuation to 12;
  it is not a long-run soak
  or a performance benchmark and contains no trading instruction.
- No permission to mark H2/H4/G2H DONE, deploy Guardian, connect a
  wallet, launch shadow, enable trades, or merge anything.

## Validation record

- Local 3-market test: `H4-MULTI-01` **1/1 PASS**.
- Final scoped regression, including existing PostgreSQL consistent
  page (14 tests), standalone books, prior economic fault tests (3)
  and new multi-market full journal:
  **368/368 PASS across 29 test files**.
- Each PG-dependent test group used a **separate** isolated
  PostgreSQL 17 container/DB on ports 55481, 55483 and 55484.
  There was no shared global durable sequence between test groups.
- `@pm/db` TypeScript PASS; Guardian source TypeScript PASS;
  forensic Engine TypeScript PASS; `@pm/core` TypeScript PASS;
  `git diff --check` PASS.
- The four-test capped H4 benchmark from previous atom was not
  re-run as part of this 368-test suite. No claim of simultaneous
  economic execution throughput or target host CPU acceptance.
- No live/prod actions; stop and remove disposable test containers
  after commit verification.

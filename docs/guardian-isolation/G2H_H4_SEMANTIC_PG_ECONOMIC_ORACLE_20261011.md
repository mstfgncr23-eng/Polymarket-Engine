# G2H H4 | Genuine economic transitions on isolated PG, independent ledger oracle
Date: 2026-10-11 | Base `da26fad` | **H2/H4 PARTIAL, G2H NOT ACCEPTED**
Requirement authority: `G2H_REQUIREMENTS_FROZEN_20261011.md`.

## Why this atom

Previous H4 tests matched historical durable transition timestamp rates
and byte-length distributions but *all* synthetic writes were
`LEDGER_MARK_TO_MARKET`. Byte-matched fake fills alone cannot prove
that a Guardian following a real economic stream does not hallucinate,
lose, or duplicate a residual owner, cash flow, settlement, or ACK.

## Executed source, not an Engine callback or mocked PG

`tests/guardian-h4-semantic-pg-oracle.test.ts` uses:
- a uniquely guarded ephemeral PostgreSQL 17 on
  **127.0.0.1:55483**, database `g2h_h4_economic_local`;
  ordinary `DATABASE_URL` and remote addresses are never consulted;
- genuine `PostgresEventJournal.commitDurableTransition` rows with
  canonical per-type operation key formats and a one-writer global
  durable sequence;
- actual restricted PostgreSQL reader role with RLS SELECT policies
  and `GuardianTrustedPgSession` coherent read snapshots;
- signer-free version 2 fsynced observation ACK with full restart
  hash reconciliation at *every* intermediate cursor;
- an independent `PaperLedger` accounting calculation (not just
  Guardian read-model compared to itself), and explicit hand-calculated
  dollar checks.

The durable sequence / economic flow:
1. Verified committed checkpoint at cursor **1**, with one genuine
   previously bought **UP 5 @ $0.40**, canonical positions, ledger
   and accounting included.
2. `RISK_REDUCTION_ADMITTED` at cursor **2**: exact matching
   current unhedged episode, admitted residual **5**, owner count 1.
3. `RISK_REDUCTION_FAILED` at cursor **3**: owner released,
   failure record 1, economic residual still **5**.
4. Genuine `PAPER_FILL` **SELL UP 2 @ $0.52** at cursor **4**:
   residual **3**, owner 0, failure no longer active. Independent
   expected cash **$999.04** and realized PnL **+$0.24**.
5. Genuine `POSITION_SETTLED` **UP payoff 1** at cursor **5**:
   no open inventory, no residual/owner/failure. Independent cash
   **$1,002.04** and realized PnL **+$2.04**.

After *every* transition, the test confirms persisted ACK cursor,
a new independent Guardian session startup replays to the same cursor
and full economic digest, the canonical PG row replay's entire
`PaperLedger.snapshot()` and positions match the independent
`PaperLedger` oracle, and the read-only Guardian view matches the
expected unhedged residual and owner/failure cardinality.

## Explicit fail-closed fault evidence

- **H4-ECO-03:** a fabricated `RISK_REDUCTION_ADMITTED` at global
  sequence 6 *after settlement* is committed to the ephemeral PG.
  Canonical reducer correctly refuses ownership for a nonexistent
  open position. Guardian HALTS; its receipt stays at 5.
- **H4-ECO-02:** a privileged actor modifies the already-ACKed
  cursor-4 SELL price **$0.52 -> $0.53** and also recomputes the
  correctly formatted durable payload hash in the isolated database.
  The new consumer restart replays prior checkpoint through cursor
  5, rejects the different economic digest with
  `G2H_PG_SESSION_RECEIPT_ECONOMIC_DIVERGENCE`, and does NOT
  overwrite or advance the old receipt.
- This does not make PostgreSQL tamper-proof: the G2H trust model
  explicitly allows a fully privileged malicious DB administrator
  to rewrite checkpoint/receipt trust context outside its
  threat model. This test proves local existing receipt mismatch,
  not resistance to complete source compromise.

All three H4 semantic PostgreSQL integration tests **3/3 PASS**.
Final full scoped baseline plus semantic tests:
**367/367 PASS in 28 test files**, with two physically separated
disposable PostgreSQL 17 databases, ports 55481 and 55483.
Database, Guardian-source, forensic Engine and Core TypeScript:
**all PASS**; `git diff --check`: **PASS**.
The earlier H4 capacity benchmark with four tests was tested in
its own disposable source on port 55482 in the prior atom; it was
**not run concurrently** with this semantic suite and must not be
counted among these 367 tests.
No production changes, no new execution actor, no wallet.

## What still blocks G2H acceptance

- This isolated fixture has *one* market and only one position/owner
  cycle, with a real fill and settlement. It does not sample the
  complete 1,779-row historical semantic event distribution
  or prove multi-market concurrent owners, maker reservations and
  economic ordering under actual stress.
- A real upstream 2-vCPU VPS soak has NOT been run. H4 separate
  local capped byte-load/32-depth tests were already completed and
  must not be conflated with this semantic functional test.
- H2 checkpoint retention coordination, target filesystem power-loss,
  failover/privilege provisioning and long-run source latency/queue
  saturation evidence remain unresolved.
- H4 must be accepted only after named limits and representative
  throughput/duration on suitable hardware; no speculative rewrite.

Proof is confined to reproducible offline local integration tests.
No VPS, Supabase production DB, external Polymarket websocket,
live/paper trade, Guardian supervisor, wallet, deploy, push or merge.

# G2H REQUIREMENTS FREEZE | Trusted Observation, Not Cryptographic Settlement
Date: 2026-10-11 | Source baseline: `aab85cb` | Gate: G2H NOT ACCEPTED
Authority: this document controls G2H *scope and acceptance criteria*; previous
G2H atom notes remain historical evidence, not product requirements.
Change discipline: amend this freeze only for an independently reproducible
business-correctness fault or explicitly approved threat-model change.
Never add a new requirement because its implementation would be interesting.
**Permanent discovery rule:** while implementing H1-H4, append any optional
improvement to `G2H_DEFERRED_IMPROVEMENTS_BACKLOG.md` and commit it in the
same scoped task; do not implement it or move the G2H finish line. Actual
violations of H-01..H-05 stay in the active gap audit and block acceptance.

## 1. Product outcome and hard scope

One independently running Guardian must ultimately be able to observe the
*authoritatively committed* open residual episode and current synchronized
UP/DOWN books, survive crash/restart, and request protection without needing
Engine's event loop. It must not be a second Guardian economic writer.

G2H supplies the TRUSTED, REPLAYABLE, BOUNDED **OBSERVATION PATH**.
It must never treat a successful observation as authorization to SELL/HEDGE.
Separate Guardian process supervision is G2I. Cross-process **single executor,
atomic per-episode protective admission, current residual recheck, unresolved
order reconciliation, and exactly-one fill** are G3. G4 is single-owner
cutover; G5 is real PAPER acceptance. No G2H task can declare any of them done.

The business rule for eventual G3:
- The executor, NOT a Guardian request, validates at admission and again
  at economic commit that the SAME open episode remains, current unpaired
  quantity is positive, requested quantity does not exceed that residual,
  no maker/taker/profit-trailing/Guardian ownership already consumes it,
  and any unknown prior submit is reconciled BEFORE another attempt.
- If Engine stalls, the eventual independent Guardian must retain observation
  capability; G3 must preserve economic execution through ONE executor.
  G2H can only provide evidence, not guarantee that outcome yet.

## 2. Frozen trusted boundary (no mandatory Ed25519)

Assumptions to validate, NOT implicitly proven:
- **ASSUMED** PostgreSQL durable transitions/checkpoints are the canonical
  record and the only authorized economic writer controls their mutation.
- **ASSUMED** Guardian gets an independently provisioned PostgreSQL
  reader credential with SELECT-only ACL, correct TLS / connection identity
  where remote, and no schema/migration/data write capability.
- **UNKNOWN** real production credentials, role grants, transport, writer
  failover fencing and consistency of multi-query reads.
- **KNOWN from source**: PostgresEventJournal.commitDurableTransition() uses
  pg_advisory_xact_lock + durable_sequence_metadata update in the same DB
  transaction; access:`readonly` exists as an *app capability*,
  NOT a database ACL. Read APIs listVerifiedCheckpoints(),
  readDurableTransitionsAfter(), latestDurableSequence() are available.

The trust model is an AUTHENTICATED canonical PostgreSQL connection plus
a correctly scoped ONE-WRITER economic system. The content hash checks
accidental/inconsistent payload corruption, not malicious DB mutation.
**Ed25519 signatures, per-page producer certificates, external signing
keys and manifest sequence are NOT REQUIRED for G2H completion.**
Retain existing signer modules as isolated experimental work until their
removal/retirement can be done with regression evidence. They MUST NOT
silently remain a mandatory dependency of the final G2H source path.

If PostgreSQL itself or its privileged writer is compromised, this model
cannot detect arbitrary forged rows. That is an acknowledged security
boundary, not a problem to fix by adding cryptography without a threat
model. Reopen only with a demonstrated need.

## 3. Mandatory G2H requirements and acceptance (fixed)

**H-01: Source identity and least privilege**
- One configured canonical database/schema and experiment scope;
  verify DB identity, role privileges and observable connection health.
- Actual PostgreSQL credential CANNOT mutate any economic tables,
  checkpoints, schema, audit or maintenance state. No migrate on connect.
- Fail closed on source loss, unexpected experiment or wrong schema;
  never fall back silently to a cached guessed authority.
- Acceptance: integration test with a real isolated PostgreSQL instance,
  SELECT reads pass; INSERT/UPDATE/DELETE and migration attempts denied;
  source loss and misconfiguration block G2H readiness.

**H-02: Coherent checkpoint, sequence and economic recovery**
- Select a VERIFIED checkpoint with valid content hash, experiment identity,
  recoverable ledger, owner/reservation/settlement states and durable cursor.
- Replay EVERY canonical global durable transition after that cursor,
  including unrelated events; accept only contiguous monotonic sequence,
  exact payload hash, replay invariant and bounded backlog.
- A checkpoint/high-water/read-page must belong to a coherent committed
  source view, or a bounded retry must prove equality without accepting
  mixed committed versions. Merely querying multiple times under default
  READ COMMITTED does not provide one snapshot.
- Handle compaction gaps, source regression, malformed/unknown payload,
  conflicting checkpoint, write-outcome-unknown and unconfirmed source
  integrity as STOP/RECONCILE, never 'assume healed'.
- Acceptance: PostgreSQL concurrent-commit and checkpoint/compaction
  interleaving tests, monotone/economic parity oracle; no skipped,
  duplicated or phantom open residual.

**H-03: Crash/restart and consumer progress (NOT order ACK)**
- Last *economically fully replayed* cursor has a durable consumer receipt
  or deterministic safe restart from verified checkpoint+tail. A receipt is
  a local observation watermark, NEVER an order execution ACK.
- Crash before/during/after receipt, resume/replay, lost receipt,
  stale source and changed checkpoint must have deterministic behavior:
  exact recovery or latched HALT with explicit reconciliation. No silent
  cursor forward or double-apply.
- Require bounded memory and no lossy duplicate-key eviction. A hard
  limit may fail closed, but cannot prevent ordinary continuous operation
  at the measured expected rate; checkpoint rotation/rebase must be safe.
- Acceptance: fault-injected restart matrix and equivalent recovered
  positions, owners, economic ledger and protection state after replay.

**H-04: Current paired market observations without Engine dependency**
- Reuse the proven PolymarketMarketDataAdapter normalization/sync semantics:
  full UP and DOWN books, token epoch, source timestamps, reconnect/gap
  invalidation, bounded level/queue freshness and both-side parity.
- Test independent ingestion contract that does NOT require Engine
  callbacks to remain alive. In G2I the isolated Guardian will use the
  existing adapter/library itself or another independently operated
  feed with equivalent guarantees. The existing
  GuardianNormalizedBookBridge alone, connected ONLY to Engine callbacks,
  is NOT independence.
- A dedicated market-data websocket used by the *sole active* Guardian
  is NOT a second Guardian or a speculative execution writer. The final
  topology and resource cost still require evidence and stage G2I.
- Acceptance: offline adapter fixture exercises fresh pair -> gap,
  asymmetric 7/8/8/8 resync, disconnected-but-previously-connected,
  token epoch rotation, stale/no ticks, delayed/out-of-order full books;
  NEVER return READY using stale one-sided books.

**H-05: Fail-closed operations and measured capacity**
- Bounded source page/backlog, queue, replay key retention, book size,
  CPU/heap and lag. Distinct health for DB source, replay cursor,
  paired books, source age and reason for blocked readiness.
- No remote/prod soak is a G2H prerequisite. Use isolated local
  PostgreSQL and offline fixtures at workload rates taken from real
  historical data, record units, repeat baseline and fault effects.
- The 8,192-key replay budget currently halts after ~82 seconds at
  100 durable rows/s if no new checkpoint is acknowledged. A permanent
  halt during an ordinary soak is NOT acceptable; prove a safe solution.
- Acceptance: reproducible benchmark plus predefined limits from measured
  deployment CPU/RAM and target lag, with no unbounded growth, loss or
  duplicate economic state. No arbitrary '10x faster' criterion.

## 4. Explicit non-goals, must not extend G2H

- No Ed25519 or per-page producer signing requirement.
- No 'cryptographic bank reconciliation', ledger Merkle chain, new Kafka,
  NATS cluster, CDC service or novel general-purpose message bus by default.
- No secondary Guardian process, no hidden shadow, no dual order writer.
- No independent wallet/Polymarket order submission in the G2H observer.
- No extraction of executor or per-episode protective admission from Engine
  (G3), no G2I supervisor/cutover, no G5 soak/deploy.
- No change to 12/5/2 Profit-trailing, 300s max-unhedged or 80s
  pre-expiry protection; no new $0.60 live policy without separate review.
- No opportunistic unrelated refactor, hot-path optimization before
  measuring a workload, or extension of G2H due to new design ideas.

## 5. Go/no-go rule

G2H DONE only when H-01 through H-05 each has concrete implementation,
named passing test(s), reproducible commands, and a sign-off audit of
remaining limitations. A passing synthetic 313-test suite by itself is
NOT that evidence. G2I may begin only after G2H acceptance. A new
safety defect may stop signoff; it cannot silently create a new milestone.

Operator constraints: PAPER ONLY; NO SHADOW; NO VPS/production DB reads;
NO DEPLOY, PUSH, MERGE, LIVE, WALLET or autonomous cutover without
new explicit authorization.

## Evidence used

- Local code: `packages/db/src/journal.ts`,
  `packages/db/src/access.ts`, `apps/engine/src/engine.ts`,
  `apps/engine/src/guardian-readonly-session.ts`,
  `apps/engine/src/guardian-real-journal-source.ts`,
  `apps/engine/src/guardian-paired-book-channel.ts`,
  `packages/market-data/src/polymarket.ts`.
- PostgreSQL official MVCC transaction snapshots:
  https://www.postgresql.org/docs/current/transaction-iso.html
- PostgreSQL official privileges:
  https://www.postgresql.org/docs/current/ddl-priv.html
- Polymarket official Market WebSocket channel:
  https://docs.polymarket.com/developers/CLOB/websocket/wss-overview

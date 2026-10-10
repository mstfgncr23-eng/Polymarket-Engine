# G2H | Offline Signed Source Session, Replay, and ACK Supervisor

Date: 2026-10-10. Parent: `1d75bfe`.
Gate: **G2H IN PROGRESS**, closed local journal adapters only.

## Trusted publication model and read order

An independently provisioned Ed25519 public key identifies the signed
checkpoint and committed durable high-water. The canonical ordered
PostgreSQL durable log is chosen as the source rather than introducing
NATS/Redis/CDC without a workload measurement. Each session:
1. Reads a separately signed short-lived writer manifest (epoch, sequence,
   experiment, exact checkpoint ID+hash and durable high-water).
2. Reads exactly that verified checkpoint and restores full economic read
   state, then applies every durable transition after checkpoint cursor.
3. Loads its own persistent cursor ACK receipt, replays to that cursor and
   checks projection digest before considering the previous ACK reconciled.
4. During a single serial poll, revalidates signed issuer epoch/expiry,
   checks DB high-water, reads only up to signed high-water, strictly
   checks sequence, replay-invariants and operation keys, then commits the
   consumer receipt *before* exposing a new cursor. No ACK after a failed page.
5. On source error, gap, compaction, concurrent poll, signature timeout,
   epoch switch, backlog overflow or uncertain ACK, HALT is latched. Retry
   requires new source validation and checkpoint replay; no automatic
   economic action. Read-only status is never an execution grant.

## Exact code

- `apps/engine/src/guardian-readonly-session.ts`: bounded single-reader
  source/ACK supervisor. Zero in-memory pending queue. Cross-process work
  remains G2I; **no separate Guardian starts here**.
- `tests/guardian-readonly-session.test.ts`: nine tests for signed
  opening, paging, ACK ordering, restart and projection matching, forged
  ACK digest, unknown commit after write, expiry/epoch change, gap and
  source disconnection, backlog/rewind, concurrent poll and 100 synthetic
  events with max eight rows per page.
- Related broad run `PM_FORENSIC_STAGE=08_current`: **260/260 PASS across
  16 files**. Core TS PASS; forensic Engine TS PASS. Engine-wide TS currently
  also includes independent pre-existing errors in old G2F source handling
  and `operator-report.ts` missing `./source-seal.js`, so no full-engine
  typecheck PASS is asserted.

## Explicitly OPEN G2H criteria

- Real canonical producer placement and independently enforced exclusive
  writer lease/epoch, private key provisioning/rotation, and actual emitted
  signed manifests are NOT implemented. Offline tests sign fixtures.
- Live read-only DB credential/TLS identity is not provisioned or used.
  Actual durable source rows are not independently signed; the design
  necessarily trusts the journal access boundary and post-commit data.
- The raw Polymarket full-book/delta normalizer and feed reconnect observer
  are not wired. `guardian-paired-book-channel.ts` only consumes caller-
  verified complete normalized books; caller-supplied `synchronized:true`
  is not security evidence.
- Load tests are local deterministic synthetic runs, not measured VPS CPU,
  PostgreSQL latency or actual dual-book 8/8 disconnection.
- ACK file atomicity on Windows power loss needs filesystem validation;
  the last ambiguous ACK forces read/replay reconciliation. Real message
  delivery and IPC/worker are G2I, but writer/source authority belongs G2H.
- No economic requests and no second active decision process are present.

## No-risk rollback

Revert this single new supervisor atom if needed. Prior G2F/G2G/G2H source,
signature, book and ACK modules remain. No production state changed.
NO SHADOW / PAPER ONLY / NO VPS / NO LIVE / NO PUSH / NO MERGE.

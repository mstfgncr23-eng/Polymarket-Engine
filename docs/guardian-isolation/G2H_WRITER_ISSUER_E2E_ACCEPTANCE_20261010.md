# G2H | Offline Canonical Writer Manifest Publisher + Real Journal E2E

Date: 2026-10-10. Parent: `613aa9d`.
Gate: **G2H IN PROGRESS**. No production writer signer is activated.

## Source selection and evidence

Current Engine checkpoints inside `durableGate.run`, and
`checkpointInsideGate` enforces lastAppliedDurableCursor equal to
`EventJournal.latestDurableSequence()` before persisting. The
`EventJournal.listVerifiedCheckpoints()` returns only verified snapshots.
We reuse this real design, rather than inventing a second economic logger.

## Implementation

- `guardian-writer-manifest-publisher.ts`: side-effect-free Ed25519
  publication of short-lived, exact checkpoint digest / identity,
  experiment, writer epoch, monotonic manifest sequence and latest
  high-water, from the existing verified `EventJournal` read interface.
- Publisher declines LIVE, unknown economic state, engine/DB cursor mismatch,
  missing recent checkpoint, invalid checkpoint digest and source failures.
- This helper MUST be called **inside** the canonical producer's durable
  gate with independently verified sole-writer/fencing authority.
  It does not itself run a timer, read secrets from disk, write DB,
  issue a lease or start a Guardian.
- End-to-end local test uses the **real MemoryEventJournal APIs**:
  canonical saveCheckpoint + commitDurableTransition -> signed manifest ->
  switch journal to readonly -> verified G2H read-only session ->
  durable cursor ACK. No economic orders.
- Three new tests: **3/3 PASS**.

## BLOCKING real-world G2H authority gaps

- The real EngineService does not yet call this publisher under its durable
  gate; signed manifests are produced in tests, not from the running writer.
- No independently fenced owner lease or retained issuer key is provisioned.
  `lastDurablyIssuedSequence` is an *input contract*, not currently
  written to a reliable operator store by this module. A process failover
  without a recovered epoch/sequence floor must HALT.
- A signed high-water does not independently hash/attest every durable row;
  consumer must trust canonical journal ACL and source integrity checks.
- Therefore local E2E tests establish that the building blocks compose,
  **NOT** that actual operational source identity has been proven.
- Main Engine runtime, production VPS/DB, paper/live status, wallet and
  source connection remain unchanged. No rollout authorized.

NO SHADOW / PAPER ONLY / NO VPS / NO PUSH / NO MERGE.

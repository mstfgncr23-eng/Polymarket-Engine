# G2H | Authenticated Durable Row Pages Are Mandatory

Date: 2026-10-10. Base: `c1cb0ea`.
Gate: **G2H IN PROGRESS, NOT ACCEPTED**.

## Correctness defect closed locally

The former consumer verified the writer's Ed25519 **checkpoint** manifest
and durable high-water, but then read the intervening durable rows only
from the ordinary PostgreSQL reader. The row's stored payloadHash
authenticates no writer: an attacker able to modify a row and recompute
the stored hash could alter the recovered economic state while the
signed checkpoint/high-water remained valid.

`guardian-signed-durable-page.ts` adds a cryptographic, short-lived
producer-issued **page certificate**, binding the exact range, canonical
row identities, payload digests, source timestamps, experiment,
checkpoint ID/hash, producer epoch, manifest sequence and high-water.
Verifiers rehash the actual full payload of **every row**, verify ordered
contiguous sequence, bound page length (max 512), and check the independently
pinned Ed25519 public key. Tampered self-rehashed economics, missing
certificates, forged signatures, gap, duplicated row, wrong writer,
old manifest and expiry all HALT.

`GuardianReadonlySession` now REQUIRES a separate
`readSignedDurablePageAfter(cursor,limit,manifest)` source for all
replay and restart. The existing canonical `guardianReadOnlyJournalSource`
is explicitly typed as a raw read-only data source, **not** a complete
Guardian authority. A signed manifest alone can no longer authorize
unsigned durable rows. A session that cannot obtain certificates stalls
closed; no cursor ACK or new view follows.

Offline end-to-end tests use the real MemoryEventJournal + a separately
generated test producer signature to exercise verified paging. An
attacker-rehashed row presented with the original signed certificate
halts and leaves the consumer ACK unchanged.

## Test and API evidence

- Eight direct signed-page contract tests, two new integrated negative
  session tests, existing session and actual-journal suite retained.
- Focused **29/29 PASS** across four files.
- Dedicated `tsconfig.guardian-source.json`: TypeScript PASS.
- Production writer/runtime, database and market sockets NOT modified.

## Important remaining authority and operations boundaries

A signer MUST be **the exclusive canonical writer** under an independently
durable epoch/fencing mechanism, not a proxy that blindly signs
untrusted client-supplied PostgreSQL rows. Signature issuance must occur
after confirmed durable commit, with unknown-commit/integrity faults
fencing it. The signed-page API is a contract, not an operational
signing endpoint, HSM, IPC implementation or running process. The local
tests intentionally supply the key as a closed fixture; **this MUST NOT
be replicated in a read-only production process**.

The source/certificate delivery path, own-issued row guarantees, process
isolation and authenticated transport remain unresolved, as do real
PostgreSQL ACL validation, realistic p95/p99 sustained throughput,
UP/DOWN feed delivery and durable writer epoch.

The design follows ordered replay and explicit ACK/failover invariants
described by PostgreSQL logical-decoding and NATS JetStream documentation:
https://www.postgresql.org/docs/16/logicaldecoding-explanation.html
https://docs.nats.io/concepts/jetstream

**NO SHADOW / PAPER ONLY / NO VPS / NO LIVE / NO DEPLOY / NO PUSH / NO MERGE.**

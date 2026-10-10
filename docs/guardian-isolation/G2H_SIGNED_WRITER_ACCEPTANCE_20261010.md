# G2H | Independent Ed25519 Writer Checkpoint Manifest

Date: 2026-10-10. Parent: `b348075`.
Gate: **G2H IN PROGRESS**. No live signing authority introduced.

## Concrete authority problem

`listVerifiedCheckpoints()` supplies payload and its own stored SHA-256.
That detects corruption but cannot authenticate a writer. An unrelated
DB actor can self-hash changed content. G2H must not treat this as a producer
attestation. The Polymarket market websocket also does not authenticate the
canonical Engine writer.

## Offline source protocol

- Added `guardian-signed-writer-manifest.ts`. Validates strictly shaped,
  fixed-order domain-separated Ed25519 attestations of exact checkpoint ID,
  content hash, experiment, durable high-water, authority ID, issuer key ID,
  writer epoch, manifest sequence, and short validity window.
- Public verification key MUST be pinned independently of PostgreSQL.
  Monotonic epoch / manifest sequence floor MUST be supplied independently.
  No private key is read or stored by this reader.
- A valid manifest pins the existing G2H checkpoint bootstrap; the database
  checkpoint and durable cursor must agree with the signature envelope.
  Tampered / expired / replayed / wrong-issuer documents halt.
- This is cryptographic **proof of signing-key possession**, not proof that a
  single canonical writer holds an exclusive operating lease. Competing
  producers sharing the signing key or a compromised issuer remain threats.
- A correct live system still needs trusted signer placement, key provisioning,
  durable epoch fencing and manifest re-attestation on each heartbeat/read.

## Tests

- New six Ed25519 tests exercise signature, forged self-hashed document,
  wrong key, epoch floor and sequence replay, expiry, malformed envelopes,
  canonical checkpoint adoption, source high-water agreement.
- `PM_FORENSIC_STAGE=08_current` focused **6/6 PASS**.
- Forensic Engine TS PASS.

## Rollback and restrictions

Revert only this small commit if its signature contract is rejected. No
engine runtime, journal, DB, wallet, market socket, remote or deployment
touched. **NO SHADOW / PAPER ONLY / NO VPS / NO PUSH / NO MERGE**.

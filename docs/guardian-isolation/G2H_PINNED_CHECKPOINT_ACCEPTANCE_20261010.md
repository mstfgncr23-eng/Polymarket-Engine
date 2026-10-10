# G2H | Pinned Read-Only Checkpoint Adoption

Date: 2026-10-10
Branch: `refactor/guardian-isolation-atomic`
Parent: `3b0b5f1`
Gate: **G2H IN PROGRESS**, one narrowly scoped offline checkpoint-handoff atom.

## Reason

The existing `loadLatestCheckpoint` / `listVerifiedCheckpoints` verify
`hashCheckpointPayload(payload) === payload.checkpointContentHash`. This detects
accidental corruption, but the digest is stored with the payload and is NOT
proof of writer identity. Blindly forwarding that DB-returned digest into the
G2F `externallyVerifiedContentHash` parameter would be circular trust.

## Changes

- `apps/engine/src/guardian-readonly-checkpoint.ts` accepts an exact
  independently provided checkpoint ID + SHA256 + experiment ID and opaque
  authority ID, alongside a read-only `listVerifiedCheckpoints` / high-water
  interface. No fallback to an unpinned older checkpoint.
- Rejects missing/duplicate checkpoints, unverified or tampered content,
  cross-experiment records, cursor regression, excessive replay backlog,
  malformed source and upstream exceptions. Reuses G2F bootstrap and its
  economic checkpoint invariants. Unresolved report-source integrity remains
  a fatal rejection.
- Returns immutable offline Guardian projection/cursor, checkpoint ID and
  observed high-water only. Does NOT start a Guardian, submit orders, mutate
  DB, open a socket, or create a second economic authority.

## Test evidence

- Six new deterministic G2H tests (G2H-07..12).
- `PM_FORENSIC_STAGE=08_current`: focused offline source **34/34 PASS**;
  scoped broad **232/232 PASS, 12 test files**.
- Engine forensic TypeScript: PASS; core TypeScript: PASS;
  `git diff --check`: PASS.

## Security and ordering limits (OPEN, not waved through)

- The pin **MUST be issued and authenticated independently** of the DB read;
  this atom does NOT create a trusted issuer, signing key, lease, or
  monotonic writer incarnation. Supplying a digest copied from the same DB
  query is NOT external attestation. This module may be exercised with
  closed fixtures, but is not an operational auth scheme.
- The existing `listVerifiedCheckpoints` reader is bounded to recent
  checkpoints. If the exact pin is no longer present, adoption HALTS instead
  of silently choosing a different checkpoint.
- High-water is an observation, not a repeatable-read transactional snapshot
  spanning checkpoint + pages, and not a durable consumer ACK.
- Retention/compaction gap and unknown commit remain HALT + authoritative
  reconcile. No fabricated durable rows, repair, or guessed events.
- G2H still needs independently authenticated producer epoch/snapshot,
  durable consumer ACK/restart/halt supervisor, fresh synchronized UP/DOWN
  book protocol and bounded queue/CPU/lag validation.
- G2I **single independent process** is not built or run.

## Rollback

Revert ONLY this atom's commit if its interface/test constraints are wrong;
do not revert G2G or the prior G2H page-feed commit. No production rollback
is implied because nothing in this atom has been deployed.

NO SHADOW / PAPER ONLY / NO VPS / NO LIVE / NO PUSH / NO MERGE.

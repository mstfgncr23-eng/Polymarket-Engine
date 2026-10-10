# G2H Current Authority | Acceptance Gate (2026-10-10)

**Status: NOT ACCEPTED, still in progress.** Do not advance G2I,
publish a PR, merge, deploy, start a second Guardian, shadow, or authorize
live PAPER/VPS source reads from these tests.

Branch: `refactor/guardian-isolation-atomic`.
This is the authoritative status summary for G2H atoms after
`b348075`. Earlier atom acceptance notes are evidence, not gate signoff.

## Closed local code paths, tested

| Required component | Evidence | Limit |
|---|---|---|
| Single pinned checkpoint and canonical global durable cursor | `guardian-readonly-checkpoint.ts`, `guardian-readonly-durable-feed.ts` | Reader only; DB checkpoint hash itself is not issuer proof |
| Source issuer authenticity | `guardian-signed-writer-manifest.ts`, Ed25519 signature + pinned public key + floor | Signer/key and lease not provisioned in real writer |
| Canonical producer manifest recipe | `guardian-writer-manifest-publisher.ts` | Offline only; caller's VERIFIED witness and durable epoch are assumptions |
| Real EventJournal read API boundary | `guardian-real-journal-source.ts`, actual MemoryEventJournal in readonly mode | No PostgreSQL account/TLS, no production connection |
| Durable read-only consumer ACK | `guardian-consumer-ack-store.ts`, `guardian-readonly-session.ts` | Crash/Windows fsync not independently proven |
| Strict replay/history parity | Exact ACK cursor, whole recovered economic digest, no cursor skip, retention gap halts | Rows still self-hashed by DB, not individually writer-signed |
| Source book pairing | `guardian-paired-book-channel.ts`, `guardian-normalized-book-bridge.ts` | Bridge is NOT wired to actual adapter callbacks or process IPC |
| Bounded resource / fail-closed | 512 levels/side, 512 rows/page, 8192 replay keys since CP, bounded lag, 100 synthetic 100Hz rows | NOT a realistic continuous load, real p95/p99 CPU or 8/8 reconnect validation |
| No economic/parallel authority | Offline pure modules and memory fixture runs; no submit API | Single independent process belongs G2I, NOT launched |

## Critical remaining implementation and acceptance

1. **Real producer ownership:** establish a durable, exclusive writer
   incarnation/lease and trusted signer placement in the canonical Engine
   `durableGate`, revalidate `PERSISTENCE_OUTCOME_UNKNOWN` /
   `REPORT_SOURCE_INTEGRITY_FAULT` as hard stops. Persist producer epoch
   and signature issuance floors outside mutable consumer DB rows.
   Signing a caller-supplied VERIFIED flag alone is NOT proof.
2. **Authenticated, coherent handoff:** issue and externally verify a signed
   checkpoint/row highwater with complete DB transaction consistency, real
   per-row source provenance or independent strict read-only PostgreSQL ACL
   and authenticated transport. Explicitly test commit-time uncertainty,
   backdated/altered payload, retention compaction, concurrent producer,
   failover/epoch change and late pages. A self-hashed altered durable row
   is currently undetectable as a producer forgery by itself.
3. **True market adapter delivery:** connect the existing canonical
   `packages/market-data/src/polymarket.ts` normalization and
   `isTokenSynchronized()/tokenEpoch()` semantics through an authenticated,
   bounded channel. Wire source-driven actual full UP/DOWN books, reconnect,
   invalidation and source heartbeat, with no raw second WebSocket/shadow.
4. **Operations/performance evidence:** locally test real PostgreSQL read-only
   mode and sustained realistic log+book arrival rates, lag p95/p99,
   memory/CPU and fencing. The 8192-key duplicate-history window can halt
   at ~82s if no new authenticated checkpoint is issued at 100 rows/s.
   This must be tuned/measured without lossy key eviction. No VPS is permitted.
5. **G2I only after G2H:** one offline isolated supervised process using
   this source path as the **only** Guardian, request-only decisions, no wallet,
   no shadow, no canonical executor duplication.

## Safety and compatibility

The full Engine tsconfig still has an **existing, unrelated**
`apps/engine/src/operator-report.ts` missing `./source-seal.js` import.
Do not silently repair/report-source seal. `tsconfig.guardian-source.json`,
Engine forensic and core typechecks are supported and separately asserted.
No code here has touched production, database, runtime, wallet, deployment,
remote branches, push, or LIVE.

**NO SHADOW / NO DUAL WRITER / PAPER ONLY / NO VPS / NO PUSH / NO MERGE.**

# G2H | H2 retained checkpoint and H3 standalone books | 2026-10-11

Baseline: `815668a` on `refactor/guardian-isolation-atomic`.
Status: H2 **PARTIAL**, H3 **PARTIAL**, G2H **NOT DONE**.
Binding scope: `G2H_REQUIREMENTS_FROZEN_20261011.md`.
No VPS, production DB, autonomous Guardian, wallet, LIVE/PAPER trade,
shadow, deploy, push, merge or architecture rewrite.

**Final scoped verification:** 358/358 tests PASS across 26 files,
including 14/14 real isolated PostgreSQL integration tests and
5/5 independent scripted market-data transport tests. All of
`@pm/db` TS, Guardian-source TS, forensic Engine TS and
`@pm/core` TS PASS; `git diff --check` PASS.
This is NOT G2H production acceptance, as target filesystem,
checkpoint cadence and four-shard availability are not yet evidenced.

## H2: Existing checkpoint outside the newest 64

- Current `readGuardianConsistentPage` selects up to 64 most recent
  verified checkpoints for one experiment. A consumer receipt can point
  to an older **STILL RETAINED** checkpoint not in that list. Previously
  a correct restart would fail despite accessible canonical source.
- Added OPTIONAL `pinnedCheckpointId` read parameter. The PostgreSQL
  reader retrieves this exact `id + experiment_id` in the **SAME
  REPEATABLE READ READ ONLY transaction** if outside the newest 64.
  The checkpoint is still hash/semantic/high-water validated. No
  modified checkpoint authority, DB write or magic fallback.
- The no-sign `GuardianTrustedPgSession` supplies pinned ID on restart,
  paged replay and normal polling. The new ID is never selected by
  guessing a different checkpoint. Missing/revoked/RLS-hidden pinned
  checkpoint still HALTS.
- In disposable PostgreSQL 17, the test inserts 70 extra valid CP rows:
  default returns newest 64, the exact requested retained CP returns
  as the 65th, and the previously ACKed consumer restores and polls.
- Additional disposable PostgreSQL test uses the **real**
  `compactDurableTransitionsThrough` maintenance API. It confirms
  a canonical checkpoint at the committed cursor permits safe restart
  after prior transitions are compacted; attempting to replay pruned
  history fails closed; removing the pinned checkpoint HALTS both poll
  and restart without advancing the observation ACK.
- This is NOT a retention lease: the actual Engine's maintenance
  writer may prune a checkpoint before a reader adopts a successor.
  Coordination / latency / stall behavior under production-like
  checkpoint cadence remains an H2 acceptance blocker. The observer
  cannot instruct maintenance to keep a checkpoint or write its own
  economic checkpoint.

## H3: independent market-data adapter, not an Engine callback alias

- `guardian-standalone-book-observer.ts` runs the **existing**
  `PolymarketMarketDataAdapter.run()` directly from the single observer
  owner. It registers canonical UP/DOWN tokens; receives normalized
  `MarketSnapshot`, `onConnection` and `onGap`; reads the adapter's
  actual `isTokenSynchronized` and `tokenEpoch`; feeds the existing
  `GuardianNormalizedBookBridge` and `GuardianPairedBookChannel`.
- It has no trade submission, EngineService, extra Guardian process,
  independent order writer, new WS parser or unbounded ingress.
- **Concrete integration bug found and fixed:** the UP normalized
  full-book arrived before DOWN was synchronized. The bridge correctly
  refused the first UP; when DOWN arrived, no UP remained to pair.
  The wrapper now retains at most TWO current-epoch normalized full
  snapshots per market. When both adapter tokens are synchronized and
  epochs still match, it forwards both. Disconnect, gap and epoch
  invalidation flush the affected cached pair. Freshness checks are
  still enforced by the existing paired-book channel.
- `tests/guardian-standalone-book-observer.test.ts` uses only the
  built-in `ScriptedBoundedMarketTransport`, without a network socket.
  5 tests assert eight markets/16 token outcomes become 8/8 READY;
  websocket reconnect invalidates all; 7/8 cannot become 8/8 until
  both new books return; old token epoch blocked until fresh paired
  books; invalid duplicate instance / concurrent `run` rejected;
  stale one-sided books remain BLOCKED; abort clears readiness.
- Single-process read-only test fixture establishes adapter
  independence from `EngineService`, not actual production process
  supervision or G3's execution authority.

## Remaining FROZEN acceptance gates, no scope expansion

H2:
- Checkpoint write cadence, retained CP visibility, regular operation
  across compacted tails and CP rotation under actual representative
  durable transition rate remain UNKNOWN.
- Target Linux filesystem fsync / abrupt power-loss, stale ACK lock
  runbook and failover integrity require separate evidence.
- Tested local MVCC / compaction transaction and stale pin failures,
  not production failover or actual PostgreSQL role/TLS deployment.

H3:
- Dedicated adapter's 4-shard reconnect / reconnect-before-full-book
  behavior under 40 qualified markets and sustained load not yet
  demonstrated. Current conservative disconnect callback invalidates
  all markets when shard attribution is unclear. This prevents stale
  READY but could reduce availability until fresh full books arrive.
- Standalone runner is an H3 source contract, not the supervised
  Guardian process (G2I) or dual-active shadow.
- Unknown venue source age and queue gap metrics at target measured
  load are evaluated in H4.

All optional nonblocking ideas remain in
`G2H_DEFERRED_IMPROVEMENTS_BACKLOG.md`. Live-risk policy and atomic
executor admission remain G3, not H2/H3.

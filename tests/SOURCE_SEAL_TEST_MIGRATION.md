# FAZ A: source-seal legacy test migration

Branch: `implement/simple-engine-guardian`
HEAD: `e6c24e832083ea3ddbd061906ba95bc41d28b8bc` (unchanged).

## Scope and cause

Two collected suites imported `apps/engine/src/source-seal.js`, removed in
`01ced7c977c6ed5378908079ceb83a074e63d74f`. Their test-only Engine seal methods
were also removed. This migration changes tests and this ledger only. It does
not restore a source-seal runtime, introduce an authority, change Guardian
thresholds or Profit-Trailing 12/5/2, or alter prior Phase A fixes.

The original suites contained **7 + 81 = 88 tests**. Every original test has
an active counterpart below. One additional post-snapshot integrity failure
test makes **89 tests**. No original test is deleted or newly skipped.
The optional real PostgreSQL test reuses the existing
`SIMPLE_RETENTION_POSTGRES_TEST=1` fixture and existing local container in an
isolated temporary schema, instead of requiring the retired Step-4 database.
No production/PAPER data or processes are used.

## Reasoned deprecations of obsolete expectations

| Removed expectation | Why obsolete | Active protection retained |
| --- | --- | --- |
| `EXPERIMENT_SOURCE_SEALED`, `loadSourceSeal`, `sealed` flags, per-seal generation | No current producer/module/authority; a marker cannot prove actual completion or source integrity. | Completed lifecycle, tracked canonical admission/research/Guardian drain, repeatable snapshot fences, integrity checks, canonical report persistence and restart hash equality. |
| `SOURCE_SEAL_MISSING` forbids an otherwise intact completed operator report | Current operator report intentionally authorizes durable completed identity plus stable complete scan and no unresolved integrity. | Test both intact report success **without** a seal and damaged source rejection. Running/entry-closed/missing identity/conflicting hashes still fail. |
| `source_sealed` Grok result | Current ingress uses experiment window and generation checks with `stale`. | Held claim rejected after completion; zero accepted/proposed/consensus/risk rows; final report deferred while ingress exists. |
| Seal-specific 40/80/100/200/250 ms timers and persistence holds | No seal persistence operation exists. Current final report defers while runtime work remains; it does not invent a seal timer. | Actual Engine `persistReportInput` abort/deadline admission, journal acquire/in-flight deadlines, release cannot resurrect a cancelled event, exact retry and restart. |
| Seal-only single flight | Engine has no seal flight to coalesce. | Current `KeyedResultFlight` primitive, already used by complete metrics; adapter COMMIT stays pending past deadline, shared work completes once. This is a primitive-level test, not a claim that every Engine write coalesces. |
| Every held COMMIT can be aborted at the deadline | Once PostgreSQL COMMIT is irreversible, elapsed client time/absent row alone cannot prove rollback. | Real PG17 held **open transaction after INSERT** exercises server `transaction_timeout`; model separately proves irreversible pending success and late commit after transport failure. No client timeout is presented as a server rollback. |
| `isSourceSealExemptType` | Classification belongs to current report-source-integrity boundary policy. | `isReportInputBoundaryType`: every canonical type is critical/fail-closed or latches integrity; boundary exceptions and operational exclusions are enumerated. |
| Automatic checkpoint purge without a recovery archive | Current retention deliberately requires verified archived candidates and two retained usable snapshots. | Budget test now prepares a local test archive through the existing verifier; the 50-row limit and latest snapshot assertions stay exact. No production retention setting changes. |

## Current code counterparts

| Control | Actual current path | Evidence asserted |
| --- | --- | --- |
| Canonical window / authenticated audit | `EngineService.isReportInputPersistenceAdmitted`, `persistReportInput`, `isCanonicalWindowAdmission`; `mcp.ts` | MCP always-on audit remains durable; canonical work is tracked during window; BoundedJournalWriter rejects untracked canonical input. |
| Research completion fencing | `beginResearchIngress`, `endResearchIngress`, `researchIngressStale`, completion generation increment in `commitExperimentStatus`; `submitGrokSignal` | Held pre-completion Grok rejects after generation/window changes and creates no economic/strategy fact. |
| Runtime drain / finality recheck | `finalReportRuntimeUnresolved`, `finalReportRuntimeDrained`, `writeFinalReportAttempt` | Pending TRIGGER/MCP/research prevents final report; release and healthy persistence permit it; fault during drain and **after snapshot** still prevents it. Guardian persistence remains part of existing runtime predicate. |
| Sticky and crash-durable loss accounting | `safeAppend`, `noteReportSourceIntegrityFault`, `restoreReportSourceIntegrity`, `repairReportSourceIntegrityForTest`; `report-source-integrity.ts` | Unrelated success does not clear fault; durable fallback/restart/overflow/orphan claims tested; exact repaired event required. |
| Stable bounded source | `streamExperimentSummary`, `scanJournalEvents`, `withReadSnapshot`, commit sequence fence | 100k scan/page bounds, equal timestamps, late commits/old timestamps, interruption, retained entity bounds. |
| Canonical report authority | `persistCanonicalFinalReport`, `resolveAuthoritativeExperiment`, `generateOperatorReport`, report conflict-safe persistence | Completed identity + stable complete non-truncated scan + integrity; same hash idempotent/conflict fails closed; filesystem failure cannot erase canonical DB row; audit repair once. |
| Append outcome | `PostgresEventJournal.append`, `JournalAppendAbortedError`, `pg_xact_status` | Actual server abort or failed callback needed for deadline classification; irreversible commit success and unknown transport outcome remain distinct. |
| Lifecycle-safe retention | `resolveCleanupLifecycle`, `RetentionManager`, `applyJournalCleanup`, verified recovery archive | Active/unknown/conflicting identities protected, durable authority overrides derived audit, bounded global budgets, canonical/durable facts retained, checkpoint archive prerequisite. |
| Operator capabilities | journal access modes, schema verification | Readonly/verify-only cannot migrate or mutate; report-write limited; missing/invalid indexes and RLS fail closed. These existing controls are tested, no new permission system added. |

## One-to-one original test ledger

The exact original titles and line numbers below refer to the unchanged HEAD
versions captured before migration. Active counterpart line numbers refer to
the final patch. `RETAINED` keeps the original control; `MIGRATED` replaces a
removed seal observation or updates its current fixture/API. Deprecations apply
to obsolete assertions listed above, **never to the underlying safety test**.

### `stability-step4-commit-deadline.test.ts` (7 original tests)

| # / original line and title | Active line and title | Treatment / current control |
| --- | --- | --- |
| 1. L149: preserves a confirmed adapter deadline abort even if the client wall clock still has time remaining | L123: preserves confirmed adapter deadline abort even when the client wall clock has time remaining | MIGRATED: persistReportInput / Postgres append: confirmed deadline, no late write, safe retry |
| 2. L167: classifies abort/deadline and transient journal errors without relying on instanceof identity | L141: classifies abort/deadline and transient journal errors without relying on instanceof identity | RETAINED: isJournalAppend* / isTransientJournalError: structural error classification |
| 3. L179: REPEATABLE READ keyset SQL binds Date fences as ISO strings for postgres.js unsafe | L153: REPEATABLE READ keyset SQL binds Date fences as ISO strings for postgres.js unsafe | RETAINED: buildJournalKeysetSql: typed immutable ISO fence bindings |
| 4. L201: real PG17: held transaction after INSERT aborts before timeout result, then retry/restart produce one canonical seal/report | L176: real PG17: INSERT followed by held open transaction is server-aborted; retry/restart preserves one input/report | MIGRATED: isolated PG17 append transaction_timeout, retry idempotency and Engine report/restart |
| 5. L299: successful INSERT near deadline, held COMMIT: timeout aborts; release cannot commit; retry/restart finality is idempotent | L233: successful INSERT near deadline, held open transaction: confirmed abort prevents late commit; retry is idempotent | MIGRATED: persistReportInput / Postgres append: confirmed deadline, no late write, safe retry |
| 6. L353: irreversible COMMIT crossing deadline cannot be called a timeout; concurrent retry shares its eventual success | L262: irreversible COMMIT crossing deadline stays pending; the current keyed-flight primitive shares eventual success | MIGRATED: Postgres append + KeyedResultFlight: pending COMMIT and shared success |
| 7. L380: unknown transport outcome at COMMIT is a persist failure, never proof of timeout/rollback | L284: unknown transport outcome during irreversible COMMIT is not proof of deadline rollback | MIGRATED: Postgres append: ambiguous transport must not prove rollback |

Original SHA-256 (raw bytes): `46ff769eef3995b78e6dee43ad60b17b7b2367b0a0e9c1757f5d263d93940f9d`.

### `stability-step4-reporting-retention.test.ts` (81 original tests)

| # / original line and title | Active line and title | Treatment / current control |
| --- | --- | --- |
| 1. L130: streamed accumulator matches array summary field-for-field on the authoritative fixture | L132: streamed accumulator matches array summary field-for-field on the authoritative fixture | RETAINED: bounded keyset snapshot/commit fence and ExperimentSummaryAccumulator |
| 2. L145: 100k+ scan retains a page + bounded accumulator, not an event-count array | L147: 100k+ scan retains a page + bounded accumulator, not an event-count array | RETAINED: bounded keyset snapshot/commit fence and ExperimentSummaryAccumulator |
| 3. L170: equal occurredAt neither duplicates nor skips | L172: equal occurredAt neither duplicates nor skips | RETAINED: bounded keyset snapshot/commit fence and ExperimentSummaryAccumulator |
| 4. L186: scan interruption marks complete=false and does not persist a canonical final | L188: scan interruption marks complete=false and does not persist a canonical final | RETAINED: streamExperimentSummary / canonical conflict-safe persistence / authoritative identity and audit repair |
| 5. L201: source fence excludes late writes deterministically | L203: source fence excludes late writes deterministically | RETAINED: bounded keyset snapshot/commit fence and ExperimentSummaryAccumulator |
| 6. L219: same hash is idempotent; conflicting hash fails closed; FS failure keeps canonical | L221: same hash is idempotent; conflicting hash fails closed; FS failure keeps canonical | RETAINED: safeAppend / report-source-integrity / canonical report guard; sticky, exact repair and crash recovery |
| 7. L245: completed complete-scope reads the persisted report; running complete does not allocate a full-history array | L247: completed complete-scope reads the persisted report; running complete does not allocate a full-history array | RETAINED: streamExperimentSummary / canonical conflict-safe persistence / authoritative identity and audit repair |
| 8. L270: GET /api/traces enforces a hard per-request page cap | L272: GET /api/traces enforces a hard per-request page cap | RETAINED: current retention-manager / cleanup-lifecycle / journal-cleanup bounded lifecycle-safe behavior |
| 9. L299: classifies telemetry, monitor, and audit; durable_transitions is never a journal delete class | L301: classifies telemetry, monitor, and audit; durable_transitions is never a journal delete class | RETAINED: streamExperimentSummary / canonical conflict-safe persistence / authoritative identity and audit repair |
| 10. L308: active experiment report-input is not pruned; completed without final report is not pruned; truncated cannot unlock | L310: active experiment report-input is not pruned; completed without final report is not pruned; truncated cannot unlock | RETAINED: streamExperimentSummary / canonical conflict-safe persistence / authoritative identity and audit repair |
| 11. L341: complete+grace archives telemetry but keeps experiment-attributed monitor detail without a verified rollup | L343: complete+grace archives telemetry but keeps experiment-attributed monitor detail without a verified rollup | RETAINED: current retention-manager / cleanup-lifecycle / journal-cleanup bounded lifecycle-safe behavior |
| 12. L370: retention failures do not corrupt trading state and skip when degraded | L372: retention failures do not corrupt trading state and skip when degraded | RETAINED: safeAppend / report-source-integrity / canonical report guard; sticky, exact repair and crash recovery |
| 13. L388: dry-run cleanup mutates nothing and APPLY refuses unsafe experiment cleanup | L390: dry-run cleanup mutates nothing and APPLY refuses unsafe experiment cleanup | RETAINED: current retention-manager / cleanup-lifecycle / journal-cleanup bounded lifecycle-safe behavior |
| 14. L409: expired key removed still rejects a stale replay | L411: expired key removed still rejects a stale replay | RETAINED: idempotency TTL plus submission freshness; stale replay cannot reopen |
| 15. L451: prunes older checkpoints and leaves the latest sequence | L453: prunes older checkpoints and leaves the latest sequence | RETAINED: verified checkpoint recovery/archive and bounded purge; latest usable snapshot retained |
| 16. L463: engine recovers after older checkpoints are pruned | L465: engine recovers after older checkpoints are pruned | RETAINED: verified checkpoint recovery/archive and bounded purge; latest usable snapshot retained |
| 17. L496: missing durable_transitions fails closed; RLS invariants are required on new tables | L498: missing durable_transitions fails closed; RLS invariants are required on new tables | RETAINED: evaluateSchemaInspection / inspectPublicSchema / verifyRequiredSchema fail closed |
| 18. L543: keeps supervisor and journal admission constants | L545: keeps supervisor and journal admission constants | RETAINED: RUNTIME_BOUNDS / EVENT_PAGE_SIZE fixed application resource limits |
| 19. L559: dry-run and readonly access cannot invoke migrate or mutating SQL | L561: dry-run and readonly access cannot invoke migrate or mutating SQL | RETAINED: connectWithoutMigrate / journal capability guards; zero migration/mutation |
| 20. L587: persists a canonical final only for the proven COMPLETED experiment | L589: persists a canonical final only for the proven COMPLETED experiment | MIGRATED: streamExperimentSummary / canonical conflict-safe persistence / authoritative identity and audit repair |
| 21. L641: excludes an event created before the fence but committed after capture | L651: excludes an event created before the fence but committed after capture | RETAINED: bounded keyset snapshot/commit fence and ExperimentSummaryAccumulator |
| 22. L665: monitor-only / no experiment allows monitor TTL; active protects in-window; completed needs final+grace | L675: monitor-only / no experiment allows monitor TTL; active protects in-window; completed needs final+grace | RETAINED: idempotency TTL plus submission freshness; stale replay cannot reopen |
| 23. L708: multi-batch drain converges an eligible backlog instead of growing unbounded | L718: multi-batch drain converges an eligible backlog instead of growing unbounded | RETAINED: current retention-manager / cleanup-lifecycle / journal-cleanup bounded lifecycle-safe behavior |
| 24. L740: missing RETENTION_ENABLED does not enable automatic journal deletion | L750: missing RETENTION_ENABLED does not enable automatic journal deletion | RETAINED: current retention-manager / cleanup-lifecycle / journal-cleanup bounded lifecycle-safe behavior |
| 25. L754: concurrent same-hash persist is idempotent; conflicting hash fails closed | L764: concurrent same-hash persist is idempotent; conflicting hash fails closed | RETAINED: streamExperimentSummary / canonical conflict-safe persistence / authoritative identity and audit repair |
| 26. L773: repairs EXPERIMENT_REPORT_GENERATED exactly once after persist-then-audit failure | L783: repairs EXPERIMENT_REPORT_GENERATED exactly once after persist-then-audit failure | RETAINED: safeAppend / report-source-integrity / canonical report guard; sticky, exact repair and crash recovery |
| 27. L795: complete-metrics flights are keyed; experiment B does not receive A | L805: complete-metrics flights are keyed; experiment B does not receive A | RETAINED: current retention-manager / cleanup-lifecycle / journal-cleanup bounded lifecycle-safe behavior |
| 28. L807: accumulator retained state follows entity cardinality, not event count | L817: accumulator retained state follows entity cardinality, not event count | MIGRATED: bounded keyset snapshot/commit fence and ExperimentSummaryAccumulator |
| 29. L828: holds a pre-completion TRIGGER_EVENT until the source seal drains | L838: holds a pre-completion TRIGGER_EVENT and defers finality until tracked admission drains | MIGRATED: writeFinalReportAttempt runtime drain + immutable completed source; report hash and restart |
| 30. L918: source-seal timeout refuses a canonical final | L924: unresolved runtime work refuses a canonical final | MIGRATED: writeFinalReportAttempt runtime drain + immutable completed source; report hash and restart |
| 31. L956: rejects every mutating method under readonly and verify-only | L959: rejects every mutating method under readonly and verify-only | RETAINED: connectWithoutMigrate / journal capability guards; zero migration/mutation |
| 32. L992: never deletes more than maxRowsPerCycle for non-divisible budgets | L995: never deletes more than maxRowsPerCycle for non-divisible budgets | RETAINED: current retention-manager / cleanup-lifecycle / journal-cleanup bounded lifecycle-safe behavior |
| 33. L1016: dry-run cutoffs match RetentionManager for a non-default config | L1019: dry-run cutoffs match RetentionManager for a non-default config | RETAINED: safeAppend / report-source-integrity / canonical report guard; sticky, exact repair and crash recovery |
| 34. L1066: fails when a required index is missing | L1069: fails when a required index is missing | RETAINED: evaluateSchemaInspection / inspectPublicSchema / verifyRequiredSchema fail closed |
| 35. L1074: fails when a required index is INVALID | L1077: fails when a required index is INVALID | RETAINED: evaluateSchemaInspection / inspectPublicSchema / verifyRequiredSchema fail closed |
| 36. L1081: fails closed when index-validity inspection throws | L1084: fails closed when index-validity inspection throws | RETAINED: evaluateSchemaInspection / inspectPublicSchema / verifyRequiredSchema fail closed |
| 37. L1093: passes when every required index is present and valid | L1096: passes when every required index is present and valid | RETAINED: evaluateSchemaInspection / inspectPublicSchema / verifyRequiredSchema fail closed |
| 38. L1099: held Grok submission is drained or rejected and cannot accept after the source seal | L1102: held Grok submission is drained or rejected and cannot accept after experiment completion | MIGRATED: submitGrokSignal: claim identity, generation/window fence and missing-fact rejection |
| 39. L1176: PRE-APPEND HOLD: stalls SOURCE_SEAL persistence itself and cannot later create a seal after timeout | L1176: already-aborted admission cannot create a late canonical input; healthy retry is idempotent | MIGRATED: persistReportInput: cancelled admission creates no input; sticky fault and exact durable repair |
| 40. L1224: deletes at most the remaining 50-row cycle budget from a 200+ checkpoint backlog | L1213: deletes at most the remaining 50-row cycle budget from a 200+ checkpoint backlog | MIGRATED: verified checkpoint recovery/archive and bounded purge; latest usable snapshot retained |
| 41. L1243: after telemetry consumes part of the budget, checkpoint deletes only the remainder | L1232: after telemetry consumes part of the budget, checkpoint deletes only the remainder | RETAINED: verified checkpoint recovery/archive and bounded purge; latest usable snapshot retained |
| 42. L1263: journal prune API never deletes more than the caller limit and keeps the latest usable | L1252: journal prune API never deletes more than the caller limit and keeps the latest usable | RETAINED: current retention-manager / cleanup-lifecycle / journal-cleanup bounded lifecycle-safe behavior |
| 43. L1291: maxBatches=3 is a global cap across telemetry and monitor, not 3 per class | L1280: maxBatches=3 is a global cap across telemetry and monitor, not 3 per class | RETAINED: current retention-manager / cleanup-lifecycle / journal-cleanup bounded lifecycle-safe behavior |
| 44. L1309: non-divisible maxRows is never exceeded | L1298: non-divisible maxRows is never exceeded | RETAINED: current retention-manager / cleanup-lifecycle / journal-cleanup bounded lifecycle-safe behavior |
| 45. L1327: time budget stops further batches | L1316: time budget stops further batches | RETAINED: current retention-manager / cleanup-lifecycle / journal-cleanup bounded lifecycle-safe behavior |
| 46. L1348: dry-run applyBounds equal the bounds APPLY actually uses | L1337: dry-run applyBounds equal the bounds APPLY actually uses | MIGRATED: current retention-manager / cleanup-lifecycle / journal-cleanup bounded lifecycle-safe behavior |
| 47. L1385: proves MCP_INTERACTION and writer/engine paths: no untracked report-input append | L1374: proves MCP_INTERACTION and writer/engine paths: no untracked report-input append | MIGRATED: tracked admission, BoundedJournalWriter canonical rejection, isReportInputBoundaryType inventory |
| 48. L1402: BoundedJournalWriter refuses report-input instead of appending untracked | L1392: BoundedJournalWriter refuses report-input instead of appending untracked | RETAINED: tracked admission, BoundedJournalWriter canonical rejection, isReportInputBoundaryType inventory |
| 49. L1410: IN-FLIGHT MCP_INTERACTION persist cannot be sealed ahead of; release yields stable hash across restart | L1400: IN-FLIGHT MCP_INTERACTION persist cannot be finalized ahead of; release yields stable hash across restart | MIGRATED: writeFinalReportAttempt runtime drain + immutable completed source; report hash and restart |
| 50. L1480: A: GROK_SIGNAL_ACCEPTED loss is sticky; later unrelated success does not clear; sampled telemetry does not poison | L1468: A: GROK_SIGNAL_ACCEPTED loss is sticky; later unrelated success does not clear; sampled telemetry does not poison | MIGRATED: safeAppend / report-source-integrity / canonical report guard; sticky, exact repair and crash recovery |
| 51. L1514: B: TRIGGER_EVENT loss refuses seal and canonical until verified durable repair | L1500: B: TRIGGER_EVENT loss refuses canonical finality until verified durable repair | MIGRATED: safeAppend / report-source-integrity / canonical report guard; sticky, exact repair and crash recovery |
| 52. L1544: C2: stale checkpoint fault is dropped after durable REPAIRED | L1531: C2: stale checkpoint fault is dropped after durable REPAIRED | MIGRATED: safeAppend / report-source-integrity / canonical report guard; sticky, exact repair and crash recovery |
| 53. L1575: C: restart with unresolved integrity must not silently forget or complete | L1563: C: restart with unresolved integrity must not silently forget or complete | MIGRATED: safeAppend / report-source-integrity / canonical report guard; sticky, exact repair and crash recovery |
| 54. L1623: A: resolves ACTIVE_RUNNING from durable EXPERIMENT_STARTED, not env | L1612: A: resolves ACTIVE_RUNNING from durable EXPERIMENT_STARTED, not env | RETAINED: current retention-manager / cleanup-lifecycle / journal-cleanup bounded lifecycle-safe behavior |
| 55. L1636: B: env hint mismatch fail-closes; never becomes PROVEN_NO_ACTIVE | L1625: B: env hint mismatch fail-closes; never becomes PROVEN_NO_ACTIVE | RETAINED: current retention-manager / cleanup-lifecycle / journal-cleanup bounded lifecycle-safe behavior |
| 56. L1651: C: missing EXPERIMENT_ID is not monitor-only while an experiment is active | L1640: C: missing EXPERIMENT_ID is not monitor-only while an experiment is active | RETAINED: current retention-manager / cleanup-lifecycle / journal-cleanup bounded lifecycle-safe behavior |
| 57. L1664: D: UNKNOWN forbids monitor delete, is visible on dry-run, and refuses APPLY | L1653: D: UNKNOWN forbids monitor delete, is visible on dry-run, and refuses APPLY | RETAINED: current retention-manager / cleanup-lifecycle / journal-cleanup bounded lifecycle-safe behavior |
| 58. L1692: E: RUNNING/ENTRY_CLOSED protect in-window; missing startedAt forbids monitor delete | L1681: E: RUNNING/ENTRY_CLOSED protect in-window; missing startedAt forbids monitor delete | RETAINED: current retention-manager / cleanup-lifecycle / journal-cleanup bounded lifecycle-safe behavior |
| 59. L1713: F: COMPLETED requires canonical complete+non-truncated + grace | L1702: F: COMPLETED requires canonical complete+non-truncated + grace | RETAINED: current retention-manager / cleanup-lifecycle / journal-cleanup bounded lifecycle-safe behavior |
| 60. L1730: G: PROVEN_NO_ACTIVE_EXPERIMENT is monitor TTL eligible | L1719: G: PROVEN_NO_ACTIVE_EXPERIMENT is monitor TTL eligible | RETAINED: idempotency TTL plus submission freshness; stale replay cannot reopen |
| 61. L1743: H: dry-run shows lifecycle state/source/id/status/times/allowed/reason | L1732: H: dry-run shows lifecycle state/source/id/status/times/allowed/reason | RETAINED: current retention-manager / cleanup-lifecycle / journal-cleanup bounded lifecycle-safe behavior |
| 62. L1762: DATABASE_URL-only (no EXPERIMENT_ID) while active experiment is SAFE | L1751: DATABASE_URL-only (no EXPERIMENT_ID) while active experiment is SAFE | MIGRATED: current retention-manager / cleanup-lifecycle / journal-cleanup bounded lifecycle-safe behavior |
| 63. L1782: IN-FLIGHT DATABASE WRITE HOLD: stall inside EventJournal source-seal append cannot later commit a timed-out seal | L1771: in-flight hold cannot commit a confirmed deadline-aborted input; retry and restart keep one canonical report | MIGRATED: Engine persistReportInput + Memory journal deadline, no late append, retry/report/restart |
| 64. L1836: A: durable EXPERIMENT_STARTED without derived audit is ACTIVE_RUNNING | L1821: A: durable EXPERIMENT_STARTED without derived audit is ACTIVE_RUNNING | RETAINED: streamExperimentSummary / canonical conflict-safe persistence / authoritative identity and audit repair |
| 65. L1848: B: two durable active identities are CONFLICTING | L1833: B: two durable active identities are CONFLICTING | RETAINED: current retention-manager / cleanup-lifecycle / journal-cleanup bounded lifecycle-safe behavior |
| 66. L1858: C: only no durable and no legacy journal lifecycle is PROVEN_NO_ACTIVE | L1843: C: only no durable and no legacy journal lifecycle is PROVEN_NO_ACTIVE | RETAINED: current retention-manager / cleanup-lifecycle / journal-cleanup bounded lifecycle-safe behavior |
| 67. L1872: D: DATABASE_URL-only APPLY is SAFE in AFTER_APPLY_BEFORE_DERIVED_AUDIT | L1857: D: DATABASE_URL-only APPLY is SAFE in AFTER_APPLY_BEFORE_DERIVED_AUDIT | RETAINED: streamExperimentSummary / canonical conflict-safe persistence / authoritative identity and audit repair |
| 68. L1905: E: journal must not override durable ACTIVE to no-active or CONFLICTING | L1890: E: journal must not override durable ACTIVE to no-active or CONFLICTING | RETAINED: current retention-manager / cleanup-lifecycle / journal-cleanup bounded lifecycle-safe behavior |
| 69. L1917: F: durable_transitions are never pruned by APPLY | L1902: F: durable_transitions are never pruned by APPLY | RETAINED: current retention-manager / cleanup-lifecycle / journal-cleanup bounded lifecycle-safe behavior |
| 70. L1932: classifies every DECISION type exactly once; every canonical type has integrity or fail-closed semantics | L1917: classifies every DECISION type exactly once; every canonical type has integrity or fail-closed semantics | MIGRATED: safeAppend / report-source-integrity / canonical report guard; sticky, exact repair and crash recovery |
| 71. L1962: an in-flight Grok claim is not a successful duplicate and does not poison its healthy first caller | L1947: an in-flight Grok claim is not a successful duplicate and does not poison its healthy first caller | RETAINED: submitGrokSignal: claim identity, generation/window fence and missing-fact rejection |
| 72. L2002: A1: resolves an aborted start as terminal and never conflicts with its retry | L1987: A1: resolves an aborted start as terminal and never conflicts with its retry | RETAINED: current retention-manager / cleanup-lifecycle / journal-cleanup bounded lifecycle-safe behavior |
| 73. L2026: triple persistence failure rejects Grok, stays fail closed, and never reinterprets the orphan claim after crash | L2011: triple persistence failure rejects Grok, stays fail closed, and never reinterprets the orphan claim after crash | MIGRATED: safeAppend / report-source-integrity / canonical report guard; sticky, exact repair and crash recovery |
| 74. L2088: strategy proposal persistence failure never reinterprets an accepted-only claim as a duplicate after restart | L2075: strategy proposal persistence failure never reinterprets an accepted-only claim as a duplicate after restart | RETAINED: safeAppend / report-source-integrity / canonical report guard; sticky, exact repair and crash recovery |
| 75. L2147: A: double-failure + crash before checkpoint → restart refuses seal/canonical | L2134: A: double-failure + crash before checkpoint → restart refuses canonical finality | MIGRATED: safeAppend / report-source-integrity / canonical report guard; sticky, exact repair and crash recovery |
| 76. L2182: B: 10k/100k unique failures stay hard-bounded and fail closed | L2169: B: 10k/100k unique failures stay hard-bounded and fail closed | MIGRATED: safeAppend / report-source-integrity / canonical report guard; sticky, exact repair and crash recovery |
| 77. L2221: C: unrelated success cannot clear a latched fault | L2210: C: unrelated success cannot clear a latched fault | MIGRATED: safeAppend / report-source-integrity / canonical report guard; sticky, exact repair and crash recovery |
| 78. L2245: D: verified repair is exact-id; overflow cannot be cleared by one proven row | L2236: D: verified repair is exact-id; overflow cannot be cleared by one proven row | MIGRATED: safeAppend / report-source-integrity / canonical report guard; sticky, exact repair and crash recovery |
| 79. L2289: fault during drain refuses seal and leaves no EXPERIMENT_SOURCE_SEALED row | L2322: fault appearing during drain refuses canonical finality | MIGRATED: safeAppend / report-source-integrity / canonical report guard; sticky, exact repair and crash recovery |
| 80. L2323: COMPLETED+sealing with continuous post-window activity seals boundedly and keeps hash stable | L2354: COMPLETED with continuous post-window activity finalizes boundedly and keeps hash stable | MIGRATED: writeFinalReportAttempt runtime drain + immutable completed source; report hash and restart |
| 81. L2376: acquire-hold analogue: txn obtained near deadline cannot later commit a timed-out seal | L2415: acquisition hold cannot commit a confirmed deadline-aborted input; retry and restart keep one canonical report | MIGRATED: Engine persistReportInput + Memory journal deadline, no late append, retry/report/restart |

Original SHA-256 (raw bytes): `f90c5c9872cd10b6b8d19f627d40e09d4cf867d5695e3a7e27a1aa502abc18d2`.

## Added control

`a canonical loss injected after snapshot capture is rechecked before report persistence`
injects a real Engine integrity fault after its frozen snapshot work finishes.
No canonical report/audit is allowed until the exact event is durably repaired.
This adds coverage for the second guard in `writeFinalReportAttempt`.

## Verification scope

The real PG test proves a deadline abort of an **open** transaction after
INSERT, invisibility before/after abort, no late row, idempotent retry, read
snapshot parity and stable canonical hash across an Engine restart. Actual
irreversible server COMMIT/transport-cut execution is not claimed: these are
deterministic adapter transaction-seam models. `KeyedResultFlight` assertions
refer only to the existing shared-work primitive.

100k events/page-bounded scan and 10k fault injection/100k fault normalization
remain at their original sizes and timeouts. The post-window test producer
now yields to I/O and stops in `finally`, so an assertion failure cannot leave
an infinite microtask producer consuming the runner heap. This changes the
test harness only; no market consumer or Guardian economic timing changes.

For actual final suite commands, counts, exits, skipped-case inventory and
failed development attempts, see the delivered Phase A final validation report.

# Durable reports, journal retention, and one-time cleanup

Operator reference for Step-4. This document does **not** start the 48h experiment, enable live trading, resize Fly machines, change secrets, or mutate production Supabase.

## Rollout (opt-in)

`RETENTION_ENABLED` defaults **false**. Missing env does not start automatic journal deletion.

1. Deploy schema + code (engine boot migrate). Do **not** enable retention yet.
2. Run `journal-cleanup` dry-run (zero mutation; see access modes).
3. Inspect eligible candidates / protection / grace.
4. Set `RETENTION_ENABLED=true` only after operator review.
5. Optional one-time APPLY for pre-experiment monitor/telemetry after explicit confirmation.

This PR does not change production Fly secrets.

## Journal access modes

| Mode | Who | Migrate / DDL | Schema verify | Data mutation |
|---|---|---|---|---|
| `boot-migrate` | Engine `initialize()` | Yes | Yes | Yes (runtime journal) |
| `verify-only` | Readiness / inspect required schema | **No** | Yes | No deletes |
| `readonly` | Dry-run cleanup, report inspect | **No** | **No** | No deletes/prunes |
| `maintenance` | APPLY cleanup | **No** | Yes | Bounded deletes only |

Operator `journal-cleanup` (dry-run) and `report` construct the journal with `readonly` / `maintenance` and call `connectWithoutMigrate`. They **never** call `initialize()` / Drizzle migrate. A `DATABASE_URL` alone must not apply `0004`/`0005` or create `__drizzle_migrations` history.

Engine boot is unchanged: `boot-migrate` + `initialize()` then `verifyRequiredSchema()`.

## Canonical report

| Store | Role |
|---|---|
| `public.experiment_reports` | **Canonical** final report. Conflict-safe insert on `(experiment_id, report_kind)` + content hash. Same hash is idempotent. Conflicting contents throw `EXPERIMENT_REPORT_CONFLICT` (never swallowed). |
| `/app/reports` JSON/Markdown | Optional ephemeral export. Fly disk is not durable. FS failure after persist keeps the canonical row. FS is never required for canonicity. |

A durable `reportKind=final` row may be persisted only when:

- the **exact** target experiment id is found via `EXPERIMENT_STARTED` + `EXPERIMENT_COMPLETED` for that id (payload or aggregateId)
- the experiment is authoritatively `COMPLETED` (not inferred from `ENGINE_STARTED` or another experiment’s window)
- the scan used a **commit-stable** source fence and finished complete / non-truncated / not interrupted

`RUNNING` and `ENTRY_CLOSED` may write an interim diagnostic export. They must not persist an immutable final.

`EXPERIMENT_REPORT_GENERATED` uses a deterministic id `reportgen:<experimentId>:<contentHash>`. If persist succeeds and the audit append fails, retry loads the existing canonical row and repairs the audit exactly once (no duplicate, no conflicting hash).

Completed MCP `get_strategy_metrics(scope=complete)` prefers the persisted row. A running experiment streams with a keyset scanner + bounded accumulator. Complete-scan flights are keyed by `experimentId|startedAt|completedAt` so experiment A in flight cannot serve B. Rolling scope is never marked `complete`.

## Commit-stable source fence

Application `recordedAt` is assigned in memory **before** commit. A time-only fence can mix snapshots if a row is created before the fence, the fence query runs, and the row commits during later pages.

Report scans use a commit-stable snapshot:

- Postgres: `REPEATABLE READ` transaction; every page of one scan runs on that snapshot
- Memory: frozen committed set + `commitSeq` assigned only when the row becomes visible

For one fence, every eligible event is deterministically IN or OUT regardless of when page queries execute. Canonical persist is refused when `fence.stable` is false.

## Schema preflight / production index gate

After Drizzle migrate-on-boot the engine inspects Postgres. Missing any of these fails readiness:

- `journal_events`, `engine_checkpoints`, `mcp_idempotency_keys`, `durable_transitions`, `experiment_reports`
- keyset/query indexes including `journal_occurred_id_idx` and `journal_type_occurred_id_idx`
- RLS enabled, **not** forced, on the required public tables
- no `INVALID` indexes (`NOT indisvalid`)

`0005` creates `experiment_reports` and small indexes only. It does **not** run `CREATE INDEX` on the large live `journal_events` table (a transactional index build can block writers).

| Environment | Large keyset indexes |
|---|---|
| Fresh / local / small `journal_events` (`reltuples` ≤ 10_000) | Engine boot may create them synchronously after migrate |
| Production / large journal | Operator/predeploy: `packages/db/operator/create-journal-keyset-indexes.concurrent.sql` (`CREATE INDEX CONCURRENTLY`, **not** inside a migration transaction) |

Sequence (not executed by this PR):

1. Let engine boot apply additive `0004` then `0005` (tables; no large-table lock).
2. Run the CONCURRENTLY script in autocommit against the same database.
3. Restart / wait for schema verification. Readiness refuses missing or `INVALID` indexes.

Do not apply Step-4 SQL to production from this PR.

## Retention classes

| Class | What | Default | Delete? |
|---|---|---|---|
| Recovery-authoritative | `durable_transitions` | **PROTECTED** | **Never** in Step-4 |
| Checkpoints | `engine_checkpoints` | keep latest 8 | Only after a newer checkpoint is committed; never the latest usable row |
| MCP idempotency | `mcp_idempotency_keys` | 24h TTL | Only after TTL > signal timestamp skew (5m) + trigger TTL (30s) + 1h margin |
| Raw telemetry | `MARKET_SNAPSHOT`, `REFERENCE_TICK`, `MARKET_EVENT` | 6h | Yes. Paper fills/settlements are self-contained and are not in this class |
| Monitor / control-plane | `TRIGGER_*`, `GROK_*`, `MCP_INTERACTION`, `SYSTEM_HEALTH_*`, `DISCOVERY_RECONCILED`, reference lifecycle | 24h | See monitor lifecycle below |
| Long-lived audit | Experiment lifecycle, `PAPER_*`, `POSITION_*`, `PAIR_REALIZED`, `PNL_SNAPSHOT`, risk/consensus/hedge/settlement, strategy change | retain | **No time-prune**. Uncertainty retains |

### Monitor lifecycle

| Experiment state | Monitor/control cleanup |
|---|---|
| `null` / IDLE / MONITOR_ONLY / no experiment | Rows older than monitor TTL are eligible (no active experiment report to protect) |
| `RUNNING` / `ENTRY_CLOSED` | Protect in-window report-input. Pre-start monitor older than `min(now-ttl, startedAt)` may delete. No `startedAt` → do not delete monitor |
| `COMPLETED` | Require canonical complete/non-truncated final **and** 7d grace before deleting transient report-input |

Pre-experiment historical monitor rows are distinguishable from an active window when `startedAt` is known.

## Execution / drain

The retention manager is single-flight, low-priority vs critical writes, skipped when disabled / degraded / `FAIL_CLOSED` / DB down.

Each cycle is a **bounded multi-batch drain**, not one huge `DELETE` and not a single 200-row batch:

- each `DELETE` is small (`RETENTION_BATCH_SIZE`, default 200, max 500)
- `RETENTION_MAX_BATCHES_PER_CYCLE` default 20
- `RETENTION_MAX_ROWS_PER_CYCLE` default 4000
- `RETENTION_CYCLE_BUDGET_MS` default 250
- fairness across telemetry / monitor / idempotency / checkpoints
- stop when budget, max batches, or max rows is exhausted, or a batch returns `< batchSize`
- no unbounded loop, no huge ID arrays

Deletes do **not** shrink PostgreSQL relation size immediately. Do not run `VACUUM FULL`.

These knobs are a **test/operator drain budget**, not a production capacity claim.

## Dry-run quality

Dry-run reports actual eligible candidates under configured horizons, still `mutated: false`:

- total rows by type
- eligible by class (telemetry / monitor)
- cutoff used
- oldest eligible timestamp
- experiment protection state / monitor-only eligibility
- canonical report + grace gate
- protected audit count
- `durable_transitions=PROTECTED_NO_DELETE`
- estimated bounded batches

## One-time existing-row cleanup

The ~1M pre-experiment telemetry rows are **not** deleted by this PR and are **not** purged on startup.

```bash
# Dry run (eligible candidates, cutoffs, protection, zero mutation, zero migrate)
DATABASE_URL=… pnpm --filter @pm/engine exec tsx src/cli.ts journal-cleanup

# Apply (explicit opt-in; bounded batches; never migrates)
DATABASE_URL=… pnpm --filter @pm/engine exec tsx src/cli.ts journal-cleanup --apply --i-understand-destructive
```

APPLY never touches `durable_transitions` or audit-critical paper/execution rows. It does **not** migrate. With an active experiment it still applies **safe** classes (telemetry TTL; pre-start monitor when `startedAt` is known) and refuses in-window monitor deletion. Monitor-only / no-experiment rows older than the monitor TTL can be removed after explicit approval.

## Accumulator diagnostic

`accumulatorHighWaterBytes` is an **estimator of retained structures**, not proof of JS heap size. Structural acceptance is O(page size + bounded entity cardinality). Retained state follows market/position cardinality, not event count.

## When transient cleanup is eligible

1. Telemetry: older than telemetry TTL (independent of experiment), unless the manager is disabled.
2. Monitor-only / no experiment: older than monitor TTL.
3. Active experiment: only pre-start monitor older than `min(ttl, startedAt)`.
4. Completed experiment transient report-input: canonical final exists, `complete=true`, `truncated=false`, and `persisted_at` older than the completed-report grace (default 7 days).
5. Never audit. Never `durable_transitions`.

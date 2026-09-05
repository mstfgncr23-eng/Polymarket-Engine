# Durable reports, journal retention, and one-time cleanup

Operator reference for Step-4. This document does **not** start the 48h experiment, enable live trading, resize Fly machines, change secrets, or mutate production Supabase.

## Canonical report

| Store | Role |
|---|---|
| `public.experiment_reports` | **Canonical** final report. Idempotent on `(experiment_id, report_kind)` + content hash. Conflicting contents fail closed. |
| `/app/reports` JSON/Markdown | Optional ephemeral export. Fly disk is not durable. A file-write failure after a successful DB persist is observable and must not delete the canonical row. |

`EXPERIMENT_REPORT_GENERATED` identifies the durable report `contentHash` / schema version. A completed experiment with `complete=true` and `truncated=false` remains readable after restart even if transient journal rows were later pruned.

Completed MCP `get_strategy_metrics(scope=complete)` prefers the persisted row. A running experiment streams with a keyset scanner + bounded accumulator and coalesces concurrent callers. It never materializes the full journal.

## Schema preflight

After Drizzle migrate-on-boot the engine inspects Postgres. Missing any of these fails readiness:

- `journal_events`, `engine_checkpoints`, `mcp_idempotency_keys`, `durable_transitions`, `experiment_reports`
- keyset/query indexes including `journal_occurred_id_idx`
- RLS enabled, **not** forced, on the required public tables

Production may still lack `durable_transitions` until `0004` is applied by normal migrate-on-boot. Do not create it by hand. Do not apply Step-4 SQL to production from this PR.

## Retention classes

| Class | What | Default | Delete? |
|---|---|---|---|
| Recovery-authoritative | `durable_transitions` | **PROTECTED** | **Never** in Step-4 |
| Checkpoints | `engine_checkpoints` | keep latest 8 | Only after a newer checkpoint is committed; never the latest usable row |
| MCP idempotency | `mcp_idempotency_keys` | 24h TTL | Only after TTL > signal timestamp skew (5m) + trigger TTL (30s) + 1h margin |
| Raw telemetry | `MARKET_SNAPSHOT`, `REFERENCE_TICK`, `MARKET_EVENT` | 6h | Yes. Paper fills/settlements are self-contained and are not in this class |
| Monitor / control-plane | `TRIGGER_*`, `GROK_*`, `MCP_INTERACTION`, `SYSTEM_HEALTH_*`, `DISCOVERY_RECONCILED`, reference lifecycle | 24h monitor-only | **Not** while an experiment is `RUNNING`/`ENTRY_CLOSED`. After `COMPLETED`, only when a canonical final report exists **and** the 7d grace has elapsed |
| Long-lived audit | Experiment lifecycle, `PAPER_*`, `POSITION_*`, `PAIR_REALIZED`, `PNL_SNAPSHOT`, risk/consensus/hedge/settlement, strategy change | retain | **No time-prune**. Uncertainty retains |

## Execution

The retention manager is single-flight, low-priority vs critical writes, skipped when the engine is degraded / `FAIL_CLOSED` / DB down. Each cycle uses small `DELETE … WHERE (occurred_at, id) IN (SELECT … ORDER BY occurred_at, id LIMIT batch)` statements (default batch 200, cycle budget 250ms). It does not take table-wide locks by design, does not load large ID arrays in the engine, and does not change RuntimeSupervisor / PR #12 admission.

Deletes do **not** shrink PostgreSQL relation size immediately. Do not run `VACUUM FULL`.

## One-time existing-row cleanup

The ~1M pre-experiment telemetry rows are **not** deleted by this PR and are **not** purged on startup.

```bash
# Dry run (counts, oldest/newest, protection, zero mutation)
DATABASE_URL=… pnpm --filter @pm/engine exec tsx src/cli.ts journal-cleanup

# Apply (explicit opt-in; bounded batches; refuses unsafe experiment cleanup)
DATABASE_URL=… pnpm --filter @pm/engine exec tsx src/cli.ts journal-cleanup --apply --i-understand-destructive
```

APPLY never touches `durable_transitions` or audit-critical paper/execution rows. It refuses if an experiment is active or a completed experiment still lacks a canonical final report.

## When transient cleanup is eligible

1. Experiment status is `COMPLETED` (not `RUNNING` / `ENTRY_CLOSED`).
2. `experiment_reports` has `complete=true`, `truncated=false`, and a content hash.
3. `persisted_at` is older than the completed-report grace (default 7 days).
4. The row is monitor/control-plane or telemetry — never audit, never `durable_transitions`.

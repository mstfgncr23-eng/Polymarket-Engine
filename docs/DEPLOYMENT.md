# Deployment foundation (Supabase + Vercel + Fly)

Operator runbook for **paper MONITOR_ONLY** readiness. This document does not start the 48-hour experiment, enable live trading, configure Slack/Grok wake, or add OpenAI/xAI inference APIs.

Safety defaults that must remain true on first Fly boot:

| Control | Required value |
|---|---|
| `TRADING_MODE` | `paper` |
| Live trading | disabled (`LIVE_TRADING_DISABLED`) |
| `GROK_ACTIVATION_PROVIDER` | `noop` |
| Experiment | `IDLE` until explicit operator start |
| Grok MCP tokens | **not** required for MONITOR_ONLY |
| Slack | **not** configured in this milestone |

## Supabase

Provisioned project (schema applied by the operator / MCP, not by inventing passwords in git):

- Name: `Polymarket-Engine`
- Ref: `hkvtmaqvhpbcfbmboibh`
- Region: `eu-central-1`
- Postgres 17

### Applied schema (operator MCP, already done)

These repo files were applied on `hkvtmaqvhpbcfbmboibh`. Do not invent connection passwords.

| Migration | Status |
|---|---|
| `0000_initial.sql` | applied |
| `0001_grok_mcp.sql` | applied |
| `0002_journal_query.sql` | applied |

All expected public engine tables exist with primary keys. Observed indexes include:

- `journal_trace_idx`
- `journal_aggregate_idx`
- `journal_type_time_idx`
- `journal_experiment_idx`
- `checkpoint_sequence_idx`
- `mcp_idempotency_scope_key_idx`

`packages/db/migrations/0003_rls_lockdown.sql` is the portable Fly/local migrate-on-boot copy of RLS lockdown (ENABLE RLS, **no** policies, **no** FORCE). On the provisioned project, RLS is **already enabled** on all public engine tables with **no** policies — keep that. Do **not** add permissive `SELECT` policies. The engine uses **direct Postgres**. Table owner / `postgres` bypasses RLS.

SQL comments in `0003` are `--` (PostgreSQL). `REVOKE` from `anon`/`authenticated` runs only when those roles exist so local `docker-compose` Postgres (no Supabase roles) can still migrate.

### Advisors (expected)

| Advisor | Level | Action |
|---|---|---|
| `rls_enabled_no_policy` | INFO | **Legitimate.** Direct-Postgres-only engine; Data API / `anon` / `authenticated` must stay denied. Do not add policies to silence the lint. |
| Unused indexes | INFO | Expected on an empty database. Do not drop journal/checkpoint indexes. |
| `mcp_idempotency_keys` has a unique index (`mcp_idempotency_scope_key_idx`) but no formal PRIMARY KEY | INFO | Style only. **Do not rewrite `0001_grok_mcp.sql`.** |

### `DATABASE_URL` strategy

Copy the connection string from the Supabase dashboard. Never commit the password. Set it only as a Fly secret.

1. Open [Supabase Dashboard](https://supabase.com/dashboard/project/hkvtmaqvhpbcfbmboibh/settings/database) → **Database** → **Connect**.
2. Copy a URI. Replace nothing in git; paste into `fly secrets set`.

**Prefer DIRECT Postgres (port 5432, SSL)** when the Fly FRA machine can reach IPv6 `db.hkvtmaqvhpbcfbmboibh.supabase.co`:

```text
postgresql://postgres:<PASSWORD>@db.hkvtmaqvhpbcfbmboibh.supabase.co:5432/postgres?sslmode=require
```

Direct connections are long-lived session connections. The engine keeps a small pool (`postgres` client `max: 10`) and runs Drizzle migrate-on-boot, health checks, and journal writes. That matches a **session**, not a transaction pooler.

**Fallback: Supavisor SESSION mode (port 5432 pooler)** if direct IPv6 is unreachable from FRA:

```text
postgresql://postgres.<PROJECT_REF>:<PASSWORD>@aws-0-eu-central-1.pooler.supabase.com:5432/postgres?sslmode=require
```

Use the dashboard’s **Session pooler** URI (port **5432**). **Never transaction mode** (typically port **6543**) for this long-lived engine unless proven with a dedicated test: transaction pooling breaks prepared statements, session state, and migrate-on-boot.

`JOURNAL_MODE=postgres` is required in production. In-memory journal is forbidden.

## Fly secrets (names only)

Set on the engine app. Values never belong in git, `fly.engine.toml`, logs, or this document.

| Secret | First MONITOR_ONLY boot |
|---|---|
| `DATABASE_URL` | **required** |
| `ENGINE_READ_TOKEN` | **required** in production. Read-only: dashboard / history / SSE. Never on Vercel as `NEXT_PUBLIC_*`. |
| `ENGINE_OPERATOR_TOKEN` | **required** in production. Exclusive authority for `POST /api/operator/experiment/start`. **Never** fall back to the read token. **Never** put on Vercel or in the browser. |
| `METRICS_TOKEN` | optional private `/metrics` scrape |
| `GROK_MCP_BEARER_TOKENS` | **later** — not required for MONITOR_ONLY |
| Slack tokens | **not in this PR** |

`DASHBOARD_ORIGIN` is not a secret. Set it to the Vercel **production** origin (see below) so engine CORS allows the dashboard browser. CORS is **not** authentication; `ENGINE_READ_TOKEN` is.

Example (operator machine; do not echo the URI):

```bash
fly secrets set -a polymarket-engine DATABASE_URL='<paste-from-supabase-dashboard>'
fly secrets set -a polymarket-engine ENGINE_READ_TOKEN='<high-entropy>' ENGINE_OPERATOR_TOKEN='<high-entropy>'
# later: GROK_MCP_BEARER_TOKENS='role:token,...'
```

## Fly deploy

`fly.engine.toml` may list `app = "polymarket-engine"` as a documented intended default. The operator must own or create **their** app. Do **not** assume that name at deploy time. Manual and GitHub Action deploys must pass `--app <operator-app>`.

Topology (already in toml): `fra`, internal `8080`, **`shared-cpu-2x`**, `512mb`, `min_machines_running = 1`, `auto_stop_machines = "off"`. Dashboard toml remains `shared-cpu-1x` / `256mb`. This document does **not** change VM size — it only matches `fly.engine.toml`. **No Fly Managed Postgres.** **No Fly volume** for reports.

```bash
# Create only if the operator does not already own the app
fly apps create <operator-app> --org <org>

fly deploy --config fly.engine.toml --app <operator-app> --ha=false
```

GitHub Action `.github/workflows/fly-deploy.yml` is **`workflow_dispatch` only** with a **required** `fly_app` input. It deploys with:

```bash
flyctl deploy --config fly.engine.toml --app "$FLY_APP" --ha=false
```

It needs repo secret `FLY_API_TOKEN`. Ordinary PR CI (`.github/workflows/ci.yml`) does **not** require that token and must stay green without Fly credentials.

Singleton `--ha=false` avoids duplicate paper decisions. Horizontal engine scaling is unsafe until leader election exists.

### Health

| Path | Meaning |
|---|---|
| `GET /health` | Process liveness. Open (no token). |
| `GET /ready` | Engine alive: not draining **and** PostgreSQL up. **Does not** require qualified markets, Polymarket WS, or Grok. |
| `GET /metrics` | Optional `METRICS_TOKEN`. |

Zero qualified markets is `discoveryStatus=ZERO_QUALIFIED`, not a crash. Fly must not kill the scanner for an empty book.

### Filesystem

Fly disks are **not** durable canonical state. PostgreSQL is. The canonical final experiment report lives in `experiment_reports` (hash + schema version + summary JSON + markdown). The engine image still creates `/app/reports` (`REPORT_DIRECTORY=/app/reports`) as an **optional ephemeral export**. A filesystem write failure must not destroy an already-persisted canonical report. Rebuild/export with `pnpm report` from `DATABASE_URL` + `EXPERIMENT_ID`.

Migrate-on-boot: `Dockerfile.engine` copies `packages/db/migrations` (SQL + `meta/_journal.json`) to `/app/packages/db/migrations` and sets `DB_MIGRATIONS_DIR`. Runtime `PostgresEventJournal.initialize()` (`boot-migrate` only) runs Drizzle migrate **then schema verification**. Required relations include `journal_events`, `engine_checkpoints`, `mcp_idempotency_keys`, `durable_transitions`, `experiment_reports`, `durable_sequence_metadata`, and `experiment_report_rollups`. If migrate-on-boot completes but a required relation/index/RLS invariant is absent (including a production database that never applied the additive lifecycle migrations), boot/readiness fails closed. SQL is additive (`CREATE IF NOT EXISTS`, idempotent `ENABLE ROW LEVEL SECURITY`, role-guarded `REVOKE`). Re-running against an already-provisioned schema does **not** DROP tables.

`0005` does **not** create large `journal_events` keyset indexes. A transactional `CREATE INDEX` on ~1M live rows can block writers. Fresh/local boot may create those indexes synchronously only when `journal_events` is small. Production must run `packages/db/operator/create-journal-keyset-indexes.concurrent.sql` (`CREATE INDEX CONCURRENTLY`, outside a migration transaction) before readiness. Schema verification fails closed on missing or `INVALID` indexes, and on failure to inspect `pg_index`. An interrupted `CREATE INDEX CONCURRENTLY` leaves an INVALID index; `IF NOT EXISTS` cannot repair it — `DROP INDEX CONCURRENTLY` then recreate (see [RETENTION.md](./RETENTION.md)). This PR does not execute that script or recovery against production.

Operator CLIs: `journal-cleanup` dry-run is `readonly`; APPLY is `maintenance`; `pnpm report` is `readonly` unless `REPORT_PERSIST=1` selects `report-write`. They use `connectWithoutMigrate`. They never call `initialize()` and must not apply `0004`/`0005` or persist a report merely because `DATABASE_URL` is set. Dry-run/APPLY use the same `retentionConfigFromEnv` path as runtime `RetentionManager`.

Do **not** manually create `durable_transitions` in production. Let migrate-on-boot apply `0004` through the additive hot-storage migration. Durable transitions remain recovery-authoritative as a tail after a verified checkpoint; compaction is bounded, fenced, and operationally disabled until reviewed.

`RETENTION_ENABLED` defaults **false**. Do not enable automatic journal deletion until after a dry-run review. This PR does not change production secrets.

See [RETENTION.md](./RETENTION.md) for access modes, retention classes, dry-run cleanup, concurrent-index sequence, and when a completed experiment may drop transient journal rows.

### Supabase migration history

Operator MCP history on `hkvtmaqvhpbcfbmboibh` currently includes:

- `0000_initial`
- `0001_grok_mcp`
- `0002_journal_query`
- `0003_rls_lockdown`
- `0003_rls_lockdown` (duplicate external apply)

The duplicate `0003_rls_lockdown` row is **harmless provisioning history**. Do **not** delete it. Do **not** re-apply `0003`. Do **not** mutate production schema for cleanup. Drizzle migrate-on-boot may also record its own `__drizzle_migrations` journal; re-executing the same idempotent SQL is safe.

## MONITOR_ONLY first-boot checklist

1. Supabase project reachable; schema/migrations applied (including RLS lockdown; no Data API policies).
2. `fly secrets` set: `DATABASE_URL`, `ENGINE_READ_TOKEN`, `ENGINE_OPERATOR_TOKEN`. **No** Grok/Slack required. Do not put the operator token on Vercel.
3. `fly.engine.toml` still has `TRADING_MODE=paper` and `GROK_ACTIVATION_PROVIDER=noop`.
4. `fly deploy --config fly.engine.toml --app <operator-app> --ha=false` (or workflow_dispatch with required `fly_app`).
5. `GET https://<engine>/health` → 200.
6. `GET https://<engine>/ready` → 200 when DB is up, even with zero qualified markets. 503 if Postgres is down (fail-closed).
7. Confirm experiment stays `IDLE` (no `EXPERIMENT_STARTED` in the journal). Do **not** call `POST /api/operator/experiment/start` or `pnpm experiment:start`.
8. Dashboard (once Vercel is pointed at the engine) shows live/degraded data or `ENGINE_UNREACHABLE` — it must not invent fills or Grok activity.

### Reference feeds on Fly

Until a Fly machine has actually been deployed and a reference WebSocket observed from FRA, treat reference connectivity as:

`REFERENCE_PROVIDER_NOT_YET_OBSERVED_ON_FLY`

Do not claim Binance/vision WS works from Fly FRA in this PR. HTTP 451/403 remains `REGION_BLOCKED` / `PROVIDER_BLOCKED` (fail closed for that asset; does not mark the whole engine unready).

## Vercel dashboard

Linked project (do not invent tokens; operator sets env in the Vercel dashboard):

| | |
|---|---|
| Team | `mstfgncr23-8899s-projects` (`team_qYdYq9bYPuMa4hVH6N9i0Zrx`) |
| Project | `polymarket-engine-dashboard` (`prj_m97M2KXYzyWxF1z8zjjVI0jCBMfQ`) |
| Git | `mstfgncr23-eng/Polymarket-Engine` |
| Production branch | `main` |
| Root Directory | `apps/dashboard` |

The Next.js app lives in `apps/dashboard`. Engine credentials must be **server-only**. There is **no production dashboard** until the operator sets those env vars. Git preview builds without them are expected to return `ENGINE_UNREACHABLE`.

### Required Vercel env (names only)

| Name | Where | Notes |
|---|---|---|
| `ENGINE_INTERNAL_URL` | Vercel **server** env | Fly engine URL. **Never** `NEXT_PUBLIC_ENGINE_INTERNAL_URL`. |
| `ENGINE_READ_TOKEN` | Vercel **server** env | Same value as Fly `ENGINE_READ_TOKEN`. **Never** `NEXT_PUBLIC_ENGINE_READ_TOKEN`. |

Do not tick “Expose to the browser”. CORS on the engine is not auth.

### Monorepo build

`apps/dashboard/vercel.json` (used when Root Directory is `apps/dashboard`):

```json
{
  "framework": "nextjs",
  "installCommand": "cd ../.. && pnpm install --frozen-lockfile",
  "buildCommand": "cd ../.. && pnpm --filter @pm/dashboard build"
}
```

`next.config.ts` uses `output: "standalone"` only when `VERCEL` is unset (Fly `Dockerfile.dashboard`). On Vercel, Next tracing needs `next-server.js.nft.json`, which standalone omits.

### Remaining operator steps

1. Confirm Root Directory is `apps/dashboard`.
2. Set server-only `ENGINE_INTERNAL_URL` and `ENGINE_READ_TOKEN` (Production, and Preview if you want previews to talk to an engine). Do **not** invent or commit values. Do **not** set `ENGINE_OPERATOR_TOKEN` on Vercel.
3. After the **first production** dashboard deploy, copy that production origin (custom domain or `https://polymarket-engine-dashboard-mstfgncr23-8899s-projects.vercel.app`).
4. Set Fly `DASHBOARD_ORIGIN` to that origin (not a secret, but set after the URL exists):

```bash
fly secrets set -a polymarket-engine DASHBOARD_ORIGIN='https://<vercel-production-origin>'
```

5. Snapshot/events proxies return JSON `{ "error": "ENGINE_UNREACHABLE" }` with 503 when the engine cannot be reached. Successful `/api/engine/events` remains `text/event-stream`.

## What this PR does not do

- Start the 48h experiment
- Enable live Polymarket orders, wallet, or signer
- Configure Slack or Grok wake
- Call OpenAI or xAI inference APIs
- Deploy to Fly automatically on every PR
- Store secrets in git

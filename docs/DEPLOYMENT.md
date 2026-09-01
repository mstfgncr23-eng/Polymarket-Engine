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

Repo migrations in `packages/db/migrations/` (`0000_initial`, `0001_grok_mcp`, `0002_journal_query`, plus `0003_rls_lockdown`) match the PR #2 engine schema. `0003` enables **ROW LEVEL SECURITY with no anon/authenticated policies** and revokes Data API grants. The engine uses **direct Postgres**. Table owner / `postgres` bypasses RLS (do **not** `FORCE ROW LEVEL SECURITY`).

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
| `ENGINE_READ_TOKEN` | **required** in production (dashboard/history/events) |
| `ENGINE_OPERATOR_TOKEN` | recommended (experiment start later); read token is accepted as fallback |
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

`fly.engine.toml` declares `app = "polymarket-engine"` as the **intended** name. The operator must own or create that app. Do not invent a different name in git. If the live app name differs, pass `--app` explicitly.

Topology (already in toml): `fra`, internal `8080`, `shared-cpu-1x`, `512mb`, `min_machines_running = 1`, `auto_stop_machines = "off"`. **No Fly Managed Postgres.**

```bash
# Create only if the operator does not already own the app
fly apps create polymarket-engine --org <org>

fly deploy --config fly.engine.toml --app polymarket-engine --ha=false
# equivalent when toml app= matches the owned app:
# fly deploy --config fly.engine.toml --ha=false
```

GitHub Action `.github/workflows/fly-deploy.yml` is **`workflow_dispatch` only**. It runs typecheck/test/build, `flyctl config validate -c fly.engine.toml`, then deploy. It needs repo secret `FLY_API_TOKEN`. Ordinary PR CI (`.github/workflows/ci.yml`) does **not** require that token and must stay green without Fly credentials.

Singleton `--ha=false` avoids duplicate paper decisions. Horizontal engine scaling is unsafe until leader election exists.

### Health

| Path | Meaning |
|---|---|
| `GET /health` | Process liveness. Open (no token). |
| `GET /ready` | Engine alive: not draining **and** PostgreSQL up. **Does not** require qualified markets, Polymarket WS, or Grok. |
| `GET /metrics` | Optional `METRICS_TOKEN`. |

Zero qualified markets is `discoveryStatus=ZERO_QUALIFIED`, not a crash. Fly must not kill the scanner for an empty book.

### Filesystem

Fly disks are **not** durable canonical state. PostgreSQL is. `reports/` on the machine can be deleted; rebuild with `pnpm report` from the journal (`DATABASE_URL` + `EXPERIMENT_ID`).

Migrate-on-boot: `Dockerfile.engine` copies `packages/db/migrations` (SQL + `meta/_journal.json`) to `/app/packages/db/migrations` and sets `DB_MIGRATIONS_DIR`. Runtime `PostgresEventJournal.initialize()` runs Drizzle migrate.

## MONITOR_ONLY first-boot checklist

1. Supabase project reachable; schema/migrations applied (including RLS lockdown; no Data API policies).
2. `fly secrets` set: `DATABASE_URL`, `ENGINE_READ_TOKEN` (and operator token). **No** Grok/Slack required.
3. `fly.engine.toml` still has `TRADING_MODE=paper` and `GROK_ACTIVATION_PROVIDER=noop`.
4. `fly deploy --config fly.engine.toml --ha=false` (or the workflow_dispatch Action).
5. `GET https://<engine>/health` → 200.
6. `GET https://<engine>/ready` → 200 when DB is up, even with zero qualified markets. 503 if Postgres is down (fail-closed).
7. Confirm experiment stays `IDLE` (no `EXPERIMENT_STARTED` in the journal). Do **not** call `POST /api/operator/experiment/start` or `pnpm experiment:start`.
8. Dashboard (once Vercel is pointed at the engine) shows live/degraded data or `ENGINE_UNREACHABLE` — it must not invent fills or Grok activity.

### Reference feeds on Fly

Until a Fly machine has actually been deployed and a reference WebSocket observed from FRA, treat reference connectivity as:

`REFERENCE_PROVIDER_NOT_YET_OBSERVED_ON_FLY`

Do not claim Binance/vision WS works from Fly FRA in this PR. HTTP 451/403 remains `REGION_BLOCKED` / `PROVIDER_BLOCKED` (fail closed for that asset; does not mark the whole engine unready).

## Vercel dashboard

The Next.js app lives in `apps/dashboard`. Engine credentials must be **server-only**.

| Vercel env | Notes |
|---|---|
| `ENGINE_INTERNAL_URL` | Server-only. Fly engine URL (public HTTPS or private Flycast if the dashboard were also on Fly). **Never** `NEXT_PUBLIC_ENGINE_INTERNAL_URL`. |
| `ENGINE_READ_TOKEN` | Server-only. Same value as Fly `ENGINE_READ_TOKEN`. **Never** `NEXT_PUBLIC_ENGINE_READ_TOKEN`. |

On Fly, set `DASHBOARD_ORIGIN` to the Vercel production origin (e.g. `https://<project>.vercel.app` or the custom domain). CORS allowlisting that origin is not auth.

### Remaining Vercel operator steps

1. Create/import a Vercel project for this GitHub repo.
2. Set **Root Directory** to `apps/dashboard`. `apps/dashboard/vercel.json` already has:
   - install: `cd ../.. && pnpm install --frozen-lockfile`
   - build: `cd ../.. && pnpm --filter @pm/dashboard build`

   `next.config.ts` uses `output: "standalone"` only when `VERCEL` is unset (Fly `Dockerfile.dashboard`). On Vercel, Next tracing needs `next-server.js.nft.json`, which standalone omits.
3. Add server env vars `ENGINE_INTERNAL_URL` and `ENGINE_READ_TOKEN` (Production + Preview as needed). Do not tick “Expose to the browser”.
4. Deploy. Snapshot/events proxies return JSON `{ "error": "ENGINE_UNREACHABLE" }` with 503 when the engine cannot be reached. Successful `/api/engine/events` remains `text/event-stream`.
5. Put the resulting production origin on Fly `DASHBOARD_ORIGIN` and redeploy or `fly secrets`/`fly machine` env as appropriate (`DASHBOARD_ORIGIN` is in `[env]` only if you add it; otherwise set via secrets or `flyctl machine update` — prefer `fly secrets` or toml `[env]` after the origin is known). For this PR, set it at deploy time once the Vercel URL exists:

```bash
fly secrets set -a polymarket-engine DASHBOARD_ORIGIN='https://<vercel-production-origin>'
```

(`DASHBOARD_ORIGIN` is an origin URL, not a credential.)

## What this PR does not do

- Start the 48h experiment
- Enable live Polymarket orders, wallet, or signer
- Configure Slack or Grok wake
- Call OpenAI or xAI inference APIs
- Deploy to Fly automatically on every PR
- Store secrets in git

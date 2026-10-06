# OVH dashboard preparation (nothing deployed)

Intended topology: browser -> host Caddy HTTPS -> one Gravelines Next.js Node
process -> localhost restricted SSH tunnel -> Frankfurt's existing authenticated
`GET /api/dashboard` RAM snapshot. Frankfurt remains bound to `127.0.0.1:8080`.
There is no public engine listener, upstream SSE, dashboard database access, or
browser-to-Frankfurt connection. The read token is server-side only.

## Prepared artifacts

`compose.example.yml` builds the existing standalone `Dockerfile.dashboard`.
It uses **Linux host networking** so the host tunnel at `127.0.0.1:18080` is
reachable from the container; Next listens on `127.0.0.1:3000` only. Caddy runs
on the host using `Caddyfile.example`. Docker bridge localhost would refer to
the container rather than the SSH tunnel, so do not substitute bridge networking
without separately designing private host connectivity. Use exactly one process
and one replica; clustering/multiple containers each have their own poller.

For a future operator deployment, from this directory the Compose invocation is
`docker compose -f compose.example.yml up -d --build`. **Do not run it as part
of this task.** Domain ownership, DNS, HTTPS access policy, tunnel installation,
and service activation remain operator work. Preserve the existing dashboard
access policy in front of Caddy; the engine token authenticates upstream reads,
not browser users.

## Environment (no secrets in these examples)

| Variable | Default / intended value | Purpose |
| --- | --- | --- |
| `ENGINE_INTERNAL_URL` | `http://127.0.0.1:18080` on Gravelines | Private host tunnel endpoint |
| `ENGINE_READ_TOKEN` | required; provision outside Git | Existing engine read authorization |
| `DASHBOARD_POLL_IDLE_MS` | `10000` | No active experiment / completed |
| `DASHBOARD_POLL_ACTIVE_MS` | `60000` | RUNNING or ENTRY_CLOSED |
| `DASHBOARD_POLL_DEGRADED_MS` | `120000` | Production degradation or upstream failure |
| `DASHBOARD_UPSTREAM_TIMEOUT_MS` | `3000` | Headers and body timeout |
| `HOSTNAME`, `PORT` | `127.0.0.1`, `3000` | Local-only Next listener |

Intervals/timeouts accept integer milliseconds from 100 to 2147483647 (Node's
timer limit); invalid values fall back
to defaults. Polls schedule their next interval **after completion**, never
overlap, and retain only one valid snapshot. Degraded takes precedence over
active. Failure before the first snapshot uses the degraded recovery cadence.
Reloads, SSR, API reads, readiness probes, and browser reconnects never trigger
extra upstream polls. Local SSE sends accepted snapshots and cache metadata
every five seconds; slow clients disconnect before unbounded buffering.

`/health` is process liveness, used by the Compose health check. `/ready` uses
the shared cache and returns 200 with a usable fresh cache, otherwise 503 with
metadata. Upstream failure retains cached data for browsers; it does not cause
trading failure or a restart loop. `unless-stopped` restarts process failures.

## Planned tunnel only

A dedicated unprivileged account and restricted public key on Frankfurt should
permit only local port forwarding to `127.0.0.1:8080`: no shell/exec, agent/X11,
PTY, remote forwarding, or other destinations. The operator must combine
`PermitOpen 127.0.0.1:8080`, `AllowTcpForwarding local`, and session restrictions
(`MaxSessions 0` on the dedicated account) with key restrictions. Verify the
restriction with both permitted and rejected forwarding/session attempts before
use. Do not generate or commit private keys. Manage tunnel restarts separately.

The intended operator-side SSH command has `-N -T`, `ExitOnForwardFailure=yes`,
bounded server-alive settings, and
`-L 127.0.0.1:18080:127.0.0.1:8080`. It is documentation only: no tunnel, VPS,
DNS, current production container, soak, or trading configuration is modified.

## Accounting compatibility

Current-experiment results live in durable checkpoint state and update only
with canonical fill/settlement application. Partial safety sells retain realized
PnL until the remaining market episode closes; fees include all its fills.
Win rate is wins / completed (including breakeven), null with no completions.
Legacy checkpoints are upgraded from complete retained current-experiment
durable history during recovery. If that history has already been compacted,
historical counters are unavailable rather than fabricated; a new experiment
resets to provable zero counters. Dashboard reads never query storage.

# Engine + Guardian PAPER

Initial process reference: `938e4dbd5b1b4757d7697639ec400137182c3f7d`.
The offline child's existing policy is reused by a continuously running worker.
No policy thresholds or economic execution algorithm are changed.

Engine owns PostgreSQL and all economic orders. Guardian has only a local Unix
socket, no network namespace access, wallet or database credential. Each 250ms
cycle discovers the current residual positions from Engine's PG-recovered
projection, plus the existing normalized top books and native consensus.
Engine persists Guardian state with the existing durable revision operation
keys and invokes its existing normal-hedge / PR3 / protective-SELL arbitration.
The canonical positions come from the existing `engine_checkpoints` and
`durable_transitions`; PAPER orders, fills and policy audit events are recorded
in the existing `journal_events`. No new migration or database is needed.
No duplicate market feed subscription is created.
The existing Engine Dockerfile builds both entrypoints into one image; Compose
runs the two independent containers with different commands.

Guardian restart reloads all currently open positions and persisted policy
states. Missing, future, invalid or unsynchronized books block only their market.
No absolute UP/DOWN timestamp-skew gate is used. Reconnect and blocked reasons,
cycle duration, CPU and event-loop p99 are logged every five seconds. Engine's
existing metrics provide its separate event-loop measurements. `QUEUED` admits
an ordinary observation checkpoint to the existing bounded/coalescing lane;
`PERSISTED` acknowledges durable state. Critical transitions and protection
requests still wait for persistence. Actual PAPER execution is proven by fills.

The runtime does not use the historical checkpoint issuer, manifest signatures,
HMAC, roster registry, child supervisor, entry attestations, or acceptance gates.
Historical modules and database records are left intact. Retention uses the
existing local archive and existing second OVH upload/readback channel.
In this mode a completed experiment's missing historical report does not block
starting a new PAPER experiment; open economic obligations and journal integrity
still do. Historical reports are never rewritten or presented as current.

Set the following Compose variables to the **existing** deployment:

```
RELEASE_TAG=<implementation HEAD>
ENGINE_ENV_FILE=<existing private PAPER Engine env>
DATABASE_URL_FILE=<existing Engine PostgreSQL URL file>
POSTGRES_NETWORK=<existing PostgreSQL Docker network>
SOCKET_DIRECTORY=/run/polymarket-paper
ENGINE_HTTP_PORT=18080
```

Prepare the one socket directory with owner `999:999`, mode `0770` and keep
`/data/archive` and `/data/learning` owned by the existing UID 999.
Build before stopping the incumbent:

```
docker compose --env-file <deployment-vars> -f deploy/guardian/compose.yml build
```

Stop the incumbent PAPER Engine and Guardian, verify no other economic Engine
is running, then start this pair using the **same existing PostgreSQL URL**:

```
docker compose --env-file <deployment-vars> -f deploy/guardian/compose.yml up -d
```

Never run a second Engine in parallel. Preserve the existing read token and
loopback port so the current dashboard follows the same target. A completed
experiment is not restarted implicitly: use the existing experiment-start API
after verifying fresh feeds. Existing open experiments recover without reset.
Keep old containers stopped for rollback; do not delete PG rows or volumes.
Six-hour soak evidence must be reported separately from the targeted E2E tests.

## Verified retention and the existing second OVH

Compose enables retention, compaction and local archival. Keep the existing
`/opt/polymarket-g4-private` upload key, verify key and known-hosts files; override
`ARCHIVE_PRIVATE_DIR` only if these existing files live elsewhere. Only Engine
mounts them. No key, account or PostgreSQL migration is created.

The existing `g4archive@141.95.162.252` restricted rsync root is
`/data/polymarket-archive/g4-independent-paper/archive`. Current `polymarket`
bundles go under its `simple-engine` subdirectory. Install the compatible
`ovh-retention-verify.py` at the existing forced-command path
`/usr/local/libexec/g4-ovh-retention-verify.py`, preserving the prior file as a
backup. Existing G4 source/probe readback remains compatible. Historical archive
directories are neither overwritten nor deleted; rsync uses `--ignore-existing`.
An invalid existing manifest/bundle is preserved and defers pruning; it cannot
be reconstructed from a partially purged hot source and overwrite the archive.

The existing defaults remain: telemetry 6h; monitor/operational 24h; canonical
completed-report grace 7d; MCP idempotency 24h; latest 8 checkpoints; DML batch
200, budget 250ms, at most 20 batches/4,000 rows. Every journal purge requires
local manifest/hash verification and remote byte/hash/event-scope verification.
Canonical data additionally requires its complete final report and stable rollup.
An unavailable archive records `retention deferred` and leaves trading running.

Active experiment rows are fenced by its start. Compaction is bounded by two
valid retained checkpoints and their durable/audit repair cursors; unresolved
positions and orders hold their economic transitions hot. Exact checkpoint
purge candidates and the bounded compaction prefix are backed up with the
required replay tail before deletion. Open and unexpired operation keys survive
the soft replay-key capacity limit. No archive-file deletion policy is introduced.
At a retention/compaction-enabled startup, Engine writes its existing checkpoint
after recovery, before opening feeds. Together with the shutdown snapshot this
keeps the fallback current and avoids repeatedly exporting a multi-hour tail.

`dist/archive-maintenance.js export-recovery` and `export-bucket` generate bounded
read-only PostgreSQL evidence using the same transfer implementation. The former
also checks actual Engine replay parity. `verify-recovery <directory>` checks a
downloaded remote bundle offline, without starting Engine or connecting to a DB.
The existing verify channel accepts `readFile` for an allowed bundle filename
only after verifying the entire bundle; it streams the file for restore evidence.

Validate and transfer/recover evidence before a controlled deployment. Let an
ongoing PAPER window finish before replacing its Engine just to change settings.
Report every interruption; elapsed six hours with restarts is not an uninterrupted
soak. The deployed latency logs contain each sampled cycle's view/evaluation/
update durations and the Engine socket wait/handler times, without secrets.

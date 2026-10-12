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
Thus existing `positions`, `durable_transitions`, `journal_events`, `paper_orders`
and `paper_fills` remain the data sources, without a new migration or database.
No duplicate market feed subscription is created.

Guardian restart reloads all currently open positions and persisted policy
states. Missing, future, invalid or unsynchronized books block only their market.
No absolute UP/DOWN timestamp-skew gate is used. Reconnect and blocked reasons,
cycle duration, CPU and event-loop p99 are logged every five seconds. Engine's
existing metrics provide its separate event-loop measurements. A request ACK
means durable state persistence; actual PAPER execution is proven by its fills.

The runtime does not use the historical checkpoint issuer, manifest signatures,
HMAC, roster registry, child supervisor, entry attestations, or acceptance gates.
Historical modules and database records are left intact. Retention stays off.

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

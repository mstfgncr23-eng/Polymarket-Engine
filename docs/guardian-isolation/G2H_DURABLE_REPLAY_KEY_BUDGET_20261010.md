# G2H | Bounded duplicate-key memory and explicit source halt

Date: 2026-10-10. Parent: `de79012`.
Gate: **G2H IN PROGRESS**, local replay-only atom.

Offline `seenOperationKeys` retained all durable operation keys since the
pinned checkpoint, growing with runtime duration and cloning for every replay
step. The canonical PostgreSQL durable journal already has a unique
`durable_transitions_operation_key_uidx` constraint, but a detached
read-only consumer must never silently discard its local duplicate-detection
evidence on untrusted input.

The offline replay model now sets a hard `GUARDIAN_REPLAY_KEY_BUDGET=8192`
and explicitly HALTS rather than evicting prior keys. Recovery needs a
**new independently authenticated source checkpoint**, not a guessed cursor
skip or automatic state reset. This is a safety bound, NOT a proof that
8192 rows is sufficient for production checkpoint cadence/100 Hz workloads.
At 100 rows/s it covers only about 82 seconds. Before any operational use,
measure production-style rate and improve the memory/CPU profile without
sacrificing ordering or historical duplicate detection.

Tests: G2H-13 capacity, last valid row, prior immutable state and duplicate
precedence. Focused 44/44 PASS across offline source/session.

No changes to production database, journal schema, writer, trading
economics, wallet or runtime.
NO SHADOW / NO VPS / PAPER ONLY / NO PUSH / NO MERGE / NO DEPLOY.

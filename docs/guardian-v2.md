# Locked Reversal Guardian V2

Authority: `CODEX_TASK_REVERSAL_GUARDIAN_V2_LOCKED_2H_SOAK.md`, source SHA
`1a1361c9c4deebb63c77a18b4206e3a56f1da631`. No thresholds were retuned.

`REVERSAL_PROTECTION_ENABLED=true` selects V2 for a newly created PAPER
profit-trailing experiment. The switch remains OFF by default. V2 requires TAKER,
naked directional OFF, and the existing `.12 / .05 / .02` target/arm/retrace.
The manifest seals `guardianVersion=2` and all values in `GUARDIAN_V2_CONFIG`.
The legacy window/count environment fields remain readable for compatibility;
count-based V1 is no longer runtime authority. Active legacy manifests fail closed.
Completed legacy 180s/360s experiments remain replayable.

States: `OBSERVING`, `EARLY_RECOVERY_WATCH`, `LATE_CONFIRMATION`,
`PROTECTION_PENDING`. State belongs to the current position/outcome/unpaired-start
episode. Normal hedge, full protective neutralization and settlement remove it
through the existing durable lifecycle reducer.

EARLY uses held-side mid divided by the episode's entry price, independent of
consensus. Price comparisons use Decimal arithmetic. Native UP-positive book
pressure is `(UP top-level imbalance - DOWN top-level imbalance) / 2`;
UP-held pressure is negated and DOWN-held pressure is preserved. Only current
token, synchronized, fresh, uncrossed books with valid quantities are admitted.

For irregular sampling, the 10s anchor is the latest admitted observation at or
before `now-10000ms`. No interpolation is used. No older sample means no arm.
History retains that anchor plus subsequent samples; bad-region persistence is
the elapsed time since the first continuously observed `ratio <= .70` sample.
Observed gaps/untrusted pairs reset EARLY history and persistence. Bootstrap
waiting for a sibling book after restart preserves the durable history.
Identical source pairs and same/older observation times are not counted again.
Consensus uses its immutable event timestamp and has a durable timestamp/id
watermark; an old replay cannot arm a released watch.

The strict EARLY deadline is checked first at `arm+60000ms`, even if books are
unavailable. A `.95` sample at the deadline is too late. `.95` before the deadline
releases immediately; `.15` inside an armed watch protects immediately. LATE
arms only on eligible opposite Native consensus at `180s < age <= 600s`; at
`arm+30000ms` it requires both a `.026` further ratio decline and opposing book
pressure of `.20`. Missing trusted decision data releases the confirmation.
An armed LATE watch can finish beyond the observation window. `.15` is its
immediate emergency floor. A release cannot rearm on the same evaluation.

Every changed history/watch/deadline is a journal-authoritative state transition,
and checkpoints retain it. Maintenance and snapshot callbacks reconstruct timer
decisions from that state. No economic authority depends on an ephemeral timer.
The unchanged shared executor compares executable guaranteed salvage for held
SELL (`PROTECTIVE_DE_RISK`) and opposite BUY (`RESCUE_SECOND_LEG`), with SELL
winning ties. Only full residuals qualify. Non-executable depth leaves the
confirmed state pending; existing retry pacing and durable order ownership apply.

Canonical `GUARDIAN_EARLY_*` and `GUARDIAN_LATE_*` events identify arms, releases,
recovery and protection decisions. Existing `RISK_EVALUATION.deRisk` records expose
execution selection/retry reasons and the Guardian sensor/reason. Fill-derived
`GUARDIAN_EXECUTION_COMPLETED` survives crash replay. Summary JSON/Markdown and
serialized rollups expose EARLY/LATE exit counts and paired/naked settlements.
Rescue BUY exits neutralize exposure before the eventual paired settlement; these
are separate action and settlement counts, not mutually exclusive position totals.

Merge, deployment and the first **2-hour PAPER** validation are separate approval
steps after independent remote diff and CI review. This implementation does not
start an experiment or change experiment duration defaults.

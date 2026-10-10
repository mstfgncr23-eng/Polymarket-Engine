# G2H | Signed producer high-water is not current source head

Date: 2026-10-10. Parent: `ee330ed`.
Gate: G2H IN PROGRESS, offline signed-source consumer only.

## Observed correctness hole

A valid publisher can attest durable high-water H while the same
canonical database already exposes H+N. The consumer formerly reported
`caughtUp=true` after replaying to H even when current canonical source
head was greater. A successfully signed *old* state is not a valid current
position authority.

## Fail-closed correction

`GuardianReadonlySession.poll` records the actual last observed source
high-water on each authenticated poll. `caughtUp` now requires:
(1) complete signed high-water replay and (2) observed DB high-water
equal to the signer high-water. A newer signer attestation is needed to
advance after unsigned source changes; no economic state is fabricated.

`readVerifiedView(nowMs)` refuses to reveal a current view unless a
poll verified the source head, the signer's cursor and consumer cursor
all matched, AND that poll was no more than 1,000ms ago. An authenticated
checkpoint alone no longer suffices. It also rejects concurrent poll,
HALT, expired issuer TTL or time regression.

The **1,000ms** here is a conservative hard stop, not a measured safe
execution latency. Real economic executor MUST independently revalidate
current open residuals, owner and books at the moment of admission.

Synthetic tests prove signed H while source H+2 is NOT considered
caught-up and cannot expose a view, plus successful fresh replay,
deep-copy isolation and TTL/1s cache expiry. Targeted **17/17 PASS**.

NO SHADOW / PAPER ONLY / NO VPS / NO DEPLOY / NO PUSH / NO MERGE.

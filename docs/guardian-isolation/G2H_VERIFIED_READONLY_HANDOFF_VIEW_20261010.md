# G2H | Verified, Copy-Safe Observational State Output

Date: 2026-10-10. Parent: `01c049a`.
Gate: G2H remains **NOT ACCEPTED** due to real producer/source gaps.

The offline authenticated consumer previously exposed only a cursor and
count, so a future isolated Guardian could not consume complete open
episodes and ownership state safely without bypassing the trust boundary.

`GuardianReadonlySession.readVerifiedView(nowMs)` now returns a bounded
snapshot with explicit PAPER-only, issuer epoch, signed highwater,
content-bound economic state digest, open residual episodes, settled
markets, maker reservations, pending risk-reduction ownership/failure
markers, trailing/reversal protection and pending maker metadata.

**No economic permission** is embedded: no order submit, cancel, reserve,
trade command, DB mutation or mutable ledger/Map/Set object escapes.
All retained source collections are defensively copied. The API rejects
consumer views while a poll is in progress, while the consumer is halted,
when behind signed durable highwater or after signer TTL expiry. An old
CONNECTED socket or old cached view does not authorize current decisions.

Two new local tests verify: a copied returned residual cannot mutate
retained live state, behind-highwater is blocked, settlement closes open
inventory and TTL expiry blocks cached views. Focused session 14/14 PASS.
Dedicated Guardian source TypeScript PASS.

The bridge to a single G2I process and request-only decision generator is
still future work, and no live economic actor or shadow loop was started.
NO SHADOW / NO VPS / PAPER ONLY / NO PUSH / NO MERGE / NO DEPLOY.

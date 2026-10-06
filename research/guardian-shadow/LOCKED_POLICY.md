# Locked two-stage Guardian shadow policy

Authority baseline: restore/native-baseline at 347d67689d63be70c1a43a42d9506889f33bafbd.

This directory is observation-only. It must not submit a Polymarket order, mutate the paper ledger, or enable LIVE.

## Frozen policy

1. Observe every open unpaired PAPER position.
2. Preserve the engine's existing NORMAL_HEDGE authority.
3. Preserve successful existing PR3 rescues.
4. Shadow trigger loss: $0.35.
5. PR3 full-residual fee-inclusive pair-lock cap: $0.60.
6. If full opposite pair-lock is unavailable, lacks full depth, or exceeds the cap, evaluate a full held-side Protective SELL.
7. A recorded RESCUE_SECOND_LEG execution error also hands the shadow to the full held-side Protective SELL path.
8. Record a helper action only when the full held residual is executable from the observed bid ladder and minimum-order/fee metadata are known.
9. No partial success is invented.
10. Once the helper acts for an episode, that episode is counterfactually closed for the primary shadow policy. The real PAPER engine remains untouched.
11. Capture real-time post-decision state at 25/50/100/150/250/500 ms. No historical interpolation.
12. A rescue execution failure belongs only to the exact `market + unhedgedStartedAt` episode carried by canonical de-risk evidence; it never leaks into a later episode.
13. A newly activated exposure without trusted direct books is an evidence-integrity failure, not an inferred fill.
14. The run may seal `COMPLETE` only when the canonical experiment is completed and final open positions, unpaired positions, unresolved count, and reserved cash are all zero.

## Historical lock

The contract-first historical screen reproduced the 166-trade authority baseline before simulation. For this policy family it found:

- baseline PnL -36.6570110804 with 6 meteors
- two-stage Guardian PnL -19.011979
- historical meteors 6 -> 0
- new meteors 0
- original NORMAL_HEDGE retained 92/92
- worst evidenced helper cycle -3.629167
- TRAIN / TEST -8.308506 / -10.703473

These are historical conditional results, not live-fill claims.

## Forward evidence objective

The 24-hour shadow must preserve contemporaneous full held SELL walks, full opposite BUY walks, fee/min-lot metadata, engine order/fill/error lifecycle, residual positions, source timestamps, local receipt timestamps, and 25-500 ms post-decision states.

The sidecar writes its own files under /data/guardian-shadow and does not add per-tick writes to Postgres.

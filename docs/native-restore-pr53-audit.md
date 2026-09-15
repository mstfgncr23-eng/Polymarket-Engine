# Native Restore: PR53 Exposure-Gate Audit

Restore baseline: `0a148e2c7416dc5afc5700bb90c261d5d13902bc`

Source PR: #53 (`fix(engine): refuse beginExperiment with leftover paper exposure`).

## Verdict

KEEP the behavior. It is independent of Astra and closes a real lifecycle/economic-safety gap in the native engine.

## Required native behavior

`beginExperiment()` must refuse to create a new RUNNING experiment when any prior paper-economic residue remains, even if the experiment projection itself is IDLE.

Blockers to preserve:

- `JOURNAL_NOT_HEALTHY`
- `OPEN_POSITIONS_REMAIN`
- `RESTING_ORDERS_REMAIN`
- `MAKER_RESERVATIONS_REMAIN`
- `RESERVED_CASH_REMAIN`
- `UNRESOLVED_ECONOMIC_OBLIGATIONS`

The clean IDLE path must still start normally.

## Native adaptation

PR53 was authored on the post-Astra engine and calls `this.experimentMode()`. The pre-Astra restore has no Astra mode switch and must keep its fixed native preflight:

```ts
this.preflight("48H_NATIVE_PAPER_EXPERIMENT")
```

Do not transplant `experimentMode()` or any Astra authority plumbing.

The residue helper itself is authority-agnostic and can be extracted unchanged in substance:

```ts
private experimentEconomicResidueBlockers(): string[] {
  const ledger = this.ledger.snapshot();
  const openPositions = [...this.ledger.positions.values()]
    .filter((position) => d(position.upQuantity).gt(0) || d(position.downQuantity).gt(0));
  const restingOrders = [...this.restingOrders.values()]
    .filter((order) => d(order.remaining).gt(0));
  const makerReservations = [...this.makerReservations.entries()]
    .filter(([, amount]) => d(amount).gt(0));
  const blockers: string[] = [];
  if (openPositions.length > 0 || this.ledger.positions.size > 0) blockers.push("OPEN_POSITIONS_REMAIN");
  if (restingOrders.length > 0 || this.restingOrders.size > 0) blockers.push("RESTING_ORDERS_REMAIN");
  if (makerReservations.length > 0 || this.makerReservations.size > 0) blockers.push("MAKER_RESERVATIONS_REMAIN");
  if (!d(ledger.reservedCash).eq(0)) blockers.push("RESERVED_CASH_REMAIN");
  if (this.obligationMarketIds().size > 0) blockers.push("UNRESOLVED_ECONOMIC_OBLIGATIONS");
  return blockers;
}
```

`obligationMarketIds()` already exists in the pre-Astra baseline, so no post-Astra dependency is required.

## Tests to carry

Extract the PR53 test concepts into native tests:

1. leftover paper position while IDLE -> start rejected with `OPEN_POSITIONS_REMAIN`
2. `journalDegraded=true` -> start rejected with `JOURNAL_NOT_HEALTHY`
3. clean IDLE/no exposure -> start succeeds

## Important non-goal

This gate does not repair or erase residue. It only refuses to start a new experiment until the prior economic state is clean. No automatic ledger clearing, position deletion, or journal mutation should be added.

## Extraction status

- [x] source patch audited against pre-Astra baseline
- [x] native dependencies verified (`obligationMarketIds`, ledger/resting/maker state already exist)
- [ ] EngineService helper + beginExperiment gate extracted
- [ ] native regression tests added

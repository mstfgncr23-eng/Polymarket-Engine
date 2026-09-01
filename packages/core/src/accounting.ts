import { Decimal } from "decimal.js";
import { createId, type LedgerState, type PaperFill, type Position } from "./domain.js";
import { d, decimal } from "./calculations.js";

export class PaperLedger {
  readonly positions = new Map<string, Position>();
  private state: LedgerState;

  constructor(startingBalance: Decimal.Value, at = new Date()) {
    const balance = decimal(startingBalance);
    this.state = {
      startingBalance: balance,
      cash: balance,
      reservedCash: "0",
      positionValue: "0",
      equity: balance,
      realizedPnl: "0",
      unrealizedPnl: "0",
      grossPnl: "0",
      fees: "0",
      slippage: "0",
      aiCost: "0",
      infrastructureCost: "0",
      netTradingPnl: "0",
      netEconomicPnl: "0",
      highWaterMark: balance,
      drawdown: "0",
      updatedAt: at.toISOString(),
    };
  }

  snapshot(): LedgerState {
    return structuredClone(this.state);
  }

  restore(state: LedgerState, positions: readonly Position[]): void {
    this.state = structuredClone(state);
    this.positions.clear();
    for (const position of positions) this.positions.set(position.marketId, structuredClone(position));
  }

  reserve(amount: Decimal.Value, at = new Date()): void {
    const value = d(amount);
    if (value.lte(0) || value.gt(this.availableCash())) throw new Error("INSUFFICIENT_CASH");
    this.state.reservedCash = decimal(d(this.state.reservedCash).plus(value));
    this.state.updatedAt = at.toISOString();
  }

  release(amount: Decimal.Value, at = new Date()): void {
    this.state.reservedCash = decimal(Decimal.max(0, d(this.state.reservedCash).minus(amount)));
    this.state.updatedAt = at.toISOString();
  }

  availableCash(): Decimal {
    return d(this.state.cash).minus(this.state.reservedCash);
  }

  applyFill(fill: PaperFill): Position {
    const now = fill.timestamp;
    const previous = this.positions.get(fill.marketId) ?? {
      id: createId("pos"), marketId: fill.marketId, upQuantity: "0", downQuantity: "0",
      averageUpCost: "0", averageDownCost: "0", pairedQuantity: "0", unpairedOutcome: null,
      unpairedQuantity: "0", combinedPairCost: "0", totalFees: "0", totalSlippage: "0",
      openedAt: now, updatedAt: now,
    } satisfies Position;

    const quantityKey = fill.outcome === "UP" ? "upQuantity" : "downQuantity";
    const costKey = fill.outcome === "UP" ? "averageUpCost" : "averageDownCost";
    const oldQuantity = d(previous[quantityKey]);
    const quantity = d(fill.quantity);
    const signedQuantity = fill.side === "BUY" ? quantity : quantity.negated();
    const newQuantity = oldQuantity.plus(signedQuantity);
    if (newQuantity.lt(0)) throw new Error("SHORT_POSITIONS_UNSUPPORTED");

    const newAverage = fill.side === "BUY" && newQuantity.gt(0)
      ? oldQuantity.mul(previous[costKey]).plus(quantity.mul(fill.fillPrice)).div(newQuantity)
      : newQuantity.eq(0) ? d(0) : d(previous[costKey]);
    const notional = quantity.mul(fill.fillPrice);
    const cashDelta = fill.side === "BUY" ? notional.plus(fill.feeUsd) : notional.minus(fill.feeUsd).negated();
    const realizedDelta = fill.side === "SELL" ? quantity.mul(d(fill.fillPrice).minus(previous[costKey])) : d(0);

    const next: Position = {
      ...previous,
      [quantityKey]: decimal(newQuantity),
      [costKey]: decimal(newAverage),
      totalFees: decimal(d(previous.totalFees).plus(fill.feeUsd)),
      totalSlippage: decimal(d(previous.totalSlippage).plus(fill.slippageUsd)),
      updatedAt: now,
      pairedQuantity: "0",
      unpairedOutcome: null,
      unpairedQuantity: "0",
      combinedPairCost: "0",
    };
    const paired = Decimal.min(next.upQuantity, next.downQuantity);
    const difference = d(next.upQuantity).minus(next.downQuantity);
    next.pairedQuantity = decimal(paired);
    next.unpairedOutcome = difference.gt(0) ? "UP" : difference.lt(0) ? "DOWN" : null;
    next.unpairedQuantity = decimal(difference.abs());
    next.combinedPairCost = decimal(d(next.averageUpCost).plus(next.averageDownCost));
    this.positions.set(fill.marketId, next);

    this.state.cash = decimal(d(this.state.cash).minus(cashDelta));
    this.state.realizedPnl = decimal(d(this.state.realizedPnl).plus(realizedDelta));
    this.state.fees = decimal(d(this.state.fees).plus(fill.feeUsd));
    this.state.slippage = decimal(d(this.state.slippage).plus(fill.slippageUsd));
    this.state.updatedAt = now;
    this.revalue({}, now);
    return structuredClone(next);
  }

  addOperatingCosts(aiCost: Decimal.Value, infrastructureCost: Decimal.Value, at = new Date()): void {
    this.state.aiCost = decimal(d(this.state.aiCost).plus(aiCost));
    this.state.infrastructureCost = decimal(d(this.state.infrastructureCost).plus(infrastructureCost));
    this.revalue({}, at.toISOString());
  }

  revalue(prices: Record<string, { up: Decimal.Value; down: Decimal.Value }>, at = new Date().toISOString()): LedgerState {
    let value = d(0);
    let cost = d(0);
    for (const position of this.positions.values()) {
      const marketPrices = prices[position.marketId];
      if (marketPrices) {
        value = value.plus(d(position.upQuantity).mul(marketPrices.up))
          .plus(d(position.downQuantity).mul(marketPrices.down));
      } else {
        value = value.plus(d(position.upQuantity).mul(position.averageUpCost))
          .plus(d(position.downQuantity).mul(position.averageDownCost));
      }
      cost = cost.plus(d(position.upQuantity).mul(position.averageUpCost))
        .plus(d(position.downQuantity).mul(position.averageDownCost));
    }
    const unrealized = value.minus(cost);
    const gross = d(this.state.realizedPnl).plus(unrealized);
    const netTrading = gross.minus(this.state.fees).minus(this.state.slippage);
    const economic = netTrading.minus(this.state.aiCost).minus(this.state.infrastructureCost);
    const equity = d(this.state.cash).plus(value);
    const high = Decimal.max(this.state.highWaterMark, equity);
    this.state = {
      ...this.state, positionValue: decimal(value), equity: decimal(equity), unrealizedPnl: decimal(unrealized),
      grossPnl: decimal(gross), netTradingPnl: decimal(netTrading), netEconomicPnl: decimal(economic),
      highWaterMark: decimal(high), drawdown: decimal(high.minus(equity)), updatedAt: at,
    };
    return this.snapshot();
  }
}

import { Decimal } from "decimal.js";
import { createId, type FeeSchedule, type LedgerState, type Outcome, type PaperFill, type Position } from "./domain.js";
import { calculateFee, d, decimal } from "./calculations.js";
import { classifySettledOutcome, settlementCashDelta, settlementRealizedDelta } from "./settlement.js";

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

  applyFill(fill: PaperFill, options?: { positionId?: string }): Position {
    const now = fill.timestamp;
    const previous = this.positions.get(fill.marketId) ?? {
      id: options?.positionId ?? createId("pos"), marketId: fill.marketId, upQuantity: "0", downQuantity: "0",
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

  settlePosition(marketId: string, upPayoff: "0" | "1", downPayoff: "0" | "1", at = new Date()): {
    position: Position;
    cashDelta: string;
    realizedDelta: string;
    outcome: "POSITION_WIN" | "POSITION_LOSS" | "POSITION_BREAKEVEN";
    grossOutcome: "POSITION_WIN" | "POSITION_LOSS" | "POSITION_BREAKEVEN";
  } | null {
    const position = this.positions.get(marketId);
    if (!position) return null;
    if (d(position.upQuantity).eq(0) && d(position.downQuantity).eq(0)) {
      this.positions.delete(marketId);
      return null;
    }
    const cashDelta = settlementCashDelta(position, upPayoff, downPayoff);
    const realizedDelta = settlementRealizedDelta(position, upPayoff, downPayoff);
    this.state.cash = decimal(d(this.state.cash).plus(cashDelta));
    this.state.realizedPnl = decimal(d(this.state.realizedPnl).plus(realizedDelta));
    this.positions.delete(marketId);
    this.state.updatedAt = at.toISOString();
    this.revalue({}, at.toISOString());
    return {
      position: structuredClone(position), cashDelta, realizedDelta,
      outcome: classifySettledOutcome(realizedDelta, position.totalFees),
      grossOutcome: classifySettledOutcome(realizedDelta, "0"),
    };
  }

  addOperatingCosts(aiCost: Decimal.Value, infrastructureCost: Decimal.Value, at = new Date()): void {
    this.state.aiCost = decimal(d(this.state.aiCost).plus(aiCost));
    this.state.infrastructureCost = decimal(d(this.state.infrastructureCost).plus(infrastructureCost));
    this.revalue({}, at.toISOString());
  }

  resetOperatingCostBaseline(at = new Date()): void {
    this.state.aiCost = "0";
    this.state.infrastructureCost = "0";
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
    const netTrading = gross.minus(this.state.fees);
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

/** Per-market / per-asset paper exposure. Unknown assets never inherit another market's book. */
export function computePaperExposures(
  positions: Iterable<Position>,
  marketId: string,
  assetByMarket: ReadonlyMap<string, string | null>,
  nowMs = Date.now(),
): { market: string; total: string; directional: string; unhedged: string; unhedgedDurationMs: number; asset: string } {
  const list = [...positions];
  let total = 0; let directional = 0; let market = 0; let duration = 0; let asset = 0;
  const assetKey = assetByMarket.get(marketId) ?? null;
  for (const position of list) {
    const value = Number(position.upQuantity) * Number(position.averageUpCost) + Number(position.downQuantity) * Number(position.averageDownCost);
    const unhedged = Number(position.unpairedQuantity) * Number(position.unpairedOutcome === "UP" ? position.averageUpCost : position.averageDownCost);
    total += value; directional += unhedged;
    if (position.marketId === marketId) { market = value; if (unhedged > 0) duration = nowMs - new Date(position.openedAt).getTime(); }
    if (assetKey && assetByMarket.get(position.marketId) === assetKey) asset += value;
  }
  const selected = list.find((position) => position.marketId === marketId);
  const unhedged = selected ? Number(selected.unpairedQuantity) * Number(selected.unpairedOutcome === "UP" ? selected.averageUpCost : selected.averageDownCost) : 0;
  return {
    market: String(market), total: String(total), directional: String(directional), unhedged: String(unhedged),
    unhedgedDurationMs: Math.max(0, duration), asset: String(asset),
  };
}

export interface ProjectablePaperLeg {
  marketId: string;
  outcome: Outcome;
  quantity: Decimal.Value;
  avgCost: Decimal.Value;
  asset?: string | null;
}

export interface PaperExposureProjectionInput {
  current: ReturnType<typeof computePaperExposures>;
  positions: readonly ProjectablePaperLeg[];
  candidate: {
    marketId: string;
    outcome: Outcome;
    quantity: Decimal.Value;
    price: Decimal.Value;
    intent: "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG";
    asset?: string | null;
  };
  nowMs?: number;
}

type ProjectedLeg = { qty: Decimal; cost: Decimal; asset: string | null };

/** Quantity-based binary projection. Fully paired UP/DOWN qty has zero directional/unhedged residual. */
export function projectPaperExposures(input: PaperExposureProjectionInput): ReturnType<typeof computePaperExposures> {
  const books = new Map<string, { UP: ProjectedLeg; DOWN: ProjectedLeg }>();
  const ensure = (marketId: string) => {
    const existing = books.get(marketId);
    if (existing) return existing;
    const created = {
      UP: { qty: d(0), cost: d(0), asset: null as string | null },
      DOWN: { qty: d(0), cost: d(0), asset: null as string | null },
    };
    books.set(marketId, created);
    return created;
  };
  const add = (marketId: string, outcome: Outcome, quantity: Decimal.Value, price: Decimal.Value, asset?: string | null) => {
    const book = ensure(marketId);
    const leg = book[outcome];
    const qty = d(quantity);
    if (qty.lte(0)) return;
    const next = leg.qty.plus(qty);
    leg.cost = next.gt(0) ? leg.qty.mul(leg.cost).plus(qty.mul(price)).div(next) : d(0);
    leg.qty = next;
    if (asset) {
      book.UP.asset = asset;
      book.DOWN.asset = asset;
    }
  };
  for (const position of input.positions) {
    add(position.marketId, position.outcome, position.quantity, position.avgCost, position.asset);
  }
  add(
    input.candidate.marketId,
    input.candidate.outcome,
    input.candidate.quantity,
    input.candidate.price,
    input.candidate.asset,
  );
  const nowIso = new Date(input.nowMs ?? Date.now()).toISOString();
  const projected: Position[] = [];
  const assetByMarket = new Map<string, string | null>();
  for (const [marketId, book] of books) {
    const paired = Decimal.min(book.UP.qty, book.DOWN.qty);
    const difference = book.UP.qty.minus(book.DOWN.qty);
    const asset = book.UP.asset ?? book.DOWN.asset;
    assetByMarket.set(marketId, asset);
    projected.push({
      id: `proj-${marketId}`, marketId,
      upQuantity: decimal(book.UP.qty), downQuantity: decimal(book.DOWN.qty),
      averageUpCost: decimal(book.UP.cost), averageDownCost: decimal(book.DOWN.cost),
      pairedQuantity: decimal(paired),
      unpairedOutcome: difference.gt(0) ? "UP" : difference.lt(0) ? "DOWN" : null,
      unpairedQuantity: decimal(difference.abs()),
      combinedPairCost: decimal(book.UP.cost.plus(book.DOWN.cost)),
      totalFees: "0", totalSlippage: "0", openedAt: nowIso, updatedAt: nowIso,
    });
  }
  const computed = computePaperExposures(projected, input.candidate.marketId, assetByMarket, input.nowMs);
  const stillUnhedged = Number(computed.unhedged) > 0;
  return {
    ...computed,
    unhedgedDurationMs: stillUnhedged ? input.current.unhedgedDurationMs : 0,
  };
}

export function makerReserveNotional(quantity: string, price: string, schedule?: FeeSchedule | null): string {
  const notional = d(quantity).mul(price);
  const fee = schedule ? d(calculateFee(quantity, price, "MAKER", schedule)) : d(0);
  return decimal(notional.plus(fee));
}

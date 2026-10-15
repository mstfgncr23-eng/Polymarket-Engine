import { classifySettledOutcome, d, decimal, type PaperFill, type Position } from "@pm/core";
import { z } from "zod";

const decimalSchema = z.string().refine((value) => d(value).isFinite());
export const economicTotalsSchema = z.object({
  trades: z.number().int().nonnegative().safe(), meteors: z.number().int().nonnegative().safe(),
  completedNetPnl: decimalSchema, completedFees: decimalSchema, worstTradeNetPnl: decimalSchema,
  buySpend: decimalSchema, sellProceeds: decimalSchema, settlementPayout: decimalSchema, fees: decimalSchema,
});
export type EconomicResults = z.infer<typeof economicTotalsSchema>;
export const resultAccountingSchema = z.object({
  available: z.boolean(),
  completed: z.number().int().nonnegative().safe(),
  wins: z.number().int().nonnegative().safe(),
  losses: z.number().int().nonnegative().safe(),
  breakeven: z.number().int().nonnegative().safe(),
  // One entry per outstanding economic episode; deleted at closure/settlement.
  episodes: z.record(z.string(), z.object({ realized: decimalSchema, fees: decimalSchema })),
  // Missing in historical checkpoints: remain unavailable until a new experiment.
  economics: economicTotalsSchema.optional(),
}).refine((r) => r.completed === r.wins + r.losses + r.breakeven, "result totals mismatch");
export type ResultAccounting = z.infer<typeof resultAccountingSchema>;
export type TradingResults = { completed: number; wins: number; losses: number; breakeven: number; winRate: number | null };

export function emptyResultAccounting(available = true): ResultAccounting {
  return { available, completed: 0, wins: 0, losses: 0, breakeven: 0, episodes: {},
    ...(available ? { economics: { trades: 0, meteors: 0, completedNetPnl: "0", completedFees: "0", worstTradeNetPnl: "0",
      buySpend: "0", sellProceeds: "0", settlementPayout: "0", fees: "0" } } : {}) };
}

export function economicResults(accounting: ResultAccounting): EconomicResults | null {
  return accounting.available && accounting.economics ? structuredClone(accounting.economics) : null;
}

export function tradingResults(accounting: ResultAccounting): TradingResults | null {
  if (!accounting.available) return null;
  const { completed, wins, losses, breakeven } = accounting;
  return { completed, wins, losses, breakeven, winRate: completed ? wins / completed : null };
}

export function completeResult(accounting: ResultAccounting, marketId: string, realized: string, fees: string): void {
  const episode = accounting.episodes[marketId];
  const outcome = classifySettledOutcome(d(realized).plus(episode?.realized ?? 0).toFixed(), fees);
  const totals = accounting.economics;
  if (totals) {
    const net = d(realized).plus(episode?.realized ?? 0).minus(fees);
    totals.completedNetPnl = d(totals.completedNetPnl).plus(net).toFixed();
    totals.completedFees = d(totals.completedFees).plus(fees).toFixed();
    if (net.lte(-2)) totals.meteors++;
    if (net.lt(totals.worstTradeNetPnl)) totals.worstTradeNetPnl = net.toFixed();
  }
  accounting.completed += 1;
  if (outcome === "POSITION_WIN") accounting.wins += 1;
  else if (outcome === "POSITION_LOSS") accounting.losses += 1;
  else accounting.breakeven += 1;
  delete accounting.episodes[marketId];
}

export function accountResultFill(accounting: ResultAccounting, fill: PaperFill, previous: Position | undefined, remaining: Position | undefined): void {
  const totals = accounting.economics;
  if (totals) {
    if (!previous && fill.side === "BUY") totals.trades++;
    const notional = d(fill.quantity).mul(fill.fillPrice);
    if (fill.side === "BUY") totals.buySpend = d(totals.buySpend).plus(notional).toFixed();
    else totals.sellProceeds = d(totals.sellProceeds).plus(notional).toFixed();
    totals.fees = d(totals.fees).plus(fill.feeUsd).toFixed();
  }
  const episode = accounting.episodes[fill.marketId] ?? { realized: "0", fees: previous?.totalFees ?? "0" };
  if (fill.side === "SELL" && previous) {
    const cost = fill.outcome === "UP" ? previous.averageUpCost : previous.averageDownCost;
    episode.realized = decimal(d(episode.realized).plus(d(fill.quantity).mul(d(fill.fillPrice).minus(cost))));
  }
  episode.fees = decimal(d(episode.fees).plus(fill.feeUsd));
  accounting.episodes[fill.marketId] = episode;
  if (!remaining && previous) completeResult(accounting, fill.marketId, "0", episode.fees);
}

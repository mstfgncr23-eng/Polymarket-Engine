import { classifySettledOutcome, d, decimal, type PaperFill, type Position } from "@pm/core";
import { z } from "zod";

const decimalSchema = z.string().refine((value) => d(value).isFinite());
export const resultAccountingSchema = z.object({
  available: z.boolean(),
  completed: z.number().int().nonnegative().safe(),
  wins: z.number().int().nonnegative().safe(),
  losses: z.number().int().nonnegative().safe(),
  breakeven: z.number().int().nonnegative().safe(),
  // One entry per outstanding economic episode; deleted at closure/settlement.
  episodes: z.record(z.string(), z.object({ realized: decimalSchema, fees: decimalSchema })),
}).refine((r) => r.completed === r.wins + r.losses + r.breakeven, "result totals mismatch");
export type ResultAccounting = z.infer<typeof resultAccountingSchema>;
export type TradingResults = { completed: number; wins: number; losses: number; breakeven: number; winRate: number | null };

export function emptyResultAccounting(available = true): ResultAccounting {
  return { available, completed: 0, wins: 0, losses: 0, breakeven: 0, episodes: {} };
}

export function tradingResults(accounting: ResultAccounting): TradingResults | null {
  if (!accounting.available) return null;
  const { completed, wins, losses, breakeven } = accounting;
  return { completed, wins, losses, breakeven, winRate: completed ? wins / completed : null };
}

export function completeResult(accounting: ResultAccounting, marketId: string, realized: string, fees: string): void {
  const episode = accounting.episodes[marketId];
  const outcome = classifySettledOutcome(d(realized).plus(episode?.realized ?? 0).toFixed(), fees);
  accounting.completed += 1;
  if (outcome === "POSITION_WIN") accounting.wins += 1;
  else if (outcome === "POSITION_LOSS") accounting.losses += 1;
  else accounting.breakeven += 1;
  delete accounting.episodes[marketId];
}

export function accountResultFill(accounting: ResultAccounting, fill: PaperFill, previous: Position | undefined, remaining: Position | undefined): void {
  const episode = accounting.episodes[fill.marketId] ?? { realized: "0", fees: previous?.totalFees ?? "0" };
  if (fill.side === "SELL" && previous) {
    const cost = fill.outcome === "UP" ? previous.averageUpCost : previous.averageDownCost;
    episode.realized = decimal(d(episode.realized).plus(d(fill.quantity).mul(d(fill.fillPrice).minus(cost))));
  }
  episode.fees = decimal(d(episode.fees).plus(fill.feeUsd));
  accounting.episodes[fill.marketId] = episode;
  if (!remaining && previous) completeResult(accounting, fill.marketId, "0", episode.fees);
}

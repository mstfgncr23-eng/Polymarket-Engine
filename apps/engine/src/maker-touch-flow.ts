import { d, type MarketTrade } from "@pm/core";

/** A bounded observation window, never a fill prediction or an executor. */
export class MakerTouchFlow {
  private readonly tokens = new Map<string, { epoch: number; events: MarketTrade[] }>();
  constructor(private readonly windowMs: number) {}
  record(trade: MarketTrade, now: number, epoch: number, synchronized: boolean): void {
    const source = Date.parse(trade.sourceTimestamp), received = Date.parse(trade.receivedAt);
    if (!synchronized || trade.tokenEpoch !== epoch || !Number.isFinite(source) || !Number.isFinite(received)
      || source > now || received > now || source > received || now - source > this.windowMs
      || trade.side !== "SELL" || !d(trade.quantity).isFinite() || d(trade.quantity).lte(0)
      || !d(trade.price).isFinite() || d(trade.price).lte(0) || d(trade.price).gte(1)) return;
    const previous = this.tokens.get(trade.tokenId);
    const events = previous?.epoch === epoch ? previous.events.filter(e => now - Date.parse(e.sourceTimestamp) <= this.windowMs) : [];
    if (events.some(e => e.eventId === trade.eventId)) return;
    events.push(trade);
    // Sort first: out-of-order observations cannot hide expired entries.
    events.sort((a,b) => Date.parse(a.sourceTimestamp) - Date.parse(b.sourceTimestamp));
    this.tokens.set(trade.tokenId, { epoch, events: events.slice(-4096) });
  }
  at(token: string, price: string, now: number, epoch: number): { count: number; qty: string } {
    const state = this.tokens.get(token);
    const events = state?.epoch === epoch ? state.events.filter(e => {
      const age = now - Date.parse(e.sourceTimestamp);
      return age >= 0 && age <= this.windowMs && d(e.price).eq(price);
    }) : [];
    return { count: events.length, qty: events.reduce((sum,e) => sum.plus(e.quantity), d(0)).toFixed() };
  }
  retain(tokens: ReadonlySet<string>): void { for (const token of this.tokens.keys()) if (!tokens.has(token)) this.tokens.delete(token); }
}

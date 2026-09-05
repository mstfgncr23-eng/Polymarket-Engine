import type { RestingPaperOrder } from "@pm/core";

/** Derived runtime index: tokenId → Set<orderId>. Not a second durable authority. */
export class RestingOrderTokenIndex {
  private readonly ordersByToken = new Map<string, Set<string>>();
  private readonly tokenByOrder = new Map<string, string>();

  rebuild(orders: Iterable<RestingPaperOrder>): void {
    this.ordersByToken.clear();
    this.tokenByOrder.clear();
    for (const resting of orders) this.add(resting.order.id, resting.order.tokenId);
  }

  add(orderId: string, tokenId: string): void {
    const previous = this.tokenByOrder.get(orderId);
    if (previous === tokenId) {
      this.ordersByToken.get(tokenId)?.add(orderId);
      return;
    }
    if (previous) this.remove(orderId);
    let set = this.ordersByToken.get(tokenId);
    if (!set) {
      set = new Set();
      this.ordersByToken.set(tokenId, set);
    }
    set.add(orderId);
    this.tokenByOrder.set(orderId, tokenId);
  }

  remove(orderId: string): void {
    const tokenId = this.tokenByOrder.get(orderId);
    if (!tokenId) return;
    this.tokenByOrder.delete(orderId);
    const set = this.ordersByToken.get(tokenId);
    if (!set) return;
    set.delete(orderId);
    if (set.size === 0) this.ordersByToken.delete(tokenId);
  }

  orderIdsFor(tokenId: string): ReadonlySet<string> {
    return this.ordersByToken.get(tokenId) ?? EMPTY_ORDER_IDS;
  }

  entryCount(): number {
    return this.tokenByOrder.size;
  }

  tokenCount(): number {
    return this.ordersByToken.size;
  }
}

const EMPTY_ORDER_IDS: ReadonlySet<string> = new Set();

/** Derived runtime index: asset → Set<marketId>. Not a durable authority. */
export class AssetMarketIndex {
  private readonly markets = new Map<string, Set<string>>();
  private readonly assetOf = new Map<string, string>();

  add(asset: string, marketId: string): void {
    const previous = this.assetOf.get(marketId);
    if (previous === asset) {
      this.markets.get(asset)?.add(marketId);
      return;
    }
    if (previous) this.removeMarket(marketId);
    let set = this.markets.get(asset);
    if (!set) {
      set = new Set();
      this.markets.set(asset, set);
    }
    set.add(marketId);
    this.assetOf.set(marketId, asset);
  }

  removeMarket(marketId: string): void {
    const asset = this.assetOf.get(marketId);
    if (!asset) return;
    this.assetOf.delete(marketId);
    const set = this.markets.get(asset);
    if (!set) return;
    set.delete(marketId);
    if (set.size === 0) this.markets.delete(asset);
  }

  dropAsset(asset: string): string[] {
    const set = this.markets.get(asset);
    if (!set) return [];
    const ids = [...set];
    for (const id of ids) this.assetOf.delete(id);
    this.markets.delete(asset);
    return ids;
  }

  hasAsset(asset: string): boolean {
    return (this.markets.get(asset)?.size ?? 0) > 0;
  }

  marketIds(asset: string): string[] {
    const set = this.markets.get(asset);
    return set ? [...set] : [];
  }

  entryCount(): number {
    return this.assetOf.size;
  }

  assetCount(): number {
    return this.markets.size;
  }

  clear(): void {
    this.markets.clear();
    this.assetOf.clear();
  }
}

const UNDERLYING_ASSET_ALIASES: Readonly<Record<string, string>> = Object.freeze({
  BITCOIN: "BTC",
  ETHEREUM: "ETH",
  SOLANA: "SOL",
  DOGECOIN: "DOGE",
});

/** Canonical economic asset identity used before venue-specific mapping. */
export function canonicalAssetIdentity(asset: string): string {
  const normalized = asset.trim().toUpperCase();
  return UNDERLYING_ASSET_ALIASES[normalized] ?? normalized;
}

/** Venue mapping is algorithmic. BTC→BTCUSDT is an example of this rule, not a closed asset list. */
export function toBinanceSpotSymbol(asset: string): string {
  const ticker = asset.replace(/[^A-Za-z0-9]/g, "").toUpperCase();
  if (!ticker) throw new Error("INVALID_ASSET");
  if (ticker.endsWith("USDT") && ticker.length > 4) return ticker;
  return `${ticker}USDT`;
}

export function assetFromVenueSymbol(symbol: string): string {
  const normalized = symbol.replace(/[^A-Za-z0-9]/g, "").toUpperCase();
  return normalized.endsWith("USDT") ? normalized.slice(0, -4) : normalized;
}

export function referenceStreamUrl(baseUrl: string, symbol: string): string {
  const stream = `${symbol.toLowerCase()}@aggTrade`;
  return baseUrl.endsWith("/ws") ? `${baseUrl}/${stream}` : `${baseUrl.replace(/\/$/, "")}/ws/${stream}`;
}

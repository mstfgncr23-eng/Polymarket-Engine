"use client";

import { useEffect, useState } from "react";
import type { CacheMetadata } from "../../lib/types";

export function CacheStatus({ cache, now }: { cache: CacheMetadata | undefined; now?: number }) {
  const [clock, setClock] = useState<number | null>(null);
  useEffect(() => {
    setClock(Date.now());
    const timer = setInterval(() => setClock(Date.now()), 1_000);
    return () => clearInterval(timer);
  }, []);
  if (!cache) return <p className="freshnessLine">Cached snapshot · upstream sync unavailable</p>;
  const age = (now ?? clock) != null && cache.lastUpstreamSync
    ? Math.max(0, (now ?? clock)! - Date.parse(cache.lastUpstreamSync)) : cache.cacheAgeMs;
  const stale = cache.stale || age === null || age > cache.staleAfterMs;
  return <p className="freshnessLine" aria-label="Dashboard cache freshness">
    <strong>{cache.mode.replace("_", " ")}</strong> · CACHED{stale ? " / STALE" : ""} · Last upstream sync: {age === null ? "unavailable" : `${Math.floor(age / 1_000)}s ago`}
    {" · "}Source snapshot: {cache.sourceGeneratedAt ?? "unavailable"} · Poll: {cache.pollIntervalMs / 1_000}s · Upstream: {cache.upstreamReachable ? "reachable" : "unreachable"}
  </p>;
}

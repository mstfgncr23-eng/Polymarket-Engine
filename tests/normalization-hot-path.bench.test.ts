import { writeFileSync } from "node:fs";
import { cpus } from "node:os";
import { describe, expect, it } from "vitest";
import { PolymarketMarketDataAdapter } from "@pm/market-data";
import { ReferencePolymarketNormalizer } from "./normalization-reference.js";

interface BenchRow {
  workload: string;
  levelsPerSide: number;
  priceChanges: number;
  snapshots: number;
  referenceWallMs: number;
  optimizedWallMs: number;
  referenceCpuMs: number;
  optimizedCpuMs: number;
  referenceUsPerSnapshot: number;
  optimizedUsPerSnapshot: number;
  referenceSnapshotsPerSec: number;
  optimizedSnapshotsPerSec: number;
  wallImprovementPct: number;
  cpuImprovementPct: number;
  referenceHeapDeltaMb: number;
  optimizedHeapDeltaMb: number;
}

function createOptimized() {
  return new PolymarketMarketDataAdapter({
    subscribe: async () => ({ close: async () => undefined, [Symbol.asyncIterator]: async function* () {} }),
    closeSubscriptions: async () => undefined,
    fetchMarket: async () => ({}),
    listEvents: async function* () {},
  } as never);
}

function setup(adapter: PolymarketMarketDataAdapter | ReferencePolymarketNormalizer): void {
  adapter.registerManualToken("tok-up", "mkt", "UP", "BTC");
  adapter.setTokenIds(["tok-up"]);
}

function levels(count: number, start: number, step: number): Array<{ price: string; size: string }> {
  return Array.from({ length: count }, (_, index) => ({
    price: (start + index * step).toFixed(4),
    size: "1",
  }));
}

function bookEvent(levelsPerSide: number) {
  return {
    type: "book",
    payload: {
      tokenId: "tok-up",
      market: "mkt",
      timestamp: 1_000,
      bids: levels(levelsPerSide, 0.1, 0.001).reverse(),
      asks: levels(levelsPerSide, 0.6, 0.001),
      lastTradePrice: "0.50",
      hash: "h0",
    },
  };
}

function priceChangeEvents(count: number, levelsPerSide: number) {
  const events: unknown[] = [];
  for (let index = 0; index < count; index += 1) {
    const buy = index % 2 === 0;
    const offset = index % levelsPerSide;
    const price = buy ? (0.1 + offset * 0.001).toFixed(4) : (0.6 + offset * 0.001).toFixed(4);
    events.push({
      type: "price_change",
      payload: {
        timestamp: 1_001 + index,
        priceChanges: [{
          tokenId: "tok-up",
          side: buy ? "BUY" : "SELL",
          price,
          size: String(1 + (index % 7)),
          hash: `h${index}`,
        }],
      },
    });
  }
  return events;
}

function cpuMs(usage: NodeJS.CpuUsage): number {
  return (usage.user + usage.system) / 1000;
}

function measure(
  label: "reference" | "optimized",
  levelsPerSide: number,
  changeCount: number,
): { wallMs: number; cpuMs: number; snapshots: number; heapDeltaMb: number } {
  const adapter = label === "optimized" ? createOptimized() : new ReferencePolymarketNormalizer();
  setup(adapter);
  const book = bookEvent(levelsPerSide);
  const changes = priceChangeEvents(changeCount, levelsPerSide);
  adapter.normalizeSourceEvent(book);
  if (typeof globalThis.gc === "function") globalThis.gc();
  const heapBefore = process.memoryUsage().heapUsed;
  const cpuBefore = process.cpuUsage();
  const started = process.hrtime.bigint();
  let snapshots = 0;
  for (const event of changes) snapshots += adapter.normalizeSourceEvent(event).length;
  const elapsedNs = process.hrtime.bigint() - started;
  const cpu = process.cpuUsage(cpuBefore);
  const heapAfter = process.memoryUsage().heapUsed;
  return {
    wallMs: Number(elapsedNs) / 1_000_000,
    cpuMs: cpuMs(cpu),
    snapshots,
    heapDeltaMb: (heapAfter - heapBefore) / (1024 * 1024),
  };
}

describe("normalization hot-path benchmark vs main reference", () => {
  it("records wall/CPU improvement for 20/100/500 level books", () => {
    const rows: BenchRow[] = [];
    const sizes = [20, 100, 500] as const;
    const bursts = [100, 500, 1_000] as const;
    for (const levelsPerSide of sizes) {
      for (const priceChanges of bursts) {
        measure("reference", levelsPerSide, 20);
        measure("optimized", levelsPerSide, 20);
        const reference = measure("reference", levelsPerSide, priceChanges);
        const optimized = measure("optimized", levelsPerSide, priceChanges);
        const wallImprovementPct = reference.wallMs === 0 ? 0 : (1 - optimized.wallMs / reference.wallMs) * 100;
        const cpuImprovementPct = reference.cpuMs === 0 ? 0 : (1 - optimized.cpuMs / reference.cpuMs) * 100;
        rows.push({
          workload: `${levelsPerSide}x${priceChanges}`,
          levelsPerSide,
          priceChanges,
          snapshots: optimized.snapshots,
          referenceWallMs: reference.wallMs,
          optimizedWallMs: optimized.wallMs,
          referenceCpuMs: reference.cpuMs,
          optimizedCpuMs: optimized.cpuMs,
          referenceUsPerSnapshot: (reference.wallMs * 1000) / reference.snapshots,
          optimizedUsPerSnapshot: (optimized.wallMs * 1000) / optimized.snapshots,
          referenceSnapshotsPerSec: reference.snapshots / (reference.wallMs / 1000),
          optimizedSnapshotsPerSec: optimized.snapshots / (optimized.wallMs / 1000),
          wallImprovementPct,
          cpuImprovementPct,
          referenceHeapDeltaMb: reference.heapDeltaMb,
          optimizedHeapDeltaMb: optimized.heapDeltaMb,
        });
      }
    }

    const report = {
      measuredAt: new Date().toISOString(),
      node: process.version,
      cpuCount: cpus().length,
      cpuModel: cpus()[0]?.model ?? "unknown",
      gcAvailable: typeof globalThis.gc === "function",
      rows,
    };
    writeFileSync("reports/normalization-hot-path-bench.json", `${JSON.stringify(report, null, 2)}\n`);
    const table = [
      "workload | ref wall ms | opt wall ms | wall % | ref cpu ms | opt cpu ms | cpu % | ref us/snap | opt us/snap | ref snap/s | opt snap/s | ref heap ΔMB | opt heap ΔMB",
      ...rows.map((row) => [
        row.workload,
        row.referenceWallMs.toFixed(2),
        row.optimizedWallMs.toFixed(2),
        row.wallImprovementPct.toFixed(1),
        row.referenceCpuMs.toFixed(2),
        row.optimizedCpuMs.toFixed(2),
        row.cpuImprovementPct.toFixed(1),
        row.referenceUsPerSnapshot.toFixed(1),
        row.optimizedUsPerSnapshot.toFixed(1),
        row.referenceSnapshotsPerSec.toFixed(0),
        row.optimizedSnapshotsPerSec.toFixed(0),
        row.referenceHeapDeltaMb.toFixed(2),
        row.optimizedHeapDeltaMb.toFixed(2),
      ].join(" | ")),
    ].join("\n");
    console.log(`\nNormalization hot-path benchmark\n${table}\n`);

    const at100x1000 = rows.find((row) => row.levelsPerSide === 100 && row.priceChanges === 1_000);
    expect(at100x1000).toBeDefined();
    expect(at100x1000!.snapshots).toBe(1_000);
    expect(at100x1000!.optimizedWallMs).toBeLessThan(at100x1000!.referenceWallMs);
  });
});

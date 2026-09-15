import { Worker } from "node:worker_threads";
import { describe, expect, it } from "vitest";
import {
  deriveBook,
  engineEnvSchema,
  TriggerEngine,
  triggerConfigSchema,
  type MarketSnapshot,
  type TriggerEvent,
} from "@pm/core";
import { snapshot as fixtureSnapshot } from "./fixtures.js";
import { EngineMetrics, FORBIDDEN_METRIC_LABEL_NAMES } from "../apps/engine/src/metrics.js";
import { MarketDerivationWorkerService } from "../apps/engine/src/market-derivation-service.js";
import { startQualifiedEngine } from "./paper-engine-testkit.js";

function book(overrides: Partial<MarketSnapshot> = {}): MarketSnapshot {
  return fixtureSnapshot(overrides);
}

function comparable(event: TriggerEvent) {
  return {
    kind: event.kind,
    severity: event.severity,
    score: event.score,
    source: event.source,
    evidence: event.evidence,
    sourceEventIds: event.sourceEventIds,
  };
}

function syncTriggers(snapshots: MarketSnapshot[]): TriggerEvent[] {
  const engine = new TriggerEngine(triggerConfigSchema.parse({
    cooldownMs: 0, imbalanceThreshold: 0.5, spreadChangeThreshold: 0.01, depthChangeThreshold: 0.2,
  }), () => new Date("2026-08-31T12:00:00.000Z"));
  return snapshots.flatMap((item) => engine.ingestMarket(item));
}

async function workerTriggers(service: MarketDerivationWorkerService, snapshots: MarketSnapshot[]): Promise<TriggerEvent[]> {
  const engine = new TriggerEngine(triggerConfigSchema.parse({
    cooldownMs: 0, imbalanceThreshold: 0.5, spreadChangeThreshold: 0.01, depthChangeThreshold: 0.2,
  }), () => new Date("2026-08-31T12:00:00.000Z"));
  const events: TriggerEvent[] = [];
  for (const item of snapshots) {
    const { derived } = await service.deriveSnapshot(item);
    events.push(...engine.ingestMarketWithDerived(item, derived));
  }
  return events;
}

const STREAM: MarketSnapshot[] = [
  book({ bids: [{ price: "0.47", quantity: "100" }], asks: [{ price: "0.48", quantity: "1" }] }),
  book({ eventId: "e2", sequence: 2, bids: [{ price: "0.40", quantity: "10" }], asks: [{ price: "0.48", quantity: "10" }] }),
];

describe("PR52 worker failure fallback", () => {
  it("falls back when the worker throws on start and still matches the sync path", async () => {
    const service = new MarketDerivationWorkerService({ timeoutMs: 500, workerData: { mode: "throw-on-start" } });
    expect(await service.start()).toBe(false);
    const events = await workerTriggers(service, STREAM);
    expect(events.map(comparable)).toEqual(syncTriggers(STREAM).map(comparable));
    await service.stop();
  });

  it("falls back when the worker exits on start", async () => {
    const service = new MarketDerivationWorkerService({ timeoutMs: 500, workerData: { mode: "exit-on-start" } });
    expect(await service.start()).toBe(false);
    expect((await service.deriveSnapshot(STREAM[0]!)).status).toBe("fallback");
    expect((await service.deriveSnapshot(STREAM[0]!)).derived).toEqual(deriveBook(STREAM[0]!));
    await service.stop();
  });

  it("falls back when the worker throws on a message", async () => {
    const service = new MarketDerivationWorkerService({ timeoutMs: 2_000, workerData: { mode: "throw-on-message" } });
    expect(await service.start()).toBe(true);
    const result = await service.deriveSnapshot(STREAM[0]!);
    expect(result.status).toBe("error");
    expect(result.derived).toEqual(deriveBook(STREAM[0]!));
    const second = await service.deriveSnapshot(STREAM[1]!);
    expect(second.status).toBe("fallback");
    expect(second.derived).toEqual(deriveBook(STREAM[1]!));
    await service.stop();
  });

  it("falls back on a malformed response and ignores a later success after disable", async () => {
    const service = new MarketDerivationWorkerService({ timeoutMs: 2_000, workerData: { mode: "malformed" } });
    expect(await service.start()).toBe(true);
    const result = await service.deriveSnapshot(STREAM[0]!);
    expect(result.status).toBe("error");
    expect(result.derived).toEqual(deriveBook(STREAM[0]!));
    const next = await service.deriveSnapshot(STREAM[1]!);
    expect(next.status).toBe("fallback");
    await service.stop();
  });

  it("times out, falls back, and ignores the late worker response", async () => {
    const service = new MarketDerivationWorkerService({ timeoutMs: 30, workerData: { mode: "hang" } });
    expect(await service.start()).toBe(true);
    const result = await service.deriveSnapshot(STREAM[0]!);
    expect(result.status).toBe("timeout");
    expect(result.derived).toEqual(deriveBook(STREAM[0]!));
    expect((service as unknown as { worker: Worker | null }).worker).toBeNull();
    const next = await service.deriveSnapshot(STREAM[1]!);
    expect(next.status).toBe("fallback");
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(service.lateResponseCount).toBeGreaterThanOrEqual(0);
    await service.stop();
  });
});

describe("PR52 worker ordering", () => {
  it("cannot ingest snapshot N+1 before N even if derive calls overlap", async () => {
    const service = new MarketDerivationWorkerService({ timeoutMs: 2_000, workerData: { delayMs: 25 } });
    expect(await service.start()).toBe(true);
    try {
      const ingested: string[] = [];
      const engine = new TriggerEngine(triggerConfigSchema.parse({
        cooldownMs: 0, imbalanceThreshold: 1, spreadChangeThreshold: 0.01, depthChangeThreshold: 0.2,
      }), () => new Date("2026-08-31T12:00:00.000Z"));
      const first = book({ eventId: "n", sequence: 1, bids: [{ price: "0.40", quantity: "10" }], asks: [{ price: "0.42", quantity: "10" }] });
      const second = book({ eventId: "n1", sequence: 2, bids: [{ price: "0.40", quantity: "10" }], asks: [{ price: "0.60", quantity: "10" }] });
      const apply = async (snapshot: MarketSnapshot) => {
        const { derived } = await service.deriveSnapshot(snapshot);
        ingested.push(snapshot.eventId);
        return engine.ingestMarketWithDerived(snapshot, derived);
      };
      const overlapping = Promise.all([apply(first), apply(second)]);
      const [firstEvents, secondEvents] = await overlapping;
      expect(ingested).toEqual(["n", "n1"]);
      expect(firstEvents.some((item) => item.kind === "SPREAD_CHANGE")).toBe(false);
      expect(secondEvents.some((item) => item.kind === "SPREAD_CHANGE")).toBe(true);
    } finally {
      await service.stop();
    }
  });
});

describe("PR52 worker shutdown", () => {
  it("terminates the worker without hanging the process", async () => {
    const service = new MarketDerivationWorkerService({ timeoutMs: 2_000 });
    expect(await service.start()).toBe(true);
    const stop = service.stop();
    await Promise.race([
      stop,
      new Promise((_, reject) => setTimeout(() => reject(new Error("WORKER_STOP_HUNG")), 2_000)),
    ]);
  });

  it("does not admit triggers if experiment generation changes during worker await", async () => {
    const started = await startQualifiedEngine({
      MARKET_DERIVATION_WORKER_ENABLED: "true",
      MARKET_DERIVATION_WORKER_TIMEOUT_MS: "2000",
    });
    const delayed = new MarketDerivationWorkerService({ timeoutMs: 2_000, workerData: { delayMs: 80 } });
    expect(await delayed.start()).toBe(true);
    (started.engine as unknown as { derivationWorker: MarketDerivationWorkerService | null }).derivationWorker = delayed;
    try {
      const snap = book({
        tokenId: "btc-up",
        marketId: "btc-mkt",
        bids: [{ price: "0.47", quantity: "100" }],
        asks: [{ price: "0.48", quantity: "1" }],
      });
      const pending = started.market.emitSnapshot(snap);
      await new Promise((resolve) => setTimeout(resolve, 20));
      (started.engine as unknown as { experimentRunGeneration: number }).experimentRunGeneration += 1;
      await pending;
      const item = started.engine.metrics.registry.getSingleMetric("triggers_emitted_total");
      const data = item ? await item.get() : { values: [] };
      const emitted = data.values
        .filter((point) => !point.metricName?.endsWith("_created") && !point.metricName?.endsWith("_bucket"))
        .reduce((sum, point) => sum + point.value, 0);
      expect(emitted).toBe(0);
    } finally {
      await delayed.stop();
      await started.engine.stop();
    }
  });

  it("engine stop joins the worker when the flag is on", async () => {
    const started = await startQualifiedEngine({
      MARKET_DERIVATION_WORKER_ENABLED: "true",
      MARKET_DERIVATION_WORKER_TIMEOUT_MS: "2000",
    });
    try {
      await started.market.emitSnapshot(book({ tokenId: "btc-up", marketId: "btc-mkt" }));
      const stop = started.engine.stop();
      await Promise.race([
        stop,
        new Promise((_, reject) => setTimeout(() => reject(new Error("ENGINE_STOP_HUNG")), 5_000)),
      ]);
    } finally {
      await started.engine.stop();
    }
  });
});

describe("PR52 feature flag default", () => {
  it("keeps MARKET_DERIVATION_WORKER_ENABLED off unless explicitly set", () => {
    expect(engineEnvSchema.parse({}).MARKET_DERIVATION_WORKER_ENABLED).toBe(false);
    expect(engineEnvSchema.parse({ MARKET_DERIVATION_WORKER_ENABLED: "true" }).MARKET_DERIVATION_WORKER_ENABLED).toBe(true);
    expect(engineEnvSchema.parse({ MARKET_DERIVATION_WORKER_ENABLED: "1" }).MARKET_DERIVATION_WORKER_ENABLED).toBe(true);
    expect(engineEnvSchema.parse({ MARKET_DERIVATION_WORKER_TIMEOUT_MS: "75" }).MARKET_DERIVATION_WORKER_TIMEOUT_MS).toBe(75);
  });
});

describe("PR52 worker metrics", () => {
  it("records bounded status labels and never uses market/token/trace identifiers", async () => {
    const metrics = new EngineMetrics();
    const service = new MarketDerivationWorkerService({
      timeoutMs: 2_000,
      observer: metrics.marketDerivationWorkerObserver(),
    });
    expect(await service.start()).toBe(true);
    await service.deriveSnapshot(STREAM[0]!);
    await service.stop();
    const text = await metrics.registry.metrics();
    expect(text).toMatch(/market_derivation_worker_requests_total/);
    expect(text).toMatch(/status="success"/);
    expect(text).toMatch(/market_derivation_worker_latency_seconds/);
    expect(text).toMatch(/market_derivation_worker_in_flight/);
    for (const forbidden of FORBIDDEN_METRIC_LABEL_NAMES) {
      expect(text).not.toContain(`${forbidden}=`);
    }
  });
});

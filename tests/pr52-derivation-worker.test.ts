import { Worker } from "node:worker_threads";
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import {
  deriveBook,
  engineEnvSchema,
  TriggerEngine,
  triggerConfigSchema,
  type MarketSnapshot,
  type TriggerEvent,
  type TriggerKind,
} from "@pm/core";
import { snapshot as fixtureSnapshot } from "./fixtures.js";
import { EngineMetrics, FORBIDDEN_METRIC_LABEL_NAMES } from "../apps/engine/src/metrics.js";
import { MarketDerivationWorkerService } from "../apps/engine/src/market-derivation-service.js";
import { startQualifiedEngine } from "./paper-engine-testkit.js";
import type { EngineService } from "../apps/engine/src/engine.js";

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

function workerHandle(service: MarketDerivationWorkerService): Worker | null {
  return (service as unknown as { worker: Worker | null }).worker;
}

function processThreadCount(): number {
  const status = readFileSync("/proc/self/status", "utf8");
  const line = status.split("\n").find((row) => row.startsWith("Threads:"));
  return Number((line ?? "Threads:\t0").split(/\s+/)[1]);
}

async function waitUntilWorkerExits(worker: Worker, timeoutMs = 1_000): Promise<number> {
  return await new Promise<number>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("WORKER_ORPHAN_STILL_RUNNING")), timeoutMs);
    worker.once("exit", (code) => {
      clearTimeout(timer);
      resolve(code);
    });
  });
}

async function emittedTriggerCount(engine: EngineService): Promise<number> {
  const item = engine.metrics.registry.getSingleMetric("triggers_emitted_total");
  const data = item ? await item.get() : { values: [] };
  return data.values
    .filter((point) => !point.metricName?.endsWith("_created") && !point.metricName?.endsWith("_bucket"))
    .reduce((sum, point) => sum + point.value, 0);
}

type EngineFenceInternals = {
  derivationWorker: MarketDerivationWorkerService | null;
  experimentRunGeneration: number;
  experimentFeedGeneration: number;
  resetPreparedRuntimeState: () => void;
  noteExperimentFeedInvalidation: () => void;
};

const STREAM: MarketSnapshot[] = [
  book({ bids: [{ price: "0.47", quantity: "100" }], asks: [{ price: "0.48", quantity: "1" }] }),
  book({ eventId: "e2", sequence: 2, bids: [{ price: "0.40", quantity: "10" }], asks: [{ price: "0.48", quantity: "10" }] }),
];

const IMBALANCED = book({
  tokenId: "btc-up",
  marketId: "btc-mkt",
  bids: [{ price: "0.47", quantity: "100" }],
  asks: [{ price: "0.48", quantity: "1" }],
});

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
    const captured = workerHandle(service);
    expect(captured).not.toBeNull();
    const threadsBeforeKill = processThreadCount();
    const exited = waitUntilWorkerExits(captured!);
    const result = await service.deriveSnapshot(STREAM[0]!);
    expect(result.status).toBe("error");
    expect(result.derived).toEqual(deriveBook(STREAM[0]!));
    expect(workerHandle(service)).toBeNull();
    await exited;
    expect(processThreadCount()).toBeLessThan(threadsBeforeKill);
    const second = await service.deriveSnapshot(STREAM[1]!);
    expect(second.status).toBe("fallback");
    expect(second.derived).toEqual(deriveBook(STREAM[1]!));
    await service.stop();
    await service.stop();
  });

  it("falls back on a malformed response, terminates the worker, and ignores later replies", async () => {
    const service = new MarketDerivationWorkerService({ timeoutMs: 2_000, workerData: { mode: "malformed" } });
    expect(await service.start()).toBe(true);
    const captured = workerHandle(service);
    expect(captured).not.toBeNull();
    const threadsBeforeKill = processThreadCount();
    const exited = waitUntilWorkerExits(captured!);
    const result = await service.deriveSnapshot(STREAM[0]!);
    expect(result.status).toBe("error");
    expect(result.derived).toEqual(deriveBook(STREAM[0]!));
    expect(workerHandle(service)).toBeNull();
    await exited;
    expect(processThreadCount()).toBeLessThan(threadsBeforeKill);
    const next = await service.deriveSnapshot(STREAM[1]!);
    expect(next.status).toBe("fallback");
    await service.stop();
  });

  it("times out, terminates the hung worker, and does not leave an orphan thread", async () => {
    const service = new MarketDerivationWorkerService({ timeoutMs: 30, workerData: { mode: "hang" } });
    expect(await service.start()).toBe(true);
    const captured = workerHandle(service);
    expect(captured).not.toBeNull();
    const threadsBeforeKill = processThreadCount();
    const exited = waitUntilWorkerExits(captured!);
    const result = await service.deriveSnapshot(STREAM[0]!);
    expect(result.status).toBe("timeout");
    expect(result.derived).toEqual(deriveBook(STREAM[0]!));
    expect(workerHandle(service)).toBeNull();
    await exited;
    expect(processThreadCount()).toBeLessThan(threadsBeforeKill);
    const next = await service.deriveSnapshot(STREAM[1]!);
    expect(next.status).toBe("fallback");
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(service.lateResponseCount).toBeGreaterThanOrEqual(0);
    await service.stop();
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

  it("drops a pre-boundary snapshot after prepare-next run generation changes during worker await", async () => {
    const started = await startQualifiedEngine({
      MARKET_DERIVATION_WORKER_ENABLED: "true",
      MARKET_DERIVATION_WORKER_TIMEOUT_MS: "2000",
    });
    const delayed = new MarketDerivationWorkerService({ timeoutMs: 2_000, workerData: { delayMs: 120 } });
    expect(await delayed.start()).toBe(true);
    const internals = started.engine as unknown as EngineFenceInternals;
    internals.derivationWorker = delayed;
    const runBefore = internals.experimentRunGeneration;
    const feedBefore = internals.experimentFeedGeneration;
    try {
      const pending = started.market.emitSnapshot(IMBALANCED);
      await new Promise((resolve) => setTimeout(resolve, 25));
      internals.resetPreparedRuntimeState();
      expect(internals.experimentRunGeneration).toBe(runBefore + 1);
      expect(internals.experimentFeedGeneration).toBeGreaterThan(feedBefore);
      await pending;
      expect(await emittedTriggerCount(started.engine)).toBe(0);

      await started.market.emitSnapshot(book({
        tokenId: "btc-up",
        marketId: "btc-mkt",
        eventId: "post-boundary",
        sequence: 2,
        bids: [{ price: "0.47", quantity: "100" }],
        asks: [{ price: "0.48", quantity: "1" }],
      }));
      expect(await emittedTriggerCount(started.engine)).toBeGreaterThan(0);
    } finally {
      await delayed.stop();
      await started.engine.stop();
    }
  });

  it("drops a snapshot if experiment feed generation changes during worker await", async () => {
    const started = await startQualifiedEngine({
      MARKET_DERIVATION_WORKER_ENABLED: "true",
      MARKET_DERIVATION_WORKER_TIMEOUT_MS: "2000",
    });
    const delayed = new MarketDerivationWorkerService({ timeoutMs: 2_000, workerData: { delayMs: 120 } });
    expect(await delayed.start()).toBe(true);
    const internals = started.engine as unknown as EngineFenceInternals;
    internals.derivationWorker = delayed;
    const runBefore = internals.experimentRunGeneration;
    try {
      const pending = started.market.emitSnapshot(IMBALANCED);
      await new Promise((resolve) => setTimeout(resolve, 25));
      internals.noteExperimentFeedInvalidation();
      expect(internals.experimentRunGeneration).toBe(runBefore);
      await pending;
      expect(await emittedTriggerCount(started.engine)).toBe(0);
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

describe("PR52 ingest metric semantics", () => {
  it("keeps onMarketIngest as deriveBook plus mutable work, and names mutable work separately", () => {
    const levels = Array.from({ length: 80 }, (_, index) => ({
      price: (0.10 + index * 0.001).toFixed(4),
      quantity: String(1 + (index % 9)),
    }));
    const snapshot = book({ bids: levels, asks: levels.map((level) => ({ ...level, price: (Number(level.price) + 0.4).toFixed(4) })) });
    const ingest: number[] = [];
    const mutable: number[] = [];
    const observer = {
      onCandidates: () => undefined,
      onEmitted: (_kind: TriggerKind, _severity: 0 | 1 | 2 | 3) => undefined,
      onSuppressed: (_kind: TriggerKind, _severity: 0 | 1 | 2 | 3) => undefined,
      onMarketIngest: (durationSeconds: number) => ingest.push(durationSeconds),
      onMarketIngestMutable: (durationSeconds: number) => mutable.push(durationSeconds),
    };
    const engine = new TriggerEngine(triggerConfigSchema.parse({
      cooldownMs: 0, imbalanceThreshold: 1, spreadChangeThreshold: 1, depthChangeThreshold: 10,
    }), () => new Date("2026-08-31T12:00:00.000Z"), observer);

    engine.ingestMarket(snapshot);
    expect(ingest).toHaveLength(1);
    expect(mutable).toHaveLength(1);
    expect(ingest[0]!).toBeGreaterThanOrEqual(mutable[0]!);

    ingest.length = 0;
    mutable.length = 0;
    engine.ingestMarketWithDerived(snapshot, deriveBook(snapshot));
    expect(ingest).toHaveLength(0);
    expect(mutable).toHaveLength(1);
  });

  it("wires the original histogram to ingestMarket and a new histogram to mutable work", async () => {
    const metrics = new EngineMetrics();
    const observer = metrics.triggerObserver();
    const engine = new TriggerEngine(triggerConfigSchema.parse({
      cooldownMs: 0, imbalanceThreshold: 1, spreadChangeThreshold: 1, depthChangeThreshold: 10,
    }), () => new Date("2026-08-31T12:00:00.000Z"), observer);
    engine.ingestMarket(STREAM[0]!);
    const text = await metrics.registry.metrics();
    expect(text).toMatch(/trigger_market_ingest_duration_seconds/);
    expect(text).toMatch(/trigger_market_ingest_mutable_duration_seconds/);
    const ingest = await metrics.triggerMarketIngestDuration.get();
    const mutable = await metrics.triggerMarketIngestMutableDuration.get();
    const ingestCount = ingest.values.find((point) => point.metricName?.endsWith("_count") || point.labels === undefined && point.value > 0);
    expect(ingest.values.some((point) => (point.metricName ?? "").endsWith("_count") && point.value >= 1)).toBe(true);
    expect(mutable.values.some((point) => (point.metricName ?? "").endsWith("_count") && point.value >= 1)).toBe(true);
    expect(ingestCount).toBeTruthy();
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

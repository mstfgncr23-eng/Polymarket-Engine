import { describe, expect, it } from "vitest";
import { createId, engineEnvSchema, externalAgentRoleDefinitions, type TriggerEvent } from "@pm/core";
import { MemoryEventJournal, createJournalEvent } from "@pm/db";
import { ExponentialBackoff, parseBinanceAggTrade } from "@pm/market-data";
import { EngineService } from "../apps/engine/src/engine.js";

const trigger: TriggerEvent = { id: createId("trigger"), traceId: createId("trace"), marketId: "btc", kind: "BOOK_IMBALANCE", severity: 2, score: 1, dedupeKey: "x", evidence: {}, sourceEventIds: ["event"], createdAt: new Date().toISOString(), expiresAt: new Date(Date.now() + 60_000).toISOString() };

describe("resilience integration boundaries", () => {
  it("defines exactly six external Grok Bot roles", () => {
    expect(externalAgentRoleDefinitions).toHaveLength(6);
    expect(new Set(externalAgentRoleDefinitions.map((role) => role.id)).size).toBe(6);
  });

  it("keeps monitoring safely when every Grok Bot connection is unavailable", async () => {
    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({ NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", POLYMARKET_TOKEN_IDS: "up,down" });
    const engine = new EngineService(env, journal, { error: () => undefined });
    await engine.registerTriggerForResearch(trigger);
    expect(engine.state.recentTriggers).toHaveLength(1);
    expect(engine.state.latestAgents.size).toBe(0);
    expect(engine.ledger.positions.size).toBe(0);
  });

  it("recovers a durable checkpoint after a simulated DB outage", async () => {
    const journal = new MemoryEventJournal();
    journal.available = false;
    await expect(journal.append(createJournalEvent({ traceId: "trace", type: "X", aggregateType: "x", aggregateId: "x", payload: {} }))).rejects.toThrow("DATABASE_UNAVAILABLE");
    journal.available = true;
    await journal.append(createJournalEvent({ traceId: "trace", type: "RECOVERED", aggregateType: "x", aggregateId: "x", payload: {} }));
    await journal.saveCheckpoint({ experimentId: "experiment", sequence: 7, payload: { ledger: { cash: "1000" }, positions: [] } });
    expect((await journal.loadLatestCheckpoint())?.sequence).toBe(7);
    expect(await journal.healthCheck()).toBe(true);
  });

  it("drains, checkpoints, and restores engine state across restart", async () => {
    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({ NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", POLYMARKET_TOKEN_IDS: "up,down" });
    const pendingFeed = { registerManualToken: () => undefined, run: async (_ids: unknown, _handlers: unknown, signal: AbortSignal) => new Promise<void>((resolve) => signal.addEventListener("abort", () => resolve(), { once: true })) };
    const pendingBtc = { run: async (_handlers: unknown, signal: AbortSignal) => new Promise<void>((resolve) => signal.addEventListener("abort", () => resolve(), { once: true })) };
    const logger = { error: () => undefined };
    const first = new EngineService(env, journal, logger, { market: pendingFeed as never, btc: pendingBtc as never });
    await first.start();
    await first.stop();
    expect(first.state.draining).toBe(true);
    expect((await journal.loadLatestCheckpoint())?.sequence).toBeGreaterThan(0);
    const second = new EngineService(env, journal, logger, { market: pendingFeed as never, btc: pendingBtc as never });
    await second.start();
    expect(second.state.experimentId).toBe(first.state.experimentId);
    await second.stop();
  });

  it("parses real feed format and uses bounded exponential reconnect delays", () => {
    expect(parseBinanceAggTrade(JSON.stringify({ s: "BTCUSDT", p: "65000.1", q: "0.2", T: Date.now(), a: 42 })).price).toBe("65000.1");
    const backoff = new ExponentialBackoff({ initialDelayMs: 100, maxDelayMs: 400, multiplier: 2, jitterRatio: 0 }, () => 0.5);
    expect([backoff.nextDelay(), backoff.nextDelay(), backoff.nextDelay(), backoff.nextDelay()]).toEqual([100, 200, 400, 400]);
  });
});

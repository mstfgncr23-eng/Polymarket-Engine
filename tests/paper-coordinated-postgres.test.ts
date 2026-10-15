import { describe, expect, it } from "vitest";
import { resolve } from "node:path";
import { PostgresEventJournal } from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { ScriptedMarketAdapter, ScriptedReferenceRegistry, discoveryReport, qualified, qualifiedPaperEnv, paperFill } from "./paper-engine-testkit.js";

const url = process.env.PAPER_COORDINATED_TEST_DATABASE_URL;
describe.skipIf(!url)("coordinated PAPER on disposable PostgreSQL 17", () => {
  it("real SQL commits, duplicate SELL, checkpoint and restart conserve net dollars", async () => {
    // Never point this integration fixture at the operational database.
    if (!url || new URL(url).hostname !== "paper-coordinated-test-pg" || new URL(url).pathname !== "/paper_test") throw new Error("DISPOSABLE_TEST_DATABASE_REQUIRED");
    const start = async () => {
      const journal = new PostgresEventJournal(url, resolve("packages/db/migrations"));
      const market = new ScriptedMarketAdapter();
      market.queue(discoveryReport([qualified("sql-mkt", "XRP", "sql-up", "sql-down")]));
      const engine = new EngineService(qualifiedPaperEnv({ ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" }), journal, { error: () => undefined }, {
        market: market as never, referenceRegistry: new ScriptedReferenceRegistry() as never, activation: new NoopGrokActivationBridge() });
      await engine.start(); return { engine, journal };
    };
    let s = await start();
    try {
      expect((await s.engine.beginExperiment()).ok).toBe(true);
      const buy = paperFill("sql-mkt", "UP", "0.65", { tokenId: "sql-up", feeUsd: "0.1" });
      const sell = paperFill("sql-mkt", "UP", "0.40", { tokenId: "sql-up", side: "SELL", feeUsd: "0.1" });
      await Promise.all([s.engine.applyObservedFill(buy), s.engine.applyObservedFill(buy)]);
      await Promise.all([s.engine.applyObservedFill(sell), s.engine.applyObservedFill(sell)]);
      const before = s.engine.state.snapshot();
      expect(before.economicResults).toMatchObject({ trades: 1, completedNetPnl: "-2.7", fees: "0.2", meteors: 1 });
      expect((await s.journal.queryDurableTransitions({ types: ["PAPER_FILL"], limit: 100 }))).toHaveLength(2);
      await s.engine.checkpointForTest(); await s.engine.stop(); s = await start();
      expect(s.engine.state.snapshot().economicResults).toEqual(before.economicResults);
      expect(s.engine.ledger.snapshot()).toEqual(before.balance);
      await s.engine.applyObservedFill(sell);
      expect(s.engine.state.snapshot().economicResults).toEqual(before.economicResults);
      console.log(JSON.stringify({ evidence: "REAL_POSTGRES17_CASHFLOW_RESTART", before: before.economicResults, after: s.engine.state.snapshot().economicResults,
        netTradingPnl: s.engine.ledger.snapshot().netTradingPnl, duplicateFills: 0, databaseHost: new URL(url).hostname }));
    } finally { await s.engine.stop(); }
  }, 30000);
});

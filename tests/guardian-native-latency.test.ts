import { describe, it, expect } from "vitest";
import { randomUUID } from "node:crypto";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { performance } from "node:perf_hooks";
import { PolymarketMarketDataAdapter } from "../packages/market-data/src/polymarket.js";
import { ScriptedBoundedMarketTransport } from "../packages/market-data/src/scripted-market-transport.js";
import { startGuardianSocket, guardianRpc } from "../apps/engine/src/guardian-link.js";

describe("Guardian socket fairness under native feed backlog", () => {
  it("serves a local observer during a buffered burst while delivering every book in order", async () => {
    const transport = new ScriptedBoundedMarketTransport();
    const adapter = new PolymarketMarketDataAdapter(undefined, { transport });
    adapter.registerManualToken("up", "fairness", "UP", "BTC"); adapter.setTokenIds(["up"]);
    const abort = new AbortController(), seen: number[] = [];
    const path = process.platform === "win32" ? "\\\\.\\pipe\\fairness-" + randomUUID() : join(tmpdir(), randomUUID() + ".sock");
    const socket = await startGuardianSocket(path, async () => ({ seen: seen.length }));
    const running = adapter.run(["up"], { onSnapshot: snapshot => {
      const until = performance.now() + .1; while (performance.now() < until) { /* representative synchronous callback work */ }
      seen.push(snapshot.sequence);
    }, onConnection: () => undefined, onError: error => { throw error; } }, abort.signal);
    try {
      while (!transport.stream) await new Promise(r => setTimeout(r, 1));
      for (let i = 0; i < 600; i++) expect(transport.push({ type: "book", payload: {
        tokenId: "up", market: "fairness", timestamp: Date.now(), bids: [{ price: "0.47", size: "10" }],
        asks: [{ price: "0.48", size: "10" }], lastTradePrice: "0.475",
      } }).ok).toBe(true);
      const reply = await guardianRpc<{ seen: number }>(path, { kind: "VIEW" });
      expect(reply.seen).toBeLessThan(600);
      const deadline = Date.now() + 5000;
      while (seen.length < 600) { if (Date.now() > deadline) throw new Error("RAW_BOOKS_NOT_DELIVERED"); await new Promise(r => setTimeout(r, 1)); }
      expect(seen).toEqual(Array.from({ length: 600 }, (_, i) => i + 1));
      console.log(JSON.stringify({ evidence: "BUFFERED_FEED_SOCKET_FAIRNESS", servedAfterBooks: reply.seen, totalBooks: seen.length }));
    } finally { abort.abort(); await running; await socket.stop(); }
  });
});

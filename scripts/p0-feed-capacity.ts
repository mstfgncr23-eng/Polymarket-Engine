/** Finite PAPER CPU benchmark: production normalizer -> real Engine callback.
 * No external feed, observer, strategy output or deployment is created. */
import { writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { performance } from "node:perf_hooks";
import { MemoryEventJournal } from "../packages/db/src/index.js";
import type { MarketSnapshot } from "@pm/core";
import { PolymarketMarketDataAdapter as SingleAdapter } from "../packages/market-data/src/polymarket.js";
import { EngineService } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { discoveryReport, qualified, qualifiedPaperEnv, ScriptedMarketAdapter, ScriptedReferenceRegistry } from "../tests/paper-engine-testkit.js";

const now = Date.now(), frames = 30000;
const fixture = new ScriptedMarketAdapter();
const markets = Array.from({length:40}, (_,i) => {
  const m = qualified(`capacity-${i}`, "XRP", `capacity-${i}-up`, `capacity-${i}-down`, new Date(now+900000).toISOString());
  m.raw = { eventStartTime: new Date(now-1000).toISOString() }; return m;
});
fixture.queue(discoveryReport(markets));
const normalizer = new SingleAdapter();
for(const m of markets) for(const t of m.tokens) {
  normalizer.registerManualToken(t.tokenId,m.id,t.outcome,m.asset);
  normalizer.getMarket(t.tokenId)!.feeSchedule=m.feeSchedule;
  fixture.markTokenSync(t.tokenId,"SYNCED");
}
normalizer.setTokenIds(markets.flatMap(m=>m.tokens.map(t=>t.tokenId)));
const journal = new MemoryEventJournal();
const engine = new EngineService(qualifiedPaperEnv({ PAPER_EXECUTION_STYLE:"TAKER", LEARNING_SHADOW_ENABLED:"false",
  PROFIT_TRAILING_PAIR_SOAK_ENABLED:"true", PROFIT_TRAILING_PAIR_ARM_EDGE:"0.05", PROFIT_TRAILING_PAIR_RETRACE_EDGE:"0.02",
  NORMAL_HEDGE_MIN_LOCKED_PAIR_EDGE:"0.12",
  GUARDIAN_SEPARATE_ENABLED:"true", ESTIMATED_INFRA_COST_PER_HOUR_USD:"0" }), journal, {error:()=>undefined},
  { market:fixture as never, referenceRegistry:new ScriptedReferenceRegistry() as never, activation:new NoopGrokActivationBridge() });
await engine.start(); await fixture.waitForHandlers();
const dispatch = (s:MarketSnapshot) => (engine as unknown as {onMarketSnapshot(s:MarketSnapshot):Promise<void>}).onMarketSnapshot(s);
for(const m of markets) for(const t of m.tokens) {
  const result=normalizer.normalizeSourceEvent({type:"book",payload:{tokenId:t.tokenId,market:m.id,timestamp:now,
    bids:Array.from({length:45},(_,i)=>({price:(0.69-i*.001).toFixed(3),size:"100"})),
    asks:Array.from({length:45},(_,i)=>({price:(0.70+i*.001).toFixed(3),size:"100"})),minOrderSize:"1",tickSize:"0.001",hash:"initial"}});
  for(const s of result) await dispatch(s);
}
const hash=createHash("sha256"); let count=0;
const start=performance.now(), cpu=process.cpuUsage();
for(let i=0;i<frames;i++) {
  const m=markets[i%40]!;
  const deep=i%3===0, size=String(100+(i%7));
  const changes=m.tokens.flatMap(t=>[{tokenId:t.tokenId,side:"BUY",price:deep?"0.650":"0.690",size,hash:`${i}-b`},
    {tokenId:t.tokenId,side:"SELL",price:deep?"0.740":"0.700",size,hash:`${i}-a`}]);
  for(const s of normalizer.normalizeSourceEvent({type:"price_change",payload:{market:m.id,timestamp:Date.now(),priceChanges:changes}})) {
    await dispatch(s);count++; hash.update(`${s.tokenId}:${s.sequence}:${s.hash}:${s.bids[0]?.quantity}:${s.asks[0]?.quantity};`);
  }
}
const elapsed=performance.now()-start, used=process.cpuUsage(cpu);
const result={kind:"FINITE_SYNTHETIC_PAPER_NORMALIZER_ENGINE",frames,snapshots:count,markets:40,levelsPerSide:45,
  wallMs:elapsed,cpuMs:(used.user+used.system)/1000,snapshotsPerSecond:count*1000/elapsed,
  observationDigest:hash.digest("hex"),fills:(await journal.queryEvents({types:["PAPER_FILL"],limit:10})).length,
  limitations:["No network or PostgreSQL traffic in this CPU benchmark; owned-transport fairness and burst tests run separately."]};
console.log(JSON.stringify(result,null,2));
if(process.env.P0_FEED_BENCH_OUTPUT)writeFileSync(process.env.P0_FEED_BENCH_OUTPUT,JSON.stringify(result,null,2));
await engine.stop();

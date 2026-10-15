import { beforeEach, afterEach, describe, expect, it } from "vitest";
import { MemoryEventJournal, type EventJournal } from "@pm/db";
import { type ExperimentWsEvent } from "@pm/core";
import { EngineService } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { ScriptedMarketAdapter, ScriptedReferenceRegistry, discoveryReport, qualified, qualifiedPaperEnv } from "./paper-engine-testkit.js";
import { p0PostgresFixture } from "./p0-postgres-fixture.js";
let pg: Awaited<ReturnType<typeof p0PostgresFixture>> | undefined;
beforeEach(async()=>{if(process.env.P0_EXECUTION_TEST_DATABASE_URL)pg=await p0PostgresFixture(process.env.P0_EXECUTION_TEST_DATABASE_URL);});
afterEach(async()=>{if(pg){await pg.close();pg=undefined;}});
const NOW=Date.parse("2026-10-15T13:00:00Z");
async function setup(journal:EventJournal=pg?.journal??new MemoryEventJournal()) {
  const adapter=new ScriptedMarketAdapter();adapter.queue(discoveryReport([qualified("ws-market","XRP","ws-up","ws-down",new Date(NOW+900000).toISOString())]));
  const engine=new EngineService(qualifiedPaperEnv({ESTIMATED_INFRA_COST_PER_HOUR_USD:"0"}),journal,{error:()=>undefined},
    {market:adapter as never,referenceRegistry:new ScriptedReferenceRegistry() as never,activation:new NoopGrokActivationBridge(),now:()=>new Date(NOW)});
  await engine.start();await adapter.waitForHandlers();return {engine,journal};
}
const stats=(e:EngineService)=>e.state.snapshot().engine.experimentWs;
const send=(e:EngineService,event:ExperimentWsEvent)=>(e as unknown as {persistExperimentWsEvent(event:ExperimentWsEvent):Promise<void>}).persistExperimentWsEvent(event);
const event=(id:string,extra:Partial<ExperimentWsEvent>={}):ExperimentWsEvent=>({eventId:id,kind:"DISCONNECTED",shard:2,retry:true,
  reason:"count_limit",at:new Date(NOW).toISOString(),...extra});

describe("durable experiment WS isolation",()=>{
  it("starts B at zero, retains A and global counts, and deduplicates actual lifecycle facts",async()=>{
    const s=await setup();try{
      expect((await s.engine.beginExperiment()).ok).toBe(true);
      const a=s.engine.experimentManifest()!.id;
      s.engine.state.connections.polymarket.reconnects=123;
      expect(stats(s.engine)?.stats.disconnects).toBe(0);
      await Promise.all([send(s.engine,event("a-disconnect")),send(s.engine,event("a-disconnect"))]);
      await send(s.engine,event("a-attempt",{kind:"CONNECTING"}));
      await send(s.engine,event("a-connected",{kind:"CONNECTED"}));
      expect(stats(s.engine)?.stats).toMatchObject({disconnects:1,overflows:1,reconnectAttempts:1,reconnects:1,reasons:{count_limit:1}});
      await s.engine.closeExperimentEntries();await s.engine.completeExperimentForTest();await s.engine.writeFinalReportForTest();
      const report=await s.journal.loadExperimentReport(a,"final");
      expect(report).toBeTruthy();
      expect(JSON.stringify(report)).toContain('"experimentWsStats"');
      const old=JSON.stringify(report);
      expect((await s.engine.beginExperiment()).ok).toBe(true);
      expect(stats(s.engine)?.experimentId).not.toBe(a);
      expect(stats(s.engine)?.stats).toMatchObject({disconnects:0,reconnects:0,overflows:0,processRestarts:0});
      expect(s.engine.state.connections.polymarket.reconnects).toBe(123);
      await send(s.engine,event("b-disconnect",{reason:"slow_consumer",shard:4}));
      expect(stats(s.engine)?.stats).toMatchObject({disconnects:1,slowConsumers:1,overflows:0});
      expect(JSON.stringify(await s.journal.loadExperimentReport(a,"final"))).toBe(old);
      expect(await s.journal.queryEvents({types:["EXPERIMENT_WS_EVENT"],experimentId:a,limit:100})).toHaveLength(3);
    }finally{await s.engine.stop();}
  });
  it("preserves the active experiment across checkpoint/restart without subtracting reset process counters",async()=>{
    let s=await setup();try{
      await s.engine.beginExperiment();const id=s.engine.experimentManifest()!.id;
      await send(s.engine,event("before-restart"));await s.engine.checkpointForTest();
      const journal=s.journal;await s.engine.stop();s=await setup(journal);
      expect(stats(s.engine)?.experimentId).toBe(id);
      expect(stats(s.engine)?.stats).toMatchObject({disconnects:1,overflows:1,processRestarts:1});
      await send(s.engine,event("before-restart"));
      await send(s.engine,event("after-restart",{reason:"byte_limit",shard:1}));
      expect(stats(s.engine)?.stats).toMatchObject({disconnects:2,overflows:2,reasons:{count_limit:1,byte_limit:1}});
      expect(stats(s.engine)?.stats.shards.map(x=>x.disconnects)).toEqual([1,1,0,0]);
      expect(s.engine.integrityFault()).toBe(false);
    }finally{await s.engine.stop();}
  });
  it("does not attribute old/pre-start or post-completion events to a new experiment",async()=>{
    const s=await setup();try{
      await send(s.engine,event("idle"));await s.engine.beginExperiment();
      await send(s.engine,event("before-start",{at:new Date(NOW-1).toISOString()}));
      expect(stats(s.engine)?.stats.disconnects).toBe(0);
      await s.engine.closeExperimentEntries();await s.engine.completeExperimentForTest();
      await send(s.engine,event("after-completion"));
      expect(stats(s.engine)?.stats.disconnects).toBe(0);
    }finally{await s.engine.stop();}
  });
  it("repairs a committed lifecycle event after a crash without counting its retry again",async()=>{
    let s=await setup();try{
      await s.engine.beginExperiment();const id=s.engine.experimentManifest()!.id;
      s.engine.setCrashInjectionForTest("AFTER_DURABLE_COMMIT_BEFORE_APPLY",{operationKey:`experiment-ws:${id}:crash-event`});
      await expect(send(s.engine,event("crash-event"))).rejects.toThrow("CRASH_INJECTED");
      const journal=s.journal;await s.engine.stop();s=await setup(journal);
      await send(s.engine,event("crash-event"));
      expect(stats(s.engine)?.stats).toMatchObject({disconnects:1,overflows:1,processRestarts:1});
      expect(await journal.queryEvents({types:["EXPERIMENT_WS_EVENT"],experimentId:id,limit:100})).toHaveLength(2);
    }finally{await s.engine.stop();}
  });
});

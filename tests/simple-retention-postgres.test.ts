import {describe,it,expect} from "vitest";
import {mkdtemp,writeFile,readFile,rm} from "node:fs/promises";
import {join} from "node:path";
import {tmpdir} from "node:os";
import {EngineService} from "../apps/engine/src/engine.js";
import {NoopGrokActivationBridge} from "../apps/engine/src/activation/index.js";
import {LocalJournalArchive,LOCAL_ARCHIVE_SENTINEL,LOCAL_ARCHIVE_SENTINEL_CONTENT} from "../apps/engine/src/archive.js";
import {verifyRecoveryDirectory} from "../apps/engine/src/archive-recovery.js";
import {RetentionManager,defaultRetentionConfig,retentionConfigFromEngineEnv} from "../apps/engine/src/retention-manager.js";
import {ScriptedMarketAdapter,ScriptedReferenceRegistry,discoveryReport,qualified,qualifiedPaperEnv,paperFill} from "./paper-engine-testkit.js";
import {isolatedPostgres} from "./simple-postgres-fixture.js";
import {GuardianMonitor} from "../apps/engine/src/guardian-worker.js";
import {snapshot} from "./fixtures.js";
import {randomUUID} from "node:crypto";
import {guardianRpc,startGuardianSocket,type GuardianUpdate} from "../apps/engine/src/guardian-link.js";

const START=Date.parse("2026-10-05T00:00:00Z");
describe.skipIf(process.env.SIMPLE_RETENTION_POSTGRES_TEST!=="1")("simple Engine retention on existing isolated PostgreSQL",()=>{
  it("persists one Guardian socket protective PAPER fill to PostgreSQL and cannot duplicate it on retry or Engine/Guardian restart",async()=>{
    const f=await isolatedPostgres();let engine:EngineService|undefined;let socket:Awaited<ReturnType<typeof startGuardianSocket>>|undefined;
    const clock={ms:START};let market:ScriptedMarketAdapter;
    const start=async()=>{
      market=new ScriptedMarketAdapter();market.queue(discoveryReport([qualified("first","SOL","first-up","first-down",new Date(START+3600000).toISOString())]));
      engine=new EngineService(qualifiedPaperEnv({GUARDIAN_SEPARATE_ENABLED:"true",REVERSAL_PROTECTION_ENABLED:"true",PROFIT_TRAILING_PAIR_SOAK_ENABLED:"true",NORMAL_HEDGE_MIN_LOCKED_PAIR_EDGE:"0.12",DISCOVERY_INTERVAL_MS:"3600000",ESTIMATED_INFRA_COST_PER_HOUR_USD:"0",PAPER_EXECUTION_STYLE:"TAKER",PAPER_EXECUTION_LATENCY_MS:"0"}),f.journal,{error:()=>undefined},
        {market:market as never,referenceRegistry:new ScriptedReferenceRegistry() as never,activation:new NoopGrokActivationBridge(),now:()=>new Date(clock.ms),resolutionFetcher:async()=>null});
      await engine.start();await market.waitForHandlers();return engine;
    };
    const books=()=>{
      const pair=Object.fromEntries((["UP","DOWN"] as const).map(outcome=>{
        const mid=outcome==="UP"?(clock.ms===START?.75:.70)*.8:.71;
        const book=snapshot({marketId:"first",tokenId:"first-"+outcome.toLowerCase(),outcome,asset:"SOL",receivedAt:new Date(clock.ms).toISOString(),sourceTimestamp:new Date(clock.ms).toISOString(),
          bids:[{price:String(mid-.005),quantity:outcome==="UP"?"10000":"30000"}],asks:[{price:String(mid+.005),quantity:outcome==="UP"?"30000":"10000"}]});
        market.markTokenSync(book.tokenId,"SYNCED");engine!.state.marketSnapshots.set(book.tokenId,book);return[outcome,book];
      }));
      (engine as unknown as {snapshots:Map<string,object>}).snapshots.set("first",pair);
    };
    try{
      let e=await start();await e.beginExperiment();await e.applyObservedFill(paperFill("first","UP","0.80",{tokenId:"first-up",quantity:"5",timestamp:new Date(START).toISOString()}));
      let monitor=new GuardianMonitor();
      const path=process.platform==="win32"?"\\\\.\\pipe\\guardian-pg-"+randomUUID():join(tmpdir(),"guardian-pg-"+randomUUID()+".sock");
      socket=await startGuardianSocket(path,async raw=>engine!.acceptGuardianUpdate((raw as {update:GuardianUpdate}).update,false));
      for(const age of [0,5000,10000]) {clock.ms=START+age;books();const update=monitor.evaluate(e.guardianView(),clock.ms).updates[0]!;
        expect(["PERSISTED","QUEUED"]).toContain((await guardianRpc<{status:string}>(path,{kind:"UPDATE",update})).status);
        const deadline=Date.now()+4000;
        while(e.guardianView().markets[0]!.state?.revision!==update.state.revision && Date.now()<deadline)await new Promise(r=>setTimeout(r,5));
        expect(e.guardianView().markets[0]!.state?.revision).toBe(update.state.revision);}
      await e.stop();e=await start();monitor=new GuardianMonitor();books();
      expect(monitor.evaluate(e.guardianView(),clock.ms).markets[0]!.phase).toBe("EARLY_RECOVERY_WATCH");
      clock.ms=START+70000;books();e.state.noteReferenceTick("SOL",new Date(clock.ms).toISOString());
      const update=monitor.evaluate(e.guardianView(),clock.ms).updates[0]!;expect(update.state.protectionReason).toBe("EARLY_TIMEOUT");
      const replies=await Promise.all([guardianRpc<{status:string}>(path,{kind:"UPDATE",update}),guardianRpc<{status:string}>(path,{kind:"UPDATE",update})]);
      expect(replies.every(r=>["PERSISTED","REPLAY"].includes(r.status))).toBe(true);
      const end=Date.now()+4000;let fills=[];
      do{fills=(await f.journal.queryEvents({types:["PAPER_FILL"]})).filter(row=>row.payload.intent==="PROTECTIVE_DE_RISK");if(fills.length)break;await new Promise(r=>setTimeout(r,10));}while(Date.now()<end);
      expect(fills).toHaveLength(1);
      const durable=await f.journal.queryDurableTransitions({types:["PAPER_FILL"],limit:100});
      expect(durable.filter(row=>(row.payload.fill as {orderId?:string})?.orderId===fills[0]!.payload.orderId)).toHaveLength(1);
      await e.stop();e=await start();monitor=new GuardianMonitor();expect(monitor.evaluate(e.guardianView(),clock.ms).markets).toHaveLength(0);
      await guardianRpc(path,{kind:"UPDATE",update});
      expect((await f.journal.queryEvents({types:["PAPER_FILL"]})).filter(row=>row.payload.intent==="PROTECTIVE_DE_RISK")).toHaveLength(1);
      expect((await f.journal.queryEvents({types:["PAPER_ORDER_CREATED"]})).filter(row=>row.payload.intent==="PROTECTIVE_DE_RISK")).toHaveLength(1);
      expect(e.integrityFault()).toBe(false);
    }finally{await socket?.stop();await engine?.stop();await f.close();}
  },60000);
  it("rejects unverified archives, then purges only verified bounded expired rows and archived checkpoints while preserving the active experiment",async()=>{
    const f=await isolatedPostgres(),root=await mkdtemp(join(tmpdir(),"simple-retention-"));let engine:EngineService|undefined;
    try{
      await writeFile(join(root,LOCAL_ARCHIVE_SENTINEL),LOCAL_ARCHIVE_SENTINEL_CONTENT);
      const market=new ScriptedMarketAdapter();market.queue(discoveryReport([]));
      engine=new EngineService(qualifiedPaperEnv({GUARDIAN_SEPARATE_ENABLED:"true",DISCOVERY_INTERVAL_MS:"3600000",ESTIMATED_INFRA_COST_PER_HOUR_USD:"0"}),f.journal,{error:()=>undefined},
        {market:market as never,referenceRegistry:new ScriptedReferenceRegistry() as never,activation:new NoopGrokActivationBridge(),now:()=>new Date(START),resolutionFetcher:async()=>null});
      await engine.start();await market.waitForHandlers();await engine.beginExperiment();
      for(let i=0;i<10;i++)await engine.checkpointForTest();
      for(const [id,at] of [["expired-one",START-48*3600000],["expired-two",START-48*3600000+1000],["active-one",START+1000],["active-two",START+2000]] as const)
        await f.journal.append({id,type:"MARKET_SNAPSHOT",aggregateType:"market",aggregateId:"first",traceId:id,correlationId:id,
          recordedAt:new Date().toISOString(),sourceEventId:id,occurredAt:new Date(at).toISOString(),payload:{experimentId:at>=START?engine.experiment!.id:null}});
      await f.journal.claimIdempotency("test","old");await f.journal.claimIdempotency("test","current");
      await f.sql`update mcp_idempotency_keys set created_at=${new Date(START-48*3600000)} where key='old'`;
      const before=Number((await f.sql`select count(*) as n from engine_checkpoints`)[0].n);
      const config=defaultRetentionConfig({enabled:true,offsiteReplicaRequired:true,guardianRecoveryHold:false,localArchiveEnabled:true,localArchiveDir:root,compactionEnabled:true,batchSize:2,maxRowsPerCycle:6});
      let reject=true,verified=0;
      const prove=async()=>{if(reject)throw Error("REMOTE_SHA_MISMATCH");verified++;};
      const manager=new RetentionManager(f.journal,config,undefined,new LocalJournalArchive(root),{verifyCanonical:prove,verifyJournalBucket:prove,verifyRecovery:async input=>{await verifyRecoveryDirectory(input.directory);await prove();}});
      const context={experiment:engine.experiment,now:new Date(START+30*3600000),degraded:false,failClosed:false,databaseUp:true,hasEconomicObligations:false};
      const denied=await manager.runCycle(context);expect(denied.skipReason).toBe("REMOTE_SHA_MISMATCH");
      expect(Object.values(denied.deletedByClass).reduce((a,b)=>a+b,0)).toBe(0);
      expect((await f.journal.queryEvents({types:["MARKET_SNAPSHOT"]})).length).toBe(4);
      reject=false;const accepted=await manager.runCycle(context);
      expect(accepted.failures).toBe(0);expect(verified).toBeGreaterThan(0);
      expect(accepted.deletedByClass.raw_telemetry).toBe(2);expect(accepted.deletedByClass.mcp_idempotency).toBe(1);
      expect(accepted.deletedByClass.checkpoint).toBe(2);expect(accepted.deletedByClass.durable_transition).toBe(0);
      expect((await f.journal.queryEvents({types:["MARKET_SNAPSHOT"]})).map(e=>e.id).sort()).toEqual(["active-one","active-two"]);
      expect(Number((await f.sql`select count(*) as n from engine_checkpoints`)[0].n)).toBe(before-2);
      expect(await f.journal.idempotencyCreatedAt("test","current")).not.toBeNull();expect(engine.integrityFault()).toBe(false);
    }finally{await engine?.stop();await f.close();await rm(root,{recursive:true,force:true});}
  },60000);
  it("preserves checkpoint/replay cash, positions, cursor and operation-key authority across verified bounded compaction and restart",async()=>{
    const f=await isolatedPostgres(), root=await mkdtemp(join(tmpdir(),"simple-retention-"));
    const clock={ms:START};let engine:EngineService|undefined;
    const start=async()=>{
      const market=new ScriptedMarketAdapter();market.queue(discoveryReport([qualified("first","SOL","first-up","first-down",new Date(START+3600000).toISOString())]));
      engine=new EngineService(qualifiedPaperEnv({DISCOVERY_INTERVAL_MS:"3600000",ESTIMATED_INFRA_COST_PER_HOUR_USD:"0",GUARDIAN_SEPARATE_ENABLED:"true",REVERSAL_PROTECTION_ENABLED:"true",PROFIT_TRAILING_PAIR_SOAK_ENABLED:"true",NORMAL_HEDGE_MIN_LOCKED_PAIR_EDGE:"0.12",PAPER_EXECUTION_STYLE:"TAKER",PAPER_EXECUTION_LATENCY_MS:"0"}),
        f.journal,{error:()=>undefined},{market:market as never,referenceRegistry:new ScriptedReferenceRegistry() as never,activation:new NoopGrokActivationBridge(),now:()=>new Date(clock.ms),resolutionFetcher:async()=>null});
      await engine.start();await market.waitForHandlers();return engine;
    };
    try{
      await writeFile(join(root,LOCAL_ARCHIVE_SENTINEL),LOCAL_ARCHIVE_SENTINEL_CONTENT);
      let e=await start();expect((await e.beginExperiment()).ok).toBe(true);
      const fill=paperFill("first","UP","0.80",{tokenId:"first-up",quantity:"5",timestamp:new Date(START).toISOString()});
      await e.applyObservedFill(fill);
      await e.checkpointForTest();await e.checkpointForTest();
      const retained=await f.journal.listVerifiedCheckpoints();
      const protectedCheckpoints=retained.slice(0,2);
      clock.ms+=1000;
      await e.applyObservedFill(paperFill("first","DOWN","0.10",{tokenId:"first-down",quantity:"5",timestamp:new Date(clock.ms).toISOString()}));
      const before=e.ledger.snapshot(), positions=[...e.ledger.positions.values()], cursor=await f.journal.latestDurableSequence();
      expect(cursor).toBeGreaterThan(Number(protectedCheckpoints[0]!.payload.durableCursor));
      const archive=new LocalJournalArchive(root);
      const bundle=await archive.ensureRecoveryBundle(f.journal,{retained:protectedCheckpoints,candidates:retained.slice(2),prefixCursor:Number(protectedCheckpoints[1]!.payload.durableCursor),maxPrefixRows:200});
      const restore=await verifyRecoveryDirectory(bundle.directory);
      expect(restore.results.every(r=>r.replayed>0&&r.cursor===cursor)).toBe(true);
      const firstManifest=await readFile(join(bundle.directory,"manifest.json"),"utf8");
      await archive.ensureRecoveryBundle(f.journal,{retained:protectedCheckpoints,candidates:retained.slice(2),prefixCursor:Number(protectedCheckpoints[1]!.payload.durableCursor),maxPrefixRows:200});
      expect(await readFile(join(bundle.directory,"manifest.json"),"utf8")).toBe(firstManifest);
      const compact=await f.journal.compactDurableTransitionsThrough(bundle.manifest!.prefixThroughCursor,{maxRows:200,batchSize:200,maxBatches:20,budgetMs:250,maxReplayKeys:1});
      expect(compact.deleted).toBeGreaterThan(0);expect(compact.highWater).toBe(cursor);
      const prefix=JSON.parse((await readFile(join(bundle.directory,"durable-transitions.jsonl"),"utf8")).split("\n").find(l=>l.includes('"type":"PAPER_FILL"'))!);
      expect((await f.journal.readDurableByOperationKey(prefix.operationKey))?.payloadHash).toBe(prefix.payloadHash);
      expect((await f.journal.commitDurableTransition({operationKey:prefix.operationKey,type:prefix.type,aggregateType:prefix.aggregateType,aggregateId:prefix.aggregateId,payload:prefix.payload},cursor)).inserted).toBe(false);
      await e.stop();
      // Force recovery from the older retained snapshot rather than stop()'s fresh checkpoint.
      await f.sql`delete from engine_checkpoints where sequence > ${protectedCheckpoints[0]!.sequence}`;
      e=await start();expect(e.integrityFault()).toBe(false);
      expect(e.ledger.snapshot().cash).toBe(before.cash);expect([...e.ledger.positions.values()]).toEqual(positions);
      expect(await f.journal.latestDurableSequence()).toBe(cursor);
      await e.applyObservedFill(fill);expect(e.ledger.snapshot().cash).toBe(before.cash);
      expect((await f.journal.queryDurableTransitions({types:["PAPER_FILL"],limit:100})).length+Number((await f.sql`select count(*) as n from durable_replay_keys where type='PAPER_FILL'`)[0].n)).toBe(2);
      const active=e.experiment!;
      const manager=new RetentionManager(f.journal,defaultRetentionConfig({enabled:true,guardianRecoveryHold:false,offsiteReplicaRequired:true,localArchiveEnabled:true,localArchiveDir:root,compactionEnabled:true}),undefined,archive,
        {verifyCanonical:async()=>undefined,verifyJournalBucket:async()=>undefined,verifyRecovery:async()=>undefined});
      const n=Number((await f.sql`select count(*) as n from durable_transitions`)[0].n);
      const result=await manager.runCycle({experiment:active,now:new Date(START+10*3600000),degraded:false,failClosed:false,databaseUp:true,hasEconomicObligations:true});
      expect(result.failures).toBe(0);expect(result.deletedByClass.durable_transition).toBe(0);
      expect(Number((await f.sql`select count(*) as n from durable_transitions`)[0].n)).toBe(n);
      // An unavailable archive cannot fail-close the economic Engine.
      const broken=new RetentionManager(f.journal,defaultRetentionConfig({enabled:true,guardianRecoveryHold:false,offsiteReplicaRequired:true,localArchiveEnabled:true,localArchiveDir:root,checkpointKeep:1}),undefined,archive,
        {verifyCanonical:async()=>{throw Error("NETWORK_DOWN");},verifyJournalBucket:async()=>{throw Error("NETWORK_DOWN");},verifyRecovery:async()=>{throw Error("NETWORK_DOWN");}});
      await e.checkpointForTest();await e.checkpointForTest();
      const cpCount=Number((await f.sql`select count(*) as n from engine_checkpoints`)[0].n);
      expect((await broken.runCycle({experiment:active,now:new Date(clock.ms),degraded:false,failClosed:false,databaseUp:true,hasEconomicObligations:true})).skipReason).toBe("NETWORK_DOWN");
      expect(Number((await f.sql`select count(*) as n from engine_checkpoints`)[0].n)).toBe(cpCount);expect(e.integrityFault()).toBe(false);
      await e.applyObservedFill(paperFill("first","UP","0.70",{tokenId:"first-up",quantity:"1",timestamp:new Date(clock.ms).toISOString()}));
      expect(await f.journal.latestDurableSequence()).toBeGreaterThan(cursor);
      const parsed=retentionConfigFromEngineEnv(qualifiedPaperEnv({GUARDIAN_SEPARATE_ENABLED:"true"}));
      expect(parsed.guardianRecoveryHold).toBe(false);expect(parsed.offsiteReplicaRequired).toBe(true);
    }finally{await engine?.stop();await f.close();await rm(root,{recursive:true,force:true});}
  },60000);
});

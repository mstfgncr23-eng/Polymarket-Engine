import {createRequire} from "node:module";
import {fileURLToPath} from "node:url";
import {mkdtemp,rm} from "node:fs/promises";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {performance} from "node:perf_hooks";
import {afterAll,beforeAll,describe,expect,it} from "vitest";
import {createIdleExperiment,PaperLedger} from "@pm/core";
import {PostgresEventJournal,CHECKPOINT_SCHEMA_VERSION} from "@pm/db";
import {PolymarketMarketDataAdapter,ScriptedBoundedMarketTransport} from "@pm/market-data";
import {emptyResultAccounting} from "../apps/engine/src/trading-results.js";
import {GuardianPgFileAckStore} from "../apps/engine/src/guardian-pg-ack-store.js";
import {GuardianTrustedPgSession} from "../apps/engine/src/guardian-trusted-pg-session.js";
import {GuardianStandaloneBookObserver} from "../apps/engine/src/guardian-standalone-book-observer.js";

const postgres=createRequire(new URL("../packages/db/package.json",import.meta.url))("postgres");
const migrations=fileURLToPath(new URL("../packages/db/migrations",import.meta.url));
const URL_STRING=process.env.G2H_H4_LOCAL_DATABASE_URL;
const REQUIRE={host:"127.0.0.1",port:"55482",db:"g2h_h4_local_20261011",admin:"g2h_admin"};
const READER="g2h_h4_readonly_20261011",PASS="local_only_g2h_h4_reader";
const EXPERIMENT="g2h_h4_capacity";
const wait=(ms:number)=>new Promise<void>(r=>setTimeout(r,Math.max(0,ms)));
const percentile=(values:number[],q:number)=>values.length===0?null:
  Math.round([...values].sort((a,b)=>a-b)[Math.min(values.length-1,Math.ceil(q*values.length)-1)]!*100)/100;
const round=(v:number)=>Math.round(v*100)/100;
function testUri(){
  if(!URL_STRING)throw Error("G2H_H4_DISPOSABLE_URL_MISSING");
  const u=new URL(URL_STRING);
  if(u.hostname!==REQUIRE.host||u.port!==REQUIRE.port
    ||u.pathname!=="/"+REQUIRE.db||u.username!==REQUIRE.admin)
    throw Error("G2H_H4_REFUSE_NONLOCAL_POSTGRES");
  return u;
}
const scope={database:REQUIRE.db,schema:"public",role:READER};
const runs:number[]=[];
let admin:PostgresEventJournal;
let reader:PostgresEventJournal;
let sql:ReturnType<typeof postgres>;
let tmp:string;
describe.skipIf(!URL_STRING)("H4 paced READ-ONLY canonical PostgreSQL + ACK local benchmark",()=>{
  beforeAll(async()=>{
    const uri=testUri();
    sql=postgres(uri.toString(),{max:2});
    admin=new PostgresEventJournal(uri.toString(),migrations);
    await admin.initialize();
    const experiment={...createIdleExperiment({
      durationHours:2,strategyVersion:"g2h-h4",riskConfiguration:{},
      modelConfiguration:{},startingBalance:"1000",systemVersion:"offline",
      gitSha:"offline",
    }),id:EXPERIMENT,status:"RUNNING" as const,startedAt:"2026-10-11T00:00:00Z"};
    await admin.commitDurableTransition({
      operationKey:"h4:seed",type:"LEDGER_MARK_TO_MARKET",
      aggregateType:"position",aggregateId:"offline-seed",
      payload:{prices:{},markedAt:"2026-10-11T00:00:00Z"},
    },0);
    const ledger=new PaperLedger("1000",new Date("2026-10-11T00:00:00Z"));
    await admin.saveCheckpoint({experimentId:EXPERIMENT,sequence:1,payload:{
      schemaVersion:CHECKPOINT_SCHEMA_VERSION,durableCursor:1,
      experiment,ledger:ledger.snapshot(),positions:[],
      resultAccounting:emptyResultAccounting(),restingOrders:[],
      makerReservations:{},activeTriggers:[],grokSignals:[],
      riskReductionOwners:[],riskReductionFailures:[],
      reversalProtectionStates:[],profitTrailingPairStates:[],
      settlementLookups:[],settledMarkets:[],
      nakedDirectionalDiagnostics:[],lastInfrastructureAccrualAt:0,
    }});
    await sql`CREATE ROLE g2h_h4_readonly_20261011 LOGIN PASSWORD 'local_only_g2h_h4_reader'`;
    await sql`GRANT USAGE ON SCHEMA public TO g2h_h4_readonly_20261011`;
    await sql`GRANT SELECT ON engine_checkpoints,durable_transitions,durable_sequence_metadata
      TO g2h_h4_readonly_20261011`;
    for(const table of ["engine_checkpoints","durable_transitions","durable_sequence_metadata"]){
      await sql.unsafe(`CREATE POLICY g2h_h4_read ON ${table} FOR SELECT TO
        g2h_h4_readonly_20261011 USING (true)`);
    }
    const readUri=new URL(uri);readUri.username=READER;readUri.password=PASS;
    reader=new PostgresEventJournal(readUri.toString(),migrations,{access:"readonly"});
    await reader.connectWithoutMigrate("readonly");
    tmp=await mkdtemp(join(tmpdir(),"g2h-h4-bench-"));
  },60000);
  afterAll(async()=>{
    await reader?.close();
    if(sql){
      for(const table of ["engine_checkpoints","durable_transitions","durable_sequence_metadata"])
        await sql.unsafe(`DROP POLICY IF EXISTS g2h_h4_read ON ${table}`);
      await sql`REVOKE ALL PRIVILEGES ON engine_checkpoints,durable_transitions,durable_sequence_metadata
        FROM g2h_h4_readonly_20261011`;
      await sql`REVOKE ALL PRIVILEGES ON SCHEMA public FROM g2h_h4_readonly_20261011`;
      await sql`DROP ROLE IF EXISTS g2h_h4_readonly_20261011`;
      await sql.end();
    }
    await admin?.close();
    if(tmp)await rm(tmp,{recursive:true,force:true});
  },20000);

  it("H4-PG paced historical 5/s peak and 20/s 4x-peak; two runs each",{
    timeout:150000,
  },async()=>{
    const ack=new GuardianPgFileAckStore(join(tmp,"capacity.guardian-pg-ack.json"));
    const session=await GuardianTrustedPgSession.open({
      source:reader,ack,scope,experimentId:EXPERIMENT,
      authorityId:"g2h-h4-local-readonly",pageSize:64,maxBacklogRows:512,
    });
    let sequence=1;
    const profiles=[{rate:5,durationMs:6000},{rate:5,durationMs:6000},
      {rate:20,durationMs:6000},{rate:20,durationMs:6000}];
    for(const [index,profile] of profiles.entries()){
      const sentTimes=new Map<number,number>();
      const pollTimes:number[]=[],ackTimes:number[]=[],lagRows:number[]=[];
      const cpuStart=process.cpuUsage();
      const heapStart=process.memoryUsage().heapUsed;
      let maxHeap=heapStart;
      const t0=performance.now();
      const startCursor=sequence;
      let finished=false;
      let error:unknown=null;
      const poller=(async()=>{
        while(!finished||session.cursor<sequence){
          try{
            const p0=performance.now();
            const prev=session.cursor;
            const result=await session.poll(Date.now());
            pollTimes.push(performance.now()-p0);
            lagRows.push(result.highWater-result.cursor);
            for(let s=prev+1;s<=result.cursor;s++){
              const began=sentTimes.get(s);
              if(began!==undefined){
                ackTimes.push(performance.now()-began);
                sentTimes.delete(s);
              }
            }
            maxHeap=Math.max(maxHeap,process.memoryUsage().heapUsed);
            await wait(150);
          }catch(e){error=e;break;}
        }
      })();
      const count=Math.round(profile.rate*profile.durationMs/1000);
      try{
        for(let i=0;i<count;i++){
          const scheduled=t0+i*1000/profile.rate;
          await wait(scheduled-performance.now());
          if(error)throw error;
          await admin.commitDurableTransition({
            operationKey:`h4:p${index}:event:${i}`,
            type:"LEDGER_MARK_TO_MARKET",aggregateType:"position",
            aggregateId:"offline-h4-marker",payload:{
              prices:{},markedAt:new Date().toISOString(),
            },
          },sequence);
          sequence++;
          sentTimes.set(sequence,performance.now());
          maxHeap=Math.max(maxHeap,process.memoryUsage().heapUsed);
        }
      }finally{
        finished=true;
        await poller;
      }
      if(error)throw error;
      const elapsedMs=performance.now()-t0;
      const cpu=process.cpuUsage(cpuStart);
      const receipt=await ack.load();
      expect(session.cursor).toBe(sequence);
      expect(receipt?.cursor).toBe(sequence);
      expect(sentTimes.size).toBe(0);
      expect(session.status).toBe("READY");
      expect(session.caughtUp).toBe(true);
      const result={
        profile:`pg-${profile.rate}rows-s-repeat-${index%2+1}`,
        targetRowsPerSecond:profile.rate,rows:sequence-startCursor,
        elapsedMs:round(elapsedMs),
        actualRowsPerSecond:round((sequence-startCursor)*1000/elapsedMs),
        nodeCpuMs:round((cpu.user+cpu.system)/1000),
        nodeCpuUtilizationRatio:round((cpu.user+cpu.system)/1000/elapsedMs),
        maxHeapDeltaMb:round((maxHeap-heapStart)/1048576),
        pollCount:pollTimes.length,pollP95Ms:percentile(pollTimes,.95),
        pollP99Ms:percentile(pollTimes,.99),
        ackLagP95Ms:percentile(ackTimes,.95),
        ackLagP99Ms:percentile(ackTimes,.99),
        maxBacklogRows:Math.max(0,...lagRows),
        cursor:session.cursor,pgRole:scope.role,
        verifiedWithoutWriterPrivilege:true,
      };
      runs.push(sequence);
      console.log("G2H_H4_PG_PROFILE "+JSON.stringify(result));
      expect(result.maxBacklogRows).toBeLessThanOrEqual(512);
    }
  });
});
type BookMarket={marketId:string;upTokenId:string;downTokenId:string;asset:string};
class RecordingTransport extends ScriptedBoundedMarketTransport {
  public tokenSets:string[][]=[];
  override async subscribe(tokens:readonly string[],flag=true,signal?:AbortSignal){
    this.tokenSets.push([...tokens]);
    return super.subscribe(tokens,flag,signal);
  }
}
const marketList:BookMarket[]=Array.from({length:40},(_,i)=>({
  marketId:"h4-market-"+i,upTokenId:"h4-up-"+i,
  downTokenId:"h4-down-"+i,asset:"BTC",
}));
function tickBook(tokenId:string,marketId:string,timestamp:number){
  return {type:"book",payload:{tokenId,market:marketId,timestamp,
    bids:[{price:"0.47",size:"10"}],asks:[{price:"0.48",size:"10"}],
    lastTradePrice:"0.475"}};
}
async function until(predicate:()=>boolean,timeout=5000){
  const start=performance.now();
  while(!predicate()){
    if(performance.now()-start>timeout)throw Error("G2H_H4_BOOK_TIMEOUT");
    await wait(5);
  }
}
describe("H4 paced OFFLINE 40-market / 4-shard normalized book load",()=>{
  it("H4-BOOK 80 full-book/s x 40 markets, two six-second repeats",{
    timeout:45000,
  },async()=>{
    const transports=Array.from({length:4},()=>new RecordingTransport());
    const adapter=new PolymarketMarketDataAdapter(undefined,{transports,stableAfterMs:0});
    const observer=new GuardianStandaloneBookObserver(adapter,marketList);
    const abort=new AbortController();
    const running=observer.run(abort.signal);
    const owner=(token:string)=>transports.find(t=>t.tokenSets.at(-1)?.includes(token));
    try{
      await until(()=>transports.every(t=>t.subscribeCalls===1));
      expect(adapter.marketShardCount()).toBe(4);
      for(const repetition of [1,2]){
        let sent=0,minimumReady=40;
        let maxQueueDepth=0,maxQueueBytes=0,maxAgeMs=0;
        const cpuStart=process.cpuUsage();
        const heapStart=process.memoryUsage().heapUsed;
        let maxHeap=heapStart;
        const ages:number[]=[],perTickReadMs:number[]=[];
        const start=performance.now();
        for(let tick=0;tick<6;tick++){
          await wait(start+tick*1000-performance.now());
          const now=Date.now();
          for(const m of marketList){
            const up=owner(m.upTokenId),down=owner(m.downTokenId);
            if(!up||!down)throw Error("H4_BOOK_SHARD_ASSIGNMENT_MISSING");
            up.push(tickBook(m.upTokenId,m.marketId,now));
            down.push(tickBook(m.downTokenId,m.marketId,now));
            sent+=2;
          }
          await until(()=>adapter.unsynchronizedTokenCount()===0);
          const r0=performance.now();
          await until(()=>marketList.every(m=>{
            const x=observer.observe(m.marketId,{
              nowMs:Date.now(),maxAgeMs:3000,maxSkewMs:1000,
            });
            return x.status==="READY"
              &&Date.parse(x.up.sourceTimestamp)>=now
              &&Date.parse(x.down.sourceTimestamp)>=now;
          }));
          const readAt=Date.now();
          let ready=0;
          for(const m of marketList){
            const x=observer.observe(m.marketId,{
              nowMs:readAt,maxAgeMs:3000,maxSkewMs:1000,
            });
            if(x.status==="READY"){
              ready++;
              const age=readAt-Math.min(Date.parse(x.up.sourceTimestamp),
                Date.parse(x.down.sourceTimestamp));
              ages.push(age);maxAgeMs=Math.max(maxAgeMs,age);
            }
          }
          minimumReady=Math.min(minimumReady,ready);
          perTickReadMs.push(performance.now()-r0);
          const ingress=adapter.ingressSnapshot();
          maxQueueDepth=Math.max(maxQueueDepth,ingress.depth);
          maxQueueBytes=Math.max(maxQueueBytes,ingress.bytes);
          maxHeap=Math.max(maxHeap,process.memoryUsage().heapUsed);
        }
        // Measure the FULL paced six-second interval, not the timestamp
        // of the final tick at t=5s, to avoid overstating 80/s as 96/s.
        await wait(start+6000-performance.now());
        const elapsedMs=performance.now()-start;
        const cpu=process.cpuUsage(cpuStart);
        const stats=adapter.ingressSnapshot();
        const result={
          profile:`book-40markets-4shards-repeat-${repetition}`,
          targetFullBooksPerSecond:80,sentFullBooks:sent,
          elapsedMs:round(elapsedMs),
          actualFullBooksPerSecond:round(sent*1000/elapsedMs),
          nodeCpuMs:round((cpu.user+cpu.system)/1000),
          nodeCpuUtilizationRatio:round((cpu.user+cpu.system)/1000/elapsedMs),
          maxHeapDeltaMb:round((maxHeap-heapStart)/1048576),
          p95SourceAgeMs:percentile(ages,.95),
          p99SourceAgeMs:percentile(ages,.99),
          maxSourceAgeMs:maxAgeMs,
          perTickReadP95Ms:percentile(perTickReadMs,.95),
          minReadyMarkets:minimumReady,
          maxQueueDepth,maxQueueBytes,
          ingressHighWaterEvents:stats.highWaterEvents,
          ingressHighWaterBytes:stats.highWaterBytes,
          ingressGapTotal:stats.gapTotal,
          unsynchronizedTokens:adapter.unsynchronizedTokenCount(),
        };
        console.log("G2H_H4_BOOK_PROFILE "+JSON.stringify(result));
        expect(result.minReadyMarkets).toBe(40);
        expect(result.ingressGapTotal).toBe(0);
        expect(result.unsynchronizedTokens).toBe(0);
      }
    }finally{
      abort.abort("offline");
      await running;
    }
  });
});

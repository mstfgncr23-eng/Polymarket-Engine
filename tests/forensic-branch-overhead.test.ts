import { performance, monitorEventLoopDelay } from "node:perf_hooks";
import { it } from "vitest";
import { startExperiment } from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import { GUARDIAN_V2_CONFIG } from "../packages/core/src/guardian-v2.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { snapshot } from "./fixtures.js";
import { cleanupAllocatedLearningDataDirs, ScriptedMarketAdapter, ScriptedReferenceRegistry, discoveryReport, qualified, paperFill, qualifiedPaperEnv } from "./paper-engine-testkit.js";
type Event={idx:number;intended:number;arrival:number};
const marketId="audit-market", BASE=Date.parse("2026-10-07T17:07:22Z");
const DURATION_MS=Number(process.env.AUDIT_DURATION_MS??"6000");
const WARMUP=Number(process.env.AUDIT_WARMUP??"520");
const rates=(process.env.AUDIT_RATES??"30,60,100,200").split(",").map(Number);
const pause=(ms:number)=>new Promise<void>(r=>setTimeout(r,ms));
function quant(a:number[],p:number){if(!a.length)return 0;const x=[...a].sort((a,b)=>a-b);return +x[Math.floor((x.length-1)*p)].toFixed(3);}
function stat(a:number[]){return {n:a.length,p50:quant(a,.5),p95:quant(a,.95),p99:quant(a,.99),max:quant(a,1),mean:a.length?+(a.reduce((x,y)=>x+y,0)/a.length).toFixed(3):0};}
async function trial(rate:number){
 let clock=BASE, count=0, draining=false, maxPending=0;
 const mkt=new ScriptedMarketAdapter();
 mkt.queue(discoveryReport([qualified(marketId,"BTC",marketId+"-up",marketId+"-down",new Date(BASE+900000).toISOString())]));
 const env=qualifiedPaperEnv({ESTIMATED_INFRA_COST_PER_HOUR_USD:"0",DISCOVERY_INTERVAL_MS:"3600000",REVERSAL_PROTECTION_ENABLED:"true"});
 const engine=new EngineService(env,new MemoryEventJournal(),{error:()=>{}},{market:mkt as never,referenceRegistry:new ScriptedReferenceRegistry() as never,activation:new NoopGrokActivationBridge(),now:()=>new Date(clock),resolutionFetcher:async()=>null});
 const skipOld=process.env.AUDIT_SKIP_OLD_AB==="1";
 const branches=["observeGuardianSnapshot","kickProfitTrailingPair","maybeProfitTrailingPair","scheduleSnapshotReversalProtection","kickReversalProtection","maybeReversalProtection","maybeProtectiveDeRisk","executeRiskReductionArbitration"];
 const counts:Record<string,number>={},syncMillis:Record<string,number>={};
 for(const name of branches){
  const raw=(engine as any)[name];
  if(typeof raw!=="function")continue;
  counts[name]=0;syncMillis[name]=0;
  (engine as any)[name]=function(...args:any[]){
   counts[name]++;
   if(skipOld&&name==="maybeProtectiveDeRisk")return Promise.resolve();
   const before=performance.now();
   const result=raw.apply(this,args);
   syncMillis[name]+=performance.now()-before;
   return result;
  };
 }
 const p=await Promise.resolve(engine.start());
 const risk={...(engine as any).riskLimits,profitTrailingPairSoakEnabled:true,reversalProtectionEnabled:true,reversalProtectionWindowMs:180000,guardianVersion:2,guardianFormatGeneration:3,guardianV2Config:GUARDIAN_V2_CONFIG};
 (engine as any).experiment=startExperiment({durationHours:8,strategyVersion:"v1",riskConfiguration:risk,modelConfiguration:{},startingBalance:"1000",systemVersion:"0.1.0",gitSha:"isolated",now:new Date(BASE),closePolicy:"observe_until_settlement"});
 if(!(engine as any).reversalProtectionEnabled())throw Error("Guardian disabled in test");
 await engine.applyObservedFill(paperFill(marketId,"DOWN","0.81",{quantity:"6.17283951",timestamp:new Date(BASE).toISOString()}),"OPEN_FIRST_LEG");
 const t0=new Date(BASE).toISOString();
 const up=snapshot({marketId,tokenId:marketId+"-up",outcome:"UP",asset:"BTC",bids:[{price:"0.18",quantity:"100"}],asks:[{price:"0.20",quantity:"100"}],receivedAt:t0,sourceTimestamp:t0,sequence:1});
 const down=snapshot({marketId,tokenId:marketId+"-down",outcome:"DOWN",asset:"BTC",bids:[{price:"0.79",quantity:"100"}],asks:[{price:"0.82",quantity:"100"}],receivedAt:t0,sourceTimestamp:t0,sequence:1});
 const feed=async(idx:number)=>{
  clock=BASE+idx*15;
  const ref=idx%2===0?down:up;
  const at=new Date(clock).toISOString();
  await mkt.emitSnapshot({...ref,eventId:"event-"+idx,traceId:"trace-"+idx,hash:"hash-"+idx,sequence:idx+2,sourceTimestamp:at,receivedAt:at});
 };
 try{
  for(let i=0;i<WARMUP;i++)await feed(i); const initialCounts={...counts}; const initialMs={...syncMillis};
  await pause(20);
  const q:Event[]=[];
  const waits:number[]=[],intentWaits:number[]=[],callbacks:number[]=[], timerLateness:number[]=[], guardianQueues:number[]=[];
  let finished=0,generated=0,workerRunning=false,workerError:unknown=null;
  const queueSnapshot=()=>{
   const src=(engine as any).guardianPersistenceQueues as Map<string,any[]>|undefined;
   let n=0;if(src)for(const v of src.values())n+=v.length;
   guardianQueues.push(n);
  };
  const handle=async()=>{
   if(workerRunning)return;
   workerRunning=true;
   try{
    while(q.length){
     const it=q.shift()!;
     waits.push(performance.now()-it.arrival);intentWaits.push(performance.now()-it.intended);
     const begun=performance.now();
     await feed(WARMUP+it.idx);
     callbacks.push(performance.now()-begun);
     finished++;
     if(finished%30===0)queueSnapshot();
    }
   }catch(e){workerError=e;}finally{workerRunning=false;}
  };
  const delay=monitorEventLoopDelay({resolution:2});delay.enable();
  const start=performance.now(),cpuStart=process.cpuUsage(),intervalMs=1000/rate;
  let maxTickLag=0;
  const timer=setInterval(()=>{
    const now=performance.now(),expected=Math.min(Math.ceil(DURATION_MS/intervalMs),Math.floor((now-start)/intervalMs));
    // Catch up for arrivals that would have happened while Node was busy.
    while(generated<expected){
      const intended=start+(generated+1)*intervalMs;
      timerLateness.push(now-intended);
      q.push({idx:generated,intended,arrival:now});
      generated++;
    }
    maxPending=Math.max(maxPending,q.length);
    void handle();
  }, Math.max(1,intervalMs));
  await pause(DURATION_MS);
  clearInterval(timer);
  // Include the last planned arrivals, even if the event loop could not service the timer.
  const deadlineCount=Math.ceil(DURATION_MS/intervalMs);
  while(generated<deadlineCount){
   q.push({idx:generated,intended:start+(generated+1)*intervalMs,arrival:performance.now()});
   generated++;
  }
  maxPending=Math.max(maxPending,q.length);
  void handle();
  const stopAt=performance.now();
  let timedOut=false;
  const maxWait=40000;
  while((workerRunning||q.length>0)&&performance.now()-stopAt<maxWait){if(!workerRunning)void handle();await pause(15);}
  if(workerRunning||q.length>0)timedOut=true;
  await pause(30);
  const elapsed=performance.now()-start;
  const cpu=process.cpuUsage(cpuStart);
  const loop={mean:delay.mean/1e6,p95:delay.percentile(95)/1e6,p99:delay.percentile(99)/1e6,max:delay.max/1e6};
  delay.disable();
  const guard=(engine as any).guardianRuntimeStates?.get(marketId);
  const pos=(engine as any).ledger.positions.get(marketId);
  const line={rate,skipOld,durationMs:DURATION_MS,warmup:WARMUP,target:deadlineCount,generated,finished,timedOut,error:String(workerError??""),
   avgSubmittedPerS: +(generated/(DURATION_MS/1000)).toFixed(1),avgCompletedPerS:+(finished/(elapsed/1000)).toFixed(1),
   sendPhaseMs:+(stopAt-start).toFixed(0),drainMs:+(elapsed-DURATION_MS).toFixed(0),maxPending,
   dequeueAgeMs:stat(waits),intentToDequeueMs:stat(intentWaits),callbackMs:stat(callbacks),timerLateMs:stat(timerLateness),
   cpuUserMs:+(cpu.user/1000).toFixed(1),cpuSystemMs:+(cpu.system/1000).toFixed(1),cpuPerSnapshotMs:+((cpu.user+cpu.system)/1000/Math.max(1,finished)).toFixed(3),
   eventLoopMs:Object.fromEntries(Object.entries(loop).map(([k,v])=>[k,+v.toFixed(2)])),
   branchCalls:Object.fromEntries(branches.filter(k=>k in counts).map(k=>[k,counts[k]-initialCounts[k]])),branchSyncMs:Object.fromEntries(branches.filter(k=>k in syncMillis).map(k=>[k,+(syncMillis[k]-initialMs[k]).toFixed(1)])),guardianRevision:guard?.revision??null,guardianHistory:guard?.history?.length??null,persistQueueHighWater:Math.max(0,...guardianQueues),persistWorkers:(engine as any).guardianPersistenceWorkers?.size??null,unpaired:pos?.unpairedQuantity??null};
  console.log("PACE_RESULT "+JSON.stringify(line));
  if(timedOut||workerError||finished!==deadlineCount)throw Error("TEST_INCOMPLETE "+JSON.stringify({rate,finished,deadlineCount,timedOut,workerError}));
 } finally{try{await engine.stop()}catch(e){console.log("STOP_FAILURE",String(e).slice(0,180));}cleanupAllocatedLearningDataDirs();}
}
it("fixed-rate Guardian + risk path consumption",async()=>{for(const r of rates)await trial(r);},300000);



import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { performance, monitorEventLoopDelay } from "node:perf_hooks";
import { fileURLToPath } from "node:url";
import { resolve, dirname } from "node:path";
import {
  BoundedClobMarketTransport, PolymarketMarketDataAdapter,
  ReferencePriceFeedAdapter,
} from "@pm/market-data";

type MarketPair={marketId:string; tokenIds:[string,string]};
type PublicEvent={offsetMs:number; channel:string; asset:string; feed?:string; data?:string; markets?:MarketPair[]};
const TESTROOT=resolve(dirname(fileURLToPath(import.meta.url)),"..");
const INPUT=resolve(TESTROOT,"research/reverse_regression/fixtures/public_market_capture_four_markets_20261009.jsonl");
const EXPECTED_SHA="7ea27f0c585d2da70ee7f9a64b2203c7b5df3e741b905d1faf4818766466ed19";
const speed=Number(process.env.PM_REPLAY_SPEED || "1");
if(!Number.isFinite(speed)||speed<0.5||speed>15)throw Error("INVALID_REPLAY_SPEED");

class PlaybackSocket extends EventTarget {
  static created:PlaybackSocket[]=[];
  static reset(){PlaybackSocket.created=[];}
  readyState=0;
  readonly subscriptions:string[]=[];
  constructor(_url:string){
    super();
    PlaybackSocket.created.push(this);
    queueMicrotask(()=>this.open());
  }
  private open(){
    if(this.readyState!==0)return;
    this.readyState=1;
    this.dispatchEvent(new Event("open"));
  }
  send(raw:string){
    if(raw==="PING")return;
    const parsed=JSON.parse(raw) as {assets_ids?:string[]};
    if(parsed.assets_ids)this.subscriptions.push(...parsed.assets_ids);
  }
  play(raw:string){
    if(this.readyState===1)
      this.dispatchEvent(new MessageEvent("message",{data:raw}));
  }
  close(code=1000,reason="shutdown"){
    if(this.readyState===3)return;
    this.readyState=3;
    const event=Object.assign(new Event("close"),{code,reason,wasClean:code===1000});
    queueMicrotask(()=>this.dispatchEvent(event));
  }
}
const delay=(ms:number)=>new Promise<void>(r=>setTimeout(r,ms));
async function waitUntil(fn:()=>boolean,timeoutMs=6000){
 const start=performance.now();
 while(!fn()){
  if(performance.now()-start>timeoutMs)throw Error("REPLAY_WAIT_TIMEOUT");
  await delay(2);
 }
}
function events(){
 const raw=readFileSync(INPUT);
 expect(createHash("sha256").update(raw).digest("hex")).toBe(EXPECTED_SHA);
 const parsed=raw.toString("utf8").trim().split("\n").map(x=>JSON.parse(x) as PublicEvent);
 expect(parsed.every(x=>x.offsetMs>=0 && Number.isSafeInteger(x.offsetMs))).toBe(true);
 expect(parsed.some(x=>x.channel==="clob")).toBe(true);
 expect(parsed.some(x=>x.channel==="reference")).toBe(true);
 return parsed;
}
function filteredClobMessage(raw:string,subscriptions:ReadonlySet<string>){
 const items=JSON.parse(raw) as unknown;
 const list=Array.isArray(items)?items:[items];
 const filtered:unknown[]=[];
 for(const item of list){
  if(!item || typeof item!=="object")continue;
  const row=item as Record<string,unknown>;
  const token=String(row.asset_id??"");
  const pc=Array.isArray(row.price_changes)?row.price_changes.filter(x=>
     typeof x==="object" && x!==null && subscriptions.has(String((x as Record<string,unknown>).asset_id??""))):[];
  if(pc.length){
   filtered.push({...row,price_changes:pc});
  }else if(subscriptions.has(token)){
   filtered.push(row);
  }
 }
 return filtered.length ? JSON.stringify(filtered):null;
}

async function playback():Promise<Record<string,unknown>>{
 const all=events();
 const marketPairs=all.find(x=>x.channel==="meta")?.markets;
 expect(marketPairs?.length).toBe(4);
 const pairs=marketPairs!;
 const marketTokenIds=pairs.flatMap(x=>x.tokenIds);
 const activeTokens=new Set(marketTokenIds);
 const refAssets=["BTC","ETH","SOL","XRP","DOGE","BNB","HYPE","ZEC"];
 const abort=new AbortController();
 PlaybackSocket.reset();
 const refSockets=new Map<string,PlaybackSocket>();
 const refTicks:Record<string,number>={};
 const refErrors:string[]=[];
 const refConnectionEvents:string[]=[];
 const tasks:Promise<unknown>[]=[];
 const clobErrors:string[]=[];
 const clobSnapshots: string[]=[];
 const seenCLOB=new Set<string>();
 const clobConnections:string[]=[];
 const h=monitorEventLoopDelay({resolution:10});
 const start=performance.now();
 h.enable();
 let maxSnapshotInFlight=0;
 let inFlight=0;
 try{
  for(const asset of refAssets){
    const adapter=new ReferencePriceFeedAdapter({
      asset,symbol:asset+"USDT",url:"wss://replay.invalid/"+asset,
      staleTimeoutMs:60000,reconnect:{initialDelayMs:20000,maxDelayMs:20000,jitterRatio:0,multiplier:1},
      factory:()=>{
        const socket=new PlaybackSocket("reference/"+asset);
        refSockets.set(asset,socket);
        return socket;
      },
    });
    tasks.push(adapter.run({
      onTick: async tick=>{refTicks[asset]=(refTicks[asset]??0)+1; await Promise.resolve();},
      onConnection: evt=>refConnectionEvents.push(asset+":"+evt.state),
      onError: err=>refErrors.push(asset+":"+err.message),
    },abort.signal));
  }
  const transports=Array.from({length:4},()=>new BoundedClobMarketTransport({
    webSocket:PlaybackSocket as unknown as typeof WebSocket,
    staleMs:60000,pingIntervalMs:60000,connectTimeoutMs:6000,
  }));
  const market=new PolymarketMarketDataAdapter(undefined,{transports,stableAfterMs:0});
  for(const pair of pairs){
    market.registerManualToken(pair.tokenIds[0],pair.marketId,"UP","BTC");
    market.registerManualToken(pair.tokenIds[1],pair.marketId,"DOWN","BTC");
  }
  market.setTokenIds(marketTokenIds);
  tasks.push(market.run(marketTokenIds,{
    onSnapshot:async snapshot=>{
      inFlight++;maxSnapshotInFlight=Math.max(maxSnapshotInFlight,inFlight);
      clobSnapshots.push(snapshot.tokenId);
      seenCLOB.add(snapshot.marketId);
      await delay(1);
      inFlight--;
    },
    onConnection:evt=>clobConnections.push(evt.state),
    onError:err=>clobErrors.push(err.message),
  },abort.signal));
  await waitUntil(()=>refSockets.size===8 && PlaybackSocket.created.filter(x=>x.subscriptions.length>0).length===4);
  const streams=PlaybackSocket.created.filter(x=>x.subscriptions.length>0);
  const recordStart=all[0]!.offsetMs;
  let previous=recordStart;
  let injectedReference=0,injectedClobFrames=0,clobRecords=0;
  for(const row of all){
    const elapsed=Math.max(0,(row.offsetMs-previous)/speed);
    if(elapsed>0)await delay(elapsed);
    previous=row.offsetMs;
    if(row.channel==="reference" && row.data){
      const socket=refSockets.get(row.asset);
      expect(socket).toBeDefined();
      socket!.play(row.data);
      injectedReference++;
    }else if(row.channel==="clob" && row.data){
      clobRecords++;
      for(const socket of streams){
        const message=filteredClobMessage(row.data,new Set(socket.subscriptions));
        if(message){socket.play(message);injectedClobFrames++;}
      }
    }
  }
  await waitUntil(()=>Object.values(refTicks).reduce((a,b)=>a+b,0)===injectedReference,6000);
  await waitUntil(()=>clobSnapshots.length>=8,6000);
  // Permit the older single global lane to drain completely before measuring.
  // Otherwise the apparent A/B difference is just truncated async work.
  let stable=0;
  let previousCallbacks=-1;
  let previousDepth=-1;
  const drainedStart=performance.now();
  while(stable<6 && performance.now()-drainedStart<5000){
    await delay(100);
    const depth=transports.reduce((n,t)=>n+t.queue.depth,0);
    if(depth===0 && inFlight===0 && previousCallbacks===clobSnapshots.length && previousDepth===depth){
      stable++;
    }else stable=0;
    previousCallbacks=clobSnapshots.length;
    previousDepth=depth;
  }
  expect(stable).toBe(6);
  const result={
    stage:process.env.PM_FORENSIC_STAGE||"unset",
    dataset_sha256:EXPECTED_SHA, capturedPublicEvents:all.length,
    injectedReference,injectedClobRecords:clobRecords,injectedClobFrames,
    acceptedReference:Object.values(refTicks).reduce((a,b)=>a+b,0),
    refTicks, refErrors, clobErrors, clobSnapshots:clobSnapshots.length,
    clobMarketsReached:seenCLOB.size,
    marketConnections:clobConnections.filter(x=>x==="CONNECTED").length,
    referenceConnections:refConnectionEvents.filter(x=>x.endsWith("CONNECTED")).length,
    maxSnapshotInFlight,drainedClobQueueDepth:transports.reduce((n,t)=>n+t.queue.depth,0),replaySpeed:speed,eventLoopP99Ms:Math.round(h.percentile(99)/1e6*100)/100,
    elapsedMs:Math.round(performance.now()-start),
  };
  console.log("PUBLIC_REPLAY_RESULT "+JSON.stringify(result));
  expect(result.acceptedReference).toBe(injectedReference);
  expect(refErrors).toEqual([]);
  expect(clobErrors).toEqual([]);
  // This public capture has eight full books and 124 token-specific price updates.
  // A complete drain must deliver every callback, not merely a subset.
  expect(clobSnapshots.length).toBe(132);
  expect(seenCLOB.size).toBe(4);
  expect(injectedClobFrames).toBe(66);
  expect(refConnectionEvents.filter(x=>x.endsWith("CONNECTED")).length).toBe(8);
  const stage=process.env.PM_FORENSIC_STAGE;
  if(stage) expect(maxSnapshotInFlight).toBe(stage==="00_safe" ? 1 : 4);
  return result;
 }finally{
  abort.abort("forensic_replay_end");
  await Promise.allSettled(tasks);
  h.disable();
 }
}

describe("same public market/reference frames through staged real adapters",()=>{
 it.skipIf(process.env.PM_REPLAY_ENABLE!=="1")("replays identical byte-verified traffic with no implicit network access",async()=>{
  await playback();
 },35000);
});

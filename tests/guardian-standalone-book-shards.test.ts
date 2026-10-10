import {describe,expect,it} from "vitest";
import {PolymarketMarketDataAdapter,ScriptedBoundedMarketTransport} from "@pm/market-data";
import {GuardianStandaloneBookObserver} from "../apps/engine/src/guardian-standalone-book-observer.js";

const markets=Array.from({length:40},(_,n)=>({
  marketId:"sharded-paper-"+n,
  upTokenId:"sharded-up-"+n,
  downTokenId:"sharded-down-"+n,
  asset:"BTC",
}));
const bounds=()=>({nowMs:Date.now(),maxAgeMs:10_000,maxSkewMs:3_000});
async function until(ok:()=>boolean,ms=7000){
  const start=Date.now();
  while(!ok()){
    if(Date.now()-start>ms)throw Error("H3_SHARD_TEST_TIMEOUT");
    await new Promise<void>(r=>setTimeout(r,5));
  }
}
class RecordingTransport extends ScriptedBoundedMarketTransport {
  tokenSets:string[][]=[];
  override async subscribe(tokenIds:readonly string[],customFeatureEnabled=true,signal?:AbortSignal){
    this.tokenSets.push([...tokenIds]);
    return super.subscribe(tokenIds,customFeatureEnabled,signal);
  }
}
function book(tokenId:string,marketId:string){
  return {type:"book",payload:{tokenId,market:marketId,
    timestamp:Date.now(),bids:[{price:"0.40",size:"10"}],
    asks:[{price:"0.41",size:"10"}],lastTradePrice:"0.405"}};
}
function start(count=40){
  const transports=Array.from({length:4},()=>new RecordingTransport());
  const adapter=new PolymarketMarketDataAdapter(undefined,{transports,stableAfterMs:0});
  const observer=new GuardianStandaloneBookObserver(adapter,markets.slice(0,count));
  const abort=new AbortController();
  const running=observer.run(abort.signal);
  const shardFor=(tokenId:string)=>transports.findIndex(t=>t.tokenSets.at(-1)?.includes(tokenId));
  const feed=(index:number,outcome:"UP"|"DOWN")=>{
    const m=markets[index]!;
    const tokenId=outcome==="UP"?m.upTokenId:m.downTokenId;
    const shard=shardFor(tokenId);
    if(shard<0)throw Error("H3_TOKEN_UNASSIGNED:"+tokenId);
    transports[shard]!.push(book(tokenId,m.marketId));
  };
  return {transports,adapter,observer,abort,running,shardFor,feed};
}
const ready=(o:GuardianStandaloneBookObserver,i:number)=>
  o.observe(markets[i]!.marketId,bounds()).status==="READY";

describe("H3 four-shard 40-market actual scripted bounded adapter",()=>{
  it("H3-4S-06 one shard slow to connect cannot blind three already healthy shards",async()=>{
    let startFourth:()=>void=()=>undefined;
    const delayed=new Promise<void>(resolve=>{startFourth=resolve;});
    class LateTransport extends RecordingTransport {
      override async subscribe(tokenIds:readonly string[],customFeatureEnabled=true,
        signal?:AbortSignal){
        await delayed;
        return super.subscribe(tokenIds,customFeatureEnabled,signal);
      }
    }
    const transports:[RecordingTransport,RecordingTransport,RecordingTransport,RecordingTransport]=[
      new RecordingTransport(),new RecordingTransport(),new RecordingTransport(),new LateTransport(),
    ];
    const adapter=new PolymarketMarketDataAdapter(undefined,{transports,stableAfterMs:0});
    const observer=new GuardianStandaloneBookObserver(adapter,markets);
    const abort=new AbortController();
    const running=observer.run(abort.signal);
    try{
      await until(()=>transports.slice(0,3).every(t=>t.subscribeCalls===1));
      expect(transports[3]!.subscribeCalls).toBe(0);
      const active=markets.map((m,i)=>
        transports.slice(0,3).some(t=>t.tokenSets[0]?.includes(m.upTokenId))?i:-1)
        .filter(i=>i>=0);
      expect(active).toHaveLength(30);
      for(const i of active){
        const m=markets[i]!;
        const t=transports.find(t=>t.tokenSets[0]?.includes(m.upTokenId))!;
        t.push(book(m.upTokenId,m.marketId));
        t.push(book(m.downTokenId,m.marketId));
      }
      await until(()=>active.every(i=>ready(observer,i)));
      expect(markets.map((_,i)=>i).filter(i=>!active.includes(i))
        .every(i=>!ready(observer,i))).toBe(true);
      startFourth();
      await until(()=>transports.every(t=>t.subscribeCalls===1));
      for(const [i,m] of markets.entries()){
        if(active.includes(i))continue;
        transports[3]!.push(book(m.upTokenId,m.marketId));
        transports[3]!.push(book(m.downTokenId,m.marketId));
      }
      await until(()=>markets.every((_,i)=>ready(observer,i)));
    }finally{
      startFourth();
      abort.abort("offline");
      await running;
    }
  });
  it("H3-4S-01 40/40 independent synchronized pairs, all 4 owned transports",async()=>{
    const x=start();
    try{
      await until(()=>x.transports.every(t=>t.subscribeCalls===1));
      expect(x.adapter.marketShardCount()).toBe(4);
      expect(x.adapter.shardSnapshots().map(s=>s.tokenCount)).toEqual([20,20,20,20]);
      expect(x.transports.every(t=>t.tokenSets[0]?.length===20)).toBe(true);
      for(let i=0;i<40;i++){x.feed(i,"UP");x.feed(i,"DOWN");}
      await until(()=>markets.every((_,i)=>ready(x.observer,i)));
      expect(x.adapter.unsynchronizedTokenCount()).toBe(0);
    }finally{x.abort.abort("offline");await x.running;}
    expect(markets.every((_,i)=>!ready(x.observer,i))).toBe(true);
  });
  it("H3-4S-02 one shard loses socket: healthy 30 remain READY; failed 10 wait for fresh pair",async()=>{
    const x=start();
    try{
      await until(()=>x.transports.every(t=>t.subscribeCalls===1));
      for(let i=0;i<40;i++){x.feed(i,"UP");x.feed(i,"DOWN");}
      await until(()=>markets.every((_,i)=>ready(x.observer,i)));
      const affected=markets.map((m,i)=>x.shardFor(m.upTokenId)===0?i:-1)
        .filter(i=>i>=0);
      const healthy=markets.map((_,i)=>i).filter(i=>!affected.includes(i));
      expect(affected).toHaveLength(10);expect(healthy).toHaveLength(30);
      const healthyStreams=x.transports.slice(1).map(t=>t.current());
      x.transports[0]!.current().endRemote();
      await until(()=>affected.every(i=>!ready(x.observer,i)));
      expect(healthy.every(i=>ready(x.observer,i))).toBe(true);
      await until(()=>x.transports[0]!.subscribeCalls===2);
      for(const i of affected)x.feed(i,"UP");
      await until(()=>affected.every(i=>x.adapter.isTokenSynchronized(markets[i]!.upTokenId)));
      expect(affected.every(i=>!ready(x.observer,i))).toBe(true);
      expect(healthy.every(i=>ready(x.observer,i))).toBe(true);
      for(const i of affected)x.feed(i,"DOWN");
      await until(()=>affected.every(i=>ready(x.observer,i)));
      expect(healthy.every(i=>ready(x.observer,i))).toBe(true);
      expect(x.transports.slice(1).every((t,i)=>t.current()===healthyStreams[i])).toBe(true);
    }finally{x.abort.abort("offline");await x.running;}
  });
  it("H3-4S-04 silent adapter sync loss immediately blocks affected pair at observe",async()=>{
    const x=start(8);
    try{
      await until(()=>x.transports.every(t=>t.subscribeCalls===1));
      for(let i=0;i<8;i++){x.feed(i,"UP");x.feed(i,"DOWN");}
      await until(()=>markets.slice(0,8).every((_,i)=>ready(x.observer,i)));
      x.adapter.markTokenGappedForTest(markets[0]!.upTokenId);
      expect(ready(x.observer,0)).toBe(false);
      expect(markets.slice(1,8).every((_,i)=>ready(x.observer,i+1))).toBe(true);
    }finally{x.abort.abort("offline");await x.running;}
  });
  it("H3-4S-05 silent token epoch rotation never reuses cached READY pair",async()=>{
    const x=start(8);
    try{
      await until(()=>x.transports.every(t=>t.subscribeCalls===1));
      for(let i=0;i<8;i++){x.feed(i,"UP");x.feed(i,"DOWN");}
      await until(()=>markets.slice(0,8).every((_,i)=>ready(x.observer,i)));
      const m=markets[2]!;
      x.adapter.bumpTokenEpochForTest(m.upTokenId);
      expect(ready(x.observer,2)).toBe(false);
      expect(markets.slice(0,8).every((_,i)=>i===2||ready(x.observer,i))).toBe(true);
      x.feed(2,"UP");
      await until(()=>x.adapter.isTokenSynchronized(m.upTokenId));
      expect(ready(x.observer,2)).toBe(false);
      x.feed(2,"DOWN");
      await until(()=>ready(x.observer,2));
    }finally{x.abort.abort("offline");await x.running;}
  });
  it("H3-4S-03 bad shard cannot retain previously READY books after disconnect",async()=>{
    const x=start(8);
    try{
      await until(()=>x.transports.every(t=>t.subscribeCalls===1));
      for(let i=0;i<8;i++){x.feed(i,"UP");x.feed(i,"DOWN");}
      await until(()=>markets.slice(0,8).every((_,i)=>ready(x.observer,i)));
      const affected=markets.slice(0,8).map((m,i)=>x.shardFor(m.upTokenId)===2?i:-1).filter(i=>i>=0);
      expect(affected.length).toBeGreaterThan(0);
      x.transports[2]!.current().endRemote();
      await until(()=>affected.every(i=>!ready(x.observer,i)));
      for(const i of affected)expect(x.observer.observe(markets[i]!.marketId,bounds()).status)
        .toBe("BLOCKED");
    }finally{x.abort.abort("offline");await x.running;}
  });
});

import {describe,expect,it} from "vitest";
import {PolymarketMarketDataAdapter,ScriptedBoundedMarketTransport} from "@pm/market-data";
import {GuardianStandaloneBookObserver} from "../apps/engine/src/guardian-standalone-book-observer.js";

const markets=Array.from({length:8},(_,n)=>({
  marketId:"independent-market-"+n,
  upTokenId:"independent-up-"+n,
  downTokenId:"independent-down-"+n,
  asset:"BTC",
}));
const bounds=()=>({nowMs:Date.now(),maxAgeMs:10_000,maxSkewMs:3_000});
const rawBook=(i:number,outcome:"UP"|"DOWN")=>({
  type:"book",
  payload:{
    tokenId:outcome==="UP"?markets[i]!.upTokenId:markets[i]!.downTokenId,
    market:markets[i]!.marketId,timestamp:Date.now(),
    bids:[{price:"0.47",size:"10"}],asks:[{price:"0.48",size:"10"}],
    lastTradePrice:"0.475",
  },
});
async function until(predicate:()=>boolean,maxMs=4500){
  const start=Date.now();
  while(!predicate()){
    if(Date.now()-start>maxMs)throw Error("OBSERVER_FIXTURE_TIMEOUT");
    await new Promise<void>(r=>setTimeout(r,5));
  }
}
function make(count=8){
  const transport=new ScriptedBoundedMarketTransport();
  const adapter=new PolymarketMarketDataAdapter(undefined,{transport,stableAfterMs:0});
  const observer=new GuardianStandaloneBookObserver(adapter,markets.slice(0,count));
  const abort=new AbortController();
  const running=observer.run(abort.signal);
  return {transport,adapter,observer,abort,running};
}
describe("H3 true standalone Guardian normalized WS observer, offline transport only",()=>{
  it("H3-01 8/8 full paired normalized books without Engine callbacks",async()=>{
    const x=make();
    try{
      await until(()=>x.transport.subscribeCalls>=1);
      expect(x.observer.marketCount).toBe(8);
      expect(x.observer.isRunning).toBe(true);
      for(let i=0;i<8;i++){
        x.transport.push(rawBook(i,"UP"));
        x.transport.push(rawBook(i,"DOWN"));
      }
      await until(()=>markets.every(m=>x.observer.observe(m.marketId,bounds()).status==="READY"));
      expect(x.adapter.unsynchronizedTokenCount()).toBe(0);
      expect(x.observer.ingressStats).toBeDefined();
    }finally{x.abort.abort("fixture");await x.running;}
    expect(x.observer.isRunning).toBe(false);
    expect(markets.every(m=>x.observer.observe(m.marketId,bounds()).status==="BLOCKED")).toBe(true);
  });
  it("H3-02 source reconnect invalidates both; 7/8 cannot fake missing eighth DOWN",async()=>{
    const x=make();
    try{
      await until(()=>x.transport.subscribeCalls>=1);
      for(let i=0;i<8;i++){
        x.transport.push(rawBook(i,"UP"));x.transport.push(rawBook(i,"DOWN"));
      }
      await until(()=>markets.every(m=>x.observer.observe(m.marketId,bounds()).status==="READY"));
      x.transport.current().endRemote();
      await until(()=>markets.every(m=>x.observer.observe(m.marketId,bounds()).status==="BLOCKED"));
      await until(()=>x.transport.subscribeCalls>=2);
      for(let i=0;i<8;i++){
        x.transport.push(rawBook(i,"UP"));
        if(i<7)x.transport.push(rawBook(i,"DOWN"));
      }
      await until(()=>markets.slice(0,7).every(
        m=>x.observer.observe(m.marketId,bounds()).status==="READY"));
      expect(x.observer.observe(markets[7]!.marketId,bounds()).status).toBe("BLOCKED");
      x.transport.push(rawBook(7,"DOWN"));
      await until(()=>x.observer.observe(markets[7]!.marketId,bounds()).status==="READY");
    }finally{x.abort.abort("fixture");await x.running;}
  });
  it("H3-03 no second concurrent standalone observer loop or duplicate market",async()=>{
    expect(()=>new GuardianStandaloneBookObserver(
      new PolymarketMarketDataAdapter(undefined,{transport:new ScriptedBoundedMarketTransport()}),
      [markets[0]!,markets[0]!],
    )).toThrow("DUPLICATE_MARKET_OR_TOKEN");
    const x=make(1);
    try{
      await until(()=>x.transport.subscribeCalls>=1);
      await expect(x.observer.run(x.abort.signal)).rejects.toThrow("SECOND_RUN");
    }finally{x.abort.abort("fixture");await x.running;}
  });
  it("H3-05 token epoch rotation discards old paired book until BOTH reappear",async()=>{
    const x=make(1);
    try{
      await until(()=>x.transport.subscribeCalls>=1);
      x.transport.push(rawBook(0,"UP"));
      x.transport.push(rawBook(0,"DOWN"));
      await until(()=>x.observer.observe(markets[0]!.marketId,bounds()).status==="READY");
      x.adapter.bumpTokenEpochForTest(markets[0]!.upTokenId);
      x.transport.push(rawBook(0,"UP"));
      await until(()=>x.adapter.isTokenSynchronized(markets[0]!.upTokenId));
      expect(x.observer.observe(markets[0]!.marketId,bounds()).status).toBe("BLOCKED");
      x.transport.push(rawBook(0,"DOWN"));
      await until(()=>x.observer.observe(markets[0]!.marketId,bounds()).status==="READY");
    }finally{x.abort.abort("fixture");await x.running;}
  });
  it("H3-04 bad/stale one side never produces READY",async()=>{
    const x=make(1);
    try{
      await until(()=>x.transport.subscribeCalls>=1);
      x.transport.push(rawBook(0,"UP"));
      await until(()=>x.adapter.isTokenSynchronized(markets[0]!.upTokenId));
      expect(x.observer.observe(markets[0]!.marketId,bounds()).status).toBe("BLOCKED");
      x.transport.push(rawBook(0,"DOWN"));
      await until(()=>x.observer.observe(markets[0]!.marketId,bounds()).status==="READY");
      expect(x.observer.observe(markets[0]!.marketId,{
        nowMs:Date.now()+25_000,maxAgeMs:1000,maxSkewMs:100,
      }).status).toBe("BLOCKED");
    }finally{x.abort.abort("fixture");await x.running;}
  });
});

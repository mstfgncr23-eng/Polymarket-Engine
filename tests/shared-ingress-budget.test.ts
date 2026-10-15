import {describe,expect,it} from "vitest";
import {BoundedIngressQueue,SharedIngressBudget,PolymarketMarketDataAdapter,ScriptedBoundedMarketTransport,MARKET_INGRESS_BOUNDS} from "@pm/market-data";

function queues() {
  const budget=new SharedIngressBudget(4,16,256,1,16);
  const queues=Array.from({length:4},()=>new BoundedIngressQueue(4,64));
  queues.forEach(q=>q.attachSharedBudget(budget));return {budget,queues};
}
describe("fixed aggregate ingress quota",()=>{
  it("production default keeps exactly the original four-queue aggregate limits",()=>{
    const adapter=new PolymarketMarketDataAdapter();
    expect(adapter.marketShardCount()).toBe(4);
    expect(adapter.ingressSnapshot()).toMatchObject({depth:0,bytes:0,
      poolMaxEvents:4*MARKET_INGRESS_BOUNDS.maxEvents,poolMaxBytes:4*MARKET_INGRESS_BOUNDS.maxBytes});
  });
  it("borrows idle quota without increasing total capacity or taking another shard's reserved allowance",()=>{
    const {budget,queues:q}=queues();
    for(let i=0;i<13;i++)expect(q[0]!.tryEnqueueWeighted(i,8).ok).toBe(true);
    expect(q[0]!.tryEnqueueWeighted("cannot-steal-reserves",8)).toEqual({ok:false,reason:"count_limit"});
    for(const other of q.slice(1))expect(other.tryEnqueueWeighted("healthy",8).ok).toBe(true);
    expect(budget.snapshot()).toMatchObject({depth:16,poolMaxEvents:16,poolMaxBytes:256});
    expect(q[0]!.snapshot().borrowedEvents).toBe(9);
    for(const queue of q)queue.clear();
    expect(budget.snapshot()).toMatchObject({depth:0,bytes:0,highWaterEvents:16});
    expect(q[1]!.tryEnqueueWeighted("after-clear",8).ok).toBe(true);
  });
  it("keeps the original byte cap and releases borrowed bytes on dequeue and clear",()=>{
    const {budget,queues:q}=queues();
    expect(q[0]!.tryEnqueueWeighted("hot",208).ok).toBe(true);
    expect(q[0]!.tryEnqueueWeighted("too-much",1)).toEqual({ok:false,reason:"byte_limit"});
    for(const other of q.slice(1))expect(other.tryEnqueueWeighted("healthy",16).ok).toBe(true);
    expect(budget.snapshot()).toMatchObject({bytes:256,highWaterBytes:256});
    q[0]!.dequeue();expect(budget.snapshot().bytes).toBe(48);
    q.forEach(queue=>queue.clear());expect(budget.snapshot()).toMatchObject({bytes:0,depth:0});
  });
  it("conserves both credits through seeded churn and refuses a fifth queue",()=>{
    const {budget,queues:q}=queues();let seed=19;
    for(let i=0;i<10000;i++){
      seed=(seed*1664525+1013904223)>>>0;
      const selected=q[seed%4]!;
      if(seed%7===0)selected.clear();else if(seed%3===0)selected.dequeue();else selected.tryEnqueueWeighted({i},1+(seed%40));
      const s=budget.snapshot();expect(s.depth).toBe(q.reduce((n,x)=>n+x.depth,0));expect(s.bytes).toBe(q.reduce((n,x)=>n+x.queuedBytes,0));
      expect(s.depth).toBeLessThanOrEqual(16);expect(s.bytes).toBeLessThanOrEqual(256);expect(s.depth).toBeGreaterThanOrEqual(0);
    }
    expect(()=>new BoundedIngressQueue().attachSharedBudget(budget)).toThrow("REGISTRATION");
  });
});

async function waitFor(predicate:()=>boolean,ms=5000){const end=Date.now()+ms;while(!predicate()){if(Date.now()>end)throw Error("waitFor timeout");await new Promise(r=>setTimeout(r,2));}}
const book=(market:number,time:number,quantity="1")=>({type:"book",payload:{tokenId:`pooled-${market}-up`,market:`pooled-${market}`,timestamp:time,
  bids:[{price:"0.69",size:quantity}],asks:[{price:"0.70",size:"100"}],hash:String(time)}});
const change=(time:number)=>({type:"price_change",payload:{timestamp:time,priceChanges:[{tokenId:"pooled-0-up",side:"BUY",price:"0.69",size:String(time),hash:String(time)}]}});

it("production-shaped four-shard skew preserves all burst observations, reserves healthy shards and still gaps at true exhaustion",async()=>{
  const transports=Array.from({length:4},()=>new ScriptedBoundedMarketTransport());
  const adapter=new PolymarketMarketDataAdapter(undefined,{transports,sharedIngressBudget:true,stableAfterMs:0});
  const tokens:string[]=[];
  for(let i=0;i<40;i++)for(const outcome of ["UP","DOWN"] as const){const token=`pooled-${i}-${outcome.toLowerCase()}`;tokens.push(token);adapter.registerManualToken(token,`pooled-${i}`,outcome,"XRP");}
  adapter.setTokenIds(tokens);const abort=new AbortController();const seen:string[]=[];let healthy=0;
  const run=adapter.run(tokens,{onSnapshot:s=>{if(s.tokenId==="pooled-0-up")seen.push(s.bids[0]!.quantity);else healthy++;},onConnection:()=>undefined,onError:()=>undefined},abort.signal);
  try{
    await waitFor(()=>transports.every(t=>t.subscribeCalls===1));
    transports.forEach((t,i)=>t.push(book(i,1000)));
    for(let i=1001;i<=10000;i++)expect(transports[0]!.push(change(i)).ok).toBe(true);
    expect(adapter.ingressSnapshot()).toMatchObject({poolMaxEvents:4*MARKET_INGRESS_BOUNDS.maxEvents,poolMaxBytes:4*MARKET_INGRESS_BOUNDS.maxBytes});
    await waitFor(()=>seen.length===9001&&healthy===3);
    expect(seen).toEqual(["1",...Array.from({length:9000},(_,i)=>String(1001+i))]);
    expect(adapter.shardSnapshots()[0]!.ingress.highWaterEvents).toBeGreaterThan(MARKET_INGRESS_BOUNDS.maxEvents);
    expect(adapter.ingressSnapshot()).toMatchObject({depth:0,bytes:0,gapTotal:0});
    let overflow=false;
    for(let i=20000;i<50000;i++)if(!transports[0]!.push(change(i)).ok){overflow=true;break;}
    expect(overflow).toBe(true);
    expect(adapter.ingressSnapshot().depth).toBeLessThanOrEqual(4*MARKET_INGRESS_BOUNDS.maxEvents);
    expect(adapter.ingressSnapshot().bytes).toBeLessThanOrEqual(4*MARKET_INGRESS_BOUNDS.maxBytes);
    transports[1]!.push(book(1,60000,"777"));await waitFor(()=>healthy===4);
    expect(adapter.isTokenSynchronized("pooled-1-up")).toBe(true);
    await waitFor(()=>transports[0]!.subscribeCalls===2);
    expect(adapter.isTokenSynchronized("pooled-0-up")).toBe(false);
    transports[0]!.push(book(0,60000,"888"));await waitFor(()=>seen.at(-1)==="888");
    expect(adapter.isTokenSynchronized("pooled-0-up")).toBe(true);
    expect(adapter.ingressSnapshot().gapTotal).toBe(1);
    expect(transports.every(t=>t.maxConcurrent===1)).toBe(true);
  }finally{abort.abort();await run;transports.forEach(t=>t.queue.clear());}
},15000);

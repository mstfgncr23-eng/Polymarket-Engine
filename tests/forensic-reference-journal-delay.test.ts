import { it,expect } from "vitest";
import { performance } from "node:perf_hooks";
import { ReferencePriceFeedAdapter } from "../packages/market-data/src/reference-feed.js";
const sleep=(ms:number)=>new Promise<void>(r=>setTimeout(r,ms));
class FakeSocket extends EventTarget {
 readyState=1;
 constructor(private readonly first: boolean){
  super();
  queueMicrotask(()=>{
   this.dispatchEvent(new Event("open"));
   if(this.first)setTimeout(()=>this.dispatchEvent(new Event("error")),10);
  });
 }
 close(code?:number,reason?:string){
  this.readyState=3;
  const ev=Object.assign(new Event("close"),{code:code??1000,reason:reason??"",wasClean:code===1000});
  queueMicrotask(()=>this.dispatchEvent(ev));
 }
}
async function trial(journalWriteDelayMs:number){
 const start=performance.now();let created=0;let secondStart:number|null=null;
 const abort=new AbortController();
 const adapter=new ReferencePriceFeedAdapter({asset:"BTC",symbol:"BTCUSDT",url:"wss://offline.invalid/stream",staleTimeoutMs:15000,
 factory:()=>{created++;if(created===2){secondStart=performance.now();setTimeout(()=>abort.abort("test_done"),5);}
 return new FakeSocket(created===1);},
 reconnect:{initialDelayMs:40,maxDelayMs:40,multiplier:1,jitterRatio:0}
 });
 let errors=0;
 const task=adapter.run({onConnection:()=>{},onTick:()=>{},onError:async()=>{errors++;if(journalWriteDelayMs)await sleep(journalWriteDelayMs);}},abort.signal);
 await Promise.race([task,sleep(4000).then(()=>{throw Error("RECONNECT_TIMEOUT")})]);
 return {journalWriteDelayMs, reconnectAfterMs:+((secondStart??performance.now())-start).toFixed(2),errors,factoryCalls:created};
}
it("reference recovery is blocked by awaited error journaling",async()=>{
 const baseline=await trial(0);const delayed=await trial(800);
 console.log("RECONNECT_DEPENDENCY_PROBE",JSON.stringify({baseline,delayed,extraMs:+(delayed.reconnectAfterMs-baseline.reconnectAfterMs).toFixed(2)}));
 expect(baseline.factoryCalls).toBe(2);expect(delayed.factoryCalls).toBe(2);
 expect(delayed.reconnectAfterMs-baseline.reconnectAfterMs).toBeGreaterThan(650);
},12000);

import {describe,it,expect} from "vitest";
import {PolymarketMarketDataAdapter,ScriptedBoundedMarketTransport} from "@pm/market-data";

import type {MarketSnapshot} from "@pm/core";

async function until(predicate:()=>boolean){const end=Date.now()+4000;while(!predicate()){if(Date.now()>end)throw Error("lane timeout");await new Promise(r=>setTimeout(r,1));}}
function fixture(){
 const transports=Array.from({length:4},()=>new ScriptedBoundedMarketTransport());
 const adapter=new PolymarketMarketDataAdapter(undefined,{transports});const tokens:string[]=[];
 for(let i=0;i<4;i++)for(const outcome of ["UP","DOWN"] as const){const token=`lane-${i}-${outcome.toLowerCase()}`;tokens.push(token);adapter.registerManualToken(token,`lane-${i}`,outcome,"XRP");}
 adapter.setTokenIds(tokens);return {adapter,transports,tokens};
}
const book=(i:number,time:number)=>({type:"book",payload:{tokenId:`lane-${i}-up`,market:`lane-${i}`,timestamp:time,bids:[{price:"0.69",size:"1"}],asks:[{price:"0.70",size:"100"}]}});

describe("owned snapshot callback lanes",()=>{
 it("delivers synchronous observations in FIFO order without a promise per callback and keeps the IO turn bounded",async()=>{
  const {adapter,transports,tokens}=fixture();const abort=new AbortController();const seen:number[]=[];let microtasks=0,maxBeforeMicrotask=0,ioAfter=0;
  const run=adapter.run(tokens,{onSnapshot:s=>{seen.push(s.sequence);maxBeforeMicrotask=Math.max(maxBeforeMicrotask,seen.length-microtasks);queueMicrotask(()=>microtasks++);},onConnection:()=>undefined,onError:e=>{throw e;}},abort.signal);
  try{await until(()=>transports.every(t=>t.stream));transports[0]!.push(book(0,1000));const down=book(0,1000);down.payload.tokenId='lane-0-down';transports[0]!.push(down);
   for(let i=0;i<120;i++)transports[0]!.push({type:'price_change',payload:{timestamp:1001+i,priceChanges:['lane-0-up','lane-0-down'].map(tokenId=>({tokenId,side:'BUY',price:'0.69',size:String(i+2)}))}});setImmediate(()=>ioAfter=seen.length);await until(()=>seen.length===242);
   expect(seen).toEqual(Array.from({length:242},(_,i)=>i+1));expect(maxBeforeMicrotask).toBeGreaterThan(1);expect(ioAfter).toBeGreaterThan(0);expect(ioAfter).toBeLessThanOrEqual(64);expect(adapter.ingressSnapshot().gapTotal).toBe(0);
  }finally{abort.abort();await run;}
 });
 it("a held async snapshot serializes its metadata and next book while another shard continues",async()=>{
  const {adapter,transports,tokens}=fixture();const abort=new AbortController();const seen:string[]=[];let release!:()=>void;
  const hold=new Promise<void>(r=>release=r);let held=false;const errors:Error[]=[];
  const handlers={onSnapshot:(s:MarketSnapshot)=>{seen.push(`book:${s.marketId}:${s.sourceTimestamp}`);if(s.marketId==='lane-0'&&!held){held=true;return hold;}},onConnection:()=>undefined,onError:(e:Error)=>{errors.push(e);},onMinOrderSizeHydrated:()=>{seen.push('metadata:lane-0');}};
  const run=adapter.run(tokens,handlers,abort.signal);
  try{await until(()=>transports.every(t=>t.stream));transports[0]!.push(book(0,1000));await until(()=>held);
   // A real REST hydration completion enters this same production dispatch;
   // inject only that completion boundary, without a second feed consumer.
   const pending=(adapter as unknown as {dispatchMinOrderSizeHydrated(i:number,e:unknown,h:typeof handlers):Promise<void>|void}).dispatchMinOrderSizeHydrated(0,{tokenId:'lane-0-up',marketId:'lane-0',tokenEpoch:1,minOrderSize:'5'},handlers);
   transports[0]!.push(book(0,1001));transports[1]!.push(book(1,1000));await until(()=>seen.some(s=>s.startsWith('book:lane-1:')));
   expect(seen.filter(s=>s.includes('lane-0'))).toHaveLength(1);release();await pending;await until(()=>seen.filter(s=>s.startsWith('book:lane-0:')).length===2);
   expect(seen.filter(s=>s.includes('lane-0'))).toEqual(['book:lane-0:1970-01-01T00:00:01.000Z','metadata:lane-0','book:lane-0:1970-01-01T00:00:01.001Z']);expect(errors).toEqual([]);
  }finally{release();abort.abort();await run;}
 });
 it("a rejected async callback releases its lane and reports the error before the next callback",async()=>{
  const {adapter,transports,tokens}=fixture();const abort=new AbortController();const seen:string[]=[];let calls=0;
  const run=adapter.run(tokens,{onSnapshot:()=>{calls++;seen.push('book:'+calls);if(calls===1)return Promise.reject(Error('EXPECTED_CALLBACK_FAILURE'));},onConnection:()=>undefined,onError:e=>{seen.push(e.message);}},abort.signal);
  try{await until(()=>transports.every(t=>t.stream));transports[0]!.push(book(0,1000));transports[0]!.push(book(0,1001));await until(()=>calls===2);expect(seen).toEqual(['book:1','EXPECTED_CALLBACK_FAILURE','book:2']);expect(adapter.ingressSnapshot().gapTotal).toBe(0);expect(transports[0]!.maxConcurrent).toBe(1);
  }finally{abort.abort();await run;}
 });
});

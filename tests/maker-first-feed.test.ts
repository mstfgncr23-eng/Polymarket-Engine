import {describe,it,expect} from 'vitest';
import {PolymarketMarketDataAdapter,ScriptedBoundedMarketTransport} from '@pm/market-data';
const book=(token='flow-up',time=1000)=>({type:'book',payload:{tokenId:token,market:'flow',timestamp:time,tickSize:'0.01',minOrderSize:'1',bids:[{price:'0.69',size:'2'}],asks:[{price:'0.7',size:'1000'}]}});
const flow={type:'last_trade_price',payload:{tokenId:'flow-up',price:'0.69',size:'3',side:'SELL',timestamp:1001,transactionHash:'tx'}};
function fixture(){const transports=Array.from({length:4},()=>new ScriptedBoundedMarketTransport());const a=new PolymarketMarketDataAdapter(undefined,{transports});const tokens:string[]=[];for(let i=0;i<4;i++)for(const outcome of ['UP','DOWN'] as const){const token=`flow-${i}-${outcome.toLowerCase()}`;tokens.push(token);a.registerManualToken(token,`flow-${i}`,outcome,'XRP');}a.setTokenIds(tokens);return {a,transports,tokens};}
async function until(p:()=>boolean){const end=Date.now()+3000;while(!p()){if(Date.now()>end)throw Error('FLOW_LANE_TIMEOUT');await new Promise(r=>setTimeout(r,1));}}
describe('Maker public-flow source integrity',()=>{
 it('requires a trusted full book, rejects malformed flow, and produces stable duplicate identity without new book snapshots',()=>{
  const a=new PolymarketMarketDataAdapter();a.registerManualToken('flow-up','flow','UP','XRP');a.setTokenIds(['flow-up']);
  expect(a.normalizeTradeEvent(flow)).toBeNull();a.normalizeSourceEvent(book());
  const first=a.normalizeTradeEvent(flow)!;expect(first.quantity).toBe('3');expect(a.normalizeTradeEvent(flow)?.eventId).toBe(first.eventId);
  expect(a.normalizeSourceEvent(flow)).toEqual([]);expect(a.normalizeTradeEvent({...flow,payload:{...flow.payload,size:null}})).toBeNull();
  a.markTokenGappedForTest('flow-up');expect(a.normalizeTradeEvent(flow)).toBeNull();
 });
 it('carries authoritative tick metadata through deltas and explicit tick-size change',()=>{
  const a=new PolymarketMarketDataAdapter();a.registerManualToken('flow-up','flow','UP','XRP');a.setTokenIds(['flow-up']);a.normalizeSourceEvent(book());
  const delta=a.normalizeSourceEvent({type:'price_change',payload:{timestamp:1001,priceChanges:[{tokenId:'flow-up',side:'BUY',price:'0.69',size:'3'}]}})[0]!;expect(delta.tickSize).toBe('0.01');
  expect(a.normalizeSourceEvent({type:'tick_size_change',payload:{tokenId:'flow-up',timestamp:1002,newTickSize:'0.001'}})[0]?.tickSize).toBe('0.001');
 });
 it('awaits trade processing in the owned shard lane before the next book while other shards progress',async()=>{
  const {a,transports,tokens}=fixture(),abort=new AbortController(),seen:string[]=[];let release!:()=>void;const hold=new Promise<void>(r=>release=r);const errors:Error[]=[];
  const run=a.run(tokens,{onSnapshot:s=>{seen.push('book:'+s.tokenId+':'+s.sourceTimestamp);},onTrade:t=>{seen.push('trade:'+t.tokenId);return hold;},onConnection:()=>undefined,onError:e=>{errors.push(e);}},abort.signal);
  try{await until(()=>transports.every(t=>t.stream));transports[0]!.push(book('flow-0-up'));transports[0]!.push({...flow,payload:{...flow.payload,tokenId:'flow-0-up'}});transports[0]!.push(book('flow-0-up',1002));await until(()=>seen.some(x=>x==='trade:flow-0-up'));transports[1]!.push(book('flow-1-up'));await until(()=>seen.some(x=>x.startsWith('book:flow-1-up')));expect(seen.filter(x=>x.startsWith('book:flow-0-up'))).toHaveLength(1);release();await until(()=>seen.filter(x=>x.startsWith('book:flow-0-up')).length===2);expect(errors).toEqual([]);expect(a.ingressSnapshot().gapTotal).toBe(0);}finally{release();abort.abort();await run;}
 });
});

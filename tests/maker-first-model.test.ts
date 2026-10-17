import {describe,it,expect} from 'vitest';
import {createRestingMakerOrder,evolveMakerOrder,evolveMakerTrade,passiveMakerBuyPrice,passiveMakerLimitPrice,makerPairNetPnl,engineEnvSchema,type MarketTrade} from '@pm/core';
import {snapshot,order} from './fixtures.js';
const T=Date.parse('2026-10-15T18:00:00Z'),iso=(n:number)=>new Date(T+n).toISOString();
const book=()=>snapshot({bids:[{price:'0.69',quantity:'2'}],asks:[{price:'0.7',quantity:'1000'}],sourceTimestamp:iso(0),receivedAt:iso(0)});
function quote(){return order('MAKER',{requestedPrice:'0.69',quantity:'2',createdAt:iso(0),makerFirst:{version:1,role:'FIRST_LEG',postOnly:true,expiresAt:iso(2000),submissionLatencyMs:150,tokenEpoch:1,postingBookEventId:'b',takerReferencePrice:'0.7'}});}
const flow=(n=151,extra:Partial<MarketTrade>={}):MarketTrade=>({eventId:'trade-'+n,marketId:'btc-15m',tokenId:'up',outcome:'UP',price:'0.69',quantity:'3',side:'SELL',sourceTimestamp:iso(n),receivedAt:iso(n+1),tokenEpoch:1,transactionHash:'0xtrade',...extra});
describe('Maker-First conservative executed-flow model',()=>{
 it('joins actual best bid and refuses crossing, off-tick, unknown metadata and below-minimum quotes',()=>{
  expect(passiveMakerBuyPrice(book(),'2','0.7')).toBe('0.69');
  for(const b of [snapshot({...book(),tickSize:null}),snapshot({...book(),minOrderSize:null}),snapshot({...book(),bids:[{price:'0.7',quantity:'2'}]}),snapshot({...book(),tickSize:'0.02'})])expect(passiveMakerBuyPrice(b,'2','0.7')).toBeNull();
  expect(passiveMakerBuyPrice(book(),'.5','0.7')).toBeNull();
 });
 it('does not infer a fill or queue progress from a book level disappearing',()=>{
  const r=createRestingMakerOrder(quote(),book(),'OPEN_FIRST_LEG');
  const evolved=evolveMakerOrder(r,book(),snapshot({...book(),bids:[],sequence:2,receivedAt:iso(200)}));
  expect(evolved.fill).toBeNull();expect(evolved.resting.queueAhead).toBe('2');
 });
 it('requires post-activation exact-price aggressor flow, fresh monotonic timestamps and matching epoch',()=>{
  const r=createRestingMakerOrder(quote(),book(),'OPEN_FIRST_LEG');
  for(const t of [flow(100),flow(151,{side:'BUY'}),flow(151,{price:'0.68'}),flow(151,{tokenEpoch:2}),flow(151,{tokenId:'other'}),flow(300)]){
    const result=evolveMakerTrade(r,t,book(),iso(150),T+152);
    expect(result.fill).toBeNull();expect(result.resting.queueAhead).toBe('2');
  }
  expect(evolveMakerTrade(r,flow(2001),book(),iso(150),T+2002).fill).toBeNull();
 });
 it('consumes queue only from executed flow and supports conservative partial then full fills',()=>{
  const r=createRestingMakerOrder(quote(),book(),'OPEN_FIRST_LEG');
  const first=evolveMakerTrade(r,flow(),book(),iso(150),T+152);
  expect(first.fill?.quantity).toBe('0.5');expect(first.fill?.feeUsd).toBe('0');expect(first.resting.remaining).toBe('1.5');
  expect(evolveMakerTrade(first.resting,flow(),book(),iso(150),T+153).fill).toBeNull();
  const next=evolveMakerTrade(first.resting,flow(154),book(),iso(150),T+155);
  expect(next.fill?.quantity).toBe('1.5');expect(next.resting.remaining).toBe('0');expect(next.fill?.makerEvidence?.trade.eventId).toBe('trade-154');
 });
 it('does not invent a rebate and applies a non-taker-only fee schedule',()=>{
  const b=snapshot({...book(),feeSchedule:{...book().feeSchedule!,takerOnly:false}});
  expect(evolveMakerTrade(createRestingMakerOrder(quote(),b),flow(),b,iso(150),T+152).fill?.feeUsd).toBe('0.00749');
  expect(makerPairNetPnl({quantity:'2',firstLegPrice:'0.69',oppositeLegPrice:'0.2',firstLegFees:'0',oppositeFee:'.02'})).toBe('0.2');
 });
 it('requotes lose queue priority and bounded configuration is explicit, default OFF',()=>{
  expect(createRestingMakerOrder({...quote(),id:'new'},book()).queueAhead).toBe('2');
  expect(engineEnvSchema.parse({}).PAPER_MAKER_FIRST_ENABLED).toBe(false);
  expect(engineEnvSchema.safeParse({PAPER_MAKER_HEDGE_QUOTE_TTL_MS:2001}).success).toBe(false);
 });
 it('post-only validates any non-crossing on-tick passive price, not just the best bid',()=>{
  // Sub-best-bid and price-improving (but below ask) quotes are valid post-only.
  expect(passiveMakerLimitPrice(book(),'0.67','2')).toBe('0.67');
  expect(passiveMakerLimitPrice(book(),'0.69','2')).toBe('0.69');
  expect(passiveMakerLimitPrice(book(),'0.7','2')).toBeNull();      // at/above best ask crosses
  expect(passiveMakerLimitPrice(book(),'0.71','2')).toBeNull();     // crosses
  expect(passiveMakerLimitPrice(book(),'0.685','2')).toBeNull();    // off tick
  expect(passiveMakerLimitPrice(book(),'0','2')).toBeNull();        // non-positive
  expect(passiveMakerLimitPrice(snapshot({...book(),tickSize:null}),'0.69','2')).toBeNull();
  expect(passiveMakerLimitPrice(book(),'0.69','.5')).toBeNull();    // below minimum size
 });
 it('createRestingMakerOrder accepts a sub-best-bid passive quote with queue ahead at that level',()=>{
  const b=snapshot({...book(),bids:[{price:'0.69',quantity:'2'},{price:'0.67',quantity:'7'}]});
  const sub=createRestingMakerOrder({...quote(),id:'sub',requestedPrice:'0.67'},b,'OPEN_FIRST_LEG');
  expect(sub.queueAhead).toBe('7');
  expect(sub.remaining).toBe('2');
  // The improved (above best bid, below ask) passive quote is also accepted.
  const improved=createRestingMakerOrder({...quote(),id:'imp',requestedPrice:'0.69'},book(),'OPEN_FIRST_LEG');
  expect(improved.queueAhead).toBe('2');
 });
});

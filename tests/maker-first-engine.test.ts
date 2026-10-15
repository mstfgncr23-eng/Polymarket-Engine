import {describe,it,expect,beforeEach,afterEach} from 'vitest';
import {evaluateNativeDecision,triggerEventSchema,type PaperOrder,type MarketSnapshot,type MarketTrade,type StrategyCandidate} from '@pm/core';
import {MemoryEventJournal,type EventJournal,hashDurablePayload} from '@pm/db';
import {EngineService} from '../apps/engine/src/engine.js';
import {NoopGrokActivationBridge} from '../apps/engine/src/activation/index.js';
import {qualifiedPaperEnv,qualified,discoveryReport,ScriptedMarketAdapter,ScriptedReferenceRegistry,paperFill} from './paper-engine-testkit.js';
import {snapshot,order} from './fixtures.js';
import {p0PostgresFixture} from './p0-postgres-fixture.js';
let pg:Awaited<ReturnType<typeof p0PostgresFixture>>|undefined;
beforeEach(async()=>{if(process.env.P0_EXECUTION_TEST_DATABASE_URL)pg=await p0PostgresFixture(process.env.P0_EXECUTION_TEST_DATABASE_URL);});
afterEach(async()=>{if(pg){try{
 for(const fact of await pg.journal.queryDurableTransitions({limit:1000}))if(Object.keys(fact.payload).length)
  expect(hashDurablePayload(fact.payload),`persisted ${fact.operationKey}`).toBe(fact.payloadHash);
}finally{await pg.close();pg=undefined;}}});
const T=Date.parse('2026-10-15T18:00:00Z'),M='maker-mkt',U='maker-up',D='maker-down';
async function setup(enabled=true,journal:EventJournal=pg?.journal??new MemoryEventJournal(),clock={ms:T},overrides:Record<string,string>={}){
 const market=new ScriptedMarketAdapter(),listing=qualified(M,'XRP',U,D,new Date(T+900000).toISOString());listing.raw={eventStartTime:new Date(T-1000).toISOString()};const second=qualified(M+'-2','XRP',U+'-2',D+'-2',new Date(T+900000).toISOString());second.raw=listing.raw;market.queue(discoveryReport([listing,second]));
 const errors:unknown[]=[];
 const engine=new EngineService(qualifiedPaperEnv({PAPER_MAKER_FIRST_ENABLED:String(enabled),PAPER_EXECUTION_LATENCY_MS:'150',ESTIMATED_INFRA_COST_PER_HOUR_USD:'0',DISCOVERY_INTERVAL_MS:'3600000',...overrides}),journal,{error:e=>errors.push(e)},{market:market as never,referenceRegistry:new ScriptedReferenceRegistry() as never,activation:new NoopGrokActivationBridge(),now:()=>new Date(clock.ms),resolutionFetcher:async()=>null});
 await engine.start();await market.waitForHandlers();if(engine.experimentManifest()?.status==='IDLE')expect((await engine.beginExperiment()).ok).toBe(true);
 const b=(outcome:'UP'|'DOWN',seq=1):MarketSnapshot=>snapshot({marketId:M,tokenId:outcome==='UP'?U:D,outcome,asset:'XRP',bids:[{price:outcome==='UP'?'0.69':'0.29',quantity:'2'}],asks:[{price:outcome==='UP'?'0.7':'0.3',quantity:'1000'}],receivedAt:new Date(clock.ms).toISOString(),sourceTimestamp:new Date(clock.ms).toISOString(),sequence:seq});
 await market.emitSnapshot(b('UP'));await market.emitSnapshot(b('DOWN'));
 await engine.ingestReferenceTickForTest({eventId:'maker-ref',traceId:'maker-ref',asset:'XRP',symbol:'XRPUSDT',venue:'test',price:'1',quantity:'1',sourceTimestamp:new Date(clock.ms).toISOString(),receivedAt:new Date(clock.ms).toISOString(),tradeId:'maker-ref'});
 const intern=engine as unknown as {submitPaperOrder(o:PaperOrder,b:MarketSnapshot,key:string,intent:string):Promise<boolean>;expireMakerQuotes():Promise<void>;onMakerTrade(t:MarketTrade):Promise<void>|void;};
 const quote=(intent:'OPEN_FIRST_LEG'|'HEDGE_SECOND_LEG'='OPEN_FIRST_LEG')=>{const bk=b(intent==='OPEN_FIRST_LEG'?'UP':'DOWN');return order('MAKER',{marketId:M,tokenId:bk.tokenId,outcome:bk.outcome,requestedPrice:bk.bids[0]!.price,quantity:'2',createdAt:new Date(clock.ms).toISOString(),strategyId:'temporal-pair-hedge',...(intent==='OPEN_FIRST_LEG'?{entryAdmission:{fairProbability:.9,signalExpiresAt:new Date(clock.ms+60000).toISOString()}}:{}),makerFirst:{version:1,role:intent==='OPEN_FIRST_LEG'?'FIRST_LEG':'HEDGE',postOnly:true,expiresAt:new Date(clock.ms+2000).toISOString(),submissionLatencyMs:150,tokenEpoch:market.tokenEpoch(bk.tokenId),postingBookEventId:bk.eventId,takerReferencePrice:bk.asks[0]!.price}});};
 const submit=async(o=quote(),intent='OPEN_FIRST_LEG')=>intern.submitPaperOrder(o,engine.state.marketSnapshots.get(o.tokenId)!,`maker:${o.id}`,intent);
 const trade=(o:PaperOrder,quantity='3',n=clock.ms):MarketTrade=>({eventId:'trade-'+n,marketId:M,tokenId:o.tokenId,outcome:o.outcome,price:o.requestedPrice,quantity,side:'SELL',sourceTimestamp:new Date(n).toISOString(),receivedAt:new Date(n).toISOString(),tokenEpoch:market.tokenEpoch(o.tokenId),transactionHash:'tx-'+n});
 return {engine,journal,market,clock,intern,b,quote,submit,trade,errors};
}
describe('Maker-First real Engine lifecycle',()=>{
 it.each(['fresh','price-worse','stale','closed','partial','dual','concurrent','cash','daily-loss','signal-expired','epoch','restart','book-expiry','close-race'] as const)('directional TTL fallback revalidates once without widening limits (%s)',async scenario=>{
  const s=await setup(true,undefined,{ms:T},{PAPER_DUAL_SIDED_MAKER_ENABLED:String(scenario==='dual')});
  try{
   const bk=(outcome:'UP'|'DOWN',seq:number)=>({...s.b(outcome,seq),bids:[{price:outcome==='UP'?'0.69':'0.26',quantity:outcome==='UP'?'100000':'1000'}],asks:[{price:outcome==='UP'?'0.70':'0.27',quantity:outcome==='UP'?'1000':'100000'}]});
   await s.market.emitSnapshot(bk('UP',10));await s.market.emitSnapshot(bk('DOWN',10));
   const trigger=triggerEventSchema.parse({id:'ttl-fallback-trigger',traceId:'ttl-fallback-trace',marketId:M,asset:'XRP',source:'polymarket',kind:'BOOK_IMBALANCE',severity:2,score:.9,dedupeKey:'ttl-fallback',evidence:{},sourceEventIds:['ttl-source'],createdAt:new Date(T).toISOString(),expiresAt:new Date(T+(scenario==='signal-expired'?1000:60000)).toISOString()});
   const native=evaluateNativeDecision({trigger,up:bk('UP',10),down:bk('DOWN',10),reference:null,referenceRequired:false,marketSynchronized:true,triggerProvenanceValid:true,now:new Date(T)})!;
   expect(native.direction).toBe('UP');
   const intern=s.engine as any;intern.activeTriggers.set(trigger.id,trigger);intern.triggerProvenance.set(trigger.id,{kind:'NONE'});
   await intern.evaluateTrade(trigger,intern.nativeCompatibilityDecision(trigger,native),native);
   const initial=[...s.engine.restingOrdersForTest().values()].map(r=>r.order);expect(initial,JSON.stringify({risk:s.engine.state.latestRisk,decisions:s.engine.state.decisions,events:await s.journal.queryEvents({types:['RISK_EVALUATION']})})).toHaveLength(scenario==='dual'?2:1);
   const first=initial.find(o=>o.outcome==='UP')!;
   if(scenario==='restart'){
    await s.engine.stop();const recovered=await setup(true,s.journal,s.clock);
    try{s.clock.ms=T+2001;await recovered.intern.expireMakerQuotes();expect(recovered.engine.restingOrderCount()).toBe(0);expect(recovered.engine.reservedCash()).toBe('0');expect(await s.journal.queryEvents({types:['PAPER_FILL']})).toHaveLength(0);expect((recovered.engine as any).makerEntryContinuations.size).toBe(0);}finally{await recovered.engine.stop();}
    return;
   }
   if(scenario==='partial'){s.clock.ms+=201;await s.intern.onMakerTrade(s.trade(first,'100001'));expect(s.engine.ledger.positions.get(M)?.unpairedQuantity).toBe('0.5');}
   s.clock.ms=T+2001;
   if(scenario==='closed')expect((await s.engine.closeExperimentEntries()).ok).toBe(true);
   if(scenario!=='stale'){
    const up=bk('UP',11);if(scenario==='price-worse')up.asks=[{price:'0.71',quantity:'10'}];
    // Direct cache update keeps TTL as the cancellation authority under test.
    intern.snapshots.set(M,{UP:up,DOWN:bk('DOWN',11)});s.engine.state.marketSnapshots.set(U,up);s.engine.state.marketSnapshots.set(D,bk('DOWN',11));
    await s.engine.ingestReferenceTickForTest({eventId:'ttl-ref-new',traceId:'ttl-ref-new',asset:'XRP',symbol:'XRPUSDT',venue:'test',price:'1',quantity:'1',sourceTimestamp:new Date(s.clock.ms).toISOString(),receivedAt:new Date(s.clock.ms).toISOString(),tradeId:'ttl-ref-new'});
   }else s.clock.ms=T+16001;
   if(scenario==='cash')s.engine.ledger.restore({...s.engine.ledger.snapshot(),cash:'0.01'},[]);
   if(scenario==='daily-loss')s.engine.ledger.restore({...s.engine.ledger.snapshot(),netTradingPnl:'-51'},[]);
   if(scenario==='epoch')s.market.markTokenSync(U,'GAPPED');
   if(scenario==='book-expiry'){
    await s.market.emitSnapshot(bk('UP',12));expect((await s.journal.queryEvents({types:['PAPER_FILL']}))).toHaveLength(0);
   }
   let closing:Promise<Record<string,unknown>>|undefined;
   if(scenario==='close-race'){
    const read=s.journal.readDurableByOperationKey.bind(s.journal);let once=false;
    s.journal.readDurableByOperationKey=async key=>{if(!once&&key===`maker:${first.id}:cancel`){once=true;closing=intern.durableGate.outside(()=>s.engine.closeExperimentEntries());}return read(key);};
   }
   if(scenario==='concurrent')await Promise.all([s.intern.expireMakerQuotes(),s.intern.expireMakerQuotes()]);else await s.intern.expireMakerQuotes();
   if(closing)await closing;
   await s.intern.expireMakerQuotes();
   const fills=(await s.journal.queryEvents({types:['PAPER_FILL']})).filter(r=>r.payload.liquidityRole==='TAKER');
   const shouldFill=scenario==='fresh'||scenario==='concurrent'||scenario==='book-expiry';expect(fills,JSON.stringify((await s.journal.queryEvents({types:['RISK_EVALUATION']})).map(r=>r.payload))).toHaveLength(shouldFill?1:0);
   if(shouldFill){
    expect(fills[0]!.payload).toMatchObject({fillPrice:'0.7',intent:'OPEN_FIRST_LEG'});
    const facts=await s.journal.queryDurableTransitions({limit:1000});
    expect(facts.find(f=>f.operationKey===`maker:${first.id}:cancel`)!.sequence)
      .toBeLessThan(facts.find(f=>f.type==='PAPER_FILL'&&f.aggregateId===fills[0]!.aggregateId)!.sequence);
    const attempts=await s.journal.queryEvents({types:['MAKER_FIRST_TAKER_FALLBACK_TERMINAL']});expect(attempts).toHaveLength(1);expect(attempts[0]!.payload).toMatchObject({parentOrderId:first.id,outcome:'FILLED'});
    s.clock.ms++;await s.intern.onMakerTrade(s.trade(first,'100000'));expect((await s.journal.queryEvents({types:['PAPER_FILL']}))).toHaveLength(1);
   }
   expect(s.engine.reservedCash()).toBe('0');expect(s.engine.integrityFault()).toBe(false);
   expect((s.engine as any).makerEntryContinuations.size).toBe(0);
  }finally{await s.engine.stop();}
 });
 it('cancellation records eligible public volume exhausted by queue without inventing a fill',async()=>{
  const s=await setup();try{
   const o=s.quote();expect(await s.submit(o)).toBe(true);
   s.clock.ms+=100;await s.intern.onMakerTrade(s.trade(o,'100'));
   s.clock.ms+=51;await s.intern.onMakerTrade(s.trade(o,'1'));s.clock.ms++;await s.intern.onMakerTrade(s.trade(o,'1'));
   s.clock.ms=T+2001;await s.intern.expireMakerQuotes();
   const p=(await s.journal.queryEvents({types:['PAPER_ORDER_CANCELLED']}))[0]!.payload;
   expect(p).toMatchObject({reason:'MAKER_FIRST_TTL_EXPIRED',makerExecutionEvidence:{queueAhead:'0',publicFlow:{observedTrades:3,exactPriceSellTrades:3,eligibleTrades:2,eligibleQuantity:'2'}}});
   expect(await s.journal.queryEvents({types:['PAPER_FILL']})).toHaveLength(0);expect(s.engine.reservedCash()).toBe('0');
  }finally{await s.engine.stop();}
 });
 it('a partial dual hedge cancels excess without reopening directional exposure or double spending',async()=>{
  const s=await setup(true,undefined,{ms:T},{PAPER_DUAL_SIDED_MAKER_ENABLED:'true'});try{
   const dual=s.engine as unknown as {trySubmitDualMakerPair(o:PaperOrder,k:string,p:Record<'UP'|'DOWN',MarketSnapshot>):Promise<boolean>};
   expect(await dual.trySubmitDualMakerPair(s.quote(),'dual:hedge-partial',{UP:s.engine.state.marketSnapshots.get(U)!,DOWN:s.engine.state.marketSnapshots.get(D)!})).toBe(true);
   const q=[...s.engine.restingOrdersForTest().values()].map(r=>r.order);s.clock.ms+=201;
   await s.intern.onMakerTrade(s.trade(q.find(o=>o.outcome==='UP')!,'6'));s.clock.ms++;
   const down=q.find(o=>o.outcome==='DOWN')!;await s.intern.onMakerTrade(s.trade(down,'3'));
   expect(s.engine.ledger.positions.get(M)).toMatchObject({pairedQuantity:'0.5',unpairedOutcome:'UP',unpairedQuantity:'1.5'});
   expect(s.engine.restingOrderCount()).toBe(0);expect(s.engine.reservedCash()).toBe('0');
   const cash=s.engine.ledger.snapshot().cash;s.clock.ms++;await s.intern.onMakerTrade(s.trade(down,'100'));
   expect(s.engine.ledger.snapshot().cash).toBe(cash);expect(await s.journal.queryEvents({types:['PAPER_FILL']})).toHaveLength(2);
   expect(s.engine.integrityFault()).toBe(false);
  }finally{await s.engine.stop();}
 });
 it('pending dual quotes receive no hedge credit in decision or serialized entry risk limits',async()=>{
  const s=await setup(true,undefined,{ms:T},{PAPER_DUAL_SIDED_MAKER_ENABLED:'true'});
  try{
   (s.engine as unknown as {riskLimits:{maxDirectionalExposureUsd:number}}).riskLimits.maxDirectionalExposureUsd=2;
   const dual=s.engine as unknown as {trySubmitDualMakerPair(o:PaperOrder,k:string,p:Record<'UP'|'DOWN',MarketSnapshot>):Promise<boolean>};
   expect(await dual.trySubmitDualMakerPair(s.quote(),'dual:pending-risk',{UP:s.engine.state.marketSnapshots.get(U)!,DOWN:s.engine.state.marketSnapshots.get(D)!})).toBe(true);
   const b={...s.b('UP',2),marketId:M+'-2',tokenId:U+'-2'};await s.market.emitSnapshot(b);
   const c:StrategyCandidate={strategyId:'test',strategyVersion:'test',marketId:b.marketId,tokenId:b.tokenId,outcome:'UP',side:'BUY',quantity:'1',limitPrice:'0.7',intent:'OPEN_FIRST_LEG',rawExpectedEdge:.2,rawEdgeUsdPerShare:.2,reasonCodes:[]};
   expect(s.engine.decideWithRuntimeBasisForTest(c,{UP:b,DOWN:s.b('DOWN')},new Date(s.clock.ms),'XRP').risk.vetoes).toContain('MAX_DIRECTIONAL_EXPOSURE');
   const o={...s.quote(),id:'other-market-after-dual',marketId:b.marketId,tokenId:b.tokenId,quantity:'1',makerFirst:{...s.quote().makerFirst!,tokenEpoch:s.market.tokenEpoch(b.tokenId)}};
   expect(await s.submit(o)).toBe(false);expect(s.engine.restingOrderCount()).toBe(2);expect(s.engine.reservedCash()).toBe('1.96');
   expect(s.engine.integrityFault()).toBe(false);
  }finally{await s.engine.stop();}
 });
 it('a sibling commit failure fails closed without later fills/cancels and recovery releases the committed first reservation',async()=>{
  let s=await setup(true,undefined,{ms:T},{PAPER_DUAL_SIDED_MAKER_ENABLED:'true'});
  try{
   const dual=s.engine as unknown as {trySubmitDualMakerPair(o:PaperOrder,k:string,p:Record<'UP'|'DOWN',MarketSnapshot>):Promise<boolean>};
   const commit=s.journal.commitDurableTransition.bind(s.journal);let count=0;
   s.journal.commitDurableTransition=async(input,...args)=>{if(input.type==='PAPER_MAKER_RESERVED'&&++count===2)throw Error('TEST_SIBLING_COMMIT_FAILED');return commit(input,...args);};
   await expect(dual.trySubmitDualMakerPair(s.quote(),'dual:db-fault',{UP:s.engine.state.marketSnapshots.get(U)!,DOWN:s.engine.state.marketSnapshots.get(D)!})).rejects.toThrow('TEST_SIBLING_COMMIT_FAILED');
   expect(s.engine.integrityFault()).toBe(true);const sequence=await s.journal.latestDurableSequence();
   const q=[...s.engine.restingOrdersForTest().values()][0]!.order;
   s.clock.ms+=201;await s.intern.onMakerTrade(s.trade(q,'100'));s.clock.ms+=2001;await s.intern.expireMakerQuotes();
   await s.market.emitSnapshot(s.b('UP',99));
   expect(await s.journal.latestDurableSequence()).toBe(sequence);expect(s.engine.ledger.positions.size).toBe(0);
   await s.engine.stop();s.journal.commitDurableTransition=commit;
   s=await setup(true,s.journal,s.clock,{PAPER_DUAL_SIDED_MAKER_ENABLED:'true'});
   expect(s.engine.restingOrderCount()).toBe(0);expect(s.engine.reservedCash()).toBe('0');expect(s.engine.integrityFault()).toBe(false);
   expect(await s.journal.queryEvents({types:['PAPER_FILL']})).toHaveLength(0);
  }finally{await s.engine.stop();}
 });
 it.each(['BEFORE_DURABLE_COMMIT','AFTER_DURABLE_COMMIT_BEFORE_APPLY','AFTER_APPLY_BEFORE_DERIVED_AUDIT'] as const)('dual first partial crash %s replays once and cancels the unproven sibling',async point=>{
  let s=await setup(true,undefined,{ms:T},{PAPER_DUAL_SIDED_MAKER_ENABLED:'true'});
  try{
   const dual=s.engine as unknown as {trySubmitDualMakerPair(o:PaperOrder,k:string,p:Record<'UP'|'DOWN',MarketSnapshot>):Promise<boolean>};
   expect(await dual.trySubmitDualMakerPair(s.quote(),'dual:partial-crash',{UP:s.engine.state.marketSnapshots.get(U)!,DOWN:s.engine.state.marketSnapshots.get(D)!})).toBe(true);
   const first=[...s.engine.restingOrdersForTest().values()].find(r=>r.order.outcome==='UP')!.order;
   s.clock.ms+=201;const trade=s.trade(first,'3');s.engine.setCrashInjectionForTest(point,{operationKey:`fill:maker-fill:${first.id}:${trade.eventId}`});
   await expect(s.intern.onMakerTrade(trade)).rejects.toThrow('CRASH_INJECTED');
   await s.engine.stop();s=await setup(true,s.journal,s.clock,{PAPER_DUAL_SIDED_MAKER_ENABLED:'true'});
   expect(s.engine.ledger.positions.get(M)?.unpairedQuantity??'0').toBe(point==='BEFORE_DURABLE_COMMIT'?'0':'0.5');
   expect(s.engine.restingOrderCount()).toBe(0);expect(s.engine.reservedCash()).toBe('0');
   expect(await s.journal.queryEvents({types:['PAPER_FILL']})).toHaveLength(point==='BEFORE_DURABLE_COMMIT'?0:1);
   expect(await s.journal.queryEvents({types:['PAPER_ORDER_TERMINAL']})).toHaveLength(2);expect(s.engine.integrityFault()).toBe(false);
  }finally{await s.engine.stop();}
 });
 it.each([false,true])('an unfilled first-leg cancellation preserves strategy cooldown (dual=%s)',async dualEnabled=>{
  const s=await setup(true,undefined,{ms:T},{PAPER_DUAL_SIDED_MAKER_ENABLED:String(dualEnabled)});
  try{
   const key=`${M}:UP:OPEN_FIRST_LEG`;
   if(dualEnabled){
    const dual=s.engine as unknown as {trySubmitDualMakerPair(o:PaperOrder,k:string,p:Record<'UP'|'DOWN',MarketSnapshot>):Promise<boolean>};
    expect(await dual.trySubmitDualMakerPair(s.quote(),key,{UP:s.engine.state.marketSnapshots.get(U)!,DOWN:s.engine.state.marketSnapshots.get(D)!})).toBe(true);
   }else expect(await s.intern.submitPaperOrder(s.quote(),s.engine.state.marketSnapshots.get(U)!,key,'OPEN_FIRST_LEG')).toBe(true);
   s.clock.ms+=2001;await s.intern.expireMakerQuotes();
   expect(s.engine.reservedCash()).toBe('0');expect(s.engine.restingOrderCount()).toBe(0);
   const candidate:StrategyCandidate={strategyId:'test',strategyVersion:'test',marketId:M,tokenId:U,outcome:'UP',side:'BUY',quantity:'2',limitPrice:'0.7',intent:'OPEN_FIRST_LEG',rawExpectedEdge:.2,rawEdgeUsdPerShare:.2,reasonCodes:[]};
   expect(s.engine.decideWithRuntimeBasisForTest(candidate,{UP:s.b('UP'),DOWN:s.b('DOWN')},new Date(s.clock.ms),'XRP').risk.vetoes).toContain('DUPLICATE_TRADE');
  }finally{await s.engine.stop();}
 });
 it('a rejected second dual reservation rolls back cash, records exact fence evidence and allows one directional fallback',async()=>{
  const s=await setup(true,undefined,{ms:T},{PAPER_DUAL_SIDED_MAKER_ENABLED:'true'});
  try{
   const dual=s.engine as unknown as {trySubmitDualMakerPair(o:PaperOrder,k:string,p:Record<'UP'|'DOWN',MarketSnapshot>):Promise<boolean>};
   const read=s.journal.readDurableByOperationKey.bind(s.journal);let calls=0;
   s.journal.readDurableByOperationKey=async key=>{
    if(key.startsWith('maker:')&&key.endsWith(':reserve')&&++calls===4)
     s.engine.state.marketSnapshots.set(D,{...s.b('DOWN',2),bids:[{price:'0.28',quantity:'2'}]});
    return read(key);
   };
   expect(await dual.trySubmitDualMakerPair(s.quote(),'dual:rollback',{UP:s.engine.state.marketSnapshots.get(U)!,DOWN:s.engine.state.marketSnapshots.get(D)!})).toBe(false);
   expect(s.engine.reservedCash()).toBe('0');expect(s.engine.restingOrderCount()).toBe(0);
   const failed=await s.journal.queryEvents({types:['DUAL_MAKER_PLAN_NOT_ADMITTED'],experimentId:s.engine.experimentManifest()!.id});expect(failed).toHaveLength(1);
   expect(failed[0]?.payload).toMatchObject({fallbackAllowed:true,makerRejectionEvidence:{stage:'BEFORE_DURABLE_COMMIT',passiveAtOriginalLimit:true,passiveJoinPrice:'0.28'}});
   expect(await s.journal.queryEvents({types:['DUAL_MAKER_QUOTES_ADMITTED']})).toHaveLength(0);
   expect(await s.submit()).toBe(true);expect(s.engine.restingOrderCount()).toBe(1);
   expect(await s.submit(s.quote())).toBe(false);expect(s.engine.restingOrderCount()).toBe(1);
   expect(s.engine.runtimeBookkeepingForTest().executionErrors).toBe(0);expect(s.engine.integrityFault()).toBe(false);
  }finally{await s.engine.stop();}
 });
 it.each(['BEFORE_DURABLE_COMMIT','AFTER_DURABLE_COMMIT_BEFORE_APPLY','AFTER_APPLY_BEFORE_DERIVED_AUDIT'] as const)('dual sibling reservation crash %s never falls through and recovers only committed authority',async point=>{
  let s=await setup(true,undefined,{ms:T},{PAPER_DUAL_SIDED_MAKER_ENABLED:'true'});
  try{
   const dual=s.engine as unknown as {trySubmitDualMakerPair(o:PaperOrder,k:string,p:Record<'UP'|'DOWN',MarketSnapshot>):Promise<boolean>};
   const read=s.journal.readDurableByOperationKey.bind(s.journal);let calls=0;
   s.journal.readDurableByOperationKey=async key=>{
    if(key.startsWith('maker:')&&key.endsWith(':reserve')&&++calls===4)s.engine.setCrashInjectionForTest(point,{operationKey:key});
    return read(key);
   };
   await expect(dual.trySubmitDualMakerPair(s.quote(),'dual:crash',{UP:s.engine.state.marketSnapshots.get(U)!,DOWN:s.engine.state.marketSnapshots.get(D)!})).rejects.toThrow('CRASH_INJECTED');
   await s.engine.stop();s=await setup(true,s.journal,s.clock,{PAPER_DUAL_SIDED_MAKER_ENABLED:'true'});
   expect(s.engine.restingOrderCount()).toBe(0);expect(s.engine.reservedCash()).toBe('0');expect(s.engine.ledger.positions.size).toBe(0);
   const orders=await s.journal.queryEvents({types:['PAPER_ORDER_CREATED'],experimentId:s.engine.experimentManifest()!.id});
   expect(orders).toHaveLength(point==='BEFORE_DURABLE_COMMIT'?1:2);
   expect(await s.journal.queryEvents({types:['PAPER_ORDER_TERMINAL']})).toHaveLength(orders.length);
   expect(await s.journal.queryEvents({types:['DUAL_MAKER_QUOTES_ADMITTED']})).toHaveLength(point==='BEFORE_DURABLE_COMMIT'?0:1);
   expect(await s.journal.queryEvents({types:['PAPER_FILL']})).toHaveLength(0);expect(s.engine.integrityFault()).toBe(false);
  }finally{await s.engine.stop();}
 });
 it.each(['passive','crossed'] as const)('an actionable Profit-Trailing target wins a %s dual-sibling race with one terminal cancel and taker fill',async mode=>{
  const s=await setup(true,undefined,{ms:T},{PAPER_DUAL_SIDED_MAKER_ENABLED:'true',PROFIT_TRAILING_PAIR_SOAK_ENABLED:'true',NORMAL_HEDGE_MIN_LOCKED_PAIR_EDGE:'0.12'});
  try{
   const dual=s.engine as unknown as {trySubmitDualMakerPair(o:PaperOrder,k:string,p:Record<'UP'|'DOWN',MarketSnapshot>):Promise<boolean>};
   if(mode==='passive')await s.market.emitSnapshot({...s.b('DOWN',2),bids:[{price:'0.15',quantity:'2'}]});
   expect(await dual.trySubmitDualMakerPair(s.quote(),'dual:pt',{UP:s.engine.state.marketSnapshots.get(U)!,DOWN:s.engine.state.marketSnapshots.get(D)!})).toBe(true);
   const q=[...s.engine.restingOrdersForTest().values()].map(r=>r.order),up=q.find(o=>o.outcome==='UP')!,down=q.find(o=>o.outcome==='DOWN')!;
   s.clock.ms+=201;await s.intern.onMakerTrade(s.trade(up,'6'));
   await s.market.emitSnapshot({...s.b('DOWN',2),bids:[{price:'0.15',quantity:'2'}],asks:[{price:'0.16',quantity:'1000'}]});
   const until=Date.now()+2500;while(s.engine.ledger.positions.get(M)?.unpairedQuantity!=='0'||(s.engine as unknown as {profitTrailingPairInFlight:Set<string>}).profitTrailingPairInFlight.has(M)){
    if(Date.now()>until)throw Error('DUAL_PT_TARGET_TIMEOUT:'+JSON.stringify({errors:s.errors,position:s.engine.ledger.positions.get(M),events:(await s.journal.queryEvents({types:['RISK_EVALUATION','PAPER_EXECUTION_ERROR','PAPER_ORDER_CANCELLED']})).map(e=>({type:e.type,payload:e.payload}))}));await new Promise(r=>setTimeout(r,2));
   }
   s.clock.ms++;await s.intern.onMakerTrade(s.trade(down,'100'));
   const hedge=(await s.journal.queryEvents({types:['PAPER_FILL']})).filter(e=>e.payload.intent==='HEDGE_SECOND_LEG');
   expect(hedge).toHaveLength(1);expect(hedge[0]?.payload.liquidityRole).toBe('TAKER');expect(s.engine.ledger.positions.get(M)?.pairedQuantity).toBe('2');
   const cancelled=(await s.journal.queryEvents({types:['PAPER_ORDER_CANCELLED']})).find(e=>e.payload.orderId===down.id);
   expect(cancelled?.payload.reason).toBe(mode==='passive'?'PROFIT_TRAILING_PRIORITY':'MAKER_FIRST_MARKET_MOVED');
   const facts=await s.journal.queryDurableTransitions({types:['PAPER_ORDER_CANCELLED','PAPER_FILL'],limit:100});
   expect(facts.find(e=>e.aggregateId===down.id)!.sequence).toBeLessThan(facts.find(e=>e.aggregateId===hedge[0]!.aggregateId)!.sequence);
   expect((await s.journal.queryEvents({types:['PAPER_ORDER_CANCELLED']})).filter(e=>e.payload.orderId===down.id)).toHaveLength(1);
   expect(s.engine.restingOrderCount()).toBe(0);expect(s.engine.reservedCash()).toBe('0');expect(s.engine.integrityFault()).toBe(false);
  }finally{await s.engine.stop();}
 });
 it('a rejected passive hedge immediately hands off to taker without inheriting maker cooldown',async()=>{const s=await setup(true,undefined,{ms:T},{PROFIT_TRAILING_PAIR_SOAK_ENABLED:'true',NORMAL_HEDGE_MIN_LOCKED_PAIR_EDGE:'0.12'});try{await s.engine.applyObservedFill(paperFill(M,'UP','0.7',{tokenId:U,quantity:'2',timestamp:new Date(T).toISOString()}));const read=s.journal.readDurableByOperationKey.bind(s.journal);let moved=false;s.journal.readDurableByOperationKey=async key=>{if(!moved&&key.startsWith('maker:')&&key.endsWith(':reserve')){moved=true;s.engine.state.marketSnapshots.set(D,{...s.b('DOWN',3),bids:[{price:'0.14',quantity:'2'}],asks:[{price:'0.16',quantity:'1000'}]});}return read(key);};await s.market.emitSnapshot({...s.b('DOWN',2),bids:[{price:'0.15',quantity:'2'}],asks:[{price:'0.16',quantity:'1000'}]});const until=Date.now()+2500;while(s.engine.ledger.positions.get(M)?.unpairedQuantity!=='0'||(s.engine as unknown as {profitTrailingPairInFlight:Set<string>}).profitTrailingPairInFlight.has(M)){if(Date.now()>until)throw Error('REJECTED_MAKER_FALLBACK_TIMEOUT');await new Promise(r=>setTimeout(r,2));}expect(moved).toBe(true);expect(s.engine.restingOrderCount()).toBe(0);expect(s.engine.reservedCash()).toBe('0');const fills=(await s.journal.queryEvents({types:['PAPER_FILL']})).filter(r=>r.payload.intent==='HEDGE_SECOND_LEG');expect(fills).toHaveLength(1);expect(fills[0]?.payload.liquidityRole).toBe('TAKER');expect(s.engine.runtimeBookkeepingForTest().executionErrors).toBe(0);}finally{await s.engine.stop();}});

 it.each(['book','cash'] as const)('expected maker %s rejection cannot trip the execution-error breaker',async reason=>{const s=await setup();try{const o=s.quote(),before=s.engine.runtimeBookkeepingForTest().executionErrors;if(reason==='book')s.engine.state.marketSnapshots.set(U,{...s.b('UP',2),bids:[{price:'0.68',quantity:'2'}]});else{s.engine.ledger.restore({...s.engine.ledger.snapshot(),cash:'0.01'},[]);}expect(await s.submit(o)).toBe(false);expect(s.engine.runtimeBookkeepingForTest().executionErrors).toBe(before);expect(s.engine.reservedCash()).toBe('0');expect(s.engine.integrityFault()).toBe(false);const terminals=await s.journal.queryEvents({types:['PAPER_ORDER_TERMINAL']});expect(terminals).toHaveLength(1);if(reason==='book')expect(terminals[0]?.payload.reason).toBe('MAKER_FIRST_POST_ONLY_INVALID');else expect(String(terminals[0]?.payload.reason)).toMatch(/^MAKER_CASH_FENCE_REJECTED:/);}finally{await s.engine.stop();}});

 it('retains TAKER when the passive bid violates the unchanged 0.65 first-leg floor',async()=>{const s=await setup();try{const b={...s.b('UP'),bids:[{price:'0.64',quantity:'2'}]},c:StrategyCandidate={strategyId:'test',strategyVersion:'test',marketId:M,tokenId:U,outcome:'UP',side:'BUY',quantity:'2',limitPrice:'0.7',intent:'OPEN_FIRST_LEG',rawExpectedEdge:.2,reasonCodes:[]};const decision=s.engine.decideWithRuntimeBasisForTest(c,{UP:b,DOWN:s.b('DOWN')},new Date(s.clock.ms),'XRP');expect(decision.accepted).toBe(true);expect(decision.order?.orderType).toBe('TAKER');expect(decision.executablePrice).toBe('0.7');}finally{await s.engine.stop();}});
 it('fee schedule changes cancel a quote instead of spending unreserved fees or claiming a free maker fill',async()=>{const s=await setup();try{const o=s.quote();o.makerFirst!.postingFeeKey=JSON.stringify(['0.07',1,true]);await s.submit(o);s.clock.ms+=151;s.engine.state.marketSnapshots.set(U,{...s.b('UP',2),feeSchedule:{...s.b('UP').feeSchedule!,rate:'0.08'}});await s.intern.onMakerTrade(s.trade(o,'100'));expect(s.engine.ledger.positions.size).toBe(0);expect(s.engine.reservedCash()).toBe('0');const cancelled=await s.journal.queryEvents({types:['PAPER_ORDER_CANCELLED']});expect(cancelled[0]?.payload.reason).toBe('MAKER_FIRST_FEES_CHANGED');}finally{await s.engine.stop();}});

 it('counts outstanding first-leg maker exposure at decision and serialized reservation boundaries',async()=>{const s=await setup();try{(s.engine as unknown as {riskLimits:{maxDirectionalExposureUsd:number}}).riskLimits.maxDirectionalExposureUsd=2;const a=s.quote();expect(await s.submit(a)).toBe(true);const b={...s.b('UP',2),marketId:M+'-2',tokenId:U+'-2'};await s.market.emitSnapshot(b);const c:StrategyCandidate={strategyId:'test',strategyVersion:'test',marketId:b.marketId,tokenId:b.tokenId,outcome:'UP',side:'BUY',quantity:'2',limitPrice:'0.7',intent:'OPEN_FIRST_LEG',rawExpectedEdge:.2,reasonCodes:[]};const decision=s.engine.decideWithRuntimeBasisForTest(c,{UP:b,DOWN:s.b('DOWN')},new Date(s.clock.ms),'XRP');expect(decision.accepted).toBe(false);expect(decision.risk.vetoes).toContain('MAX_DIRECTIONAL_EXPOSURE');const o={...s.quote(),id:'second-pending-market',marketId:b.marketId,tokenId:b.tokenId,makerFirst:{...s.quote().makerFirst!,tokenEpoch:s.market.tokenEpoch(b.tokenId)}};expect(await s.submit(o)).toBe(false);expect(s.engine.reservedCash()).toBe('1.38');expect(s.engine.restingOrderCount()).toBe(1);expect(s.engine.integrityFault()).toBe(false);}finally{await s.engine.stop();}});

 it('does not put pre-activation queue growth behind our quote',async()=>{const s=await setup();try{const o=s.quote();await s.submit(o);s.clock.ms+=100;await s.market.emitSnapshot({...s.b('UP',2),bids:[{price:'0.69',quantity:'10'}]});s.clock.ms+=51;await s.intern.onMakerTrade(s.trade(o));expect(s.engine.ledger.positions.size).toBe(0);expect(s.engine.restingOrdersForTest().get(o.id)?.queueAhead).toBe('7');s.clock.ms++;await s.intern.onMakerTrade(s.trade(o,'8'));expect(s.engine.ledger.positions.get(M)?.upQuantity).toBe('0.5');}finally{await s.engine.stop();}});
 it('freezes valid maker execution B before a held durable identity read; later C cannot reprice that fill',async()=>{const s=await setup();let release!:()=>void;try{const o=s.quote();await s.submit(o);s.clock.ms+=151;const read=s.journal.readDurableByOperationKey.bind(s.journal);let reached!:()=>void;const entered=new Promise<void>(r=>reached=r),hold=new Promise<void>(r=>release=r);let once=false;s.journal.readDurableByOperationKey=async key=>{if(!once&&key.startsWith('fill:maker-fill:')){once=true;reached();await hold;}return read(key);};const pending=s.intern.onMakerTrade(s.trade(o));await entered;s.engine.state.marketSnapshots.set(U,{...s.b('UP',3),asks:[{price:'0.68',quantity:'1000'}]});release();await pending;expect(s.engine.ledger.positions.get(M)?.upQuantity).toBe('0.5');expect(s.engine.integrityFault()).toBe(false);}finally{release?.();await s.engine.stop();}});
 it.each(['BEFORE_DURABLE_COMMIT','AFTER_DURABLE_COMMIT_BEFORE_APPLY','AFTER_APPLY_BEFORE_DERIVED_AUDIT'] as const)('reservation crash %s recovers capital and exactly one terminal result',async point=>{let s=await setup();try{const o=s.quote();s.engine.setCrashInjectionForTest(point,{operationKey:`maker:${o.id}:reserve`});await expect(s.submit(o)).rejects.toThrow('CRASH_INJECTED');await s.engine.stop();s=await setup(true,s.journal,s.clock);expect(s.engine.restingOrderCount()).toBe(0);expect(s.engine.reservedCash()).toBe('0');expect(s.engine.ledger.positions.size).toBe(0);const terminal=(await s.journal.queryEvents({types:['PAPER_ORDER_TERMINAL']})).filter(r=>r.payload.orderId===o.id);expect(terminal).toHaveLength(1);expect(terminal[0]?.payload.status).toBe(point==='BEFORE_DURABLE_COMMIT'?'REJECTED':'CANCELLED');}finally{await s.engine.stop();}});
 it.each(['BEFORE_DURABLE_COMMIT','AFTER_DURABLE_COMMIT_BEFORE_APPLY','AFTER_APPLY_BEFORE_DERIVED_AUDIT'] as const)('partial fill crash %s recovers exactly the committed quantity and cancels only its remainder',async point=>{let s=await setup();try{const o=s.quote();await s.submit(o);s.clock.ms+=151;const trade=s.trade(o);s.engine.setCrashInjectionForTest(point,{operationKey:`fill:maker-fill:${o.id}:${trade.eventId}`});await expect(s.intern.onMakerTrade(trade)).rejects.toThrow('CRASH_INJECTED');await s.engine.stop();s=await setup(true,s.journal,s.clock);expect(s.engine.restingOrderCount()).toBe(0);expect(s.engine.reservedCash()).toBe('0');expect(s.engine.ledger.positions.get(M)?.upQuantity??'0').toBe(point==='BEFORE_DURABLE_COMMIT'?'0':'0.5');const fills=(await s.journal.queryEvents({types:['PAPER_FILL']})).filter(r=>r.payload.orderId===o.id);expect(fills).toHaveLength(point==='BEFORE_DURABLE_COMMIT'?0:1);const terminal=(await s.journal.queryEvents({types:['PAPER_ORDER_TERMINAL']})).filter(r=>r.payload.orderId===o.id);expect(terminal).toHaveLength(1);expect(s.engine.integrityFault()).toBe(false);}finally{await s.engine.stop();}});

 it('serializes simultaneous public trades without resurrecting remaining quantity',async()=>{const s=await setup();try{const o=s.quote();await s.submit(o);s.clock.ms+=152;await Promise.all([s.intern.onMakerTrade(s.trade(o,'3',T+151)),s.intern.onMakerTrade(s.trade(o,'3',T+152))]);expect(s.engine.ledger.positions.get(M)?.upQuantity).toBe('2');expect(s.engine.restingOrderCount()).toBe(0);expect(s.engine.reservedCash()).toBe('0');}finally{await s.engine.stop();}});
 it('durable cancel wins over a held in-flight trade fill and duplicate cancel is harmless',async()=>{const s=await setup();let release!:()=>void;try{const o=s.quote();await s.submit(o);s.clock.ms+=151;const read=s.journal.readDurableByOperationKey.bind(s.journal);let reached!:()=>void;const entered=new Promise<void>(r=>reached=r),hold=new Promise<void>(r=>release=r);let once=false;s.journal.readDurableByOperationKey=async key=>{if(!once&&key.startsWith('fill:maker-fill:')){once=true;reached();await hold;}return read(key);};const pending=s.intern.onMakerTrade(s.trade(o));await entered;await s.engine.cancelMakerForTest(o.id);release();await pending;await s.engine.cancelMakerForTest(o.id);expect(s.engine.ledger.positions.size).toBe(0);expect(s.engine.reservedCash()).toBe('0');expect(s.engine.integrityFault()).toBe(false);}finally{release?.();await s.engine.stop();}});
 it('Profit-Trailing permits one bounded maker hedge then executes the unchanged taker fallback',async()=>{const s=await setup(true,undefined,{ms:T},{PROFIT_TRAILING_PAIR_SOAK_ENABLED:'true',NORMAL_HEDGE_MIN_LOCKED_PAIR_EDGE:'0.12'});try{await s.engine.applyObservedFill(paperFill(M,'UP','0.7',{tokenId:U,quantity:'2',timestamp:new Date(T).toISOString()}));const opposite={...s.b('DOWN',2),bids:[{price:'0.15',quantity:'2'}],asks:[{price:'0.16',quantity:'1000'}]};await s.market.emitSnapshot(opposite);const wait=async(test:()=>boolean)=>{const until=Date.now()+2500;while(!test()){if(Date.now()>until)throw Error('HYBRID_FALLBACK_TIMEOUT:'+JSON.stringify(s.errors));await new Promise(r=>setTimeout(r,2));}};await wait(()=>s.engine.restingOrderCount()===1);expect([...s.engine.restingOrdersForTest().values()][0]?.order.makerFirst?.role).toBe('HEDGE');s.clock.ms+=800;await s.intern.expireMakerQuotes();await wait(()=>s.engine.ledger.positions.get(M)?.unpairedQuantity==='0'&&!(s.engine as unknown as {profitTrailingPairInFlight:Set<string>}).profitTrailingPairInFlight.has(M));const fills=(await s.journal.queryEvents({types:['PAPER_FILL']})).filter(r=>r.payload.intent==='HEDGE_SECOND_LEG');expect(fills).toHaveLength(1);expect(fills[0]?.payload.liquidityRole).toBe('TAKER');expect(s.engine.reservedCash()).toBe('0');expect(s.engine.integrityFault()).toBe(false);}finally{await s.engine.stop();}});

 it('expires an unfilled first leg and releases all capital with one CANCELLED terminal',async()=>{const s=await setup();try{expect(await s.submit()).toBe(true);expect(s.engine.ledger.snapshot().reservedCash).toBe('1.38');s.clock.ms+=2001;await s.intern.expireMakerQuotes();expect(s.engine.restingOrderCount()).toBe(0);expect(s.engine.ledger.snapshot().reservedCash).toBe('0');expect((await s.journal.queryEvents({types:['PAPER_ORDER_TERMINAL']}))[0]?.payload.status).toBe('CANCELLED');expect(s.engine.ledger.positions.size).toBe(0);}finally{await s.engine.stop();}});
 it('public trade partial fills reserve only the actual remainder, duplicate trade cannot fill twice',async()=>{const s=await setup();try{const o=s.quote();expect(await s.submit(o)).toBe(true);s.clock.ms+=151;const tr=s.trade(o);await s.intern.onMakerTrade(tr);expect(s.engine.ledger.positions.get(M)?.upQuantity).toBe('0.5');expect(s.engine.ledger.snapshot().reservedCash).toBe('1.035');await s.intern.onMakerTrade(tr);expect(s.engine.ledger.positions.get(M)?.upQuantity).toBe('0.5');s.clock.ms++;await s.intern.onMakerTrade(s.trade(o));expect(s.engine.ledger.positions.get(M)?.upQuantity).toBe('2');expect(s.engine.ledger.snapshot().reservedCash).toBe('0');expect((await s.journal.queryEvents({types:['PAPER_FILL']})).length).toBe(2);}finally{await s.engine.stop();}});
 it('does not count pre-ack/submission-latency trades or book cancellation as a fill',async()=>{const s=await setup();try{const o=s.quote();await s.submit(o);s.clock.ms+=100;await s.intern.onMakerTrade(s.trade(o,'100'));await s.market.emitSnapshot({...s.b('UP',2),bids:[]});expect(s.engine.ledger.positions.size).toBe(0);expect((await s.journal.queryEvents({types:['PAPER_FILL']})).length).toBe(0);}finally{await s.engine.stop();}});
 it('close-entries cancels first-leg remainders and fences delayed trade flow',async()=>{const s=await setup();try{const o=s.quote();await s.submit(o);await s.engine.closeExperimentEntries();s.clock.ms+=151;await s.intern.onMakerTrade(s.trade(o,'100'));expect(s.engine.ledger.positions.size).toBe(0);expect(s.engine.ledger.snapshot().reservedCash).toBe('0');expect(await s.submit(s.quote())).toBe(false);}finally{await s.engine.stop();}});
 it('a partial opposite maker hedge preserves the exact residual and cancellation releases only remainder',async()=>{const s=await setup();try{await s.engine.applyObservedFill(paperFill(M,'UP','0.7',{tokenId:U,quantity:'2',timestamp:new Date(T).toISOString()}));const o=s.quote('HEDGE_SECOND_LEG');expect(await s.submit(o,'HEDGE_SECOND_LEG')).toBe(true);s.clock.ms+=151;await s.intern.onMakerTrade(s.trade(o));expect(s.engine.ledger.positions.get(M)?.unpairedQuantity).toBe('1.5');expect(s.engine.ledger.positions.get(M)?.pairedQuantity).toBe('0.5');await s.engine.cancelMakerForTest(o.id);expect(s.engine.ledger.snapshot().reservedCash).toBe('0');expect(s.engine.ledger.positions.get(M)?.unpairedQuantity).toBe('1.5');}finally{await s.engine.stop();}});
 it('gap/reconnect never reuses the old queue and a changed epoch cancels the quote',async()=>{const s=await setup();try{const o=s.quote();await s.submit(o);s.market.markTokenSync(U,'GAPPED');s.market.markTokenSync(U,'SYNCED');s.clock.ms+=151;await s.market.emitSnapshot(s.b('UP',2));await s.intern.onMakerTrade(s.trade(o,'100'));expect(s.engine.restingOrderCount()).toBe(0);expect(s.engine.ledger.positions.size).toBe(0);expect(s.engine.ledger.snapshot().reservedCash).toBe('0');}finally{await s.engine.stop();}});
 it('checkpoint restart reconciles open quote and reservation without inventing missed fills',async()=>{const s=await setup();await s.submit();await s.engine.checkpoint();await s.engine.stop();const recovered=await setup(true,s.journal,s.clock);try{expect(recovered.engine.restingOrderCount()).toBe(0);expect(recovered.engine.ledger.snapshot().reservedCash).toBe('0');expect(recovered.engine.ledger.positions.size).toBe(0);}finally{await recovered.engine.stop();}});
 it('the first-leg admission lock blocks a second outcome and double cash reservation',async()=>{const s=await setup();try{expect(await s.submit()).toBe(true);const opposite={...s.quote(),id:'other-side',tokenId:D,outcome:'DOWN' as const,requestedPrice:'0.29'};expect(await s.submit(opposite)).toBe(false);expect(s.engine.restingOrderCount()).toBe(1);expect(s.engine.ledger.snapshot().reservedCash).toBe('1.38');}finally{await s.engine.stop();}});
 it.each(['UP','DOWN'] as const)('dual maker accepts %s as the first partial or full leg then pairs the opposite maker',async firstOutcome=>{
  const s=await setup(true,undefined,{ms:T},{PAPER_DUAL_SIDED_MAKER_ENABLED:'true'});
  try{
    const dual=s.engine as unknown as {trySubmitDualMakerPair(o:PaperOrder,key:string,p:Record<'UP'|'DOWN',MarketSnapshot>):Promise<boolean>;restingOrders:Map<string,{order:PaperOrder;remaining:string}>};
    const books={UP:s.engine.state.marketSnapshots.get(U)!,DOWN:s.engine.state.marketSnapshots.get(D)!};
    const admitted=await dual.trySubmitDualMakerPair(s.quote(),'dual:test',books);
    expect(admitted).toBe(true);
    expect(s.engine.restingOrderCount()).toBe(2);
    const q=[...dual.restingOrders.values()].map(v=>v.order);
    expect(new Set(q.map(o=>o.makerFirst?.dualGroupId)).size).toBe(1);
    expect(new Set(q.map(o=>o.outcome))).toEqual(new Set(['UP','DOWN']));
    s.clock.ms+=201;
    const first=q.find(x=>x.outcome===firstOutcome)!;
    await s.intern.onMakerTrade(s.trade(first,'6',s.clock.ms));
    const p=s.engine.ledger.positions.get(M);
    expect(p?.unpairedOutcome).toBe(firstOutcome);
    expect(p?.unpairedQuantity).toBe('2');
    expect(s.engine.restingOrderCount()).toBe(1);
    s.clock.ms+=10;
    const sibling=q.find(x=>x.outcome!==firstOutcome)!;
    await s.intern.onMakerTrade(s.trade(sibling,'6',s.clock.ms));
    const completed=s.engine.ledger.positions.get(M);
    expect(completed?.pairedQuantity).toBe('2');
    expect(completed?.unpairedQuantity).toBe('0');
    expect(s.engine.restingOrderCount()).toBe(0);
    expect(s.engine.ledger.snapshot().reservedCash).toBe('0');
    expect(s.errors).toEqual([]);
  }finally{await s.engine.stop();}
 });
 it('serializes simultaneous dual-side public trades through one market lane',async()=>{
  const s=await setup(true,undefined,{ms:T},{PAPER_DUAL_SIDED_MAKER_ENABLED:'true'});
  try{
    const dual=s.engine as unknown as {trySubmitDualMakerPair(o:PaperOrder,key:string,p:Record<'UP'|'DOWN',MarketSnapshot>):Promise<boolean>;restingOrders:Map<string,{order:PaperOrder}>};
    expect(await dual.trySubmitDualMakerPair(s.quote(),'dual:simultaneous',{
      UP:s.engine.state.marketSnapshots.get(U)!,DOWN:s.engine.state.marketSnapshots.get(D)!})).toBe(true);
    const q=[...dual.restingOrders.values()].map(x=>x.order);
    s.clock.ms+=201;
    await Promise.all(q.map(o=>s.intern.onMakerTrade(s.trade(o,'6',s.clock.ms))));
    expect(s.engine.ledger.positions.get(M)?.pairedQuantity).toBe('2');
    expect(s.engine.ledger.positions.get(M)?.unpairedQuantity).toBe('0');
    expect(s.engine.ledger.snapshot().reservedCash).toBe('0');
    expect(s.errors).toEqual([]);
  }finally{await s.engine.stop();}
 });
 it('dual-sided startup recovery cancels both quotes and releases both durable reservations',async()=>{
  const s=await setup(true,undefined,{ms:T},{PAPER_DUAL_SIDED_MAKER_ENABLED:'true'});
  const dual=s.engine as unknown as {trySubmitDualMakerPair(o:PaperOrder,key:string,p:Record<'UP'|'DOWN',MarketSnapshot>):Promise<boolean>};
  expect(await dual.trySubmitDualMakerPair(s.quote(),'dual:restart',{
    UP:s.engine.state.marketSnapshots.get(U)!,DOWN:s.engine.state.marketSnapshots.get(D)!})).toBe(true);
  expect(s.engine.restingOrderCount()).toBe(2);
  await s.engine.checkpoint();
  await s.engine.stop();
  const recovered=await setup(true,s.journal,s.clock,{PAPER_DUAL_SIDED_MAKER_ENABLED:'true'});
  try{
    expect(recovered.engine.restingOrderCount()).toBe(0);
    expect(recovered.engine.ledger.snapshot().reservedCash).toBe('0');
    expect(recovered.engine.ledger.positions.size).toBe(0);
    expect(recovered.engine.integrityFault()).toBe(false);
  }finally{await recovered.engine.stop();}
 });
 it('dual fresh quotes both cancel on close-entries without retaining cash or creating entries',async()=>{
  const s=await setup(true,undefined,{ms:T},{PAPER_DUAL_SIDED_MAKER_ENABLED:'true'});
  try{
    const dual=s.engine as unknown as {trySubmitDualMakerPair(o:PaperOrder,k:string,p:Record<'UP'|'DOWN',MarketSnapshot>):Promise<boolean>;restingOrders:Map<string,{order:PaperOrder}>};
    expect(await dual.trySubmitDualMakerPair(s.quote(),'dual:close',{
      UP:s.engine.state.marketSnapshots.get(U)!,DOWN:s.engine.state.marketSnapshots.get(D)!})).toBe(true);
    const quotes=[...dual.restingOrders.values()].map(r=>r.order);
    expect(quotes).toHaveLength(2);
    await s.engine.closeExperimentEntries();
    expect(s.engine.restingOrderCount()).toBe(0);
    expect(s.engine.reservedCash()).toBe('0');
    s.clock.ms+=220;
    await Promise.all(quotes.map(q=>s.intern.onMakerTrade(s.trade(q,'20',s.clock.ms))));
    expect(s.engine.ledger.positions.size).toBe(0);
    expect(s.errors).toEqual([]);
  }finally{await s.engine.stop();}
 });
 it('dual first fill retains the opposite hedge across entry close while refusing new first legs',async()=>{
  const s=await setup(true,undefined,{ms:T},{PAPER_DUAL_SIDED_MAKER_ENABLED:'true'});
  try{
    const dual=s.engine as unknown as {trySubmitDualMakerPair(o:PaperOrder,k:string,p:Record<'UP'|'DOWN',MarketSnapshot>):Promise<boolean>;restingOrders:Map<string,{order:PaperOrder}>};
    expect(await dual.trySubmitDualMakerPair(s.quote(),'dual:close-with-residual',{
      UP:s.engine.state.marketSnapshots.get(U)!,DOWN:s.engine.state.marketSnapshots.get(D)!})).toBe(true);
    const quotes=[...dual.restingOrders.values()].map(r=>r.order);
    const up=quotes.find(q=>q.outcome==='UP')!,down=quotes.find(q=>q.outcome==='DOWN')!;
    s.clock.ms+=201;await s.intern.onMakerTrade(s.trade(up,'6',s.clock.ms));
    expect(s.engine.ledger.positions.get(M)?.unpairedOutcome).toBe('UP');
    await s.engine.closeExperimentEntries();
    expect(s.engine.restingOrderCount()).toBe(1);
    s.clock.ms+=10;await s.intern.onMakerTrade(s.trade(down,'6',s.clock.ms));
    expect(s.engine.ledger.positions.get(M)?.unpairedQuantity).toBe('0');
    expect(s.engine.ledger.positions.get(M)?.pairedQuantity).toBe('2');
    expect(s.engine.reservedCash()).toBe('0');
    expect(s.errors).toEqual([]);
  }finally{await s.engine.stop();}
 });
 it.each(['UP','DOWN'] as const)('dual first partial %s caps the opposite fill to the actual residual',async outcome=>{
  const s=await setup(true,undefined,{ms:T},{PAPER_DUAL_SIDED_MAKER_ENABLED:'true'});
  try{
   const dual=s.engine as unknown as {trySubmitDualMakerPair(o:PaperOrder,k:string,p:Record<'UP'|'DOWN',MarketSnapshot>):Promise<boolean>};
   expect(await dual.trySubmitDualMakerPair(s.quote(),'dual:partial',{UP:s.engine.state.marketSnapshots.get(U)!,DOWN:s.engine.state.marketSnapshots.get(D)!})).toBe(true);
   const quotes=[...s.engine.restingOrdersForTest().values()].map(r=>r.order);
   s.clock.ms+=201;await s.intern.onMakerTrade(s.trade(quotes.find(o=>o.outcome===outcome)!,'3'));
   expect(s.engine.ledger.positions.get(M)?.unpairedQuantity).toBe('0.5');
   s.clock.ms++;await s.intern.onMakerTrade(s.trade(quotes.find(o=>o.outcome!==outcome)!,'100'));
   expect(s.engine.ledger.positions.get(M)?.pairedQuantity).toBe('0.5');
   expect(s.engine.ledger.positions.get(M)?.unpairedQuantity).toBe('0');
   expect(s.engine.restingOrderCount()).toBe(0);expect(s.engine.reservedCash()).toBe('0');
   const fills=await s.journal.queryEvents({types:['PAPER_FILL']});expect(fills).toHaveLength(2);
   expect(fills.map(x=>x.payload.quantity)).toEqual(['0.5','0.5']);expect(s.errors).toEqual([]);
  }finally{await s.engine.stop();}
 });
 it('dual creation and both terminal fills are experiment-scoped with durable group admission',async()=>{
  const s=await setup(true,undefined,{ms:T},{PAPER_DUAL_SIDED_MAKER_ENABLED:'true'});
  try{
   const dual=s.engine as unknown as {trySubmitDualMakerPair(o:PaperOrder,k:string,p:Record<'UP'|'DOWN',MarketSnapshot>):Promise<boolean>};
   expect(await dual.trySubmitDualMakerPair(s.quote(),'dual:scope',{UP:s.engine.state.marketSnapshots.get(U)!,DOWN:s.engine.state.marketSnapshots.get(D)!})).toBe(true);
   const id=s.engine.experimentManifest()!.id;
   for(const fact of await s.journal.queryDurableTransitions({types:['PAPER_MAKER_RESERVED'],limit:100}))
    expect(hashDurablePayload(JSON.parse(JSON.stringify(fact.payload)))).toBe(fact.payloadHash);
   expect(await s.journal.queryEvents({types:['PAPER_ORDER_CREATED'],experimentId:id})).toHaveLength(2);
   expect(await s.journal.queryEvents({types:['DUAL_MAKER_QUOTES_ADMITTED'],experimentId:id})).toHaveLength(1);
   const quotes=[...s.engine.restingOrdersForTest().values()].map(r=>r.order);s.clock.ms+=201;
   for(const o of quotes){await s.intern.onMakerTrade(s.trade(o,'6'));s.clock.ms++;}
   expect(await s.journal.queryEvents({types:['PAPER_ORDER_TERMINAL'],experimentId:id})).toHaveLength(2);
   expect(s.engine.integrityFault()).toBe(false);
  }finally{await s.engine.stop();}
 });
 it('close-entries during the second reservation cannot leave a new dual quote or reserved cash',async()=>{
  const s=await setup(true,undefined,{ms:T},{PAPER_DUAL_SIDED_MAKER_ENABLED:'true'});let release!:()=>void;
  try{
   const dual=s.engine as unknown as {trySubmitDualMakerPair(o:PaperOrder,k:string,p:Record<'UP'|'DOWN',MarketSnapshot>):Promise<boolean>};
   const read=s.journal.readDurableByOperationKey.bind(s.journal);let calls=0,reached!:()=>void;
   const entered=new Promise<void>(r=>reached=r),hold=new Promise<void>(r=>release=r);
   s.journal.readDurableByOperationKey=async key=>{if(key.startsWith('maker:')&&key.endsWith(':reserve')&&++calls===4){reached();await hold;}return read(key);};
   const pending=dual.trySubmitDualMakerPair(s.quote(),'dual:close-race',{UP:s.engine.state.marketSnapshots.get(U)!,DOWN:s.engine.state.marketSnapshots.get(D)!});
   await entered;await s.engine.closeExperimentEntries();release();await pending;
   expect(s.engine.restingOrderCount()).toBe(0);expect(s.engine.reservedCash()).toBe('0');
   const closed=(await s.journal.queryDurableTransitions({types:['EXPERIMENT_ENTRY_CLOSED'],limit:100}))[0]!;
   const reserved=await s.journal.queryDurableTransitions({types:['PAPER_MAKER_RESERVED'],limit:100});
   expect(reserved.filter(x=>x.sequence>closed.sequence)).toHaveLength(0);
   expect(s.engine.integrityFault()).toBe(false);
  }finally{release?.();await s.engine.stop();}
 });
 it('a first fill before sibling reservation cancels that plan without another first leg or fatal error',async()=>{
  const s=await setup(true,undefined,{ms:T},{PAPER_DUAL_SIDED_MAKER_ENABLED:'true'});
  try{
   const dual=s.engine as unknown as {trySubmitDualMakerPair(o:PaperOrder,k:string,p:Record<'UP'|'DOWN',MarketSnapshot>):Promise<boolean>};
   const read=s.journal.readDurableByOperationKey.bind(s.journal);let calls=0;
   s.journal.readDurableByOperationKey=async key=>{
    if(key.startsWith('maker:')&&key.endsWith(':reserve')&&++calls===4){
     const first=[...s.engine.restingOrdersForTest().values()][0]!.order;s.clock.ms+=201;
     await s.intern.onMakerTrade(s.trade(first,'6'));
    }return read(key);
   };
   expect(await dual.trySubmitDualMakerPair(s.quote(),'dual:early-fill',{UP:s.engine.state.marketSnapshots.get(U)!,DOWN:s.engine.state.marketSnapshots.get(D)!})).toBe(true);
   expect(s.engine.ledger.positions.get(M)?.unpairedQuantity).toBe('2');
   expect(s.engine.restingOrderCount()).toBe(0);expect(s.engine.reservedCash()).toBe('0');
   expect(s.engine.runtimeBookkeepingForTest().executionErrors).toBe(0);expect(s.engine.integrityFault()).toBe(false);
  }finally{await s.engine.stop();}
 });
 it.each([false,true])('feature enabled=%s preserves original gates and selects only the authorized execution style',async enabled=>{const s=await setup(enabled);try{const c:StrategyCandidate={strategyId:'test',strategyVersion:'test',marketId:M,tokenId:U,outcome:'UP',side:'BUY',quantity:'2',limitPrice:'0.7',intent:'OPEN_FIRST_LEG',rawEdgeUsdPerShare:.2,rawExpectedEdge:.2,reasonCodes:[]};const decision=s.engine.decideWithRuntimeBasisForTest(c,{UP:s.engine.state.marketSnapshots.get(U)!,DOWN:s.engine.state.marketSnapshots.get(D)!},new Date(s.clock.ms),'XRP');expect(decision.accepted,JSON.stringify(decision.risk)).toBe(true);expect(decision.order?.orderType).toBe(enabled?'MAKER':'TAKER');if(enabled)expect(decision.order?.makerFirst?.postOnly).toBe(true);}finally{await s.engine.stop();}});
});

import {readFileSync} from 'node:fs';
import {describe,it,expect} from 'vitest';
import {decidePaperOrder,riskLimitsSchema} from '@pm/core';
import {snapshot,safeRisk,now} from './fixtures.js';
const cases=JSON.parse(readFileSync(new URL('./fixtures/economic-envelope-python.json',import.meta.url),'utf8')) as Array<{price:string;quantity:string;fair:string;modes:Record<string,{entryFee:string;exitFee:string;expectedROI:string}>}>;
describe('independent Python Decimal economic envelope matches TypeScript',()=>{
 for(const role of ['MAKER','TAKER'] as const)for(const mode of ['maximum','fair_value'] as const){
  it.each(cases)(`${role}/${mode} exact fee and ROI at $price, fair $fair`,c=>{
   const p=Number(c.price),f=Number(c.fair),reference=c.modes[role+':'+mode]!;
   const b=snapshot({bids:[{price:role==='MAKER'?c.price:String(p-.01),quantity:'10000'}],asks:[{price:role==='TAKER'?c.price:String(p+.01),quantity:'10000'}]});
   const decision=decidePaperOrder({candidate:{strategyId:'envelope',strategyVersion:'test',marketId:b.marketId,tokenId:b.tokenId,outcome:'UP',side:'BUY',quantity:c.quantity,limitPrice:c.price,intent:'OPEN_FIRST_LEG',rawExpectedEdge:f-p,reasonCodes:[]},
    consensus:{id:'c',traceId:'t',triggerId:'g',consensusDirection:'UP',consensusConfidence:.9,agentAgreement:1,signalFreshness:1,conflicts:[],estimatedEdge:f-p,evidenceIds:[],calculatedAt:now.toISOString(),fairProbability:f},
    snapshotAfterDecisionLatency:b,baseRisk:safeRisk({intent:'OPEN_FIRST_LEG'}),limits:riskLimitsSchema.parse({freshEntryNetRoiFloor:.01,freshEntryExitFeeReserve:true,freshEntryExitFeeMode:mode}),triggerId:'g',signalExpiresAt:new Date(now.getTime()+60000),orderType:role});
   expect(Number(decision.feeUsdTotal)).toBe(Number(reference.entryFee));
   expect(Number(decision.exitFeeReserveUsd)).toBe(Number(reference.exitFee));
   expect(decision.expectedROI).toBeCloseTo(Number(reference.expectedROI),11);
   expect(decision.risk.vetoes.includes('MINIMUM_EXPECTED_ROI')).toBe(Number(reference.expectedROI)<.01);
  });
 }
});

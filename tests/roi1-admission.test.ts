import { describe, it, expect } from "vitest";
import { evaluateRisk, riskLimitsSchema, engineEnvSchema, type MarketTrade, costBoundMakerBuyPrice, singleFillHedgeEconomics } from "@pm/core";
import { safeRisk, snapshot, now, order } from "./fixtures.js";
import { firstLegEntryFilterReason, firstLegExecutionEconomics } from "../apps/engine/src/first-leg-admission.js";
import { MakerTouchFlow } from "../apps/engine/src/maker-touch-flow.js";

const policy={minNetRoi:0.01,minPrice:0};
const limits=riskLimitsSchema.parse({freshEntryNetRoiFloor:policy.minNetRoi,freshEntryMinPrice:policy.minPrice});
describe("explicit ROI1 entry policy",()=>{
  it.each([0.29,0.6499,0.65,0.89999,0.9,0.97])("uses one decimal net return floor at price %s",price=>{
    const input=safeRisk({intent:'OPEN_FIRST_LEG',executablePrice:String(price),expectedROI:0.01,expectedEdge:price*0.01});
    expect(evaluateRisk(input,limits).vetoes).toEqual([]);
    expect(firstLegEntryFilterReason(price,0.01,policy)).toBe(null);
    expect(evaluateRisk({...input,expectedROI:0.009999},limits).vetoes).toContain('MINIMUM_EXPECTED_ROI');
  });
  it("does not change hedge economics or bypass fee, cash, start/entry and risk fences",()=>{
    expect(evaluateRisk(safeRisk({intent:'HEDGE_SECOND_LEG',expectedROI:0.01,expectedEdge:0.009}),limits).vetoes).toContain('MINIMUM_EXPECTED_EDGE');
    const r=evaluateRisk(safeRisk({intent:'OPEN_FIRST_LEG',expectedROI:0.01,expectedEdge:0.005,
      feeScheduleKnown:false,availableCashUsd:'0',experimentEntryOpen:false,dailyPnlUsd:'-51',signalConfidence:0.59}),limits);
    for(const veto of ['UNKNOWN_FEE_SCHEDULE','INSUFFICIENT_CASH','EXPERIMENT_ENTRY_CLOSED','DAILY_LOSS_LIMIT','MINIMUM_SIGNAL_CONFIDENCE'])expect(r.vetoes).toContain(veto);
  });
  it("keeps pair risk independent of directional confidence and ROI but retains every hard veto",()=>{
    const input=safeRisk({intent:'OPEN_FIRST_LEG',expectedROI:-0.9,expectedEdge:-0.1,signalConfidence:0,
      referenceRequired:true,referenceFeedAt:null,dualPairEconomics:{netRoi:0.01,netUsdPerShare:0.009}});
    expect(evaluateRisk(input,limits).vetoes).toEqual([]);
    const r=evaluateRisk({...input,feeScheduleKnown:false,databaseAvailable:false,projectedDirectionalExposureUsd:'100',availableCashUsd:'0'},limits);
    for(const veto of ['UNKNOWN_FEE_SCHEDULE','DATABASE_UNAVAILABLE','MAX_DIRECTIONAL_EXPOSURE','INSUFFICIENT_CASH'])expect(r.vetoes).toContain(veto);
    expect(evaluateRisk({...input,dualPairEconomics:{netRoi:0.00999,netUsdPerShare:0.009}},limits).vetoes).toContain('MINIMUM_EXPECTED_ROI');
  });
  it("revalidates actual maker and taker fees at execution with the explicit policy",()=>{
    const book=snapshot({bids:[{price:'0.5',quantity:'1000'}],asks:[{price:'0.5',quantity:'1000'}],feeSchedule:{...snapshot().feeSchedule!,rate:'0'},sourceTimestamp:now.toISOString(),receivedAt:now.toISOString()});
    const base=order('TAKER',{requestedPrice:'0.5',entryAdmission:{fairProbability:0.505,signalExpiresAt:new Date(now.getTime()+60000).toISOString()}});
    expect(firstLegExecutionEconomics(base,book,now.getTime(),policy).reason).toBe(null);
    expect(firstLegExecutionEconomics(base,book,now.getTime()).reason).toBe('FIRST_LEG_PRICE_BELOW_FLOOR');
    const maker={...base,orderType:'MAKER' as const,makerFirst:{version:1 as const,role:'FIRST_LEG' as const,postOnly:true as const,expiresAt:new Date(now.getTime()+2000).toISOString(),submissionLatencyMs:150,tokenEpoch:1,postingBookEventId:'b',takerReferencePrice:'0.51'}};
    expect(firstLegExecutionEconomics(maker,{...book,asks:[{price:'0.51',quantity:'1000'}]},now.getTime(),policy).reason).toBe(null);
    expect(firstLegExecutionEconomics(base,{...book,feeSchedule:null},now.getTime(),policy).reason).toBe('ENTRY_ECONOMICS_UNAVAILABLE');
  });
  it("uses the quoted token tick, and never assumes an unknown hedge fee is zero",()=>{
    const up=snapshot({tickSize:'0.01',bids:[{price:'0.5',quantity:'1000'}],asks:[{price:'0.55',quantity:'1000'}]});
    const down=snapshot({tickSize:'0.001',outcome:'DOWN',tokenId:'down',asks:[{price:'0.43',quantity:'1000'}]});
    const result=costBoundMakerBuyPrice({book:up,opposite:down,quantity:'5',minPairNetRoi:0.01});
    expect('price' in result).toBe(true); if('price' in result)expect(Number(result.price)*100).toBeCloseTo(Math.round(Number(result.price)*100),8);
    expect(singleFillHedgeEconomics({book:up,opposite:{...down,feeSchedule:null},quantity:'5',price:'0.5',makerFeeUsd:'0',minPairNetRoi:0.01}).covered).toBe(false);
  });
  it("seals fractional ROI in env; 1 remains 100 percent",()=>{
    expect(engineEnvSchema.parse({PAPER_ENTRY_NET_ROI_FLOOR:'0.01'}).PAPER_ENTRY_NET_ROI_FLOOR).toBe(0.01);
    expect(firstLegEntryFilterReason('0.7',0.01,{minPrice:0,minNetRoi:1})).toBe('FIRST_LEG_NET_ROI_BELOW_FLOOR');
  });
});

describe("SELL touch observations",()=>{
  const t=now.getTime(), flow=()=>new MakerTouchFlow(30000);
  const trade=(id:string,ts=t,extra:Partial<MarketTrade>={}):MarketTrade=>({eventId:id,marketId:'m',tokenId:'u',outcome:'UP',side:'SELL',price:'0.5',quantity:'3',sourceTimestamp:new Date(ts).toISOString(),receivedAt:new Date(ts).toISOString(),tokenEpoch:1,transactionHash:id,...extra});
  it("counts SELL only, deduplicates, accepts out-of-order in-window events and expires at 30s",()=>{
    const f=flow(); f.record(trade('buy',t,{side:'BUY'}),t,1,true);
    f.record(trade('a'),t+1,1,true); f.record(trade('a'),t+1,1,true);
    f.record(trade('older',t-1),t+1,1,true);
    expect(f.at('u','0.5',t+1,1)).toEqual({count:2,qty:'6'});
    expect(f.at('u','0.5',t+30000,1)).toEqual({count:1,qty:'3'});
    expect(f.at('u','0.5',t+30001,1)).toEqual({count:0,qty:'0'});
    expect(f.at('u','0.49',t+1,1)).toEqual({count:0,qty:'0'});
  });
  it("rejects future/invalid/old timestamps, wrong epochs, gaps and nonpositive volume",()=>{
    const f=flow();
    for(const extra of [{sourceTimestamp:'bad'},{sourceTimestamp:new Date(t+1).toISOString()},{receivedAt:new Date(t+1).toISOString()},{tokenEpoch:0},{quantity:'0'}])f.record(trade(JSON.stringify(extra),t,extra),t,1,true);
    f.record(trade('stale',t-30001),t,1,true);f.record(trade('gap'),t,1,false);
    expect(f.at('u','0.5',t,1).count).toBe(0);
    f.record(trade('valid'),t,1,true);expect(f.at('u','0.5',t,2).count).toBe(0);
    f.record(trade('epoch2',t,{tokenEpoch:2}),t,2,true);expect(f.at('u','0.5',t,2).count).toBe(1);
    f.retain(new Set());expect(f.at('u','0.5',t,2).count).toBe(0);
  });
});

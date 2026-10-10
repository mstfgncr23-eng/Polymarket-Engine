import { describe, expect, it } from "vitest";
import { createIdleExperiment, PaperLedger } from "@pm/core";
import {
  CHECKPOINT_SCHEMA_VERSION, hashCheckpointPayload,
  hashDurablePayload, type DurableTransition,
} from "@pm/db";
import {
  bootstrapGuardianOfflineSource, advanceGuardianOfflineSource,
  GuardianOfflineSourceRejected,
} from "../apps/engine/src/guardian-offline-durable-source.js";
import { plannedDurableTransition } from "../apps/engine/src/recovery.js";
import { emptyResultAccounting } from "../apps/engine/src/trading-results.js";
import { paperFill } from "./paper-engine-testkit.js";

const AT="2026-10-10T10:00:00.000Z";
const AT2="2026-10-10T10:00:05.000Z";
const AT3="2026-10-10T10:00:07.000Z";
const MARKET="g2f:btc";
const POS=`pos:${MARKET}`;
const experiment = () => ({
  ...createIdleExperiment({durationHours:1,strategyVersion:"test",riskConfiguration:{},
    modelConfiguration:{},startingBalance:"1000",systemVersion:"test",gitSha:"test"}),
  id:"g2f-experiment",status:"RUNNING" as const,startedAt:AT,
});
const row=(type:string,sequence:number,payload:Record<string,unknown>,key:string):DurableTransition=>{
  const base=plannedDurableTransition({
    operationKey:key,type,aggregateType:type==="PAPER_FILL"?"paper_fill":"position",
    aggregateId:type==="PAPER_FILL"?String((payload.fill as {id:string}).id):MARKET,payload,
  },new Date(sequence===21?AT2:AT3),sequence);
  return {...base,payloadHash:hashDurablePayload(payload)};
};
const first=() => paperFill(MARKET,"UP","0.40",{id:"g2f-buy",quantity:"5",timestamp:AT});
const makePayload=()=>{
  const ledger=new PaperLedger("1000",new Date(AT));
  ledger.applyFill(first(),{positionId:POS});
  return {
    schemaVersion:CHECKPOINT_SCHEMA_VERSION,durableCursor:20,
    experiment:experiment(),ledger:ledger.snapshot(),positions:[...ledger.positions.values()],
    resultAccounting:emptyResultAccounting(),restingOrders:[],makerReservations:{},
    activeTriggers:[],grokSignals:[],settledMarkets:[],riskReductionOwners:[],
    riskReductionFailures:[],reversalProtectionStates:[],profitTrailingPairStates:[],
    lastInfrastructureAccrualAt:0,
  };
};
const boot=(payload:Record<string,unknown>=makePayload())=>
  bootstrapGuardianOfflineSource({
    payload,externallyVerifiedContentHash:hashCheckpointPayload(payload),
    authorityId:"test-single-writer-epoch",
  });
const sell=()=>paperFill(MARKET,"UP","0.52",{id:"g2f-partial",quantity:"2",side:"SELL",timestamp:AT2});
const fillRow=(seq=21)=>{
  const fill=sell();
  return row("PAPER_FILL",seq,{fill,positionId:POS,reservationAfter:null},
    `fill:${fill.id}`);
};
const settleRow=(seq=22) => row("POSITION_SETTLED",seq,{
  marketId:MARKET,upPayoff:"1",downPayoff:"0",settledAt:AT3,
},`settlement:${MARKET}:g2f-experiment`);
const fail=(action:()=>unknown,reason:string)=>
  expect(action).toThrowError(new GuardianOfflineSourceRejected(reason));

describe("G2F offline adapter: checkpoint + REAL durable replay logic, no shadow",()=>{
  it("G2F-01 bootstrap checks v2 content hash and reconstructs exact unhedged episode",()=>{
    const value=boot();
    expect(value.projection.cursor).toBe(20);
    expect(value.projection.open.get(MARKET)).toEqual({
      episode:{marketId:MARKET,positionId:POS,unhedgedStartedAt:AT,heldOutcome:"UP"},
      residualQty:"5",entryPrice:"0.4",
    });
  });
  it("G2F-02 partial real PAPER_FILL updates read-model and economic ledger in lockstep",()=>{
    const firstState=boot();
    const next=advanceGuardianOfflineSource(firstState,fillRow());
    expect(next.projection.cursor).toBe(21);
    expect(next.projection.open.get(MARKET)?.residualQty).toBe("3");
    expect(next.recovery.ledger.positions.get(MARKET)?.unpairedQuantity).toBe("3");
    expect(firstState.projection.open.get(MARKET)?.residualQty).toBe("5");
  });
  it("G2F-03 settlement closes the read-model and durable inventory",()=>{
    const afterSell=advanceGuardianOfflineSource(boot(),fillRow());
    const next=advanceGuardianOfflineSource(afterSell,settleRow());
    expect(next.projection.open.size).toBe(0);
    expect(next.projection.settledMarkets.has(MARKET)).toBe(true);
    expect(next.recovery.ledger.positions.has(MARKET)).toBe(false);
  });
  it("G2F-04 checkpoint tampering, fake hash, unmodeled ownership and old schema fail closed",()=>{
    const payload=makePayload();
    fail(()=>bootstrapGuardianOfflineSource({
      payload:{...payload,durableCursor:19},
      externallyVerifiedContentHash:hashCheckpointPayload(payload),authorityId:"writer",
    }),"CHECKPOINT_HASH_MISMATCH");
    fail(()=>bootstrapGuardianOfflineSource({
      payload,externallyVerifiedContentHash:"0".repeat(64),authorityId:"writer",
    }),"CHECKPOINT_HASH_MISMATCH");
    fail(()=>boot({...payload,schemaVersion:1}),"CHECKPOINT_UNSUPPORTED");
    fail(()=>boot({...payload,riskReductionOwners:[{fake:"pending"}]}),"CHECKPOINT_HAS_UNMODELED_AUTHORITY");
  });
  it("G2F-05 missing/out-of-order durable row cannot be converted to NOOP",()=>{
    fail(()=>advanceGuardianOfflineSource(boot(),fillRow(22)),"DURABLE_CURSOR_GAP");
    const next=advanceGuardianOfflineSource(boot(),fillRow(21));
    fail(()=>advanceGuardianOfflineSource(next,fillRow(21)),"DURABLE_CURSOR_GAP");
  });
  it("G2F-06 altered or compacted durable payload is rejected, not silently replayed",()=>{
    const orig=fillRow();
    fail(()=>advanceGuardianOfflineSource(boot(),{...orig,payloadHash:"f".repeat(64)}),"DURABLE_PAYLOAD_UNVERIFIED");
    fail(()=>advanceGuardianOfflineSource(boot(),{...orig,payload:{}}),"DURABLE_PAYLOAD_UNVERIFIED");
    fail(()=>advanceGuardianOfflineSource(boot(),{...orig,payload:{...orig.payload,positionId:"wrong"}}),"DURABLE_PAYLOAD_UNVERIFIED");
  });
  it("G2F-07 a real unrelated durable MTM row advances the global cursor without changing the episode",()=>{
    const src=row("LEDGER_MARK_TO_MARKET",21,{
      prices:{[MARKET]:{up:"0.45",down:"0.55"}},markedAt:AT2,
    },"mtm:g2f:21");
    const next=advanceGuardianOfflineSource(boot(),src);
    expect(next.projection.cursor).toBe(21);
    expect(next.projection.open.get(MARKET)?.residualQty).toBe("5");
    const sold=advanceGuardianOfflineSource(next,fillRow(22));
    expect(sold.projection.open.get(MARKET)?.residualQty).toBe("3");
  });
  it("G2F-08 duplicate durable operation keys at different sequences fail closed",()=>{
    const state=advanceGuardianOfflineSource(boot(),fillRow(21));
    fail(()=>advanceGuardianOfflineSource(state,fillRow(22)),"DUPLICATE_OPERATION_KEY");
  });
  it("G2F-08 unknown/conflicting source events or wrong settlement are rejected",()=>{
    const src=fillRow();
    fail(()=>advanceGuardianOfflineSource(boot(),{
      ...src,type:"REPORT_SOURCE_INTEGRITY_FAULT",
      payloadHash:hashDurablePayload(src.payload),
    }),"FRAME_INVALID_FRAME");
    const noInventory=boot({...makePayload(),positions:[]});
    fail(()=>advanceGuardianOfflineSource(noInventory,settleRow(21)),"SETTLEMENT_SOURCE_MISSING");
  });
});

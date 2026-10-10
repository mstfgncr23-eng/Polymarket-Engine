import { describe, expect, it } from "vitest";
import { createIdleExperiment, PaperLedger, GUARDIAN_V2_CONFIG,
  createNakedDirectionalDiagnostic,
  evaluateGuardianV2, evaluateProfitTrailingPair } from "@pm/core";
import { order } from "./fixtures.js";
import {
  CHECKPOINT_SCHEMA_VERSION, hashCheckpointPayload,
  hashDurablePayload, type DurableTransition,
} from "@pm/db";
import {
  bootstrapGuardianOfflineSource, advanceGuardianOfflineSource,
  GuardianOfflineSourceRejected, GUARDIAN_REPLAY_KEY_BUDGET,
} from "../apps/engine/src/guardian-offline-durable-source.js";
import { plannedDurableTransition } from "../apps/engine/src/recovery.js";
import { accountResultFill, emptyResultAccounting } from "../apps/engine/src/trading-results.js";
import { paperFill } from "./paper-engine-testkit.js";
import { pullGuardianDurablePage, type GuardianDurableReadSource } from "../apps/engine/src/guardian-readonly-durable-feed.js";
import { bootstrapGuardianPinnedCheckpoint, type GuardianCheckpointReadSource } from "../apps/engine/src/guardian-readonly-checkpoint.js";

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
  const resultAccounting=emptyResultAccounting();
  accountResultFill(resultAccounting,first(),null,ledger.positions.get(MARKET));
  return {
    schemaVersion:CHECKPOINT_SCHEMA_VERSION,durableCursor:20,
    experiment:experiment(),ledger:ledger.snapshot(),positions:[...ledger.positions.values()],
    resultAccounting,restingOrders:[],makerReservations:{},
    activeTriggers:[],grokSignals:[],settledMarkets:[],riskReductionOwners:[],
    riskReductionFailures:[],reversalProtectionStates:[],profitTrailingPairStates:[],
    settlementLookups:[],nakedDirectionalDiagnostics:[],
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
    fail(()=>boot({...payload,riskReductionOwners:[{fake:"pending"}]}),"CHECKPOINT_RECOVERY_INVALID");
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
    const noInventory=boot({...makePayload(),positions:[],ledger:new PaperLedger("1000",new Date(AT)).snapshot(),resultAccounting:emptyResultAccounting()});
    fail(()=>advanceGuardianOfflineSource(noInventory,settleRow(21)),"SETTLEMENT_SOURCE_MISSING");
  });
});

describe("G2G-1 full economic owner hydration, offline only",()=>{
  const owner=()=>{
    const pending=order("TAKER",{id:"g2g-owner",traceId:"g2g-trace",triggerId:"g2g-trigger",marketId:MARKET,tokenId:"up",
      outcome:"UP",side:"SELL",requestedPrice:"0.5",quantity:"5",createdAt:AT});
    return {marketId:MARKET,unhedgedStartedAt:AT,intent:"PROTECTIVE_DE_RISK",
      order:pending,admittedResidualQuantity:"5",decisionReceivedAt:AT,state:"ADMITTED"};
  };
  it("G2G-01 restores a valid pending taker and retains it through an unrelated MTM",()=>{
    const payload={...makePayload(),riskReductionOwners:[owner()]};
    const base=boot(payload);
    expect(base.recovery.riskReductionOwners?.get(MARKET)).toEqual(owner());
    const next=advanceGuardianOfflineSource(base,row("LEDGER_MARK_TO_MARKET",21,{
      prices:{[MARKET]:{up:"0.46",down:"0.54"}},markedAt:AT2,
    },"mtm:g2g:21"));
    expect(next.recovery.riskReductionOwners?.get(MARKET)).toEqual(owner());
    expect(next.projection.open.get(MARKET)?.residualQty).toBe("5");
  });
  it("G2G-02 committed execution failure releases pending ownership and records failure",()=>{
    const active=boot({...makePayload(),riskReductionOwners:[owner()]});
    const release=row("RISK_REDUCTION_FAILED",21,{
      ownership:owner(),reason:"INSUFFICIENT_DEPTH",executionFailure:true,
    },"risk:g2g-owner:failed");
    const after=advanceGuardianOfflineSource(active,release);
    expect(after.recovery.riskReductionOwners?.size).toBe(0);
    expect(after.recovery.riskReductionFailures?.get(MARKET)?.reason).toBe("INSUFFICIENT_DEPTH");
    expect(after.projection.open.get(MARKET)?.residualQty).toBe("5");
  });
  it("G2G-03 wrong episode, duplicate pending takers, and owner/failure collision fail closed",()=>{
    const own=owner();
    fail(()=>boot({...makePayload(),riskReductionOwners:[{...own,unhedgedStartedAt:AT2}]}),
      "OWNER_EPISODE_MISMATCH");
    fail(()=>boot({...makePayload(),riskReductionOwners:[own,own]}),
      "DUPLICATE_ECONOMIC_OWNER");
    fail(()=>boot({...makePayload(),riskReductionOwners:[own],
      riskReductionFailures:[{ownership:own,reason:"STALE"}]}),"FAILURE_EPISODE_MISMATCH");
  });
  it("G2G-04 restores a real reserved maker intent without losing reserved cash",()=>{
    const payload=makePayload();
    const ledger=new PaperLedger("1000",new Date(AT));
    ledger.restore(payload.ledger as ReturnType<PaperLedger["snapshot"]>,payload.positions as never);
    ledger.reserve("0.5",new Date(AT2));
    const maker=order("MAKER",{id:"g2g-maker",marketId:MARKET,tokenId:"down",
      outcome:"DOWN",side:"BUY",quantity:"1",requestedPrice:"0.5",createdAt:AT});
    const resting={order:{...maker,status:"RESTING"},queueAhead:"0",
      remaining:"1",observedAt:AT2,intent:"HEDGE_SECOND_LEG"};
    const hydrated=boot({...payload,ledger:ledger.snapshot(),
      restingOrders:[resting],makerReservations:{"g2g-maker":"0.5"}});
    expect(hydrated.recovery.restingOrders.get("g2g-maker")).toEqual(resting);
    expect(hydrated.recovery.makerReservations.get("g2g-maker")).toBe("0.5");
    expect(hydrated.recovery.ledger.snapshot().reservedCash).toBe("0.5");
    const cancelled=advanceGuardianOfflineSource(hydrated,row("PAPER_ORDER_CANCELLED",21,
      {orderId:"g2g-maker",cancel:true,reason:"TEST_CANCEL"},"maker:g2g-maker:cancel"));
    expect(cancelled.recovery.restingOrders.size).toBe(0);
    expect(cancelled.recovery.makerReservations.size).toBe(0);
    expect(cancelled.recovery.ledger.snapshot().reservedCash).toBe("0");
    expect(cancelled.projection.open.get(MARKET)?.residualQty).toBe("5");
    fail(()=>boot({...payload,ledger:ledger.snapshot(),
      restingOrders:[resting],makerReservations:{"g2g-maker":"0.4"}}),"CHECKPOINT_UNSUPPORTED");
    fail(()=>boot({...payload,ledger:ledger.snapshot(),
      restingOrders:[resting,resting],makerReservations:{"g2g-maker":"0.5"}}),"DUPLICATE_ECONOMIC_OWNER");
  });
  it("G2G-05 restores a prior execution failure without claiming an active owner",()=>{
    const prior=owner();
    const result=boot({...makePayload(),riskReductionFailures:[{
      ownership:prior,reason:"INSUFFICIENT_DEPTH",
    }]});
    expect(result.recovery.riskReductionOwners?.size).toBe(0);
    expect(result.recovery.riskReductionFailures?.get(MARKET)?.reason).toBe("INSUFFICIENT_DEPTH");
    fail(()=>boot({...makePayload(),riskReductionFailures:[{
      ownership:{...prior,unhedgedStartedAt:AT3},reason:"INVALID_EPISODE",
    }]}),"FAILURE_EPISODE_MISMATCH");
  });
});

describe("G2G-2 Guardian/trailing/settlement state hydration, read-only",()=>{
  function configured(){
    const base=makePayload();
    const risk={
      ...base.experiment.riskConfiguration,
      reversalProtectionEnabled:true,reversalProtectionWindowMs:180000,
      guardianVersion:2,guardianFormatGeneration:3,guardianV2Config:GUARDIAN_V2_CONFIG,
      profitTrailingPairSoakEnabled:true,profitTrailingPairTargetEdge:0.12,
      normalHedgeMinLockedPairEdge:0.12,profitTrailingPairArmEdge:0.05,
      profitTrailingPairRetraceEdge:0.02,profitTrailingPairExecutionStyle:"TAKER",
      nakedDirectionalSoakEnabled:false,
    };
    const episode={marketId:MARKET,positionId:POS,unhedgedStartedAt:AT,heldOutcome:"UP" as const};
    const guardian=evaluateGuardianV2({
      generation:"BAD_REGION_V3",state:null,episode,entryPrice:"0.4",
      now:new Date(AT),observation:{eventId:"g2g-observation",heldPrice:"0.4",bookPressure:"0.2"},
    }).nextState!;
    const trailing=evaluateProfitTrailingPair({
      state:null,marketId:MARKET,positionId:POS,unhedgedStartedAt:AT,now:new Date(AT2),
      currentGrossEdge:"0.08",currentLockedPnl:"0.1",targetEdge:0.12,armEdge:0.05,retraceEdge:0.02,
    }).nextState!;
    const lookup={marketId:MARKET,slug:"g2g-market",endDate:"2026-10-10T11:00:00.000Z",
      yesMapsTo:"UP",noMapsTo:"DOWN"};
    return {payload:{...base,experiment:{...base.experiment,riskConfiguration:risk},
      reversalProtectionStates:[guardian],profitTrailingPairStates:[trailing],settlementLookups:[lookup]},
      guardian,trailing,lookup};
  }
  it("G2G2-01 recovers complete episode-bound Guardian V2 and trailing high water",()=>{
    const {payload,guardian,trailing,lookup}=configured();
    const base=boot(payload);
    expect(base.recovery.reversalProtectionStates?.get(MARKET)).toEqual(guardian);
    expect(base.recovery.profitTrailingPairStates?.get(MARKET)).toEqual(trailing);
    expect(base.recovery.settlementLookups?.get(MARKET)).toEqual(lookup);
    const after=advanceGuardianOfflineSource(base,row("LEDGER_MARK_TO_MARKET",21,{
      prices:{[MARKET]:{up:"0.46",down:"0.54"}},markedAt:AT2,
    },"mtm:g2g2:21"));
    expect(after.recovery.reversalProtectionStates?.get(MARKET)).toEqual(guardian);
    expect(after.recovery.profitTrailingPairStates?.get(MARKET)).toEqual(trailing);
  });
  it("G2G2-02 rejects same market state duplicated or attached to another episode",()=>{
    const {payload,guardian,trailing,lookup}=configured();
    fail(()=>boot({...payload,reversalProtectionStates:[guardian,guardian]}),"DUPLICATE_ECONOMIC_OWNER");
    fail(()=>boot({...payload,profitTrailingPairStates:[trailing,trailing]}),"DUPLICATE_ECONOMIC_OWNER");
    fail(()=>boot({...payload,settlementLookups:[lookup,lookup]}),"DUPLICATE_ECONOMIC_OWNER");
    fail(()=>boot({...payload,reversalProtectionStates:[{...guardian,unhedgedStartedAt:AT2}]}),
      "CHECKPOINT_UNSUPPORTED");
    fail(()=>boot({...payload,profitTrailingPairStates:[{...trailing,unhedgedStartedAt:AT3}]}),
      "CHECKPOINT_INVARIANT_VIOLATION:profit_trailing_pair_state_invalid");
  });
  it("G2G2-03 rejects checkpoint source policy inconsistent with saved Guardian generation",()=>{
    const {payload}=configured();
    const experiment=payload.experiment;
    fail(()=>boot({...payload,experiment:{...experiment,
      riskConfiguration:{...experiment.riskConfiguration,reversalProtectionEnabled:false}}}),
      "CHECKPOINT_INVARIANT_VIOLATION:reversal_protection_state_invalid");
  });
  it("G2G2-04 complete committed close clears both policies and settlement mapping",()=>{
    const {payload}=configured();
    const before=boot(payload);
    const closing=paperFill(MARKET,"UP","0.52",{
      id:"g2g-close",quantity:"5",side:"SELL",timestamp:AT2,feeUsd:"0",
    });
    const next=advanceGuardianOfflineSource(before,row("PAPER_FILL",21,{
      fill:closing,positionId:POS,reservationAfter:null,
    },`fill:${closing.id}`));
    expect(next.projection.open.has(MARKET)).toBe(false);
    expect(next.recovery.reversalProtectionStates?.has(MARKET)).toBe(false);
    expect(next.recovery.profitTrailingPairStates?.has(MARKET)).toBe(false);
    expect(next.recovery.settlementLookups?.has(MARKET)).toBe(false);
  });
});

describe("G2G closeout: soak diagnostics and unresolved integrity",()=>{
  const diagnostic=()=>{
    const payload=makePayload();
    const position=payload.positions[0]!;
    return createNakedDirectionalDiagnostic(position,first());
  };
  it("G2G-01 restores valid soak diagnostics without granting trade authority",()=>{
    const original=diagnostic();
    const base=boot({...makePayload(),nakedDirectionalDiagnostics:[original]});
    expect(base.recovery.nakedDirectionalDiagnostics?.get(MARKET)).toEqual(original);
    expect(base.projection.open.get(MARKET)?.residualQty).toBe("5");
    const next=advanceGuardianOfflineSource(base,row("LEDGER_MARK_TO_MARKET",21,{
      prices:{[MARKET]:{up:"0.45",down:"0.55"}},markedAt:AT2,
    },"mtm:g2g:21"));
    expect(next.recovery.nakedDirectionalDiagnostics?.get(MARKET)).toEqual(original);
  });
  it("G2G-02 rejects duplicate, cross-market or orphaned diagnostic identity",()=>{
    const orig=diagnostic();
    fail(()=>boot({...makePayload(),nakedDirectionalDiagnostics:[orig,orig]}),"DUPLICATE_ECONOMIC_OWNER");
    fail(()=>boot({...makePayload(),nakedDirectionalDiagnostics:[{...orig,positionId:"obsolete"}]}),
      "CHECKPOINT_INVARIANT_VIOLATION:naked_directional_diagnostic_invalid");
    fail(()=>boot({...makePayload(),nakedDirectionalDiagnostics:[{...orig,marketId:"other"}]}),
      "CHECKPOINT_INVARIANT_VIOLATION:naked_directional_diagnostic_invalid");
  });
  it("G2G-03 releases diagnostics after committed settlement",()=>{
    const before=boot({...makePayload(),nakedDirectionalDiagnostics:[diagnostic()]});
    const after=advanceGuardianOfflineSource(before,settleRow(21));
    expect(after.recovery.nakedDirectionalDiagnostics?.has(MARKET)).toBe(false);
    expect(after.projection.open.has(MARKET)).toBe(false);
  });
  it("G2G-04 halts on unresolved report integrity and quarantine without fabricating recovery",()=>{
    const checkpoint=makePayload();
    fail(()=>boot({...checkpoint,reportSourceIntegrity:[{experimentId:"g2f-experiment",eventId:"fault"}]}),
      "CHECKPOINT_HAS_UNMODELED_AUTHORITY");
    fail(()=>boot({...checkpoint,reportQuarantine:{unsafe:true}}),"CHECKPOINT_HAS_UNMODELED_AUTHORITY");
    fail(()=>boot({...checkpoint,nakedDirectionalDiagnostics:"not-an-array"}),"CHECKPOINT_UNSUPPORTED");
  });
});

describe("G2H canonical read-only durable page feed: closed fixtures, NO SHADOW",()=>{
  const source=(rows:ReturnType<typeof fillRow>[],highWater=22):GuardianDurableReadSource=>({
    latestDurableSequence:async()=>highWater,
    readDurableTransitionsAfter:async(cursor,limit)=>rows.filter(r=>r.sequence>cursor).slice(0,limit),
  });
  const pull=(s:GuardianDurableReadSource,opts:Record<string,number>={})=>
    pullGuardianDurablePage({source:s,state:boot(),pageSize:opts.pageSize??1,
      maxBacklogRows:opts.maxBacklogRows??100});
  const blocked=async(action:()=>Promise<unknown>,reason:string)=>
    expect(action()).rejects.toMatchObject({name:"Error",reason});
  it("G2H-01 consumes contiguous source rows one page at a time",async()=>{
    const s=source([fillRow(21),settleRow(22)],22);
    const one=await pull(s);
    expect(one.readCount).toBe(1);
    expect(one.state.projection.cursor).toBe(21);
    expect(one.state.projection.open.get(MARKET)?.residualQty).toBe("3");
    const two=await pullGuardianDurablePage({
      source:s,state:one.state,pageSize:1,maxBacklogRows:100,
    });
    expect(two.state.projection.cursor).toBe(22);
    expect(two.state.projection.open.size).toBe(0);
    expect(two.state.projection.settledMarkets.has(MARKET)).toBe(true);
    const idle=await pullGuardianDurablePage({source:s,state:two.state,
      pageSize:1,maxBacklogRows:100});
    expect(idle.readCount).toBe(0);
    expect(idle.state).toBe(two.state);
  });
  it("G2H-02 refuses a compacted missing row even if high-water advanced",async()=>{
    await blocked(()=>pull(source([settleRow(22)],22)),"DURABLE_SOURCE_GAP");
    await blocked(()=>pull(source([],22)),"DURABLE_PAGE_INVALID");
    await blocked(()=>pull(source([fillRow(21)],21))
      .then(({state})=>pullGuardianDurablePage({
        source:source([],22),state,pageSize:1,maxBacklogRows:100,
      })), "DURABLE_PAGE_INVALID");
  });
  it("G2H-03 refuses source regression, excessive lag and unbounded consumer settings",async()=>{
    await blocked(()=>pull(source([],19)),"SOURCE_CURSOR_REGRESSION");
    await blocked(()=>pull(source([],22),{pageSize:1,maxBacklogRows:1}),"BACKLOG_EXCEEDED");
    await blocked(()=>pull(source([],22),{pageSize:0}),"INVALID_LIMITS");
    await blocked(()=>pull(source([],22),{pageSize:513,maxBacklogRows:1000}),"INVALID_LIMITS");
  });
  it("G2H-04 fails closed on unavailable source, never returns cached position as current",async()=>{
    await blocked(()=>pull({
      latestDurableSequence:async()=>{throw Error("DB_CONNECT_TIMEOUT")},
      readDurableTransitionsAfter:async()=>[],
    }),"SOURCE_UNAVAILABLE");
    await blocked(()=>pull({
      latestDurableSequence:async()=>21,
      readDurableTransitionsAfter:async()=>{throw Error("QUERY_TIMEOUT")},
    }),"SOURCE_UNAVAILABLE");
  });
  it("G2H-05 fails closed on invalid, out-of-order or future durable rows",async()=>{
    await blocked(()=>pull({
      latestDurableSequence:async()=>21,
      readDurableTransitionsAfter:async()=>[fillRow(21),settleRow(22)],
    },{pageSize:2}),"DURABLE_PAGE_INVALID");
    await blocked(()=>pull(source([fillRow(22)],22)),"DURABLE_SOURCE_GAP");
    await blocked(()=>pull(source([fillRow(21),fillRow(21)],22),{pageSize:2}),"DURABLE_SOURCE_GAP");
  });
  it("G2H-06 compacted/modified economic record cannot masquerade as a valid empty heartbeat",async()=>{
    const full=fillRow(21);
    await blocked(()=>pull(source([{...full,payload:{}}],21)),"DURABLE_REPLAY_FAILED");
    await blocked(()=>pull(source([{...full,payloadHash:"0".repeat(64)}],21)),"DURABLE_REPLAY_FAILED");
  });
});

describe("G2H pinned read-only checkpoint adoption: CLOSED source fixtures",()=>{
  const original=makePayload();
  const digest=hashCheckpointPayload(original);
  const checkpointPayload={...original,checkpointContentHash:digest};
  const record={
    id:"checkpoint-g2h",experimentId:"g2f-experiment",sequence:3,createdAt:AT,
    verified:true,contentHash:digest,payload:checkpointPayload,
  };
  const pin={checkpointId:record.id,experimentId:"g2f-experiment",
    authorityId:"out-of-band-writer-epoch",contentHash:digest};
  const source=(entries=[record],highWater=22):GuardianCheckpointReadSource=>({
    listVerifiedCheckpoints:async()=>entries,
    latestDurableSequence:async()=>highWater,
  });
  const acquire=(s:GuardianCheckpointReadSource=source(),p=pin,maxBacklogRows=100)=>
    bootstrapGuardianPinnedCheckpoint({source:s,pin:p,maxBacklogRows});
  const stopped=async(work:()=>Promise<unknown>,reason:string)=>
    expect(work()).rejects.toMatchObject({reason});
  it("G2H-07 binds independently pinned digest, experiment and durable cursor",async()=>{
    const {state,checkpointId,highWater}=await acquire();
    expect(checkpointId).toBe(record.id);
    expect(highWater).toBe(22);
    expect(state.projection.cursor).toBe(20);
    expect(state.projection.authorityId).toBe(pin.authorityId);
    expect(state.projection.open.get(MARKET)?.residualQty).toBe("5");
  });
  it("G2H-08 refuses missing, duplicate and unverified checkpoint records",async()=>{
    await stopped(()=>acquire(source([])),"CHECKPOINT_NOT_FOUND");
    await stopped(()=>acquire(source([record,record])),"CHECKPOINT_AMBIGUOUS");
    await stopped(()=>acquire(source([{...record,verified:false}])),"CHECKPOINT_MISMATCH");
    await stopped(()=>acquire(source([{...record,experimentId:"unrelated"}])),"CHECKPOINT_MISMATCH");
  });
  it("G2H-09 a self-consistent but rehashed DB record cannot replace the external pin",async()=>{
    const tampered={...makePayload(),durableCursor:19};
    const alteredHash=hashCheckpointPayload(tampered);
    const replaced={...record,contentHash:alteredHash,
      payload:{...tampered,checkpointContentHash:alteredHash}};
    await stopped(()=>acquire(source([replaced])),"CHECKPOINT_MISMATCH");
    await stopped(()=>acquire(source(),{...pin,contentHash:"0".repeat(64)}),"CHECKPOINT_MISMATCH");
    await stopped(()=>acquire(source(),{...pin,checkpointId:"absent"}),"CHECKPOINT_NOT_FOUND");
  });
  it("G2H-10 fences stale/invalid source high-water, lag, pin and upstream failure",async()=>{
    await stopped(()=>acquire(source([record],19)),"SOURCE_CURSOR_REGRESSION");
    await stopped(()=>acquire(source([record],122)),"BACKLOG_EXCEEDED");
    await stopped(()=>acquire(source(),{...pin,authorityId:""}),"INVALID_PIN");
    await stopped(()=>acquire({
      listVerifiedCheckpoints:async()=>{throw Error("CONNECT_TIMEOUT")},
      latestDurableSequence:async()=>22,
    }),"SOURCE_UNAVAILABLE");
  });
  it("G2H-11 malformed source payload is a typed halt, not an uncaught exception",async()=>{
    const malformed={...record,payload:null as unknown as typeof checkpointPayload};
    await stopped(()=>acquire(source([malformed])),"CHECKPOINT_MISMATCH");
  });
  it("G2H-12 unresolved integrity in a correctly pinned checkpoint remains fatal",async()=>{
    const payload={...makePayload(),reportSourceIntegrity:[{eventId:"unknown"}]};
    const hash=hashCheckpointPayload(payload);
    await stopped(()=>acquire(source([{...record,contentHash:hash,
      payload:{...payload,checkpointContentHash:hash}}]),
      {...pin,contentHash:hash}),"CHECKPOINT_RECOVERY_REJECTED");
  });
});

describe("G2H bounded duplicate operation-key retention",()=>{
  it("G2H-13 halts at the key budget without evicting previously authenticated keys",()=>{
    const state=boot();
    const makeKeys=(n:number)=>new Set(Array.from({length:n},(_,i)=>`prior-verified-key:${i}`));
    fail(()=>advanceGuardianOfflineSource({
      ...state,seenOperationKeys:makeKeys(GUARDIAN_REPLAY_KEY_BUDGET),
    },fillRow(21)),"REPLAY_OPERATION_KEY_BUDGET_EXCEEDED");
    const nearlyFull=makeKeys(GUARDIAN_REPLAY_KEY_BUDGET-1);
    const next=advanceGuardianOfflineSource({...state,seenOperationKeys:nearlyFull},fillRow(21));
    expect(next.seenOperationKeys.size).toBe(GUARDIAN_REPLAY_KEY_BUDGET);
    expect(state.seenOperationKeys.size).toBe(0);
    expect(nearlyFull.size).toBe(GUARDIAN_REPLAY_KEY_BUDGET-1);
    fail(()=>advanceGuardianOfflineSource({...state,seenOperationKeys:new Set([fillRow(21).operationKey])},
      fillRow(21)),"DUPLICATE_OPERATION_KEY");
  });
});

import {mkdtemp,readFile,rm,writeFile} from "node:fs/promises";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {afterEach,describe,expect,it} from "vitest";
import {createIdleExperiment,PaperLedger} from "@pm/core";
import {CHECKPOINT_SCHEMA_VERSION,hashCheckpointPayload,hashDurablePayload,
  type DurableTransition,type GuardianConsistentPgPage} from "@pm/db";
import {emptyResultAccounting} from "../apps/engine/src/trading-results.js";
import {plannedDurableTransition} from "../apps/engine/src/recovery.js";
import {bootstrapGuardianOfflineSource,advanceGuardianOfflineSource} from "../apps/engine/src/guardian-offline-durable-source.js";
import {GuardianPgFileAckStore,parseGuardianPgReceipt,
  type GuardianPgAckStore,type GuardianPgReceipt} from "../apps/engine/src/guardian-pg-ack-store.js";
import {GuardianTrustedPgSession,type GuardianPgPageSource} from "../apps/engine/src/guardian-trusted-pg-session.js";
const at="2026-10-11T00:00:00.000Z";
const scope={database:"local_test",schema:"public",role:"g2h_reader"};
const expId="g2h-pg-h2";
const experiment={...createIdleExperiment({
  durationHours:1,strategyVersion:"test",riskConfiguration:{},modelConfiguration:{},
  startingBalance:"1000",systemVersion:"test",gitSha:"test",
}),id:expId,status:"RUNNING" as const,startedAt:at};
const ledger=new PaperLedger("1000",new Date(at));
const body={schemaVersion:CHECKPOINT_SCHEMA_VERSION,durableCursor:0,experiment,
  ledger:ledger.snapshot(),positions:[],resultAccounting:emptyResultAccounting(),
  restingOrders:[],makerReservations:{},activeTriggers:[],grokSignals:[],
  riskReductionOwners:[],riskReductionFailures:[],reversalProtectionStates:[],
  profitTrailingPairStates:[],settlementLookups:[],settledMarkets:[],
  nakedDirectionalDiagnostics:[],lastInfrastructureAccrualAt:0};
const hash=hashCheckpointPayload(body);
const cp={id:"local-cp",experimentId:expId,sequence:1,
  createdAt:at,verified:true,contentHash:hash,payload:{...body,checkpointContentHash:hash}};
const row=(sequence:number):DurableTransition=>{
  const payload={prices:{},markedAt:new Date(Date.parse(at)+sequence*1000).toISOString()};
  return {...plannedDurableTransition({operationKey:"pg-h2:"+sequence,type:"LEDGER_MARK_TO_MARKET",
    aggregateType:"position",aggregateId:"market",payload},new Date(Date.parse(at)+sequence*1000),sequence),
    payloadHash:hashDurablePayload(payload)};
};
class FakePg implements GuardianPgPageSource {
  checkpoints=[cp];
  rows:DurableTransition[]=[1,2,3,4,5].map(row);
  highWater=5;
  wrongScope=false;
  fail=false;
  pause:(()=>Promise<void>)|null=null;
  async readGuardianConsistentPage(input:{
    database:string;schema:string;role:string;experimentId:string;
    afterSequence:number|null;limit:number;
  }):Promise<GuardianConsistentPgPage>{
    if(this.fail)throw Error("DB_DISCONNECTED");
    await this.pause?.();
    if(input.experimentId!==expId||input.database!==scope.database)
      throw Error("SOURCE_SCOPE_NOT_FOUND");
    const fromCursor=input.afterSequence??Number(this.checkpoints[0]?.payload.durableCursor??0);
    const rows=this.rows.filter(r=>r.sequence>fromCursor&&r.sequence<=this.highWater)
      .slice(0,input.limit).map(r=>structuredClone(r));
    return {source:this.wrongScope?{...scope,role:"wrong"}:scope,
      fromCursor,highWater:this.highWater,
      checkpoints:structuredClone(this.checkpoints),rows};
  }
}
const checkpointAt=(seq:number,overrideAccrual?:number)=>{
  let state=bootstrapGuardianOfflineSource({
    payload:cp.payload,externallyVerifiedContentHash:cp.contentHash,
    authorityId:"paper-reader-1",
  });
  for(let i=1;i<=seq;i++)state=advanceGuardianOfflineSource(state,row(i));
  const r=state.recovery;
  const nextBody={...body,durableCursor:seq,experiment:r.experiment,
    ledger:r.ledger.snapshot(),positions:[...r.ledger.positions.values()],
    resultAccounting:r.resultAccounting,restingOrders:[...r.restingOrders.values()],
    makerReservations:Object.fromEntries(r.makerReservations),
    riskReductionOwners:[...(r.riskReductionOwners??new Map()).values()],
    riskReductionFailures:[...(r.riskReductionFailures??new Map()).values()],
    reversalProtectionStates:[...(r.reversalProtectionStates??new Map()).values()],
    profitTrailingPairStates:[...(r.profitTrailingPairStates??new Map()).values()],
    settlementLookups:[...(r.settlementLookups??new Map()).values()],
    settledMarkets:[...r.settledMarkets],
    nakedDirectionalDiagnostics:[...(r.nakedDirectionalDiagnostics??new Map()).values()],
    lastInfrastructureAccrualAt:overrideAccrual??r.lastInfrastructureAccrualAt,
  };
  const h=hashCheckpointPayload(nextBody);
  return {...cp,id:"cp-"+seq,sequence:seq+1,contentHash:h,
    payload:{...nextBody,checkpointContentHash:h}};
};
const dirs:string[]=[];
afterEach(async()=>{for(const dir of dirs.splice(0))await rm(dir,{recursive:true,force:true});});
async function setup(){
  const dir=await mkdtemp(join(tmpdir(),"g2h-pg-h2-"));dirs.push(dir);
  const path=join(dir,"p.guardian-pg-ack.json");
  return {path,ack:new GuardianPgFileAckStore(path),source:new FakePg()};
}
const open=(input:ReturnType<typeof setup> extends Promise<infer T>?T:never,
  maxBacklogRows=100,pageSize=2,ack:GuardianPgAckStore=input.ack)=>
  GuardianTrustedPgSession.open({
    source:input.source,ack,scope,experimentId:expId,authorityId:"paper-reader-1",
    pageSize,maxBacklogRows,
  });
describe("H2 non-signing PostgreSQL ACK/restart, NO executable authority",()=>{
  it("H2-01 pages 0..5, commits exact cursor/economic receipt only AFTER replay",async()=>{
    const i=await setup();const s=await open(i);
    expect(s.cursor).toBe(0);
    expect((await i.ack.load())?.cursor).toBe(0);
    expect(s.readVerifiedView).toBeDefined();
    expect(()=>s.readVerifiedView(1000)).toThrow("NOT_VERIFIED_FRESH_OR_CAUGHT_UP");
    expect(await s.poll(1000)).toEqual({cursor:2,highWater:5,caughtUp:false});
    expect((await i.ack.load())?.cursor).toBe(2);
    expect(await s.poll(1010)).toEqual({cursor:4,highWater:5,caughtUp:false});
    expect(await s.poll(1020)).toEqual({cursor:5,highWater:5,caughtUp:true});
    const view=s.readVerifiedView(1021);
    expect(view.cursor).toBe(5);
    expect(view.open).toEqual([]);
    expect(view.riskReductionOwners).toEqual([]);
    expect((await i.ack.load())?.economicHash).toBe(view.economicHash);
    expect(()=>s.readVerifiedView(2022)).toThrow("NOT_VERIFIED_FRESH_OR_CAUGHT_UP");
  });
  it("H2-02 restart reconstructs full economics and matches exact durable ACK",async()=>{
    const i=await setup();let s=await open(i);await s.poll(1000);await s.poll(1010);
    const saved=await i.ack.load();expect(saved?.cursor).toBe(4);
    s=await open(i);expect(s.cursor).toBe(4);
    expect(await i.ack.load()).toEqual(saved);
    await s.poll(1020);expect(s.cursor).toBe(5);
  });
  it("H2-03 crashes before ACK and replays from last durable receipt, no skipped cursor",async()=>{
    const i=await setup();let s=await open(i);
    const previous=await i.ack.load();
    const badAck:GuardianPgAckStore={load:()=>i.ack.load(),
      commit:async()=>{throw Error("SIMULATED_PROCESS_DIES_BEFORE_ACK");}};
    s=await open(i,100,2,badAck);
    await expect(s.poll(1000)).rejects.toThrow("SIMULATED_PROCESS_DIES_BEFORE_ACK");
    expect(s.status).toBe("HALTED");
    expect(s.cursor).toBe(0);
    expect(await i.ack.load()).toEqual(previous);
    const restarted=await open(i);
    expect(restarted.cursor).toBe(0);
    await restarted.poll(1001);expect(restarted.cursor).toBe(2);
  });
  it("H2-04 different persisted economics at same cursor HALTS before new ACK",async()=>{
    const i=await setup();const s=await open(i);await s.poll(1000);
    const rec=(await i.ack.load())!;
    await writeFile(i.path,JSON.stringify({...rec,economicHash:"f".repeat(64)}));
    await expect(open(i)).rejects.toThrow("RECEIPT_ECONOMIC_DIVERGENCE");
  });
  it("H2-05 old signer version 1 requires operator reconciliation, never silent migration",async()=>{
    const i=await setup();
    await writeFile(i.path,JSON.stringify({version:1,writerEpoch:7,manifestSequence:1}));
    await expect(open(i)).rejects.toThrow("LEGACY_FORMAT_REQUIRES_OPERATOR_RECONCILE");
  });
  it("H2-06 existing receipt from another DB/role/experiment is refused",async()=>{
    const i=await setup();await open(i);
    const rec=(await i.ack.load())!;
    await writeFile(i.path,JSON.stringify({...rec,source:{...scope,role:"unexpected"}}));
    await expect(open(i)).rejects.toThrow("RECEIPT_AUTHORITY_MISMATCH");
  });
  it("H2-07 row payload alteration is detected and latches the session",async()=>{
    const i=await setup();const s=await open(i);
    i.source.rows[0]={...i.source.rows[0]!,payload:{prices:{},markedAt:"bad"}};
    await expect(s.poll(1000)).rejects.toThrow("DURABLE_PAYLOAD_UNVERIFIED");
    expect(s.status).toBe("HALTED");
    expect((await i.ack.load())?.cursor).toBe(0);
  });
  it("H2-08 ordered cursor hole HALTS, never skip sequence",async()=>{
    const i=await setup();const s=await open(i);
    i.source.rows=i.source.rows.filter(x=>x.sequence!==1);
    await expect(s.poll(1000)).rejects.toThrow("DURABLE_CURSOR_GAP");
    expect(s.cursor).toBe(0);expect(s.status).toBe("HALTED");
  });
  it("H2-09 source head regression, checkpoint mutation and source switch HALT",async()=>{
    const a=await setup();let s=await open(a);
    a.source.highWater=4;
    await expect(s.poll(1000)).rejects.toThrow("SOURCE_CURSOR_REGRESSION");
    const b=await setup();s=await open(b);
    b.source.checkpoints=[];
    await expect(s.poll(1000)).rejects.toThrow("PINNED_CHECKPOINT_DISAPPEARED_OR_CHANGED");
    const c=await setup();s=await open(c);
    c.source.wrongScope=true;
    await expect(s.poll(1000)).rejects.toThrow("SOURCE_IDENTITY_CHANGED");
  });
  it("H2-10 preexisting lock and CAS mismatch refuse a second observer writer",async()=>{
    const i=await setup();await open(i);
    const current=(await i.ack.load())!;
    await expect(i.ack.commit(null,{...current,cursor:1})).rejects.toThrow("CAS_MISMATCH");
    await writeFile(i.path+".lock","crashed prior owner");
    await expect(i.ack.commit(current,{...current,cursor:1})).rejects.toThrow("CONCURRENT_WRITER");
    expect((await i.ack.load())?.cursor).toBe(0);
  });
  it("H2-11 checkpoint change is not silently admitted under same receipt",async()=>{
    const i=await setup();await open(i);
    const current=(await i.ack.load())!;
    await expect(i.ack.commit(current,{...current,checkpointId:"new-cp",cursor:1}))
      .rejects.toThrow("REGRESSION_OR_CHECKPOINT_ROTATION_UNPROVEN");
  });
  it("H2-12 rejects backlog beyond configured budget rather than dropping events",async()=>{
    const i=await setup();
    await expect(open(i,2,2)).rejects.toThrow("BACKLOG_EXCEEDED");
  });
  it("H2-13 rejects malformed receipt and retains single current evidence",async()=>{
    const i=await setup();
    expect(()=>parseGuardianPgReceipt({version:2,cursor:0})).toThrow("CORRUPT_OR_LEGACY");
    await open(i);
    const persisted=await readFile(i.path,"utf8");
    expect(JSON.parse(persisted).version).toBe(2);
    expect(persisted).not.toContain("writerEpoch");
    expect(persisted).not.toContain("manifestSequence");
  });
  it("H2-14 poll refusal when DB disconnects latches HALT",async()=>{
    const i=await setup();const s=await open(i);
    i.source.fail=true;
    await expect(s.poll(1000)).rejects.toThrow("DB_DISCONNECTED");
    expect(s.status).toBe("HALTED");
  });

  it("H2-16 lost ACK response AFTER durable rename forces HALT, restart reconciles",async()=>{
    const i=await setup();const s=await open(i);
    const ambiguous:GuardianPgAckStore={load:()=>i.ack.load(),
      commit:async(expected,next)=>{
        await i.ack.commit(expected,next);
        throw Error("ACK_RESPONSE_LOST_AFTER_DURABLE_RENAME");
      }};
    const retry=await open(i,100,2,ambiguous);
    await expect(retry.poll(1000)).rejects.toThrow("ACK_RESPONSE_LOST_AFTER_DURABLE_RENAME");
    expect(retry.status).toBe("HALTED");
    expect(retry.cursor).toBe(0);
    expect((await i.ack.load())?.cursor).toBe(2);
    const restored=await open(i);
    expect(restored.cursor).toBe(2);
    await restored.poll(1010);
    expect((await i.ack.load())?.cursor).toBe(4);
    expect(s.cursor).toBe(0);
  });
  it("H2-17 concurrent poll latches HALT and the earlier poll cannot commit",async()=>{
    const i=await setup();const s=await open(i);
    let release:()=>void=()=>undefined;
    const latch=new Promise<void>(resolve=>{release=resolve;});
    i.source.pause=()=>latch;
    const first=s.poll(1000);
    await expect(s.poll(1001)).rejects.toThrow("CONCURRENT_POLL");
    release();
    await expect(first).rejects.toThrow("HALTED_AFTER_CONCURRENT_POLL");
    expect(s.status).toBe("HALTED");
    expect(s.cursor).toBe(0);
    expect((await i.ack.load())?.cursor).toBe(0);
  });
  it("H2-18 changed source checkpoint across restart refuses skip forward",async()=>{
    const i=await setup();const s=await open(i);await s.poll(1000);
    i.source.checkpoints=[];
    await expect(open(i)).rejects.toThrow("CHECKPOINT_NOT_RECOVERABLE");
    expect((await i.ack.load())?.cursor).toBe(2);
  });


  it("H2-25 safely continues past 8192 rows by parity-rebasing at a real checkpoint",{
    timeout:60000,
  },async()=>{
    const i=await setup();
    i.source.highWater=8200;
    i.source.rows=Array.from({length:8200},(_,index)=>row(index+1));
    const s=await open(i,9000,512);
    i.source.checkpoints.unshift(checkpointAt(4000));
    for(let count=0;count<25&&!s.caughtUp;count++)
      await s.poll(1000+count);
    expect(s.cursor).toBe(8200);
    expect(s.caughtUp).toBe(true);
    expect((await i.ack.load())?.checkpointId).toBe("cp-4000");
    const resumed=await open(i,9000,512);
    expect(resumed.cursor).toBe(8200);
    expect(resumed.caughtUp).toBe(true);
  });
  it("H2-26 without a parity-proven new checkpoint refuses silent key eviction",{
    timeout:60000,
  },async()=>{
    const i=await setup();
    i.source.highWater=8193;
    i.source.rows=Array.from({length:8193},(_,index)=>row(index+1));
    const s=await open(i,9000,512);
    for(let count=0;count<16;count++)await s.poll(1000+count);
    expect(s.cursor).toBe(8192);
    await expect(s.poll(1020)).rejects.toThrow("REPLAY_OPERATION_KEY_BUDGET_EXCEEDED");
    expect(s.status).toBe("HALTED");
    expect((await i.ack.load())?.cursor).toBe(8192);
  });
  it("H2-22 rebases exact economic state at new checkpoint, then restart no longer needs old CP",async()=>{
    const i=await setup();
    const s=await open(i);
    expect(s.cursor).toBe(0);
    i.source.checkpoints.unshift(checkpointAt(4));
    expect(await s.poll(1000)).toEqual({cursor:2,highWater:5,caughtUp:false});
    const p=await s.poll(1010);
    expect(p).toEqual({cursor:4,highWater:5,caughtUp:false});
    expect((await i.ack.load())?.checkpointId).toBe("cp-4");
    expect((await i.ack.load())?.cursor).toBe(4);
    await s.poll(1020);
    expect((await i.ack.load())?.cursor).toBe(5);
    i.source.checkpoints=[i.source.checkpoints[0]!];
    const restarted=await open(i);
    expect(restarted.cursor).toBe(5);
    expect(restarted.readVerifiedView).toBeDefined();
    expect((await i.ack.load())?.checkpointId).toBe("cp-4");
  });
  it("H2-23 valid-hash checkpoint with DIFFERENT economics never rebases",async()=>{
    const i=await setup();const s=await open(i);
    i.source.checkpoints.unshift(checkpointAt(4,9876543));
    await s.poll(1000);
    await expect(s.poll(1010)).rejects.toThrow("REBASE_ECONOMIC_PARITY_MISMATCH");
    expect(s.status).toBe("HALTED");
    expect((await i.ack.load())?.checkpointId).toBe("local-cp");
    expect((await i.ack.load())?.cursor).toBe(4);
  });
  it("H2-24 a checkpoint with no old economic receipt parity proof is refused",async()=>{
    const i=await setup();const s=await open(i);
    i.source.checkpoints.unshift(checkpointAt(4));
    await s.poll(1000);
    await s.poll(1010);
    const old=(await i.ack.load())!;
    await expect(i.ack.commit(old,{...old,checkpointId:"forged-checkpoint",
      checkpointHash:"f".repeat(64)},
      {kind:"VERIFIED_CHECKPOINT_REBASE",previousEconomicHash:"e".repeat(64),
       fromCheckpointId:old.checkpointId,
       fromCheckpointHash:old.checkpointHash,newCheckpointCursor:4}))
      .rejects.toThrow("REGRESSION_OR_CHECKPOINT_ROTATION_UNPROVEN");
  });
  it("H2-21 concurrency during ACK fsync preserves old volatile cursor and reconciles",async()=>{
    const i=await setup();await open(i);
    let afterCommit:()=>void=()=>undefined;
    let continueAck:()=>void=()=>undefined;
    const committed=new Promise<void>(resolve=>{afterCommit=resolve;});
    const pause=new Promise<void>(resolve=>{continueAck=resolve;});
    const delayedAck:GuardianPgAckStore={load:()=>i.ack.load(),
      commit:async(expected,next)=>{
        await i.ack.commit(expected,next);
        afterCommit();
        await pause;
      }};
    const s=await open(i,100,2,delayedAck);
    const first=s.poll(1000);
    await committed;
    expect((await i.ack.load())?.cursor).toBe(2);
    await expect(s.poll(1001)).rejects.toThrow("CONCURRENT_POLL");
    continueAck();
    await expect(first).rejects.toThrow("HALTED_AFTER_CONCURRENT_ACK");
    expect(s.status).toBe("HALTED");
    expect(s.cursor).toBe(0);
    const recovered=await open(i);
    expect(recovered.cursor).toBe(2);
  });
  it("H2-20 restart refuses DB rollback even when last ACK cursor still exists",async()=>{
    const i=await setup();
    const session=await open(i);
    expect(session.cursor).toBe(0);
    expect((await i.ack.load())?.observedHighWater).toBe(5);
    i.source.highWater=4;
    await expect(open(i)).rejects.toThrow("OBSERVED_SOURCE_HIGHWATER_REGRESSION");
    expect((await i.ack.load())?.cursor).toBe(0);
  });
  it("H2-19 source returns duplicated operation key at later cursor and HALTs",async()=>{
    const i=await setup();const s=await open(i);
    i.source.rows[1]={...i.source.rows[1]!,operationKey:i.source.rows[0]!.operationKey};
    await expect(s.poll(1000)).rejects.toThrow("DUPLICATE_OPERATION_KEY");
    expect((await i.ack.load())?.cursor).toBe(0);
  });
  it("H2-15 new rows are consumed after first caught-up observation",async()=>{
    const i=await setup();const s=await open(i);
    await s.poll(1000);await s.poll(1010);await s.poll(1020);
    i.source.highWater=6;i.source.rows.push(row(6));
    const result=await s.poll(1030);
    expect(result).toEqual({cursor:6,highWater:6,caughtUp:true});
    expect((await i.ack.load())?.cursor).toBe(6);
  });
});

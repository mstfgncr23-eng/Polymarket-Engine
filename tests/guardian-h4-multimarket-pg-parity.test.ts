import {createRequire} from "node:module";
import {fileURLToPath} from "node:url";
import {mkdtemp,rm} from "node:fs/promises";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {afterAll,beforeAll,describe,expect,it} from "vitest";
import {createIdleExperiment,PaperLedger} from "@pm/core";
import {CHECKPOINT_SCHEMA_VERSION,PostgresEventJournal} from "@pm/db";
import {GuardianPgFileAckStore} from "../apps/engine/src/guardian-pg-ack-store.js";
import {GuardianTrustedPgSession} from "../apps/engine/src/guardian-trusted-pg-session.js";
import {bootstrapGuardianOfflineSource,advanceGuardianOfflineSource}
  from "../apps/engine/src/guardian-offline-durable-source.js";
import {hashGuardianRecoverableState} from "../apps/engine/src/guardian-readonly-session.js";
import {emptyResultAccounting} from "../apps/engine/src/trading-results.js";
import {order} from "./fixtures.js";
import {paperFill} from "./paper-engine-testkit.js";

const postgres=createRequire(new URL("../packages/db/package.json",import.meta.url))("postgres");
const migrations=fileURLToPath(new URL("../packages/db/migrations",import.meta.url));
const RAW=process.env.G2H_H4_MULTIMARKET_DATABASE_URL;
const DB="g2h_h4_multimarket_local",EXP="g2h_h4_multi";
const READER="g2h_h4_multimarket_reader",PASS="g2h_multimarket_local_reader";
const M=["g2h:multi:btc","g2h:multi:eth","g2h:multi:sol"] as const;
const T0="2026-10-11T09:00:00.000Z";
const T1="2026-10-11T09:00:03.000Z";
const T2="2026-10-11T09:00:05.000Z";
const T3="2026-10-11T09:00:09.000Z";
const scope={database:DB,schema:"public",role:READER};
function url(){
  if(!RAW)throw Error("G2H_MULTIMARKET_NO_DISPOSABLE_URL");
  const u=new URL(RAW);
  if(u.hostname!=="127.0.0.1"||u.port!=="55484"
    ||u.pathname!=="/"+DB||u.username!=="g2h_admin")
    throw Error("G2H_MULTIMARKET_REFUSE_NONLOCAL_DB");
  return u;
}
let admin:PostgresEventJournal,read:PostgresEventJournal;
let sql:ReturnType<typeof postgres>,dir:string;
const experiment=()=>({...createIdleExperiment({
  durationHours:1,strategyVersion:"h4-multi",riskConfiguration:{},
  modelConfiguration:{},startingBalance:"1000",systemVersion:"offline",gitSha:"offline",
}),id:EXP,status:"RUNNING" as const,startedAt:T0});
describe.skipIf(!RAW)("H4 3-market / overlapping owner and maker economic parity on REAL local PG",()=>{
  beforeAll(async()=>{
    const u=url();
    admin=new PostgresEventJournal(u.toString(),migrations);
    await admin.initialize();
    sql=postgres(u.toString(),{max:2});
    await sql`CREATE ROLE g2h_h4_multimarket_reader LOGIN PASSWORD 'g2h_multimarket_local_reader'`;
    await sql`GRANT USAGE ON SCHEMA public TO g2h_h4_multimarket_reader`;
    await sql`GRANT SELECT ON engine_checkpoints,durable_transitions,durable_sequence_metadata
      TO g2h_h4_multimarket_reader`;
    for(const table of ["engine_checkpoints","durable_transitions","durable_sequence_metadata"])
      await sql.unsafe(`CREATE POLICY h4_multimarket_select ON ${table}
        FOR SELECT TO g2h_h4_multimarket_reader USING (true)`);
    const r=new URL(u);r.username=READER;r.password=PASS;
    read=new PostgresEventJournal(r.toString(),migrations,{access:"readonly"});
    await read.connectWithoutMigrate("readonly");
    dir=await mkdtemp(join(tmpdir(),"g2h-h4-multimarket-"));
  },60000);
  afterAll(async()=>{
    await read?.close();
    if(sql){
      for(const table of ["engine_checkpoints","durable_transitions","durable_sequence_metadata"])
        await sql.unsafe(`DROP POLICY IF EXISTS h4_multimarket_select ON ${table}`);
      await sql`REVOKE ALL PRIVILEGES ON engine_checkpoints,durable_transitions,durable_sequence_metadata
        FROM g2h_h4_multimarket_reader`;
      await sql`REVOKE ALL PRIVILEGES ON SCHEMA public FROM g2h_h4_multimarket_reader`;
      await sql`DROP ROLE IF EXISTS g2h_h4_multimarket_reader`;
      await sql.end();
    }
    await admin?.close();
    if(dir)await rm(dir,{recursive:true,force:true});
  },30000);
  it("H4-MULTI-01 interleaved dual owners and maker reservation, parity-proven checkpoint rebase, restart",{
    timeout:50000,
  },async()=>{
    const ledger=new PaperLedger("1000",new Date(T0));
    const inputs=[
      paperFill(M[0],"UP","0.40",{
        id:"multi-buy-btc",orderId:"multi-order-btc",quantity:"5",
        timestamp:T0,feeUsd:"0",slippageUsd:"0",
      }),
      paperFill(M[1],"UP","0.30",{
        id:"multi-buy-eth",orderId:"multi-order-eth",quantity:"4",
        timestamp:T0,feeUsd:"0",slippageUsd:"0",
      }),
      paperFill(M[2],"UP","0.44",{
        id:"multi-buy-sol",orderId:"multi-order-sol",quantity:"3",
        timestamp:T0,feeUsd:"0",slippageUsd:"0",
      }),
    ];
    const oracle=new PaperLedger("1000",new Date(T0));
    const initial={
      schemaVersion:CHECKPOINT_SCHEMA_VERSION,durableCursor:1,
      experiment:experiment(),ledger:ledger.snapshot(),
      positions:[],resultAccounting:emptyResultAccounting(),
      restingOrders:[],makerReservations:{},riskReductionOwners:[],
      riskReductionFailures:[],activeTriggers:[],grokSignals:[],
      reversalProtectionStates:[],profitTrailingPairStates:[],
      settlementLookups:[],settledMarkets:[],
      nakedDirectionalDiagnostics:[],lastInfrastructureAccrualAt:0,
    };
    await admin.commitDurableTransition({
      operationKey:"h4:multi:seed",type:"LEDGER_MARK_TO_MARKET",
      aggregateType:"position",aggregateId:M[0],
      payload:{prices:{},markedAt:T0},
    },0);
    const oldCp=await admin.saveCheckpoint({
      experimentId:EXP,sequence:1,payload:initial,
    });
    const ack=new GuardianPgFileAckStore(join(dir,"multi.guardian-pg-ack.json"));
    const config={source:read,ack,scope,experimentId:EXP,
      authorityId:"h4-multi-trusted-local",pageSize:2,maxBacklogRows:64};
    const session=await GuardianTrustedPgSession.open(config);
    expect(session.cursor).toBe(1);
    const owners=[
      {marketId:M[0],unhedgedStartedAt:T0,intent:"PROTECTIVE_DE_RISK",
        order:order("TAKER",{id:"multi-owner-btc",marketId:M[0],
          tokenId:inputs[0]!.tokenId,outcome:"UP",side:"SELL",
          requestedPrice:"0.52",quantity:"5",createdAt:T0}),
        admittedResidualQuantity:"5",decisionReceivedAt:T1,state:"ADMITTED" as const},
      {marketId:M[1],unhedgedStartedAt:T0,intent:"PROTECTIVE_DE_RISK",
        order:order("TAKER",{id:"multi-owner-eth",marketId:M[1],
          tokenId:inputs[1]!.tokenId,outcome:"UP",side:"SELL",
          requestedPrice:"0.42",quantity:"4",createdAt:T0}),
        admittedResidualQuantity:"4",decisionReceivedAt:T1,state:"ADMITTED" as const},
    ];
    const makerOrder=order("MAKER",{
      id:"multi-maker-sol",marketId:M[2],tokenId:M[2]+"-down",
      outcome:"DOWN",side:"BUY",requestedPrice:"0.30",quantity:"2",createdAt:T1,
    });
    const resting={order:{...makerOrder,status:"RESTING" as const},
      queueAhead:"0",remaining:"2",observedAt:T1,intent:"HEDGE_SECOND_LEG" as const};
    let cursor=1;
    const put=async(type:string,key:string,payload:Record<string,unknown>,
      aggregateId:string,aggregateType="position")=>{
      const before=cursor;
      const row=await admin.commitDurableTransition({
        type,operationKey:key,aggregateType,aggregateId,payload,
      },cursor);
      expect(row.sequence).toBe(before+1);
      cursor++;
      const pg=await read.readGuardianConsistentPage({
        ...scope,experimentId:EXP,afterSequence:before,limit:1,
      });
      expect(pg.rows[0]?.sequence).toBe(cursor);
      return pg.rows[0]!;
    };
    const compare=async(want:{owners:string[];failures:string[];
      maker:number;residuals:Record<string,string>;settled:string[]})=>{
      while(session.cursor<cursor)await session.poll(Date.now());
      await session.poll(Date.now());
      const view=session.readVerifiedView(Date.now());
      expect(view.cursor).toBe(cursor);
      expect((await ack.load())?.cursor).toBe(cursor);
      expect(view.riskReductionOwners.map(x=>x.marketId).sort()).toEqual([...want.owners].sort());
      expect(view.riskReductionFailures.map(x=>x.ownership.marketId).sort()).toEqual([...want.failures].sort());
      expect(view.restingOrders).toHaveLength(want.maker);
      expect(view.makerReservations).toHaveLength(want.maker);
      expect(Object.fromEntries(view.open.map(x=>[
        x.episode.marketId,x.residualQty,
      ]))).toEqual(want.residuals);
      const page=await read.readGuardianConsistentPage({
        ...scope,experimentId:EXP,afterSequence:1,limit:32,
        pinnedCheckpointId:oldCp.id,
      });
      const cp=page.checkpoints.find(x=>x.id===oldCp.id)!;
      let state=bootstrapGuardianOfflineSource({
        payload:cp.payload,externallyVerifiedContentHash:cp.contentHash!,
        authorityId:config.authorityId,
      });
      for(const row of page.rows.filter(x=>x.sequence<=cursor))
        state=advanceGuardianOfflineSource(state,row);
      expect(state.projection.cursor).toBe(cursor);
      expect(state.recovery.ledger.snapshot()).toEqual(oracle.snapshot());
      expect([...state.recovery.ledger.positions.values()])
        .toEqual([...oracle.positions.values()]);
      expect([...state.recovery.settledMarkets].sort()).toEqual([...want.settled].sort());
      // Validate the full authenticated Guardian view's hash against the
      // replayed source INCLUDING owner/reservation/settlement state.
      const receipt=await ack.load();
      const vhash=hashGuardianRecoverableState(state);
      if(receipt?.checkpointId===oldCp.id)expect(vhash).toBe(view.economicHash);
      const resumed=await GuardianTrustedPgSession.open(config);
      expect(resumed.cursor).toBe(cursor);
      await resumed.poll(Date.now());
      expect(resumed.readVerifiedView(Date.now()).economicHash).toBe(view.economicHash);
      return state;
    };
    // Three real canonical SQL PAPER_FILLs create the inventories.
    // Initial checkpoint contains ZERO inventory and zero accounting.
    for(const fill of inputs){
      await put("PAPER_FILL","fill:"+fill.id,{
        fill,positionId:"pos:"+fill.marketId,reservationAfter:null,
      },fill.id,"paper_fill");
      oracle.applyFill(fill,{positionId:"pos:"+fill.marketId});
    }
    await compare({owners:[],failures:[],maker:0,
      residuals:{[M[0]]:"5",[M[1]]:"4",[M[2]]:"3"},settled:[]});
    await put("RISK_REDUCTION_ADMITTED","multi-admit-btc",
      {ownership:owners[0]},M[0]);
    await put("RISK_REDUCTION_ADMITTED","multi-admit-eth",
      {ownership:owners[1]},M[1]);
    await compare({owners:[M[0],M[1]],failures:[],maker:0,
      residuals:{[M[0]]:"5",[M[1]]:"4",[M[2]]:"3"},settled:[]});
    const reserved=await put("PAPER_MAKER_RESERVED","multi-reserve-sol",
      {orderId:makerOrder.id,reserved:"0.6",resting,intent:"HEDGE_SECOND_LEG"},M[2]);
    oracle.reserve("0.6",new Date(reserved.occurredAt));
    await compare({owners:[M[0],M[1]],failures:[],maker:1,
      residuals:{[M[0]]:"5",[M[1]]:"4",[M[2]]:"3"},settled:[]});
    await put("RISK_REDUCTION_FAILED","multi-fail-eth",
      {ownership:owners[1],reason:"OFFLINE_DEPTH",executionFailure:true},M[1]);
    await compare({owners:[M[0]],failures:[M[1]],maker:1,
      residuals:{[M[0]]:"5",[M[1]]:"4",[M[2]]:"3"},settled:[]});
    const released=await put("PAPER_ORDER_CANCELLED","multi-cancel-sol",
      {orderId:makerOrder.id,cancel:true,reason:"OFFLINE_CANCEL"},M[2]);
    oracle.release("0.6",new Date(released.occurredAt));
    const atFive=await compare({owners:[M[0]],failures:[M[1]],maker:0,
      residuals:{[M[0]]:"5",[M[1]]:"4",[M[2]]:"3"},settled:[]});
    // Save the exact canonical economically equivalent state at cursor 6.
    // A mismatch must cause rebase HALT, not an optimistic cursor jump.
    const rr=atFive.recovery;
    const nextCpPayload={...initial,durableCursor:cursor,
      ledger:rr.ledger.snapshot(),positions:[...rr.ledger.positions.values()],
      resultAccounting:rr.resultAccounting,experiment:rr.experiment,
      restingOrders:[...rr.restingOrders.values()],
      makerReservations:Object.fromEntries(rr.makerReservations),
      riskReductionOwners:[...rr.riskReductionOwners.values()],
      riskReductionFailures:[...rr.riskReductionFailures.values()],
      reversalProtectionStates:[...rr.reversalProtectionStates.values()],
      profitTrailingPairStates:[...rr.profitTrailingPairStates.values()],
      settlementLookups:[...rr.settlementLookups.values()],
      settledMarkets:[...rr.settledMarkets],
      nakedDirectionalDiagnostics:[...rr.nakedDirectionalDiagnostics.values()],
      lastInfrastructureAccrualAt:rr.lastInfrastructureAccrualAt,
    };
    const replacement=await admin.saveCheckpoint({
      experimentId:EXP,sequence:2,payload:nextCpPayload,
    });
    await session.poll(Date.now());
    expect((await ack.load())?.checkpointId).toBe(replacement.id);
    expect((await ack.load())?.checkpointId).not.toBe(oldCp.id);
    const afterRebase=await GuardianTrustedPgSession.open(config);
    expect(afterRebase.cursor).toBe(cursor);
    await afterRebase.poll(Date.now());
    await compare({owners:[M[0]],failures:[M[1]],maker:0,
      residuals:{[M[0]]:"5",[M[1]]:"4",[M[2]]:"3"},settled:[]});
    const sellBtc=paperFill(M[0],"UP","0.52",{
      id:"multi-sell-btc",orderId:owners[0]!.order.id,side:"SELL",
      quantity:"5",timestamp:T2,feeUsd:"0",slippageUsd:"0",
    });
    await put("PAPER_FILL","fill:"+sellBtc.id,{
      fill:sellBtc,positionId:"pos:"+M[0],reservationAfter:null,
    },sellBtc.id,"paper_fill");
    oracle.applyFill(sellBtc,{positionId:"pos:"+M[0]});
    await compare({owners:[],failures:[M[1]],maker:0,
      residuals:{[M[1]]:"4",[M[2]]:"3"},settled:[]});
    const sellEth=paperFill(M[1],"UP","0.42",{
      id:"multi-sell-eth",orderId:"multi-new-eth-sell",side:"SELL",
      quantity:"1",timestamp:T2,feeUsd:"0",slippageUsd:"0",
    });
    await put("PAPER_FILL","fill:"+sellEth.id,{
      fill:sellEth,positionId:"pos:"+M[1],reservationAfter:null,
    },sellEth.id,"paper_fill");
    oracle.applyFill(sellEth,{positionId:"pos:"+M[1]});
    await compare({owners:[],failures:[],maker:0,
      residuals:{[M[1]]:"3",[M[2]]:"3"},settled:[]});
    await put("POSITION_SETTLED","settlement:"+M[2]+":"+EXP,
      {marketId:M[2],upPayoff:"1",downPayoff:"0",settledAt:T3},M[2]);
    oracle.settlePosition(M[2],"1","0",new Date(T3));
    await compare({owners:[],failures:[],maker:0,
      residuals:{[M[1]]:"3"},settled:[M[2]]});
    // 1000 - 2 - 1.2 - 1.32 + 2.6 + .42 + 3 = 1001.50.
    expect(oracle.snapshot().cash).toBe("1001.5");
    // BTC sale +0.60, ETH sale +0.12, SOL settlement +1.68 = +2.40.
    expect(oracle.snapshot().realizedPnl).toBe("2.4");
    console.log("G2H_H4_MULTI_MARKET_PROOF "+JSON.stringify({
      markets:3,cursor,checkpointRebased:true,
      riskOwnerOverlapMax:2,makerReservationVerified:true,
      remaining:{[M[1]]:"3"},settled:M[2],
      cash:oracle.snapshot().cash,realizedPnl:oracle.snapshot().realizedPnl,
    }));
  });
});

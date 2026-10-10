import {createRequire} from "node:module";
import {fileURLToPath} from "node:url";
import {mkdtemp,rm} from "node:fs/promises";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {beforeAll,afterAll,describe,expect,it} from "vitest";
import {createIdleExperiment,PaperLedger} from "@pm/core";
import {PostgresEventJournal,CHECKPOINT_SCHEMA_VERSION,hashDurablePayload} from "@pm/db";
import {GuardianPgFileAckStore} from "../apps/engine/src/guardian-pg-ack-store.js";
import {GuardianTrustedPgSession} from "../apps/engine/src/guardian-trusted-pg-session.js";
import {bootstrapGuardianOfflineSource,advanceGuardianOfflineSource}
  from "../apps/engine/src/guardian-offline-durable-source.js";
import {hashGuardianRecoverableState} from "../apps/engine/src/guardian-readonly-session.js";
import {accountResultFill,emptyResultAccounting} from "../apps/engine/src/trading-results.js";
import {order} from "./fixtures.js";
import {paperFill} from "./paper-engine-testkit.js";

const postgres=createRequire(new URL("../packages/db/package.json",import.meta.url))("postgres");
const migrations=fileURLToPath(new URL("../packages/db/migrations",import.meta.url));
const RAW=process.env.G2H_H4_ECONOMIC_LOCAL_DATABASE_URL;
const E="g2h_h4_economic_semantic",M="g2h:semantic:btc",POS="pos:"+M;
const R="g2h_h4_semantic_reader",P="g2h_semantic_reader_local_only";
const T0="2026-10-11T08:00:00.000Z",T1="2026-10-11T08:00:05.000Z";
const T2="2026-10-11T08:00:07.000Z",T3="2026-10-11T08:00:09.000Z";
const db="g2h_h4_economic_local",scope={database:db,schema:"public",role:R};
const guard=()=>{
  if(!RAW)throw Error("G2H_SEMANTIC_NO_LOCAL_URL");
  const url=new URL(RAW);
  if(url.hostname!=="127.0.0.1"||url.port!=="55483"
    ||url.pathname!=="/"+db||url.username!=="g2h_admin")
    throw Error("G2H_SEMANTIC_REJECTS_NONLOCAL_DB");
  return url;
};
let admin:PostgresEventJournal;
let read:PostgresEventJournal;
let sql:ReturnType<typeof postgres>;
let dir:string;
let opening:ReturnType<typeof paperFill>;
let sale:ReturnType<typeof paperFill>;
let firstLedger:PaperLedger;
const oracle=new PaperLedger("1000",new Date(T0));
const exp=()=>({...createIdleExperiment({
  durationHours:2,strategyVersion:"h4-semantic",riskConfiguration:{},
  modelConfiguration:{},startingBalance:"1000",systemVersion:"offline",gitSha:"offline",
}),id:E,status:"RUNNING" as const,startedAt:T0});
const readerOpts=(ack:GuardianPgFileAckStore)=>({
  source:read,ack,scope,experimentId:E,
  authorityId:"h4-semantic-single-writer-local",pageSize:1,maxBacklogRows:80,
});
describe.skipIf(!RAW)("H4 genuine economic transitions through local SELECT-only PG, independent ledger oracle",()=>{
  beforeAll(async()=>{
    const url=guard();
    sql=postgres(url.toString(),{max:2});
    admin=new PostgresEventJournal(url.toString(),migrations);
    await admin.initialize();
    opening=paperFill(M,"UP","0.40",{
      id:"h4:entry:buy",orderId:"h4:order:buy",
      quantity:"5",timestamp:T0,feeUsd:"0",slippageUsd:"0",
    });
    sale=paperFill(M,"UP","0.52",{
      id:"h4:partial:sell",orderId:"h4:order:sell",
      quantity:"2",side:"SELL",timestamp:T2,feeUsd:"0",slippageUsd:"0",
    });
    firstLedger=new PaperLedger("1000",new Date(T0));
    firstLedger.applyFill(opening,{positionId:POS});
    oracle.applyFill(opening,{positionId:POS});
    const result=emptyResultAccounting();
    accountResultFill(result,opening,null,firstLedger.positions.get(M));
    await admin.commitDurableTransition({
      operationKey:"h4:sem:seed",type:"LEDGER_MARK_TO_MARKET",
      aggregateType:"position",aggregateId:M,
      payload:{prices:{},markedAt:T0},
    },0);
    await admin.saveCheckpoint({experimentId:E,sequence:1,payload:{
      schemaVersion:CHECKPOINT_SCHEMA_VERSION,durableCursor:1,
      experiment:exp(),ledger:firstLedger.snapshot(),
      positions:[...firstLedger.positions.values()],resultAccounting:result,
      restingOrders:[],makerReservations:{},activeTriggers:[],grokSignals:[],
      riskReductionOwners:[],riskReductionFailures:[],
      reversalProtectionStates:[],profitTrailingPairStates:[],
      settlementLookups:[],settledMarkets:[],
      nakedDirectionalDiagnostics:[],lastInfrastructureAccrualAt:0,
    }});
    await sql`CREATE ROLE g2h_h4_semantic_reader LOGIN PASSWORD 'g2h_semantic_reader_local_only'`;
    await sql`GRANT USAGE ON SCHEMA public TO g2h_h4_semantic_reader`;
    await sql`GRANT SELECT ON engine_checkpoints,durable_transitions,durable_sequence_metadata
      TO g2h_h4_semantic_reader`;
    for(const table of ["engine_checkpoints","durable_transitions","durable_sequence_metadata"])
      await sql.unsafe(`CREATE POLICY h4_semantic_read ON ${table}
        FOR SELECT TO g2h_h4_semantic_reader USING (true)`);
    const readerUrl=new URL(url);readerUrl.username=R;readerUrl.password=P;
    read=new PostgresEventJournal(readerUrl.toString(),migrations,{access:"readonly"});
    await read.connectWithoutMigrate("readonly");
    dir=await mkdtemp(join(tmpdir(),"g2h-semantic-oracle-"));
  },60000);
  afterAll(async()=>{
    await read?.close();
    if(sql){
      for(const table of ["engine_checkpoints","durable_transitions","durable_sequence_metadata"])
        await sql.unsafe(`DROP POLICY IF EXISTS h4_semantic_read ON ${table}`);
      await sql`REVOKE ALL PRIVILEGES ON engine_checkpoints,durable_transitions,durable_sequence_metadata
        FROM g2h_h4_semantic_reader`;
      await sql`REVOKE ALL PRIVILEGES ON SCHEMA public FROM g2h_h4_semantic_reader`;
      await sql`DROP ROLE IF EXISTS g2h_h4_semantic_reader`;
      await sql.end();
    }
    await admin?.close();
    if(dir)await rm(dir,{recursive:true,force:true});
  },20000);
  it("H4-ECO-01 owner admit, failure release, partial real sell, settlement, ACK and restart exact parity",{
    timeout:60000,
  },async()=>{
    const ack=new GuardianPgFileAckStore(join(dir,"semantic.guardian-pg-ack.json"));
    const input=readerOpts(ack);
    const s=await GuardianTrustedPgSession.open(input);
    const check=async(cursor:number,residual:string,owners:number,failures:number)=>{
      expect((await s.poll(Date.now())).cursor).toBe(cursor);
      const v=s.readVerifiedView(Date.now());
      expect(v.cursor).toBe(cursor);
      expect(v.open.length).toBe(residual==="0"?0:1);
      if(residual!=="0")expect(v.open[0]!.residualQty).toBe(residual);
      expect(v.riskReductionOwners).toHaveLength(owners);
      expect(v.riskReductionFailures).toHaveLength(failures);
      expect((await ack.load())?.cursor).toBe(cursor);
      const boot=await GuardianTrustedPgSession.open(input);
      expect(boot.cursor).toBe(cursor);
      await boot.poll(Date.now());
      expect(boot.readVerifiedView(Date.now()).economicHash).toBe(v.economicHash);
      // Independent PaperLedger oracle: compare the FULL economic ledger
      // after fetching all real committed PG rows and replaying the actual
      // canonical reducer. Do not simply compare Guardian view to itself.
      const page=await read.readGuardianConsistentPage({
        ...scope,experimentId:E,afterSequence:1,limit:10,
      });
      const cp=page.checkpoints.find(c=>Number(c.payload.durableCursor)===1)!;
      let state=bootstrapGuardianOfflineSource({
        payload:cp.payload,externallyVerifiedContentHash:cp.contentHash!,
        authorityId:input.authorityId,
      });
      for(const row of page.rows.filter(row=>row.sequence<=cursor))
        state=advanceGuardianOfflineSource(state,row);
      expect(state.projection.cursor).toBe(cursor);
      expect(state.recovery.ledger.snapshot()).toEqual(oracle.snapshot());
      expect([...state.recovery.ledger.positions.values()])
        .toEqual([...oracle.positions.values()]);
      expect(hashGuardianRecoverableState(state)).toBe(v.economicHash);
    };
    const owner={
      marketId:M,unhedgedStartedAt:T0,intent:"PROTECTIVE_DE_RISK",
      order:order("TAKER",{id:"h4:semantic:owner",traceId:"h4:trace",
        triggerId:"h4:trigger",marketId:M,tokenId:opening.tokenId,
        outcome:"UP",side:"SELL",requestedPrice:"0.52",
        quantity:"5",createdAt:T0}),
      admittedResidualQuantity:"5",decisionReceivedAt:T1,
      state:"ADMITTED" as const,
    };
    await admin.commitDurableTransition({
      operationKey:"h4:semantic:admit",type:"RISK_REDUCTION_ADMITTED",
      aggregateType:"position",aggregateId:M,payload:{ownership:owner},
    },1);
    await check(2,"5",1,0);
    await admin.commitDurableTransition({
      operationKey:"h4:semantic:failure",type:"RISK_REDUCTION_FAILED",
      aggregateType:"position",aggregateId:M,
      payload:{ownership:owner,reason:"INSUFFICIENT_DEPTH",executionFailure:true},
    },2);
    await check(3,"5",0,1);
    await admin.commitDurableTransition({
      operationKey:"fill:"+sale.id,type:"PAPER_FILL",aggregateType:"paper_fill",
      aggregateId:sale.id,payload:{fill:sale,positionId:POS,reservationAfter:null},
    },3);
    oracle.applyFill(sale,{positionId:POS});
    await check(4,"3",0,0);
    expect(oracle.positions.get(M)?.unpairedQuantity).toBe("3");
    // Independent hand-calculated economic oracle:
    // Buy 5 @ $0.40; sell 2 @ $0.52 => cash 1000-2+1.04=999.04,
    // realized profit on those two = 2*($0.52-$0.40) = $0.24.
    expect(oracle.snapshot().cash).toBe("999.04");
    expect(oracle.snapshot().realizedPnl).toBe("0.24");
    const snapshot=await GuardianTrustedPgSession.open(input);
    // Force poll to establish current observation without changing economics.
    await snapshot.poll(Date.now());
    expect(snapshot.readVerifiedView(Date.now()).open[0]?.residualQty)
      .toBe(oracle.positions.get(M)?.unpairedQuantity);
    await admin.commitDurableTransition({
      operationKey:"settlement:"+M+":"+E,type:"POSITION_SETTLED",
      aggregateType:"position",aggregateId:M,
      payload:{marketId:M,upPayoff:"1",downPayoff:"0",settledAt:T3},
    },4);
    oracle.settlePosition(M,"1","0",new Date(T3));
    await check(5,"0",0,0);
    expect(oracle.positions.has(M)).toBe(false);
    // Settlement pays 3*$1.00: cash 1002.04, final realized PnL 2.04.
    expect(oracle.snapshot().cash).toBe("1002.04");
    expect(oracle.snapshot().realizedPnl).toBe("2.04");
    const r=await GuardianTrustedPgSession.open(input);
    await r.poll(Date.now());
    expect(r.readVerifiedView(Date.now()).open).toEqual([]);
    expect((await ack.load())?.cursor).toBe(5);
  });
  it("H4-ECO-03 wrong risk owner AFTER settlement HALTS without ACK",async()=>{
    const ack=new GuardianPgFileAckStore(join(dir,"semantic.guardian-pg-ack.json"));
    const input=readerOpts(ack);
    const previous=await ack.load();
    expect(previous?.cursor).toBe(5);
    const readerBefore=await GuardianTrustedPgSession.open(input);
    const admitted=await sql`
      SELECT payload FROM durable_transitions WHERE sequence=2
    `;
    const ownership=(admitted[0]!.payload as Record<string,unknown>).ownership;
    // A new admission after settled inventory is a canonical invalid
    // transition, even if the caller can write the durable row.
    await admin.commitDurableTransition({
      operationKey:"h4:semantic:invalid-after-settle",
      type:"RISK_REDUCTION_ADMITTED",aggregateType:"position",
      aggregateId:M,payload:{ownership},
    },5);
    await expect(readerBefore.poll(Date.now()))
      .rejects.toThrow("GUARDIAN_OFFLINE_SOURCE_REJECTED:DURABLE_REPLAY_REJECTED");
    expect(readerBefore.status).toBe("HALTED");
    expect(await ack.load()).toEqual(previous);
  });
  it("H4-ECO-02 fully rehashed historical SELL tampering cannot rewrite ACKed economic truth",async()=>{
    const ack=new GuardianPgFileAckStore(join(dir,"semantic.guardian-pg-ack.json"));
    const before=await ack.load();
    expect(before?.cursor).toBe(5);
    const existing=await sql`
      SELECT payload FROM durable_transitions WHERE sequence=4
    `;
    expect(existing).toHaveLength(1);
    const payload=structuredClone(existing[0]!.payload) as Record<string,unknown>;
    const fill=payload.fill as Record<string,unknown>;
    fill.fillPrice="0.53"; // same count/episode, DIFFERENT settled PnL
    const digest=hashDurablePayload(payload);
    await sql`
      UPDATE durable_transitions
      SET payload=${sql.json(payload)},payload_hash=${digest}
      WHERE sequence=4
    `;
    // This is an intentional mutation only in a disposable DB. A
    // compromised privileged writer is outside G2H trust, but a local
    // economic receipt prevents silently adopting the changed replay.
    await expect(GuardianTrustedPgSession.open(readerOpts(ack)))
      .rejects.toThrow("G2H_PG_SESSION_RECEIPT_ECONOMIC_DIVERGENCE");
    expect(await ack.load()).toEqual(before);
  });
});

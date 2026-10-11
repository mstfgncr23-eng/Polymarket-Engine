import {createRequire} from "node:module";
import {fileURLToPath} from "node:url";
import {mkdtemp,rm} from "node:fs/promises";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {beforeAll,afterAll,describe,expect,it} from "vitest";
import {createIdleExperiment,PaperLedger} from "@pm/core";
import {PostgresEventJournal,CHECKPOINT_SCHEMA_VERSION} from "@pm/db";
import {GuardianPgFileAckStore,type GuardianPgAckStore}
  from "../apps/engine/src/guardian-pg-ack-store.js";
import {GuardianTrustedPgSession} from "../apps/engine/src/guardian-trusted-pg-session.js";
import {bootstrapGuardianOfflineSource,advanceGuardianOfflineSource}
  from "../apps/engine/src/guardian-offline-durable-source.js";
import {hashGuardianRecoverableState} from "../apps/engine/src/guardian-readonly-session.js";
import {emptyResultAccounting} from "../apps/engine/src/trading-results.js";
import {paperFill,qualifiedPaperEnv} from "./paper-engine-testkit.js";
import {RetentionManager,retentionConfigFromEngineEnv,defaultRetentionConfig}
  from "../apps/engine/src/retention-manager.js";

const pg=createRequire(new URL("../packages/db/package.json",import.meta.url))("postgres");
const migrations=fileURLToPath(new URL("../packages/db/migrations",import.meta.url));
const RAW=process.env.G2H_H2_RETENTION_LOCAL_DATABASE_URL;
const DB="g2h_h2_retention_local",R="g2h_h2_retention_reader",P="g2h_h2_retention_local_only";
const E="g2h_h2_retention",M="g2h:retention:btc",T="2026-10-11T10:00:00.000Z";
const scope={database:DB,schema:"public",role:R};
function getUrl(){
  if(!RAW)throw Error("H2_RETENTION_DISPOSABLE_URL_REQUIRED");
  const u=new URL(RAW);
  if(u.hostname!=="127.0.0.1"||u.port!=="55485"
    ||u.pathname!=="/"+DB||u.username!=="g2h_admin")
    throw Error("H2_RETENTION_REFUSE_NONLOCAL_DB");
  return u;
}
const experiment=()=>({...createIdleExperiment({
  durationHours:1,strategyVersion:"h2-offline",riskConfiguration:{},
  modelConfiguration:{},startingBalance:"1000",systemVersion:"local",gitSha:"local",
}),id:E,status:"RUNNING" as const,startedAt:T});
const body=(cursor:number, r:{
  ledger:PaperLedger;experiment:unknown;resultAccounting:unknown;
  restingOrders:Map<string,unknown>;makerReservations:Map<string,string>;
  riskReductionOwners:Map<string,unknown>;riskReductionFailures:Map<string,unknown>;
  reversalProtectionStates:Map<string,unknown>;profitTrailingPairStates:Map<string,unknown>;
  settlementLookups:Map<string,unknown>;settledMarkets:Set<string>;
  nakedDirectionalDiagnostics:Map<string,unknown>;lastInfrastructureAccrualAt:number;
})=>({
  schemaVersion:CHECKPOINT_SCHEMA_VERSION,durableCursor:cursor,
  experiment:r.experiment,ledger:r.ledger.snapshot(),
  positions:[...r.ledger.positions.values()],resultAccounting:r.resultAccounting,
  restingOrders:[...r.restingOrders.values()],
  makerReservations:Object.fromEntries(r.makerReservations),
  riskReductionOwners:[...r.riskReductionOwners.values()],
  riskReductionFailures:[...r.riskReductionFailures.values()],
  reversalProtectionStates:[...r.reversalProtectionStates.values()],
  profitTrailingPairStates:[...r.profitTrailingPairStates.values()],
  settlementLookups:[...r.settlementLookups.values()],
  settledMarkets:[...r.settledMarkets],
  nakedDirectionalDiagnostics:[...r.nakedDirectionalDiagnostics.values()],
  lastInfrastructureAccrualAt:r.lastInfrastructureAccrualAt,
  activeTriggers:[],grokSignals:[],
});
let admin:PostgresEventJournal,reader:PostgresEventJournal,sql:ReturnType<typeof pg>,dir:string;
describe.skipIf(!RAW)("H2 local PG retention and ambiguous ACK outcome",()=>{
  beforeAll(async()=>{
    const u=getUrl();
    admin=new PostgresEventJournal(u.toString(),migrations);
    await admin.initialize();
    sql=pg(u.toString(),{max:2});
    await sql`CREATE ROLE g2h_h2_retention_reader LOGIN PASSWORD 'g2h_h2_retention_local_only'`;
    await sql`GRANT USAGE ON SCHEMA public TO g2h_h2_retention_reader`;
    await sql`GRANT SELECT ON engine_checkpoints,durable_transitions,durable_sequence_metadata
      TO g2h_h2_retention_reader`;
    for(const table of ["engine_checkpoints","durable_transitions","durable_sequence_metadata"])
      await sql.unsafe(`CREATE POLICY h2_retention_read ON ${table}
        FOR SELECT TO g2h_h2_retention_reader USING (true)`);
    const r=new URL(u);r.username=R;r.password=P;
    reader=new PostgresEventJournal(r.toString(),migrations,{access:"readonly"});
    await reader.connectWithoutMigrate("readonly");
    dir=await mkdtemp(join(tmpdir(),"g2h-h2-retention-"));
  },60000);
  afterAll(async()=>{
    await reader?.close();
    if(sql){
      for(const table of ["engine_checkpoints","durable_transitions","durable_sequence_metadata"])
        await sql.unsafe(`DROP POLICY IF EXISTS h2_retention_read ON ${table}`);
      await sql`REVOKE ALL PRIVILEGES ON engine_checkpoints,durable_transitions,durable_sequence_metadata
        FROM g2h_h2_retention_reader`;
      await sql`REVOKE ALL PRIVILEGES ON SCHEMA public FROM g2h_h2_retention_reader`;
      await sql`DROP ROLE IF EXISTS g2h_h2_retention_reader`;
      await sql.end();
    }
    await admin?.close();
    if(dir)await rm(dir,{recursive:true,force:true});
  },20000);

  it("H2-RETENTION-01 ambiguous ACK, parity-rebase before cleanup, lagging consumer HALT, resumed semantic order",{
    timeout:45000,
  },async()=>{
    const initialLedger=new PaperLedger("1000",new Date(T));
    const checkpoint1={schemaVersion:CHECKPOINT_SCHEMA_VERSION,durableCursor:1,
      experiment:experiment(),ledger:initialLedger.snapshot(),positions:[],
      resultAccounting:emptyResultAccounting(),restingOrders:[],makerReservations:{},
      riskReductionOwners:[],riskReductionFailures:[],reversalProtectionStates:[],
      profitTrailingPairStates:[],settlementLookups:[],settledMarkets:[],
      nakedDirectionalDiagnostics:[],activeTriggers:[],grokSignals:[],
      lastInfrastructureAccrualAt:0};
    let cursor=0;
    const commit=async(type:string,key:string,payload:Record<string,unknown>,
      aggregateId=M,aggregateType="position")=>{
      const row=await admin.commitDurableTransition({
        type,operationKey:key,aggregateType,aggregateId,payload,
      },cursor);
      expect(row.sequence).toBe(cursor+1);
      cursor++;
      return row;
    };
    await commit("LEDGER_MARK_TO_MARKET","h2:retention:seed",
      {prices:{},markedAt:T});
    const cp1=await admin.saveCheckpoint({experimentId:E,sequence:1,payload:checkpoint1});
    const cfg=(ack:GuardianPgAckStore)=>({source:reader,ack,scope,
      experimentId:E,authorityId:"h2-retention-scope",pageSize:2,maxBacklogRows:80});
    const primaryAck=new GuardianPgFileAckStore(join(dir,"primary.guardian-pg-ack.json"));
    const staleAck=new GuardianPgFileAckStore(join(dir,"stale.guardian-pg-ack.json"));
    const beforeAck=new GuardianPgFileAckStore(join(dir,"precrash.guardian-pg-ack.json"));
    const afterAck=new GuardianPgFileAckStore(join(dir,"postcrash.guardian-pg-ack.json"));
    let healthy=await GuardianTrustedPgSession.open(cfg(primaryAck));
    const stale=await GuardianTrustedPgSession.open(cfg(staleAck));
    expect(stale.cursor).toBe(1);
    const buy=paperFill(M,"UP","0.40",{
      id:"h2-retention-buy",orderId:"h2-buy",quantity:"5",
      timestamp:T,feeUsd:"0",slippageUsd:"0",
    });
    const sell=paperFill(M,"UP","0.50",{
      id:"h2-retention-sell",orderId:"h2-sell",side:"SELL",quantity:"1",
      timestamp:"2026-10-11T10:00:05.000Z",feeUsd:"0",slippageUsd:"0",
    });
    const maker={order:{
      id:"h2-maker",traceId:"h2-trace",triggerId:"h2-trigger",
      strategyId:"temporal-pair-hedge",marketId:M,tokenId:M+"-down",
      outcome:"DOWN",side:"BUY",orderType:"MAKER",requestedPrice:"0.3",
      quantity:"2",status:"RESTING",createdAt:T,
    },queueAhead:"0",remaining:"2",observedAt:T,intent:"HEDGE_SECOND_LEG"};
    await commit("PAPER_FILL","fill:"+buy.id,
      {fill:buy,positionId:"pos:"+M,reservationAfter:null},buy.id,"paper_fill");
    await commit("PAPER_MAKER_RESERVED","h2-retention-maker-reserve",
      {orderId:"h2-maker",reserved:"0.6",resting:maker,intent:"HEDGE_SECOND_LEG"});
    await commit("PAPER_ORDER_CANCELLED","h2-retention-maker-cancel",
      {orderId:"h2-maker",cancel:true,reason:"OFFLINE_CANCEL"});
    await commit("PAPER_FILL","fill:"+sell.id,
      {fill:sell,positionId:"pos:"+M,reservationAfter:null},sell.id,"paper_fill");
    expect(cursor).toBe(5);

    // Inject before and after the *real local fsync/rename* using an
    // ACK store wrapper. No production filesystem is touched.
    const crashBefore:GuardianPgAckStore={
      load:()=>beforeAck.load(),
      commit:async(expected,next,rebase)=>{
        if(next.cursor>1)throw Error("SIMULATED_CRASH_BEFORE_ACK_RENAME");
        await beforeAck.commit(expected,next,rebase);
      },
    };
    const crashAfter:GuardianPgAckStore={
      load:()=>afterAck.load(),
      commit:async(expected,next,rebase)=>{
        await afterAck.commit(expected,next,rebase);
        if(next.cursor>1)throw Error("SIMULATED_CRASH_AFTER_ACK_RENAME");
      },
    };
    const before=await GuardianTrustedPgSession.open(cfg(crashBefore));
    await expect(before.poll(Date.now()))
      .rejects.toThrow("SIMULATED_CRASH_BEFORE_ACK_RENAME");
    expect(before.status).toBe("HALTED");
    expect(before.cursor).toBe(1);
    expect((await beforeAck.load())?.cursor).toBe(1);
    const after=await GuardianTrustedPgSession.open(cfg(crashAfter));
    await expect(after.poll(Date.now()))
      .rejects.toThrow("SIMULATED_CRASH_AFTER_ACK_RENAME");
    expect(after.status).toBe("HALTED");
    expect(after.cursor).toBe(1);
    expect((await afterAck.load())?.cursor).toBe(3);
    const afterRestart=await GuardianTrustedPgSession.open(cfg(afterAck));
    expect(afterRestart.cursor).toBe(3);
    while(afterRestart.cursor<5)await afterRestart.poll(Date.now());
    expect((await afterAck.load())?.cursor).toBe(5);

    while(healthy.cursor<5)await healthy.poll(Date.now());
    await healthy.poll(Date.now());
    const baseline=healthy.readVerifiedView(Date.now());
    expect(baseline.open[0]?.residualQty).toBe("4");
    expect(baseline.makerReservations).toEqual([]);
    expect(baseline.restingOrders).toEqual([]);
    expect((await primaryAck.load())?.cursor).toBe(5);

    const source=await reader.readGuardianConsistentPage({
      ...scope,experimentId:E,afterSequence:1,limit:20,
      pinnedCheckpointId:cp1.id,
    });
    const old=source.checkpoints.find(x=>x.id===cp1.id)!;
    let recovered=bootstrapGuardianOfflineSource({
      payload:old.payload,externallyVerifiedContentHash:old.contentHash!,
      authorityId:"h2-retention-scope",
    });
    for(const row of source.rows)recovered=advanceGuardianOfflineSource(recovered,row);
    expect(recovered.projection.cursor).toBe(5);
    expect(hashGuardianRecoverableState(recovered)).toBe(baseline.economicHash);
    const oracle=new PaperLedger("1000",new Date(T));
    oracle.applyFill(buy,{positionId:"pos:"+M});
    oracle.applyFill(sell,{positionId:"pos:"+M});
    expect(recovered.recovery.ledger.snapshot()).toEqual(oracle.snapshot());

    // Two newer independently verified checkpoints make the *second
    // most recent checkpoint* a legal maintenance compaction fence.
    const cp2=await admin.saveCheckpoint({
      experimentId:E,sequence:2,payload:body(5,recovered.recovery),
    });
    const cp3=await admin.saveCheckpoint({
      experimentId:E,sequence:3,payload:body(5,recovered.recovery),
    });
    // The real Engine and CLI derive their retention configuration from
    // EngineEnv. Even if operator enables retention + compaction and
    // sets checkpointKeep=1, G2H must HOLD all destructive recovery
    // cleanup until consumer pin coordination exists.
    const livePolicy=retentionConfigFromEngineEnv(qualifiedPaperEnv({
      RETENTION_ENABLED:"true",
      RETENTION_COMPACTION_ENABLED:"true",
      RETENTION_CHECKPOINT_KEEP:"1",
    }));
    expect(livePolicy.guardianRecoveryHold).toBe(true);
    expect(defaultRetentionConfig().guardianRecoveryHold).toBe(true);
    // Holding both cleanup paths must be the default even when a future
    // caller constructs a manager without EngineEnv.
    const managed=new RetentionManager(admin,{
      ...livePolicy,enabled:true,compactionEnabled:true,checkpointKeep:1,
      localArchiveEnabled:false,analyticsRollupEnabled:false,
      cycleBudgetMs:10000,maxRowsPerCycle:100,maxBatchesPerCycle:5,
    },undefined,null);
    const automated=await managed.runCycle({
      experiment:{id:E,status:"RUNNING",startedAt:T},
      now:new Date(),degraded:false,failClosed:false,databaseUp:true,
    });
    expect(automated.failures).toBe(0);
    expect(automated.guardianRecoveryHeld).toBe(true);
    expect(automated.deletedByClass.checkpoint).toBe(0);
    expect(automated.deletedByClass.durable_transition).toBe(0);
    expect(automated.durableTransitionsPruned).toBe(false);
    expect((await sql`SELECT count(*)::int AS total FROM engine_checkpoints`)[0]?.total).toBe(3);
    expect((await sql`SELECT count(*)::int AS total FROM durable_transitions`)[0]?.total).toBe(5);
    // The next direct maintenance calls deliberately BYPASS RetentionManager
    // to show exactly what would strand an uncoordinated consumer.
    await healthy.poll(Date.now());
    const adopted=await primaryAck.load();
    expect([cp2.id,cp3.id]).toContain(adopted?.checkpointId);
    expect(adopted?.checkpointId).not.toBe(cp1.id);
    healthy=await GuardianTrustedPgSession.open(cfg(primaryAck));
    await healthy.poll(Date.now());
    expect(healthy.readVerifiedView(Date.now()).open[0]?.residualQty).toBe("4");

    // Actual canonical SQL compactor: takes advisory mutation lock and
    // preserves replay-key idempotency records, removes the old rows.
    const compacted=await admin.compactDurableTransitionsThrough(5,{
      maxReplayKeys:4096,ttlMs:7*24*60*60_000,
      maxRows:25,maxBatches:1,batchSize:25,budgetMs:20000,
    });
    expect(compacted.deleted).toBe(5);
    // With its old cp STILL PRESENT, the stale reader already
    // detects the missing global transition prefix. Pruning the
    // pinned checkpoint is a second, separate fail-closed condition.
    const prePrunePinned=await GuardianTrustedPgSession.open(cfg(staleAck));
    await expect(prePrunePinned.poll(Date.now()))
      .rejects.toThrow("G2H_PG_DURABLE_REPLAY_GAP");
    expect(prePrunePinned.status).toBe("HALTED");
    expect((await staleAck.load())?.cursor).toBe(1);
    const pruned=await admin.pruneOldCheckpoints(2,25);
    expect(pruned.deleted).toBe(1);
    const rows=await sql`SELECT count(*)::int AS total FROM durable_transitions`;
    expect(Number(rows[0]!.total)).toBe(0);
    const replayKeys=await sql`SELECT count(*)::int AS total FROM durable_replay_keys`;
    expect(Number(replayKeys[0]!.total)).toBe(5);
    const retained=await GuardianTrustedPgSession.open(cfg(primaryAck));
    expect(retained.cursor).toBe(5);
    await retained.poll(Date.now());
    expect(retained.readVerifiedView(Date.now()).open[0]?.residualQty).toBe("4");

    // Critical missing retention coordination: a separate legitimate
    // consumer still pinned to cp1 must not silently jump to cp2/cp3.
    const staleReceipt=await staleAck.load();
    await expect(GuardianTrustedPgSession.open(cfg(staleAck))).rejects.toThrow();
    expect(await staleAck.load()).toEqual(staleReceipt);
    await expect(stale.poll(Date.now())).rejects.toThrow();
    expect(stale.status).toBe("HALTED");

    const nextSell=paperFill(M,"UP","0.50",{
      id:"h2-retention-sell-again",orderId:"h2-sell-2",side:"SELL",
      quantity:"1",timestamp:"2026-10-11T10:00:08.000Z",
      feeUsd:"0",slippageUsd:"0",
    });
    await commit("PAPER_FILL","fill:"+nextSell.id,
      {fill:nextSell,positionId:"pos:"+M,reservationAfter:null},
      nextSell.id,"paper_fill");
    oracle.applyFill(nextSell,{positionId:"pos:"+M});
    expect((await retained.poll(Date.now())).cursor).toBe(6);
    expect(retained.readVerifiedView(Date.now()).open[0]?.residualQty).toBe("3");
    const final=await GuardianTrustedPgSession.open(cfg(primaryAck));
    await final.poll(Date.now());
    expect(final.readVerifiedView(Date.now()).economicHash)
      .toBe(retained.readVerifiedView(Date.now()).economicHash);
    expect((await primaryAck.load())?.cursor).toBe(6);
    expect(oracle.snapshot().realizedPnl).toBe("0.2");
    console.log("G2H_H2_RETENTION_PROOF "+JSON.stringify({
      compactionDeleted:compacted.deleted,
      retainedReplayKeys:Number(replayKeys[0]!.total),
      prunedCheckpoints:pruned.deleted,
      goodConsumerCursor:final.cursor,badConsumerHalted:true,
      preAckCrashRecovered:true,postAckCrashRecovered:true,
      trueSqlRebase:true,remainingResidual:"3",
    }));
  });
});

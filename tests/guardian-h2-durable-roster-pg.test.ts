import {createRequire} from "node:module";
import {fileURLToPath} from "node:url";
import {beforeAll,afterAll,describe,expect,it} from "vitest";
import {createIdleExperiment,PaperLedger} from "@pm/core";
import {
  PostgresEventJournal,PostgresGuardianRecoveryRegistry,
  CHECKPOINT_SCHEMA_VERSION,
} from "@pm/db";
import {emptyResultAccounting} from "../apps/engine/src/trading-results.js";
import {defaultRetentionConfig} from "../apps/engine/src/retention-manager.js";

const postgres=createRequire(new URL("../packages/db/package.json",import.meta.url))("postgres");
const migrations=fileURLToPath(new URL("../packages/db/migrations",import.meta.url));
const RAW=process.env.G2H_H2_ROSTER_LOCAL_DATABASE_URL;
const DB="g2h_h2_roster_local";
const SOURCE="local:g2h:roster:isolated";
const guard=()=>{
  if(!RAW)throw Error("G2H_ROSTER_NEEDS_DISPOSABLE_LOCAL_URL");
  const u=new URL(RAW);
  if(u.hostname!=="127.0.0.1"||u.port!=="55487"
    ||u.pathname!=="/"+DB||u.username!=="g2h_admin")
    throw Error("G2H_ROSTER_REFUSES_NONLOCAL_DB");
  return u;
};
let journal:PostgresEventJournal;
let sql:ReturnType<typeof postgres>;
let registry:PostgresGuardianRecoveryRegistry;
let readonly:PostgresGuardianRecoveryRegistry;
let cpId="",cpHash="";
describe.skipIf(!RAW)("H2 new durable roster against canonical PostgresEventJournal",()=>{
  beforeAll(async()=>{
    const url=guard().toString();
    journal=new PostgresEventJournal(url,migrations);
    await journal.initialize();
    sql=postgres(url,{max:3});
    registry=new PostgresGuardianRecoveryRegistry(url,SOURCE,"maintenance");
    readonly=new PostgresGuardianRecoveryRegistry(url,SOURCE,"readonly");
    const exp={...createIdleExperiment({
      durationHours:1,strategyVersion:"h2-pin",riskConfiguration:{},
      modelConfiguration:{},startingBalance:"1000",systemVersion:"local",
      gitSha:"local",
    }),id:"h2-durable-roster",status:"RUNNING",startedAt:"2026-10-11T10:00:00.000Z"};
    const row=await journal.commitDurableTransition({
      operationKey:"h2:roster:mark:seed",aggregateId:"h2:roster:test",
      aggregateType:"position",type:"LEDGER_MARK_TO_MARKET",
      payload:{prices:{},markedAt:"2026-10-11T10:00:00.000Z"},
    },0);
    expect(row.sequence).toBe(1);
    const cp=await journal.saveCheckpoint({
      experimentId:exp.id,sequence:1,
      payload:{
        schemaVersion:CHECKPOINT_SCHEMA_VERSION,durableCursor:1,
        experiment:exp,ledger:new PaperLedger("1000",new Date(exp.startedAt)).snapshot(),
        positions:[],resultAccounting:emptyResultAccounting(),
        restingOrders:[],makerReservations:{},riskReductionOwners:[],
        riskReductionFailures:[],reversalProtectionStates:[],
        profitTrailingPairStates:[],settlementLookups:[],settledMarkets:[],
        activeTriggers:[],grokSignals:[],nakedDirectionalDiagnostics:[],
        lastInfrastructureAccrualAt:0,
      },
    });
    cpId=cp.id;cpHash=cp.contentHash!;
    expect(cp.verified).toBe(true);
  },60000);
  afterAll(async()=>{
    await readonly?.close();await registry?.close();await sql?.end();
    await journal?.close();
  },30000);
  it("H2-ROSTER-01 migration RLS, empty block, scoped registration and immutable sealed nonempty set",async()=>{
    const tables=await sql`SELECT tablename,rowsecurity AS x
      FROM pg_tables WHERE schemaname='public'
        AND tablename LIKE 'guardian_recovery_%'`;
    expect(tables.map((r:any)=>r.tablename).sort())
      .toEqual(["guardian_recovery_pins","guardian_recovery_roster"]);
    expect(tables.every((r:any)=>r.x===true)).toBe(true);
    expect((await registry.inspect()).state).toBe("BLOCKED");
    const input=[
      {consumerId:"guardian-primary",experimentId:"h2-durable-roster",
        checkpointId:cpId,expectedCheckpointHash:cpHash,authorityEpoch:1},
      {consumerId:"guardian-secondary",experimentId:"h2-durable-roster",
        checkpointId:cpId,expectedCheckpointHash:cpHash,authorityEpoch:1},
    ];
    await expect(readonly.enrollAndSeal(input)).rejects.toThrow("JOURNAL_MUTATE_FORBIDDEN");
    await expect(registry.enrollAndSeal([{...input[0]!,
      expectedCheckpointHash:"0".repeat(64)}])).rejects.toThrow("CHECKPOINT_UNVERIFIED");
    expect((await registry.inspect()).state).toBe("BLOCKED");
    const enrolled=await registry.enrollAndSeal(input);
    expect(enrolled).toHaveLength(2);
    expect((await registry.inspect()).state).toBe("SEALED_PINNED");
    await expect(registry.enrollAndSeal(input)).rejects.toThrow("ROSTER_ALREADY_INITIALIZED");
    const other=new PostgresGuardianRecoveryRegistry(guard().toString(),
      "other:source:identifier","maintenance");
    try{expect((await other.inspect()).state).toBe("BLOCKED");}
    finally{await other.close();}
    expect(defaultRetentionConfig().guardianRecoveryHold).toBe(true);
  });
  it("H2-ROSTER-02 real SQL FK prevents pinned checkpoint DELETE by legacy maintenance",async()=>{
    const cp2=await journal.saveCheckpoint({
      experimentId:"h2-durable-roster",sequence:2,payload:{
        schemaVersion:CHECKPOINT_SCHEMA_VERSION,durableCursor:1,
      },
    });
    const cp3=await journal.saveCheckpoint({
      experimentId:"h2-durable-roster",sequence:3,payload:{
        schemaVersion:CHECKPOINT_SCHEMA_VERSION,durableCursor:1,
      },
    });
    expect(cp2.id).not.toBe(cp3.id);
    await expect(journal.pruneOldCheckpoints(1,10)).rejects.toThrow();
    expect((await sql`SELECT id FROM engine_checkpoints
      WHERE id=${cpId}`)).toHaveLength(1);
    expect((await registry.inspect()).state).toBe("SEALED_PINNED");
    // No runtime pin-advance exists: a local ACK must NOT silently
    // move a durable pin when its filesystem can roll back.
    const pins=await sql`SELECT checkpoint_id,revision FROM
      guardian_recovery_pins ORDER BY consumer_id`;
    expect(pins).toHaveLength(2);
    expect(pins.every((r:any)=>r.checkpoint_id===cpId&&Number(r.revision)===1)).toBe(true);
  });
  it("H2-ROSTER-04 sealed membership completeness detects missing required consumer",async()=>{
    const [one]=await sql`SELECT * FROM guardian_recovery_pins
      WHERE consumer_id='guardian-secondary'`;
    expect(one?.consumer_id).toBe("guardian-secondary");
    await sql`DELETE FROM guardian_recovery_pins
      WHERE consumer_id='guardian-secondary'`;
    try{
      expect(await registry.inspect()).toMatchObject({
        state:"BLOCKED",reason:"ROSTER_MEMBERSHIP_MISMATCH",
      });
    }finally{
      await sql`INSERT INTO guardian_recovery_pins
        (consumer_id,experiment_id,checkpoint_id,checkpoint_hash,
        checkpoint_cursor,authority_epoch,revision,required,enrolled_at)
        VALUES(${one.consumer_id},${one.experiment_id},
          ${one.checkpoint_id},${one.checkpoint_hash},
          ${one.checkpoint_cursor},${one.authority_epoch},
          ${one.revision},${one.required},${one.enrolled_at})`;
    }
    expect((await registry.inspect()).state).toBe("SEALED_PINNED");
  });
  it("H2-ROSTER-05 a privileged checkpoint mutation invalidates sealed pin integrity",async()=>{
    const [original]=await sql`SELECT payload FROM engine_checkpoints
      WHERE id=${cpId}`;
    expect(original?.payload).toBeDefined();
    const changed=structuredClone(original.payload) as Record<string,unknown>;
    changed.durableCursor=0; // historical anchor tamper
    await sql`UPDATE engine_checkpoints SET payload=${sql.json(changed)}
      WHERE id=${cpId}`;
    try{
      expect(await registry.inspect()).toMatchObject({
        state:"BLOCKED",reason:"PINNED_CHECKPOINT_UNVERIFIABLE",
      });
    }finally{
      await sql`UPDATE engine_checkpoints SET payload=${sql.json(original.payload)}
        WHERE id=${cpId}`;
    }
    expect((await registry.inspect()).state).toBe("SEALED_PINNED");
  });
  it("H2-ROSTER-03 Guardian read-only SQL principal cannot read or change durable roster",async()=>{
    await sql`CREATE ROLE g2h_roster_observer LOGIN PASSWORD 'local_only_roster_reader'`;
    await sql`GRANT USAGE ON SCHEMA public TO g2h_roster_observer`;
    await sql`GRANT SELECT ON engine_checkpoints,durable_transitions,durable_sequence_metadata
      TO g2h_roster_observer`;
    const rUrl=guard();rUrl.username="g2h_roster_observer";
    rUrl.password="local_only_roster_reader";
    const conn=postgres(rUrl.toString(),{max:1});
    try{
      await expect(conn`SELECT * FROM guardian_recovery_pins`)
        .rejects.toThrow();
      await expect(conn`DELETE FROM guardian_recovery_pins`)
        .rejects.toThrow();
      await expect(conn`INSERT INTO guardian_recovery_roster
        (id,source_identity,sealed) VALUES(2,'faked',true)`).rejects.toThrow();
    }finally{
      await conn.end();
      await sql`REVOKE ALL PRIVILEGES ON engine_checkpoints,durable_transitions,durable_sequence_metadata
        FROM g2h_roster_observer`;
      await sql`REVOKE ALL PRIVILEGES ON SCHEMA public FROM g2h_roster_observer`;
      await sql`DROP ROLE g2h_roster_observer`;
    }
    expect((await registry.inspect()).state).toBe("SEALED_PINNED");
  });
});

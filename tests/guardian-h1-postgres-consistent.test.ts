import { fileURLToPath } from "node:url";
import {mkdtemp,rm} from "node:fs/promises";
import {tmpdir} from "node:os";
import {join} from "node:path";
import { createRequire } from "node:module";
// Reuse the exact postgres.js already owned by @pm/db; do not add packages.
const postgres=createRequire(new URL("../packages/db/package.json",import.meta.url))("postgres");
import { beforeAll, afterAll, describe, expect, it } from "vitest";
import { createIdleExperiment, PaperLedger } from "@pm/core";
import { CHECKPOINT_SCHEMA_VERSION, PostgresEventJournal } from "@pm/db";
import { emptyResultAccounting } from "../apps/engine/src/trading-results.js";
import { bootstrapGuardianTrustedPg } from "../apps/engine/src/guardian-trusted-pg-bootstrap.js";
import { GuardianTrustedPgSession } from "../apps/engine/src/guardian-trusted-pg-session.js";
import { GuardianPgFileAckStore } from "../apps/engine/src/guardian-pg-ack-store.js";

/**
 * Only a disposable, loopback PostgreSQL container is allowed.
 * NEVER read ordinary DATABASE_URL or any production credential.
 */
const rawUrl=process.env.G2H_LOCAL_TEST_DATABASE_URL;
function guardedUrl(){
  if(!rawUrl)throw Error("G2H_DISPOSABLE_URL_MISSING");
  const url=new URL(rawUrl);
  if(!["localhost","127.0.0.1"].includes(url.hostname)
    || url.port!=="55481" || url.pathname!=="/g2h_local_20261011"
    || url.username!=="g2h_admin")
    throw Error("G2H_TEST_REFUSES_NON_DISPOSABLE_POSTGRES");
  return url;
}
const migrations=fileURLToPath(new URL("../packages/db/migrations",import.meta.url));
const R="g2h_readonly_20261011";
const PASSWORD="g2h_reader_local_only_20261011";
const EXPECT={database:"g2h_local_20261011",schema:"public",role:R,
  experimentId:"g2h_h1_exp_20261011",afterSequence:1,limit:10};
let admin:PostgresEventJournal;
let sql:ReturnType<typeof postgres>;
let reader:PostgresEventJournal;
const noDb=describe.skipIf(!rawUrl);

noDb("G2H-H1 real isolated PostgreSQL: repeatable read + actual SELECT-only role",()=>{
  beforeAll(async()=>{
    const adminUrl=guardedUrl();
    sql=postgres(adminUrl.toString(),{max:2});
    admin=new PostgresEventJournal(adminUrl.toString(),migrations);
    await admin.initialize();
    const experiment={...createIdleExperiment({
      durationHours:1,strategyVersion:"g2h-test",riskConfiguration:{},
      modelConfiguration:{},startingBalance:"1000",systemVersion:"test",gitSha:"offline",
    }),id:EXPECT.experimentId,status:"RUNNING" as const,
      startedAt:"2026-10-11T00:00:00Z"};
    await admin.commitDurableTransition({
      operationKey:"g2h-h1-mtm:1",type:"LEDGER_MARK_TO_MARKET",
      aggregateType:"position",aggregateId:"h1-position",
      payload:{prices:{},markedAt:"2026-10-11T00:00:01Z"},
    },0);
    const ledger=new PaperLedger("1000",new Date("2026-10-11T00:00:00Z"));
    await admin.saveCheckpoint({experimentId:experiment.id,sequence:1,payload:{
      schemaVersion:CHECKPOINT_SCHEMA_VERSION,durableCursor:1,
      experiment,ledger:ledger.snapshot(),positions:[],resultAccounting:emptyResultAccounting(),
      restingOrders:[],makerReservations:{},activeTriggers:[],grokSignals:[],
      riskReductionOwners:[],riskReductionFailures:[],reversalProtectionStates:[],
      profitTrailingPairStates:[],settlementLookups:[],settledMarkets:[],
      nakedDirectionalDiagnostics:[],lastInfrastructureAccrualAt:0,
    }});
    await admin.commitDurableTransition({
      operationKey:"g2h-h1-mtm:2",type:"LEDGER_MARK_TO_MARKET",
      aggregateType:"position",aggregateId:"h1-position",
      payload:{prices:{},markedAt:"2026-10-11T00:00:02Z"},
    },1);
    await sql`CREATE ROLE g2h_readonly_20261011 LOGIN PASSWORD 'g2h_reader_local_only_20261011'`;
    await sql`GRANT USAGE ON SCHEMA public TO g2h_readonly_20261011`;
    await sql`GRANT SELECT ON engine_checkpoints,durable_transitions,durable_sequence_metadata
      TO g2h_readonly_20261011`;
    // The existing schema ENABLES RLS without public SELECT policies:
    // a GRANT alone exposes ZERO rows to the actual restricted observer.
    // Policies below exist ONLY inside the disposable localhost database.
    await sql`CREATE POLICY g2h_h1_read ON engine_checkpoints
      FOR SELECT TO g2h_readonly_20261011 USING (true)`;
    await sql`CREATE POLICY g2h_h1_read ON durable_transitions
      FOR SELECT TO g2h_readonly_20261011 USING (true)`;
    await sql`CREATE POLICY g2h_h1_read ON durable_sequence_metadata
      FOR SELECT TO g2h_readonly_20261011 USING (true)`;
    const readerUrl=new URL(adminUrl);
    readerUrl.username=R;readerUrl.password=PASSWORD;
    reader=new PostgresEventJournal(readerUrl.toString(),migrations,{access:"readonly"});
    await reader.connectWithoutMigrate("readonly");
  },60000);
  afterAll(async()=>{
    await reader?.close();
    if(sql){
      await sql`DROP POLICY IF EXISTS g2h_h1_read ON engine_checkpoints`;
      await sql`DROP POLICY IF EXISTS g2h_h1_read ON durable_transitions`;
      await sql`DROP POLICY IF EXISTS g2h_h1_read ON durable_sequence_metadata`;
      await sql`REVOKE ALL PRIVILEGES ON engine_checkpoints,durable_transitions,durable_sequence_metadata
        FROM g2h_readonly_20261011`;
      await sql`REVOKE ALL PRIVILEGES ON SCHEMA public FROM g2h_readonly_20261011`;
      await sql`DROP ROLE IF EXISTS g2h_readonly_20261011`;
      await sql.end();
    }
    await admin?.close();
  },20000);

  it("H1-01 reads committed checkpoint and contiguous tail with role identity",async()=>{
    const page=await reader.readGuardianConsistentPage(EXPECT);
    expect(page.source).toEqual({database:EXPECT.database,schema:EXPECT.schema,role:R});
    expect(page.highWater).toBe(2);
    expect(page.checkpoints).toHaveLength(1);
    expect(page.checkpoints[0]).toMatchObject({experimentId:EXPECT.experimentId,verified:true});
    expect(page.rows.map(x=>x.sequence)).toEqual([2]);
    expect(page.rows[0]?.operationKey).toBe("g2h-h1-mtm:2");
  });
  it("H1-02 rejects wrong schema/database/role, bounds and writer-mode journal",async()=>{
    for(const override of [{database:"not_this_database"},{schema:"not_this_schema"},
      {role:"g2h_admin"}]){
      await expect(reader.readGuardianConsistentPage({...EXPECT,...override}))
        .rejects.toThrow("G2H_PG_SOURCE_IDENTITY_OR_SNAPSHOT_UNTRUSTED");
    }
    await expect(reader.readGuardianConsistentPage({...EXPECT,limit:513}))
      .rejects.toThrow("G2H_PG_INVALID_SCOPE_OR_BOUNDS");
    await expect(admin.readGuardianConsistentPage({...EXPECT,role:"g2h_admin"}))
      .rejects.toThrow("G2H_PG_REQUIRES_READONLY_JOURNAL");
  });
  it("H1-03 actual reader DB principal CANNOT INSERT/UPDATE/DELETE/DDL",async()=>{
    const uri=guardedUrl();uri.username=R;uri.password=PASSWORD;
    const raw=postgres(uri.toString(),{max:1});
    try{
      await expect(raw`INSERT INTO durable_transitions
        (sequence,operation_key,type,aggregate_type,aggregate_id,payload_hash,payload,occurred_at)
        VALUES (99,'attack','PAPER_FILL','x','x','bad','{}',now())`)
        .rejects.toMatchObject({code:"42501"});
      await expect(raw`UPDATE durable_sequence_metadata SET high_water=999 WHERE id=1`)
        .rejects.toMatchObject({code:"42501"});
      await expect(raw`DELETE FROM engine_checkpoints`)
        .rejects.toMatchObject({code:"42501"});
      await expect(raw`CREATE TABLE g2h_escape_test(id int)`)
        .rejects.toMatchObject({code:"42501"});
    }finally{await raw.end();}
    expect(await reader.readGuardianConsistentPage(EXPECT)).toHaveProperty("highWater",2);
  });
  it("H1-04 revoking read privilege or granting any writer access fails closed",async()=>{
    await sql`REVOKE SELECT ON durable_sequence_metadata FROM g2h_readonly_20261011`;
    try{
      await expect(reader.readGuardianConsistentPage(EXPECT))
        .rejects.toThrow();
    }finally{
      await sql`GRANT SELECT ON durable_sequence_metadata TO g2h_readonly_20261011`;
    }
    await sql`GRANT UPDATE ON durable_sequence_metadata TO g2h_readonly_20261011`;
    try{
      await expect(reader.readGuardianConsistentPage(EXPECT))
        .rejects.toThrow("G2H_PG_ROLE_NOT_SELECT_ONLY");
    }finally{
      await sql`REVOKE UPDATE ON durable_sequence_metadata FROM g2h_readonly_20261011`;
    }
  });
  it("H1-05 PostgreSQL MVCC snapshot is stable across a committed writer update",async()=>{
    const uri=guardedUrl();uri.username=R;uri.password=PASSWORD;
    const raw=postgres(uri.toString(),{max:1});
    try{
      await raw.begin("isolation level repeatable read read only",async tx=>{
        const a=await tx`SELECT high_water FROM durable_sequence_metadata WHERE id=1`;
        expect(Number(a[0]?.high_water)).toBe(2);
        await admin.commitDurableTransition({
          operationKey:"g2h-h1-mtm:3",type:"LEDGER_MARK_TO_MARKET",
          aggregateType:"position",aggregateId:"h1-position",
          payload:{prices:{},markedAt:"2026-10-11T00:00:03Z"},
        },2);
        const b=await tx`SELECT high_water FROM durable_sequence_metadata WHERE id=1`;
        expect(Number(b[0]?.high_water)).toBe(2);
        const page=await tx`SELECT sequence FROM durable_transitions ORDER BY sequence`;
        expect(page.map(x=>Number(x.sequence))).toEqual([1,2]);
      });
      const next=await reader.readGuardianConsistentPage(EXPECT);
      expect(next.highWater).toBe(3);
      expect(next.rows.map(x=>x.sequence)).toEqual([2,3]);
    }finally{await raw.end();}
  });
  it("H1-06 rejects replay cursor ahead of committed source",async()=>{
    await expect(reader.readGuardianConsistentPage({...EXPECT,afterSequence:10}))
      .rejects.toThrow("G2H_PG_SOURCE_CURSOR_REGRESSION");
  });

  it("H1-07 fails closed if SELECT GRANT remains but RLS hides global cursor",async()=>{
    await sql`DROP POLICY g2h_h1_read ON durable_sequence_metadata`;
    try{
      await expect(reader.readGuardianConsistentPage(EXPECT))
        .rejects.toThrow("G2H_PG_MISSING_SEQUENCE_METADATA");
    }finally{
      await sql`CREATE POLICY g2h_h1_read ON durable_sequence_metadata
        FOR SELECT TO g2h_readonly_20261011 USING (true)`;
    }
  });
  it("H1-08 rejects a corrupted persisted checkpoint hash",async()=>{
    const before=await sql`SELECT id,payload FROM engine_checkpoints
      WHERE experiment_id=${EXPECT.experimentId}`;
    expect(before).toHaveLength(1);
    await sql`UPDATE engine_checkpoints
      SET payload=jsonb_set(payload,'{checkpointContentHash}',to_jsonb('corrupt-hash'::text))
      WHERE experiment_id=${EXPECT.experimentId}`;
    try{
      await expect(reader.readGuardianConsistentPage(EXPECT))
        .rejects.toThrow("G2H_PG_CHECKPOINT_INTEGRITY_OR_WATERMARK");
    }finally{
      await sql`UPDATE engine_checkpoints SET payload=${sql.json(before[0]!.payload)}
        WHERE id=${before[0]!.id}`;
    }
  });
  it("H1-09 boots real recovered Guardian economics from trusted PG WITHOUT signer",async()=>{
    const result=await bootstrapGuardianTrustedPg({
      source:reader,
      sourceIdentity:{database:EXPECT.database,schema:EXPECT.schema,role:EXPECT.role},
      experimentId:EXPECT.experimentId,authorityId:"operator-pinned-paper-db",
      pageSize:10,maxBacklogRows:100,
    });
    expect(result.caughtUp).toBe(true);
    expect(result.highWater).toBe(3);
    expect(result.state.projection.cursor).toBe(3);
    expect(result.state.projection.open.size).toBe(0);
    expect(result.state.recovery.riskReductionOwners.size).toBe(0);
    expect(result.state.seenOperationKeys.size).toBe(2);
    await expect(bootstrapGuardianTrustedPg({
      source:reader,sourceIdentity:{database:EXPECT.database,schema:EXPECT.schema,role:EXPECT.role},
      experimentId:EXPECT.experimentId,authorityId:"operator-pinned-paper-db",
      pageSize:1,maxBacklogRows:1,
    })).rejects.toThrow("G2H_PG_BACKLOG_LIMIT");
  });
  it("H2-REAL-PG-01 reads multiple live PG pages and reconstructs ACK after restart",async()=>{
    const dir=await mkdtemp(join(tmpdir(),"g2h-h2-pg-"));
    const ack=new GuardianPgFileAckStore(join(dir,"consumer.guardian-pg-ack.json"));
    try{
      const input={
        source:reader,ack,scope:{database:EXPECT.database,schema:EXPECT.schema,role:EXPECT.role},
        experimentId:EXPECT.experimentId,authorityId:"canonical-paper-db-reader",
        pageSize:1,maxBacklogRows:100,
      };
      const s=await GuardianTrustedPgSession.open(input);
      expect(s.cursor).toBe(1);
      expect((await ack.load())?.cursor).toBe(1);
      expect(await s.poll(1000)).toEqual({cursor:2,highWater:3,caughtUp:false});
      expect((await ack.load())?.cursor).toBe(2);
      const resumed=await GuardianTrustedPgSession.open(input);
      expect(resumed.cursor).toBe(2);
      expect(await resumed.poll(1010)).toEqual({cursor:3,highWater:3,caughtUp:true});
      expect((await ack.load())?.cursor).toBe(3);
      expect(resumed.readVerifiedView(1011).open).toEqual([]);
    }finally{await rm(dir,{recursive:true,force:true});}
  });
  it("H1-10 catches a physically missing global durable transition",async()=>{
    // Last test intentionally mutates only the disposable database.
    await sql`DELETE FROM durable_transitions WHERE sequence=2`;
    await expect(reader.readGuardianConsistentPage(EXPECT))
      .rejects.toThrow("G2H_PG_DURABLE_ROW_GAP_OR_CORRUPT");
  });
});

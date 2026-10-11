import {createRequire} from "node:module";
import {afterAll,beforeAll,beforeEach,describe,expect,it} from "vitest";
import {DURABLE_MUTATION_LOCK_ID} from "../packages/db/src/durable.js";

const postgres=createRequire(new URL("../packages/db/package.json",import.meta.url))("postgres");
const RAW=process.env.G2H_H2_PROTOCOL_LOCAL_DATABASE_URL;
const DB="g2h_h2_protocol_local";
const guard=()=>{
  if(!RAW)throw Error("H2_PROTOCOL_NEEDS_DISPOSABLE_LOCAL_DATABASE");
  const u=new URL(RAW);
  if(u.hostname!=="127.0.0.1"||u.port!=="55486"||
    u.pathname!=="/"+DB||u.username!=="g2h_admin")
    throw Error("H2_PROTOCOL_REFUSES_NONLOCAL_DATABASE");
  return u;
};
type DBClient=ReturnType<typeof postgres>;
let sql:DBClient;
const lock=async(tx:any)=>{
  await tx`SELECT pg_advisory_xact_lock(${DURABLE_MUTATION_LOCK_ID})`;
};
type Refusal="ROSTER_UNSEALED"|"NO_REQUIRED_CONSUMERS"|
  "UNKNOWN_OR_STALE_CONSUMER"|"PIN_BEFORE_DELETE_BOUNDARY"|
  "CHECKPOINT_PINNED"|"TARGET_NOT_VERIFIED"|"DELETION_UNSAFE";
class Refused extends Error{
  constructor(public readonly reason:Refusal){super("H2_PROTOCOL_"+reason);}
}
// Pure LOCAL protocol proof. This is NOT the live Journal/Guardian
// integration, nor an approved migration or privilege grant.
const bootstrap=async()=>{
  await sql`TRUNCATE h2_proto_roster,h2_proto_checkpoints,h2_proto_durable,h2_proto_state RESTART IDENTITY`;
  await sql`INSERT INTO h2_proto_state(singleton,roster_sealed) VALUES (1,false)`;
  await sql`INSERT INTO h2_proto_checkpoints(id,cursor,hash) VALUES
    ('cp1',1,repeat('a',64)),('cp2',5,repeat('b',64)),
    ('cp3',8,repeat('c',64))`;
  await sql`INSERT INTO h2_proto_durable(sequence)
    SELECT generate_series(2,9)`;
  await sql`INSERT INTO h2_proto_roster(consumer,required,checkpoint_id,epoch,revision)
    VALUES ('guardian-primary',true,'cp1',1,1),
      ('guardian-secondary',true,'cp1',1,1)`;
};
const seal=async()=>{await sql`UPDATE h2_proto_state SET roster_sealed=true WHERE singleton=1`;};
async function rebase(consumer:string,fromRev:number,fromEpoch:number,
  checkpoint:string,confirmedHash:string){
  return sql.begin(async(tx:any)=>{
    await lock(tx);
    const [r]=await tx`SELECT consumer,checkpoint_id,revision,epoch
      FROM h2_proto_roster WHERE consumer=${consumer} AND required=true`;
    if(!r||Number(r.revision)!==fromRev||Number(r.epoch)!==fromEpoch)
      throw Error("H2_PROTOCOL_STALE_EPOCH_OR_REVISION");
    const [cp]=await tx`SELECT id,hash,cursor FROM h2_proto_checkpoints
      WHERE id=${checkpoint}`;
    if(!cp||cp.hash!==confirmedHash)throw Error("H2_PROTOCOL_BAD_ANCHOR_PROOF");
    const [old]=await tx`SELECT cursor FROM h2_proto_checkpoints
      WHERE id=${r.checkpoint_id}`;
    if(!old||Number(cp.cursor)<Number(old.cursor))
      throw Error("H2_PROTOCOL_CHECKPOINT_REGRESSION");
    const changed=await tx`UPDATE h2_proto_roster
      SET checkpoint_id=${checkpoint},revision=revision+1
      WHERE consumer=${consumer} AND revision=${fromRev}
        AND epoch=${fromEpoch} AND required=true
      RETURNING revision`;
    if(changed.length!==1)throw Error("H2_PROTOCOL_LOST_CAS");
    return Number(changed[0].revision);
  });
}
async function register(consumer:string,checkpoint:string){
  return sql.begin(async(tx:any)=>{
    await lock(tx);
    const [state]=await tx`SELECT roster_sealed FROM h2_proto_state WHERE singleton=1`;
    if(!state?.roster_sealed)throw Error("H2_PROTOCOL_REGISTRATION_NOT_SEALED");
    const [registered]=await tx`SELECT consumer FROM h2_proto_roster
      WHERE consumer=${consumer}`;
    if(!registered)throw Error("H2_PROTOCOL_UNKNOWN_MEMBER");
    const [cp]=await tx`SELECT id FROM h2_proto_checkpoints WHERE id=${checkpoint}`;
    if(!cp)throw Error("H2_PROTOCOL_CHECKPOINT_NOT_RETAINED");
    return true;
  });
}
async function fencedCleanup(target:number,holdMs=0){
  return sql.begin(async(tx:any)=>{
    await lock(tx); // Same lock key as real PostgresEventJournal compaction.
    const [state]=await tx`SELECT roster_sealed FROM h2_proto_state
      WHERE singleton=1`;
    if(!state?.roster_sealed)throw new Refused("ROSTER_UNSEALED");
    const [n]=await tx`SELECT count(*)::int AS n FROM h2_proto_roster
      WHERE required=true`;
    if(Number(n?.n)<1)throw new Refused("NO_REQUIRED_CONSUMERS");
    const missing=await tx`SELECT p.consumer FROM h2_proto_roster p
      LEFT JOIN h2_proto_checkpoints c ON c.id=p.checkpoint_id
      WHERE p.required=true AND (p.checkpoint_id IS NULL OR c.id IS NULL)`;
    if(missing.length)throw new Refused("UNKNOWN_OR_STALE_CONSUMER");
    const [targetCp]=await tx`SELECT id FROM h2_proto_checkpoints
      WHERE cursor=${target}`;
    if(!targetCp)throw new Refused("TARGET_NOT_VERIFIED");
    const danger=await tx`SELECT p.consumer FROM h2_proto_roster p
      JOIN h2_proto_checkpoints c ON c.id=p.checkpoint_id
      WHERE p.required=true AND c.cursor<${target}`;
    if(danger.length)throw new Refused("PIN_BEFORE_DELETE_BOUNDARY");
    const pinned=await tx`SELECT consumer FROM h2_proto_roster
      WHERE required=true AND checkpoint_id='cp1'`;
    if(pinned.length)throw new Refused("CHECKPOINT_PINNED");
    if(holdMs>0)await tx`SELECT pg_sleep(${holdMs/1000})`;
    const deleted=await tx`DELETE FROM h2_proto_durable
      WHERE sequence<=${target} RETURNING sequence`;
    const dropped=await tx`DELETE FROM h2_proto_checkpoints
      WHERE cursor<${target}
        AND NOT EXISTS(SELECT 1 FROM h2_proto_roster p
          WHERE p.required=true AND p.checkpoint_id=h2_proto_checkpoints.id)
      RETURNING id`;
    return {deleted:deleted.length,pruned:dropped.length};
  });
}
const checkpointHash=(cp:string)=>"abc"[
  cp==="cp1"?0:cp==="cp2"?1:2]!.repeat(64);

describe.skipIf(!RAW)("H2 atomically fenced retention PROTOCOL ONLY: local PG 17",()=>{
  beforeAll(async()=>{
    sql=postgres(guard().toString(),{max:8});
    await sql`CREATE TABLE IF NOT EXISTS h2_proto_state(
      singleton integer PRIMARY KEY CHECK(singleton=1),
      roster_sealed boolean NOT NULL DEFAULT false)`;
    await sql`CREATE TABLE IF NOT EXISTS h2_proto_checkpoints(
      id text PRIMARY KEY,cursor bigint NOT NULL UNIQUE,hash text NOT NULL)`;
    await sql`CREATE TABLE IF NOT EXISTS h2_proto_durable(sequence bigint PRIMARY KEY)`;
    await sql`CREATE TABLE IF NOT EXISTS h2_proto_roster(
      consumer text PRIMARY KEY,required boolean NOT NULL,
      checkpoint_id text REFERENCES h2_proto_checkpoints(id),
      epoch integer NOT NULL,revision integer NOT NULL)`;
  },20000);
  beforeEach(bootstrap);
  afterAll(async()=>{await sql?.end();});
  it("H2-PIN-01 sealed required consumers block deletion until BOTH independently advance",async()=>{
    await expect(fencedCleanup(5)).rejects.toThrow("ROSTER_UNSEALED");
    await seal();
    await expect(fencedCleanup(5)).rejects.toThrow("PIN_BEFORE_DELETE_BOUNDARY");
    expect(await rebase("guardian-primary",1,1,"cp2",checkpointHash("cp2"))).toBe(2);
    await expect(fencedCleanup(5)).rejects.toThrow("PIN_BEFORE_DELETE_BOUNDARY");
    expect(await rebase("guardian-secondary",1,1,"cp2",checkpointHash("cp2"))).toBe(2);
    expect(await fencedCleanup(5)).toEqual({deleted:4,pruned:1});
    const c=await sql`SELECT id FROM h2_proto_checkpoints ORDER BY cursor`;
    expect(c.map((r:any)=>r.id)).toEqual(["cp2","cp3"]);
    expect((await sql`SELECT count(*)::int AS n FROM h2_proto_durable`)[0].n).toBe(4);
  });
  it("H2-PIN-02 expired/offline required reader is NOT discarded by wall-clock guess",async()=>{
    await seal();
    await sql`UPDATE h2_proto_roster SET epoch=2 WHERE consumer='guardian-secondary'`;
    await expect(rebase("guardian-secondary",1,1,"cp2",checkpointHash("cp2")))
      .rejects.toThrow("STALE_EPOCH");
    await rebase("guardian-primary",1,1,"cp2",checkpointHash("cp2"));
    await expect(fencedCleanup(5)).rejects.toThrow("PIN_BEFORE_DELETE_BOUNDARY");
    // Even deleting its membership is a separate privileged operator decision,
    // never tied to lease timeout. No automatic decommission in this prototype.
    expect((await sql`SELECT count(*)::int AS n FROM h2_proto_roster
      WHERE required=true`)[0].n).toBe(2);
  });
  it("H2-PIN-03 registration vs cleanup SERIALIZES under canonical transaction lock",async()=>{
    await seal();
    await rebase("guardian-primary",1,1,"cp2",checkpointHash("cp2"));
    await rebase("guardian-secondary",1,1,"cp2",checkpointHash("cp2"));
    // Start the fenced physical deletion and hold the canonical lock
    // during its SQL transaction. An old-anchor registration must block
    // and, after commit, fail rather than acquire a missing checkpoint.
    let started!:()=>void;
    const entered=new Promise<void>(resolve=>{started=resolve;});
    const cleanup=sql.begin(async(tx:any)=>{
      await lock(tx);
      started();
      await tx`SELECT pg_sleep(0.30)`;
      await tx`DELETE FROM h2_proto_durable WHERE sequence<=5`;
      await tx`DELETE FROM h2_proto_checkpoints WHERE id='cp1'`;
      return true;
    });
    await entered;
    const attempted=register("guardian-primary","cp1");
    expect(await cleanup).toBe(true);
    await expect(attempted).rejects.toThrow("CHECKPOINT_NOT_RETAINED");
    expect(await register("guardian-primary","cp2")).toBe(true);
  });
  it("H2-PIN-05 failure after a physical DELETE rolls back BOTH data and pin fence",async()=>{
    await seal();
    await rebase("guardian-primary",1,1,"cp2",checkpointHash("cp2"));
    await rebase("guardian-secondary",1,1,"cp2",checkpointHash("cp2"));
    await expect(sql.begin(async(tx:any)=>{
      await lock(tx);
      const pinned=await tx`SELECT count(*)::int AS n FROM h2_proto_roster
        WHERE required=true AND checkpoint_id='cp1'`;
      expect(pinned[0].n).toBe(0);
      await tx`DELETE FROM h2_proto_durable WHERE sequence<=5`;
      // Crash-equivalent transaction ABORT after deletion but before
      // checkpoint cleanup and COMMIT. No partial prefix persists.
      throw Error("H2_PROTOCOL_SIMULATED_PG_TRANSACTION_ABORT");
    })).rejects.toThrow("SIMULATED_PG_TRANSACTION_ABORT");
    expect((await sql`SELECT count(*)::int AS n FROM h2_proto_durable`)[0].n).toBe(8);
    expect((await sql`SELECT count(*)::int AS n FROM h2_proto_checkpoints`)[0].n).toBe(3);
    expect(await fencedCleanup(5)).toEqual({deleted:4,pruned:1});
  });
  it("H2-PIN-04 all claims absent or malformed fail closed; no silent empty roster unlock",async()=>{
    await seal();
    await sql`UPDATE h2_proto_roster SET required=false`;
    await expect(fencedCleanup(5)).rejects.toThrow("NO_REQUIRED_CONSUMERS");
    await sql`UPDATE h2_proto_roster SET required=true,checkpoint_id=NULL
      WHERE consumer='guardian-primary'`;
    await expect(fencedCleanup(5)).rejects.toThrow("UNKNOWN_OR_STALE_CONSUMER");
  });
});

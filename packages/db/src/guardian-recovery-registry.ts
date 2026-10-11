import postgres from "postgres";
import {createHash} from "node:crypto";
import {
  CHECKPOINT_SCHEMA_VERSION,DURABLE_MUTATION_LOCK_ID,
  hashCheckpointPayload,isSemanticallyValidCheckpointPayload,
} from "./durable.js";
import {JournalMutateForbiddenError,type JournalAccessMode} from "./access.js";

/**
 * Registration authority scaffold, NOT the eventual ACK authority.
 * Caller MUST be an independently authenticated operator using
 * maintenance credentials. The Guardian's SELECT-only connection
 * cannot register, advance, delete or unseal consumers.
 *
 * No method here advances a pin or authorizes compaction. Runtime
 * RetentionManager.guardianRecoveryHold must remain TRUE.
 */
export interface GuardianRegistration {
  consumerId:string;
  experimentId:string;
  checkpointId:string;
  expectedCheckpointHash:string;
  authorityEpoch:number;
}
export interface GuardianRegisteredPin extends GuardianRegistration {
  checkpointCursor:number;
  revision:number;
}
export type GuardianRosterInspection =
  |{state:"BLOCKED";reason:string;pins:GuardianRegisteredPin[]}
  |{state:"SEALED_PINNED";reason:null;pins:GuardianRegisteredPin[]};

function validId(s:string,max:number):boolean {
  return typeof s==="string"&&s.length>0&&s.length<=max
    &&/^[A-Za-z0-9:_./-]+$/.test(s);
}
function validHash(s:string):boolean{return typeof s==="string"&&/^[a-f0-9]{64}$/.test(s);}
function membershipHash(rows:readonly {consumerId:string;experimentId:string}[]):string{
  const pairs=rows.map(c=>[c.consumerId,c.experimentId]).sort((a,b)=>
    JSON.stringify(a).localeCompare(JSON.stringify(b)));
  return createHash("sha256").update(JSON.stringify(pairs)).digest("hex");
}

export class PostgresGuardianRecoveryRegistry {
  private readonly client:ReturnType<typeof postgres>;
  constructor(url:string,private readonly sourceIdentity:string,
    private readonly access:JournalAccessMode="readonly"){
    if(!validId(sourceIdentity,256))throw Error("G2H_PIN_SOURCE_ID_INVALID");
    this.client=postgres(url,{max:2,connect_timeout:5});
  }
  async close(){await this.client.end();}
  /** One-time, nonempty and SEALED operator enrollment. Never autoinit. */
  async enrollAndSeal(consumers:readonly GuardianRegistration[]):Promise<GuardianRegisteredPin[]>{
    if(this.access!=="maintenance")
      throw new JournalMutateForbiddenError(this.access,"enrollAndSeal");
    if(consumers.length<1||consumers.length>32)
      throw Error("G2H_PIN_ROSTER_SIZE_INVALID");
    const seen=new Set<string>();
    for(const c of consumers){
      if(!validId(c.consumerId,128)||!validId(c.experimentId,128)
        ||!validId(c.checkpointId,256)
        ||!validHash(c.expectedCheckpointHash)
        ||!Number.isSafeInteger(c.authorityEpoch)||c.authorityEpoch<1
        ||seen.has(c.consumerId))throw Error("G2H_PIN_REGISTRATION_INVALID");
      seen.add(c.consumerId);
    }
    return this.client.begin(async tx=>{
      // Identical canonical mutation lock used by the durable writer and
      // compactor. Current CP prune is NOT yet fenced by it.
      await tx`SELECT pg_advisory_xact_lock(${DURABLE_MUTATION_LOCK_ID})`;
      const existing=await tx`SELECT id FROM guardian_recovery_roster WHERE id=1`;
      if(existing.length)throw Error("G2H_PIN_ROSTER_ALREADY_INITIALIZED");
      const [high]=await tx`SELECT high_water FROM durable_sequence_metadata WHERE id=1`;
      if(!high)throw Error("G2H_PIN_SOURCE_WATERMARK_UNAVAILABLE");
      const approved:GuardianRegisteredPin[]=[];
      for(const c of consumers){
        const rows=await tx`SELECT id,experiment_id,payload
          FROM engine_checkpoints WHERE id=${c.checkpointId}`;
        const row=rows[0],p=row?.payload as Record<string,unknown>|undefined;
        if(!p||row?.experiment_id!==c.experimentId
          ||p.schemaVersion!==CHECKPOINT_SCHEMA_VERSION
          ||typeof p.checkpointContentHash!=="string"
          ||p.checkpointContentHash!==c.expectedCheckpointHash
          ||hashCheckpointPayload(p)!==c.expectedCheckpointHash
          ||!isSemanticallyValidCheckpointPayload(p)
          ||!p.ledger||typeof p.ledger!=="object"
          ||!Number.isSafeInteger(p.durableCursor)
          ||Number(p.durableCursor)>Number(high.high_water))
          throw Error("G2H_PIN_CHECKPOINT_UNVERIFIED");
        approved.push({...c,checkpointCursor:Number(p.durableCursor),revision:1});
      }
      await tx`INSERT INTO guardian_recovery_roster
        (id,source_identity,sealed,member_count,member_hash)
        VALUES (1,${this.sourceIdentity},false,${approved.length},
          ${membershipHash(approved)})`;
      for(const c of approved){
        await tx`INSERT INTO guardian_recovery_pins
          (consumer_id,experiment_id,checkpoint_id,checkpoint_hash,
           checkpoint_cursor,authority_epoch,revision,required)
          VALUES(${c.consumerId},${c.experimentId},${c.checkpointId},
            ${c.expectedCheckpointHash},${c.checkpointCursor},
            ${c.authorityEpoch},1,true)`;
      }
      await tx`UPDATE guardian_recovery_roster SET sealed=true,revision=revision+1 WHERE id=1`;
      return approved;
    });
  }
  /** Observability only: NO 'allow cleanup' method. */
  async inspect():Promise<GuardianRosterInspection>{
    return this.client.begin(async tx=>{
      // Roster and every pinned checkpoint must come from ONE
      // coherent committed PostgreSQL view, never mixed snapshots.
      await tx`SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY`;
      const roster=await tx`SELECT source_identity,sealed,member_count,member_hash
        FROM guardian_recovery_roster WHERE id=1`;
      if(roster.length!==1||!roster[0]?.sealed||roster[0]?.source_identity!==this.sourceIdentity)
        return {state:"BLOCKED",reason:"ROSTER_MISSING_UNSEALED_OR_SCOPE_MISMATCH",pins:[]};
      const raw=await tx`
        SELECT p.consumer_id,p.experiment_id,p.checkpoint_id,p.checkpoint_hash,
          p.checkpoint_cursor,p.authority_epoch,p.revision,
          c.experiment_id AS actual_experiment,c.payload AS actual_payload
        FROM guardian_recovery_pins p
        LEFT JOIN engine_checkpoints c ON c.id=p.checkpoint_id
        WHERE p.required=true
        ORDER BY p.consumer_id`;
      if(raw.length<1)return {state:"BLOCKED",reason:"REQUIRED_CONSUMERS_MISSING",pins:[]};
      if(raw.length!==Number(roster[0]!.member_count)
        ||membershipHash(raw.map(x=>({consumerId:x.consumer_id,
          experimentId:x.experiment_id})))!==roster[0]!.member_hash)
        return {state:"BLOCKED",reason:"ROSTER_MEMBERSHIP_MISMATCH",pins:[]};
      const pins:GuardianRegisteredPin[]=[];
      for(const r of raw){
        const p=r.actual_payload as Record<string,unknown>|undefined;
        if(!p||r.actual_experiment!==r.experiment_id||
          p.checkpointContentHash!==r.checkpoint_hash
          ||hashCheckpointPayload(p)!==r.checkpoint_hash
          ||!isSemanticallyValidCheckpointPayload(p)
          ||Number(p.durableCursor)!==Number(r.checkpoint_cursor))
          return {state:"BLOCKED",reason:"PINNED_CHECKPOINT_UNVERIFIABLE",pins:[]};
        pins.push({consumerId:r.consumer_id,experimentId:r.experiment_id,
          checkpointId:r.checkpoint_id,expectedCheckpointHash:r.checkpoint_hash,
          checkpointCursor:Number(r.checkpoint_cursor),authorityEpoch:Number(r.authority_epoch),
          revision:Number(r.revision)});
      }
      return {state:"SEALED_PINNED",reason:null,pins};
    });
  }
}

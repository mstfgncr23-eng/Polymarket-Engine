import { createPrivateKey, sign } from "node:crypto";
import { hashCheckpointPayload, type EngineCheckpoint } from "@pm/db";
import { parseCheckpointDurableCursor } from "./recovery.js";
import { guardianWriterManifestBytes, type GuardianWriterManifest } from "./guardian-signed-writer-manifest.js";

/**
 * G2H producer-side, OFFLINE-only publisher. Must be called INSIDE the
 * canonical Engine's durable gate after an independently fenced single-writer
 * lease was proven by the caller. This module cannot establish that lease,
 * owns no timer/socket/private-key file and never alters the journal.
 */
export interface GuardianCanonicalWriterWitness {
  experimentId: string;
  /** Engine's last fully applied durable cursor under the durable gate. */
  appliedCursor: number;
  /** Caller must fail this gate if an unknown DB commit/integrity fault remains. */
  economicState: "VERIFIED" | "UNKNOWN";
  tradingMode: "paper" | "live";
}
export interface GuardianManifestReadJournal {
  latestDurableSequence():Promise<number>;
  listVerifiedCheckpoints():Promise<EngineCheckpoint[]>;
}
export class GuardianManifestPublishHalted extends Error {
  constructor(readonly reason:string){super(`GUARDIAN_MANIFEST_PUBLISH_HALTED:${reason}`);}
}
const fail=(reason:string):never=>{throw new GuardianManifestPublishHalted(reason);};
export class GuardianWriterManifestPublisher {
  private sequence:number;
  private issuing=false;
  constructor(private readonly input:{
    issuerKeyId:string;authorityId:string;writerEpoch:number;
    lastDurablyIssuedSequence:number;privateKeyPem:string;maxCheckpointLagRows:number;
  }){
    const cfg=input;
    if (!cfg.issuerKeyId || !cfg.authorityId
      || !Number.isSafeInteger(cfg.writerEpoch) || cfg.writerEpoch<1
      || !Number.isSafeInteger(cfg.lastDurablyIssuedSequence) || cfg.lastDurablyIssuedSequence<0
      || !Number.isSafeInteger(cfg.maxCheckpointLagRows)
      || cfg.maxCheckpointLagRows<0 || cfg.maxCheckpointLagRows>100_000)
      fail("INVALID_PUBLISHER_CONFIGURATION");
    this.sequence=cfg.lastDurablyIssuedSequence;
  }
  async issue(input:{
    journal:GuardianManifestReadJournal;
    witness:GuardianCanonicalWriterWitness;
    nowMs:number;
  }):Promise<GuardianWriterManifest> {
    if (this.issuing) return fail("CONCURRENT_SIGNING_NOT_FENCED");
    this.issuing=true;
    try { return await this.issueWithinFence(input); }
    finally { this.issuing=false; }
  }
  private async issueWithinFence(input:{
    journal:GuardianManifestReadJournal;
    witness:GuardianCanonicalWriterWitness;
    nowMs:number;
  }):Promise<GuardianWriterManifest> {
    const {journal,witness,nowMs}=input;
    if (witness.tradingMode!=="paper" || witness.economicState!=="VERIFIED"
      || !witness.experimentId
      || !Number.isSafeInteger(witness.appliedCursor) || witness.appliedCursor<0
      || !Number.isSafeInteger(nowMs)
      || !Number.isSafeInteger(this.sequence+1)) return fail("WRITER_NOT_SAFE_TO_ATTEST");
    let highWater:number,checkpoints:EngineCheckpoint[];
    try {
      highWater=await journal.latestDurableSequence();
      checkpoints=await journal.listVerifiedCheckpoints();
    }catch{return fail("SOURCE_UNAVAILABLE");}
    if (!Number.isSafeInteger(highWater) || highWater!==witness.appliedCursor)
      return fail("WRITER_NOT_AT_SOURCE_HIGHWATER");
    if (!Array.isArray(checkpoints)) return fail("SOURCE_UNAVAILABLE");
    const eligible=checkpoints.filter(cp=>{
      if (!cp || !cp.verified || cp.experimentId!==witness.experimentId
        || !cp.payload || typeof cp.payload!=="object")return false;
      const cursor=parseCheckpointDurableCursor(cp.payload);
      return cursor!==null && cursor<=highWater
        && highWater-cursor<=this.input.maxCheckpointLagRows;
    }).sort((a,b)=>{
      return (parseCheckpointDurableCursor(b.payload)??0)-(parseCheckpointDurableCursor(a.payload)??0);
    });
    if (!eligible.length) return fail("NO_TRUSTED_RECENT_CHECKPOINT");
    const cp=eligible[0]!;
    const chosenCursor=parseCheckpointDurableCursor(cp.payload);
    // Conflicting valid self-hashed snapshots at the same durable cursor
    // have no unique business ordering proof; never choose one arbitrarily.
    if (eligible.some(other=>other.id!==cp.id
      && parseCheckpointDurableCursor(other.payload)===chosenCursor
      && other.contentHash!==cp.contentHash))
      return fail("CHECKPOINT_ECONOMIC_AMBIGUITY");
    if ((Array.isArray(cp.payload.reportSourceIntegrity)
        && cp.payload.reportSourceIntegrity.length>0)
      || cp.payload.reportQuarantine!=null)
      return fail("UNRESOLVED_CANONICAL_SOURCE_INTEGRITY");
    try {
      if (!cp.contentHash || cp.payload.checkpointContentHash!==cp.contentHash
        || hashCheckpointPayload(cp.payload)!==cp.contentHash)
        return fail("CHECKPOINT_INTEGRITY_FAILURE");
    }catch(error){
      if (error instanceof GuardianManifestPublishHalted) throw error;
      return fail("CHECKPOINT_INTEGRITY_FAILURE");
    }
    const meta={
      version:1 as const,issuerKeyId:this.input.issuerKeyId,
      authorityId:this.input.authorityId,writerEpoch:this.input.writerEpoch,
      manifestSequence:this.sequence+1,experimentId:witness.experimentId,
      checkpointId:cp.id,checkpointHash:cp.contentHash,
      durableHighWater:highWater,issuedAtMs:nowMs,expiresAtMs:nowMs+15_000,
    };
    let signature:string;
    try {
      const key=createPrivateKey(this.input.privateKeyPem);
      if(key.asymmetricKeyType!=="ed25519")return fail("INVALID_SIGNING_KEY");
      signature=sign(null,guardianWriterManifestBytes(meta),key).toString("base64");
    }catch{return fail("INVALID_SIGNING_KEY");}
    // Caller MUST persist this monotonic sequence as part of the independent
    // writer-epoch authority; memory-only counters cannot survive failover.
    this.sequence++;
    return {...meta,signature};
  }
}

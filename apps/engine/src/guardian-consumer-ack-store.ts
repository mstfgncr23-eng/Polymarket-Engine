import { randomUUID } from "node:crypto";
import { mkdir, open, readFile, rename, unlink } from "node:fs/promises";
import { dirname } from "node:path";

/**
 * Small, fail-closed, non-DB durable local G2H read-progress watermark.
 * This is NOT an acknowledgment that an economic request or order executed.
 *
 * Filesystem and directory-sync requirements must be proven on the target
 * production volume before ever using this for an authorized cutover.
 */
export interface GuardianConsumerReceipt {
  version: 1;
  authorityId: string;
  experimentId: string;
  writerEpoch: number;
  manifestSequence: number;
  checkpointId: string;
  checkpointHash: string;
  cursor: number;
  projectionHash: string;
}
export interface GuardianConsumerAckStore {
  load(): Promise<GuardianConsumerReceipt | null>;
  commit(expected: GuardianConsumerReceipt | null, next: GuardianConsumerReceipt): Promise<void>;
}
export type GuardianConsumerAckHaltReason = "ACK_CORRUPT" | "ACK_UNAVAILABLE"
  | "ACK_CONCURRENT_WRITER" | "ACK_CAS_MISMATCH" | "ACK_REGRESSION" | "ACK_COMMIT_UNKNOWN";
export class GuardianConsumerAckHalted extends Error {
  constructor(readonly reason: GuardianConsumerAckHaltReason) {
    super(`GUARDIAN_CONSUMER_ACK_HALTED:${reason}`);
  }
}
const halt=(reason:GuardianConsumerAckHaltReason):never=>{throw new GuardianConsumerAckHalted(reason);};
export function validateGuardianReceipt(raw:unknown): GuardianConsumerReceipt {
  if (!raw || typeof raw!=="object" || Array.isArray(raw)) return halt("ACK_CORRUPT");
  const r=raw as Record<string,unknown>;
  const ident=(x:unknown)=>typeof x==="string" && x.length>=1 && x.length<=256;
  if (Object.keys(r).length!==9 || r.version!==1 || !ident(r.authorityId)
    || !ident(r.experimentId) || !ident(r.checkpointId)
    || !Number.isSafeInteger(r.writerEpoch) || Number(r.writerEpoch)<1
    || !Number.isSafeInteger(r.manifestSequence) || Number(r.manifestSequence)<1
    || !Number.isSafeInteger(r.cursor) || Number(r.cursor)<0
    || typeof r.checkpointHash!=="string" || !/^[a-f0-9]{64}$/.test(r.checkpointHash)
    || typeof r.projectionHash!=="string" || !/^[a-f0-9]{64}$/.test(r.projectionHash)) {
    return halt("ACK_CORRUPT");
  }
  return raw as GuardianConsumerReceipt;
}
function equal(a: GuardianConsumerReceipt|null,b: GuardianConsumerReceipt|null): boolean {
  return JSON.stringify(a)===JSON.stringify(b);
}
function checkAdvance(previous:GuardianConsumerReceipt|null,next:GuardianConsumerReceipt):void {
  if (!previous) return;
  if (previous.authorityId!==next.authorityId || previous.experimentId!==next.experimentId
    || next.writerEpoch<previous.writerEpoch
    || (next.writerEpoch===previous.writerEpoch
      && next.manifestSequence<previous.manifestSequence)
    || next.cursor<previous.cursor) halt("ACK_REGRESSION");
  if (next.cursor===previous.cursor && next.writerEpoch===previous.writerEpoch
    && next.manifestSequence===previous.manifestSequence
    && !equal(previous,next)) halt("ACK_REGRESSION");
}
/**
 * Exclusive single-process lock via O_EXCL, atomic replacement, flush file,
 * then directory fsync when supported. Unknown fsync/rename outcome HALTS.
 * A stale lock after crash is an OPERATOR reconcile gate, not auto-deleted.
 */
export class GuardianFileAckStore implements GuardianConsumerAckStore {
  constructor(private readonly path: string) {
    if (!path || !path.endsWith(".guardian-ack.json")) throw new Error("INVALID_GUARDIAN_ACK_PATH");
  }
  async load(): Promise<GuardianConsumerReceipt|null> {
    try {
      const data=await readFile(this.path,"utf8");
      if (data.length>4096) return halt("ACK_CORRUPT");
      return validateGuardianReceipt(JSON.parse(data));
    } catch(error) {
      if ((error as NodeJS.ErrnoException)?.code==="ENOENT") return null;
      if (error instanceof GuardianConsumerAckHalted) throw error;
      if (error instanceof SyntaxError) return halt("ACK_CORRUPT");
      return halt("ACK_UNAVAILABLE");
    }
  }
  async commit(expected:GuardianConsumerReceipt|null,next:GuardianConsumerReceipt):Promise<void> {
    validateGuardianReceipt(next);
    const folder=dirname(this.path), lockPath=this.path+".lock";
    try { await mkdir(folder,{recursive:true}); }
    catch { return halt("ACK_UNAVAILABLE"); }
    let lock: Awaited<ReturnType<typeof open>>;
    try { lock=await open(lockPath,"wx",0o600); }
    catch (error) {
      if ((error as NodeJS.ErrnoException)?.code==="EEXIST") return halt("ACK_CONCURRENT_WRITER");
      return halt("ACK_UNAVAILABLE");
    }
    let replaced=false;
    const tempPath=this.path+"."+randomUUID()+".tmp";
    try {
      const previous=await this.load();
      if (!equal(previous,expected)) return halt("ACK_CAS_MISMATCH");
      checkAdvance(previous,next);
      const file=await open(tempPath,"wx",0o600);
      try { await file.writeFile(JSON.stringify(next)+"\n","utf8"); await file.sync(); }
      finally { await file.close(); }
      await rename(tempPath,this.path);
      replaced=true;
      // POSIX directory flush preserves rename across sudden power loss.
      // Windows cannot always open a directory: no false durability claim.
      if (process.platform!=="win32") {
        const dir=await open(folder,"r");
        try { await dir.sync(); } finally { await dir.close(); }
      }
    } catch(error) {
      if (error instanceof GuardianConsumerAckHalted) throw error;
      return halt(replaced?"ACK_COMMIT_UNKNOWN":"ACK_UNAVAILABLE");
    } finally {
      try { await unlink(tempPath); } catch { /* absent after rename */ }
      try { await lock.close(); } finally {
        // A failed lock cleanup requires manual reconcile before retry.
        try { await unlink(lockPath); } catch { /* lock remains, next call HALTS */ }
      }
    }
  }
}

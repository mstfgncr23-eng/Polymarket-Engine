import {randomUUID} from "node:crypto";
import {mkdir,open,readFile,rename,unlink} from "node:fs/promises";
import {dirname} from "node:path";

/** G2H: observation progress only. NEVER an order submit/accept/fill ACK. */
export interface GuardianPgReceipt {
  readonly version:2;
  readonly authorityId:string;
  readonly experimentId:string;
  readonly source:Readonly<{database:string;schema:string;role:string}>;
  readonly checkpointId:string;
  readonly checkpointHash:string;
  readonly cursor:number;
  /** Last committed database high-water observed, even if consumer is behind. */
  readonly observedHighWater:number;
  readonly economicHash:string;
}
export interface GuardianPgAckStore {
  load():Promise<GuardianPgReceipt|null>;
  commit(expected:GuardianPgReceipt|null,next:GuardianPgReceipt):Promise<void>;
}
const error=(reason:string):never=>{throw Error("G2H_PG_ACK_"+reason);};
const ident=(s:unknown):s is string=>typeof s==="string"&&s.length>0&&s.length<=256;
const hash=(s:unknown):s is string=>typeof s==="string"&&/^[a-f0-9]{64}$/.test(s);
export function parseGuardianPgReceipt(x:unknown):GuardianPgReceipt {
  if(!x||typeof x!=="object"||Array.isArray(x))return error("CORRUPT_OR_LEGACY");
  const r=x as Record<string,unknown>;
  if(r.version!==2)return error("LEGACY_FORMAT_REQUIRES_OPERATOR_RECONCILE");
  const s=r.source;
  if(!s||typeof s!=="object"||Array.isArray(s))return error("CORRUPT_OR_LEGACY");
  const source=s as Record<string,unknown>;
  if(Object.keys(r).length!==9||Object.keys(source).length!==3
    ||!ident(r.authorityId)||!ident(r.experimentId)
    ||!ident(r.checkpointId)||!hash(r.checkpointHash)||!hash(r.economicHash)
    ||!Number.isSafeInteger(r.cursor)||Number(r.cursor)<0
    ||!Number.isSafeInteger(r.observedHighWater)
    ||Number(r.observedHighWater)<Number(r.cursor)
    ||!ident(source.database)||!ident(source.schema)||!ident(source.role))
    return error("CORRUPT_OR_LEGACY");
  return r as unknown as GuardianPgReceipt;
}
const same=(a:GuardianPgReceipt|null,b:GuardianPgReceipt|null)=>
  JSON.stringify(a)===JSON.stringify(b);
function checkAdvance(previous:GuardianPgReceipt|null,next:GuardianPgReceipt):void {
  if(!previous)return;
  if(previous.authorityId!==next.authorityId
    ||previous.experimentId!==next.experimentId
    ||JSON.stringify(previous.source)!==JSON.stringify(next.source)
    ||previous.checkpointId!==next.checkpointId
    ||previous.checkpointHash!==next.checkpointHash
    ||next.cursor<previous.cursor
    ||next.observedHighWater<previous.observedHighWater
    ||(next.cursor===previous.cursor&&!same(previous,next)))
    error("REGRESSION_OR_CHECKPOINT_ROTATION_UNPROVEN");
}
/** Single-reader local CAS; stale lock and uncertain fsync need operator reconcile. */
export class GuardianPgFileAckStore implements GuardianPgAckStore {
  constructor(private readonly path:string){
    if(!path.endsWith(".guardian-pg-ack.json"))throw Error("G2H_PG_ACK_INVALID_PATH");
  }
  async load():Promise<GuardianPgReceipt|null>{
    try {
      const content=await readFile(this.path,"utf8");
      if(content.length>4096)return error("CORRUPT_OR_LEGACY");
      return parseGuardianPgReceipt(JSON.parse(content));
    }catch(e){
      if((e as NodeJS.ErrnoException).code==="ENOENT")return null;
      if(e instanceof Error&&e.message.startsWith("G2H_PG_ACK_"))throw e;
      return error("UNAVAILABLE_OR_CORRUPT");
    }
  }
  async commit(expected:GuardianPgReceipt|null,next:GuardianPgReceipt):Promise<void>{
    parseGuardianPgReceipt(next);
    const dir=dirname(this.path);
    try{await mkdir(dir,{recursive:true});}
    catch{return error("UNAVAILABLE");}
    let lock:Awaited<ReturnType<typeof open>>;
    try{lock=await open(this.path+".lock","wx",0o600);}
    catch(e){
      if((e as NodeJS.ErrnoException).code==="EEXIST")return error("CONCURRENT_WRITER");
      return error("UNAVAILABLE");
    }
    const temp=this.path+"."+randomUUID()+".tmp";
    let replaced=false;
    try{
      const current=await this.load();
      if(!same(current,expected))return error("CAS_MISMATCH");
      checkAdvance(current,next);
      const handle=await open(temp,"wx",0o600);
      try{await handle.writeFile(JSON.stringify(next)+"\n","utf8");await handle.sync();}
      finally{await handle.close();}
      await rename(temp,this.path);
      replaced=true;
      // Windows cannot assert durable directory rename on power loss.
      if(process.platform!=="win32"){
        const folder=await open(dir,"r");
        try{await folder.sync();}finally{await folder.close();}
      }
    }catch(e){
      if(e instanceof Error&&e.message.startsWith("G2H_PG_ACK_"))throw e;
      return error(replaced?"COMMIT_UNKNOWN":"UNAVAILABLE");
    }finally{
      try{await unlink(temp);}catch{/* rename succeeded or file absent */}
      try{await lock.close();}finally{
        try{await unlink(this.path+".lock");}catch{/* keep fail-closed on retry */}
      }
    }
  }
}

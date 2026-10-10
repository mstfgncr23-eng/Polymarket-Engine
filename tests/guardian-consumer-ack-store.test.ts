import { mkdtemp, open, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { GuardianFileAckStore, validateGuardianReceipt,
  type GuardianConsumerReceipt } from "../apps/engine/src/guardian-consumer-ack-store.js";

const dirs:string[]=[];
afterEach(async()=>{for (const dir of dirs.splice(0)) await rm(dir,{recursive:true,force:true});});
const init=async()=>{
  const dir=await mkdtemp(join(tmpdir(),"pm-guardian-g2h-"));
  dirs.push(dir);
  const path=join(dir,"state.guardian-ack.json");
  return {path,store:new GuardianFileAckStore(path)};
};
const receipt=(cursor=20):GuardianConsumerReceipt=>({
  version:1,authorityId:"writer-1",experimentId:"exp-1",writerEpoch:1,
  manifestSequence:1,checkpointId:"cp-1",checkpointHash:"a".repeat(64),
  cursor,projectionHash:"b".repeat(64),
});
const blocked=async(p:Promise<unknown>,reason:string)=>
  expect(p).rejects.toMatchObject({reason});

describe("G2H on-disk cursor ACK (read-only economic semantics)",()=>{
  it("G2H-ACK-01 creates and reloads stable cursor across store instances",async()=>{
    const {path,store}=await init();
    expect(await store.load()).toBeNull();
    await store.commit(null,receipt());
    const fromDisk=new GuardianFileAckStore(path);
    expect(await fromDisk.load()).toEqual(receipt());
    await fromDisk.commit(receipt(),receipt(21));
    expect(await fromDisk.load()).toEqual(receipt(21));
  });
  it("G2H-ACK-02 rejects unknown previous watermark after concurrent advance",async()=>{
    const {store}=await init();
    await store.commit(null,receipt());
    await blocked(store.commit(null,receipt(21)),"ACK_CAS_MISMATCH");
    await blocked(store.commit(receipt(19),receipt(21)),"ACK_CAS_MISMATCH");
    expect((await store.load())?.cursor).toBe(20);
  });
  it("G2H-ACK-03 refuses regression, authority swap or unrecognized replay",async()=>{
    const {store}=await init();
    await store.commit(null,receipt(20));
    await blocked(store.commit(receipt(20),receipt(19)),"ACK_REGRESSION");
    await blocked(store.commit(receipt(20),{...receipt(21),authorityId:"wrong"}),"ACK_REGRESSION");
    await blocked(store.commit(receipt(20),{...receipt(21),writerEpoch:0}),"ACK_CORRUPT");
    await blocked(store.commit(receipt(20),{...receipt(20),projectionHash:"c".repeat(64)}),"ACK_REGRESSION");
    expect(await store.load()).toEqual(receipt(20));
  });
  it("G2H-ACK-04 corruption, truncated JSON and absent signature hash stop boot",async()=>{
    const {path,store}=await init();
    await writeFile(path,'{"version":1,"cursor":2}',"utf8");
    await blocked(store.load(),"ACK_CORRUPT");
    await writeFile(path,'{"version":',"utf8");
    await blocked(store.load(),"ACK_CORRUPT");
    expect(()=>validateGuardianReceipt({...receipt(),checkpointHash:"wrong"}))
      .toThrowError(expect.objectContaining({reason:"ACK_CORRUPT"}));
  });
  it("G2H-ACK-05 stale/exclusive writer lock is a halt, never silently stolen",async()=>{
    const {path,store}=await init();
    const lock=await open(path+".lock","wx");
    try {await blocked(store.commit(null,receipt()),"ACK_CONCURRENT_WRITER");}
    finally {await lock.close();}
    expect(await store.load()).toBeNull();
  });
  it("G2H-ACK-06 refused CAS cannot leave a corrupt replacement or a stale lock",async()=>{
    const {path,store}=await init();
    await store.commit(null,receipt());
    await blocked(store.commit(null,receipt(30)),"ACK_CAS_MISMATCH");
    const raw=JSON.parse(await readFile(path,"utf8"));
    expect(raw).toEqual(receipt());
    await store.commit(receipt(),receipt(21));
    expect((await store.load())?.cursor).toBe(21);
  });
});

import {describe,it,expect,vi} from "vitest";
import {mkdtemp,writeFile,rm} from "node:fs/promises";
import {join} from "node:path";
import {tmpdir} from "node:os";
import {MemoryEventJournal} from "@pm/db";
import {LocalJournalArchive,LOCAL_ARCHIVE_SENTINEL,LOCAL_ARCHIVE_SENTINEL_CONTENT} from "../apps/engine/src/archive.js";
import {RetentionManager,defaultRetentionConfig} from "../apps/engine/src/retention-manager.js";

describe("simple retention bounded archive invariants",()=>{
  it("reuses the exact sealed bucket while draining batches despite unrelated new commits",async()=>{
    const root=await mkdtemp(join(tmpdir(),"simple-bucket-")),journal=new MemoryEventJournal();
    try{
      await writeFile(join(root,LOCAL_ARCHIVE_SENTINEL),LOCAL_ARCHIVE_SENTINEL_CONTENT);
      const at="2026-10-05T00:10:00.000Z", now=new Date("2026-10-12T10:00:00Z");
      for(let i=0;i<450;i++)await journal.append({id:"event-"+i,type:"MARKET_SNAPSHOT",aggregateType:"market",aggregateId:"first",traceId:"trace",correlationId:"trace",sourceEventId:"source-"+i,occurredAt:at,recordedAt:at,payload:{i}});
      const directories:string[]=[], hashes:string[]=[];
      const manager=new RetentionManager(journal,defaultRetentionConfig({enabled:true,offsiteReplicaRequired:true,localArchiveEnabled:true,localArchiveDir:root,maxRowsPerCycle:200}),undefined,new LocalJournalArchive(root),{
        verifyCanonical:async()=>undefined,verifyRecovery:async()=>undefined,verifyJournalBucket:async input=>{
          directories.push(input.directory);hashes.push(input.manifest.rawEvents.sha256);expect(input.manifest.rawEvents.eventCount).toBe(450);}});
      const context={experiment:null,now,degraded:false,failClosed:false,databaseUp:true};
      expect((await manager.runCycle(context)).deletedByClass.raw_telemetry).toBe(200);
      await journal.append({id:"unrelated",type:"ENGINE_STARTED",aggregateType:"engine",aggregateId:"engine",traceId:"trace",correlationId:"trace",sourceEventId:"unrelated",occurredAt:now.toISOString(),recordedAt:now.toISOString(),payload:{}});
      expect((await manager.runCycle(context)).deletedByClass.raw_telemetry).toBe(200);
      expect((await manager.runCycle(context)).deletedByClass.raw_telemetry).toBe(50);
      expect(new Set(directories).size).toBe(1);expect(new Set(hashes).size).toBe(1);
      expect(await journal.countByType(["MARKET_SNAPSHOT"])).toEqual({});
    }finally{await rm(root,{recursive:true,force:true});}
  });
  it("a soft key cap never evicts an unexpired identity, and only expired closed identities can leave",async()=>{
    const journal=new MemoryEventJournal();const now=Date.now();
    for(let i=0;i<3;i++)await journal.commitDurableTransition({operationKey:"closed-"+i,type:"INFRASTRUCTURE_COST_ACCRUED",aggregateType:"ledger",aggregateId:"ledger",payload:{i}},i);
    await journal.commitDurableTransition({operationKey:"open-fill",type:"PAPER_FILL",aggregateType:"fill",aggregateId:"fill",payload:{fill:{marketId:"first"}}},3);
    await journal.compactDurableTransitionsThrough(4,{maxRows:200,batchSize:200,maxReplayKeys:1});
    for(let i=0;i<3;i++)expect(await journal.readDurableByOperationKey("closed-"+i)).not.toBeNull();
    vi.spyOn(Date,"now").mockReturnValue(now+8*24*3600000);
    try{
      await journal.compactDurableTransitionsThrough(4,{maxReplayKeys:0});
      for(let i=0;i<3;i++)expect(await journal.readDurableByOperationKey("closed-"+i)).toBeNull();
      expect(await journal.readDurableByOperationKey("open-fill")).not.toBeNull();
    }finally{vi.restoreAllMocks();}
  });
});

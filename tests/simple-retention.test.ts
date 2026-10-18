import {describe,it,expect,vi} from "vitest";
import {mkdtemp,writeFile,readFile,rm} from "node:fs/promises";
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
      const rawPath=join(directories[0]!,"events.jsonl"), original=await readFile(rawPath);
      const manifestBefore=await readFile(join(directories[0]!,"manifest.json"));
      await writeFile(rawPath,"corrupt");
      expect((await manager.runCycle(context)).skipReason).toBe("ARCHIVE_EXISTING_BUCKET_INVALID");
      expect(await readFile(rawPath,"utf8")).toBe("corrupt");
      expect(await readFile(join(directories[0]!,"manifest.json"))).toEqual(manifestBefore);
      expect((await journal.queryEvents({types:["MARKET_SNAPSHOT"]})).length).toBe(250);
      await writeFile(rawPath,original);
      await rm(join(directories[0]!,"manifest.json"));
      expect((await manager.runCycle(context)).skipReason).toBe("ARCHIVE_EXISTING_BUCKET_INCOMPLETE");
      expect(await readFile(rawPath)).toEqual(original);
      expect((await journal.queryEvents({types:["MARKET_SNAPSHOT"]})).length).toBe(250);
      await writeFile(join(directories[0]!,"manifest.json"),manifestBefore);
      expect((await manager.runCycle(context)).deletedByClass.raw_telemetry).toBe(200);
      expect((await manager.runCycle(context)).deletedByClass.raw_telemetry).toBe(50);
      expect(new Set(directories).size).toBe(1);expect(new Set(hashes).size).toBe(1);
      expect(await journal.countByType(["MARKET_SNAPSHOT"])).toEqual({});
    }finally{await rm(root,{recursive:true,force:true});}
  });
  it("holds partial legacy canonical sources but still archives independently eligible telemetry",async()=>{
    const root=await mkdtemp(join(tmpdir(),"legacy-retention-")),journal=new MemoryEventJournal();
    try{
      await writeFile(join(root,LOCAL_ARCHIVE_SENTINEL),LOCAL_ARCHIVE_SENTINEL_CONTENT);
      const now=new Date("2026-10-18T00:00:00Z");
      await journal.append({id:"telemetry-legacy",type:"MARKET_SNAPSHOT",aggregateType:"market",aggregateId:"m",traceId:"t",correlationId:"t",sourceEventId:"legacy-telemetry",occurredAt:"2026-10-01T00:00:00Z",recordedAt:"2026-10-01T00:00:00Z",payload:{}});
      vi.spyOn(journal,"listArchiveCandidates").mockResolvedValue([{
        report:{experimentId:"experiment_old",sourceEventCount:100,persistedAt:"2026-09-01T00:00:00Z"},
        rollup:{sourceFenceOccurredAt:"2026-09-01T01:00:00Z",sourceCommitSeq:120,sourceRecordedAt:"2026-09-01T01:00:00Z"},
      }] as any);
      vi.spyOn(journal,"countCanonicalEventsThrough").mockResolvedValue({count:2,oldestOccurredAt:"2026-09-01T00:00:00Z"});
      const manager=new RetentionManager(journal,defaultRetentionConfig({enabled:true,localArchiveEnabled:true,localArchiveDir:root,guardianRecoveryHold:true}),undefined,new LocalJournalArchive(root));
      const result=await manager.runCycle({experiment:null,now,degraded:false,failClosed:false,databaseUp:true});
      expect(result.failures).toBe(0);
      expect(result.heldCanonicalCount).toBe(1);
      expect(result.deletedByClass.raw_telemetry).toBe(1);
      expect(await journal.countByType(["MARKET_SNAPSHOT"])).toEqual({});
    }finally{vi.restoreAllMocks();await rm(root,{recursive:true,force:true});}
  });
  it("holds recovery compaction on durable hash failure and keeps verified journal cleanup running",async()=>{
    const root=await mkdtemp(join(tmpdir(),"recovery-integrity-hold-")),journal=new MemoryEventJournal();
    try {
      await writeFile(join(root,LOCAL_ARCHIVE_SENTINEL),LOCAL_ARCHIVE_SENTINEL_CONTENT);
      const now=new Date("2026-10-18T00:00:00Z");
      await journal.append({id:"telemetry-integrity",type:"MARKET_SNAPSHOT",aggregateType:"market",aggregateId:"m",traceId:"t",correlationId:"t",sourceEventId:"telemetry-integrity",occurredAt:"2026-10-01T00:00:00Z",recordedAt:"2026-10-01T00:00:00Z",payload:{}});
      const cp=(n:number)=>({id:"cp-"+n,sequence:n,payload:{durableCursor:n,derivedAuditCursor:n}});
      vi.spyOn(journal,"listVerifiedCheckpoints").mockResolvedValue([cp(3),cp(2)] as any);
      vi.spyOn(journal,"listCheckpoints").mockResolvedValue([cp(3),cp(2)] as any);
      vi.spyOn(journal,"queryDurableTransitions").mockResolvedValue([{sequence:1,type:"TEST_TRANSITION"}] as any);
      const archive=new LocalJournalArchive(root);
      const recovery=vi.spyOn(archive,"ensureRecoveryBundle").mockRejectedValue(new Error("RECOVERY_ARCHIVE_DURABLE_HASH"));
      const manager=new RetentionManager(journal,defaultRetentionConfig({enabled:true,localArchiveEnabled:true,localArchiveDir:root,guardianRecoveryHold:false,compactionEnabled:true}),undefined,archive);
      const context={experiment:null,now,degraded:false,failClosed:false,databaseUp:true,hasEconomicObligations:false};
      const first=await manager.runCycle(context);
      expect(first.failures).toBe(0);
      expect(first.recoveryIntegrityHeldReason).toBe("RECOVERY_ARCHIVE_DURABLE_HASH");
      expect(first.durableTransitionsPruned).toBe(false);
      expect(first.deletedByClass.checkpoint).toBe(0);
      expect(first.deletedByClass.durable_transition).toBe(0);
      expect(first.deletedByClass.raw_telemetry).toBe(1);
      const second=await manager.runCycle(context);
      expect(second.failures).toBe(0);
      expect(second.recoveryIntegrityHeldReason).toBe("RECOVERY_ARCHIVE_DURABLE_HASH");
      expect(recovery).toHaveBeenCalledTimes(1);
    }finally{vi.restoreAllMocks();await rm(root,{recursive:true,force:true});}
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

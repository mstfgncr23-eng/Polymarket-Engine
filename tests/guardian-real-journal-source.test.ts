import { describe, expect, it } from "vitest";
import { PaperLedger,createIdleExperiment } from "@pm/core";
import { CHECKPOINT_SCHEMA_VERSION, MemoryEventJournal } from "@pm/db";
import { guardianReadOnlyJournalSource } from "../apps/engine/src/guardian-real-journal-source.js";
import { emptyResultAccounting } from "../apps/engine/src/trading-results.js";
const NOW="2026-10-10T10:00:00.000Z";
const setup=async()=>{
  const journal=new MemoryEventJournal();
  const experiment={...createIdleExperiment({durationHours:1,strategyVersion:"test",riskConfiguration:{},
    modelConfiguration:{},startingBalance:"1000",systemVersion:"test",gitSha:"test"}),
    id:"journal-backed-source",status:"RUNNING" as const,startedAt:NOW};
  const ledger=new PaperLedger("1000",new Date(NOW));
  const saved=await journal.saveCheckpoint({experimentId:experiment.id,sequence:1,payload:{
    schemaVersion:CHECKPOINT_SCHEMA_VERSION,durableCursor:0,
    experiment,ledger:ledger.snapshot(),positions:[],resultAccounting:emptyResultAccounting(),
    restingOrders:[],makerReservations:{},activeTriggers:[],grokSignals:[],
    settledMarkets:[],riskReductionOwners:[],riskReductionFailures:[],
    reversalProtectionStates:[],profitTrailingPairStates:[],settlementLookups:[],
    nakedDirectionalDiagnostics:[],lastInfrastructureAccrualAt:0,
  }});
  await journal.connectWithoutMigrate("readonly");
  return {journal,saved};
};
describe("G2H actual journal API under source-only capability",()=>{
  it("G2H-JOURNAL-01 reads the production EventJournal API and rejects all canonical writes",async()=>{
    const {journal,saved}=await setup();
    const reader=guardianReadOnlyJournalSource(journal);
    const checkpoints=await reader.listVerifiedCheckpoints();
    expect(checkpoints).toHaveLength(1);
    expect(checkpoints[0]?.id).toBe(saved.id);
    expect(checkpoints[0]?.verified).toBe(true);
    expect(await reader.latestDurableSequence()).toBe(0);
    expect(await reader.readDurableTransitionsAfter(0,1)).toEqual([]);
    await expect(journal.saveCheckpoint({experimentId:"x",sequence:2,payload:{}}))
      .rejects.toMatchObject({code:"JOURNAL_MUTATE_FORBIDDEN"});
  });
  it("G2H-JOURNAL-02 refuses a mode escalation and invalid read bounds",async()=>{
    const {journal}=await setup();
    const reader=guardianReadOnlyJournalSource(journal);
    await expect(reader.readDurableTransitionsAfter(0,0)).rejects.toThrow("GUARDIAN_SOURCE_INVALID_READ_BOUNDS");
    journal.access="boot-migrate";
    await expect(reader.latestDurableSequence()).rejects.toThrow("GUARDIAN_SOURCE_REQUIRES_READONLY_JOURNAL");
    expect(()=>guardianReadOnlyJournalSource(journal)).toThrow("GUARDIAN_SOURCE_REQUIRES_READONLY_JOURNAL");
  });
});

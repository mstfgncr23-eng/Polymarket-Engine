import { generateKeyPairSync } from "node:crypto";
import { mkdtemp,rm } from "node:fs/promises";
import {tmpdir} from "node:os";
import {join} from "node:path";
import {afterEach,describe,expect,it} from "vitest";
import { PaperLedger,createIdleExperiment } from "@pm/core";
import { CHECKPOINT_SCHEMA_VERSION,MemoryEventJournal } from "@pm/db";
import { GuardianWriterManifestPublisher } from "../apps/engine/src/guardian-writer-manifest-publisher.js";
import { guardianReadOnlyJournalSource } from "../apps/engine/src/guardian-real-journal-source.js";
import { GuardianReadonlySession } from "../apps/engine/src/guardian-readonly-session.js";
import { GuardianFileAckStore } from "../apps/engine/src/guardian-consumer-ack-store.js";
import { verifyGuardianWriterManifest } from "../apps/engine/src/guardian-signed-writer-manifest.js";
import { signGuardianDurablePage } from "../apps/engine/src/guardian-signed-durable-page.js";
import { emptyResultAccounting } from "../apps/engine/src/trading-results.js";

const NOW=Date.parse("2026-10-10T10:00:00Z");
const pair=generateKeyPairSync("ed25519");
const key=pair.privateKey.export({format:"pem",type:"pkcs8"}).toString();
const publicKey=pair.publicKey.export({format:"pem",type:"spki"}).toString();
const make=async()=>{
  const journal=new MemoryEventJournal();
  const experiment={...createIdleExperiment({durationHours:1,strategyVersion:"test",riskConfiguration:{},
    modelConfiguration:{},startingBalance:"1000",systemVersion:"test",gitSha:"test"}),
    id:"canonical-writer-exp",status:"RUNNING" as const,startedAt:new Date(NOW).toISOString()};
  const ledger=new PaperLedger("1000",new Date(NOW));
  await journal.saveCheckpoint({experimentId:experiment.id,sequence:1,payload:{
    schemaVersion:CHECKPOINT_SCHEMA_VERSION,durableCursor:0,ledger:ledger.snapshot(),
    experiment,positions:[],resultAccounting:emptyResultAccounting(),
    restingOrders:[],makerReservations:{},activeTriggers:[],grokSignals:[],
    riskReductionOwners:[],riskReductionFailures:[],reversalProtectionStates:[],
    profitTrailingPairStates:[],settlementLookups:[],settledMarkets:[],
    nakedDirectionalDiagnostics:[],lastInfrastructureAccrualAt:0,
  }});
  const issuer=new GuardianWriterManifestPublisher({
    issuerKeyId:"operator-key",authorityId:"canonical-epoch",writerEpoch:12,
    lastDurablyIssuedSequence:90,privateKeyPem:key,maxCheckpointLagRows:10,
  });
  const witness={experimentId:experiment.id,appliedCursor:0,
    economicState:"VERIFIED" as const,tradingMode:"paper" as const};
  return {journal,issuer,witness};
};
const dirs:string[]=[];
afterEach(async()=>{for(const dir of dirs.splice(0))await rm(dir,{recursive:true,force:true});});
describe("G2H producer signer -> canonical real Journal -> read-only consumer",()=>{
  it("G2H-PUBLISH-01 issues valid independently authenticated snapshot with bounded lag",async()=>{
    const {journal,issuer,witness}=await make();
    const m=await issuer.issue({journal,witness,nowMs:NOW});
    expect(m).toMatchObject({writerEpoch:12,manifestSequence:91,durableHighWater:0});
    expect(verifyGuardianWriterManifest(m,{
      issuerKeyId:"operator-key",authorityId:"canonical-epoch",publicKeyPem:publicKey,
      minimumWriterEpoch:12,minimumManifestSequence:90,nowMs:NOW+1,
    })).toEqual(m);
  });
  it("G2H-PUBLISH-02 withholds attestation for unknown economic commit or engine/DB divergence",async()=>{
    const {journal,issuer,witness}=await make();
    await expect(issuer.issue({journal,witness:{...witness,economicState:"UNKNOWN"},
      nowMs:NOW})).rejects.toMatchObject({reason:"WRITER_NOT_SAFE_TO_ATTEST"});
    await expect(issuer.issue({journal,witness:{...witness,tradingMode:"live"},
      nowMs:NOW})).rejects.toMatchObject({reason:"WRITER_NOT_SAFE_TO_ATTEST"});
    await expect(issuer.issue({journal,witness:{...witness,appliedCursor:1},
      nowMs:NOW})).rejects.toMatchObject({reason:"WRITER_NOT_AT_SOURCE_HIGHWATER"});
  });
  it("G2H-PUBLISH-03 uses real journal committed row and read-only downstream without any orders",async()=>{
    const {journal,issuer,witness}=await make();
    await journal.commitDurableTransition({operationKey:"mtm:1",type:"LEDGER_MARK_TO_MARKET",
      aggregateType:"position",aggregateId:"mkt",
      payload:{prices:{},markedAt:new Date(NOW+1000).toISOString()}},0);
    const manifest=await issuer.issue({
      journal,witness:{...witness,appliedCursor:1},nowMs:NOW,
    });
    expect(manifest.durableHighWater).toBe(1);
    await journal.connectWithoutMigrate("readonly");
    const canonical=guardianReadOnlyJournalSource(journal);
    // Closed fixture: a canonical fenced producer would issue signed pages
    // over a distinct authenticated channel, not from this read-only role.
    const source={...canonical,readSignedDurablePageAfter:async(
      cursor:number,limit:number,proof:typeof manifest,
    )=>{
      const rows=await canonical.readDurableTransitionsAfter(cursor,limit);
      return {rows,certificate:signGuardianDurablePage({
        rows,fromCursor:cursor,manifest:proof,privateKeyPem:key,nowMs:NOW,
      })};
    }};
    const dir=await mkdtemp(join(tmpdir(),"guardian-issuer-e2e-"));dirs.push(dir);
    const ack=new GuardianFileAckStore(join(dir,"cursor.guardian-ack.json"));
    const session=await GuardianReadonlySession.open({
      source,ackStore:ack,trust:{
        issuerKeyId:"operator-key",authorityId:"canonical-epoch",publicKeyPem:publicKey,
        minimumWriterEpoch:12,minimumManifestSequence:90,nowMs:NOW+1,
      },manifest,pageSize:1,maxBacklogRows:10,
    });
    expect(session.cursor).toBe(0);
    expect(await session.poll(manifest,NOW+2)).toEqual({cursor:1,highWater:1,caughtUp:true});
    expect((await ack.load())?.cursor).toBe(1);
    await expect(journal.commitDurableTransition({operationKey:"other",type:"LEDGER_MARK_TO_MARKET",
      aggregateType:"position",aggregateId:"mkt",payload:{prices:{},markedAt:new Date(NOW+2000).toISOString()}},1))
      .rejects.toMatchObject({code:"JOURNAL_MUTATE_FORBIDDEN"});
  });
});

describe("G2H issuer self-fencing before publication",()=>{
  it("G2H-PUBLISH-04 unresolved canonical source integrity or quarantine suppresses writer signature",async()=>{
    const {journal,issuer,witness}=await make();
    const checkpoints=await journal.listVerifiedCheckpoints(),baseline=checkpoints[0]!;
    const dirty={...baseline,payload:{...baseline.payload,reportSourceIntegrity:[{eventId:"unresolved"}]}};
    await expect(issuer.issue({journal:{
      latestDurableSequence:()=>journal.latestDurableSequence(),
      listVerifiedCheckpoints:async()=>[dirty],
    },witness,nowMs:NOW})).rejects.toMatchObject({reason:"UNRESOLVED_CANONICAL_SOURCE_INTEGRITY"});
    const quarantined={...baseline,payload:{...baseline.payload,reportQuarantine:{disposition:"QUARANTINED_NON_FINAL"}}};
    await expect(issuer.issue({journal:{
      latestDurableSequence:()=>journal.latestDurableSequence(),
      listVerifiedCheckpoints:async()=>[quarantined],
    },witness,nowMs:NOW})).rejects.toMatchObject({reason:"UNRESOLVED_CANONICAL_SOURCE_INTEGRITY"});
  });
  it("G2H-PUBLISH-05 conflicting source snapshots at exact same durable cursor require reconcile",async()=>{
    const {journal,issuer,witness}=await make();
    const checkpoints=await journal.listVerifiedCheckpoints(),first=checkpoints[0]!;
    const fork={...first,id:"competing-checkpoint",contentHash:"f".repeat(64)};
    await expect(issuer.issue({journal:{
      latestDurableSequence:()=>journal.latestDurableSequence(),
      listVerifiedCheckpoints:async()=>[first,fork],
    },witness,nowMs:NOW})).rejects.toMatchObject({reason:"CHECKPOINT_ECONOMIC_AMBIGUITY"});
  });
  it("G2H-PUBLISH-06 concurrent attestations never reuse the same sequence in one process",async()=>{
    const {journal,issuer,witness}=await make();
    let unblock:()=>void=()=>{};
    const hold=new Promise<void>(resolve=>unblock=resolve);
    const delayed={
      latestDurableSequence:async()=>{await hold;return journal.latestDurableSequence();},
      listVerifiedCheckpoints:()=>journal.listVerifiedCheckpoints(),
    };
    const first=issuer.issue({journal:delayed,witness,nowMs:NOW});
    await expect(issuer.issue({journal:delayed,witness,nowMs:NOW}))
      .rejects.toMatchObject({reason:"CONCURRENT_SIGNING_NOT_FENCED"});
    unblock();
    const accepted=await first;
    expect(accepted.manifestSequence).toBe(91);
    const next=await issuer.issue({journal,witness,nowMs:NOW+1});
    expect(next.manifestSequence).toBe(92);
  });
});

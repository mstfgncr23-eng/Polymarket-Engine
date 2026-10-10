import { generateKeyPairSync, sign } from "node:crypto";
import { describe, expect, it } from "vitest";
import {
  guardianWriterManifestBytes, verifyGuardianWriterManifest,
  bootstrapGuardianSignedCheckpoint, type GuardianWriterManifest,
  type GuardianWriterTrust,
} from "../apps/engine/src/guardian-signed-writer-manifest.js";
import {
  CHECKPOINT_SCHEMA_VERSION, hashCheckpointPayload,
} from "@pm/db";
import { PaperLedger, createIdleExperiment } from "@pm/core";
import { emptyResultAccounting } from "../apps/engine/src/trading-results.js";

const NOW=Date.parse("2026-10-10T10:00:00Z");
const {privateKey,publicKey}=generateKeyPairSync("ed25519");
const other=generateKeyPairSync("ed25519");
const digest="a".repeat(64);
const payload:Omit<GuardianWriterManifest,"signature">={
  version:1,issuerKeyId:"operator-auth-key-1",authorityId:"canonical-writer",
  writerEpoch:7,manifestSequence:15,experimentId:"exp-one",
  checkpointId:"checkpoint-1",checkpointHash:digest,durableHighWater:20,
  issuedAtMs:NOW-1000,expiresAtMs:NOW+3000,
};
const issue=(data=payload,key=privateKey):GuardianWriterManifest=>({
  ...data,signature:sign(null,guardianWriterManifestBytes(data),key).toString("base64"),
});
const trust=(overrides:Partial<GuardianWriterTrust>={}):GuardianWriterTrust=>({
  issuerKeyId:payload.issuerKeyId,authorityId:payload.authorityId,
  publicKeyPem:publicKey.export({format:"pem",type:"spki"}).toString(),
  minimumWriterEpoch:7,minimumManifestSequence:14,nowMs:NOW,...overrides,
});
const rejection=(fn:()=>unknown,reason:string)=>expect(fn).toThrowError(
  expect.objectContaining({reason}),
);

describe("G2H Ed25519 external writer manifest: offline producer attestation",()=>{
  it("G2H-AUTH-01 authenticates an independently pinned issuer and writer incarnation",()=>{
    expect(verifyGuardianWriterManifest(issue(),trust())).toMatchObject(payload);
  });
  it("G2H-AUTH-02 rejects forged manifest, same-DB hash swap and wrong key",()=>{
    rejection(()=>verifyGuardianWriterManifest({...issue(),checkpointHash:"b".repeat(64)},trust()),"SIGNATURE_INVALID");
    rejection(()=>verifyGuardianWriterManifest(issue(payload,other.privateKey),trust()),"SIGNATURE_INVALID");
    rejection(()=>verifyGuardianWriterManifest(issue(),trust({
      publicKeyPem:other.publicKey.export({format:"pem",type:"spki"}).toString(),
    })),"SIGNATURE_INVALID");
    rejection(()=>verifyGuardianWriterManifest(issue(),trust({authorityId:"another-writer"})),"UNTRUSTED_ISSUER");
    rejection(()=>verifyGuardianWriterManifest({...issue(),issuerKeyId:"wrong"},trust()),"UNTRUSTED_ISSUER");
  });
  it("G2H-AUTH-03 rejects replay/older epoch but accepts newer signed incarnation",()=>{
    rejection(()=>verifyGuardianWriterManifest(issue(),trust({minimumWriterEpoch:8})),"EPOCH_REGRESSION");
    rejection(()=>verifyGuardianWriterManifest(issue(),trust({minimumManifestSequence:15})),"MANIFEST_REPLAY");
    expect(verifyGuardianWriterManifest(issue({...payload,writerEpoch:8,manifestSequence:1}),
      trust({minimumManifestSequence:50})).writerEpoch).toBe(8);
  });
  it("G2H-AUTH-04 enforces short manifest validity and future clock bound",()=>{
    rejection(()=>verifyGuardianWriterManifest(issue(),trust({nowMs:NOW+3100})),"MANIFEST_EXPIRED");
    rejection(()=>verifyGuardianWriterManifest(issue({...payload,issuedAtMs:NOW+6000,
      expiresAtMs:NOW+7000}),trust()),"MANIFEST_EXPIRED");
    rejection(()=>verifyGuardianWriterManifest(issue({...payload,expiresAtMs:NOW+40000}),
      trust()),"MANIFEST_EXPIRED");
  });
  it("G2H-AUTH-05 malformed envelopes, extra fields and malformed public key fail closed",()=>{
    rejection(()=>verifyGuardianWriterManifest({...issue(),extra:"field"},trust()),"INVALID_MANIFEST");
    rejection(()=>verifyGuardianWriterManifest({...issue(),signature:"not-base64"},trust()),"INVALID_MANIFEST");
    rejection(()=>verifyGuardianWriterManifest(null,trust()),"INVALID_MANIFEST");
    rejection(()=>verifyGuardianWriterManifest(issue(),trust({publicKeyPem:"bad-pem"})),"SIGNATURE_INVALID");
  });
  it("G2H-AUTH-06 authenticated hash pins exact DB checkpoint, not an unrelated self-hashed row",async()=>{
    const ledger=new PaperLedger("1000",new Date(NOW));
    const exp={...createIdleExperiment({durationHours:1,strategyVersion:"test",riskConfiguration:{},
      modelConfiguration:{},startingBalance:"1000",systemVersion:"test",gitSha:"test"}),
      id:"exp-one",status:"RUNNING" as const,startedAt:new Date(NOW).toISOString()};
    const body={schemaVersion:CHECKPOINT_SCHEMA_VERSION,durableCursor:20,
      experiment:exp,ledger:ledger.snapshot(),positions:[],resultAccounting:emptyResultAccounting(),
      restingOrders:[],makerReservations:{},activeTriggers:[],grokSignals:[],
      settledMarkets:[],riskReductionOwners:[],riskReductionFailures:[],
      reversalProtectionStates:[],profitTrailingPairStates:[],settlementLookups:[],
      nakedDirectionalDiagnostics:[],lastInfrastructureAccrualAt:0,
    };
    const hash=hashCheckpointPayload(body);
    const record={id:"checkpoint-1",experimentId:"exp-one",createdAt:new Date(NOW).toISOString(),
      sequence:1,verified:true,contentHash:hash,
      payload:{...body,checkpointContentHash:hash}};
    const manifest=issue({...payload,checkpointHash:hash});
    const source=(rows=[record],highWater=20)=>({
      listVerifiedCheckpoints:async()=>rows,
      latestDurableSequence:async()=>highWater,
    });
    const opened=await bootstrapGuardianSignedCheckpoint({
      source:source(),manifest,trust:trust(),maxBacklogRows:10,
    });
    expect(opened.state.projection.cursor).toBe(20);
    expect(opened.manifest.authorityId).toBe("canonical-writer");
    await expect(bootstrapGuardianSignedCheckpoint({
      source:source([{...record,id:"other"}]),manifest,trust:trust(),maxBacklogRows:10,
    })).rejects.toMatchObject({reason:"CHECKPOINT_NOT_FOUND"});
    await expect(bootstrapGuardianSignedCheckpoint({
      source:source([record],19),manifest,trust:trust(),maxBacklogRows:10,
    })).rejects.toMatchObject({reason:"SOURCE_CURSOR_REGRESSION"});
    await expect(bootstrapGuardianSignedCheckpoint({
      source:source([record],20),
      manifest:issue({...payload,checkpointHash:hash,durableHighWater:21}),
      trust:trust(),maxBacklogRows:10,
    })).rejects.toMatchObject({reason:"SOURCE_BEHIND_PRODUCER"});
    await expect(bootstrapGuardianSignedCheckpoint({
      source:source([record],20),
      manifest:issue({...payload,checkpointHash:hash,durableHighWater:19}),
      trust:trust(),maxBacklogRows:10,
    })).rejects.toMatchObject({reason:"CHECKPOINT_BEYOND_PRODUCER"});
  });
});

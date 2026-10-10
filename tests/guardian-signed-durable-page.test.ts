import { generateKeyPairSync, sign } from "node:crypto";
import { describe, expect, it } from "vitest";
import { hashDurablePayload, type DurableTransition } from "@pm/db";
import { plannedDurableTransition } from "../apps/engine/src/recovery.js";
import {
  guardianDurableRowsDigest,signGuardianDurablePage,verifyGuardianSignedPage,
} from "../apps/engine/src/guardian-signed-durable-page.js";
import {
  guardianWriterManifestBytes,type GuardianWriterManifest,type GuardianWriterTrust,
} from "../apps/engine/src/guardian-signed-writer-manifest.js";

const NOW=Date.parse("2026-10-10T10:00:00Z");
const keys=generateKeyPairSync("ed25519"),forger=generateKeyPairSync("ed25519");
const priv=keys.privateKey.export({type:"pkcs8",format:"pem"}).toString();
const trust:GuardianWriterTrust={
  issuerKeyId:"writer-key-1",authorityId:"engine-epoch-1",
  publicKeyPem:keys.publicKey.export({type:"spki",format:"pem"}).toString(),
  minimumWriterEpoch:1,minimumManifestSequence:0,nowMs:NOW,
};
const manifest=():GuardianWriterManifest=>{
  const body={version:1 as const,issuerKeyId:trust.issuerKeyId,authorityId:trust.authorityId,
    writerEpoch:1,manifestSequence:4,experimentId:"source-experiment",
    checkpointId:"source-checkpoint",checkpointHash:"a".repeat(64),
    durableHighWater:22,issuedAtMs:NOW-1000,expiresAtMs:NOW+10000};
  return {...body,signature:sign(null,guardianWriterManifestBytes(body),keys.privateKey).toString("base64")};
};
const make=(sequence:number):DurableTransition=>{
  const payload={prices:{abc:{up:"0.40",down:"0.60"}},markedAt:new Date(NOW+sequence).toISOString()};
  const t=plannedDurableTransition({type:"LEDGER_MARK_TO_MARKET",
    operationKey:`mtm:${sequence}`,aggregateType:"position",
    aggregateId:"abc",payload},new Date(NOW+sequence),sequence);
  return {...t,payloadHash:hashDurablePayload(payload)};
};
const rows=[make(21),make(22)];
const signed=()=>signGuardianDurablePage({rows,fromCursor:20,manifest:manifest(),privateKeyPem:priv,nowMs:NOW});
const verifyPage=(input:Partial<Parameters<typeof verifyGuardianSignedPage>[0]>={})=>
  verifyGuardianSignedPage({
    rows,certificate:signed(),manifest:manifest(),trust,nowMs:NOW,
    fromCursor:20,maximumRows:2,...input,
  });
const fail=(callback:()=>unknown,reason:string)=>
  expect(callback).toThrowError(expect.objectContaining({reason}));
describe("G2H signed contiguous source page, independent writer proof",()=>{
  it("G2H-PAGE-01 authenticates exact two rows, sequence, checkpoint and writer",()=>{
    expect(verifyPage()).toEqual(rows);
    expect(guardianDurableRowsDigest(rows,20,22)).toMatch(/^[a-f0-9]{64}$/);
  });
  it("G2H-PAGE-02 modified economic payload plus recomputed self-hash cannot pass issuer signature",()=>{
    const tampered=structuredClone(rows);
    tampered[0]!.payload.prices={abc:{up:"0.91",down:"0.09"}};
    tampered[0]!.payloadHash=hashDurablePayload(tampered[0]!.payload);
    fail(()=>verifyPage({rows:tampered}),"PAGE_DIGEST_MISMATCH");
  });
  it("G2H-PAGE-03 fields omitted from DB's payload digest remain authenticated",()=>{
    fail(()=>verifyPage({rows:[{...rows[0]!,operationKey:"another-op"},rows[1]!]}),"PAGE_DIGEST_MISMATCH");
    fail(()=>verifyPage({rows:[{...rows[0]!,recordedAt:new Date(NOW+5000).toISOString()},rows[1]!]}),
      "PAGE_DIGEST_MISMATCH");
  });
  it("G2H-PAGE-04 refuses missing or forged page signature",()=>{
    fail(()=>verifyPage({certificate:null}),"MISSING_PAGE_CERTIFICATE");
    fail(()=>verifyPage({certificate:{...signed(),signature:"A".repeat(86)+"=="}}),"PAGE_SIGNATURE_INVALID");
    const wrong=signGuardianDurablePage({rows,fromCursor:20,manifest:manifest(),
      privateKeyPem:forger.privateKey.export({type:"pkcs8",format:"pem"}).toString(),nowMs:NOW});
    fail(()=>verifyPage({certificate:wrong}),"PAGE_SIGNATURE_INVALID");
  });
  it("G2H-PAGE-05 wrong epoch, issuer, checkpoint, manifest generation all HALT",()=>{
    fail(()=>verifyPage({certificate:{...signed(),writerEpoch:2}}),"PAGE_WRITER_MISMATCH");
    fail(()=>verifyPage({certificate:{...signed(),checkpointId:"wrong"}}),"PAGE_WRITER_MISMATCH");
    fail(()=>verifyPage({certificate:{...signed(),manifestSequence:3}}),"PAGE_WRITER_MISMATCH");
  });
  it("G2H-PAGE-06 gaps, reordering, missing rows and out-of-bound read HALT",()=>{
    fail(()=>verifyPage({rows:[rows[1]!,rows[0]!]}),"INVALID_PAGE_ROW");
    fail(()=>verifyPage({rows:[]}),"PAGE_RANGE_MISMATCH");
    fail(()=>verifyPage({fromCursor:19}),"PAGE_RANGE_MISMATCH");
    fail(()=>verifyPage({maximumRows:1}),"PAGE_RANGE_MISMATCH");
    fail(()=>verifyPage({certificate:{...signed(),throughCursor:23}}),"PAGE_RANGE_MISMATCH");
  });
  it("G2H-PAGE-07 rejects expired page and absent row provenance",()=>{
    fail(()=>verifyPage({nowMs:NOW+5010}),"PAGE_EXPIRED");
    fail(()=>verifyPage({rows:[{...rows[0]!,payloadHash:"f".repeat(64)},rows[1]!]}),
      "PAGE_PAYLOAD_HASH_MISMATCH");
    fail(()=>guardianDurableRowsDigest([rows[1]!],20,21),"INVALID_PAGE_ROW");
  });
  it("G2H-PAGE-08 signer refuses to attest pages crossing signed waterline",()=>{
    fail(()=>signGuardianDurablePage({rows,fromCursor:21,manifest:manifest(),
      privateKeyPem:priv,nowMs:NOW}),"UNSAFE_PRODUCER_RANGE");
    fail(()=>signGuardianDurablePage({rows:[],fromCursor:20,manifest:manifest(),
      privateKeyPem:priv,nowMs:NOW}),"INVALID_PAGE_RANGE");
  });
});

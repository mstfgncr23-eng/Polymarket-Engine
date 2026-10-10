import { createHash, createPrivateKey, createPublicKey, sign, verify } from "node:crypto";
import { hashDurablePayload, type DurableTransition } from "@pm/db";
import type { GuardianWriterManifest, GuardianWriterTrust } from "./guardian-signed-writer-manifest.js";

/** G2H OFFLINE authenticated page. This is NOT a journal replacement. */
export interface GuardianSignedDurablePage {
  version:1;
  issuerKeyId:string;
  authorityId:string;
  writerEpoch:number;
  manifestSequence:number;
  experimentId:string;
  checkpointId:string;
  checkpointHash:string;
  fromCursor:number;
  throughCursor:number;
  durableHighWater:number;
  rowsHash:string;
  issuedAtMs:number;
  expiresAtMs:number;
  signature:string;
}
export interface GuardianSignedPageSource {
  /** A second, independently authenticated canonical-writer delivery path. */
  readSignedDurablePageAfter(cursor:number,limit:number,
    manifest:GuardianWriterManifest):Promise<{
    rows:DurableTransition[];certificate:unknown;
  }>;
}
export class GuardianSignedPageHalted extends Error {
  constructor(readonly reason:string){super(`GUARDIAN_SIGNED_PAGE_HALTED:${reason}`);}
}
const halt=(reason:string):never=>{throw new GuardianSignedPageHalted(reason);};
const digestPattern=/^[a-f0-9]{64}$/;
const id=(s:unknown)=>typeof s==="string" && s.length>=1 && s.length<=256;
const natural=(n:unknown)=>Number.isSafeInteger(n) && Number(n)>=0;
const bytes=(page:Omit<GuardianSignedDurablePage,"signature">)=>{
  const body={
    version:page.version,issuerKeyId:page.issuerKeyId,
    authorityId:page.authorityId,writerEpoch:page.writerEpoch,
    manifestSequence:page.manifestSequence,experimentId:page.experimentId,
    checkpointId:page.checkpointId,checkpointHash:page.checkpointHash,
    fromCursor:page.fromCursor,throughCursor:page.throughCursor,
    durableHighWater:page.durableHighWater,rowsHash:page.rowsHash,
    issuedAtMs:page.issuedAtMs,expiresAtMs:page.expiresAtMs,
  };
  return Buffer.from("pm-guardian-g2h-durable-page-v1\n"+JSON.stringify(body));
};

/**
 * One cryptographic digest binds ALL canonical fields of EACH durable row,
 * including the full economic payload hash, not just sequence/high-water.
 * The signed range has NO hidden gaps, duplicates or omitted records.
 */
export function guardianDurableRowsDigest(rows:DurableTransition[],fromCursor:number,throughCursor:number):string {
  if (!Array.isArray(rows) || rows.length<1 || rows.length>512
    || !natural(fromCursor) || !natural(throughCursor)
    || throughCursor!==fromCursor+rows.length) return halt("INVALID_PAGE_RANGE");
  const values=rows.map((row,index)=>{
    if (!row || row.sequence!==fromCursor+index+1
      || !id(row.operationKey)||!id(row.type)||!id(row.aggregateType)
      || !id(row.aggregateId)||!id(row.occurredAt)||!id(row.recordedAt)
      || typeof row.payloadHash!=="string"||!digestPattern.test(row.payloadHash)
      || !row.payload || typeof row.payload!=="object"
      || Array.isArray(row.payload)) return halt("INVALID_PAGE_ROW");
    let payloadDigest:string;
    try {payloadDigest=hashDurablePayload(row.payload);}
    catch{return halt("INVALID_PAGE_ROW");}
    if (payloadDigest!==row.payloadHash) return halt("PAGE_PAYLOAD_HASH_MISMATCH");
    return {
      sequence:row.sequence,operationKey:row.operationKey,type:row.type,
      aggregateType:row.aggregateType,aggregateId:row.aggregateId,
      payloadHash:row.payloadHash,occurredAt:row.occurredAt,
      recordedAt:row.recordedAt,
    };
  });
  return createHash("sha256").update("pm-guardian-g2h-row-range-v1\n"+JSON.stringify(values)).digest("hex");
}

/** TEST/producer helper. Caller MUST own independently fenced signing authority. */
export function signGuardianDurablePage(input:{
  rows:DurableTransition[];fromCursor:number;
  manifest:GuardianWriterManifest;privateKeyPem:string;nowMs:number;
}):GuardianSignedDurablePage {
  const {manifest,fromCursor,rows,nowMs}=input;
  if(!natural(nowMs)||!natural(fromCursor)
    || fromCursor+rows.length>manifest.durableHighWater
    || nowMs<manifest.issuedAtMs-5000||nowMs>=manifest.expiresAtMs)
    return halt("UNSAFE_PRODUCER_RANGE");
  const throughCursor=fromCursor+rows.length;
  const rowsHash=guardianDurableRowsDigest(rows,fromCursor,throughCursor);
  const body={
    version:1 as const,issuerKeyId:manifest.issuerKeyId,
    authorityId:manifest.authorityId,writerEpoch:manifest.writerEpoch,
    manifestSequence:manifest.manifestSequence,experimentId:manifest.experimentId,
    checkpointId:manifest.checkpointId,checkpointHash:manifest.checkpointHash,
    fromCursor,throughCursor,durableHighWater:manifest.durableHighWater,
    rowsHash,issuedAtMs:nowMs,expiresAtMs:Math.min(nowMs+5000,manifest.expiresAtMs),
  };
  try{
    const key=createPrivateKey(input.privateKeyPem);
    if(key.asymmetricKeyType!=="ed25519")return halt("INVALID_SIGNER");
    return {...body,signature:sign(null,bytes(body),key).toString("base64")};
  }catch{return halt("INVALID_SIGNER");}
}

export function verifyGuardianSignedPage(input:{
  rows:DurableTransition[];certificate:unknown;
  manifest:GuardianWriterManifest;trust:GuardianWriterTrust;
  nowMs:number;fromCursor:number;maximumRows:number;
}):DurableTransition[] {
  const {certificate:raw,manifest,trust,rows,nowMs,fromCursor,maximumRows}=input;
  if (!raw || typeof raw!=="object" || Array.isArray(raw)) return halt("MISSING_PAGE_CERTIFICATE");
  const c=raw as Record<string,unknown>;
  if (Object.keys(c).length!==15 || c.version!==1
    || !id(c.issuerKeyId)||!id(c.authorityId)||!id(c.experimentId)
    || !id(c.checkpointId)||!natural(c.writerEpoch)||!natural(c.manifestSequence)
    || !natural(c.fromCursor)||!natural(c.throughCursor)
    || !natural(c.durableHighWater)||!natural(c.issuedAtMs)||!natural(c.expiresAtMs)
    || typeof c.checkpointHash!=="string"||!digestPattern.test(c.checkpointHash)
    || typeof c.rowsHash!=="string"||!digestPattern.test(c.rowsHash)
    || typeof c.signature!=="string"||!/^[A-Za-z0-9+/]{86}==$/.test(c.signature))
    return halt("INVALID_PAGE_CERTIFICATE");
  const page=c as unknown as GuardianSignedDurablePage;
  if (page.issuerKeyId!==trust.issuerKeyId||page.authorityId!==trust.authorityId
    || page.issuerKeyId!==manifest.issuerKeyId||page.authorityId!==manifest.authorityId
    || page.writerEpoch!==manifest.writerEpoch
    || page.manifestSequence!==manifest.manifestSequence
    || page.experimentId!==manifest.experimentId
    || page.checkpointId!==manifest.checkpointId
    || page.checkpointHash!==manifest.checkpointHash
    || page.durableHighWater!==manifest.durableHighWater) return halt("PAGE_WRITER_MISMATCH");
  if (!natural(nowMs)||!natural(fromCursor)||!natural(maximumRows)
    || maximumRows<1||maximumRows>512||page.fromCursor!==fromCursor
    || !Array.isArray(rows)||rows.length<1||rows.length>maximumRows
    || page.throughCursor!==fromCursor+rows.length
    || page.throughCursor>manifest.durableHighWater) return halt("PAGE_RANGE_MISMATCH");
  if (nowMs<page.issuedAtMs-5000||nowMs>=page.expiresAtMs
    || page.expiresAtMs<=page.issuedAtMs
    || page.expiresAtMs>manifest.expiresAtMs
    || page.expiresAtMs-page.issuedAtMs>5000
    || page.issuedAtMs<manifest.issuedAtMs-5000) return halt("PAGE_EXPIRED");
  let key;
  try{
    key=createPublicKey(trust.publicKeyPem);
    if (key.asymmetricKeyType!=="ed25519"
      || !verify(null,bytes(page),key,Buffer.from(page.signature,"base64")))
      return halt("PAGE_SIGNATURE_INVALID");
  }catch{return halt("PAGE_SIGNATURE_INVALID");}
  if (guardianDurableRowsDigest(rows,fromCursor,page.throughCursor)!==page.rowsHash)
    return halt("PAGE_DIGEST_MISMATCH");
  return rows;
}

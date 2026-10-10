import { createPublicKey, verify, type KeyObject } from "node:crypto";
import {
  bootstrapGuardianPinnedCheckpoint,
  type GuardianCheckpointReadSource,
} from "./guardian-readonly-checkpoint.js";

/**
 * Offline G2H producer attestation boundary. The Ed25519 public key and epoch
 * floor MUST come from independently trusted operator-owned configuration.
 * DB checkpoint hashes alone cannot establish writer identity.
 *
 * This proves *signature ownership*, not an exclusive running writer lease.
 * No signer, private key, network listener, DB mutation, or second Guardian.
 */
export interface GuardianWriterManifest {
  version: 1;
  issuerKeyId: string;
  authorityId: string;
  writerEpoch: number;
  manifestSequence: number;
  experimentId: string;
  checkpointId: string;
  checkpointHash: string;
  durableHighWater: number;
  issuedAtMs: number;
  expiresAtMs: number;
  signature: string;
}
export interface GuardianWriterTrust {
  issuerKeyId: string;
  authorityId: string;
  publicKeyPem: string;
  minimumWriterEpoch: number;
  minimumManifestSequence: number;
  nowMs: number;
}
export type GuardianAttestationHaltReason =
  | "INVALID_MANIFEST" | "UNTRUSTED_ISSUER" | "EPOCH_REGRESSION"
  | "MANIFEST_REPLAY" | "MANIFEST_EXPIRED" | "SIGNATURE_INVALID"
  | "CHECKPOINT_BEYOND_PRODUCER" | "SOURCE_BEHIND_PRODUCER";
export class GuardianWriterAttestationHalted extends Error {
  constructor(readonly reason: GuardianAttestationHaltReason) {
    super(`GUARDIAN_WRITER_ATTESTATION_HALTED:${reason}`);
  }
}
function halt(reason: GuardianAttestationHaltReason): never {
  throw new GuardianWriterAttestationHalted(reason);
}
function identifier(value: unknown): value is string {
  return typeof value === "string" && value.length >= 1 && value.length <= 256;
}
/**
 * Canonical byte contract. Do not serialize caller object property order.
 * Signed with "pm-guardian-g2h-writer-v1\n" domain separation.
 */
export function guardianWriterManifestBytes(manifest: Omit<GuardianWriterManifest, "signature">): Buffer {
  const obj = {
    version: manifest.version, issuerKeyId: manifest.issuerKeyId,
    authorityId: manifest.authorityId, writerEpoch: manifest.writerEpoch,
    manifestSequence: manifest.manifestSequence, experimentId: manifest.experimentId,
    checkpointId: manifest.checkpointId, checkpointHash: manifest.checkpointHash,
    durableHighWater: manifest.durableHighWater, issuedAtMs: manifest.issuedAtMs,
    expiresAtMs: manifest.expiresAtMs,
  };
  return Buffer.from("pm-guardian-g2h-writer-v1\n" + JSON.stringify(obj), "utf8");
}
export function verifyGuardianWriterManifest(raw: unknown, trust: GuardianWriterTrust): GuardianWriterManifest {
  if (!raw || typeof raw !== "object" || Array.isArray(raw) || !trust
    || !identifier(trust.authorityId) || !identifier(trust.issuerKeyId)
    || !Number.isSafeInteger(trust.minimumWriterEpoch) || trust.minimumWriterEpoch < 1
    || !Number.isSafeInteger(trust.minimumManifestSequence) || trust.minimumManifestSequence < 0
    || !Number.isSafeInteger(trust.nowMs) || typeof trust.publicKeyPem !== "string") {
    return halt("INVALID_MANIFEST");
  }
  const m = raw as Record<string,unknown>;
  if (Object.keys(m).length !== 12 || m.version !== 1
    || !identifier(m.issuerKeyId) || !identifier(m.authorityId)
    || !identifier(m.experimentId) || !identifier(m.checkpointId)
    || typeof m.checkpointHash !== "string" || !/^[a-f0-9]{64}$/.test(m.checkpointHash)
    || !Number.isSafeInteger(m.writerEpoch) || (m.writerEpoch as number) < 1
    || !Number.isSafeInteger(m.manifestSequence) || (m.manifestSequence as number) < 1
    || !Number.isSafeInteger(m.durableHighWater) || (m.durableHighWater as number) < 0
    || !Number.isSafeInteger(m.issuedAtMs) || !Number.isSafeInteger(m.expiresAtMs)
    || typeof m.signature !== "string" || !/^[A-Za-z0-9+/]{86}==$/.test(m.signature)) {
    return halt("INVALID_MANIFEST");
  }
  const manifest=m as unknown as GuardianWriterManifest;
  if (manifest.issuerKeyId !== trust.issuerKeyId || manifest.authorityId !== trust.authorityId)
    return halt("UNTRUSTED_ISSUER");
  if (manifest.writerEpoch < trust.minimumWriterEpoch) return halt("EPOCH_REGRESSION");
  if (manifest.writerEpoch === trust.minimumWriterEpoch
    && manifest.manifestSequence <= trust.minimumManifestSequence) return halt("MANIFEST_REPLAY");
  // A signed manifest is a short-lived observation, not a permanent permit.
  // Allow at most 5 seconds future clock skew and at most 30 seconds life.
  if (manifest.issuedAtMs > trust.nowMs + 5_000
    || manifest.expiresAtMs <= trust.nowMs
    || manifest.expiresAtMs <= manifest.issuedAtMs
    || manifest.expiresAtMs - manifest.issuedAtMs > 30_000) return halt("MANIFEST_EXPIRED");
  let key: KeyObject;
  try {
    key=createPublicKey(trust.publicKeyPem);
    if (key.asymmetricKeyType !== "ed25519"
      || !verify(null,guardianWriterManifestBytes(manifest),key,Buffer.from(manifest.signature,"base64")))
      return halt("SIGNATURE_INVALID");
  } catch {
    return halt("SIGNATURE_INVALID");
  }
  return manifest;
}

/** One authenticated manifest -> pinned DB checkpoint; no implicit fallback. */
export async function bootstrapGuardianSignedCheckpoint(input: {
  source: GuardianCheckpointReadSource;
  manifest: unknown;
  trust: GuardianWriterTrust;
  maxBacklogRows: number;
}) {
  const manifest=verifyGuardianWriterManifest(input.manifest,input.trust);
  const result=await bootstrapGuardianPinnedCheckpoint({
    source:input.source, maxBacklogRows:input.maxBacklogRows,
    pin:{ checkpointId:manifest.checkpointId, contentHash:manifest.checkpointHash,
      experimentId:manifest.experimentId, authorityId:manifest.authorityId },
  });
  if (result.state.projection.cursor > manifest.durableHighWater) return halt("CHECKPOINT_BEYOND_PRODUCER");
  if (result.highWater < manifest.durableHighWater) return halt("SOURCE_BEHIND_PRODUCER");
  return { ...result, manifest };
}

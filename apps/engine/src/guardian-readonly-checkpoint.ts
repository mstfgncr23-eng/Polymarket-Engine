import { hashCheckpointPayload, type EngineCheckpoint } from "@pm/db";
import { parseCheckpointDurableCursor } from "./recovery.js";
import {
  bootstrapGuardianOfflineSource, type GuardianOfflineState,
} from "./guardian-offline-durable-source.js";

/**
 * G2H OFFLINE handoff: verify one explicitly pinned checkpoint from a trusted
 * read-only journal connection. The pin MUST arrive independently of this
 * journal response (e.g. from a separately authenticated writer manifest).
 *
 * The current DB checkpoint digest is self-contained, NOT a writer signature.
 * This adapter does not establish transport identity, a writer lease, an ACK
 * watermark, a process, a network connection, or economic order authority.
 */
export interface GuardianCheckpointReadSource {
  listVerifiedCheckpoints(): Promise<EngineCheckpoint[]>;
  latestDurableSequence(): Promise<number>;
}
export interface GuardianCheckpointPin {
  checkpointId: string;
  contentHash: string;
  experimentId: string;
  /** Already authenticated by the caller; merely carried through this adapter. */
  authorityId: string;
}
export type GuardianCheckpointHaltReason =
  | "INVALID_PIN" | "SOURCE_UNAVAILABLE" | "CHECKPOINT_NOT_FOUND"
  | "CHECKPOINT_AMBIGUOUS" | "CHECKPOINT_MISMATCH"
  | "SOURCE_CURSOR_REGRESSION" | "BACKLOG_EXCEEDED"
  | "CHECKPOINT_RECOVERY_REJECTED";
export class GuardianCheckpointHalted extends Error {
  constructor(readonly reason: GuardianCheckpointHaltReason) {
    super(`GUARDIAN_CHECKPOINT_HALTED:${reason}`);
  }
}
function halt(reason: GuardianCheckpointHaltReason): never {
  throw new GuardianCheckpointHalted(reason);
}

export async function bootstrapGuardianPinnedCheckpoint(input: {
  source: GuardianCheckpointReadSource;
  /** Pin must be trusted independently, never copied from source.listVerifiedCheckpoints(). */
  pin: GuardianCheckpointPin;
  /** Bound the mandatory replay after this checkpoint. */
  maxBacklogRows: number;
}): Promise<{ state: GuardianOfflineState; checkpointId: string; highWater: number }> {
  const { source, pin, maxBacklogRows } = input;
  if (!pin || !pin.checkpointId || !pin.experimentId || !pin.authorityId
    || !/^[a-f0-9]{64}$/.test(pin.contentHash)
    || !Number.isSafeInteger(maxBacklogRows) || maxBacklogRows < 1 || maxBacklogRows > 100_000) {
    return halt("INVALID_PIN");
  }
  let checkpoints: EngineCheckpoint[];
  let highWater: number;
  try {
    checkpoints = await source.listVerifiedCheckpoints();
    highWater = await source.latestDurableSequence();
  } catch {
    return halt("SOURCE_UNAVAILABLE");
  }
  if (!Array.isArray(checkpoints)) return halt("SOURCE_UNAVAILABLE");
  const matches = checkpoints.filter(row => row?.id === pin.checkpointId);
  if (matches.length === 0) return halt("CHECKPOINT_NOT_FOUND");
  if (matches.length !== 1) return halt("CHECKPOINT_AMBIGUOUS");
  const checkpoint = matches[0]!;
  if (!checkpoint.payload || typeof checkpoint.payload !== "object"
    || Array.isArray(checkpoint.payload)
    || !checkpoint.verified || checkpoint.experimentId !== pin.experimentId
    || checkpoint.contentHash !== pin.contentHash
    || checkpoint.payload.checkpointContentHash !== pin.contentHash) {
    return halt("CHECKPOINT_MISMATCH");
  }
  try {
    if (hashCheckpointPayload(checkpoint.payload) !== pin.contentHash) {
      return halt("CHECKPOINT_MISMATCH");
    }
  } catch {
    return halt("CHECKPOINT_MISMATCH");
  }
  const cursor = parseCheckpointDurableCursor(checkpoint.payload);
  if (cursor === null || !Number.isSafeInteger(highWater) || highWater < cursor) {
    return halt("SOURCE_CURSOR_REGRESSION");
  }
  if (highWater - cursor > maxBacklogRows) return halt("BACKLOG_EXCEEDED");
  let state: GuardianOfflineState;
  try {
    state = bootstrapGuardianOfflineSource({
      payload: checkpoint.payload, externallyVerifiedContentHash: pin.contentHash,
      authorityId: pin.authorityId,
    });
  } catch {
    return halt("CHECKPOINT_RECOVERY_REJECTED");
  }
  if (state.projection.experimentId !== pin.experimentId || state.projection.cursor !== cursor) {
    return halt("CHECKPOINT_MISMATCH");
  }
  return { state, checkpointId: checkpoint.id, highWater };
}

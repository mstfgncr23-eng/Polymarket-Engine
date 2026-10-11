import { createHash } from "node:crypto";
import { z } from "zod";
import {
  bootstrapGuardianPositionProjection, evaluateGuardianV2,
  guardianPositionBootstrapSchema, guardianProtectionRequestSchema,
  marketSnapshotSchema, projectGuardianBookSignal, sameReversalEpisode,
  type GuardianV2State,
} from "@pm/core";
import {
  advanceGuardianBookChannel, beginGuardianBookGeneration,
  createGuardianBookChannel, readGuardianPairedBook,
} from "./guardian-paired-book-channel.js";

/**
 * G2I FIRST VERTICAL SLICE ONLY. Run via IPC in a separate, explicitly
 * offline child process. This file has NO database, network, wallet,
 * execution, or production Engine imports. The sending fixture/supervisor
 * is responsible for authenticating checkpoint and book provenance.
 * Structural validation here is NOT source authentication.
 */
if (process.env.PM_GUARDIAN_OFFLINE_CHILD !== "1" || !process.send) {
  throw new Error("GUARDIAN_OFFLINE_CHILD_ONLY");
}

const inputSchema = z.strictObject({
  kind: z.literal("OBSERVE"),
  id: z.number().int().positive(),
  bootstrap: guardianPositionBootstrapSchema,
  paired: z.strictObject({
    status: z.literal("READY"),
    generation: z.number().int().positive(),
    ingressSequence: z.number().int().positive(),
    up: marketSnapshotSchema,
    down: marketSnapshotSchema,
  }),
  now: z.string().datetime({ offset: true }),
  maxAgeMs: z.number().int().min(1).max(60_000),
  maxSkewMs: z.number().int().min(0).max(60_000),
});
const states = new Map<string, GuardianV2State>();
const seenBooks = new Map<string, { generation: number; sequence: number; upToken: string; downToken: string }>();
let source: { authorityId: string; experimentId: string; cursor: number; hash: string } | null = null;
let halted = false;

function reply(payload: Record<string, unknown>): void {
  process.send!({ ...payload, pid: process.pid });
}
function halt(id: unknown, reason: string): void {
  halted = true;
  states.clear();
  seenBooks.clear();
  reply({ kind: "RESULT", id: typeof id === "number" ? id : -1, status: "HALTED", reason });
}
function observe(raw: unknown): void {
  const parsed = inputSchema.safeParse(raw);
  const id = parsed.success ? parsed.data.id : (raw as { id?: unknown } | null)?.id;
  if (halted) { reply({ kind: "RESULT", id, status: "HALTED", reason: "HALT_LATCHED" }); return; }
  if (!parsed.success) { halt(id, "INVALID_IPC_INPUT"); return; }
  const { bootstrap, paired, now, maxAgeMs, maxSkewMs } = parsed.data;
  const projected = bootstrapGuardianPositionProjection(bootstrap);
  if (projected.status !== "APPLIED") { halt(id, "INVALID_POSITION_BOOTSTRAP"); return; }
  const previous = source;
  if (previous && (previous.authorityId !== bootstrap.authorityId
    || previous.experimentId !== bootstrap.experimentId
    || bootstrap.cursor < previous.cursor
    || (bootstrap.cursor === previous.cursor && bootstrap.checkpointContentHash !== previous.hash))) {
    halt(id, "SOURCE_AUTHORITY_OR_CURSOR_CHANGED"); return;
  }
  source = { authorityId: bootstrap.authorityId, experimentId: bootstrap.experimentId,
    cursor: bootstrap.cursor, hash: bootstrap.checkpointContentHash };
  const up = paired.up, down = paired.down;
  if (up.outcome !== "UP" || down.outcome !== "DOWN" || up.marketId !== down.marketId
    || up.tokenId === down.tokenId || up.eventId === down.eventId) {
    halt(id, "INVALID_PAIRED_IDENTITY"); return;
  }
  const marketId = up.marketId;
  const earlier = seenBooks.get(marketId);
  if (earlier && (up.tokenId !== earlier.upToken || down.tokenId !== earlier.downToken
    || paired.generation < earlier.generation
    || (paired.generation === earlier.generation && paired.ingressSequence <= earlier.sequence))) {
    halt(id, "STALE_BOOK_OR_TOKEN_ROTATION"); return;
  }
  // Use existing G2H book gates, never trust a READY label on IPC by itself.
  let channel = beginGuardianBookGeneration(createGuardianBookChannel({
    marketId, upTokenId: up.tokenId, downTokenId: down.tokenId,
  }));
  channel = advanceGuardianBookChannel(channel, {
    kind: "FULL_BOOK", generation: channel.generation, ingressSequence: 1,
    synchronized: true, snapshot: up,
  });
  channel = advanceGuardianBookChannel(channel, {
    kind: "FULL_BOOK", generation: channel.generation, ingressSequence: 2,
    synchronized: true, snapshot: down,
  });
  const at = Date.parse(now);
  const book = readGuardianPairedBook(channel, { nowMs: at, maxAgeMs, maxSkewMs });
  if (book.status !== "READY") {
    reply({ kind: "RESULT", id, status: "NO_ACTION", reason: "BOOK_" + book.reason });
    return;
  }
  seenBooks.set(marketId, { generation: paired.generation, sequence: paired.ingressSequence,
    upToken: up.tokenId, downToken: down.tokenId });
  const position = projected.projection.open.get(marketId);
  if (!position) {
    states.delete(marketId);
    reply({ kind: "RESULT", id, status: "NO_ACTION", reason: "NO_OPEN_POSITION" });
    return;
  }
  let state = states.get(marketId) ?? null;
  if (state && !sameReversalEpisode(state, position.episode)) state = null;
  const observation = projectGuardianBookSignal(book.up, book.down, position.episode.heldOutcome);
  if (!observation) { halt(id, "INVALID_BOOK_SIGNAL"); return; }
  const evaluated = evaluateGuardianV2({
    generation: "BAD_REGION_V3", state, episode: position.episode,
    entryPrice: position.entryPrice, now: new Date(now), observation,
    observationGap: !!state?.lastObservationAt && at - Date.parse(state.lastObservationAt) > 15_000,
  });
  if (evaluated.nextState) states.set(marketId, evaluated.nextState);
  if (!evaluated.stateChanged || !evaluated.nextState?.confirmedAt
    || !evaluated.nextState.protectionReason) {
    reply({ kind: "RESULT", id, status: "NO_ACTION", reason: "NO_PROTECTION_TRANSITION",
      events: evaluated.events }); return;
  }
  const next = evaluated.nextState;
  const requestId = "g2i:" + createHash("sha256").update(JSON.stringify([
    bootstrap.authorityId, bootstrap.experimentId, position.episode,
    next.revision, next.confirmedAt,
  ])).digest("hex").slice(0, 32);
  const request = guardianProtectionRequestSchema.parse({
    version: 1, mode: "PAPER", policyGeneration: 3, requestId,
    experimentId: bootstrap.experimentId, episode: position.episode,
    expectedResidualQty: position.residualQty, sourceDurableCursor: bootstrap.cursor,
    requestedKind: "PROTECTIVE_SELL", decisionReason: next.protectionReason,
    evidence: { upEventId: up.eventId, downEventId: down.eventId,
      syncEpoch: paired.generation, observedAt: now },
    confirmedAt: next.confirmedAt, requestedAt: now,
    expiresAt: new Date(at + 5_000).toISOString(),
  });
  // Output only: no order admission, fill, mutation, or economic authority.
  reply({ kind: "RESULT", id, status: "REQUEST", request, events: evaluated.events });
}
process.on("message", raw => {
  try { observe(raw); }
  catch { halt((raw as { id?: unknown } | null)?.id, "EVALUATION_FAILURE"); }
});
process.on("disconnect", () => process.exit(0));
reply({ kind: "READY", status: "OFFLINE_ONLY" });

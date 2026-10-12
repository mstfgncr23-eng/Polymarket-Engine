import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createReadStream } from "node:fs";
import { createInterface } from "node:readline";
import { join } from "node:path";
import { engineEnvSchema } from "@pm/core";
import { canonicalJson, hashCheckpointPayload, hashDurablePayload, isSemanticallyValidCheckpointPayload,
  MemoryEventJournal, type EngineCheckpoint, type DurableTransition } from "@pm/db";
import { EngineService } from "./engine.js";
import { validatePostReplayInvariants, type RecoverableEngineState } from "./recovery.js";
import type { RecoveryArchiveManifest, VerifiedArchiveFile } from "./archive.js";

/** Offline verification only: no start(), socket, feed, database or economic writes.
 * Reuses the actual Engine checkpoint hydration and durable apply path. */
export async function verifyRecoveryDirectory(directory: string) {
  const manifest = JSON.parse(await readFile(join(directory, "manifest.json"), "utf8")) as RecoveryArchiveManifest;
  if (manifest.kind !== "engine_recovery") throw Error("RESTORE_MANIFEST_KIND");
  for (const file of [manifest.checkpoints, manifest.durableTransitions]) {
    const actual = await digestFile(join(directory, file.file));
    if (actual.sha256 !== file.sha256 || actual.bytes !== file.bytes) throw Error("RESTORE_FILE_HASH");
  }
  const checkpoints = JSON.parse(await readFile(join(directory, "checkpoints.json"), "utf8")) as EngineCheckpoint[];
  const retained = checkpoints.filter(c => c.payload.checkpointContentHash === hashCheckpointPayload(c.payload)
    && isSemanticallyValidCheckpointPayload(c.payload) && Number(c.payload.durableCursor) >= manifest.replayFromCursor
    && Number(c.payload.durableCursor) <= manifest.throughCursor).sort((a,b) => b.sequence-a.sequence).slice(0,2);
  if (retained.length !== 2) throw Error("RESTORE_TWO_CHECKPOINTS_REQUIRED");
  const results = [];
  for (const checkpoint of retained) {
    const engine = new EngineService(engineEnvSchema.parse({ TRADING_MODE: "paper", GUARDIAN_SEPARATE_ENABLED: "true" }),
      new MemoryEventJournal(), {error: () => undefined});
    const internal = engine as unknown as { hydrateCheckpointPayload(p:Record<string,unknown>):unknown;
      lastAppliedDurableCursor:number; applyCommitted(row:DurableTransition):boolean; recoverableBag():RecoverableEngineState };
    internal.hydrateCheckpointPayload(checkpoint.payload);
    internal.lastAppliedDurableCursor = Number(checkpoint.payload.durableCursor);
    let rowCount=0, replayed=0;
    const identities = new Set<string>();
    const stream = createInterface({input:createReadStream(join(directory, manifest.durableTransitions.file)),crlfDelay:Infinity});
    for await (const line of stream) {
      const row = JSON.parse(line) as DurableTransition; rowCount++;
      if (row.payloadHash !== hashDurablePayload(row.payload) || identities.has(row.operationKey)) throw Error("RESTORE_OPERATION_HASH");
      identities.add(row.operationKey);
      if (row.sequence > Number(checkpoint.payload.durableCursor)) {
        if (!internal.applyCommitted(row) || internal.applyCommitted(row)) throw Error("RESTORE_DUPLICATE_APPLY");
        replayed++;
      }
    }
    if (rowCount !== manifest.durableTransitions.eventCount) throw Error("RESTORE_EVENT_COUNT");
    const bag = internal.recoverableBag();
    const invariant = validatePostReplayInvariants(bag, internal.lastAppliedDurableCursor, manifest.throughCursor);
    if (!invariant.ok) throw Error("RESTORE_INVARIANT:"+invariant.reasons.join(","));
    const normalized = {ledger:bag.ledger.snapshot(),positions:[...bag.ledger.positions.values()].sort((a,b)=>a.marketId.localeCompare(b.marketId)),
      experiment:bag.experiment,resultAccounting:bag.resultAccounting,
      orders:[...bag.restingOrders].sort(),reservations:[...bag.makerReservations].sort(),
      owners:[...(bag.riskReductionOwners??[])].sort(),failures:[...(bag.riskReductionFailures??[])].sort(),
      settled:[...bag.settledMarkets].sort(),reversal:[...(bag.reversalProtectionStates??[])].sort(),
      trailing:[...(bag.profitTrailingPairStates??[])].sort(),cursor:internal.lastAppliedDurableCursor};
    results.push({checkpointId:checkpoint.id,checkpointCursor:Number(checkpoint.payload.durableCursor),replayed,
      economicStateHash:createHash("sha256").update(canonicalJson(normalized)).digest("hex"),
      cursor:internal.lastAppliedDurableCursor,operationKeys:identities.size});
  }
  if (results[0]!.economicStateHash !== results[1]!.economicStateHash) throw Error("RESTORE_CHECKPOINT_PARITY");
  return {status:"PASSED",kind:manifest.kind,manifest,results,duplicateApplyPrevented:true};
}

async function digestFile(path:string):Promise<Omit<VerifiedArchiveFile,"file">> {
  const hash=createHash("sha256"); let bytes=0;
  for await(const chunk of createReadStream(path)) {hash.update(chunk);bytes+=chunk.length;}
  return {bytes,sha256:hash.digest("hex")};
}

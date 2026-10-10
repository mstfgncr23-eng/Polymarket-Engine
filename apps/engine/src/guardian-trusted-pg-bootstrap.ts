import type { PostgresEventJournal } from "@pm/db";
import {
  bootstrapGuardianOfflineSource,advanceGuardianOfflineSource,
  type GuardianOfflineState,
} from "./guardian-offline-durable-source.js";

/**
 * G2H H1: PostgreSQL reader-only checkpoint + first durable page.
 * Source trust is the independently provisioned canonical DB and SELECT-only
 * identity, never a signer key. No ACK, no orders, no socket, no shadow.
 *
 * H2 must implement durable consumer ACK, safe checkpoint rotation and
 * multi-page recovery before the consumer is accepted for G2I.
 */
export async function bootstrapGuardianTrustedPg(input:{
  source:Pick<PostgresEventJournal,"readGuardianConsistentPage">;
  sourceIdentity:{database:string;schema:string;role:string};
  experimentId:string;authorityId:string;
  pageSize:number;maxBacklogRows:number;
}):Promise<{
  state:GuardianOfflineState;highWater:number;
  sourceIdentity:{database:string;schema:string;role:string};
  caughtUp:boolean;
}> {
  if(!input||!input.authorityId||input.authorityId.length>256
    ||!input.experimentId||input.experimentId.length>128
    ||!Number.isSafeInteger(input.pageSize)||input.pageSize<1||input.pageSize>512
    ||!Number.isSafeInteger(input.maxBacklogRows)
    ||input.maxBacklogRows<input.pageSize||input.maxBacklogRows>100_000)
    throw Error("G2H_PG_INVALID_BOOTSTRAP");
  const page=await input.source.readGuardianConsistentPage({
    ...input.sourceIdentity,experimentId:input.experimentId,
    afterSequence:null,limit:input.pageSize,
  });
  const chosen=page.checkpoints[0];
  if(!chosen?.verified||!chosen.contentHash
    ||chosen.experimentId!==input.experimentId
    ||Number(chosen.payload.durableCursor)!==page.fromCursor)
    throw Error("G2H_PG_CHECKPOINT_NOT_PINNED");
  if(page.highWater-page.fromCursor>input.maxBacklogRows)
    throw Error("G2H_PG_BACKLOG_LIMIT");
  let state=bootstrapGuardianOfflineSource({
    payload:chosen.payload,
    externallyVerifiedContentHash:chosen.contentHash,
    authorityId:input.authorityId,
  });
  for(const row of page.rows)state=advanceGuardianOfflineSource(state,row);
  return {
    state,highWater:page.highWater,sourceIdentity:page.source,
    caughtUp:state.projection.cursor===page.highWater,
  };
}

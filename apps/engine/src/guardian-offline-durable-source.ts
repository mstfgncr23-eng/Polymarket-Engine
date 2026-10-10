import {
  bootstrapGuardianPositionProjection, applyGuardianPositionFrame,
  d, ledgerStateSchema, positionSchema, experimentManifestSchema, restingPaperOrderSchema,
  reversalProtectionStateSchema, profitTrailingPairStateSchema, PaperLedger,
  type GuardianOpenResidual, type GuardianPositionProjection, type Position,
} from "@pm/core";
import {
  CHECKPOINT_SCHEMA_VERSION, hashCheckpointPayload, hashDurablePayload,
  isSemanticallyValidCheckpointPayload, type DurableTransition,
} from "@pm/db";
import {
  applyDurableTransition, cloneRecoverableState, parseCheckpointDurableCursor,
  riskReductionOwnershipSchema, riskReductionExecutionFailureSchema, settlementLookupSchema,
  validatePostReplayInvariants,
  type RecoverableEngineState,
} from "./recovery.js";
import { resultAccountingSchema } from "./trading-results.js";

/**
 * G2F ONLY: deterministic read-only conversion of trusted, already fetched
 * checkpoint + contiguous authoritative rows. Does not open a DB, socket,
 * process, timer, order, subscription, or run a shadow Guardian.
 */
export class GuardianOfflineSourceRejected extends Error {
  constructor(readonly reason: string) {
    super(`GUARDIAN_OFFLINE_SOURCE_REJECTED:${reason}`);
  }
}
function fail(reason: string): never { throw new GuardianOfflineSourceRejected(reason); }
function episode(position: Position): GuardianOpenResidual | null {
  if (!position.unpairedOutcome || !d(position.unpairedQuantity).gt(0)) return null;
  const price = position.unpairedOutcome === "UP" ? position.averageUpCost : position.averageDownCost;
  return {
    episode: {
      marketId: position.marketId, positionId: position.id,
      unhedgedStartedAt: position.unhedgedStartedAt ?? position.openedAt,
      heldOutcome: position.unpairedOutcome,
    },
    residualQty: position.unpairedQuantity, entryPrice: price,
  };
}
function visible(state: RecoverableEngineState): GuardianOpenResidual[] {
  return [...state.ledger.positions.values()].flatMap(p => {
    const row=episode(p);
    return row ? [row] : [];
  });
}
function sourceShape(payload: Record<string, unknown>): RecoverableEngineState {
  const ledger = ledgerStateSchema.parse(payload.ledger);
  const positions = (payload.positions as unknown[]).map(item => positionSchema.parse(item));
  const stateLedger = new PaperLedger(ledger.startingBalance, new Date(ledger.updatedAt));
  stateLedger.restore(ledger,positions);
  const resting = (payload.restingOrders as unknown[]).map(row=>restingPaperOrderSchema.parse(row));
  const makerReservations = payload.makerReservations as Record<string,string>;
  const owners = (payload.riskReductionOwners as unknown[]).map(row=>riskReductionOwnershipSchema.parse(row));
  const failures = (payload.riskReductionFailures as unknown[]).map(row=>riskReductionExecutionFailureSchema.parse(row));
  const keyed = <T>(rows:T[], getKey:(value:T)=>string):Map<string,T> => {
    const map = new Map<string,T>();
    for(const row of rows) {
      const key=getKey(row);
      if(map.has(key)) fail("DUPLICATE_ECONOMIC_OWNER");
      map.set(key,row);
    }
    return map;
  };
  const reversalStates=(payload.reversalProtectionStates as unknown[])
    .map(row=>reversalProtectionStateSchema.parse(row));
  const trailingStates=(payload.profitTrailingPairStates as unknown[])
    .map(row=>profitTrailingPairStateSchema.parse(row));
  const settlementLookups=(payload.settlementLookups as unknown[])
    .map(row=>settlementLookupSchema.parse(row));
  const restingOrders=keyed(resting,row=>row.order.id);
  const reversalMap=keyed(reversalStates,row=>row.marketId);
  const trailingMap=keyed(trailingStates,row=>row.marketId);
  const lookupMap=keyed(settlementLookups,row=>row.marketId);
  for(const lookup of settlementLookups){
    const pos=stateLedger.positions.get(lookup.marketId);
    if(!pos || (!d(pos.upQuantity).gt(0) && !d(pos.downQuantity).gt(0))
      || (payload.settledMarkets as string[]).includes(lookup.marketId)){
      fail("SETTLEMENT_LOOKUP_WITHOUT_INVENTORY");
    }
  }
  const ownerMap=keyed(owners,row=>row.marketId);
  const failureMap=keyed(failures,row=>row.ownership.marketId);
  for(const owner of owners) {
    const position=stateLedger.positions.get(owner.marketId);
    if(!position?.unpairedOutcome || (position.unhedgedStartedAt??position.openedAt)!==owner.unhedgedStartedAt
      || !d(position.unpairedQuantity).eq(owner.admittedResidualQuantity)
      || (payload.settledMarkets as string[]).includes(owner.marketId)) fail("OWNER_EPISODE_MISMATCH");
  }
  for(const failure of failures) {
    const owner=failure.ownership;
    const position=stateLedger.positions.get(owner.marketId);
    if(!position?.unpairedOutcome || (position.unhedgedStartedAt??position.openedAt)!==owner.unhedgedStartedAt
      || !d(position.unpairedQuantity).eq(owner.admittedResidualQuantity)
      || ownerMap.has(owner.marketId) || (payload.settledMarkets as string[]).includes(owner.marketId)) {
      fail("FAILURE_EPISODE_MISMATCH");
    }
  }
  return {
    ledger:stateLedger,
    experiment:experimentManifestSchema.parse(payload.experiment),
    resultAccounting:resultAccountingSchema.parse(payload.resultAccounting),
    restingOrders,
    makerReservations:new Map(Object.entries(makerReservations)),
    riskReductionOwners:ownerMap,
    riskReductionFailures:failureMap,
    reversalProtectionStates:reversalMap,
    profitTrailingPairStates:trailingMap,
    settlementLookups:lookupMap,
    settledMarkets:new Set(payload.settledMarkets as string[]),
    lastInfrastructureAccrualAt:Number(payload.lastInfrastructureAccrualAt ?? 0),
  };
}
export interface GuardianOfflineState {
  readonly projection: GuardianPositionProjection;
  readonly recovery: RecoverableEngineState;
  readonly seenOperationKeys: ReadonlySet<string>;
}
export function bootstrapGuardianOfflineSource(input: {
  /** Read via a trusted read-only checkpoint source; not a self-signed claim. */
  payload: Record<string, unknown>;
  externallyVerifiedContentHash: string;
  authorityId: string;
}): GuardianOfflineState {
  const {payload,externallyVerifiedContentHash,authorityId}=input;
  // Unsupported older/partial checkpoints MUST be rejected, never assumed.
  if (payload.schemaVersion!==CHECKPOINT_SCHEMA_VERSION
    || !Number.isSafeInteger(parseCheckpointDurableCursor(payload))
    || !isSemanticallyValidCheckpointPayload(payload)
    || !Array.isArray(payload.positions) || !Array.isArray(payload.settledMarkets)
    || !Array.isArray(payload.restingOrders) || !Array.isArray(payload.activeTriggers)
    || !Array.isArray(payload.grokSignals) || !payload.resultAccounting) fail("CHECKPOINT_UNSUPPORTED");
  // All hydrated state is OFFLINE and read-only. Unknown telemetry and source
  // integrity fields remain forbidden until a complete recovery proof exists.
  if (!payload.makerReservations || typeof payload.makerReservations!=="object"
    || Array.isArray(payload.makerReservations)
    || !Array.isArray(payload.riskReductionOwners) || !Array.isArray(payload.riskReductionFailures)) fail("CHECKPOINT_UNSUPPORTED");
  if (!Array.isArray(payload.reversalProtectionStates) || !Array.isArray(payload.profitTrailingPairStates)
    || !Array.isArray(payload.settlementLookups)) fail("CHECKPOINT_UNSUPPORTED");
  if ((Array.isArray(payload.nakedDirectionalDiagnostics) && payload.nakedDirectionalDiagnostics.length)
    || (Array.isArray(payload.reportSourceIntegrity) && payload.reportSourceIntegrity.length)
    || payload.reportQuarantine != null) fail("CHECKPOINT_HAS_UNMODELED_AUTHORITY");
  if (hashCheckpointPayload(payload)!==externallyVerifiedContentHash) fail("CHECKPOINT_HASH_MISMATCH");
  try {
    const recovery=sourceShape(payload);
    if (!recovery.experiment || !recovery.experiment.startedAt) fail("EXPERIMENT_UNVERIFIED");
    const invariants=validatePostReplayInvariants(recovery,parseCheckpointDurableCursor(payload)!,parseCheckpointDurableCursor(payload)!);
    if(!invariants.ok) fail("CHECKPOINT_INVARIANT_VIOLATION:"+invariants.reasons.join(","));
    const result=bootstrapGuardianPositionProjection({
      version:1,mode:"PAPER",authorityId,experimentId:recovery.experiment.id,
      checkpointContentHash:externallyVerifiedContentHash,
      cursor:parseCheckpointDurableCursor(payload),open:visible(recovery),
      settledMarkets:[...recovery.settledMarkets],
    });
    if (result.status!=="APPLIED") fail("BOOTSTRAP_"+result.reason);
    return {projection:result.projection,recovery,seenOperationKeys:new Set()};
  } catch(error) {
    if (error instanceof GuardianOfflineSourceRejected) throw error;
    return fail("CHECKPOINT_RECOVERY_INVALID");
  }
}
export function advanceGuardianOfflineSource(current: GuardianOfflineState, row: DurableTransition): GuardianOfflineState {
  const nextCursor=current.projection.cursor+1;
  if (row.sequence!==nextCursor) fail("DURABLE_CURSOR_GAP");
  if (current.seenOperationKeys.has(row.operationKey)) fail("DUPLICATE_OPERATION_KEY");
  if (row.payloadHash!==hashDurablePayload(row.payload) || Object.keys(row.payload).length===0) {
    fail("DURABLE_PAYLOAD_UNVERIFIED");
  }
  const previous=cloneRecoverableState(current.recovery);
  const next=cloneRecoverableState(current.recovery);
  try { applyDurableTransition(next,row); }
  catch { return fail("DURABLE_REPLAY_REJECTED"); }
  if (next.experiment?.id!==current.projection.experimentId) fail("EXPERIMENT_CHANGED");
  const invariants=validatePostReplayInvariants(next,row.sequence,row.sequence);
  if(!invariants.ok) fail("REPLAY_INVARIANT_VIOLATION:"+invariants.reasons.join(","));
  const base = {
    version:1 as const, mode:"PAPER" as const,
    authorityId:current.projection.authorityId,
    experimentId:current.projection.experimentId,
    previousCursor:current.projection.cursor,cursor:row.sequence,
    sourceOperationKey:row.operationKey,occurredAt:row.occurredAt,
  };
  let frame: unknown;
  if (row.type==="PAPER_FILL") {
    const id=(row.payload.fill as {marketId?:unknown}|undefined)?.marketId;
    if (typeof id!=="string" || id.length===0) fail("MISSING_FILL_MARKET");
    const prior=previous.ledger.positions.get(id);
    const following=next.ledger.positions.get(id);
    frame={...base,kind:"FILL_POSITION",sourceType:"PAPER_FILL",marketId:id,
      previous:prior?episode(prior)?.episode??null:null,next:following?episode(following):null};
  } else if (row.type==="POSITION_SETTLED") {
    const marketId=row.payload.marketId;
    if (typeof marketId!=="string" || !previous.ledger.positions.has(marketId)) fail("SETTLEMENT_SOURCE_MISSING");
    // The existing recovery reducer settles inventory, while EngineService.afterApply
    // marks settledMarkets. Derive that terminal bit from the committed settlement
    // here; NEVER infer it from an ordinary journal event or provider signal.
    next.settledMarkets.add(marketId);
    frame={...base,kind:"SETTLED",sourceType:"POSITION_SETTLED",marketId};
  } else {
    frame={...base,kind:"NO_POSITION_CHANGE",sourceType:row.type};
  }
  const result=applyGuardianPositionFrame(current.projection,frame);
  if (result.status!=="APPLIED") fail(result.status==="REJECTED" ? `FRAME_${result.reason}` : "UNEXPECTED_FRAME_REPLAY");
  // Independent oracle: recovered ledger residual must match read-only model
  // after EVERY authoritative transition, not only upon an economic fill.
  const expected=visible(next).sort((a,b)=>a.episode.marketId.localeCompare(b.episode.marketId));
  const got=[...result.projection.open.values()].sort((a,b)=>a.episode.marketId.localeCompare(b.episode.marketId));
  if (JSON.stringify(expected)!==JSON.stringify(got)
    || JSON.stringify([...next.settledMarkets].sort())!==JSON.stringify([...result.projection.settledMarkets].sort())) {
    fail("LEDGER_PROJECTION_DIVERGENCE");
  }
  return {projection:result.projection,recovery:next,
    seenOperationKeys:new Set([...current.seenOperationKeys,row.operationKey])};
}

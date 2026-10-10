import { createHash } from "node:crypto";
import {
  type GuardianConsumerAckStore, type GuardianConsumerReceipt,
} from "./guardian-consumer-ack-store.js";
import {
  pullGuardianDurablePage, type GuardianDurableReadSource,
} from "./guardian-readonly-durable-feed.js";
import {
  bootstrapGuardianSignedCheckpoint, verifyGuardianWriterManifest,
  type GuardianWriterManifest, type GuardianWriterTrust,
} from "./guardian-signed-writer-manifest.js";
import type { GuardianCheckpointReadSource } from "./guardian-readonly-checkpoint.js";
import type { GuardianOfflineState } from "./guardian-offline-durable-source.js";
import {
  verifyGuardianSignedPage, type GuardianSignedPageSource,
} from "./guardian-signed-durable-page.js";

export type GuardianSessionHaltReason =
  | "SOURCE_AUTHORITY_CHANGED" | "ACK_AHEAD_OF_SOURCE" | "ACK_STATE_DIVERGENCE"
  | "STALE_CHECKPOINT_REPLAY" | "MANIFEST_CHECKPOINT_CHANGED"
  | "MANIFEST_EPOCH_CHANGED" | "MANIFEST_WATERMARK_REGRESSION"
  | "SIGNED_HIGHWATER_NOT_VISIBLE" | "SOURCE_REPLAY_FAILED" | "ACK_COMMIT_FAILED";
export class GuardianReadonlySessionHalted extends Error {
  constructor(readonly reason: GuardianSessionHaltReason) {
    super(`GUARDIAN_READONLY_SESSION_HALTED:${reason}`);
  }
}
const halt=(reason:GuardianSessionHaltReason):never=>{throw new GuardianReadonlySessionHalted(reason);};
export type GuardianG2HJournal=GuardianCheckpointReadSource
  & Pick<GuardianDurableReadSource,"latestDurableSequence">
  & GuardianSignedPageSource;
/**
 * Authenticate each actual durable row page against the operator-trusted
 * Ed25519 producer and the exact signed checkpoint/epoch before replay.
 */
const certifiedRead=async(
  source:GuardianG2HJournal,manifest:GuardianWriterManifest,
  trust:GuardianWriterTrust,nowMs:number,cursor:number,limit:number,
)=>{
  const signed=await source.readSignedDurablePageAfter(cursor,limit,manifest);
  return verifyGuardianSignedPage({
    rows:signed?.rows,certificate:signed?.certificate,
    manifest,trust,nowMs,fromCursor:cursor,maximumRows:limit,
  });
};
const sortedRows=(map:ReadonlyMap<string,unknown>|undefined)=>
  [...(map??new Map())].sort(([a],[b])=>a.localeCompare(b));
/**
 * Consumer ACK must bind ALL recovered economic decision authority, not only
 * the open-position projection. A changed maker owner or protection phase at
 * an unchanged cursor is a divergent recovery and MUST halt.
 */
export const hashGuardianRecoverableState=(state:GuardianOfflineState):string=>{
  const projection=state.projection,recovery=state.recovery;
  const content=JSON.stringify({
    authorityId:projection.authorityId,experimentId:projection.experimentId,
    checkpointHash:projection.checkpointHash,cursor:projection.cursor,
    open:sortedRows(projection.open),
    settledMarkets:[...projection.settledMarkets.values()].sort(),
    ledger:recovery.ledger.snapshot(),
    positions:[...recovery.ledger.positions.entries()].sort(([a],[b])=>a.localeCompare(b)),
    experiment:recovery.experiment,resultAccounting:recovery.resultAccounting,
    restingOrders:sortedRows(recovery.restingOrders),
    makerReservations:sortedRows(recovery.makerReservations),
    riskReductionOwners:sortedRows(recovery.riskReductionOwners),
    riskReductionFailures:sortedRows(recovery.riskReductionFailures),
    reversalProtectionStates:sortedRows(recovery.reversalProtectionStates),
    profitTrailingPairStates:sortedRows(recovery.profitTrailingPairStates),
    settlementLookups:sortedRows(recovery.settlementLookups),
    nakedDirectionalDiagnostics:sortedRows(recovery.nakedDirectionalDiagnostics),
    settledRecovery:[...recovery.settledMarkets].sort(),
    lastInfrastructureAccrualAt:recovery.lastInfrastructureAccrualAt,
    seenOperationKeys:[...state.seenOperationKeys].sort(),
  });
  return createHash("sha256").update("pm-guardian-consumer-economics-v2\n"+content).digest("hex");
};
const receiptFor=(state:GuardianOfflineState,manifest:GuardianWriterManifest):GuardianConsumerReceipt=>({
  version:1,authorityId:manifest.authorityId,experimentId:manifest.experimentId,
  writerEpoch:manifest.writerEpoch,manifestSequence:manifest.manifestSequence,
  checkpointId:manifest.checkpointId,checkpointHash:manifest.checkpointHash,
  cursor:state.projection.cursor,projectionHash:hashGuardianRecoverableState(state),
});

/**
 * G2H closed, single-reader, no-queue and no-economic-output supervisor.
 * The signed high-water limits what rows can be observed in this epoch.
 * It cannot approve any hedge, SELL, or risk action.
 */
export class GuardianReadonlySession {
  private stopped: string|null=null;
  private inFlight=false;
  private observedSourceHighWater:number|null=null;
  private lastVerifiedPollAtMs:number|null=null;
  private constructor(
    private readonly source:GuardianG2HJournal,
    private readonly ackStore:GuardianConsumerAckStore,
    private readonly trust:GuardianWriterTrust,
    private readonly pageSize:number,
    private readonly maxBacklogRows:number,
    private current:GuardianOfflineState,
    private signed:GuardianWriterManifest,
    private receipt:GuardianConsumerReceipt,
  ) {}
  get status(): "READY"|"HALTED" {return this.stopped===null?"READY":"HALTED";}
  get cursor():number {return this.current.projection.cursor;}
  get lastHalt():string|null {return this.stopped;}
  /** NO ledger or mutable projection escapes this class. */
  get openPositionCount():number {return this.current.projection.open.size;}

  /**
   * Complete, signed-highwater-aligned observational handoff for a future
   * single Guardian process. Never exposes live mutable Map/Set references
   * or any method that can submit, cancel or reserve economic orders.
   */
  readVerifiedView(nowMs:number) {
    if (this.stopped!==null || this.inFlight || !Number.isSafeInteger(nowMs)
      || nowMs<this.signed.issuedAtMs-5_000 || nowMs>=this.signed.expiresAtMs
      || this.lastVerifiedPollAtMs===null || nowMs<this.lastVerifiedPollAtMs
      || nowMs-this.lastVerifiedPollAtMs>1_000
      || this.observedSourceHighWater!==this.signed.durableHighWater
      || this.current.projection.cursor!==this.signed.durableHighWater)
      return halt("SOURCE_REPLAY_FAILED");
    const state=this.current,p=state.projection,r=state.recovery;
    return {
      version:1 as const,mode:"PAPER" as const,authorityId:p.authorityId,
      experimentId:p.experimentId,writerEpoch:this.signed.writerEpoch,
      manifestSequence:this.signed.manifestSequence,
      checkpointId:this.signed.checkpointId,
      cursor:p.cursor,highWater:this.signed.durableHighWater,
      economicStateHash:hashGuardianRecoverableState(state),
      open:structuredClone([...p.open.values()].sort((a,b)=>a.episode.marketId.localeCompare(b.episode.marketId))),
      settledMarkets:[...p.settledMarkets].sort(),
      restingOrders:structuredClone([...r.restingOrders.values()]),
      makerReservations:structuredClone([...r.makerReservations.entries()]),
      riskReductionOwners:structuredClone([...(r.riskReductionOwners??new Map()).values()]),
      riskReductionFailures:structuredClone([...(r.riskReductionFailures??new Map()).values()]),
      reversalProtectionStates:structuredClone([...(r.reversalProtectionStates??new Map()).values()]),
      profitTrailingPairStates:structuredClone([...(r.profitTrailingPairStates??new Map()).values()]),
      settlementLookups:structuredClone([...(r.settlementLookups??new Map()).values()]),
    };
  }

  static async open(input:{
    source:GuardianG2HJournal;ackStore:GuardianConsumerAckStore;
    trust:GuardianWriterTrust;manifest:unknown;
    pageSize:number;maxBacklogRows:number;
  }):Promise<GuardianReadonlySession> {
    const {source,ackStore,pageSize,maxBacklogRows}=input;
    if (!Number.isSafeInteger(pageSize) || pageSize<1 || pageSize>512
      || !Number.isSafeInteger(maxBacklogRows) || maxBacklogRows<pageSize || maxBacklogRows>100_000)
      return halt("SOURCE_REPLAY_FAILED");
    let prior:GuardianConsumerReceipt|null;
    try {prior=await ackStore.load();}
    catch {return halt("ACK_COMMIT_FAILED");}
    const minimumWriterEpoch=Math.max(input.trust.minimumWriterEpoch,prior?.writerEpoch??1);
    const verified=await bootstrapGuardianSignedCheckpoint({
      source,manifest:input.manifest,
      trust:{...input.trust,minimumWriterEpoch,
        minimumManifestSequence:prior && prior.writerEpoch===minimumWriterEpoch
          ? Math.max(input.trust.minimumManifestSequence,prior.manifestSequence-1)
          : input.trust.minimumManifestSequence},
      maxBacklogRows,
    });
    const {manifest}=verified;
    if (prior && (prior.authorityId!==manifest.authorityId || prior.experimentId!==manifest.experimentId))
      return halt("SOURCE_AUTHORITY_CHANGED");
    if (prior && prior.cursor>manifest.durableHighWater) return halt("ACK_AHEAD_OF_SOURCE");
    let state=verified.state;
    // Replay every durable row between signed checkpoint and previous ACK.
    // An ACK is NEVER a substitute for real recovered ledger state.
    if (prior && state.projection.cursor<prior.cursor) {
      for (let batches=0;state.projection.cursor<prior.cursor;batches++) {
        if (batches>maxBacklogRows) return halt("STALE_CHECKPOINT_REPLAY");
        try {
          const result=await pullGuardianDurablePage({
            source:{
              latestDurableSequence:async()=>prior.cursor,
              readDurableTransitionsAfter:(cursor,count)=>certifiedRead(
                source,manifest,input.trust,input.trust.nowMs,cursor,count),
            },
            state,pageSize,maxBacklogRows,
          });
          if (result.readCount===0) return halt("STALE_CHECKPOINT_REPLAY");
          state=result.state;
        } catch {return halt("SOURCE_REPLAY_FAILED");}
      }
      if (hashGuardianRecoverableState(state)!==prior.projectionHash) return halt("ACK_STATE_DIVERGENCE");
    }
    if (prior && state.projection.cursor===prior.cursor
      && hashGuardianRecoverableState(state)!==prior.projectionHash) {
      // A newer manifest sequence is not permission to rewrite history
      // already ACKed by an independently stored consumer receipt.
      return halt("ACK_STATE_DIVERGENCE");
    }
    if (prior && state.projection.cursor>prior.cursor) {
      // Never skip the previously ACKed cursor via a newer checkpoint.
      // Reconcile against an earlier independently pinned checkpoint first.
      return halt("STALE_CHECKPOINT_REPLAY");
    }
    const next=receiptFor(state,manifest);
    try {
      if (!prior || JSON.stringify(prior)!==JSON.stringify(next)) await ackStore.commit(prior,next);
    } catch {return halt("ACK_COMMIT_FAILED");}
    return new GuardianReadonlySession(source,ackStore,input.trust,pageSize,maxBacklogRows,state,manifest,next);
  }

  /**
   * Reauthenticate every polling cycle. No signed producer heartbeat means
   * eventual expiry and a latched HALT. A different issuer epoch or checkpoint
   * mandates a completely new operator-attested bootstrap.
   */
  async poll(manifestRaw:unknown,nowMs:number):Promise<{cursor:number;highWater:number;caughtUp:boolean}> {
    if (this.stopped!==null) return halt("SOURCE_REPLAY_FAILED");
    if (this.inFlight) {
      this.stopped="CONCURRENT_POLL";
      return halt("SOURCE_REPLAY_FAILED");
    }
    this.inFlight=true;
    try {
      const manifest=verifyGuardianWriterManifest(manifestRaw,{
        ...this.trust,nowMs,minimumWriterEpoch:this.signed.writerEpoch,
        minimumManifestSequence:this.signed.manifestSequence-1,
      });
      if (manifest.writerEpoch!==this.signed.writerEpoch) return halt("MANIFEST_EPOCH_CHANGED");
      if (manifest.checkpointId!==this.signed.checkpointId
        || manifest.checkpointHash!==this.signed.checkpointHash
        || manifest.experimentId!==this.signed.experimentId)
        return halt("MANIFEST_CHECKPOINT_CHANGED");
      if (manifest.manifestSequence<this.signed.manifestSequence
        || manifest.durableHighWater<this.current.projection.cursor
        || manifest.durableHighWater<this.signed.durableHighWater)
        return halt("MANIFEST_WATERMARK_REGRESSION");
      // A signed high-water is an upper authorization bound, not a promise
      // that every later row from a mutable DB is authenticated.
      const visible=await this.source.latestDurableSequence();
      if (!Number.isSafeInteger(visible) || visible<manifest.durableHighWater)
        return halt("SIGNED_HIGHWATER_NOT_VISIBLE");
      const result=await pullGuardianDurablePage({
        source:{
          latestDurableSequence:async()=>manifest.durableHighWater,
          readDurableTransitionsAfter:(cursor,count)=>certifiedRead(
            this.source,manifest,this.trust,nowMs,cursor,count),
        },
        state:this.current,pageSize:this.pageSize,maxBacklogRows:this.maxBacklogRows,
      });
      if (this.stopped!==null) return halt("SOURCE_REPLAY_FAILED");
      const next=receiptFor(result.state,manifest);
      if (JSON.stringify(this.receipt)!==JSON.stringify(next))
        await this.ackStore.commit(this.receipt,next);
      this.current=result.state;
      this.receipt=next;
      this.signed=manifest;
      // An issuer may sign highwater H while PostgreSQL already exposes H+N.
      // This is useful bounded replay progress, NOT a current decision view.
      this.observedSourceHighWater=visible;
      this.lastVerifiedPollAtMs=nowMs;
      return {cursor:this.current.projection.cursor,highWater:manifest.durableHighWater,
        caughtUp:this.current.projection.cursor===manifest.durableHighWater
          && visible===manifest.durableHighWater};
    } catch(error) {
      this.stopped=error instanceof Error?error.message:"UNKNOWN_SOURCE_OR_ACK_COMMIT";
      throw error;
    } finally {
      this.inFlight=false;
    }
  }
}

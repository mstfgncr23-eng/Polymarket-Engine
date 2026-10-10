import type {PostgresEventJournal, GuardianConsistentPgPage} from "@pm/db";
import {bootstrapGuardianOfflineSource,advanceGuardianOfflineSource,
  type GuardianOfflineState} from "./guardian-offline-durable-source.js";
import {hashGuardianRecoverableState} from "./guardian-readonly-session.js";
import {type GuardianPgAckStore,type GuardianPgReceipt} from "./guardian-pg-ack-store.js";

type Scope=Readonly<{database:string;schema:string;role:string}>;
export type GuardianPgPageSource=Pick<PostgresEventJournal,"readGuardianConsistentPage">;
const fail=(reason:string):never=>{throw Error("G2H_PG_SESSION_"+reason);};
const sameScope=(a:Scope,b:Scope)=>
  a.database===b.database&&a.schema===b.schema&&a.role===b.role;
const equalCheckpoint=(page:GuardianConsistentPgPage,chosen:{
  id:string;contentHash:string;cursor:number
})=>page.checkpoints.some(cp=>cp.id===chosen.id
  &&cp.contentHash===chosen.contentHash
  &&Number(cp.payload.durableCursor)===chosen.cursor
  &&cp.verified);
const mkReceipt=(s:GuardianOfflineState,scope:Scope,cp:{
  id:string;hash:string
},observedHighWater:number):GuardianPgReceipt=>({
  version:2,source:{...scope},authorityId:s.projection.authorityId,
  experimentId:s.projection.experimentId,checkpointId:cp.id,
  checkpointHash:cp.hash,cursor:s.projection.cursor,observedHighWater,
  economicHash:hashGuardianRecoverableState(s),
});
/**
 * G2H H2 closed local consumer. Exactly one PostgreSQL read authority,
 * NO signatures and NO economic submission. Every new page is its own
 * committed snapshot; checkpoint identity and global cursor fence it.
 *
 * H2's checkpoint-rotation/rebase beyond the replay key budget still
 * needs separate proof. No automatic operator-less adoption of a new CP.
 */
export class GuardianTrustedPgSession {
  private stopped:string|null=null;
  private active=false;
  private readHighWater:number;
  private verifiedAtMs:number|null=null;
  private constructor(
    private readonly source:GuardianPgPageSource,
    private readonly ack:GuardianPgAckStore,
    private readonly scope:Scope,
    private readonly experimentId:string,
    private readonly pageSize:number,
    private readonly maxBacklogRows:number,
    private readonly checkpoint:{id:string;hash:string;cursor:number},
    private state:GuardianOfflineState,
    private receipt:GuardianPgReceipt,
    highWater:number,
  ){this.readHighWater=highWater;}
  get status():"READY"|"HALTED"{return this.stopped===null?"READY":"HALTED";}
  get haltReason():string|null{return this.stopped;}
  get cursor():number{return this.state.projection.cursor;}
  get highWater():number{return this.readHighWater;}
  get caughtUp():boolean{return this.cursor===this.readHighWater&&this.stopped===null;}
  private stop(reason:string):never{this.stopped=reason;return fail(reason);}
  private async page(afterSequence:number|null,limit=this.pageSize){
    const data=await this.source.readGuardianConsistentPage({
      ...this.scope,experimentId:this.experimentId,afterSequence,limit,
    });
    if(!sameScope(data.source,this.scope))return fail("SOURCE_IDENTITY_CHANGED");
    return data;
  }
  private verify(page:GuardianConsistentPgPage):void {
    if(!equalCheckpoint(page,{id:this.checkpoint.id,
      contentHash:this.checkpoint.hash,cursor:this.checkpoint.cursor}))
      return fail("PINNED_CHECKPOINT_DISAPPEARED_OR_CHANGED");
    if(page.highWater<this.state.projection.cursor||page.highWater<this.readHighWater)
      return fail("SOURCE_CURSOR_REGRESSION");
  }
  static async open(input:{
    source:GuardianPgPageSource;ack:GuardianPgAckStore;
    scope:Scope;experimentId:string;authorityId:string;
    pageSize:number;maxBacklogRows:number;
  }):Promise<GuardianTrustedPgSession>{
    if(!input||!input.source||!input.ack||!input.scope
      ||!input.authorityId||input.authorityId.length>256
      ||!input.experimentId||input.experimentId.length>128
      ||!Number.isSafeInteger(input.pageSize)||input.pageSize<1||input.pageSize>512
      ||!Number.isSafeInteger(input.maxBacklogRows)
      ||input.maxBacklogRows<input.pageSize||input.maxBacklogRows>100_000)
      return fail("INVALID_CONFIG");
    const old=await input.ack.load(); // v1/unknown receipt is a HARD HALT, not dropped.
    if(old&&(old.authorityId!==input.authorityId
      ||old.experimentId!==input.experimentId
      ||!sameScope(old.source,input.scope)))return fail("RECEIPT_AUTHORITY_MISMATCH");
    const first=await input.source.readGuardianConsistentPage({
      ...input.scope,experimentId:input.experimentId,
      afterSequence:null,limit:input.pageSize,
    });
    if(!sameScope(first.source,input.scope))return fail("SOURCE_IDENTITY_CHANGED");
    if(old&&first.highWater<old.observedHighWater)
      return fail("OBSERVED_SOURCE_HIGHWATER_REGRESSION");
    const chosen=old
      ?first.checkpoints.find(cp=>cp.id===old.checkpointId
        &&cp.contentHash===old.checkpointHash)
      :first.checkpoints[0];
    if(!chosen?.verified||!chosen.contentHash
      ||chosen.experimentId!==input.experimentId)
      return fail("CHECKPOINT_NOT_RECOVERABLE");
    const cpCursor=Number(chosen.payload.durableCursor);
    if(!Number.isSafeInteger(cpCursor)||cpCursor<0||cpCursor>first.highWater
      ||(old&&cpCursor>old.cursor))
      return fail("CHECKPOINT_CURSOR_NOT_RECOVERABLE");
    if(first.highWater-cpCursor>input.maxBacklogRows)
      return fail("BACKLOG_EXCEEDED");
    let state=bootstrapGuardianOfflineSource({payload:chosen.payload,
      externallyVerifiedContentHash:chosen.contentHash,authorityId:input.authorityId});
    const anchor={id:chosen.id,hash:chosen.contentHash,cursor:cpCursor};
    // The prior local ACK is NEVER enough to restore economics. Replay each
    // row again from the pinned checkpoint and prove exact state at old cursor.
    if(old){
      if(old.cursor>first.highWater)return fail("RECEIPT_AHEAD_OF_SOURCE");
      while(state.projection.cursor<old.cursor){
        const page=await input.source.readGuardianConsistentPage({
          ...input.scope,experimentId:input.experimentId,
          afterSequence:state.projection.cursor,
          limit:Math.min(input.pageSize,old.cursor-state.projection.cursor),
        });
        if(!sameScope(page.source,input.scope)
          ||!equalCheckpoint(page,{...anchor,contentHash:anchor.hash})
          ||page.highWater<old.cursor||!page.rows.length)
          return fail("REPLAY_ANCHOR_OR_SOURCE_MISSING");
        for(const row of page.rows){
          if(row.sequence>old.cursor)return fail("REPLAY_PAST_RECEIPT");
          state=advanceGuardianOfflineSource(state,row);
        }
      }
      if(hashGuardianRecoverableState(state)!==old.economicHash)
        return fail("RECEIPT_ECONOMIC_DIVERGENCE");
    }
    const receipt=old??mkReceipt(state,input.scope,anchor,first.highWater);
    if(!old)await input.ack.commit(null,receipt);
    return new GuardianTrustedPgSession(input.source,input.ack,input.scope,
      input.experimentId,input.pageSize,input.maxBacklogRows,
      anchor,state,receipt,first.highWater);
  }
  /** One bounded page per call; committed observation ACK after full replay. */
  async poll(nowMs:number):Promise<{cursor:number;highWater:number;caughtUp:boolean}> {
    if(this.stopped!==null)return fail("HALTED");
    if(this.active)return this.stop("CONCURRENT_POLL");
    this.active=true;
    try{
      if(!Number.isSafeInteger(nowMs)||nowMs<0)return fail("INVALID_CLOCK");
      const page=await this.page(this.state.projection.cursor);
      this.verify(page);
      if(this.stopped!==null)return fail("HALTED_AFTER_CONCURRENT_POLL");
      if(page.highWater-this.state.projection.cursor>this.maxBacklogRows)
        return fail("BACKLOG_EXCEEDED");
      let next=this.state;
      for(const row of page.rows)next=advanceGuardianOfflineSource(next,row);
      // If the ACK is lost/unknown, we do NOT advance volatile state.
      if(this.stopped!==null)return fail("HALTED_AFTER_CONCURRENT_POLL");
      if(next.projection.cursor!==this.state.projection.cursor){
        const receipt=mkReceipt(next,this.scope,this.checkpoint,page.highWater);
        await this.ack.commit(this.receipt,receipt);
        // A second concurrent poll may have latched HALT while fsync awaited.
        // In that case the receipt may be durable, but volatile state MUST
        // stay unchanged; restart will replay/reconcile against the receipt.
        if(this.stopped!==null)return fail("HALTED_AFTER_CONCURRENT_ACK");
        this.receipt=receipt;
        this.state=next;
      }
      this.readHighWater=page.highWater;
      this.verifiedAtMs=nowMs;
      return {cursor:this.cursor,highWater:page.highWater,caughtUp:this.caughtUp};
    }catch(e){
      this.stopped=e instanceof Error?e.message:"UNKNOWN_SOURCE_ERROR";
      throw e;
    }finally{this.active=false;}
  }
  /** Read-only observational hint, NOT an executor admission/fill witness. */
  readVerifiedView(nowMs:number){
    if(this.stopped!==null||this.active||this.verifiedAtMs===null
      ||!Number.isSafeInteger(nowMs)||nowMs<this.verifiedAtMs
      ||nowMs-this.verifiedAtMs>1000||!this.caughtUp)
      return fail("NOT_VERIFIED_FRESH_OR_CAUGHT_UP");
    const p=this.state.projection,r=this.state.recovery;
    return {
      version:2 as const,mode:"PAPER" as const,
      authorityId:p.authorityId,experimentId:p.experimentId,
      checkpointId:this.checkpoint.id,cursor:p.cursor,
      highWater:this.readHighWater,
      economicHash:hashGuardianRecoverableState(this.state),
      open:structuredClone([...p.open.values()]),
      restingOrders:structuredClone([...r.restingOrders.values()]),
      riskReductionOwners:structuredClone([...(r.riskReductionOwners??new Map()).values()]),
      makerReservations:structuredClone([...r.makerReservations.entries()]),
      riskReductionFailures:structuredClone([...(r.riskReductionFailures??new Map()).values()]),
      profitTrailingPairStates:structuredClone([...(r.profitTrailingPairStates??new Map()).values()]),
      reversalProtectionStates:structuredClone([...(r.reversalProtectionStates??new Map()).values()]),
      settlementLookups:structuredClone([...(r.settlementLookups??new Map()).values()]),
    };
  }
}

import { describe, expect, it } from "vitest";
import {
  PaperLedger, type Position,
  bootstrapGuardianPositionProjection, applyGuardianPositionFrame,
  guardianPositionFrameSchema, type GuardianPositionProjection,
  type GuardianOpenResidual, type GuardianPositionFrame,
} from "@pm/core";
import { paperFill } from "./paper-engine-testkit.js";

const AT = "2026-10-10T10:00:00.000Z";
const AT2 = "2026-10-10T10:00:05.000Z";
const AT3 = "2026-10-10T10:00:08.000Z";
const CHECKSUM = "a".repeat(64);

const episode = (at=AT) => ({
  marketId: "m:guardian", positionId: "pos:m:guardian",
  unhedgedStartedAt: at, heldOutcome: "UP" as const,
});
const open = (at=AT, qty="5.0", entry="0.8"): GuardianOpenResidual =>
  ({ episode: episode(at), residualQty: qty, entryPrice: entry });
const boot = (additional: Record<string, unknown>={}) => ({
  version: 1, mode: "PAPER", authorityId: "engine-epoch-1",
  experimentId: "exp-1", checkpointContentHash: CHECKSUM,
  cursor: 12, open: [open()], settledMarkets: [], ...additional,
});
const projection = (input: unknown=boot()): GuardianPositionProjection => {
  const result=bootstrapGuardianPositionProjection(input);
  if (result.status !== "APPLIED") throw Error(JSON.stringify(result));
  return result.projection;
};
const common = (kind: GuardianPositionFrame["kind"], cursor=13) => ({
  version: 1, mode: "PAPER", authorityId: "engine-epoch-1",
  experimentId: "exp-1", previousCursor: cursor-1, cursor,
  sourceOperationKey: `fill:source-${cursor}`, occurredAt: AT3, kind,
});
const fill = (previous: ReturnType<typeof episode> | null, next: GuardianOpenResidual | null,
  cursor=13): GuardianPositionFrame => ({
  ...common("FILL_POSITION",cursor), kind: "FILL_POSITION", sourceType: "PAPER_FILL",
  marketId: "m:guardian", previous, next,
});
const advance = (cursor=13): GuardianPositionFrame => ({
  ...common("NO_POSITION_CHANGE",cursor), kind: "NO_POSITION_CHANGE",
  sourceType: "LEDGER_MARK_TO_MARKET",
});
const settle = (cursor=13): GuardianPositionFrame => ({
  ...common("SETTLED",cursor), kind: "SETTLED",
  sourceType: "POSITION_SETTLED", marketId: "m:guardian",
  sourceOperationKey: "settlement:m:guardian:exp-1",
});
const applied = (prior: GuardianPositionProjection, f: unknown): GuardianPositionProjection => {
  const result=applyGuardianPositionFrame(prior,f);
  if (result.status !== "APPLIED") throw Error(JSON.stringify(result));
  return result.projection;
};
const reject = (prior: GuardianPositionProjection,f: unknown,reason: string) =>
  expect(applyGuardianPositionFrame(prior,f)).toEqual({status:"REJECTED",reason});

describe("G2E read-only Guardian position authority frames, NO SHADOW", () => {
  it("G2E-01 bootstraps a verified checkpoint envelope without granting execution authority",()=>{
    const state=projection();
    expect(state.cursor).toBe(12);
    expect(state.open.get("m:guardian")).toEqual(open());
    expect(state.settledMarkets.size).toBe(0);
    expect(state.lastFrameJson).toBeNull();
  });
  it("G2E-02 rejects corrupt, duplicate, overlapping and LIVE bootstrap",()=>{
    for(const invalid of [
      boot({mode:"LIVE"}),boot({cursor:-1}),boot({checkpointContentHash:"bogus"}),
      boot({open:[open(),open()]}),boot({settledMarkets:["m:guardian"]}),
      boot({settledMarkets:["another","another"]}),boot({open:[{...open(),residualQty:"0"}]}),
      boot({open:[{...open(),entryPrice:"1.0001"}]}),boot({adminOverride:true}),
    ]) expect(bootstrapGuardianPositionProjection(invalid)).toEqual({status:"REJECTED",reason:"INVALID_BOOTSTRAP"});
  });
  it("G2E-03 updates residual while preserving exact episode; prior state is immutable",()=>{
    const prev=projection();
    const next=applied(prev,fill(episode(),open(AT,"2.5"),13));
    expect(prev.cursor).toBe(12);
    expect(prev.open.get("m:guardian")?.residualQty).toBe("5.0");
    expect(next.cursor).toBe(13);
    expect(next.open.get("m:guardian")?.residualQty).toBe("2.5");
    expect(next.open.get("m:guardian")?.episode).toEqual(prev.open.get("m:guardian")?.episode);
  });
  it("G2E-04 accepts PAIR/SELL residual close, then a new episode of same stable position id",()=>{
    const first=projection();
    const closed=applied(first,fill(episode(),null,13));
    expect(closed.open.size).toBe(0);
    const reopened=applied(closed,fill(null,open(AT2,"4"),14));
    expect(reopened.open.get("m:guardian")?.episode.unhedgedStartedAt).toBe(AT2);
    reject(reopened,fill(episode(),null,15),"EPISODE_PRECONDITION_FAILED");
    expect(reopened.cursor).toBe(14);
  });
  it("G2E-05 explicitly fences a same-market episode replacement at increasing start time",()=>{
    const prev=projection();
    const newer=applied(prev,fill(episode(),open(AT2,"3"),13));
    expect(newer.open.get("m:guardian")?.episode.unhedgedStartedAt).toBe(AT2);
    reject(prev,fill(episode(),open("2026-10-09T23:59:59Z","2"),13),"INVALID_EPISODE_TRANSITION");
  });
  it("G2E-06 cannot skip even unrelated durable sequences",()=>{
    const start=projection();
    const next=applied(start,advance(13));
    expect(next.open.get("m:guardian")).toEqual(open());
    reject(start,advance(14),"DURABLE_CURSOR_GAP");
    reject(next,{...advance(14),previousCursor:12},"DURABLE_CURSOR_GAP");
    reject(next,fill(episode(),open(AT,"3"),16),"DURABLE_CURSOR_GAP");
  });
  it("G2E-07 only the exact last event can be retransmitted; changed payload fails closed",()=>{
    const first=projection();
    const event=fill(episode(),open(AT,"3"),13);
    const next=applied(first,event);
    const retry=applyGuardianPositionFrame(next,event);
    expect(retry).toEqual({status:"DUPLICATE",projection:next});
    expect(retry.status === "DUPLICATE" && retry.projection).toBe(next);
    reject(next,{...event,next:open(AT,"2")},"DUPLICATE_CURSOR_CONFLICT");
    reject(next,fill(episode(),null,12),"DURABLE_CURSOR_GAP");
  });
  it("G2E-08 invalid source type cannot be hidden as an advance-only frame",()=>{
    const state=projection();
    reject(state,{...advance(),sourceType:"PAPER_FILL"},"INVALID_FRAME");
    reject(state,{...advance(),sourceType:"POSITION_SETTLED"},"INVALID_FRAME");
    reject(state,{...advance(),sourceType:"REPORT_SOURCE_INTEGRITY_FAULT"},"INVALID_FRAME");
    reject(state,{...advance(),sourceType:"EXPERIMENT_STARTED"},"INVALID_FRAME");
    reject(state,{...fill(episode(),null),sourceType:"RISK_REDUCTION_ADMITTED"},"INVALID_FRAME");
    reject(state,{...advance(),debugExecute:true},"INVALID_FRAME");
    reject(state,{...fill(episode(),null),sourceOperationKey:"not-a-fill"},"INVALID_FRAME");
    reject(state,{...settle(),sourceOperationKey:"fill:fake"},"INVALID_FRAME");
  });
  it("G2E-09 authority identity and experiment never change midstream",()=>{
    const state=projection();
    reject(state,{...advance(),authorityId:"other-engine"},"AUTHORITY_MISMATCH");
    reject(state,{...advance(),experimentId:"exp-2"},"EXPERIMENT_MISMATCH");
    reject(state,{...advance(),mode:"LIVE"},"INVALID_FRAME");
  });
  it("G2E-10 post-settlement is terminal and cannot re-create residual inventory",()=>{
    const state=projection();
    const next=applied(state,settle());
    expect(next.open.size).toBe(0);
    expect(next.settledMarkets.has("m:guardian")).toBe(true);
    reject(next,fill(null,open(AT2,"1"),14),"ALREADY_SETTLED");
    reject(next,settle(14),"ALREADY_SETTLED");
    expect(next.cursor).toBe(13);
  });
  it("G2E-11 rejects fake prior episode, wrong market, malformed amounts and missing type",()=>{
    const state=projection();
    reject(state,fill(episode(AT2),open(AT2),13),"EPISODE_PRECONDITION_FAILED");
    reject(state,{...fill(episode(),open()),marketId:"other-market"},"INVALID_EPISODE_TRANSITION");
    reject(state,fill(null,open(AT2),13),"EPISODE_PRECONDITION_FAILED");
    reject(state,fill(episode(),{...open(),residualQty:"5e3"}),"INVALID_FRAME");
    reject(state,fill(episode(),{...open(),residualQty:"0"}),"INVALID_FRAME");
    reject(state,fill(episode(),{...open(),residualQty:"10000000000000000000000000000000000000000000000000"}),"INVALID_FRAME");
    reject(state,{...fill(episode(),open()),sourceType:undefined},"INVALID_FRAME");
  });
  it("G2E-12 handles a paired-only PAPER_FILL with no directional episode safely",()=>{
    const state=projection(boot({open:[]}));
    const next=applied(state,fill(null,null,13));
    expect(next.cursor).toBe(13);
    expect(next.open.size).toBe(0);
    reject(projection(),fill(null,null,13),"EPISODE_PRECONDITION_FAILED");
  });
  it("G2E-13 restart from a checkpoint at the new cursor stays fenced",()=>{
    const prior=applied(projection(),fill(episode(),open(AT,"1"),13));
    const recovery=projection(boot({
      cursor:13,open:[open(AT,"1")],
      checkpointContentHash:"b".repeat(64),
    }));
    expect(recovery.open.get("m:guardian")).toEqual(prior.open.get("m:guardian"));
    reject(recovery,fill(episode(),open(AT,"1"),13),"DUPLICATE_CURSOR_CONFLICT");
    const next=applied(recovery,advance(14));
    expect(next.cursor).toBe(14);
  });
  it("G2E-14 maps the real PaperLedger unhedged episode lifecycle, not marketId alone",()=>{
    const ledger=new PaperLedger("1000",new Date(AT));
    const mk=(stamp:string,quantity="5",side:"BUY"|"SELL"="BUY") => paperFill(
      "m:guardian","UP","0.8",{id:`fill-${stamp}-${side}`,timestamp:stamp,quantity,side});
    const first:Position=ledger.applyFill(mk(AT),{positionId:"pos:m:guardian"});
    expect(first.unhedgedStartedAt).toBe(AT);
    const partial=ledger.applyFill(mk(AT2,"2","SELL"));
    expect(partial.unhedgedStartedAt).toBe(AT);
    expect(partial.unpairedQuantity).toBe("3");
    const closed=ledger.applyFill(mk(AT3,"3","SELL"));
    expect(closed.unhedgedStartedAt).toBeNull();
    const reopened=ledger.applyFill(mk("2026-10-10T10:00:10.000Z","1"),{positionId:"pos:m:guardian"});
    expect(reopened.id).toBe(first.id);
    expect(reopened.unhedgedStartedAt).not.toBe(first.unhedgedStartedAt);
  });
});

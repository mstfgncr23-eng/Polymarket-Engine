import { generateKeyPairSync, sign } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { createIdleExperiment, PaperLedger } from "@pm/core";
import { CHECKPOINT_SCHEMA_VERSION, hashCheckpointPayload, hashDurablePayload, type DurableTransition } from "@pm/db";
import { GuardianFileAckStore, type GuardianConsumerAckStore } from "../apps/engine/src/guardian-consumer-ack-store.js";
import { GuardianReadonlySession, hashGuardianRecoverableState, type GuardianG2HJournal } from "../apps/engine/src/guardian-readonly-session.js";
import { bootstrapGuardianOfflineSource } from "../apps/engine/src/guardian-offline-durable-source.js";
import { guardianWriterManifestBytes, type GuardianWriterManifest, type GuardianWriterTrust } from "../apps/engine/src/guardian-signed-writer-manifest.js";
import { signGuardianDurablePage } from "../apps/engine/src/guardian-signed-durable-page.js";
import { plannedDurableTransition } from "../apps/engine/src/recovery.js";
import { accountResultFill, emptyResultAccounting } from "../apps/engine/src/trading-results.js";
import { paperFill } from "./paper-engine-testkit.js";

const NOW=Date.parse("2026-10-10T10:00:00Z"),AT=new Date(NOW).toISOString();
const MARKET="g2h:btc", POS="pos:g2h:btc";
const dirs:string[]=[];
afterEach(async()=>{for (const dir of dirs.splice(0)) await rm(dir,{recursive:true,force:true});});
const {publicKey,privateKey}=generateKeyPairSync("ed25519");
const makeCheckpoint=()=>{
  const exp={...createIdleExperiment({durationHours:1,strategyVersion:"test",riskConfiguration:{},
    modelConfiguration:{},startingBalance:"1000",systemVersion:"test",gitSha:"test"}),
    id:"session-experiment",status:"RUNNING" as const,startedAt:AT,
  };
  const ledger=new PaperLedger("1000",new Date(NOW));
  const first=paperFill(MARKET,"UP","0.40",{id:"initial-buy",quantity:"5",timestamp:AT});
  ledger.applyFill(first,{positionId:POS});
  const resultAccounting=emptyResultAccounting();
  accountResultFill(resultAccounting,first,null,ledger.positions.get(MARKET));
  const body={schemaVersion:CHECKPOINT_SCHEMA_VERSION,durableCursor:20,
    experiment:exp,ledger:ledger.snapshot(),positions:[...ledger.positions.values()],resultAccounting,
    restingOrders:[],makerReservations:{},activeTriggers:[],grokSignals:[],settledMarkets:[],
    riskReductionOwners:[],riskReductionFailures:[],reversalProtectionStates:[],
    profitTrailingPairStates:[],settlementLookups:[],nakedDirectionalDiagnostics:[],
    lastInfrastructureAccrualAt:0,
  };
  const hash=hashCheckpointPayload(body);
  return {id:"signed-checkpoint",experimentId:exp.id,sequence:1,createdAt:AT,
    verified:true,contentHash:hash,payload:{...body,checkpointContentHash:hash}};
};
const checkpoint=makeCheckpoint();
const transition=(sequence:number,type:string,payload:Record<string,unknown>,key:string)=>{
  const t=plannedDurableTransition({operationKey:key,type,
    aggregateType:type==="PAPER_FILL"?"paper_fill":"position",
    aggregateId:type==="PAPER_FILL"?String((payload.fill as {id:string}).id):MARKET,payload},new Date(NOW+sequence*100),sequence);
  return {...t,payloadHash:hashDurablePayload(payload)};
};
const sell=paperFill(MARKET,"UP","0.52",{id:"g2h-sell",quantity:"2",side:"SELL",
  timestamp:new Date(NOW+1000).toISOString()});
const rows:DurableTransition[]=[
  transition(21,"PAPER_FILL",{fill:sell,positionId:POS,reservationAfter:null},`fill:${sell.id}`),
  transition(22,"POSITION_SETTLED",{marketId:MARKET,upPayoff:"1",downPayoff:"0",
    settledAt:new Date(NOW+2000).toISOString()},`settlement:${MARKET}`),
];
const trust:GuardianWriterTrust={
  issuerKeyId:"operator-pinned",authorityId:"writer-epoch-7",
  publicKeyPem:publicKey.export({format:"pem",type:"spki"}).toString(),
  minimumWriterEpoch:7,minimumManifestSequence:0,nowMs:NOW,
};
const signed=(highWater:number,manifestSequence=1,writerEpoch=7,expiresAtMs=NOW+5000):GuardianWriterManifest=>{
  const meta={
    version:1 as const,issuerKeyId:trust.issuerKeyId,authorityId:trust.authorityId,
    writerEpoch,manifestSequence,experimentId:checkpoint.experimentId,
    checkpointId:checkpoint.id,checkpointHash:checkpoint.contentHash,
    durableHighWater:highWater,issuedAtMs:NOW-500,expiresAtMs,
  };
  return {...meta,signature:sign(null,guardianWriterManifestBytes(meta),privateKey).toString("base64")};
};
const source=(highWater=22,events=rows):GuardianG2HJournal=>({
  listVerifiedCheckpoints:async()=>[checkpoint],
  latestDurableSequence:async()=>highWater,
  readSignedDurablePageAfter:async(cursor,limit,manifest)=>{
    const page=events.filter(x=>x.sequence>cursor).slice(0,limit);
    const certificate=page.length?signGuardianDurablePage({
      rows:page,fromCursor:cursor,manifest,
      privateKeyPem:privateKey.export({type:"pkcs8",format:"pem"}).toString(),nowMs:NOW,
    }):null;
    return {rows:page,certificate};
  },
});
const setup=async()=>{
  const dir=await mkdtemp(join(tmpdir(),"pm-g2h-session-"));dirs.push(dir);
  return new GuardianFileAckStore(join(dir,"receipt.guardian-ack.json"));
};
const start=(ackStore:GuardianConsumerAckStore,manifest=signed(20),
  journal=source(),pageSize=1,maxBacklogRows=100)=>GuardianReadonlySession.open({
    source:journal,ackStore,trust,manifest,pageSize,maxBacklogRows,
  });
const rejected=(p:Promise<unknown>,reason:string)=>expect(p).rejects.toMatchObject({reason});

describe("G2H read-only authenticated journal session, no shadow or orders",()=>{
  it("G2H-SESSION-01 pins manifest, pages 21->22 and ACKs only verified progress",async()=>{
    const ack=await setup();
    const session=await start(ack);
    expect(session.cursor).toBe(20);
    expect(session.openPositionCount).toBe(1);
    expect((await ack.load())?.cursor).toBe(20);
    const one=await session.poll(signed(22,2),NOW);
    expect(one).toEqual({cursor:21,highWater:22,caughtUp:false});
    expect((await ack.load())?.cursor).toBe(21);
    const two=await session.poll(signed(22,3),NOW);
    expect(two).toEqual({cursor:22,highWater:22,caughtUp:true});
    expect(session.openPositionCount).toBe(0);
    expect((await ack.load())?.cursor).toBe(22);
    expect((await session.poll(signed(22,4),NOW)).caughtUp).toBe(true);
  });
  it("G2H-SESSION-02 restart does not skip rows, replay reproduces persisted projection",async()=>{
    const ack=await setup();
    const initial=await start(ack);
    await initial.poll(signed(22,2),NOW);
    const resume=await start(ack,signed(22,3));
    expect(resume.cursor).toBe(21);
    expect(resume.openPositionCount).toBe(1);
    expect((await resume.poll(signed(22,4),NOW)).cursor).toBe(22);
  });
  it("G2H-SESSION-03 tampered ACK digest cannot substitute for replay proof",async()=>{
    const ack=await setup();
    const first=await start(ack);
    await first.poll(signed(22,2),NOW);
    const receipt=(await ack.load())!;
    await ack.commit(receipt,{...receipt,manifestSequence:3,projectionHash:"f".repeat(64)});
    await rejected(start(ack,signed(22,4)),"ACK_STATE_DIVERGENCE");
  });
  it("G2H-SESSION-04 unknown ACK write latches halt; never returns advanced in-memory view",async()=>{
    const real=await setup();
    const store:GuardianConsumerAckStore={
      load:()=>real.load(),
      commit:async(expected,next)=>{
        await real.commit(expected,next);
        if(next.cursor===21) throw new Error("ACK_RESULT_UNKNOWN");
      },
    };
    const session=await start(store);
    await expect(session.poll(signed(22,2),NOW)).rejects.toThrow("ACK_RESULT_UNKNOWN");
    expect(session.status).toBe("HALTED");
    expect(session.cursor).toBe(20);
    expect((await real.load())?.cursor).toBe(21);
    await rejected(session.poll(signed(22,3),NOW),"SOURCE_REPLAY_FAILED");
    const reconciled=await start(real,signed(22,4));
    expect(reconciled.cursor).toBe(21);
  });
  it("G2H-SESSION-05 signing expiry/changed epoch and checkpoint halt safely",async()=>{
    const ack=await setup();
    const session=await start(ack);
    await expect(session.poll(signed(22,2),NOW+5001)).rejects.toMatchObject({reason:"MANIFEST_EXPIRED"});
    expect(session.status).toBe("HALTED");
    await rejected(session.poll(signed(22,3),NOW),"SOURCE_REPLAY_FAILED");
    const other=await start(await setup());
    await rejected(other.poll(signed(22,2,8),NOW),"MANIFEST_EPOCH_CHANGED");
  });
  it("G2H-SESSION-06 missing/compacted row, reader failure and signed highwater ahead halt",async()=>{
    const ack=await setup();
    const bad=await start(ack,signed(20),source(22,[rows[1]!]));
    await rejected(bad.poll(signed(22,2),NOW),"SOURCE_UNAVAILABLE");
    expect(bad.status).toBe("HALTED");
    const ack2=await setup();
    const delayed=await start(ack2,signed(20),source(21,[rows[0]!]));
    await rejected(delayed.poll(signed(22,2),NOW),"SIGNED_HIGHWATER_NOT_VISIBLE");
    const ack3=await setup();
    const unavailable=await start(ack3,signed(20),{
      ...source(),readSignedDurablePageAfter:async()=>{throw Error("DB_TIMEOUT");},
    });
    await rejected(unavailable.poll(signed(22,2),NOW),"SOURCE_UNAVAILABLE");
    expect(unavailable.status).toBe("HALTED");
  });
  it("G2H-SESSION-07 source backlog and watermark regression are fatal",async()=>{
    const ack=await setup();
    await rejected(start(ack,signed(20),source(),1,1),"BACKLOG_EXCEEDED");
    const second=await start(await setup());
    await rejected(second.poll(signed(19,2),NOW),"MANIFEST_WATERMARK_REGRESSION");
  });
});

describe("G2H closed concurrency and bounded page stress",()=>{
  it("G2H-SESSION-08 overlapping polls latch halt; late first read cannot ACK",async()=>{
    const ack=await setup();
    let release:()=>void=()=>{};
    const pending=new Promise<void>(resolve=>{release=resolve;});
    let hold=false;
    const base=source();
    const slow:GuardianG2HJournal={
      ...base,latestDurableSequence:async()=>{if(hold) await pending;return 22;},
    };
    const s=await start(ack,signed(20),slow);
    hold=true;
    const first=s.poll(signed(22,2),NOW);
    await rejected(s.poll(signed(22,3),NOW),"SOURCE_REPLAY_FAILED");
    release();
    await rejected(first,"SOURCE_REPLAY_FAILED");
    expect(s.status).toBe("HALTED");
    expect((await ack.load())?.cursor).toBe(20);
  });
  it("G2H-SESSION-09 100 synthetic 100Hz rows remain page bounded and resume cleanly",async()=>{
    const ack=await setup();
    const more:DurableTransition[]=[];
    for(let i=21;i<=120;i++){
      const at=new Date(NOW+(i-20)*10).toISOString();
      more.push(transition(i,"LEDGER_MARK_TO_MARKET",{
        prices:{[MARKET]:{up:"0.46",down:"0.54"}},markedAt:at,
      },`mtm:100hz:${i}`));
    }
    const journal=source(120,more);
    const s=await start(ack,signed(20),journal,8,120);
    const begin=Date.now();
    let maxPageAdvance=0;
    for(let i=2;i<16;i++){
      const before=s.cursor;
      const out=await s.poll(signed(120,i),NOW);
      maxPageAdvance=Math.max(maxPageAdvance,out.cursor-before);
      if(out.caughtUp)break;
    }
    expect(s.cursor).toBe(120);
    expect(maxPageAdvance).toBeLessThanOrEqual(8);
    expect(s.openPositionCount).toBe(1);
    expect((await ack.load())?.cursor).toBe(120);
    const resumed=await start(ack,signed(120,16),journal,8,120);
    expect(resumed.cursor).toBe(120);
    expect(Date.now()-begin).toBeGreaterThanOrEqual(0);
  });
});

describe("G2H strict receipt-to-checkpoint cursor proof",()=>{
  it("G2H-SESSION-10 rejects replacing an ACKed projection at same cursor even with newer signer sequence",async()=>{
    const ack=await setup();
    await start(ack);
    const current=(await ack.load())!;
    await ack.commit(current,{...current,manifestSequence:2,projectionHash:"e".repeat(64)});
    await rejected(start(ack,signed(22,3)),"ACK_STATE_DIVERGENCE");
  });
  it("G2H-SESSION-11 refuses to jump past a receipt when a checkpoint begins ahead",async()=>{
    const original=await setup();
    await start(original);
    const current=(await original.load())!;
    let writes=0;
    const old:GuardianConsumerAckStore={
      load:async()=>({...current,cursor:19,projectionHash:"d".repeat(64)}),
      commit:async()=>{writes++;},
    };
    await rejected(start(old,signed(22,3)),"STALE_CHECKPOINT_REPLAY");
    expect(writes).toBe(0);
  });
});

describe("G2H ACK binds all risk and maker economic state",()=>{
  it("G2H-SESSION-12 changing maker reservations without changing open positions changes ACK digest",()=>{
    const base=bootstrapGuardianOfflineSource({
      payload:checkpoint.payload,
      externallyVerifiedContentHash:checkpoint.contentHash,
      authorityId:"canonical-epoch",
    });
    const baseline=hashGuardianRecoverableState(base);
    const manipulated={...base,recovery:{...base.recovery,
      makerReservations:new Map([["maker-order","100"]])}};
    expect([...manipulated.projection.open]).toEqual([...base.projection.open]);
    expect(hashGuardianRecoverableState(manipulated)).not.toBe(baseline);
    const other={...base,recovery:{...base.recovery,
      profitTrailingPairStates:new Map([["unexpected",{} as never]])}};
    expect(hashGuardianRecoverableState(other)).not.toBe(baseline);
    const existingKeys={...base,seenOperationKeys:new Set(["previous-commit-key"])};
    expect(hashGuardianRecoverableState(existingKeys)).not.toBe(baseline);
  });
});

describe("G2H complete observational handoff has no execution authority",()=>{
  it("G2H-VIEW-01 returns complete pinned open episodes only at signed highwater and copies values",async()=>{
    const pristineAck=await setup();
    const pristine=await start(pristineAck,signed(20),source(20,[]));
    expect(()=>pristine.readVerifiedView(NOW)).toThrowError(
      expect.objectContaining({reason:"SOURCE_REPLAY_FAILED"}));
    expect((await pristine.poll(signed(20,2),NOW)).caughtUp).toBe(true);
    const view=pristine.readVerifiedView(NOW);
    expect(view).toMatchObject({mode:"PAPER",authorityId:"writer-epoch-7",
      writerEpoch:7,cursor:20,highWater:20,open:[{residualQty:"5"}]});
    expect(view.economicStateHash).toMatch(/^[a-f0-9]{64}$/);
    view.open[0]!.residualQty="999";
    expect(pristine.readVerifiedView(NOW).open[0]?.residualQty).toBe("5");
    const ack=await setup();
    const session=await start(ack,signed(20),source(22));
    expect((await session.poll(signed(20,2),NOW)).caughtUp).toBe(false);
    expect(()=>session.readVerifiedView(NOW)).toThrowError(
      expect.objectContaining({reason:"SOURCE_REPLAY_FAILED"}));
    expect((await session.poll(signed(22,3),NOW)).caughtUp).toBe(false);
    expect(()=>session.readVerifiedView(NOW)).toThrowError(
      expect.objectContaining({reason:"SOURCE_REPLAY_FAILED"}));
    expect((await session.poll(signed(22,4),NOW)).caughtUp).toBe(true);
    expect(session.readVerifiedView(NOW).open).toEqual([]);
    expect(session.readVerifiedView(NOW).settledMarkets).toEqual([MARKET]);
  });
  it("G2H-VIEW-02 expired producer heartbeat blocks even an already ACKed, unchanged cursor",async()=>{
    const ack=await setup();
    const session=await start(ack,signed(20),source(20,[]));
    await session.poll(signed(20,2),NOW);
    expect(()=>session.readVerifiedView(NOW+5000)).toThrowError(
      expect.objectContaining({reason:"SOURCE_REPLAY_FAILED"}));
    expect(()=>session.readVerifiedView(NOW+1001)).toThrowError(
      expect.objectContaining({reason:"SOURCE_REPLAY_FAILED"}));
    expect(()=>session.readVerifiedView(NOW+1000)).not.toThrow();
  });
});

describe("G2H no unsigned or self-hashed durable rows reach the source supervisor",()=>{
  it("G2H-SESSION-13 a self-hashed DB mutation after independent signing HALTS and cannot ACK",async()=>{
    const ack=await setup();
    const producer=source();
    const evil:GuardianG2HJournal={
      ...producer,
      readSignedDurablePageAfter:async(cursor,limit,manifest)=>{
        const authentic=await producer.readSignedDurablePageAfter(cursor,limit,manifest);
        const mutated=structuredClone(authentic.rows);
        mutated[0]!.payload={...mutated[0]!.payload,positionId:"ATTACKER-CONTROLLED"};
        mutated[0]!.payloadHash=hashDurablePayload(mutated[0]!.payload);
        return {rows:mutated,certificate:authentic.certificate};
      },
    };
    const session=await start(ack,signed(20),evil);
    await rejected(session.poll(signed(22,2),NOW),"SOURCE_UNAVAILABLE");
    expect(session.status).toBe("HALTED");
    expect((await ack.load())?.cursor).toBe(20);
  });
  it("G2H-SESSION-14 no writer page certificate HALTS; a raw journal page is insufficient",async()=>{
    const ack=await setup();
    const sourceOnly:GuardianG2HJournal={
      ...source(),
      readSignedDurablePageAfter:async(cursor,limit)=>({
        rows:rows.filter(r=>r.sequence>cursor).slice(0,limit),
        certificate:null,
      }),
    };
    const session=await start(ack,signed(20),sourceOnly);
    await rejected(session.poll(signed(22,2),NOW),"SOURCE_UNAVAILABLE");
    expect(session.status).toBe("HALTED");
    expect((await ack.load())?.cursor).toBe(20);
  });
});

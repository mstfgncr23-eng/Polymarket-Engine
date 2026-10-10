import { describe, expect, it } from "vitest";
import { snapshot } from "./fixtures.js";
import {
  advanceGuardianBookChannel, beginGuardianBookGeneration, createGuardianBookChannel,
  readGuardianPairedBook, type GuardianBookChannel,
} from "../apps/engine/src/guardian-paired-book-channel.js";

const NOW=Date.parse("2026-08-31T12:00:00.000Z");
const LIMITS={nowMs:NOW,maxAgeMs:1000,maxSkewMs:100};
const init=()=>beginGuardianBookGeneration(createGuardianBookChannel({
  marketId:"btc-15m",upTokenId:"up",downTokenId:"down",
}));
const book=(outcome:"UP"|"DOWN",seq=1,ms=NOW)=>snapshot({
  marketId:"btc-15m",tokenId:outcome.toLowerCase(),outcome,
  eventId:`local-${outcome}-${seq}`,sequence:seq,
  sourceTimestamp:new Date(ms).toISOString(),receivedAt:new Date(ms).toISOString(),
  bids:[{price:"0.40",quantity:"2"},{price:"0.39",quantity:"4"}],
  asks:[{price:"0.41",quantity:"3"},{price:"0.42",quantity:"5"}],
});
const ingest=(s:GuardianBookChannel,outcome:"UP"|"DOWN",seq:number,ms=NOW)=>
  advanceGuardianBookChannel(s,{kind:"FULL_BOOK",generation:s.generation,
    ingressSequence:seq,synchronized:true,snapshot:book(outcome,seq,ms)});
const paired=()=>{
  const a=ingest(init(),"UP",1);
  return ingest(a,"DOWN",2);
};

describe("G2H normalized complete-book channel, OFFLINE only",()=>{
  it("G2H-BOOK-01 requires both fresh, synchronized UP/DOWN books, never a single side",()=>{
    const a=init();
    expect(readGuardianPairedBook(a,LIMITS)).toEqual({status:"BLOCKED",reason:"MISSING_SIDE"});
    const b=ingest(a,"UP",1);
    expect(b.status).toBe("AWAITING_BOOKS");
    expect(readGuardianPairedBook(b,LIMITS).status).toBe("BLOCKED");
    const c=ingest(b,"DOWN",2);
    expect(c.status).toBe("ACTIVE");
    const pair=readGuardianPairedBook(c,LIMITS);
    expect(pair.status).toBe("READY");
    if(pair.status==="READY") {
      expect(pair.generation).toBe(1);
      expect(pair.ingressSequence).toBe(2);
      expect(pair.up.tokenId).toBe("up");
      expect(pair.down.tokenId).toBe("down");
    }
  });
  it("G2H-BOOK-02 disconnect clears BOTH sides; reconnect requires two new full books",()=>{
    const ready=paired();
    const invalidated=advanceGuardianBookChannel(ready,{
      kind:"INVALIDATE",generation:ready.generation,ingressSequence:3,reason:"DISCONNECT",
    });
    expect(invalidated.status).toBe("DISCONNECTED");
    expect(Object.keys(invalidated.books)).toHaveLength(0);
    expect(readGuardianPairedBook(invalidated,LIMITS)).toEqual({status:"BLOCKED",reason:"DISCONNECTED"});
    const next=beginGuardianBookGeneration(invalidated);
    expect(next.generation).toBe(2);
    expect(readGuardianPairedBook(ingest(next,"UP",1),LIMITS).status).toBe("BLOCKED");
    expect(readGuardianPairedBook(ingest(ingest(next,"UP",1),"DOWN",2),LIMITS).status).toBe("READY");
    const oldLate=advanceGuardianBookChannel(next,{kind:"FULL_BOOK",generation:ready.generation,
      ingressSequence:1,synchronized:true,snapshot:book("DOWN")});
    expect(oldLate.status).toBe("HALTED");
    expect(oldLate.books).toEqual({});
  });
  it("G2H-BOOK-03 enforces local envelope ingress ordering, no upstream hash as sequence",()=>{
    const a=ingest(init(),"UP",1);
    const gap=advanceGuardianBookChannel(a,{kind:"FULL_BOOK",
      generation:a.generation,ingressSequence:3,synchronized:true,snapshot:book("DOWN")});
    expect(gap.status).toBe("HALTED");
    const duplicate=advanceGuardianBookChannel(a,{kind:"FULL_BOOK",
      generation:a.generation,ingressSequence:1,synchronized:true,snapshot:book("DOWN")});
    expect(duplicate.status).toBe("HALTED");
    expect(readGuardianPairedBook(gap,LIMITS).status).toBe("BLOCKED");
    expect(advanceGuardianBookChannel(gap,{kind:"FULL_BOOK",generation:a.generation,
      ingressSequence:2,synchronized:true,snapshot:book("DOWN")}).status).toBe("HALTED");
  });
  it("G2H-BOOK-04 rejects mismatched tokens, crossed books, missing full depth, unsynced producer",()=>{
    const a=init();
    expect(advanceGuardianBookChannel(a,{kind:"FULL_BOOK",generation:1,ingressSequence:1,
      synchronized:true,snapshot:book("UP",1)}).status).toBe("AWAITING_BOOKS");
    const wrong=advanceGuardianBookChannel(a,{kind:"FULL_BOOK",generation:1,ingressSequence:1,
      synchronized:true,snapshot:{...book("UP"),tokenId:"former-up"}});
    expect(wrong.status).toBe("HALTED");
    const crossed=advanceGuardianBookChannel(a,{kind:"FULL_BOOK",generation:1,ingressSequence:1,
      synchronized:true,snapshot:{...book("UP"),asks:[{price:"0.20",quantity:"1"}]}});
    expect(crossed.status).toBe("HALTED");
    const noBids=advanceGuardianBookChannel(a,{kind:"FULL_BOOK",generation:1,ingressSequence:1,
      synchronized:true,snapshot:{...book("UP"),bids:[]}});
    expect(noBids.status).toBe("HALTED");
    const invalid=advanceGuardianBookChannel(a,{kind:"FULL_BOOK",generation:1,ingressSequence:1,
      synchronized:false,snapshot:book("UP")} as never);
    expect(invalid.status).toBe("HALTED");
  });
  it("G2H-BOOK-05 blocks stale, future, and overly skewed source/receiver clocks",()=>{
    const ready=paired();
    expect(readGuardianPairedBook(ready,{...LIMITS,nowMs:NOW+1001})).toEqual({
      status:"BLOCKED",reason:"STALE_OR_FUTURE",
    });
    expect(readGuardianPairedBook(ready,{...LIMITS,nowMs:NOW-1}).status).toBe("BLOCKED");
    expect(readGuardianPairedBook(ready,{...LIMITS,maxSkewMs:1001}).status).toBe("BLOCKED");
    const skewed=ingest(ingest(init(),"UP",1,NOW-200),"DOWN",2,NOW);
    expect(readGuardianPairedBook(skewed,LIMITS)).toEqual({status:"BLOCKED",reason:"SKEW"});
    const delayed=advanceGuardianBookChannel(init(),{kind:"FULL_BOOK",generation:1,ingressSequence:1,
      synchronized:true,snapshot:{...book("UP"),sourceTimestamp:new Date(NOW-2000).toISOString()}});
    const both=ingest(delayed,"DOWN",2);
    expect(readGuardianPairedBook(both,LIMITS).status).toBe("BLOCKED");
  });
  it("G2H-BOOK-06 bounds retained books, does not mutate caller input; burst has no history queue",()=>{
    const start=init();
    const external=book("UP",1);
    const admitted=advanceGuardianBookChannel(start,{kind:"FULL_BOOK",generation:1,
      ingressSequence:1,synchronized:true,snapshot:external});
    external.bids[0]!.price="0.99";
    expect(admitted.books.UP?.bids[0]?.price).toBe("0.40");
    let state=ingest(admitted,"DOWN",2);
    for(let i=3;i<=1002;i++) state=ingest(state,i%2?"UP":"DOWN",i);
    expect(state.ingressSequence).toBe(1002);
    expect(Object.keys(state.books)).toEqual(["UP","DOWN"]);
    expect(state.books.UP?.bids).toHaveLength(2);
    expect(state.books.DOWN?.asks).toHaveLength(2);
    const read=readGuardianPairedBook(state,LIMITS);
    expect(read.status).toBe("READY");
    if(read.status==="READY") {
      read.up.bids[0]!.price="0.99";
      const again=readGuardianPairedBook(state,LIMITS);
      expect(again.status).toBe("READY");
      if(again.status==="READY") expect(again.up.bids[0]!.price).toBe("0.40");
    }
  });
  it("G2H-BOOK-07 mass invalidation of eight independent markets cannot expose old book pairs",()=>{
    for (let i=0;i<8;i++) {
      const marketId=`asset-${i}`;
      const initial=beginGuardianBookGeneration(createGuardianBookChannel({marketId,
        upTokenId:`up-${i}`,downTokenId:`down-${i}`}));
      const first=advanceGuardianBookChannel(initial,{kind:"FULL_BOOK",generation:1,
        ingressSequence:1,synchronized:true,
        snapshot:{...book("UP"),marketId,tokenId:`up-${i}`}});
      const s=advanceGuardianBookChannel(first,{kind:"FULL_BOOK",generation:1,
        ingressSequence:2,synchronized:true,
        snapshot:{...book("DOWN"),marketId,tokenId:`down-${i}`}});
      expect(readGuardianPairedBook(s,LIMITS).status).toBe("READY");
      const stopped=advanceGuardianBookChannel(s,{kind:"INVALIDATE",generation:s.generation,
        ingressSequence:s.ingressSequence+1,reason:"OVERLOAD"});
      expect(readGuardianPairedBook(stopped,LIMITS).status).toBe("BLOCKED");
      expect(stopped.books).toEqual({});
    }
  });
});

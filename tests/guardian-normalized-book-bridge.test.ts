import { describe,expect,it } from "vitest";
import { GuardianNormalizedBookBridge } from "../apps/engine/src/guardian-normalized-book-bridge.js";
import { snapshot } from "./fixtures.js";
const NOW=Date.parse("2026-08-31T12:00:00.000Z");
const limits={nowMs:NOW,maxAgeMs:1000,maxSkewMs:100};
const make=()=>{
  const sync:{UP:{synchronized:boolean;epoch:number};DOWN:{synchronized:boolean;epoch:number}}={
    UP:{synchronized:true,epoch:1},DOWN:{synchronized:true,epoch:1},
  };
  const bridge=new GuardianNormalizedBookBridge({
    marketId:"mkt",upTokenId:"token-up",downTokenId:"token-down",
  },{tokenTrust:id=>id==="token-up"?sync.UP:sync.DOWN});
  const book=(outcome:"UP"|"DOWN",sequence=1)=>snapshot({
    marketId:"mkt",tokenId:`token-${outcome.toLowerCase()}`,outcome,sequence,
    bids:[{price:"0.40",quantity:"2"}],asks:[{price:"0.41",quantity:"3"}],
  });
  return {sync,bridge,book};
};
describe("G2H adapter-owned normalization and per-token epoch bridge",()=>{
  it("G2H-BRIDGE-01 both trusted normalized UP/DOWN books are necessary",()=>{
    const {bridge,book}=make();
    bridge.onConnected();
    bridge.onNormalizedSnapshot(book("UP"));
    expect(bridge.observe(limits).status).toBe("BLOCKED");
    bridge.onNormalizedSnapshot(book("DOWN"));
    expect(bridge.observe(limits).status).toBe("READY");
    expect(bridge.ingressSequence).toBe(2);
  });
  it("G2H-BRIDGE-02 disconnect immediately flushes both sides, late frames are ignored",()=>{
    const {bridge,book}=make();
    bridge.onConnected();bridge.onNormalizedSnapshot(book("UP"));bridge.onNormalizedSnapshot(book("DOWN"));
    const before=bridge.generation;
    bridge.invalidate("DISCONNECT");
    bridge.onNormalizedSnapshot(book("DOWN",2));
    expect(bridge.observe(limits).status).toBe("BLOCKED");
    bridge.onConnected();
    expect(bridge.generation).toBeGreaterThan(before);
    bridge.onNormalizedSnapshot(book("DOWN",3));
    expect(bridge.observe(limits).status).toBe("BLOCKED");
    bridge.onNormalizedSnapshot(book("UP",4));
    expect(bridge.observe(limits).status).toBe("READY");
  });
  it("G2H-BRIDGE-03 one-token adapter resync forces BOTH to reappear in new epoch",()=>{
    const {sync,bridge,book}=make();
    bridge.onConnected();bridge.onNormalizedSnapshot(book("UP"));bridge.onNormalizedSnapshot(book("DOWN"));
    const previous=bridge.generation;
    sync.UP.epoch++;
    bridge.onNormalizedSnapshot(book("UP",2));
    expect(bridge.generation).toBeGreaterThan(previous);
    expect(bridge.observe(limits).status).toBe("BLOCKED");
    bridge.onNormalizedSnapshot(book("DOWN",2));
    expect(bridge.observe(limits).status).toBe("READY");
  });
  it("G2H-BRIDGE-04 unsync or wrong canonical market token does not leak old READY",()=>{
    const {sync,bridge,book}=make();
    bridge.onConnected();bridge.onNormalizedSnapshot(book("UP"));bridge.onNormalizedSnapshot(book("DOWN"));
    sync.DOWN.synchronized=false;
    bridge.onNormalizedSnapshot(book("UP",2));
    expect(bridge.observe(limits).status).toBe("BLOCKED");
    sync.DOWN.synchronized=true;sync.DOWN.epoch++;
    bridge.onNormalizedSnapshot(book("UP",3));
    expect(bridge.observe(limits).status).toBe("BLOCKED");
    bridge.onNormalizedSnapshot(book("DOWN",3));
    expect(bridge.observe(limits).status).toBe("READY");
    bridge.onNormalizedSnapshot({...book("DOWN"),tokenId:"rotated-down"});
    expect(bridge.observe(limits).status).toBe("BLOCKED");
  });
  it("G2H-BRIDGE-05 overload requires explicit connect and fresh pair",()=>{
    const {bridge,book}=make();
    bridge.onConnected();bridge.onNormalizedSnapshot(book("UP"));bridge.onNormalizedSnapshot(book("DOWN"));
    bridge.invalidate("OVERLOAD");
    bridge.onNormalizedSnapshot(book("UP"));
    bridge.onNormalizedSnapshot(book("DOWN"));
    expect(bridge.observe(limits).status).toBe("BLOCKED");
    bridge.onConnected();
    bridge.onNormalizedSnapshot(book("UP"));bridge.onNormalizedSnapshot(book("DOWN"));
    expect(bridge.observe(limits).status).toBe("READY");
  });
  it("G2H-BRIDGE-06 invalid normalized depth halts and cannot recover without reconnect",()=>{
    const {bridge,book}=make();
    bridge.onConnected();bridge.onNormalizedSnapshot(book("UP"));
    bridge.onNormalizedSnapshot({...book("DOWN"),bids:[]});
    expect(bridge.status).toBe("HALTED");
    bridge.onNormalizedSnapshot(book("DOWN"));
    expect(bridge.observe(limits).status).toBe("BLOCKED");
    bridge.onConnected();
    bridge.onNormalizedSnapshot(book("UP"));bridge.onNormalizedSnapshot(book("DOWN"));
    expect(bridge.observe(limits).status).toBe("READY");
  });
});

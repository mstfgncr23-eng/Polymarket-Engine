import { describe, expect, it } from "vitest";
import { PolymarketMarketDataAdapter } from "../packages/market-data/src/polymarket.js";
import { ScriptedBoundedMarketTransport, MARKET_DRAIN_BOUNDS } from "@pm/market-data";

describe("bounded market drain fairness", () => {
  it("drains a saturated FIFO in exact order while servicing external IO turns", async () => {
    const transport = new ScriptedBoundedMarketTransport();
    const adapter = new PolymarketMarketDataAdapter(undefined, { transport });
    adapter.registerManualToken("drain-up", "drain", "UP", "XRP");
    adapter.setTokenIds(["drain-up"]);
    const abort = new AbortController(); let count=0, ioAt=-1;
    const prices: string[]=[]; const errors: Error[]=[];
    const run=adapter.run(["drain-up"], { onSnapshot:s=>{count++;prices.push(s.bids[0]!.quantity);},
      onConnection:()=>undefined,onError:e=>{errors.push(e);}},abort.signal);
    while(!transport.stream) await new Promise(r=>setTimeout(r,1));
    transport.push({type:"book",payload:{tokenId:"drain-up",market:"drain",timestamp:1000,
      bids:[{price:"0.69",size:"1"}],asks:[{price:"0.70",size:"100"}]}});
    for(let i=1;i<=4000;i++)expect(transport.push({type:"price_change",payload:{timestamp:1000+i,
      priceChanges:[{tokenId:"drain-up",side:"BUY",price:"0.69",size:String(i+1),hash:String(i)}]}}).ok).toBe(true);
    setImmediate(()=>{ioAt=count;});
    try {
      const end=Date.now()+5000;
      while(count<4001 && Date.now()<end)await new Promise(r=>setTimeout(r,2));
      expect(count).toBe(4001);expect(errors).toEqual([]);
      expect(prices).toEqual(Array.from({length:4001},(_,i)=>String(i+1)));
      expect(ioAt).toBeGreaterThan(0);expect(ioAt).toBeLessThanOrEqual(MARKET_DRAIN_BOUNDS.maxEventsPerTurn);
      expect(adapter.ingressSnapshot()).toMatchObject({depth:0,processed:4001,gapTotal:0});
      expect(transport.maxConcurrent).toBe(1);
    } finally {abort.abort();await run;}
  });
});

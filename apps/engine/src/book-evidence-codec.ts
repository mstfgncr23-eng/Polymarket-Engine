import type { MarketSnapshot } from "@pm/core";
type Level=MarketSnapshot['bids'][number];
type Change={price:string;quantity:string|null};
export type BookEvidence = {encoding:'FULL';snapshot:MarketSnapshot} | {
  encoding:'DELTA';baseEventId:string;snapshot:Omit<MarketSnapshot,'bids'|'asks'>;
  bidsDelta:Change[];asksDelta:Change[];bidOrder?:string[];askOrder?:string[];
};
type State={snapshot:MarketSnapshot;bucket:number;epoch:number;sync:boolean};
function changes(old:Level[],current:Level[]):Change[]{
  const before=new Map(old.map(x=>[x.price,x.quantity]));
  const result:Change[]=[];
  for(const x of current){if(before.get(x.price)!==x.quantity)result.push({...x});before.delete(x.price);}
  for(const price of before.keys())result.push({price,quantity:null});
  return result;
}
function orderChanged(a:Level[],b:Level[]):boolean{return a.length!==b.length||a.some((x,i)=>x.price!==b[i]!.price);}
/** Exact captured observations in a lossless transport encoding; no synthetic
 * time steps, fill assumptions, interpolation or economic calculations. */
export class BookEvidenceEncoder {
  private readonly states=new Map<string,State>();
  clear(){this.states.clear();}
  forget(token:string){this.states.delete(token);}
  encode(snapshot:MarketSnapshot,bucket:number,epoch:number,sync:boolean):BookEvidence{
    const old=this.states.get(snapshot.tokenId);
    const {bids,asks,...header}=snapshot;
    const full=!old||old.bucket!==bucket||old.epoch!==epoch||old.sync!==sync
      ||old.snapshot.marketId!==snapshot.marketId||old.snapshot.outcome!==snapshot.outcome
      ||new Set(bids.map(x=>x.price)).size!==bids.length||new Set(asks.map(x=>x.price)).size!==asks.length;
    const result:BookEvidence=full?{encoding:'FULL',snapshot}:{encoding:'DELTA',baseEventId:old!.snapshot.eventId,snapshot:header,
      bidsDelta:changes(old!.snapshot.bids,bids),asksDelta:changes(old!.snapshot.asks,asks),
      ...(orderChanged(old!.snapshot.bids,bids)?{bidOrder:bids.map(x=>x.price)}:{}),
      ...(orderChanged(old!.snapshot.asks,asks)?{askOrder:asks.map(x=>x.price)}:{})};
    // Copy depth because adapters may reuse their current normalized arrays.
    this.states.set(snapshot.tokenId,{snapshot:{...snapshot,bids:bids.map(x=>({...x})),asks:asks.map(x=>({...x}))},bucket,epoch,sync});
    while(this.states.size>256)this.states.delete(this.states.keys().next().value!);
    return result;
  }
}
export class BookEvidenceDecoder {
  private readonly states=new Map<string,MarketSnapshot>();
  apply(record:BookEvidence):MarketSnapshot{
    if(record.encoding==='FULL'){this.states.set(record.snapshot.tokenId,structuredClone(record.snapshot));return record.snapshot;}
    const old=this.states.get(record.snapshot.tokenId);
    if(!old||old.eventId!==record.baseEventId)throw Error('BOOK_EVIDENCE_BASE_MISSING');
    const levels=(before:Level[],delta:Change[],order?:string[]):Level[]=>{
      const map=new Map(before.map(x=>[x.price,x.quantity]));
      for(const x of delta){if(x.quantity===null)map.delete(x.price);else map.set(x.price,x.quantity);}
      const keys=order??before.map(x=>x.price);
      if(keys.length!==map.size||keys.some(x=>!map.has(x)))throw Error('BOOK_EVIDENCE_ORDER_INVALID');
      return keys.map(price=>({price,quantity:map.get(price)!}));
    };
    const snapshot={...record.snapshot,bids:levels(old.bids,record.bidsDelta,record.bidOrder),asks:levels(old.asks,record.asksDelta,record.askOrder)};
    this.states.set(snapshot.tokenId,snapshot);return snapshot;
  }
}

import type { MarketSnapshot } from "@pm/core";
type Level=MarketSnapshot['bids'][number];
type Change={price:string;quantity:string|null};
export type BookEvidence = {encoding:'FULL';snapshot:MarketSnapshot} | {
  encoding:'DELTA';baseEventId:string;snapshot:Omit<MarketSnapshot,'bids'|'asks'>;
  bidsDelta:Change[];asksDelta:Change[];bidOrder?:string[];askOrder?:string[];
} | {
  encoding:'DELTA_V2';baseEventId:string;snapshot:Partial<Omit<MarketSnapshot,'bids'|'asks'>>&{tokenId:string};
  removedHeaderFields:string[];bidsDelta:Change[];asksDelta:Change[];bidOrder?:string[];askOrder?:string[];
};
type Header=Omit<MarketSnapshot,'bids'|'asks'>;
type State={header:Header;bids:Map<string,string>;asks:Map<string,string>;bidOrder:string[];askOrder:string[];duplicates:boolean;bucket:number;epoch:number;sync:boolean};
/** Exact captured observations in a lossless transport encoding; no synthetic
 * time steps, fill assumptions, interpolation or economic calculations. */
export class BookEvidenceEncoder {
  private readonly states=new Map<string,State>();
  clear(){this.states.clear();}
  forget(token:string){this.states.delete(token);}
  encode(snapshot:MarketSnapshot,bucket:number,epoch:number,sync:boolean):BookEvidence{
    const old=this.states.get(snapshot.tokenId);
    const {bids,asks,...header}=snapshot;
    // Keep primitive maps rather than repeatedly cloning and rescanning every
    // unchanged depth level. These maps never retain mutable adapter arrays.
    const diff=(levels:Level[],before:Map<string,string>|undefined)=>{
      const map=new Map<string,string>(),order:string[]=[],delta:Change[]=[];
      for(const level of levels){map.set(level.price,level.quantity);order.push(level.price);
        if(before?.get(level.price)!==level.quantity)delta.push({price:level.price,quantity:level.quantity});}
      if(before)for(const price of before.keys())if(!map.has(price))delta.push({price,quantity:null});
      return {map,order,delta};
    };
    const bid=diff(bids,old?.bids),ask=diff(asks,old?.asks);
    const duplicates=bid.map.size!==bids.length||ask.map.size!==asks.length;
    const full=!old||old.bucket!==bucket||old.epoch!==epoch||old.sync!==sync||old.duplicates||duplicates
      ||old.header.marketId!==snapshot.marketId||old.header.outcome!==snapshot.outcome;
    const changed:Record<string,unknown>={tokenId:snapshot.tokenId},removedHeaderFields:string[]=[];
    if(!full){
      const previous=old!.header as unknown as Record<string,unknown>;
      for(const [key,value] of Object.entries(header)){
        if(value===undefined){if(previous[key]!==undefined)removedHeaderFields.push(key);continue;}
        const before=previous[key];
        if(typeof value==='object'?JSON.stringify(value)!==JSON.stringify(before):value!==before)changed[key]=value;
      }
      for(const key of Object.keys(previous))if(!(key in header))removedHeaderFields.push(key);
    }
    const sameOrder=(a:string[],b:string[])=>a.length===b.length&&a.every((price,i)=>price===b[i]);
    const result:BookEvidence=full?{encoding:'FULL',snapshot}:{encoding:'DELTA_V2',baseEventId:old!.header.eventId,
      snapshot:changed as Header,removedHeaderFields,bidsDelta:bid.delta,asksDelta:ask.delta,
      ...(!sameOrder(old!.bidOrder,bid.order)?{bidOrder:bid.order}:{}),
      ...(!sameOrder(old!.askOrder,ask.order)?{askOrder:ask.order}:{})};
    this.states.set(snapshot.tokenId,{header:{...header,feeSchedule:header.feeSchedule?{...header.feeSchedule}:header.feeSchedule},
      bids:bid.map,asks:ask.map,bidOrder:bid.order,askOrder:ask.order,duplicates,bucket,epoch,sync});
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
    const {bids:_bids,asks:_asks,...previousHeader}=old;
    const header=record.encoding==='DELTA_V2'?{...previousHeader,...record.snapshot}:{...record.snapshot};
    if(record.encoding==='DELTA_V2')for(const key of record.removedHeaderFields)delete (header as Record<string,unknown>)[key];
    const snapshot={...header,bids:levels(old.bids,record.bidsDelta,record.bidOrder),asks:levels(old.asks,record.asksDelta,record.askOrder)} as MarketSnapshot;
    this.states.set(snapshot.tokenId,snapshot);return snapshot;
  }
}

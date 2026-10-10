import type {MarketSnapshot,Outcome} from "@pm/core";
import {PolymarketMarketDataAdapter,
  type FeedConnectionEvent,type MarketIngressGapEvent} from "@pm/market-data";
import {GuardianNormalizedBookBridge} from "./guardian-normalized-book-bridge.js";
import type {GuardianBookFreshness} from "./guardian-paired-book-channel.js";

/**
 * H3 single-observer, adapter-owned source: no EngineService callbacks,
 * no second Guardian process, no side-effect/wallet/order methods.
 * Reuses production Polymarket normalizer, bounded transport, per-token
 * sync/epochs. An external G2I runner owns EXACTLY ONE instance/adaptor.
 */
export interface GuardianObservedMarket {
  marketId:string;upTokenId:string;downTokenId:string;asset:string|null;
}
const deny=(reason:string):never=>{throw Error("G2H_STANDALONE_BOOK_"+reason);};
export class GuardianStandaloneBookObserver {
  private readonly bridges=new Map<string,GuardianNormalizedBookBridge>();
  private readonly byToken=new Map<string,string>();
  private readonly tokenPairs=new Map<string,{UP:string;DOWN:string}>();
  /** At most TWO current-epoch normalized books per market. */
  private readonly pending=new Map<string,Partial<Record<Outcome,{
    snapshot:MarketSnapshot;epoch:number;
  }>>>();
  private readonly tokens:string[]=[];
  private running=false;
  private connected=false;

  constructor(private readonly adapter:PolymarketMarketDataAdapter,
    markets:readonly GuardianObservedMarket[]){
    if(!markets||markets.length<1||markets.length>40)
      deny("INVALID_MARKET_COUNT");
    if(!adapter.usesBoundedMarketTransport())
      deny("UNBOUNDED_TRANSPORT");
    for(const m of markets){
      if(!m||this.bridges.has(m.marketId)||this.byToken.has(m.upTokenId)
        ||this.byToken.has(m.downTokenId)
        ||m.upTokenId===m.downTokenId)deny("DUPLICATE_MARKET_OR_TOKEN");
      const bridge=new GuardianNormalizedBookBridge({
        marketId:m.marketId,upTokenId:m.upTokenId,downTokenId:m.downTokenId,
      },{tokenTrust:tokenId=>({
        synchronized:this.adapter.isTokenSynchronized(tokenId),
        epoch:this.adapter.tokenEpoch(tokenId),
      })});
      this.bridges.set(m.marketId,bridge);
      this.tokenPairs.set(m.marketId,{UP:m.upTokenId,DOWN:m.downTokenId});
      this.byToken.set(m.upTokenId,m.marketId);
      this.byToken.set(m.downTokenId,m.marketId);
      this.tokens.push(m.upTokenId,m.downTokenId);
      adapter.registerManualToken(m.upTokenId,m.marketId,"UP",m.asset);
      adapter.registerManualToken(m.downTokenId,m.marketId,"DOWN",m.asset);
    }
    this.adapter.setTokenIds(this.tokens);
  }
  private invalidateAll(reason:"DISCONNECT"|"SOURCE_GAP"|"UNSYNC"|"OVERLOAD"){
    this.pending.clear();
    for(const bridge of this.bridges.values())bridge.invalidate(reason);
    if(reason==="DISCONNECT"||reason==="OVERLOAD")this.connected=false;
  }
  /** An aggregate shard disconnect is not a global book disconnect. */
  private invalidateUnsynchronized():number{
    let affected=0;
    for(const [marketId,bridge] of this.bridges){
      const {UP,DOWN}=this.tokenPairs.get(marketId)!;
      if(!this.adapter.isTokenSynchronized(UP)
        ||!this.adapter.isTokenSynchronized(DOWN)){
        this.pending.delete(marketId);
        bridge.invalidate("SOURCE_GAP");
        affected++;
      }
    }
    return affected;
  }
  private onConnection(event:FeedConnectionEvent){
    if(event.feed!=="polymarket")return;
    if(event.state==="CONNECTING"||event.state==="CONNECTED"){
      // Sharded facade reports aggregate CONNECTING until the last socket
      // connects. Other shards can already send verified normalized books.
      // Starting this read-only bridge does NOT declare any book READY.
      if(!this.connected){
        this.connected=true;
        this.pending.clear();
        for(const bridge of this.bridges.values())bridge.onConnected();
      }
    }else if(event.state==="DISCONNECTED"||event.state==="STALE"){
      // Sharded facade emits aggregate DISCONNECTED for ONE failed socket.
      // Per-token sync is already fenced by the real adapter's gap handler.
      // If no scoped invalidation evidence exists, fail closed globally.
      if(!this.connected||this.invalidateUnsynchronized()===0)
        this.invalidateAll("SOURCE_GAP");
    }else if(event.state==="STOPPED"){
      this.invalidateAll("DISCONNECT");
    }
  }
  private onGap(event:MarketIngressGapEvent){
    // Token-scoped gaps invalidate both outcomes of EACH affected market.
    // Unknown/empty scope can never be interpreted as no loss.
    if(event.tokenIds.length===0){this.invalidateAll("SOURCE_GAP");return;}
    let recognized=false;
    for(const tokenId of event.tokenIds){
      const marketId=this.byToken.get(tokenId);
      if(!marketId){this.invalidateAll("SOURCE_GAP");return;}
      this.pending.delete(marketId);
      this.bridges.get(marketId)!.invalidate("SOURCE_GAP");
      recognized=true;
    }
    if(!recognized)this.invalidateAll("SOURCE_GAP");
  }
  private onSnapshot(frame:MarketSnapshot):void{
    if(!this.connected)return;
    const bridge=this.bridges.get(frame.marketId);
    if(!bridge)return;
    const outcome=frame.outcome;
    const tokenId=this.byToken.get(frame.tokenId)===frame.marketId
      ?frame.tokenId:null;
    if(!tokenId){
      bridge.onNormalizedSnapshot(frame);
      this.pending.delete(frame.marketId);
      return;
    }
    const epoch=this.adapter.tokenEpoch(tokenId);
    if(!Number.isSafeInteger(epoch)||epoch<1){
      this.pending.delete(frame.marketId);
      bridge.invalidate("UNSYNC");
      return;
    }
    const held=this.pending.get(frame.marketId)??{};
    if(Object.values(held).some(x=>x&&this.adapter.tokenEpoch(x.snapshot.tokenId)!==x.epoch)){
      this.pending.delete(frame.marketId);
      bridge.invalidate("TOKEN_ROTATION");
    }
    const next=this.pending.get(frame.marketId)??{};
    next[outcome]={snapshot:structuredClone(frame),epoch};
    this.pending.set(frame.marketId,next);
    if(!next.UP||!next.DOWN
      ||!this.adapter.isTokenSynchronized(next.UP.snapshot.tokenId)
      ||!this.adapter.isTokenSynchronized(next.DOWN.snapshot.tokenId)
      ||this.adapter.tokenEpoch(next.UP.snapshot.tokenId)!==next.UP.epoch
      ||this.adapter.tokenEpoch(next.DOWN.snapshot.tokenId)!==next.DOWN.epoch)return;
    bridge.onNormalizedSnapshot(next.UP.snapshot);
    bridge.onNormalizedSnapshot(next.DOWN.snapshot);
  }
  async run(signal:AbortSignal):Promise<void>{
    if(this.running)deny("SECOND_RUN");
    if(signal.aborted)deny("ABORTED");
    this.running=true;
    try{
      await this.adapter.run(this.tokens,{
        onSnapshot:frame=>{
          if(signal.aborted)return;
          this.onSnapshot(frame);
        },
        onConnection:event=>this.onConnection(event),
        onError:()=>{
          // If the source supplies a token-scoped gap, keep healthy shards.
          // Unknown/unattributable faults must still block every market.
          if(!this.connected||this.invalidateUnsynchronized()===0)
            this.invalidateAll("SOURCE_GAP");
        },
        onGap:event=>this.onGap(event),
      },signal);
    }finally{
      this.invalidateAll("DISCONNECT");
      this.running=false;
    }
  }
  observe(marketId:string,limits:GuardianBookFreshness){
    const bridge=this.bridges.get(marketId);
    if(!bridge)return deny("UNKNOWN_MARKET");
    const {UP,DOWN}=this.tokenPairs.get(marketId)!;
    // Callback delivery is not a security boundary. Even if a gap or
    // epoch notification is delayed/lost, an unsynchronized token cannot
    // leave a cached previously READY book trusted.
    const held=this.pending.get(marketId);
    if(held&&(held.UP&&this.adapter.tokenEpoch(UP)!==held.UP.epoch
      ||held.DOWN&&this.adapter.tokenEpoch(DOWN)!==held.DOWN.epoch)){
      this.pending.delete(marketId);
      bridge.invalidate("TOKEN_ROTATION");
    }else if((!this.adapter.isTokenSynchronized(UP)
      ||!this.adapter.isTokenSynchronized(DOWN))
      &&bridge.status==="ACTIVE"){
      // An ACTIVE pair cannot remain READY if either source loses sync.
      // AWAITING/DISCONNECTED may already hold one FRESH synced side:
      // retain that side until its counterpart arrives after reconnect.
      this.pending.delete(marketId);
      bridge.invalidate("UNSYNC");
    }
    return bridge.observe(limits);
  }
  get isRunning():boolean{return this.running;}
  get marketCount():number{return this.bridges.size;}
  get ingressStats(){return this.adapter.ingressSnapshot();}
}

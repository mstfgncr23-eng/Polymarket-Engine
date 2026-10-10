import type { MarketSnapshot, Outcome } from "@pm/core";
import {
  advanceGuardianBookChannel, beginGuardianBookGeneration, createGuardianBookChannel,
  readGuardianPairedBook, type GuardianBookChannel, type GuardianBookFreshness,
} from "./guardian-paired-book-channel.js";

/**
 * G2H purely local bridge for the EXISTING production-used Polymarket
 * normalizer's complete MarketSnapshot and per-token sync epochs.
 *
 * The input MUST come from the adapter's trusted onSnapshot callback AFTER
 * commitFullBook/commitPriceChange, not from raw websocket JSON/partial delta.
 * No socket, timer, publish queue, extra Guardian, or economic action.
 */
export interface GuardianNormalizedTokenTrust {
  synchronized: boolean;
  /** Current per-token epoch owned by the existing market-data adapter. */
  epoch: number;
}
export interface GuardianTokenTrustReader {
  tokenTrust(tokenId: string): GuardianNormalizedTokenTrust;
}
export class GuardianNormalizedBookBridge {
  private channel: GuardianBookChannel;
  private connected=false;
  private tokenEpochs:Partial<Record<Outcome,number>>={};
  constructor(input:{marketId:string;upTokenId:string;downTokenId:string},
    private readonly adapter:GuardianTokenTrustReader) {
    this.channel=createGuardianBookChannel(input);
  }
  get generation():number {return this.channel.generation;}
  get status():GuardianBookChannel["status"] {return this.channel.status;}
  get ingressSequence():number {return this.channel.ingressSequence;}

  /** Called only on transport CONNECTED after the initial trusted subscription. */
  onConnected():void {
    this.connected=true;
    this.tokenEpochs={};
    this.channel=beginGuardianBookGeneration(this.channel);
  }

  /** Transport loss, overflow or token synchronization gap invalidates BOTH. */
  invalidate(reason:"DISCONNECT"|"SOURCE_GAP"|"TOKEN_ROTATION"|"UNSYNC"|"OVERLOAD"):void {
    if (this.channel.status!=="DISCONNECTED" && this.channel.status!=="HALTED") {
      this.channel=advanceGuardianBookChannel(this.channel,{
        kind:"INVALIDATE",generation:this.channel.generation,
        ingressSequence:this.channel.ingressSequence+1,reason,
      });
    }
    this.connected=reason!=="DISCONNECT"&&reason!=="OVERLOAD";
    this.tokenEpochs={};
  }
  onNormalizedSnapshot(snapshot:MarketSnapshot):void {
    if (!this.connected || this.channel.status==="HALTED") return;
    if (snapshot.marketId!==this.channel.marketId
      || this.channel.tokenIds[snapshot.outcome]!==snapshot.tokenId) {
      // Unrelated markets don't poison this channel. Mismatched canonical
      // token for a claimed market/outcome IS a trust boundary violation.
      if (snapshot.marketId===this.channel.marketId) this.invalidate("TOKEN_ROTATION");
      return;
    }
    let up:GuardianNormalizedTokenTrust,down:GuardianNormalizedTokenTrust;
    try {
      up=this.adapter.tokenTrust(this.channel.tokenIds.UP);
      down=this.adapter.tokenTrust(this.channel.tokenIds.DOWN);
    } catch {this.invalidate("UNSYNC");return;}
    if (!up.synchronized||!down.synchronized
      || !Number.isSafeInteger(up.epoch)||up.epoch<1
      || !Number.isSafeInteger(down.epoch)||down.epoch<1) {
      this.invalidate("UNSYNC");return;
    }
    if (this.tokenEpochs.UP!==undefined && this.tokenEpochs.UP!==up.epoch
      || this.tokenEpochs.DOWN!==undefined && this.tokenEpochs.DOWN!==down.epoch) {
      this.invalidate("TOKEN_ROTATION");
    }
    if (this.channel.status==="DISCONNECTED") this.channel=beginGuardianBookGeneration(this.channel);
    if (this.channel.status==="HALTED") return;
    this.tokenEpochs={UP:up.epoch,DOWN:down.epoch};
    this.channel=advanceGuardianBookChannel(this.channel,{
      kind:"FULL_BOOK",generation:this.channel.generation,
      ingressSequence:this.channel.ingressSequence+1,synchronized:true,
      snapshot,
    });
  }
  observe(limits:GuardianBookFreshness) {return readGuardianPairedBook(this.channel,limits);}
}

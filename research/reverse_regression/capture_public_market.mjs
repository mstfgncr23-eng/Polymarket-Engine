#!/usr/bin/env node
// Forensic public market/reference capture. No production credentials or engine access.
// Exit when bounded deadline or bytes/events cap reached. Node >=22.
import { createHash } from "node:crypto";
import { writeFileSync, mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { performance } from "node:perf_hooks";

const output = process.argv[2];
const deadlineMs = Number(process.argv[3] || 25000);
if (!output || !Number.isInteger(deadlineMs) || deadlineMs < 1000 || deadlineMs > 60000) {
  console.error("usage: capture_public_market.mjs <output.jsonl> <1000-60000ms>");
  process.exit(2);
}
const MAX_EVENTS=2500, MAX_BYTES=3*1024*1024, MAX_ONE=140000;
const assets=["BTC","ETH","SOL","XRP","DOGE","BNB","HYPE","ZEC"];
const sockets=[];
const data=[];
const t0=performance.now();
let bytes=0, rejected=0, sealed=false;
const counts={reference:0,clob:0,connect:0,close:0,error:0};
const append=(obj) => {
  if(sealed) return;
  const row={offsetMs:Math.round(performance.now()-t0),...obj};
  const encoded=JSON.stringify(row);
  const b=Buffer.byteLength(encoded,"utf8")+1;
  if(b>MAX_ONE || data.length>=MAX_EVENTS || bytes+b>MAX_BYTES){rejected++;return;}
  data.push(encoded);bytes+=b;
  if(obj.channel in counts)counts[obj.channel]++;
};
const on=(socket,channel,asset)=>{
  socket.addEventListener("open",()=>append({channel:"connect",feed:channel,asset}));
  socket.addEventListener("message",(evt)=>{
    const value=typeof evt.data==="string"?evt.data:String(evt.data??"");
    append({channel,asset,data:value});
  });
  socket.addEventListener("error",()=>append({channel:"error",feed:channel,asset}));
  socket.addEventListener("close",(e)=>append({channel:"close",feed:channel,asset,code:e.code,wasClean:e.wasClean,reason:String(e.reason||"").slice(0,100)}));
  sockets.push(socket);
};
for(const asset of assets){
  const w=new WebSocket("wss://data-stream.binance.vision/ws/"+asset.toLowerCase()+"usdt@aggTrade");
  on(w,"reference",asset);
}
let clobAssets=[];
const marketPairs=[];
try{
 const res=await fetch("https://gamma-api.polymarket.com/markets?closed=false&active=true&limit=60",
    {signal:AbortSignal.timeout(8000)});
 if(!res.ok)throw Error("Gamma HTTP "+res.status);
 const markets=await res.json();
 for(const m of markets){
  let ids=m.clobTokenIds??m.clob_token_ids;
  if(typeof ids==="string"){try{ids=JSON.parse(ids)}catch{ids=[]}}
  if(Array.isArray(ids) && ids.length===2 && ids.every(v=>typeof v==="string" && /^[0-9]+$/.test(v))){
   const marketId=String(m.conditionId??m.condition_id??m.id??"");
   if(!marketPairs.some(pair=>pair.marketId===marketId))marketPairs.push({marketId,tokenIds:ids});
   if(marketPairs.length===4)break;
  }
 }
 clobAssets=marketPairs.flatMap(pair=>pair.tokenIds);
 append({channel:"meta",feed:"clob",asset:"public-market",markets:marketPairs});
 if(clobAssets.length){
   const ws=new WebSocket("wss://ws-subscriptions-clob.polymarket.com/ws/market");
   on(ws,"clob","public-market");
   ws.addEventListener("open",()=>ws.send(JSON.stringify({assets_ids:clobAssets,type:"market",custom_feature_enabled:false})));
 }else append({channel:"error",feed:"gamma",asset:"public-market",detail:"NO_ACTIVE_PAIR"});
}catch(e){append({channel:"error",feed:"gamma",asset:"public-market",detail:String(e.message).slice(0,100)});}
await new Promise(resolve=>setTimeout(resolve,deadlineMs));
sealed=true;
for(const w of sockets){try{w.close(1000,"capture complete")}catch{}}
mkdirSync(dirname(resolve(output)),{recursive:true});
writeFileSync(resolve(output),data.join("\n")+"\n",{mode:0o600,flag:"wx"});
const digest=createHash("sha256").update(data.join("\n")+"\n").digest("hex");
console.log(JSON.stringify({capture:"PUBLIC_ONLY",sha256:digest,rows:data.length,bytes,counts,rejected,assets,
  clobTokenCount:clobAssets.length,elapsedMs:Math.round(performance.now()-t0)}));

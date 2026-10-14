#!/usr/bin/env python3
"""Read-only, bounded, crash-recoverable PAPER soak market-data observer.

Every second persists all subscribed token timestamps and best quotes, including
WebSocket callback time (when instrumented). Native directions MUST be sourced
from canonical CONSENSUS_DECISION journal in a separate post-soak join. This
observer's implied book bias is NOT an Engine directional signal.
"""
import argparse
import collections
import datetime as dt
import gzip
import json
import os
import pathlib
import re
import subprocess
import time
import urllib.error
import urllib.request

UTC = dt.timezone.utc
def utc_now():
    return dt.datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
def timestamp(value):
    try:
        return dt.datetime.fromisoformat(str(value).replace("Z","+00:00"))
    except (TypeError, ValueError):
        return None
def seconds_between(later, earlier):
    a,b=timestamp(later),timestamp(earlier)
    return round((a-b).total_seconds(),3) if a and b else None
def get_env():
    container=json.loads(subprocess.check_output(["sudo","-n","docker","inspect","polymarket-paper-engine"],timeout=10))[0]
    return dict(v.split("=",1) for v in container["Config"].get("Env",[]) if "=" in v)
def http_json(url, headers=None, timeout=5):
    req=urllib.request.Request(url,headers=headers or {})
    with urllib.request.urlopen(req,timeout=timeout) as resp:
        return json.load(resp)
def classify_start(market):
    """Never use Gamma startDate (listing creation) as contract fixing start."""
    start=market.get("eventStartTime")
    slug=market.get("slug") or ""
    end=market.get("endDate")
    s,e=timestamp(start),timestamp(end)
    m=re.search(r"-updown-(5|15)m-(\d{10})$",slug)
    if not(s and e and m):
        return {"verified":False,"eventStartTime":start,"endDate":end,"slug":slug,"reason":"MISSING_WINDOW_METADATA"}
    slug_start=dt.datetime.fromtimestamp(int(m.group(2)), UTC)
    length=int(m.group(1))*60
    if abs((s-slug_start).total_seconds())>1 or abs((e-s).total_seconds()-length)>1:
        return {"verified":False,"eventStartTime":start,"endDate":end,"slug":slug,"reason":"WINDOW_METADATA_CONFLICT"}
    return {"verified":True,"eventStartTime":start,"endDate":end,"slug":slug,"reason":"GAMMA_EVENT_START_SLUG_END_MATCH","durationSec":length}

def run(args):
    out=pathlib.Path(args.output).resolve()
    out.mkdir(parents=True,exist_ok=False)
    files={}
    def append(name,obj):
        f=files.get(name)
        if f is None:
            f=(out/(name+".jsonl")).open("a",encoding="utf-8",buffering=1)
            files[name]=f
        f.write(json.dumps(obj,ensure_ascii=False,separators=(",",":"))+"\n")
    env=get_env()
    if env.get("ENGINE_READ_TOKEN") is None:raise RuntimeError("ENGINE_READ_TOKEN unavailable")
    auth={"Authorization":"Bearer "+env["ENGINE_READ_TOKEN"]}
    base="http://127.0.0.1:18080"
    market_meta={}
    meta_pending=collections.deque()
    seen_feed=set()
    retained_events=0
    canonical_events=0
    canonical_known=set()
    canonical_types={"CONSENSUS_DECISION","RISK_EVALUATION","PAPER_ORDER_CREATED",
                     "FIRST_LEG_FILLED","PAPER_FILL","POSITION_UPDATED","POSITION_SETTLED",
                     "POSITION_WIN","POSITION_LOSS","POSITION_BREAKEVEN","PAIR_REALIZED",
                     "EXPERIMENT_STARTED","EXPERIMENT_START_VALIDATED",
                     "EXPERIMENT_ENTRY_CLOSED","EXPERIMENT_COMPLETED"}
    last_canonical_at=-1e10
    snapshots=0
    last_error=None
    start_mon=time.monotonic()
    deadline=start_mon+args.seconds
    last_dashboard_at=0.0
    last_metadata_at=0.0
    last_heartbeat_at=0.0
    experiment_id=None
    current_start_status=None
    append("manifest",{"createdAt":utc_now(),"version":"pre-soak-audit-v1","intervalSec":args.interval,"durationSec":args.seconds,
                       "host":subprocess.getoutput("hostname"),
                       "windowsSource":"Gamma eventStartTime verified against unix-slug and endDate; not startDate",
                       "nativeDirection":"From canonical CONSENSUS_DECISION only; price-implied bias is separate",
                       "wsCallbackAt":"Optional, absent means no raw callback time; never backfill"})
    try:
        while time.monotonic()<deadline:
            tick=time.monotonic()
            now=utc_now()
            try:
                shadow=http_json(base+"/api/shadow/guardian-state",auth)
                positions=shadow.get("positions") or []
                exp=shadow.get("experiment") or {}
                if experiment_id is None and exp.get("status") in ("RUNNING","ENTRY_CLOSED"):
                    experiment_id=exp.get("id")
                    append("experiments",{"observedAt":now,"experiment":exp})
                if experiment_id and exp.get("id")!=experiment_id:
                    raise RuntimeError("EXPERIMENT_ID_CHANGED")
                tokens=shadow.get("tokenMap") or []
                for t in tokens:
                    market_id=t.get("marketId")
                    if market_id and market_id not in market_meta and market_id not in meta_pending:
                        meta_pending.append(market_id)
                book_rows=[]
                for t in tokens:
                    if not isinstance(t,dict):continue
                    book_rows.append({
                        "marketId":t.get("marketId"),"tokenId":t.get("tokenId"),"outcome":t.get("outcome"),
                        "sourceTimestamp":t.get("sourceTimestamp"),"wsCallbackAt":t.get("wsCallbackAt"),
                        "receivedAt":t.get("receivedAt"),
                        "sourceAgeMs":int(v*1000) if (v:=seconds_between(now,t.get("sourceTimestamp"))) is not None else None,
                        "receiveAgeMs":int(v*1000) if (v:=seconds_between(now,t.get("receivedAt"))) is not None else None,
                        "queueAgeMs":int(v*1000) if (v:=seconds_between(t.get("receivedAt"),t.get("wsCallbackAt"))) is not None else None,
                    })
                append("books",{"observedAt":now,"experimentId":exp.get("id"),"status":exp.get("status"),"books":book_rows})
                snapshots+=1
                # Positions sampled independently of trading events. Journal remains source of truth for fills.
                if positions:
                    append("positions",{"observedAt":now,"positions":positions})
                    for pos in positions:
                        m=str(pos.get("marketId") or "")
                        spec=market_meta.get(m,{}).get("spec",{})
                        if spec.get("verified"):
                            start=spec.get("eventStartTime")
                            opened=pos.get("openedAt")
                            if opened and seconds_between(opened,start) is not None and seconds_between(opened,start)<0:
                                append("early_position_samples",{
                                    "observedAt":now,"marketId":m,"openedAt":opened,"initialOutcome":pos.get("unpairedOutcome"),
                                    "secondsFromWindowStart":seconds_between(now,start),"start":start,
                                    "books":[row for row in book_rows if row.get("marketId")==m],
                                    "note":"NO_NATIVE_SIGNAL_INFERRED_FROM_BOOK_AGE"})
                if tick-last_dashboard_at>=args.dashboard_interval:
                    dash=http_json(base+"/api/dashboard",auth,timeout=8)
                    last_dashboard_at=tick
                    quotes={}
                    for row in dash.get("scanner") or []:
                        m=row.get("marketId")
                        quotes.setdefault(str(m),{})[row.get("outcome")]={
                            "bid":row.get("bid"),"ask":row.get("ask"),"lastMessageAt":row.get("lastMessageAt")}
                    append("quotes",{"observedAt":now,"quotes":quotes})
                    for event in reversed(dash.get("decisionFeed") or []):
                        event_id=event.get("id")
                        if event_id and event_id not in seen_feed:
                            append("feed",event)
                            seen_feed.add(event_id)
                            retained_events+=1
                    # A bounded UI feed can overrun; journal is canonical for full direction history.
                    if len(seen_feed)>15000:seen_feed=set(list(seen_feed)[-7000:])
                if tick-last_canonical_at>=args.canonical_interval:
                    historical=http_json(base+"/api/history/decisions?limit=500",auth,timeout=10)
                    fetched=historical.get("events") or []
                    for record in fetched:
                        if record.get("type") not in canonical_types:continue
                        if experiment_id:
                            pid=(record.get("payload") or {}).get("experimentId")
                            # Keep no-id sources too; all may be linked by market, trace and time.
                            if pid not in (None,experiment_id):continue
                        event_id=record.get("id")
                        if event_id and event_id not in canonical_known:
                            append("canonical_events",record)
                            canonical_known.add(event_id)
                            canonical_events+=1
                    # Alarm when the 500-row return window may be hiding data.
                    if len(fetched)>=500:
                        old=min((timestamp(e.get("occurredAt")) for e in fetched if timestamp(e.get("occurredAt"))),default=None)
                        if old and (dt.datetime.now(UTC)-old).total_seconds()<args.canonical_interval:
                            append("errors",{"observedAt":now,"kind":"CANONICAL_WINDOW_TRUNCATION_RISK",
                                             "count":len(fetched),"oldest":old.isoformat()})
                    last_canonical_at=tick
                if meta_pending and tick-last_metadata_at>=args.metadata_interval:
                    m=meta_pending.popleft()
                    try:
                        item=http_json("https://gamma-api.polymarket.com/markets/"+m,{"User-Agent":"PM-paper-research-observer/1.0"},timeout=6)
                        spec=classify_start(item)
                        market_meta[m]={"observedAt":now,"spec":spec}
                        append("market_specs",{"observedAt":now,"marketId":m,"spec":spec})
                    except Exception as ex:
                        append("errors",{"observedAt":now,"kind":"GAMMA_ERROR","marketId":m,"reason":str(ex)[:180]})
                        if m not in meta_pending:meta_pending.append(m)
                    last_metadata_at=tick
                if tick-last_heartbeat_at>=10:
                    lag_ms=int((time.monotonic()-tick)*1000)
                    append("heartbeat",{"observedAt":now,"experimentId":exp.get("id"),
                                        "experimentStatus":exp.get("status"),"tokens":len(book_rows),
                                        "metadata":len(market_meta),"wsStamped":sum(bool(t.get("wsCallbackAt")) for t in book_rows),
                                        "samples":snapshots,"uiEvents":retained_events,
                                        "canonicalEvents":canonical_events,"pollMs":lag_ms})
                    last_heartbeat_at=tick
                last_error=None
            except Exception as ex:
                last_error=str(ex)
                append("errors",{"observedAt":now,"kind":"OBSERVATION_FAILURE","reason":last_error[:250]})
                if "EXPERIMENT_ID_CHANGED" in last_error:raise
            remaining=args.interval-(time.monotonic()-tick)
            if remaining>0:time.sleep(remaining)
        append("manifest",{"endedAt":utc_now(),"status":"COLLECTION_COMPLETE","samples":snapshots,
                           "uiEvents":retained_events,"canonicalEvents":canonical_events,"verifiedMetadata":sum(1 for x in market_meta.values() if x.get("spec",{}).get("verified")),
                           "experimentId":experiment_id,"lastError":last_error})
    except BaseException as ex:
        append("manifest",{"endedAt":utc_now(),"status":"COLLECTION_ABORTED","error":str(ex)[:200],"experimentId":experiment_id})
        raise
    finally:
        for f in files.values():
            f.flush()
            os.fsync(f.fileno())
            f.close()

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--seconds",type=int,default=60)
    p.add_argument("--interval",type=float,default=1)
    p.add_argument("--dashboard-interval",type=float,default=2)
    p.add_argument("--metadata-interval",type=float,default=1)
    p.add_argument("--canonical-interval",type=float,default=5)
    p.add_argument("--output",required=True)
    a=p.parse_args()
    if a.seconds<1 or a.interval<0.25 or a.dashboard_interval<1:raise SystemExit("invalid duration/interval")
    run(a)
if __name__=="__main__":main()

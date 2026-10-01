#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from collections import Counter, defaultdict
from decimal import Decimal
from pathlib import Path

def read_rows(path: Path):
    if not path.exists():
        return []
    out=[]
    with path.open(encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if line:
                out.append(json.loads(line))
    return out

def main():
    p=argparse.ArgumentParser()
    p.add_argument("run_dir")
    args=p.parse_args()
    root=Path(args.run_dir)
    actions=read_rows(root/"guardian_actions.jsonl")
    latency=read_rows(root/"latency_samples.jsonl")
    events=read_rows(root/"engine_events.jsonl")
    ledger=read_rows(root/"ledger_samples.jsonl")

    pnls=[]
    reasons=Counter()
    for a in actions:
        sell=((a.get("economics") or {}).get("sell") or {})
        if sell.get("guaranteed_terminal_pnl") is not None:
            pnls.append(Decimal(str(sell["guaranteed_terminal_pnl"])))
        reasons[a.get("failure_reason")]+=1

    by_latency=defaultdict(list)
    for x in latency:
        sell=((x.get("economics") or {}).get("sell") or {})
        if sell.get("guaranteed_terminal_pnl") is not None:
            by_latency[int(x["target_latency_ms"])].append(Decimal(str(sell["guaranteed_terminal_pnl"])))

    order_intent={}
    fills=Counter()
    for rec in events:
        e=rec.get("event") or {}
        d=e.get("detail") or {}
        if e.get("type")=="PAPER_ORDER_CREATED" and d.get("orderId"):
            order_intent[d["orderId"]]=d.get("intent")
        elif e.get("type")=="PAPER_FILL":
            intent=order_intent.get(d.get("orderId"))
            if intent:
                fills[intent]+=1

    net_delta=None
    if ledger:
        a=(ledger[0].get("balance") or {}).get("netTradingPnl")
        b=(ledger[-1].get("balance") or {}).get("netTradingPnl")
        if a is not None and b is not None:
            net_delta=Decimal(str(b))-Decimal(str(a))

    lines=[
        "# Guardian 24h forward shadow summary","",
        f"- helper actions: **{len(actions)}**",
        f"- helper failure reasons: **{dict(reasons)}**",
        f"- worst helper guaranteed PnL: **{min(pnls) if pnls else 'N/A'}**",
        f"- helper actions <= -$4: **{sum(1 for x in pnls if x <= Decimal('-4'))}**",
        f"- observed actual PAPER fill intents: **{dict(fills)}**",
    ]
    if net_delta is not None:
        lines.append(f"- engine netTradingPnl change over capture: **{net_delta}**")
    lines += ["","## Real-time latency samples",""]
    for ms in (25,50,100,150,250,500):
        vals=by_latency.get(ms,[])
        lines.append(
            f"- {ms} ms: samples={len(vals)}, worst={min(vals) if vals else 'UNKNOWN'}, "
            f"meteors={sum(1 for x in vals if x <= Decimal('-4')) if vals else 'UNKNOWN'}"
        )
    lines += [
        "",
        "## Evidence boundary","",
        "This is contemporaneous shadow evidence. Final strategy-level PnL must be regenerated "
        "from the canonical experiment archive plus these raw CLOB files. Missing state is never "
        "converted into a fill or a meteor.",
    ]
    text="\n".join(lines)+"\n"
    (root/"FORWARD_SHADOW_SUMMARY.md").write_text(text,encoding="utf-8")
    print(text)

if __name__=="__main__":
    main()

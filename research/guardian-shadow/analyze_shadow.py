#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter, defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Any

LATENCIES = (25, 50, 100, 150, 250, 500)


def read_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    out: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def verify_checksums(root: Path) -> list[str]:
    sums = root / "SHA256SUMS.txt"
    if not sums.exists():
        return ["SHA256SUMS_MISSING"]
    failures: list[str] = []
    for raw in sums.read_text(encoding="utf-8").splitlines():
        if not raw.strip():
            continue
        try:
            expected, name = raw.split("  ", 1)
        except ValueError:
            failures.append("SHA256SUMS_MALFORMED")
            continue
        path = root / name
        if not path.exists():
            failures.append(f"SHA256_MISSING:{name}")
            continue
        observed = sha256_file(path)
        if observed != expected:
            failures.append(f"SHA256_MISMATCH:{name}")
    return failures


def incomplete(root: Path, reasons: list[str]) -> None:
    lines = [
        "# Guardian 24h forward shadow",
        "",
        "**TEST INCOMPLETE**",
        "",
        "Per TEST_CONTRACT, no numeric conclusion is emitted from an incomplete run.",
        "",
        "Reasons:",
        *[f"- {reason}" for reason in sorted(set(reasons))],
        "",
    ]
    text = "\n".join(lines)
    (root / "FORWARD_SHADOW_SUMMARY.md").write_text(text, encoding="utf-8")
    print(text)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dir")
    args = parser.parse_args()
    root = Path(args.run_dir)

    reasons = verify_checksums(root)
    complete_path = root / "run_complete.json"
    if not complete_path.exists():
        reasons.append("RUN_COMPLETE_MISSING")
        incomplete(root, reasons)
        return

    try:
        run_complete = json.loads(complete_path.read_text(encoding="utf-8"))
    except Exception as exc:
        reasons.append(f"RUN_COMPLETE_INVALID:{exc!r}")
        incomplete(root, reasons)
        return

    if run_complete.get("status") != "COMPLETE":
        reasons.extend(str(x) for x in run_complete.get("incomplete_reasons", []))
        reasons.append(f"RUN_STATUS:{run_complete.get('status')}")
    if run_complete.get("experiment_status") != "COMPLETED":
        reasons.append(f"EXPERIMENT_STATUS:{run_complete.get('experiment_status')}")
    if run_complete.get("experiment_id") != run_complete.get("bound_experiment_id"):
        reasons.append("EXPERIMENT_ID_MISMATCH")
    if run_complete.get("paper_only") is not True:
        reasons.append("PAPER_SAFETY_NOT_PROVEN")
    if run_complete.get("final_open_positions") != 0:
        reasons.append(f"FINAL_OPEN_POSITIONS:{run_complete.get('final_open_positions')}")
    if run_complete.get("final_unpaired_positions") != 0:
        reasons.append(f"FINAL_UNPAIRED_POSITIONS:{run_complete.get('final_unpaired_positions')}")
    if run_complete.get("final_unresolved_position_count") != 0:
        reasons.append(f"FINAL_UNRESOLVED_POSITIONS:{run_complete.get('final_unresolved_position_count')}")
    try:
        if Decimal(str(run_complete.get("final_reserved_cash"))) != 0:
            reasons.append(f"FINAL_RESERVED_CASH:{run_complete.get('final_reserved_cash')}")
    except Exception:
        reasons.append("FINAL_RESERVED_CASH_INVALID")

    manifest_path = root / "manifest.json"
    contract_path = root / "TEST_CONTRACT.md"
    if not manifest_path.exists() or not contract_path.exists():
        reasons.append("MANIFEST_OR_CONTRACT_MISSING")
    else:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("contract_sha256") != sha256_file(contract_path):
            reasons.append("CONTRACT_HASH_MISMATCH")
        if manifest.get("source_git_sha") != run_complete.get("source_git_sha"):
            reasons.append("SOURCE_SHA_MISMATCH")

    actions = read_rows(root / "guardian_actions.jsonl")
    latency = read_rows(root / "latency_samples.jsonl")
    expected_latency = len(actions) * len(LATENCIES)
    if len(latency) != expected_latency:
        reasons.append(f"LATENCY_SAMPLE_COUNT:{len(latency)}!={expected_latency}")

    action_ids = {str(a.get("action_id")) for a in actions}
    per_key: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for row in latency:
        aid = str(row.get("action_id"))
        try:
            ms = int(row.get("target_latency_ms"))
        except Exception:
            reasons.append("LATENCY_ROW_MALFORMED")
            continue
        if aid not in action_ids or ms not in LATENCIES:
            reasons.append(f"LATENCY_ROW_UNEXPECTED:{aid}:{ms}")
            continue
        per_key[(aid, ms)].append(row)

    for aid in action_ids:
        for ms in LATENCIES:
            rows = per_key.get((aid, ms), [])
            if len(rows) != 1:
                reasons.append(f"LATENCY_CARDINALITY:{aid}:{ms}:{len(rows)}")

    if reasons:
        incomplete(root, reasons)
        return

    pnls: list[Decimal] = []
    failure_reasons: Counter[str] = Counter()
    for action in actions:
        sell = ((action.get("economics") or {}).get("sell") or {})
        value = sell.get("guaranteed_terminal_pnl")
        if value is None:
            incomplete(root, ["ACTION_SELL_PNL_MISSING"])
            return
        pnls.append(Decimal(str(value)))
        failure_reasons[str(action.get("failure_reason"))] += 1

    engine_events = read_rows(root / "engine_events.jsonl")
    order_intent: dict[str, str] = {}
    fills: Counter[str] = Counter()
    # Authority polls are newest-first while SSE is chronological, so event
    # ingestion order is intentionally not trusted. Resolve order intent in a
    # first pass, then attribute fills in a second pass.
    for rec in engine_events:
        event = rec.get("event") or {}
        detail = event.get("detail") or {}
        if event.get("type") == "PAPER_ORDER_CREATED" and detail.get("orderId"):
            order_intent[str(detail["orderId"])] = str(detail.get("intent"))
    for rec in engine_events:
        event = rec.get("event") or {}
        detail = event.get("detail") or {}
        if event.get("type") == "PAPER_FILL":
            intent = order_intent.get(str(detail.get("orderId")))
            if intent:
                fills[intent] += 1

    ledger = read_rows(root / "ledger_samples.jsonl")
    baseline_net_delta: Decimal | None = None
    if ledger:
        first = (ledger[0].get("balance") or {}).get("netTradingPnl")
        last = (ledger[-1].get("balance") or {}).get("netTradingPnl")
        if first is not None and last is not None:
            baseline_net_delta = Decimal(str(last)) - Decimal(str(first))

    lines = [
        "# Guardian 24h forward shadow summary",
        "",
        "Run evidence status: **COMPLETE**",
        f"Experiment: **{run_complete.get('experiment_id')}**",
        f"Source SHA: **{run_complete.get('source_git_sha')}**",
        "",
        "## Decision-time helper evidence",
        "",
        f"- helper actions: **{len(actions)}**",
        f"- helper failure reasons: **{dict(failure_reasons)}**",
        f"- worst helper guaranteed terminal PnL: **{min(pnls) if pnls else 'N/A'}**",
        f"- decision-time helper actions <= -$4: **{sum(1 for value in pnls if value <= Decimal('-4'))}**",
        f"- actual PAPER fill intents observed by sidecar: **{dict(fills)}**",
    ]
    if baseline_net_delta is not None:
        lines.append(f"- actual PAPER engine netTradingPnl change over captured interval: **{baseline_net_delta}**")

    lines += ["", "## Contract-exact latency evidence", ""]
    for ms in LATENCIES:
        rows = [per_key[(aid, ms)][0] for aid in action_ids]
        unknown = [row for row in rows if row.get("status") != "KNOWN"]
        if unknown:
            lines.append(
                f"- {ms} ms: KNOWN={len(rows)-len(unknown)}/{len(rows)}, UNKNOWN={len(unknown)}; "
                "**full-policy delayed PnL = UNKNOWN; delayed meteor result = UNKNOWN**"
            )
            continue

        values: list[Decimal] = []
        malformed = False
        for row in rows:
            sell = ((row.get("economics") or {}).get("sell") or {})
            value = sell.get("guaranteed_terminal_pnl")
            if value is None:
                malformed = True
                break
            values.append(Decimal(str(value)))
        if malformed:
            lines.append(
                f"- {ms} ms: malformed KNOWN row; **full-policy delayed PnL = UNKNOWN; delayed meteor result = UNKNOWN**"
            )
        else:
            lines.append(
                f"- {ms} ms: KNOWN={len(values)}/{len(rows)}, helper-only worst={min(values) if values else 'N/A'}, "
                f"helper-only <=-$4={sum(1 for value in values if value <= Decimal('-4'))}. "
                "Full strategy PnL still requires canonical experiment archive replay."
            )

    lines += [
        "",
        "## Evidence boundary",
        "",
        "This summary never converts UNKNOWN into a fill or meteor. Decision-time helper economics "
        "are contemporaneous shadow calculations from full recorded ladders. Final strategy-level "
        "counterfactual PnL, NORMAL retention, new-meteor count, TRAIN/TEST comparison, and the "
        "historical GO/NO-GO rubric require the canonical experiment archive joined to this sealed evidence set.",
        "",
    ]
    text = "\n".join(lines)
    (root / "FORWARD_SHADOW_SUMMARY.md").write_text(text, encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()

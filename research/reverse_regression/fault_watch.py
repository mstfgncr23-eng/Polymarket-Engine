#!/usr/bin/env python3
"""Bounded failure-trigger observer for ISOLATED PAPER regression runs.

Offline: python fault_watch.py --fixture fixtures/reference_mass.jsonl
Live:    python fault_watch.py --confirm-isolated-paper --max-seconds 7200 -- python ...
Live execution is intentionally opt-in; no engine is started by default.
"""
import argparse
from collections import deque
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import queue
import re
import subprocess
import sys
import threading
import time

REF_RE = re.compile(r"REFERENCE_CLOSE:kind=([^:]+):code=([^:]+):wasClean=([^:]+):abort=([^:]+):reason=(.*)")
ASSET_RE = re.compile(r"reference:([A-Za-z0-9_-]+)")
MAX_LINE = 16384
MAX_EVIDENCE = 5000
MAX_TIME_WINDOW_MS = 120_000

def decode(line):
    try:
        record = json.loads(line)
        if isinstance(record, dict) and isinstance(record.get("log"), str):
            inner = json.loads(record["log"])
            if isinstance(inner, dict):
                return inner
        return record if isinstance(record, dict) else {"msg": str(record)}
    except (ValueError, TypeError):
        return {"msg": line.strip()[:MAX_LINE]}

def event_time(record, fallback):
    for key in ("time", "timestamp", "ts"):
        value = record.get(key)
        if isinstance(value, (int, float)):
            return int(value if value > 1e11 else value * 1000)
        if isinstance(value, str):
            try:
                return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)
            except ValueError:
                pass
    return fallback

def descriptions(reason):
    return {
        "REF_MASS": "At least seven distinct reference feeds closed within one second. The logged 1011 can be an application-generated code, not the upstream provider's response.",
        "REF_AND_CLOB": "Reference mass-close and CLOB stale coincide. The common initiating cause cannot be inferred from these log codes alone.",
        "DB_CONNECT_TIMEOUT": "PostgreSQL TCP connection deadline expired. Connection path, Node scheduling and server should be distinguished with additional evidence.",
        "FAIL_CLOSED": "The engine entered a fail-closed safety state. This is an observable safety action, not itself a diagnosis.",
    }.get(reason, "Unknown event type")

class Detector:
    def __init__(self):
        self.references = deque()
        self.clob = deque()
        self.events = deque(maxlen=MAX_EVIDENCE)
        self.lines_observed = 0
        self.evidence_dropped = 0
        self.last_ms = 0

    def note(self, line, now_ms=None):
        self.lines_observed += 1
        if len(line) > MAX_LINE:
            line = line[:MAX_LINE] + " <TRUNCATED>"
        record = decode(line)
        at = event_time(record, now_ms if now_ms is not None else int(time.time() * 1000))
        self.last_ms = max(at, self.last_ms)
        msg = str(record.get("msg", ""))
        err = record.get("err")
        err_text = err.get("message", "") if isinstance(err, dict) else str(err or "")
        feed = str(record.get("feed", ""))
        text = msg + " " + err_text
        while self.references and self.references[0][0] < at - 1000:
            self.references.popleft()
        while self.clob and self.clob[0] < at - 1000:
            self.clob.popleft()
        event = {
            "at_ms": at, "msg": msg[:256], "feed": feed[:100], "err": err_text[:420],
        }
        if len(self.events) == self.events.maxlen:
            self.evidence_dropped += 1
        self.events.append(event)
        close = REF_RE.search(text)
        if close and close.group(4).lower() == "false":
            found = ASSET_RE.search(feed)
            asset = found.group(1) if found else str(record.get("asset") or record.get("aggregateId") or "?")
            self.references.append((at, asset, close.group(1), close.group(2)))
        if "CLOB_MARKET_WS_STALE" in text:
            self.clob.append(at)
        if "CONNECT_TIMEOUT" in text and (
            "postgres" in text.lower() or "journal" in text.lower() or "5432" in text
            or "polymarket-postgres" in text.lower()
        ):
            return self.failure("DB_CONNECT_TIMEOUT", at)
        if "FAIL_CLOSED" in text and ("entered" in text.lower() or "critical" in msg.lower()):
            return self.failure("FAIL_CLOSED", at)
        assets = {x[1] for x in self.references if x[1] != "?"}
        if len(assets) >= 7:
            times = [x[0] for x in self.references]
            nearest_clob = min((abs(x - at) for x in self.clob), default=None)
            reason = "REF_AND_CLOB" if nearest_clob is not None and nearest_clob <= 20 else "REF_MASS"
            return self.failure(reason, at, {
                "reference_assets": sorted(assets),
                "reference_closures": len(self.references),
                "close_spread_ms": max(times) - min(times),
                "clob_stale_delta_ms": nearest_clob,
            })
        return None

    def failure(self, reason, at, extra=None):
        oldest = at - MAX_TIME_WINDOW_MS
        evidence = [x for x in self.events if x["at_ms"] >= oldest]
        return {
            "status": "FAULT_DETECTED",
            "reason": reason,
            "description": descriptions(reason),
            "time_utc": datetime.fromtimestamp(at / 1000, timezone.utc).isoformat(),
            "trigger_at_ms": at,
            "detail": extra or {},
            "evidence": evidence,
            "observed_lines": self.lines_observed,
            "bounded_evidence_overwrites": self.evidence_dropped,
            "limitations": [
                "Without native WebSocket errors, this report cannot identify an upstream close cause.",
                "Do not interpret no observed failure as a passed regression.",
            ],
        }

def persist(report, out):
    out.mkdir(parents=True, exist_ok=True)
    target = out / "incident.json"
    target.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    summary = out / "summary.txt"
    summary.write_text(
        "STATUS: " + report["status"] + "\n"
        + "REASON: " + str(report.get("reason", "none")) + "\n"
        + "DETAIL: " + str(report.get("description", "")) + "\n"
        + "EVIDENCE EVENTS: " + str(len(report.get("evidence", []))) + "\n"
        + "NOT A ROOT-CAUSE CLAIM.\n",
        encoding="utf-8",
    )
    print("EVIDENCE_WRITTEN", str(target), flush=True)

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--fixture", type=Path, help="Replay JSONL or raw Pino lines, no processes started")
    group.add_argument("--confirm-isolated-paper", action="store_true", help="Permit an explicit isolated PAPER test command")
    ap.add_argument("--out", type=Path, default=Path(__file__).parent / "runs" / "last")
    ap.add_argument("--max-seconds", type=int, default=7200)
    ap.add_argument("--grace-seconds", type=int, default=5)
    ap.add_argument("command", nargs=argparse.REMAINDER)
    args = ap.parse_args()
    if not 1 <= args.max_seconds <= 21600:
        ap.error("--max-seconds must be 1..21600; timeout without a fault is INCONCLUSIVE")
    observer = Detector()
    report = None
    if args.fixture:
        if args.command:
            ap.error("Fixture mode cannot execute a command")
        for line in args.fixture.open("r", encoding="utf-8"):
            report = observer.note(line)
            if report:
                break
        if report is None:
            report = {"status": "INCONCLUSIVE_NO_TARGET", "reason": None,
                      "description": "No watched fault in supplied finite fixture.",
                      "evidence": list(observer.events),
                      "observed_lines": observer.lines_observed}
    else:
        command = args.command[1:] if args.command and args.command[0] == "--" else args.command
        if not command:
            ap.error("Live mode requires an explicit command after --; NO automatic production command")
        if os.environ.get("PM_FORENSIC_ENV", "") != "isolated-paper":
            ap.error("Set PM_FORENSIC_ENV=isolated-paper after verifying independent DB, data and PAPER execution")
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, encoding="utf-8", errors="replace", bufsize=1)
        messages = queue.Queue(maxsize=10000)
        def pump():
            for line in proc.stdout:
                try:
                    messages.put_nowait(line)
                except queue.Full:
                    pass
            messages.put(None)
        thread = threading.Thread(target=pump, daemon=True)
        thread.start()
        end = time.monotonic() + args.max_seconds
        while time.monotonic() < end:
            try:
                line = messages.get(timeout=min(0.25, max(.01, end - time.monotonic())))
            except queue.Empty:
                if proc.poll() is not None and messages.empty():
                    break
                continue
            if line is None:
                break
            report = observer.note(line)
            if report:
                break
        if report is None:
            report = {"status": "INCONCLUSIVE_NO_TARGET", "reason": None,
                      "description": "Process exited or timed out without a target fault; no PASS claim.",
                      "process_returncode": proc.poll(),
                      "evidence": list(observer.events), "observed_lines": observer.lines_observed}
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=max(1,args.grace_seconds))
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=3)
    persist(report, args.out)
    print("TEST_STATUS",report["status"],"TRIGGER",report.get("reason"), flush=True)
    return 10 if report["status"] == "FAULT_DETECTED" else 2

if __name__ == "__main__":
    sys.exit(main())

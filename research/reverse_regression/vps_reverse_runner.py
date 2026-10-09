#!/usr/bin/env python3
"""VPS-only, bounded PAPER reference regression runner. No production modification."""
import argparse
from collections import deque
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import time

IMAGES = {
 "safe": "polymarket-engine:guardian65-21a195a6",
 "step2": "polymarket-engine:guardian-step2-21a195a6",
 "step3": "polymarket-engine:guardian-step3-21a195a6",
 "identity": "polymarket-engine:prepared-identity-candidate-20261008",
 "cpu1": "polymarket-engine:cpu-hotpath-v1-20261008",
 "cpu2": "polymarket-engine:cpu-hotpath-v2-quarantine-drainfix-20261008",
 "cpu3": "polymarket-engine:cpu-hotpath-v3-quarantine-diagnostics-20261008",
 "current": "polymarket-engine:no-source-seal-v1-20261008",
}
NETWORK = "pm-forensic-net-20261009"
PROD_CONTAINER = "polymarket-engine-no-seal"

def docker(*args, timeout=15):
    return subprocess.run(["sudo", "-n", "docker", *args], text=True,
                          capture_output=True, timeout=timeout)

def container_arguments(stage, container):
    return [
        "run", "-d", "--name", container, "--network", NETWORK,
        "--cpuset-cpus", "1", "--cpu-shares", "128",
        "--memory", "512m", "--memory-swap", "512m", "--pids-limit", "80",
        "--restart", "no", "--log-opt", "max-size=6m", "--log-opt", "max-file=2",
        "--security-opt", "no-new-privileges",
        "-e", "NODE_ENV=development", "-e", "TRADING_MODE=paper",
        "-e", "JOURNAL_MODE=memory", "-e", "GROK_ACTIVATION_PROVIDER=noop",
        "-e", "ENGINE_REGION=fra", "-e", "MAX_QUALIFIED_MARKETS=40",
        "-e", "LOCAL_ARCHIVE_ENABLED=false", "-e", "LEARNING_ENABLED=false",
        "-e", "RETENTION_ENABLED=false", "-e", "REVERSAL_PROTECTION_ENABLED=true",
        IMAGES[stage],
    ]

def parse_events(logs):
    events = []
    for line in logs.splitlines():
        try:
            record = json.loads(line)
        except ValueError:
            continue
        if not isinstance(record, dict):
            continue
        error = record.get("err")
        message = error.get("message", "") if isinstance(error, dict) else ""
        at = record.get("time", 0)
        if not isinstance(at, (int, float)):
            continue
        if "REFERENCE_CLOSE:" in message and "abort=false" in message:
            feed = str(record.get("feed", ""))
            if feed.startswith("reference:"):
                events.append({"at_ms": int(at), "kind": "REFERENCE", "asset": feed[10:],
                               "message": message[:240]})
        elif "CLOB_MARKET_WS_STALE" in message:
            events.append({"at_ms": int(at), "kind": "CLOB_STALE", "asset": "",
                           "message": message[:240]})
    return sorted(events, key=lambda e: e["at_ms"])

def find_reference_cluster(events, window_ms=1000, unique_assets=7):
    recent = deque()
    for event in events:
        if event["kind"] != "REFERENCE":
            continue
        at = event["at_ms"]
        while recent and recent[0]["at_ms"] < at - window_ms:
            recent.popleft()
        recent.append(event)
        assets = sorted({x["asset"] for x in recent})
        if len(assets) >= unique_assets:
            return {"at_ms": at, "utc": datetime.fromtimestamp(at / 1000, timezone.utc).isoformat(),
                    "assets": assets,
                    "spread_ms": max(x["at_ms"] for x in recent) - min(x["at_ms"] for x in recent)}
    return None

def run(stage, seconds, outdir):
    assert stage in IMAGES
    if not os.path.exists("/proc/loadavg"):
        raise RuntimeError("Linux VPS required")
    if docker("network", "inspect", NETWORK).returncode:
        raise RuntimeError("isolated network missing: " + NETWORK)
    if docker("image", "inspect", IMAGES[stage]).returncode:
        raise RuntimeError("image missing: " + IMAGES[stage])
    container = "pmf-reg-" + stage + "-" + str(int(time.time()))
    if docker("inspect", container).returncode == 0:
        raise RuntimeError("refusing existing container")
    outdir.mkdir(parents=True, exist_ok=True)
    result = {"stage": stage, "image": IMAGES[stage], "container": container,
              "cpu_policy": "CPU 1, shares 128, no hard quota",
              "journal_mode": "memory", "mode": "paper",
              "condition": ">=7 distinct reference abort=false closes within 1000ms",
              "status": "INCONCLUSIVE_NO_TARGET"}
    started = time.monotonic()
    launched = False
    try:
        launch = docker(*container_arguments(stage, container), timeout=25)
        if launch.returncode:
            raise RuntimeError("isolated launch failure: " + launch.stderr[-300:])
        launched = True
        consecutive_load = 0
        while time.monotonic() - started < seconds:
            logs = docker("logs", "--since", "65m", "--tail", "12000", container,
                          timeout=18)
            events = parse_events(logs.stdout + "\n" + logs.stderr)
            fault = find_reference_cluster(events)
            if fault:
                result["status"] = "FAULT_DETECTED"
                result["trigger"] = fault
                time.sleep(.3)
                break
            state = docker("inspect", "--format", "{{.State.Running}}", container)
            if state.stdout.strip() != "true":
                result["status"] = "CONTAINER_EXITED"
                break
            load = float(Path("/proc/loadavg").read_text().split()[0])
            consecutive_load = consecutive_load + 1 if load > 2.2 else 0
            if consecutive_load >= 3:
                result["status"] = "HOST_LOAD_GUARD"
                break
            time.sleep(2)
        events = parse_events((lambda r: r.stdout + "\n" + r.stderr)(
            docker("logs", "--since", "65m", "--tail", "12000", container, timeout=20)))
        result["reference_errors"] = sum(e["kind"] == "REFERENCE" for e in events)
        result["clob_stale_errors"] = sum(e["kind"] == "CLOB_STALE" for e in events)
        result["evidence"] = events[-1200:]
        cgroup = docker("exec", container, "sh", "-c",
                        "cat /sys/fs/cgroup/cpu.stat; cat /sys/fs/cgroup/cpu.max")
        if cgroup.returncode == 0:
            result["cpu_cgroup"] = cgroup.stdout
        if result.get("trigger"):
            target = result["trigger"]["at_ms"]
            before = docker("logs", "--since", "65m", "--tail", "30000",
                            PROD_CONTAINER, timeout=22)
            prod_events = parse_events(before.stdout + "\n" + before.stderr)
            result["production_signals_plusminus_2s"] = [
                e for e in prod_events if abs(e["at_ms"] - target) <= 2000
            ]
    finally:
        if launched:
            stopped = docker("stop", "-t", "10", container, timeout=20)
            result["stop_rc"] = stopped.returncode
        result["elapsed_seconds"] = round(time.monotonic() - started, 2)
        path = outdir / (container + ".json")
        path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
        os.chmod(path, 0o600)
        print(json.dumps({
            "status": result["status"], "stage": stage,
            "elapsed": result["elapsed_seconds"],
            "reference_errors": result.get("reference_errors"),
            "trigger": result.get("trigger"), "evidence_path": str(path)
        }, ensure_ascii=False), flush=True)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=sorted(IMAGES), required=True)
    parser.add_argument("--seconds", type=int, default=240)
    parser.add_argument("--outdir", type=Path, default=Path.home() / "pm-forensic-20261009")
    parser.add_argument("--execute-isolated-paper", action="store_true")
    args = parser.parse_args()
    if not 30 <= args.seconds <= 3600:
        parser.error("seconds must be in [30, 3600]")
    if not args.execute_isolated_paper:
        print(json.dumps({"dry_run": True, "stage": args.stage,
                          "image": IMAGES[args.stage],
                          "run_command": container_arguments(args.stage, "pmf-reg-preview")}))
        return
    run(args.stage, args.seconds, args.outdir)

if __name__ == "__main__":
    main()

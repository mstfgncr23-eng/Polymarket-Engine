#!/usr/bin/env python3
"""Local-only, bounded PAPER runtime smoke; explicitly NO production endpoints or wallet."""
import os
import pathlib
import socket
import subprocess
import sys
import json
from urllib.parse import urlparse

ROOT = pathlib.Path(__file__).resolve().parents[2]
HERE = pathlib.Path(__file__).resolve().parent
pgfile = HERE / "evidence/private/local_db_url.txt"
if not pgfile.exists():
    raise SystemExit("LOCAL_DB_URL_FILE_MISSING")
pg = pgfile.read_text(encoding="utf-8").strip()
url = urlparse(pg)
if (url.hostname != "127.0.0.1" or url.path != "/forensic"
        or url.username != "forensic"):
    raise SystemExit("LOCAL_DATABASE_TARGET_REJECTED")
if not (ROOT/"apps/engine/dist/main.js").exists():
    raise SystemExit("LOCAL_ENGINE_BUILD_REQUIRED")
with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
allowed = ["SYSTEMROOT", "WINDIR", "PATH", "PATHEXT", "TEMP", "TMP",
           "USERPROFILE", "APPDATA", "LOCALAPPDATA", "COMSPEC", "SYSTEMDRIVE"]
env = {k: os.environ[k] for k in allowed if k in os.environ}
env.update({
    "PM_FORENSIC_ENV": "isolated-paper",
    "TRADING_MODE": "paper",
    "NODE_ENV": "development",
    "DATABASE_URL": pg,
    "JOURNAL_MODE": "postgres",
    "MAX_QUALIFIED_MARKETS": "16",
    "DISCOVERY_INTERVAL_MS": "300000",
    "GROK_ACTIVATION_PROVIDER": "noop",
    "REVERSAL_PROTECTION_ENABLED": "false",
    "LEARNING_ENABLED": "false",
    "RETENTION_ENABLED": "false",
    "LOCAL_ARCHIVE_ENABLED": "false",
    "PROFIT_TRAILING_PAIR_SOAK_ENABLED": "false",
    "PORT": str(port),
    "LOG_LEVEL": "info",
    "DB_MIGRATIONS_DIR": str(ROOT / "packages/db/migrations"),
    "NODE_OPTIONS": "--max-old-space-size=768",
})
cmd = [
    sys.executable, str(HERE/"fault_watch.py"),
    "--confirm-isolated-paper", "--max-seconds", "65",
    "--out", str(HERE/"runs"/"current-runtime-smoke"),
    "--", "node", str(ROOT/"apps/engine/dist/main.js"),
]
print("RUNTIME_BOUNDARY", json.dumps({
    "paper": True, "db": "127.0.0.1/forensic", "engine_http_local_port": port,
    "max_seconds": 65, "no_credentials_inherited": True,
    "guardian_enabled": False, "market_limit": 16
}), flush=True)
r = subprocess.run(cmd, cwd=ROOT, env=env, stdout=subprocess.PIPE,
                   stderr=subprocess.STDOUT, text=True, timeout=90)
print("WATCH_RETURN_CODE", r.returncode, flush=True)
print(r.stdout[-1800:], flush=True)
out=HERE/"runs/current-runtime-smoke/incident.json"
if out.exists():
    rep=json.loads(out.read_text(encoding="utf8"))
    print("RUNTIME_TEST_RESULT", json.dumps({
        "status":rep["status"], "reason":rep.get("reason"),
        "observed_lines":rep.get("observed_lines"),
        "process_returncode":rep.get("process_returncode"),
        "recorded_evidence":len(rep.get("evidence", []))
    }), flush=True)

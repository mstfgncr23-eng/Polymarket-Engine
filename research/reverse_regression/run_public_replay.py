#!/usr/bin/env python3
"""Replay SAME SHA-256-verified public WebSocket capture across exact compiled source stages."""
import json
import os
from pathlib import Path
import subprocess
import time

ROOT=Path(__file__).resolve().parents[2]
TOOLS=ROOT/"research/reverse_regression"
STAGES=["08_current","07_cpu_v3","06_cpu_v2","05_cpu_v1","03_identity","02_step3","01_step2","00_safe"]
OUT=TOOLS/"runs/public_capture_replay"
OUT.mkdir(parents=True,exist_ok=True)
summary=[]
def cmd(argv, timeout=50, env=None):
 return subprocess.run(argv,cwd=ROOT,env=env,capture_output=True,text=True,timeout=timeout)
try:
 for stage in STAGES:
  changed=cmd(["python",str(TOOLS/"stage.py"),"--stage",stage,"--apply"])
  check=cmd(["python",str(TOOLS/"stage.py"),"--stage",stage,"--verify"])
  if changed.returncode!=0 or check.returncode!=0:raise RuntimeError("SOURCE_STAGE_MISMATCH:"+stage)
  env=dict(os.environ,PM_FORENSIC_STAGE=stage,CI="true",NO_COLOR="1",FORCE_COLOR="0",PM_REPLAY_SPEED="1",PM_REPLAY_ENABLE="1")
  t=time.monotonic()
  p=cmd([str(ROOT/"node_modules/.bin/vitest.cmd"),"run","tests/forensic-public-replay.test.ts",
         "--reporter=verbose","--maxWorkers=1","--testTimeout=35000"],timeout=60,env=env)
  metrics=[]
  for line in p.stdout.splitlines():
   if "PUBLIC_REPLAY_RESULT " not in line:continue
   candidate=line.split("PUBLIC_REPLAY_RESULT ",1)[1].strip().replace("\x1b[0m","")
   try:metrics.append(json.loads(candidate))
   except ValueError:pass
  result={"stage":stage,"exit":p.returncode,"duration_sec":round(time.monotonic()-t,2),
          "metrics":metrics,"error_tail":(p.stderr+"\n"+p.stdout[-1800:])[-2200:] if p.returncode else ""}
  summary.append(result)
  (OUT/(stage+".json")).write_text(json.dumps(result,indent=2),encoding="utf8")
  (OUT/"summary.json").write_text(json.dumps(summary,indent=2),encoding="utf8")
  if metrics:
   m=metrics[0]
   print("REPLAY_STAGE",stage,"TEST",p.returncode,"REF",m["acceptedReference"],
         "CLOB",m["clobSnapshots"],"MARKETS",m["clobMarketsReached"],
         "CONCURRENT",m["maxSnapshotInFlight"],"LOOP_P99",m["eventLoopP99Ms"],
         "REF_ERRORS",len(m["refErrors"]),"CLOB_ERRORS",len(m["clobErrors"]),flush=True)
  else:print("REPLAY_STAGE",stage,"RC",p.returncode,"NO_METRICS",result["error_tail"][-700:],flush=True)
  if p.returncode!=0:break
finally:
 a=cmd(["python",str(TOOLS/"stage.py"),"--stage","08_current","--apply"])
 v=cmd(["python",str(TOOLS/"stage.py"),"--stage","08_current","--verify"])
 print("FINAL_RESTORED_08_CURRENT",a.returncode,v.returncode,flush=True)

import os, pathlib, subprocess, json, time, traceback
root=pathlib.Path(__file__).resolve().parents[2]
tool=root/"research/reverse_regression/stage.py"
steps=["08_current","07_cpu_v3","06_cpu_v2","05_cpu_v1","03_identity","02_step3","01_step2","00_safe"]
out=root/"research/reverse_regression/runs/reverse_stage_tests"
out.mkdir(parents=True,exist_ok=True)
report=[]
pnpm=str(root/"node_modules/.bin/vitest.cmd")
tsc=str(root/"node_modules/.bin/tsc.cmd")
print("REVERSE_STAGE_TEST_BEGIN",steps,flush=True)
try:
 for stage in steps:
  print("STAGE_START",stage,flush=True)
  app=subprocess.run(["python",str(tool),"--stage",stage,"--apply"],cwd=root,capture_output=True,text=True,timeout=40)
  ver=subprocess.run(["python",str(tool),"--stage",stage,"--verify"],cwd=root,capture_output=True,text=True,timeout=40)
  if app.returncode or ver.returncode:raise RuntimeError(f"Stage switch failed {stage}: {app.stderr} {ver.stdout}")
  env=dict(os.environ,PM_FORENSIC_STAGE=stage,CI="true")
  tests=subprocess.run([pnpm,"run","tests/forensic-reference-journal-delay.test.ts","tests/guardian-v2.test.ts","tests/polymarket-sharded-adapter.test.ts","--reporter=dot","--maxWorkers=1","--testTimeout=12000"],cwd=root,env=env,capture_output=True,text=True,timeout=90)
  compile=subprocess.run([tsc,"-p","apps/engine/tsconfig.forensic.json","--noEmit"],cwd=root,env=env,capture_output=True,text=True,timeout=75)
  build=subprocess.run(["node","apps/engine/build.mjs"],cwd=root,env=env,capture_output=True,text=True,timeout=60)
  stdout=tests.stdout.replace("\x1b","ESC")
  result={"stage":stage,"vitest_rc":tests.returncode,"typecheck_rc":compile.returncode,"build_rc":build.returncode,
     "test_summary":"; ".join(x.strip() for x in tests.stdout.splitlines() if "Tests " in x or "Test Files " in x)[-280:],
     "test_errors":(tests.stderr + "\n" + tests.stdout[-1400:])[-2000:] if tests.returncode else "",
     "compile_error":(compile.stdout+"\n"+compile.stderr)[-900:] if compile.returncode else "",
     "build_error":(build.stdout+"\n"+build.stderr)[-900:] if build.returncode else ""}
  report.append(result)
  (out/(stage+".json")).write_text(json.dumps(result,indent=2),encoding="utf8")
  print("STAGE_RESULT",json.dumps({k:result[k] for k in ("stage","vitest_rc","typecheck_rc","build_rc")}),flush=True)
except Exception as exc:
 print("ORCHESTRATOR_EXCEPTION",str(exc),flush=True)
finally:
 reset=subprocess.run(["python",str(tool),"--stage","08_current","--apply"],cwd=root,capture_output=True,text=True,timeout=40)
 verify=subprocess.run(["python",str(tool),"--stage","08_current","--verify"],cwd=root,capture_output=True,text=True,timeout=40)
 print("RESTORED_CURRENT",reset.returncode,verify.returncode,verify.stdout.strip()[:110],flush=True)
 (out/"summary.json").write_text(json.dumps(report,indent=2),encoding="utf8")
 print("STAGES_COMPLETED",len(report),flush=True)

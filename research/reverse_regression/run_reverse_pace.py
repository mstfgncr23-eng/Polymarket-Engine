import os,pathlib,json,subprocess,time
r=pathlib.Path(__file__).resolve().parents[2]
stage_tool=r/'research/reverse_regression/stage.py'
test=r/'tests/forensic-fixed-rate.test.ts'
out=r/'research/reverse_regression/runs/reverse_stage_pace'
out.mkdir(parents=True,exist_ok=True)
steps=['08_current','07_cpu_v3','06_cpu_v2','05_cpu_v1','03_identity','02_step3','01_step2','00_safe']
env=os.environ.copy()
env.update({'CI':'true','AUDIT_RATES':'100','AUDIT_DURATION_MS':'2000','AUDIT_WARMUP':'100'})
results=[]
print('START_ACTIVE_GUARDIAN_REVERSE_PACE',len(steps),'stages',flush=True)
try:
 for stage in steps:
  app=subprocess.run(['python',str(stage_tool),'--stage',stage,'--apply'],cwd=r,capture_output=True,text=True,timeout=30)
  vf=subprocess.run(['python',str(stage_tool),'--stage',stage,'--verify'],cwd=r,capture_output=True,text=True,timeout=30)
  if app.returncode or vf.returncode:raise RuntimeError('STAGE_SWAP_INVALID_'+stage)
  env['PM_FORENSIC_STAGE']=stage
  st=time.monotonic()
  p=subprocess.run([str(r/'node_modules/.bin/vitest.cmd'),'run',str(test),'--reporter=verbose','--maxWorkers=1','--testTimeout=90000'],cwd=r,env=env,capture_output=True,text=True,timeout=110)
  metrics=[]
  for line in p.stdout.splitlines():
   if 'PACE_RESULT ' in line:
    try:metrics.append(json.loads(line.split('PACE_RESULT ',1)[1].strip()))
    except Exception:pass
  result={'stage':stage,'rc':p.returncode,'test_duration_sec':round(time.monotonic()-st,2),'metrics':metrics,'stderr_tail':p.stderr[-250:]}
  results.append(result)
  (out/(stage+'.json')).write_text(json.dumps(result,indent=2),encoding='utf8')
  print('PACE_STAGE_RESULT',stage,'RC',p.returncode,'COMPLETED',metrics[0].get('finished') if metrics else 'NO_METRICS','LOOP_P99',metrics[0].get('eventLoopMs',{}).get('p99') if metrics else '-','CALLBACK_P99',metrics[0].get('callbackMs',{}).get('p99') if metrics else '-',flush=True)
finally:
 reset=subprocess.run(['python',str(stage_tool),'--stage','08_current','--apply'],cwd=r,capture_output=True,text=True,timeout=30)
 check=subprocess.run(['python',str(stage_tool),'--stage','08_current','--verify'],cwd=r,capture_output=True,text=True,timeout=30)
 print('PACE_RESTORED_CURRENT',reset.returncode,check.returncode,flush=True)
 (out/'summary.json').write_text(json.dumps(results,indent=2),encoding='utf8')

import os,sys,pathlib,subprocess,json,time
root=pathlib.Path(__file__).resolve().parents[2]
resdir=root/'research/reverse_regression/runs/component_ablation'
resdir.mkdir(parents=True,exist_ok=True)
sys.path.insert(0,str(root/'research/reverse_regression'))
from stage import source_files
base=source_files('08_current')
previous=source_files('07_cpu_v3')
paths=sorted(p for p in set(base)|set(previous) if base.get(p)!=previous.get(p))
print('LATEST_CHANGE_SINGLE_FILE_REVERSE',len(paths),paths,flush=True)
tool=root/'research/reverse_regression/stage.py'
out=[]
try:
 for rel in paths:
  reset=subprocess.run(['python',str(tool),'--stage','08_current','--apply'],cwd=root,capture_output=True,text=True,timeout=35)
  if reset.returncode:raise RuntimeError('CANNOT_RESTORE_BASE')
  target=root/rel
  older=previous.get(rel)
  if older is None:target.unlink(missing_ok=True)
  else:
   target.parent.mkdir(parents=True,exist_ok=True)
   target.write_bytes(older.encode('utf8'))
  env=dict(os.environ,PM_FORENSIC_STAGE='08_current',CI='true')
  tsc=subprocess.run([str(root/'node_modules/.bin/tsc.cmd'),'-p','apps/engine/tsconfig.forensic.json','--noEmit'],cwd=root,env=env,capture_output=True,text=True,timeout=65)
  tests=None
  if tsc.returncode==0:
   tests=subprocess.run([str(root/'node_modules/.bin/vitest.cmd'),'run','tests/forensic-reference-journal-delay.test.ts','tests/guardian-v2.test.ts','tests/polymarket-sharded-adapter.test.ts','--reporter=dot','--maxWorkers=1','--testTimeout=12000'],cwd=root,env=env,capture_output=True,text=True,timeout=90)
  result={'replaced_with_previous_file':rel,'tsc_rc':tsc.returncode,'tests_rc':tests.returncode if tests else None,'tsc_errors':tsc.stdout[-700:] if tsc.returncode else None,'tests_tail':tests.stdout[-700:] if tests and tests.returncode else None}
  out.append(result)
  (resdir/'summary.json').write_text(json.dumps(out,indent=2),encoding='utf8')
  print('FILE_REMOVAL',rel,'TYPECHECK',result['tsc_rc'],'TESTS',result['tests_rc'],flush=True)
finally:
 rr=subprocess.run(['python',str(tool),'--stage','08_current','--apply'],cwd=root,capture_output=True,text=True,timeout=35)
 verify=subprocess.run(['python',str(tool),'--stage','08_current','--verify'],cwd=root,capture_output=True,text=True,timeout=35)
 print('FINAL_RESTORE',rr.returncode,verify.returncode,flush=True)

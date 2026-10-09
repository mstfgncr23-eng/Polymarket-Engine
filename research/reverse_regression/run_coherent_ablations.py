import os,pathlib,subprocess,json,sys
root=pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0,str(root/'research/reverse_regression'))
from stage import source_files
old=source_files('07_cpu_v3')
cur=source_files('08_current')
tool=root/'research/reverse_regression/stage.py'
pre='apps/engine/src/'
cases={
 'A_report_plus_source_seal':['final-report.ts','source-seal.ts'],
 'B_integrity_plus_source_seal':['report-source-integrity.ts','source-seal.ts'],
 'C_engine_plus_source_seal':['engine.ts','source-seal.ts'],
 'D_engine_integrity_source_seal':['engine.ts','report-source-integrity.ts','source-seal.ts'],
 'E_engine_report_integrity_source_seal':['engine.ts','final-report.ts','report-source-integrity.ts','source-seal.ts'],
 'F_all_source_seal_feature':['engine.ts','final-report.ts','report-source-integrity.ts','source-seal.ts','runtime-supervisor.ts','@core/retention.ts'],
}
outdir=root/'research/reverse_regression/runs/coherent_group_ablations'
outdir.mkdir(parents=True,exist_ok=True)
env=dict(os.environ,CI='true',PM_FORENSIC_STAGE='08_current')
data=[]
try:
 for name,files in cases.items():
  a=subprocess.run(['python',str(tool),'--stage','08_current','--apply'],cwd=root,capture_output=True,text=True,timeout=40)
  if a.returncode:raise RuntimeError('BASE_RESTORE_FAILURE')
  for short in files:
   rel='packages/core/src/retention.ts' if short.startswith('@core/') else pre+short
   target=root/rel
   val=old.get(rel)
   if val is None:target.unlink(missing_ok=True)
   else:
    target.parent.mkdir(parents=True,exist_ok=True)
    target.write_bytes(val.encode('utf8'))
  check=subprocess.run([str(root/'node_modules/.bin/tsc.cmd'),'-p','apps/engine/tsconfig.forensic.json','--noEmit'],cwd=root,env=env,capture_output=True,text=True,timeout=50)
  test=None
  if check.returncode==0:
   test=subprocess.run([str(root/'node_modules/.bin/vitest.cmd'),'run','tests/forensic-reference-journal-delay.test.ts','tests/guardian-v2.test.ts','tests/polymarket-sharded-adapter.test.ts','--reporter=dot','--maxWorkers=1'],cwd=root,env=env,capture_output=True,text=True,timeout=90)
  item={'name':name,'files':files,'typecheck_rc':check.returncode,'test_rc':test.returncode if test else None,'errors':(check.stdout+check.stderr)[-1400:] if check.returncode else ''}
  data.append(item)
  (outdir/'summary.json').write_text(json.dumps(data,indent=2),encoding='utf8')
  print('COHERENT_REMOVAL',name,'typecheck',check.returncode,'tests',item['test_rc'],flush=True)
finally:
 r=subprocess.run(['python',str(tool),'--stage','08_current','--apply'],cwd=root,capture_output=True,text=True,timeout=35)
 v=subprocess.run(['python',str(tool),'--stage','08_current','--verify'],cwd=root,capture_output=True,text=True,timeout=35)
 print('RESTORE_CURRENT',r.returncode,v.returncode,flush=True)

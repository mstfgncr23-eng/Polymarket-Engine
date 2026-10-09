import os,pathlib,subprocess,json,time
root=pathlib.Path(__file__).resolve().parents[2]
out=root/'research/reverse_regression/runs/vps_reverse_walk'
out.mkdir(parents=True,exist_ok=True)
ssh=r'C:\Program Files\Git\usr\bin\ssh.exe'
stages=['current','cpu3','cpu2','cpu1','identity','step3','step2','safe']
all_results=[]
print('VPS_REVERSE_WALK',stages,'PER_STAGE_SECONDS',90,'ONE_AT_A_TIME',flush=True)
for stage in stages:
 print('VPS_STAGE_START',stage,flush=True)
 start=time.monotonic()
 cmd=[ssh,'-T','-o','BatchMode=yes','polymarket-vps',
      f'python3 /home/ubuntu/pm-forensic-20261009/vps_reverse_runner.py --stage {stage} --seconds 90 --execute-isolated-paper']
 try:
  res=subprocess.run(cmd,capture_output=True,text=True,timeout=155)
  entry={'stage':stage,'exit_rc':res.returncode,'elapsed_s':round(time.monotonic()-start,1),
        'result_line':next((x for x in reversed(res.stdout.splitlines()) if x.startswith('{') and '"status":' in x),None),
        'stderr_tail':res.stderr[-400:]}
 except subprocess.TimeoutExpired:
  entry={'stage':stage,'exit_rc':124,'elapsed_s':round(time.monotonic()-start,1),
         'error':'WRAPPER_TIMEOUT: inspect and stop only this stage container before proceeding'}
  all_results.append(entry)
  (out/'summary.json').write_text(json.dumps(all_results,indent=2),encoding='utf8')
  print('VPS_STAGE_TIMEOUT_ABORT',stage,flush=True)
  break
 all_results.append(entry)
 (out/'summary.json').write_text(json.dumps(all_results,indent=2),encoding='utf8')
 print('VPS_STAGE_DONE',json.dumps(entry,ensure_ascii=False),flush=True)
 status = None
 if entry.get("result_line"):
  try: status = json.loads(entry["result_line"]).get("status")
  except ValueError: status = "MALFORMED_REPORT"
 if res.returncode or status not in ("FAULT_DETECTED", "INCONCLUSIVE_NO_TARGET"):
  print('ABORT_AFTER_STAGE_ERROR_OR_SAFETY_GUARD',stage,status,flush=True)
  break
print('VPS_WALK_FINISHED',len(all_results),flush=True)

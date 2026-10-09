#!/usr/bin/env python3
"""Read-only telemetry sampler for a running isolated forensic engine."""
import datetime, json, os, pathlib, subprocess, sys, time
if len(sys.argv)!=2 or not sys.argv[1].startswith('pmf-reg-current-'):
    sys.exit('EXPECTED_FORENSIC_CONTAINER_NAME')
NAME=sys.argv[1]
OUT=pathlib.Path('/home/ubuntu/pm-forensic-20261009')/(NAME+'-metrics-samples.jsonl')
KEYS=('reference_ws_close_total','nodejs_eventloop_utilization',
      'polymarket_nodejs_eventloop_lag_p99_seconds',
      'polymarket_nodejs_eventloop_lag_max_seconds',
      'market_ingress_queue_depth','market_ingress_queue_bytes',
      'runtime_work_queue_depth','journal_direct_waiting',
      'journal_direct_failures_total','reference_ticks_total',
      'telemetry_queue_depth','polymarket_process_cpu_seconds_total',
      'market_ingress_high_water_total','ws_reconnects_total')
def docker(*args,timeout=8):
 return subprocess.run(['sudo','-n','docker',*args],capture_output=True,text=True,timeout=timeout)
js="""Promise.all([
fetch('http://127.0.0.1:8080/metrics',{signal:AbortSignal.timeout(2500)}).then(r=>r.text()),
fetch('http://127.0.0.1:8080/ready',{signal:AbortSignal.timeout(2500)}).then(r=>r.json())
]).then(([m,r])=>console.log(JSON.stringify({metrics:m,ready:{status:r.status,ws:r.polymarketWs,qualified:r.qualifiedMarketCount,overload:r.overloadState,database:r.database}}))).catch(e=>console.log(JSON.stringify({error:String(e.message)})))"""
OUT.parent.mkdir(parents=True,exist_ok=True)
if OUT.exists():sys.exit('REFUSE_OVERWRITE')
fd=os.open(OUT,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
with os.fdopen(fd,'w') as f:
 for i in range(180):
  started=time.monotonic()
  state=docker('inspect','--format','{{.State.Running}}',NAME,timeout=5)
  if state.stdout.strip()!='true':
   f.write(json.dumps({'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'state':'TEST_STOPPED'})+'\n');f.flush();break
  rs=docker('exec',NAME,'node','-e',js)
  obj={'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'state':'RUNNING'}
  try:
   x=json.loads(rs.stdout.strip())
   obj['ready']=x.get('ready')
   metrics=x.get('metrics','')
   obj['metrics']={line.split(' ',1)[0]:line.rsplit(' ',1)[-1] for line in metrics.splitlines() if line and not line.startswith('#') and line.split('{',1)[0].split(' ',1)[0] in KEYS}
   if x.get('error'):obj['error']=x['error']
  except Exception as e:obj['metrics_read_error']=str(e)[:130]
  if i%3==0:
   v=docker('exec',NAME,'sh','-c','cat /sys/fs/cgroup/cpu.stat; cat /sys/fs/cgroup/cpu.max',timeout=6)
   obj['cgroup_cpu']=v.stdout[:800]
   obj['host_load1']=float(open('/proc/loadavg').read().split()[0])
  f.write(json.dumps(obj,ensure_ascii=False)+'\n');f.flush()
  time.sleep(max(0,5-(time.monotonic()-started)))
print('METRIC_SAMPLER_DONE',OUT,flush=True)

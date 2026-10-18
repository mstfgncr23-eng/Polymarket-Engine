import { it, expect } from 'vitest';
import { mkdtemp, readdir, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { gunzipSync } from 'node:zlib';
import { RawEvidence } from '../apps/engine/src/raw-evidence.js';
import { snapshot } from './fixtures.js';
it('archives full book/flow records, reports gaps, seals and flushes complete gzip files',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'roi1-raw-'));
  try{
    const archive=new RawEvidence(dir);
    archive.record('experiment_test',{kind:'book',bids:[['0.49','20']],asks:[['0.5','10']]});
    archive.record('experiment_test',{kind:'public_trade',side:'SELL'});
    archive.record('experiment_test',{oversize:'x'.repeat(4*1024**2)});
    await archive.close();
    archive.record('experiment_test',{kind:'late'});
    const root=join(dir,'roi1-evidence','experiment_test');
    const gz=(await readdir(root)).filter(x=>x.endsWith('.gz'));
    const rows=gunzipSync(await readFile(join(root,gz[0]!))).toString().trim().split('\n').map(x=>JSON.parse(x));
    expect(rows).toHaveLength(2);expect(rows[0].bids).toEqual([['0.49','20']]);
    expect(JSON.parse(await readFile(join(root,'capture-status.json'),'utf8'))).toMatchObject({accepted:2,dropped:1,fault:null});
    expect(archive.status().gaps).toHaveLength(1);
  }finally{await rm(dir,{recursive:true,force:true});}
});
it('bounds a burst across all closing parts and resumes with a full book after an explicit gap',async()=>{
  const dir=await mkdtemp(join(tmpdir(),'roi1-burst-'));
  try{
    const archive=new RawEvidence(dir,32*1024),book=snapshot(),context={tokenEpoch:1,synchronized:true,siblingEventId:null};
    archive.recordBook('experiment_burst',book,context);
    for(let i=0;i<100;i++)archive.record('experiment_burst',{kind:'burst',value:'x'.repeat(4000)});
    const beforeGap=archive.status().dropped;
    archive.recordBook('experiment_burst',{...book,eventId:'dropped',traceId:'x'.repeat(33000)},context);
    expect(archive.status().dropped).toBe(beforeGap+1);
    expect(archive.status().dropped).toBeGreaterThan(0);
    expect(archive.status().maxQueuedBytes).toBeLessThanOrEqual(32*1024);
    await new Promise(r=>setTimeout(r,150));
    archive.recordBook('experiment_burst',{...book,eventId:'resumed'},context);
    await archive.close();
    const root=join(dir,'roi1-evidence','experiment_burst'),rows=[];
    for(const name of (await readdir(root)).filter(x=>x.endsWith('.gz')))
      rows.push(...gunzipSync(await readFile(join(root,name))).toString().trim().split('\n').map(x=>JSON.parse(x)));
    expect(rows.find(x=>x.snapshot?.eventId==='resumed')?.encoding).toBe('FULL');
    expect(rows).toHaveLength(archive.status().accepted);expect(archive.status().fault).toBeNull();
  }finally{await rm(dir,{recursive:true,force:true});}
});

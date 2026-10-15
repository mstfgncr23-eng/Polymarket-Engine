import {it,expect} from 'vitest';
import {mkdtemp,rm,readdir} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {spawn} from 'node:child_process';
it.each(['recovery','journal'])('archive %s failure preserves the original rejection without a late process crash',async mode=>{
 const root=await mkdtemp(join(tmpdir(),'archive-stream-failure-'));
 try{
  const child=spawn(process.execPath,['--import','tsx',resolve('tests/archive-stream-failure.fixture.ts'),root,mode],{cwd:process.cwd(),windowsHide:true,stdio:['ignore','pipe','pipe']});
  let stdout='',stderr='';child.stdout.on('data',b=>stdout+=b);child.stderr.on('data',b=>stderr+=b);
  const result=await new Promise<{code:number|null;signal:string|null}>((resolve,reject)=>{child.once('error',reject);child.once('close',(code,signal)=>resolve({code,signal}));});
  expect({result,stdout,stderr}).toMatchObject({result:{code:0,signal:null},stderr:''});
  expect(stdout).toContain('EXPECTED_ARCHIVE_REJECTION:');expect(stdout).toContain('CHILD_SURVIVED_LATE_STREAM_ERROR');
  const files=await readdir(root,{recursive:true});expect(files.some(f=>String(f).includes('.tmp'))).toBe(false);
 }finally{await rm(root,{recursive:true,force:true});}
},10000);

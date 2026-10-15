import {writeFile} from 'node:fs/promises';
import {join} from 'node:path';
import {EVENT_PAGE_SIZE} from '../packages/core/src/index.js';
import {hashDurablePayload,type EventJournal,type EngineCheckpoint} from '../packages/db/src/index.js';
import {LocalJournalArchive,LOCAL_ARCHIVE_SENTINEL,LOCAL_ARCHIVE_SENTINEL_CONTENT} from '../apps/engine/src/archive.js';
const [root,mode]=process.argv.slice(2);if(!root)throw Error('ROOT_REQUIRED');
await writeFile(join(root,LOCAL_ARCHIVE_SENTINEL),LOCAL_ARCHIVE_SENTINEL_CONTENT);
const archive=new LocalJournalArchive(root);
const payload={infrastructureCost:'0',padding:'x'.repeat(180)};
const row={sequence:1,operationKey:'fixture:1',type:'INFRASTRUCTURE_COST_ACCRUED',aggregateType:'ledger',aggregateId:'fixture',payload,payloadHash:hashDurablePayload(payload),occurredAt:'2026-10-15T00:00:00Z',recordedAt:'2026-10-15T00:00:00Z'};
try{
 if(mode==='recovery'){
  const rows=Array.from({length:400},(_,i)=>({...row,sequence:i+1,operationKey:'fixture:'+i}));
  rows.push({...row,sequence:401,operationKey:'fixture:bad',payloadHash:'deliberately-invalid-hash'});
  const journal={latestDurableSequence:async()=>401,queryDurableTransitions:async()=>rows} as unknown as EventJournal;
  const retained=[{id:'one',payload:{durableCursor:401}},{id:'two',payload:{durableCursor:401}}] as unknown as EngineCheckpoint[];
  await archive.ensureRecoveryBundle(journal,{retained,candidates:[],prefixCursor:401,maxPrefixRows:500});
 }else{
  const good={id:'one',type:'TRIGGER_EVENT',aggregateType:'fixture',aggregateId:'fixture',traceId:'fixture',payload:{},occurredAt:'2026-10-15T00:00:01.000Z',recordedAt:'2026-10-15T00:00:01.000Z'};
  const rows=Array.from({length:EVENT_PAGE_SIZE},(_,i)=>({...good,id:'fixture:'+i,payload:{padding:'x'.repeat(180)}}));let page=0;
  const journal={queryEventsKeyset:async()=>{if(page++)throw Error('TEST_ARCHIVE_SOURCE_READ_FAILURE');return rows;}} as unknown as EventJournal;
  await archive.ensureJournalBucket(journal,{retentionClass:'monitor_control',eventTypes:['TRIGGER_EVENT'],occurredFrom:new Date('2026-10-15T00:00:00Z'),occurredBefore:new Date('2026-10-15T00:01:00Z'),commitSeqTo:2,recordedAtTo:new Date('2026-10-15T00:02:00Z')});
 }
 throw Error('ARCHIVE_SHOULD_REJECT');
}catch(error){
 const expected=mode==='recovery'?'RECOVERY_ARCHIVE_DURABLE_HASH':'TEST_ARCHIVE_SOURCE_READ_FAILURE';
 if(!(error instanceof Error)||error.message!==expected)throw error;
 console.log('EXPECTED_ARCHIVE_REJECTION:'+expected);
}
// Allow an outstanding fs.write callback to deliver its error; a child crash
// must not be hidden by stopping at the first rejected archive promise.
await new Promise(r=>setTimeout(r,150));
console.log('CHILD_SURVIVED_LATE_STREAM_ERROR');

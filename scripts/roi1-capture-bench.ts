import {createReadStream} from 'node:fs';
import {createGunzip} from 'node:zlib';
import {createInterface} from 'node:readline';
import {mkdir,writeFile} from 'node:fs/promises';
import {performance,monitorEventLoopDelay} from 'node:perf_hooks';
import {RawEvidence} from '../apps/engine/src/raw-evidence.js';
import {BookEvidenceEncoder,BookEvidenceDecoder} from '../apps/engine/src/book-evidence-codec.js';
import type {MarketSnapshot} from '@pm/core';
import {isDeepStrictEqual} from 'node:util';

const input=process.argv[2]!,output=process.argv[3]!;
if(!input||!output)throw Error('INPUT_AND_OUTPUT_REQUIRED');
const archive=new RawEvidence(output),encoder=new BookEvidenceEncoder(),decoder=new BookEvidenceDecoder();
const lag=monitorEventLoopDelay({resolution:10});lag.enable();
const started=performance.now(),cpu=process.cpuUsage();let count=0,fullBytes=0,encodedBytes=0;
const rate=2000; // Above the observed normalized input rate; no market subscriptions.
for await(const line of createInterface({input:createReadStream(input).pipe(createGunzip()),crlfDelay:Infinity})){
  const item=JSON.parse(line);if(item.kind!=='book'||!item.snapshot)continue;
  const book=item.snapshot as MarketSnapshot;
  const encoded=encoder.encode(book,1,item.tokenEpoch,item.synchronized);
  if(!isDeepStrictEqual(decoder.apply(JSON.parse(JSON.stringify(encoded))),book))throw Error('LOSSLESS_ROUNDTRIP_FAILED');
  fullBytes+=Buffer.byteLength(JSON.stringify(item));encodedBytes+=Buffer.byteLength(JSON.stringify(encoded));
  archive.recordBook('offline_capture_bench',book,{tokenEpoch:item.tokenEpoch,synchronized:item.synchronized,siblingEventId:item.siblingEventId??null});
  count++;
  if(count%100===0){const wait=count/rate*1000-(performance.now()-started);if(wait>0)await new Promise(r=>setTimeout(r,wait));}
  if(count>=40000)break;
}
await archive.close();lag.disable();
const used=process.cpuUsage(cpu),result={kind:'OFFLINE_CAPTURE_LOAD_TEST_NOT_TRADING',count,elapsedMs:performance.now()-started,
  cpuUserMs:used.user/1000,cpuSystemMs:used.system/1000,eventLoopP95Ms:lag.percentile(95)/1e6,eventLoopP99Ms:lag.percentile(99)/1e6,
  fullBytes,encodedBytes,losslessRoundtrips:count,capture:archive.status()};
await mkdir(output,{recursive:true});await writeFile(output+'/bench-result.json',JSON.stringify(result,null,2));
process.stdout.write(JSON.stringify(result)+'\n');
if(result.capture.dropped||result.capture.fault)process.exitCode=1;

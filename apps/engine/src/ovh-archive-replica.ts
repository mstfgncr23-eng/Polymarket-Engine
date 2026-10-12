import {createHash} from "node:crypto";
import {createReadStream} from "node:fs";
import {lstat,readFile} from "node:fs/promises";
import {basename,join,resolve} from "node:path";
import {spawn} from "node:child_process";
import type {ExperimentRawArchiveManifest,JournalBucketArchiveManifest,
  RecoveryArchiveManifest,VerifiedArchiveFile} from "./archive.js";
import type {RetentionReplicaVerifier} from "./retention-manager.js";

const NEW_SOURCE = "polymarket";
const UPLOAD_ROOT = "/data/polymarket-archive/g4-independent-paper/archive";
const DESTINATION = UPLOAD_ROOT+"/simple-engine";
const fail=(code:string):never=>{throw Error("OVH_ARCHIVE_"+code);};
const hex=(s:unknown)=>typeof s==="string"&&/^[0-9a-f]{64}$/.test(s);
const component=(s:string)=>/^[A-Za-z0-9_.-]{5,180}$/.test(s)
  &&s!=="."&&s!==".."&&!s.includes("..");
type ProofFile={bytes:number;sha256:string};
type Proof={directory:string;kind:"experiment_canonical"|"journal_bucket"|"engine_recovery";
  files:Record<string,ProofFile>;source:typeof NEW_SOURCE};
export type OvhCommandRunner=(program:string,args:readonly string[],
  stdin?:string)=>Promise<string>;
async function command(program:string,args:readonly string[],stdin?:string):Promise<string>{
  return new Promise((ok,bad)=>{
    const child=spawn(program,args as string[],{
      windowsHide:true,stdio:["pipe","pipe","pipe"],shell:false,
    });
    let stdout="",stderr="";
    const timer=setTimeout(()=>child.kill(),600_000);
    child.stdout.on("data",(bytes:Buffer)=>{
      stdout+=bytes.toString("utf8");
      if(stdout.length>4096)child.kill();
    });
    child.stderr.on("data",(bytes:Buffer)=>{
      stderr+=bytes.toString("utf8");
      if(stderr.length>4096)child.kill();
    });
    child.on("error",bad);
    child.on("close",(code)=>{
      clearTimeout(timer);
      if(code!==0)bad(Error("OVH_ARCHIVE_COMMAND_FAILED"));
      else ok(stdout.trim());
    });
    child.stdin.end(stdin??"");
  });
}
async function sourceProof(directory:string,names:Readonly<Record<string,VerifiedArchiveFile>>){
  const result:Record<string,ProofFile>={};
  for(const [name,expected] of Object.entries(names)){
    if(!component(name)||expected.file!==name||!hex(expected.sha256)
      ||!Number.isSafeInteger(expected.bytes)||expected.bytes<0)
      fail("SOURCE_MANIFEST_INVALID");
    const path=join(directory,name);
    const stat=await lstat(path).catch(()=>fail("SOURCE_FILE_MISSING"));
    if(!stat.isFile()||stat.isSymbolicLink()||stat.nlink!==1
      ||stat.size!==expected.bytes)fail("SOURCE_FILE_INVALID");
    const digest=createHash("sha256");
    let length=0;
    for await(const buf of createReadStream(path)){
      length+=buf.length;
      digest.update(buf);
    }
    if(length!==expected.bytes||digest.digest("hex")!==expected.sha256)
      fail("SOURCE_CHECKSUM_INVALID");
    result[name]={bytes:length,sha256:expected.sha256};
  }
  return result;
}
export class OvhArchiveReplicaVerifier implements RetentionReplicaVerifier{
  static forEngine(remoteHost:string, privateDir="/run/private") {
    return new OvhArchiveReplicaVerifier({remoteHost, allowUpload:true,
      uploadIdentityFile:join(privateDir,"g4-archive-upload-key"),
      verifyIdentityFile:join(privateDir,"g4-archive-verify-key"),
      knownHostsFile:join(privateDir,"g4-archive-known-hosts")});
  }
  constructor(private readonly options:{
    readonly remoteHost:string;
    readonly uploadIdentityFile:string;
    readonly verifyIdentityFile:string;
    readonly knownHostsFile:string;
    readonly allowUpload:boolean;
    readonly runner?:OvhCommandRunner;
  }){
    if(options.remoteHost!=="g4archive@141.95.162.252") fail("REMOTE_HOST_IDENTITY");
  }
  private get runner(){return this.options.runner??command;}
  private async prove(input:{
    localDirectory:string;remoteDirectory:string;kind:Proof["kind"];
    files:Readonly<Record<string,VerifiedArchiveFile>>;
  }):Promise<void>{
    const {localDirectory,remoteDirectory,kind,files}=input;
    const manifestStat=await lstat(localDirectory).catch(()=>fail("SOURCE_DIRECTORY_MISSING"));
    if(!manifestStat.isDirectory()||manifestStat.isSymbolicLink())
      fail("SOURCE_DIRECTORY_INVALID");
    const actual=await sourceProof(localDirectory,files);
    const proof:Proof={directory:remoteDirectory,kind,files:actual,source:NEW_SOURCE};
    const remote=this.options.remoteHost;
    const flags=(identity:string)=>["-T","-i",identity,
      "-o","IdentitiesOnly=yes","-o","BatchMode=yes",
      "-o","StrictHostKeyChecking=yes",
      "-o","UserKnownHostsFile="+this.options.knownHostsFile,
      "-o","ConnectTimeout=8"];
    if(this.options.allowUpload){
      // Forced-command /usr/bin/rrsync -wo confines the upload key to only
      // the existing restricted archive root. --mkpath is supported by rsync >=3.2.3.
      // Destination paths in rrsync's namespace are RELATIVE, never absolute.
      const base=UPLOAD_ROOT+"/";
      if(!remoteDirectory.startsWith(base))fail("REMOTE_DIRECTORY_SCOPE");
      const relative=remoteDirectory.slice(base.length);
      if(relative.split("/").some(x=>!component(x)))fail("REMOTE_PATH_COMPONENT");
      const sshCommand=["ssh",...flags(this.options.uploadIdentityFile)].join(" ");
      await this.runner("rsync",["-a","--mkpath","--ignore-existing",
        "--no-specials","--no-devices","-e",sshCommand,
        "--",...Object.keys(files).map(name=>join(localDirectory,name)),
        remote+":"+relative+"/"]).catch(()=>fail("RESTRICTED_RSYNC_FAILED"));
    }
    // The verifier is a fixed ROOT-owned Python program on the receiving OVH.
    // DO NOT execute caller-supplied Python via 'python3 -' on the other VPS.
    const response=await this.runner("ssh",[
      ...flags(this.options.verifyIdentityFile),remote,
      "python3 /usr/local/libexec/g4-ovh-retention-verify.py",
    ],JSON.stringify(proof)+"\n").catch(()=>fail("REMOTE_READBACK_FAILED"));
    const parsed:Record<string,unknown>=(()=>{
      try{return JSON.parse(response) as Record<string,unknown>;}
      catch{return fail("REMOTE_PROOF_UNPARSEABLE");}
    })();
    if(parsed.status!=="G4_OVH_REMOTE_READBACK_VERIFIED"
      ||parsed.source!==NEW_SOURCE
      ||parsed.fileCount!==Object.keys(files).length
      ||parsed.totalBytes!==Object.values(actual).reduce((n,f)=>n+f.bytes,0))
      fail("REMOTE_PROOF_INVALID");
  }
  async verifyCanonical(input:{directory:string;manifest:ExperimentRawArchiveManifest}){
    const {directory,manifest:m}=input;
    if(!/^experiment_[0-9a-f-]{36}$/.test(m.experimentId)
      ||!directory.endsWith(m.experimentId))fail("EXPERIMENT_IDENTITY");
    const manifestPath=join(directory,"raw-manifest.json");
    const buf=await readFile(manifestPath);
    if(JSON.stringify(JSON.parse(buf.toString("utf8")))!==JSON.stringify(m))
      fail("SOURCE_MANIFEST_MISMATCH");
    const remote=DESTINATION+"/experiments/"+m.experimentId;
    return this.prove({localDirectory:directory,remoteDirectory:remote,
      kind:"experiment_canonical",files:{
        "raw-manifest.json":{file:"raw-manifest.json",bytes:buf.length,
          sha256:createHash("sha256").update(buf).digest("hex")},
        "canonical-events.jsonl":m.rawEvents,
        "final-report.json":m.finalReport,
        "canonical-rollup.json":m.canonicalRollup,
      }});
  }
  async verifyJournalBucket(input:{directory:string;manifest:JournalBucketArchiveManifest}){
    const {directory,manifest:m}=input;
    const bucket=basename(resolve(directory));
    if(!component(m.retentionClass)||!component(bucket)
      ||!["raw_telemetry","monitor_control","operational_audit"].includes(m.retentionClass))
      fail("JOURNAL_BUCKET_SCOPE");
    const buf=await readFile(join(directory,"manifest.json"));
    if(JSON.stringify(JSON.parse(buf.toString("utf8")))!==JSON.stringify(m))
      fail("JOURNAL_MANIFEST_MISMATCH");
    return this.prove({localDirectory:directory,kind:"journal_bucket",
      remoteDirectory:DESTINATION+"/journal/"+m.retentionClass+"/"+bucket,
      files:{
        "manifest.json":{file:"manifest.json",bytes:buf.length,
          sha256:createHash("sha256").update(buf).digest("hex")},
        "events.jsonl":m.rawEvents,
      }});
  }
  async verifyRecovery(input:{directory:string;manifest:RecoveryArchiveManifest}){
    const {directory,manifest:m}=input, bucket=basename(resolve(directory));
    if(!component(bucket)) fail("RECOVERY_SCOPE");
    const buf=await readFile(join(directory,"manifest.json"));
    if(JSON.stringify(JSON.parse(buf.toString("utf8")))!==JSON.stringify(m)) fail("RECOVERY_MANIFEST_MISMATCH");
    return this.prove({localDirectory:directory,kind:"engine_recovery",
      remoteDirectory:DESTINATION+"/recovery/"+bucket, files:{
        "manifest.json":{file:"manifest.json",bytes:buf.length,sha256:createHash("sha256").update(buf).digest("hex")},
        "checkpoints.json":m.checkpoints,"durable-transitions.jsonl":m.durableTransitions}});
  }
}


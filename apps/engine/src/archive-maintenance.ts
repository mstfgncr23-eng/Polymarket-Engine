import {readFile} from "node:fs/promises";
import {LOCAL_ARCHIVE_OPERATIONAL_EVENT_TYPES} from "@pm/core";
import {PostgresEventJournal} from "@pm/db";
import {LocalJournalArchive} from "./archive.js";
import {verifyRecoveryDirectory} from "./archive-recovery.js";
import {OvhArchiveReplicaVerifier} from "./ovh-archive-replica.js";

/** One-shot archive evidence tool. It never starts Engine or Guardian, migrates,
 * purges, or writes to PostgreSQL. Normal retention stays in RetentionManager. */
async function main(){
  const [action,directory]=process.argv.slice(2);
  if(action==="verify-recovery"){
    if(!directory)throw Error("RESTORE_DIRECTORY_REQUIRED");
    console.log(JSON.stringify(await verifyRecoveryDirectory(directory)));return;
  }
  if(!["export-recovery","export-bucket"].includes(action??""))throw Error("ARCHIVE_ACTION_INVALID");
  const file=process.env.DATABASE_URL_FILE;if(!file)throw Error("DATABASE_URL_FILE_REQUIRED");
  const journal=new PostgresEventJournal((await readFile(file,"utf8")).trim(),"unused",{access:"readonly"});
  const archive=new LocalJournalArchive(process.env.LOCAL_ARCHIVE_DIR??"/data/archive");
  const remote=OvhArchiveReplicaVerifier.forEngine(process.env.ARCHIVE_REMOTE_HOST??"g4archive@141.95.162.252");
  try{
    if(action==="export-recovery"){
      const retained=(await journal.listVerifiedCheckpoints()).slice(0,2);
      const result=await archive.ensureRecoveryBundle(journal,{retained,candidates:[],prefixCursor:Number(retained[1]?.payload.durableCursor)||0,maxPrefixRows:200});
      if(!result.verified||!result.manifest)throw Error("ARCHIVE_NOT_VERIFIED");
      const recovery=await verifyRecoveryDirectory(result.directory);
      await remote.verifyRecovery({directory:result.directory,manifest:result.manifest});
      console.log(JSON.stringify({status:"PASSED",action,localDirectory:result.directory,manifest:result.manifest,recovery:recovery.results,remote:"existing restricted rsync + forced-command SHA-256 readback",postgresMutations:0}));
    }else{
      const cutoff=new Date(Date.now()-24*3600000);
      const eligible=await journal.countEligible([...LOCAL_ARCHIVE_OPERATIONAL_EVENT_TYPES],cutoff);
      if(!eligible.oldestOccurredAt)throw Error("NO_EXPIRED_OPERATIONAL_BUCKET");
      const from=new Date(Math.floor(Date.parse(eligible.oldestOccurredAt)/3600000)*3600000),before=new Date(from.getTime()+3600000);
      if(before>cutoff)throw Error("NO_FULL_EXPIRED_BUCKET");
      const result=await archive.ensureJournalBucket(journal,{retentionClass:"operational_audit",eventTypes:LOCAL_ARCHIVE_OPERATIONAL_EVENT_TYPES,occurredFrom:from,occurredBefore:before,commitSeqTo:await journal.latestCommitSeq(),recordedAtTo:new Date()});
      if(!result.verified||!result.manifest)throw Error("ARCHIVE_NOT_VERIFIED");
      await remote.verifyJournalBucket({directory:result.directory,manifest:result.manifest});
      console.log(JSON.stringify({status:"PASSED",action,localDirectory:result.directory,manifest:result.manifest,postgresMutations:0}));
    }
  }finally{await journal.close();}
}
main().catch(error=>{
  const message=error instanceof Error?error.message:"UNKNOWN";
  const reason=/^(ARCHIVE|RESTORE|OVH|RECOVERY|LOCAL|NO_|DATABASE_URL_FILE)/.test(message)?message.slice(0,240):"ARCHIVE_IO_OR_DATABASE_FAILURE";
  console.error(JSON.stringify({status:"FAILED",reason}));process.exitCode=1;
});

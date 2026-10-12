import {execFileSync} from "node:child_process";
import {createRequire} from "node:module";
import {randomUUID} from "node:crypto";
import {PostgresEventJournal} from "@pm/db";
const postgres=createRequire(new URL("../packages/db/package.json",import.meta.url))("postgres");

/** Reuse an existing local PostgreSQL; isolate all writes in a new temporary schema.
 * No new database, role, service, migration, or access to production. */
export async function isolatedPostgres() {
  const container=process.env.SIMPLE_TEST_PG_CONTAINER??"g5-h2-crash-55485";
  const inspect=JSON.parse(execFileSync("docker",["inspect",container],{encoding:"utf8"}))[0];
  const env=Object.fromEntries(inspect.Config.Env.map((s:string)=>{const i=s.indexOf("=");return[s.slice(0,i),s.slice(i+1)];}));
  const port=inspect.NetworkSettings.Ports["5432/tcp"]?.find((p:{HostIp:string})=>p.HostIp==="127.0.0.1")?.HostPort;
  if(!port || !/^\d+$/.test(port))throw Error("LOCAL_PG_REQUIRED");
  const url=new URL("postgres://127.0.0.1:"+port+"/"+env.POSTGRES_DB);
  url.username=env.POSTGRES_USER;url.password=env.POSTGRES_PASSWORD;
  const admin=postgres(url.toString(),{max:2,onnotice:()=>undefined});
  const schema="simple_retention_"+randomUUID().replaceAll("-","");
  const before=Number((await admin`select count(*) as n from public.durable_transitions`)[0].n);
  await admin.unsafe('CREATE SCHEMA "'+schema+'"');
  const tables=await admin`select tablename from pg_tables where schemaname='public'`;
  for(const table of tables) {
    if(!/^[a-z_0-9]+$/.test(table.tablename))throw Error("UNEXPECTED_TABLE_NAME");
    await admin.unsafe(`CREATE TABLE "${schema}"."${table.tablename}" (LIKE public."${table.tablename}" INCLUDING ALL)`);
  }
  url.searchParams.set("options","-c search_path="+schema);
  const journal=new PostgresEventJournal(url.toString(),"unused");
  const closeJournal=journal.close.bind(journal);
  journal.close=async()=>undefined; // Engine restart shares only the isolated DB fixture.
  const sql=postgres(url.toString(),{max:2});
  journal.initialize=async()=>{if((await sql`select current_schema() as s`)[0].s!==schema)throw Error("SCHEMA_ISOLATION_FAILED");};
  return {journal,sql,async close(){
    await closeJournal();await sql.end();
    const after=Number((await admin`select count(*) as n from public.durable_transitions`)[0].n);
    if(!/^simple_retention_[a-f0-9]{32}$/.test(schema))throw Error("UNSAFE_SCHEMA_CLEANUP");
    await admin.unsafe('DROP SCHEMA "'+schema+'" CASCADE');await admin.end();
    if(before!==after)throw Error("PUBLIC_DATA_CHANGED");
  }};
}

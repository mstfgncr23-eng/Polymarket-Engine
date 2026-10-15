import { z } from "zod";
const counters = z.object({ disconnects:z.number().int().nonnegative(), reconnectAttempts:z.number().int().nonnegative(),
  reconnects:z.number().int().nonnegative(), overflows:z.number().int().nonnegative(), slowConsumers:z.number().int().nonnegative() });
export const experimentWsStatsSchema = counters.extend({ schemaVersion:z.literal(1), processRestarts:z.number().int().nonnegative(),
  reasons:z.record(z.string(),z.number().int().nonnegative()),
  shards:z.array(counters.extend({shard:z.number().int().min(1).max(4)})).length(4), lastEventAt:z.string().nullable() });
export type ExperimentWsStats = z.infer<typeof experimentWsStatsSchema>;
export const experimentWsEventSchema = z.object({ eventId:z.string().min(1).max(120),
  kind:z.enum(["CONNECTING","CONNECTED","DISCONNECTED","STALE","STOPPED","PROCESS_RESTART"]),
  shard:z.number().int().min(0).max(4), retry:z.boolean(), at:z.string().datetime({offset:true}),
  reason:z.enum(["count_limit","byte_limit","slow_consumer","clean_remote_close","abnormal_remote_close","connect_failure",
    "stale_heartbeat","socket_error","generation_change","operator_shutdown","unknown"]) });
export type ExperimentWsEvent = z.infer<typeof experimentWsEventSchema>;
const zero=()=>({disconnects:0,reconnectAttempts:0,reconnects:0,overflows:0,slowConsumers:0});
export function emptyExperimentWsStats(): ExperimentWsStats {
  return {...zero(),schemaVersion:1,processRestarts:0,reasons:{},shards:[1,2,3,4].map(shard=>({...zero(),shard})),lastEventAt:null};
}
/** Called only for a newly applied durable event. Operation-key/cursor fences
 * own deduplication; resettable process counter differences are never used. */
export function applyExperimentWsEvent(previous: ExperimentWsStats, input: ExperimentWsEvent): ExperimentWsStats {
  const event=experimentWsEventSchema.parse(input), next=structuredClone(previous);
  for(const t of [next,...next.shards.filter(s=>s.shard===event.shard)]) {
    if(event.kind==="DISCONNECTED") {
      t.disconnects++;
      if(event.reason==="count_limit"||event.reason==="byte_limit")t.overflows++;
      if(event.reason==="slow_consumer")t.slowConsumers++;
    }
    if(event.kind==="CONNECTING"&&event.retry)t.reconnectAttempts++;
    if(event.kind==="CONNECTED"&&event.retry)t.reconnects++;
  }
  if(event.kind==="DISCONNECTED")next.reasons[event.reason]=(next.reasons[event.reason]??0)+1;
  if(event.kind==="PROCESS_RESTART")next.processRestarts++;
  if(!next.lastEventAt||Date.parse(event.at)>Date.parse(next.lastEventAt))next.lastEventAt=event.at;
  return next;
}
export function marketWsReason(message?: string): ExperimentWsEvent["reason"] {
  if(message?.includes("MARKET_INGRESS_OVERFLOW:count_limit"))return "count_limit";
  if(message?.includes("MARKET_INGRESS_OVERFLOW:byte_limit"))return "byte_limit";
  const kind=/CLOB_MARKET_CLOSE:kind=([a-z_]+)/.exec(message??"")?.[1];
  const parsed=experimentWsEventSchema.shape.reason.safeParse(kind);
  return parsed.success?parsed.data:"unknown";
}

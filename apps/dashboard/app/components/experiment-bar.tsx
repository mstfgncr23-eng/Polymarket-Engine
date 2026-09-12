import type { DashboardSnapshot } from "../../lib/types";
import { formatAge, referenceTone, toneForState } from "../../lib/dashboard-selectors";
import { StatusPill } from "./primitives";

export function ExperimentBar({ snapshot, stream }: { snapshot: DashboardSnapshot | null; stream: "CONNECTING" | "LIVE" | "DEGRADED" }) {
  const health = snapshot?.health;
  const engineState = snapshot ? (snapshot.engine.alive && snapshot.engine.ready ? "ALIVE" : snapshot.engine.draining ? "DRAINING" : "NOT READY") : "UNKNOWN";
  const database = health?.database ?? "UNKNOWN";
  const reference = health?.referenceFeeds?.length ? `${health.referenceFeeds.filter((feed) => feed.state === "CONNECTED").length}/${health.referenceFeeds.length} CONNECTED` : health?.btcWs.state ?? "UNKNOWN";
  const referenceHealth = health?.referenceFeeds?.length ? referenceTone(snapshot!) : toneForState(health?.btcWs.state);
  const discovery = snapshot?.engine.discoveryStatus ?? health?.discoveryStatus ?? "UNKNOWN";
  const monitoring = snapshot?.engine.marketMonitoring ? "MONITORING" : "IDLE";
  return <header className="experimentBar">
    <div className="brandLockup"><span className="brandMark">✦</span><div><p className="eyebrow">POLYMARKET // PAPER CONTROL PLANE</p><h1>Galaxy System Dashboard</h1></div></div>
    <div className="experimentReadout"><span className="paperBadge">PAPER MODE</span><span className="readoutItem"><small>EXPERIMENT</small><strong>{snapshot?.engine.experimentStatus ?? "—"}</strong></span><span className="readoutItem"><small>ENGINE</small><strong>{snapshot ? engineState : "—"}</strong></span><span className="readoutItem"><small>DB</small><strong>{database}</strong></span></div>
    <div className="healthStrip" aria-label="Live transport and system health">
      <StatusPill label={`TRANSPORT ${stream}`} tone={stream === "LIVE" ? "healthy" : stream === "DEGRADED" ? "critical" : "warning"} />
      <StatusPill label={`REF ${reference}`} tone={referenceHealth} />
      <StatusPill label={`DISCOVERY ${discovery}`} tone={toneForState(discovery)} />
      <StatusPill label={`MARKETS ${monitoring}`} tone={toneForState(monitoring)} />
    </div>
    <p className="freshnessLine">Snapshot {formatAge(snapshot?.generatedAt)} · experiment elapsed time unavailable from this snapshot</p>
  </header>;
}

import type { DashboardSnapshot } from "../../lib/types";
import { formatAge, formatNumber, formatTime, referenceTone, toneForState } from "../../lib/dashboard-selectors";
import { DataValue, Metric, Panel, StatusPill } from "./primitives";

function ConnectionRow({ label, connection, generatedAt }: { label: string; connection: { state: string; lastMessageAt: string | null; reconnects: number }; generatedAt: string }) {
  return <div className="healthRow"><div><strong>{label}</strong><span>{connection.state} · {formatAge(connection.lastMessageAt, generatedAt)}</span></div><StatusPill label={`${connection.reconnects} RECONNECTS`} tone={toneForState(connection.state)} /></div>;
}

export function SystemHealth({ snapshot }: { snapshot: DashboardSnapshot }) {
  const feeds = snapshot.health.referenceFeeds ?? [];
  const engineState = snapshot.health.engineAlive ?? snapshot.engine.alive ?? snapshot.engine.ready;
  return <Panel title="System Health" kicker="RUNTIME OBSERVABILITY" meta={`snapshot ${formatTime(snapshot.generatedAt)}`}>
    <div className="metricGrid metricGrid--four"><Metric label="Engine" value={engineState ? "ALIVE" : "DOWN"} detail={`uptime ${formatNumber(snapshot.engine.uptimeSeconds, 0)}s`} tone={engineState ? "healthy" : "critical"} /><Metric label="Database" value={snapshot.health.database} detail="read path" tone={toneForState(snapshot.health.database)} /><Metric label="Event latency" value={snapshot.health.eventLatencyMs === null ? "—" : `${formatNumber(snapshot.health.eventLatencyMs, 0)}ms`} detail="latest observed" tone={snapshot.health.eventLatencyMs === null ? "neutral" : "healthy"} /><Metric label="Queue depth" value={formatNumber(snapshot.engine.queueDepth, 0)} detail={snapshot.engine.overloadState ?? "normal"} tone={snapshot.engine.overloadState && snapshot.engine.overloadState !== "NORMAL" ? "warning" : "healthy"} /></div>
    <div className="healthGrid"><div><ConnectionRow label="Polymarket WebSocket" connection={snapshot.health.polymarketWs} generatedAt={snapshot.generatedAt} /><ConnectionRow label="BTC WebSocket" connection={snapshot.health.btcWs} generatedAt={snapshot.generatedAt} /><div className="healthRow"><div><strong>Reference feeds</strong><span>{feeds.length} dynamic feed{feeds.length === 1 ? "" : "s"}</span></div><StatusPill label={feeds.length ? "REPORTING" : "NO DATA"} tone={referenceTone(snapshot)} /></div></div><div className="healthNotes"><DataValue label="Discovery" value={snapshot.health.discoveryStatus ?? snapshot.engine.discoveryStatus ?? "—"} /><DataValue label="Monitoring" value={snapshot.health.marketMonitoring ? "ACTIVE" : "OFF"} /><DataValue label="Journal" value={snapshot.health.journalDegraded ? "DEGRADED" : "OK"} /><DataValue label="Telemetry" value={snapshot.health.telemetryDegraded ? "DEGRADED" : "OK"} /><DataValue label="Last BTC message" value={formatAge(snapshot.health.lastBtcMessageAt, snapshot.generatedAt)} /></div></div>
  </Panel>;
}

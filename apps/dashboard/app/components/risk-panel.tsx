import type { DashboardSnapshot } from "../../lib/types";
import { formatPercent, formatTime } from "../../lib/dashboard-selectors";
import { DataValue, EmptyState, Metric, Panel, StatusPill } from "./primitives";

export function RiskPanel({ snapshot }: { snapshot: DashboardSnapshot }) {
  const risk = snapshot.risk;
  return <Panel title="Risk Guardian" kicker="CAPITAL PROTECTION" meta={risk ? formatTime(risk.evaluatedAt) : "not evaluated"}>
    {!risk ? <EmptyState title="No risk evaluation" detail="Risk state is intentionally blank until the engine persists an evaluation." /> : <>
      <div className="riskHero"><StatusPill label={risk.accepted ? "ACCEPTED" : "VETOED"} tone={risk.accepted ? "healthy" : "critical"} /><strong>{risk.accepted ? "Risk checks passed" : "Execution blocked by risk"}</strong><span>{risk.vetoes.length} vetoes persisted</span></div>
      <div className="metricGrid metricGrid--four"><Metric label="Utilization checks" value={String(Object.keys(risk.utilization).length)} detail="reported dimensions" /><Metric label="Vetoes" value={String(risk.vetoes.length)} detail="persisted reasons" tone={risk.vetoes.length ? "critical" : "healthy"} /><Metric label="Evaluated" value={formatTime(risk.evaluatedAt)} detail="risk timestamp" /><Metric label="Decision" value={risk.accepted ? "PASS" : "BLOCK"} detail="authoritative risk state" tone={risk.accepted ? "healthy" : "critical"} /></div>
      <div className="utilizationList">{Object.entries(risk.utilization).length === 0 ? <span className="muted">No utilization dimensions reported.</span> : Object.entries(risk.utilization).map(([key, value]) => <div className="utilizationRow" key={key}><span>{key}</span><div className="utilizationBar"><i style={{ width: `${Math.max(0, Math.min(100, value * 100))}%` }} /></div><DataValue label="" value={formatPercent(value)} /></div>)}</div>
      <div className="vetoList">{risk.vetoes.length === 0 ? <span className="muted">No veto reasons persisted.</span> : risk.vetoes.map((veto) => <span key={veto}>⚠ {veto}</span>)}</div>
    </>}
  </Panel>;
}

import type { DashboardSnapshot } from "../../lib/types";
import { decisionActivityEvents, decisionActivityLabel, eventSummary, formatPercent, formatTime, latest } from "../../lib/dashboard-selectors";
import { DataValue, EmptyState, Metric, Panel, StatusPill } from "./primitives";

function pendingRiskState(snapshot: DashboardSnapshot): { title: string; detail: string; at: string | null } {
  if (snapshot.consensus) {
    return {
      title: "DECISION READY · RISK PENDING",
      detail: "An authoritative economic decision exists, but no risk evaluation has been persisted yet.",
      at: snapshot.consensus.calculatedAt,
    };
  }

  const activity = latest(decisionActivityEvents(snapshot));
  if (activity) {
    if (activity.type === "ASTRA_DECISION" && activity.detail.accepted === false) {
      const reason = typeof activity.detail.gateReason === "string" ? activity.detail.gateReason : "deterministic gate rejection";
      return { title: "RISK NOT REACHED", detail: `Astra rejected the opportunity before risk evaluation: ${reason}.`, at: activity.at };
    }
    if (activity.type === "ASTRA_FAILURE") {
      return { title: "RISK NOT REACHED", detail: `Astra failed before producing an executable proposal: ${eventSummary(activity)}.`, at: activity.at };
    }
    if (activity.type === "ASTRA_BUDGET_BLOCK") {
      return { title: "RISK NOT REACHED", detail: "A historical Astra budget gate blocked model dispatch before risk evaluation.", at: activity.at };
    }
    if (activity.type === "BOOK_REVALIDATION_REJECTED") {
      return { title: "RISK / EXECUTION FENCE", detail: `The current book invalidated the proposal before execution: ${eventSummary(activity)}.`, at: activity.at };
    }
    return { title: "DECISION PIPELINE ACTIVE", detail: `${decisionActivityLabel(activity)} observed. No authoritative risk evaluation has been persisted yet.`, at: activity.at };
  }

  const trigger = snapshot.recentTriggers[0] ?? null;
  if (trigger && snapshot.engine.experimentStatus === "RUNNING") {
    return { title: "TRIGGER OBSERVED · RISK WAITING", detail: `${trigger.kind} entered the decision pipeline. Risk waits for an executable economic decision.`, at: trigger.createdAt };
  }

  return { title: "No risk evaluation", detail: "Risk state is blank because no authoritative evaluation has been persisted.", at: null };
}

export function RiskPanel({ snapshot }: { snapshot: DashboardSnapshot }) {
  const risk = snapshot.risk;
  const pending = pendingRiskState(snapshot);
  return <Panel title="Risk Guardian" kicker="CAPITAL PROTECTION" meta={risk ? formatTime(risk.evaluatedAt) : pending.at ? formatTime(pending.at) : "not evaluated"}>
    {!risk ? <EmptyState title={pending.title} detail={pending.detail} /> : <>
      <div className="riskHero"><StatusPill label={risk.accepted ? "ACCEPTED" : "VETOED"} tone={risk.accepted ? "healthy" : "critical"} /><strong>{risk.accepted ? "Risk checks passed" : "Execution blocked by risk"}</strong><span>{risk.vetoes.length} vetoes persisted</span></div>
      <div className="metricGrid metricGrid--four"><Metric label="Utilization checks" value={String(Object.keys(risk.utilization).length)} detail="reported dimensions" /><Metric label="Vetoes" value={String(risk.vetoes.length)} detail="persisted reasons" tone={risk.vetoes.length ? "critical" : "healthy"} /><Metric label="Evaluated" value={formatTime(risk.evaluatedAt)} detail="risk timestamp" /><Metric label="Decision" value={risk.accepted ? "PASS" : "BLOCK"} detail="authoritative risk state" tone={risk.accepted ? "healthy" : "critical"} /></div>
      <div className="utilizationList">{Object.entries(risk.utilization).length === 0 ? <span className="muted">No utilization dimensions reported.</span> : Object.entries(risk.utilization).map(([key, value]) => <div className="utilizationRow" key={key}><span>{key}</span><div className="utilizationBar"><i style={{ width: `${Math.max(0, Math.min(100, value * 100))}%` }} /></div><DataValue label="" value={formatPercent(value)} /></div>)}</div>
      <div className="vetoList">{risk.vetoes.length === 0 ? <span className="muted">No veto reasons persisted.</span> : risk.vetoes.map((veto) => <span key={veto}>⚠ {veto}</span>)}</div>
    </>}
  </Panel>;
}

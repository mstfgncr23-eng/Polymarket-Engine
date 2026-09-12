import type { DashboardSnapshot } from "../../lib/types";
import { AGENT_ROLES, formatAge, formatTime } from "../../lib/dashboard-selectors";
import { EmptyState, Metric, Panel, StatusPill } from "./primitives";

export function ExternalResearch({ snapshot }: { snapshot: DashboardSnapshot }) {
  const activity = snapshot.grokActivity;
  const hasAgents = Object.keys(snapshot.latestAgents).length > 0;
  return <Panel title="External Research / Diagnostic Only" kicker="SECONDARY SIGNAL CONTEXT" meta="Grok non-authoritative" className="researchPanel">
    <div className="researchBanner"><StatusPill label="NON-AUTHORITATIVE" tone="warning" /><span>External research can inform diagnostics; it never decides, overrides, or becomes the visual center.</span></div>
    <div className="metricGrid metricGrid--four"><Metric label="Accepted signals" value={String(activity.acceptedSignals)} detail="snapshot counters" /><Metric label="Rejected" value={String(activity.rejectedSignals)} detail="snapshot counters" tone={activity.rejectedSignals ? "warning" : "neutral"} /><Metric label="Duplicates" value={String(activity.duplicates)} detail="snapshot counters" /><Metric label="Unauthorized calls" value={String(activity.unauthorizedCalls)} detail="policy telemetry" tone={activity.unauthorizedCalls ? "critical" : "healthy"} /></div>
    {!hasAgents ? <EmptyState title="No external research signals" detail="Agent signal state is empty in the current snapshot." /> : <div className="agentGrid">{AGENT_ROLES.map(([role, label]) => { const agent = snapshot.latestAgents[role]; return <div className="agentCard" key={role}><div><strong>{label}</strong><span>{agent?.agentId ?? role}</span></div><StatusPill label={agent?.resultingDecision ?? "NO SIGNAL"} tone={agent ? "warning" : "neutral"} /><small>{agent ? `${formatTime(agent.receivedAt)} · ${formatAge(agent.receivedAt, snapshot.generatedAt)}` : "No latest signal"}</small></div>; })}</div>}
    <div className="panelFoot"><span className="muted">Last external signal: {formatTime(activity.lastSeenAt)}</span><span className="muted">Rate limited: {activity.rateLimitedCalls}</span></div>
  </Panel>;
}

import type { DashboardSnapshot } from "../../lib/types";
import { eventSummary, formatTime, missionEvents, toneForState } from "../../lib/dashboard-selectors";
import { EmptyState, Panel, StatusPill } from "./primitives";

export function MissionTimeline({ snapshot }: { snapshot: DashboardSnapshot }) {
  const events = missionEvents(snapshot);
  return <Panel title="Mission Timeline" kicker="LIVE DECISION FEED" meta={`${events.length} mission events`}>
    {events.length === 0 ? <EmptyState title="No mission events" detail="The authoritative decision feed has not reported a timeline event." /> : <div className="timeline">{events.slice(0, 16).map((event) => { const tone = event.type === "RISK_VETO" || event.type === "PAPER_EXECUTION_ERROR" || event.type === "ASTRA_FAILURE" ? "critical" : event.type === "RISK_ACCEPT" || event.type === "PAPER_FILL" || event.type === "ASTRA_DECISION" ? "healthy" : event.type === "ASTRA_PROVIDER_DISPATCH_STARTED" ? "warning" : toneForState("CONNECTED"); return <div className={`timelineItem timelineItem--${tone}`} key={event.id}><div className="timelineRail"><i /></div><div className="timelineContent"><div><strong>{event.type}</strong><StatusPill label={event.marketId ? "MARKET" : "SYSTEM"} tone={tone} /></div><span>{eventSummary(event)}</span><small>{formatTime(event.at)} · trace {event.traceId}</small></div></div>; })}</div>}
  </Panel>;
}

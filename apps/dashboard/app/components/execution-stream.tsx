import type { DashboardSnapshot } from "../../lib/types";
import { decisionActivityEvents, eventSummary, executionEvents, formatTime, latest, shortId, toneForState } from "../../lib/dashboard-selectors";
import { EmptyState, Panel, StatusPill } from "./primitives";

function detailValue(detail: Record<string, unknown>, ...keys: string[]): string {
  for (const key of keys) if (detail[key] !== undefined && detail[key] !== null) return String(detail[key]);
  return "—";
}

function pendingExecutionState(snapshot: DashboardSnapshot): { title: string; detail: string } {
  if (snapshot.risk) {
    if (!snapshot.risk.accepted) return { title: "EXECUTION BLOCKED BY RISK", detail: `Risk vetoed the candidate: ${snapshot.risk.vetoes.join(", ") || "unspecified veto"}.` };
    return { title: "RISK PASSED · EXECUTION WAITING", detail: "Risk accepted the candidate, but no paper order/fill/cancel/error has been persisted yet." };
  }
  if (snapshot.consensus) return { title: "DECISION READY · RISK WAITING", detail: "An authoritative economic decision exists. Execution waits for the deterministic risk layer." };

  const activity = latest(decisionActivityEvents(snapshot));
  if (activity) return { title: "DECISION PIPELINE HAS NOT REACHED EXECUTION", detail: `${eventSummary(activity)}. No authoritative risk or paper execution event is present yet.` };

  const trigger = snapshot.recentTriggers[0] ?? null;
  if (trigger && snapshot.engine.experimentStatus === "RUNNING") return { title: "TRIGGER OBSERVED · EXECUTION WAITING", detail: `${trigger.kind} entered the decision pipeline. No executable decision has reached risk/execution yet.` };

  return { title: "No paper execution events", detail: "Orders, fills, cancels, and errors will appear here when persisted." };
}

export function ExecutionStream({ snapshot }: { snapshot: DashboardSnapshot }) {
  const events = executionEvents(snapshot);
  const latestEvent = latest(events);
  const pending = events.length === 0 ? pendingExecutionState(snapshot) : null;
  return <Panel title="Execution Stream" kicker="PAPER ONLY" meta={latestEvent ? formatTime(latestEvent.at) : "no execution events"}>
    {events.length === 0 ? <EmptyState title={pending?.title} detail={pending?.detail} /> : <div className="eventList">{events.slice(0, 10).map((event) => { const error = event.type === "PAPER_EXECUTION_ERROR"; const detail = event.detail; return <article className={`eventRow eventRow--${error ? "critical" : event.type === "PAPER_ORDER_CANCELLED" ? "warning" : "healthy"}`} key={event.id}><div className="eventMarker" /><div className="eventBody"><div className="eventHeading"><strong>{event.type.replace("PAPER_", "")}</strong><StatusPill label={error ? "ERROR" : "PAPER"} tone={error ? "critical" : toneForState("CONNECTED")} /></div><span>{eventSummary(event)}</span><small>{formatTime(event.at)} · market {shortId(event.marketId)}</small>{event.type === "PAPER_FILL" && <div className="fillDetail"><span>Fill {detailValue(detail, "fillId")}</span><span>Order {detailValue(detail, "orderId")}</span><span>Price {detailValue(detail, "price", "fillPrice")}</span><span>Qty {detailValue(detail, "quantity")}</span><span>Role {detailValue(detail, "role", "liquidityRole")}</span></div>}</div></article>; })}</div>}
  </Panel>;
}

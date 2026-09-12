import type { DashboardSnapshot } from "../../lib/types";
import { eventSummary, executionEvents, formatTime, latest, shortId, toneForState } from "../../lib/dashboard-selectors";
import { EmptyState, Panel, StatusPill } from "./primitives";

function detailValue(detail: Record<string, unknown>, ...keys: string[]): string {
  for (const key of keys) if (detail[key] !== undefined && detail[key] !== null) return String(detail[key]);
  return "—";
}

export function ExecutionStream({ snapshot }: { snapshot: DashboardSnapshot }) {
  const events = executionEvents(snapshot);
  const latestEvent = latest(events);
  return <Panel title="Execution Stream" kicker="PAPER ONLY" meta={latestEvent ? formatTime(latestEvent.at) : "no execution events"}>
    {events.length === 0 ? <EmptyState title="No paper execution events" detail="Orders, fills, cancels, and errors will appear here when persisted." /> : <div className="eventList">{events.slice(0, 10).map((event) => { const error = event.type === "PAPER_EXECUTION_ERROR"; const detail = event.detail; return <article className={`eventRow eventRow--${error ? "critical" : event.type === "PAPER_ORDER_CANCELLED" ? "warning" : "healthy"}`} key={event.id}><div className="eventMarker" /><div className="eventBody"><div className="eventHeading"><strong>{event.type.replace("PAPER_", "")}</strong><StatusPill label={error ? "ERROR" : "PAPER"} tone={error ? "critical" : toneForState("CONNECTED")} /></div><span>{eventSummary(event)}</span><small>{formatTime(event.at)} · market {shortId(event.marketId)}</small>{event.type === "PAPER_FILL" && <div className="fillDetail"><span>Fill {detailValue(detail, "fillId")}</span><span>Order {detailValue(detail, "orderId")}</span><span>Price {detailValue(detail, "price", "fillPrice")}</span><span>Qty {detailValue(detail, "quantity")}</span><span>Role {detailValue(detail, "role", "liquidityRole")}</span></div>}</div></article>; })}</div>}
  </Panel>;
}

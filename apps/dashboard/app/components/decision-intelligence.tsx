import type { DashboardSnapshot } from "../../lib/types";
import { decisionAuthorityLabel, decisionAuthorityTone, eventSummary, formatBasisPoints, formatPercent, formatTime, latest, nativeDecisionEvents, shortId } from "../../lib/dashboard-selectors";
import { DataValue, EmptyState, Metric, Panel, StatusPill } from "./primitives";

export function DecisionIntelligence({ snapshot }: { snapshot: DashboardSnapshot }) {
  const decision = snapshot.consensus;
  const event = latest(nativeDecisionEvents(snapshot));
  const vector = decision?.featureVector ?? {};
  return <Panel title="Latest Decisions" kicker="DECISION INTELLIGENCE" meta={decision ? formatTime(decision.calculatedAt) : "awaiting decision"}>
    {!decision ? <EmptyState title="AWAITING DECISION" detail="No consensus decision is present in the authoritative snapshot." /> : <>
      <div className="decisionHero"><div><span className="nodeEyebrow">DECISION AUTHORITY</span><strong>{decision.consensusDirection}</strong><span>{decisionAuthorityLabel(decision)}</span></div><StatusPill label={decisionAuthorityLabel(decision)} tone={decisionAuthorityTone(decision)} /></div>
      <div className="metricGrid metricGrid--four">
        <Metric label="Confidence" value={formatPercent(decision.consensusConfidence)} detail="consensus" />
        <Metric label="Fair probability" value={formatPercent(decision.fairProbability)} detail="decision output" />
        <Metric label="Market probability" value={formatPercent(decision.marketProbability)} detail="market input" />
        <Metric label="Estimated edge" value={formatBasisPoints(decision.estimatedEdge)} detail="decision calculation" tone={decision.estimatedEdge > 0 ? "healthy" : "warning"} />
      </div>
      <div className="decisionDetails"><DataValue label="Agreement" value={formatPercent(decision.agentAgreement)} /><DataValue label="Signal freshness" value={formatPercent(decision.signalFreshness)} /><DataValue label="Calculated" value={formatTime(decision.calculatedAt)} /><DataValue label="Decision event" value={event ? shortId(event.id) : "—"} /></div>
      {event && <div className="eventCallout"><span className="sectionLabel">Latest native event</span><strong>{eventSummary(event)}</strong><small>{formatTime(event.at)} · trace {shortId(event.traceId)}</small></div>}
      <div className="featureStrip"><span className="sectionLabel">Reference feature vector</span><div>{Object.keys(vector).length === 0 ? <span className="muted">No feature vector detail in snapshot</span> : Object.entries(vector).filter(([key]) => ["referenceMoveBps", "referenceWindowMs", "referenceVenue", "fairProbabilityUp", "marketProbability"].includes(key)).map(([key, value]) => <DataValue key={key} label={key} value={String(value ?? "—")} />)}</div></div>
    </>}
  </Panel>;
}

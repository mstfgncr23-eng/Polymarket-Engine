import type { DashboardSnapshot } from "../../lib/types";
import { currentConsensus, decisionActivityLabel, decisionActivityTone, decisionAuthorityLabel, decisionAuthorityTone, eventSummary, formatBasisPoints, formatPercent, formatTime, latestTrigger, pipelineDecisionActivity, shortId } from "../../lib/dashboard-selectors";
import { DataValue, EmptyState, Metric, Panel, StatusPill } from "./primitives";

export function DecisionIntelligence({ snapshot }: { snapshot: DashboardSnapshot }) {
  const decision = currentConsensus(snapshot);
  const event = pipelineDecisionActivity(snapshot);
  const trigger = latestTrigger(snapshot);
  const vector = decision?.featureVector ?? {};
  const meta = decision ? formatTime(decision.calculatedAt) : event ? formatTime(event.at) : trigger ? formatTime(trigger.createdAt) : "awaiting decision";

  return <Panel title="Latest Decisions" kicker="DECISION INTELLIGENCE" meta={meta}>
    {!decision ? <>
      {event
        ? <EmptyState title={decisionActivityLabel(event)} detail={`${eventSummary(event)}. No authoritative executable decision is present for the current pipeline.`} />
        : trigger && snapshot.engine.experimentStatus === "RUNNING"
          ? <EmptyState title="TRIGGER OBSERVED" detail={`${trigger.kind} entered the current decision pipeline. No authoritative economic decision has been produced for this trigger yet.`} />
          : <EmptyState title="AWAITING DECISION" detail="No current consensus decision is present in the authoritative snapshot." />}
      {event && <div className="eventCallout"><span className="sectionLabel">Current decision activity</span><strong><StatusPill label={decisionActivityLabel(event)} tone={decisionActivityTone(event)} /></strong><small>{formatTime(event.at)} · trace {shortId(event.traceId)}</small></div>}
    </> : <>
      <div className="decisionHero"><div><span className="nodeEyebrow">DECISION AUTHORITY</span><strong>{decision.consensusDirection}</strong><span>{decisionAuthorityLabel(decision)}</span></div><StatusPill label={decisionAuthorityLabel(decision)} tone={decisionAuthorityTone(decision)} /></div>
      <div className="metricGrid metricGrid--four">
        <Metric label="Confidence" value={formatPercent(decision.consensusConfidence)} detail="consensus" />
        <Metric label="Fair probability" value={formatPercent(decision.fairProbability)} detail="decision output" />
        <Metric label="Market probability" value={formatPercent(decision.marketProbability)} detail="market input" />
        <Metric label="Estimated edge" value={formatBasisPoints(decision.estimatedEdge)} detail="decision calculation" tone={decision.estimatedEdge > 0 ? "healthy" : "warning"} />
      </div>
      <div className="decisionDetails"><DataValue label="Agreement" value={formatPercent(decision.agentAgreement)} /><DataValue label="Signal freshness" value={formatPercent(decision.signalFreshness)} /><DataValue label="Calculated" value={formatTime(decision.calculatedAt)} /><DataValue label="Trace" value={shortId(decision.traceId)} /></div>
      {event && <div className="eventCallout"><span className="sectionLabel">Current decision activity</span><strong>{decisionActivityLabel(event)}</strong><span>{eventSummary(event)}</span><small>{formatTime(event.at)} · trace {shortId(event.traceId)}</small></div>}
      <div className="featureStrip"><span className="sectionLabel">Reference feature vector</span><div>{Object.keys(vector).length === 0 ? <span className="muted">No feature vector detail in snapshot</span> : Object.entries(vector).filter(([key]) => ["referenceMoveBps", "referenceWindowMs", "referenceVenue", "fairProbabilityUp", "marketProbability"].includes(key)).map(([key, value]) => <DataValue key={key} label={key} value={String(value ?? "—")} />)}</div></div>
    </>}
  </Panel>;
}

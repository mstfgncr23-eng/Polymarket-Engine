import type { DashboardSnapshot } from "../../lib/types";
import { decisionAuthorityLabel, decisionAuthorityTone, formatBasisPoints, formatPercent, formatTime } from "../../lib/dashboard-selectors";
import { StatusPill } from "./primitives";

export function NativeCore({ consensus, pulse }: { consensus: DashboardSnapshot["consensus"]; pulse?: string | null }) {
  return <article className="nativeCore" aria-labelledby="native-core-title">
    {pulse && <span className="corePulse" key={pulse} aria-hidden="true" />}
    <div className="coreHalo coreHalo--one" /><div className="coreHalo coreHalo--two" />
    <p className="coreEyebrow">CENTRAL AUTHORITY</p>
    <div className="coreGlyph" aria-hidden="true">✦</div>
    <h2 id="native-core-title">Native Decision Core</h2>
    <code>ARCHITECTURE · NATIVE_DETERMINISTIC_V1</code>
    <StatusPill label={consensus ? "DECISION READY" : "AWAITING DECISION"} tone={consensus ? "healthy" : "warning"} />
    {consensus && <StatusPill label={decisionAuthorityLabel(consensus)} tone={decisionAuthorityTone(consensus)} />}
    {consensus ? <div className="coreDecision"><strong>{consensus.consensusDirection}</strong><span>{formatPercent(consensus.consensusConfidence)} confidence</span></div> : <p className="coreEmpty">No consensus snapshot is available yet.</p>}
    {consensus && <div className="coreStats"><span><small>FAIR</small><b>{formatPercent(consensus.fairProbability)}</b></span><span><small>MARKET</small><b>{formatPercent(consensus.marketProbability)}</b></span><span><small>EDGE</small><b>{formatBasisPoints(consensus.estimatedEdge)}</b></span><span><small>CALCULATED</small><b>{formatTime(consensus.calculatedAt)}</b></span></div>}
  </article>;
}

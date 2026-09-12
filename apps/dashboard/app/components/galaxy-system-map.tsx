import type { DashboardSnapshot } from "../../lib/types";
import { formatMoney, formatTime, latest, pulseToken, referenceTone, triggerEvents } from "../../lib/dashboard-selectors";
import { Panel } from "./primitives";
import { NativeCore } from "./native-core";
import { ReferenceOrbit } from "./reference-orbit";
import { SystemNode } from "./system-node";

export function GalaxySystemMap({ snapshot }: { snapshot: DashboardSnapshot }) {
  const feeds = snapshot.health.referenceFeeds ?? [];
  const triggerEvent = latest(triggerEvents(snapshot));
  const trigger = latest(snapshot.recentTriggers);
  const risk = snapshot.risk;
  const execution = snapshot.decisionFeed.find((event) => event.type.startsWith("PAPER_") || event.type === "POSITION_SETTLED");
  const referenceState = referenceTone(snapshot);
  const marketTone = snapshot.engine.marketMonitoring ? "healthy" : snapshot.engine.discoveryStatus === "RECONCILING" ? "warning" : "neutral";
  const triggerSeverity = trigger?.severity ?? (triggerEvent ? Number(triggerEvent.type.split("_").at(-1)) : 0);
  const triggerTone = triggerSeverity >= 3 ? "critical" : trigger || triggerEvent ? "healthy" : "neutral";
  const riskTone = risk ? risk.accepted ? "healthy" : "critical" : "neutral";
  const executionTone = execution?.type === "PAPER_EXECUTION_ERROR" ? "critical" : execution?.type === "PAPER_ORDER_CANCELLED" ? "warning" : execution ? "healthy" : "neutral";
  const portfolioTone = snapshot.positions.length ? "healthy" : "neutral";
  return <Panel title="Galaxy System Map" kicker="LIVE ARCHITECTURE" meta="authoritative snapshot → bounded event stream" className="galaxyPanel">
    <div className="galaxyMap" aria-label="Data flow from market and reference data through the native decision core, risk, execution, and portfolio">
      <svg className="flowLines" viewBox="0 0 1200 570" role="img" aria-label="Market and reference data flow through Trigger Engine, Native Decision Core, Risk Guardian, Execution, and Portfolio">
        <defs><marker id="flow-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" /></marker></defs>
        <path className="flowLine flowLine--reference" d="M190 164 C315 164 355 260 454 260" markerEnd="url(#flow-arrow)" />
        <path className="flowLine" d="M190 405 C315 405 355 315 454 315" markerEnd="url(#flow-arrow)" />
        <path className="flowLine" d="M746 287 C825 287 850 180 930 180" markerEnd="url(#flow-arrow)" />
        <path className="flowLine" d="M930 250 C850 305 850 410 930 410" markerEnd="url(#flow-arrow)" />
        <path className="flowLine" d="M1060 452 C1120 452 1125 470 1150 470" markerEnd="url(#flow-arrow)" />
      </svg>
      <div className="mapNode mapNode--reference"><ReferenceOrbit feeds={feeds} overallTone={referenceState} /></div>
      <div className="mapNode mapNode--market"><SystemNode id="market-scanner" eyebrow="DISCOVERY" title="Market Scanner" status={snapshot.engine.marketMonitoring ? "MONITORING" : snapshot.engine.discoveryStatus ?? "WAITING"} tone={marketTone} metric={`${snapshot.engine.qualifiedMarketCount ?? 0} qualified`} detail={`${snapshot.scanner.length} live outcomes`} /></div>
      <div className="mapNode mapNode--trigger"><SystemNode id="trigger-engine" eyebrow="SIGNAL ROUTING" title="Trigger Engine" status={trigger || triggerEvent ? `LEVEL ${triggerSeverity}` : "QUIET"} tone={triggerTone} metric={trigger?.kind ?? (typeof triggerEvent?.detail.kind === "string" ? triggerEvent.detail.kind : "Awaiting trigger")} detail={trigger ? `${trigger.asset ?? "system"} · ${trigger.marketId ?? "unscoped"} · ${formatTime(trigger.createdAt)}` : triggerEvent ? `${triggerEvent.asset ?? "system"} · ${triggerEvent.marketId ?? "unscoped"}` : "No trigger event in feed"} pulse={pulseToken(snapshot, "trigger")} /></div>
      <div className="mapCore"><NativeCore consensus={snapshot.consensus} pulse={pulseToken(snapshot, "core")} /></div>
      <div className="mapNode mapNode--risk"><SystemNode id="risk-guardian" eyebrow="CAPITAL PROTECTION" title="Risk Guardian" status={risk ? risk.accepted ? "PASS" : "VETO" : "NOT RUN"} tone={riskTone} metric={risk ? `${risk.vetoes.length} vetoes` : "—"} detail={risk ? `evaluated ${formatTime(risk.evaluatedAt)}` : "No risk evaluation persisted"} pulse={pulseToken(snapshot, "risk")} /></div>
      <div className="mapNode mapNode--execution"><SystemNode id="execution-node" eyebrow="PAPER EXECUTION" title="Execution" status={executionTone === "critical" ? "ERROR" : execution ? execution.type.replace("PAPER_", "") : "IDLE"} tone={executionTone} metric={execution?.type ?? "Awaiting execution"} detail={execution?.marketId ?? "No execution event in feed"} pulse={pulseToken(snapshot, "execution")} /></div>
      <div className="mapNode mapNode--portfolio"><SystemNode id="portfolio-node" eyebrow="ACCOUNTING" title="Portfolio / Ledger" status={snapshot.positions.length ? "TRACKING" : "EMPTY"} tone={portfolioTone} metric={formatMoney(snapshot.balance.equity)} detail={`${snapshot.positions.length} positions · ${formatMoney(snapshot.balance.cash)} cash`} pulse={pulseToken(snapshot, "portfolio")} /></div>
      <div className="mapCaption"><span className="captionDot captionDot--healthy" /> healthy path <span className="captionDot captionDot--warning" /> awaiting / reconnecting <span className="captionDot captionDot--critical" /> veto / fault</div>
    </div>
  </Panel>;
}

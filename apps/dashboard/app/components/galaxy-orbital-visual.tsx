import type { CSSProperties } from "react";
import {
  buildGalaxyVisualSlots,
  coreCaptionLines,
  eventOrbitId,
  galaxyCoreIdentity,
  labelAnchor,
  layoutSemanticLabels,
  settlementAppliesToSlot,
  type GalaxyProjectedPoint,
  type GalaxyVisualSlot,
  type LaidOutSemanticLabel,
} from "../../lib/galaxy-presentation";
import { GALAXY_VIEW } from "../../lib/galaxy-presentation";
import { projectMarkerInField } from "../../lib/galaxy-field";
import { GALAXY_FPS, type GalaxySceneModel, type GalaxyTransient } from "../../lib/galaxy-scene";
import { GalaxyFieldCanvas } from "./galaxy-field-canvas";

export type GalaxyOrbitalVisualProps = {
  model: GalaxySceneModel;
  transients: GalaxyTransient[];
  frame: number;
  reducedMotion: boolean;
  durationInFrames: number;
  ambient?: boolean;
  forceFieldFallback?: boolean;
  freezeTimeSec?: number | null;
};

const MARKER_RADIUS: Record<string, number> = {
  trigger: 228,
  risk: 298,
  execution: 368,
  market: 438,
  portfolio: 500,
  reference: 540,
  position: 500,
  density: 438,
  halo: 148,
};

function toneColor(tone: string): string {
  if (tone === "healthy") return "#7dcea0";
  if (tone === "warning") return "#e8b86a";
  if (tone === "critical") return "#d46a6a";
  return "#8a7a64";
}

function eventColor(kind: GalaxyTransient["kind"]): string {
  if (kind === "risk-veto" || kind === "execution-error") return "#d46a6a";
  if (kind === "cancel") return "#e8b86a";
  if (kind === "risk-accept" || kind === "fill" || kind === "settlement") return "#7dcea0";
  if (kind === "reference") return "#c4a06a";
  return "#ffd27a";
}

function elapsedFrames(frame: number, event: GalaxyTransient, durationInFrames: number): number {
  return (frame - event.startFrame + durationInFrames) % durationInFrames;
}

function eventWindow(frame: number, event: GalaxyTransient | undefined, durationInFrames: number): number {
  return event ? elapsedFrames(frame, event, durationInFrames) : 999;
}

function ramp(value: number, stops: number[], values: number[]): number {
  if (value <= stops[0]!) return values[0]!;
  const last = stops.length - 1;
  if (value >= stops[last]!) return values[last]!;
  for (let index = 1; index < stops.length; index += 1) {
    if (value <= stops[index]!) {
      const span = stops[index]! - stops[index - 1]!;
      const amount = span === 0 ? 1 : (value - stops[index - 1]!) / span;
      return values[index - 1]! + (values[index]! - values[index - 1]!) * amount;
    }
  }
  return values[last]!;
}

function lerpPoint(from: GalaxyProjectedPoint, to: GalaxyProjectedPoint, amount: number): GalaxyProjectedPoint {
  return {
    x: from.x + (to.x - from.x) * amount,
    y: from.y + (to.y - from.y) * amount,
    z: from.z + (to.z - from.z) * amount,
    scale: from.scale + (to.scale - from.scale) * amount,
  };
}

function markerPoint(slot: GalaxyVisualSlot, seconds: number, reducedMotion: boolean): GalaxyProjectedPoint {
  const radius = MARKER_RADIUS[slot.kind] ?? MARKER_RADIUS[slot.orbitId] ?? 360;
  const t = reducedMotion ? 0 : seconds;
  const drift = reducedMotion || slot.size === "tick" ? 0 : seconds * 0.04;
  return projectMarkerInField(radius, slot.angle + drift, t);
}

export function GalaxyOrbitalVisual({
  model,
  transients,
  frame,
  reducedMotion,
  durationInFrames,
  ambient = false,
  forceFieldFallback = false,
  freezeTimeSec = null,
}: GalaxyOrbitalVisualProps) {
  const seconds = frame / GALAXY_FPS;
  const slots = buildGalaxyVisualSlots(model);
  const decisionEvent = transients.find((event) => event.kind === "decision");
  const riskEvent = transients.find((event) => event.kind === "risk-accept" || event.kind === "risk-veto");
  const marketEvent = transients.find((event) => event.kind === "market");
  const settlementEvent = transients.find((event) => event.kind === "settlement");
  const triggerEvent = transients.find((event) => event.kind === "trigger");
  const executionEvent = transients.find((event) => event.kind === "order" || event.kind === "fill" || event.kind === "cancel" || event.kind === "execution-error");
  const corePulse = eventWindow(frame, decisionEvent, durationInFrames);
  const riskPulse = eventWindow(frame, riskEvent, durationInFrames);
  const marketPulse = eventWindow(frame, marketEvent, durationInFrames);
  const settlementPulse = eventWindow(frame, settlementEvent, durationInFrames);
  const triggerPulse = eventWindow(frame, triggerEvent, durationInFrames);
  const coreScale = 1
    + (reducedMotion ? 0 : Math.sin(seconds * 0.82) * 0.018)
    + (corePulse < 78 ? ramp(corePulse, [0, 12, 78], [0, 0.16, 0]) : 0);
  const identity = galaxyCoreIdentity(model);
  const executionSlot = slots.find((slot) => slot.kind === "execution")!;
  const executionPoint = markerPoint(executionSlot, seconds, reducedMotion);
  const corePoint: GalaxyProjectedPoint = { x: GALAXY_VIEW.cx, y: GALAXY_VIEW.cy, z: 80, scale: 1 };
  const nodes = slots
    .filter((slot) => slot.kind !== "core")
    .map((slot) => ({ slot, point: markerPoint(slot, seconds, reducedMotion) }))
    .sort((left, right) => left.point.z - right.point.z);
  const caption = coreCaptionLines(identity);
  const labelLayouts = layoutSemanticLabels(
    nodes.map(({ slot, point }) => {
      const anchor = labelAnchor(point);
      return { id: slot.id, kind: slot.kind, size: slot.size, x: anchor.x, y: anchor.y };
    }),
  );
  const labelById = new Map(labelLayouts.map((layout) => [layout.id, layout]));
  const satelliteProps = { model, transients, frame, durationInFrames, settlementPulse, settlementEvent, triggerPulse, marketPulse, labelById };

  return <div
    className={`galaxyOrbital${ambient ? " galaxyOrbital--ambient" : ""}`}
    aria-label="Living Galaxy system architecture driven by the current authoritative snapshot"
  >
    <GalaxyFieldCanvas
      reducedMotion={reducedMotion || ambient}
      transients={transients}
      frame={frame}
      durationInFrames={durationInFrames}
      forceFallback={forceFieldFallback}
      freezeTimeSec={freezeTimeSec}
    />
    <div className="galaxyOrbitalWash" />
    <svg className="galaxyOrbitalField" viewBox={`0 0 ${GALAXY_VIEW.width} ${GALAXY_VIEW.height}`} role="img" aria-hidden="true">
      {riskEvent?.kind === "risk-veto" && riskPulse < 54 ? <circle
        cx={GALAXY_VIEW.cx}
        cy={GALAXY_VIEW.cy}
        r={ramp(riskPulse, [0, 54], [36, 290])}
        fill="none"
        stroke="#d46a6a"
        strokeWidth="1.6"
        opacity={ramp(riskPulse, [0, 8, 54], [0, 0.7, 0])}
      /> : null}

      {riskEvent?.kind === "risk-accept" && riskPulse < 50 ? <circle
        cx={GALAXY_VIEW.cx}
        cy={GALAXY_VIEW.cy}
        r={ramp(riskPulse, [0, 50], [28, 210])}
        fill="none"
        stroke="#7dcea0"
        strokeWidth="1.3"
        opacity={ramp(riskPulse, [0, 10, 50], [0, 0.55, 0])}
      /> : null}

      {corePulse < 70 ? <circle
        cx={GALAXY_VIEW.cx}
        cy={GALAXY_VIEW.cy}
        r={ramp(corePulse, [0, 70], [42, 168])}
        fill="none"
        stroke="#ffd27a"
        strokeWidth="1.4"
        opacity={ramp(corePulse, [0, 10, 70], [0, 0.7, 0])}
      /> : null}

      {executionEvent && elapsedFrames(frame, executionEvent, durationInFrames) < 56 ? (() => {
        const elapsed = elapsedFrames(frame, executionEvent, durationInFrames);
        const travel = ramp(elapsed, [0, 56], [0, 1]);
        const point = lerpPoint(corePoint, executionPoint, travel);
        const tail = lerpPoint(corePoint, executionPoint, Math.max(0, travel - 0.12));
        const color = eventColor(executionEvent.kind);
        return <g>
          <line x1={tail.x} y1={tail.y} x2={point.x} y2={point.y} stroke={color} strokeWidth="2.4" strokeLinecap="round" opacity={ramp(elapsed, [0, 8, 56], [0, 0.9, 0])} />
          <circle cx={point.x} cy={point.y} r={3.4} fill={color} />
        </g>;
      })() : null}

      {nodes.filter(({ point }) => point.z < 36).map(({ slot, point }) => <Satellite key={`behind-${slot.id}`} slot={slot} point={point} {...satelliteProps} />)}

      <g className="galaxyCoreGroup" style={{ transform: `translate(${GALAXY_VIEW.cx}px, ${GALAXY_VIEW.cy}px) scale(${coreScale})`, transformOrigin: "0 0" } as CSSProperties}>
        <circle r="52" fill="transparent" />
      </g>
      {caption.titles.length === 1 ? (
        <text x={GALAXY_VIEW.cx} y={GALAXY_VIEW.cy - 2} textAnchor="middle" className="galaxyCoreTitle">{caption.titles[0]}</text>
      ) : (
        <>
          <text x={GALAXY_VIEW.cx} y={GALAXY_VIEW.cy - 10} textAnchor="middle" className="galaxyCoreKicker">{caption.titles[0]}</text>
          <text x={GALAXY_VIEW.cx} y={GALAXY_VIEW.cy + 6} textAnchor="middle" className="galaxyCoreTitle">{caption.titles[1]}</text>
        </>
      )}
      <text x={GALAXY_VIEW.cx} y={GALAXY_VIEW.cy + 22} textAnchor="middle" className="galaxyCoreDetail">{caption.detail}</text>

      {nodes.filter(({ point }) => point.z >= 36).map(({ slot, point }) => <Satellite key={`front-${slot.id}`} slot={slot} point={point} {...satelliteProps} />)}
    </svg>

    <div className="galaxyOrbitalHud" aria-hidden="true">
      <span><i>DIR</i><b>{identity.detail}</b></span>
      <span><i>AUTH</i><b>{model.core.authority}</b></span>
      <span><i>FAIR</i><b>{model.core.fairProbability}</b></span>
      <span><i>MKT</i><b>{model.core.marketProbability}</b></span>
      <span><i>EDGE</i><b>{model.core.estimatedEdge}</b></span>
      <span><i>REF</i><b>{model.reference.connected}/{model.reference.feeds.length}</b></span>
      <span><i>OPEN</i><b>{model.portfolio.positions}</b></span>
      <span><i>SIGNALS</i><b>{transients.length}</b></span>
    </div>
  </div>;
}

function Satellite({
  slot,
  point,
  model,
  transients,
  frame,
  durationInFrames,
  settlementPulse,
  settlementEvent,
  triggerPulse,
  marketPulse,
  labelById,
}: {
  slot: GalaxyVisualSlot;
  point: GalaxyProjectedPoint;
  model: GalaxySceneModel;
  transients: GalaxyTransient[];
  frame: number;
  durationInFrames: number;
  settlementPulse: number;
  settlementEvent: GalaxyTransient | undefined;
  triggerPulse: number;
  marketPulse: number;
  labelById: Map<string, LaidOutSemanticLabel>;
}) {
  const fallback = labelAnchor(point);
  const layout = labelById.get(slot.id);
  const label = layout ?? { x: fallback.x, y: fallback.y, showLabel: slot.size !== "tick", showDetail: slot.size !== "tick" };
  const radius = slot.size === "primary" ? 5.4 : slot.size === "secondary" ? 3.6 : 1.7;
  const satellites = model.portfolio.satellites;
  const settlementApplies = settlementEvent
    ? settlementAppliesToSlot(settlementEvent, slot, satellites)
    : false;
  const activity = transients.find((event) => {
    if (slot.kind === "reference") return event.kind === "reference" && event.asset === slot.label;
    if (slot.kind === "position" || slot.kind === "portfolio") {
      return event.kind === "settlement" && settlementAppliesToSlot(event, slot, satellites);
    }
    return eventOrbitId(event.kind) === slot.orbitId;
  });
  const activityFrame = activity ? elapsedFrames(frame, activity, durationInFrames) : 999;
  const settling = settlementApplies && settlementPulse < 82;
  const scale = point.scale * (settling ? ramp(settlementPulse, [0, 18, 82], [1, 0.35, 1]) : 1);
  const pulse = (slot.kind === "trigger" && triggerPulse < 64)
    || (slot.kind === "market" && marketPulse < 64)
    || (slot.kind === "reference" && activityFrame < 64)
    || (slot.kind === "risk" && activityFrame < 64);
  const fill = toneColor(slot.tone);

  return <g className={`galaxySatellite galaxySatellite--${slot.kind}`} opacity={slot.tone === "critical" && slot.kind === "reference" ? 0.62 : 1}>
    <g style={{ transform: `translate(${point.x}px, ${point.y}px) scale(${scale})`, transformOrigin: "0 0" } as CSSProperties}>
      {pulse ? <circle r={radius + 7} fill="none" stroke={fill} opacity="0.45" /> : null}
      <circle r={radius} fill={fill} stroke="rgba(255,244,220,.55)" strokeWidth="0.7" />
    </g>
    {slot.size !== "tick" && label.showLabel ? <>
      <text x={label.x} y={label.y} textAnchor="middle" className="galaxySatelliteLabel">{slot.label}</text>
      {label.showDetail ? <text x={label.x} y={label.y + 11} textAnchor="middle" className="galaxySatelliteDetail">{slot.detail}</text> : null}
    </> : null}
    {slot.kind === "market" ? <title>{`${model.market.status} Â· ${model.market.outcomes} outcomes`}</title> : null}
  </g>;
}



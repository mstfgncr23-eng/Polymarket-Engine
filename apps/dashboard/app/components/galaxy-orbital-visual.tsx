import type { CSSProperties } from "react";
import {
  GALAXY_ORBITS,
  GALAXY_VIEW,
  buildGalaxyVisualSlots,
  eventOrbitId,
  galaxyOrbit,
  labelAnchor,
  orbitDepthPaths,
  orbitPolyline,
  projectOrbitPoint,
  type GalaxyOrbitDef,
  type GalaxyProjectedPoint,
  type GalaxyVisualSlot,
} from "../../lib/galaxy-presentation";
import { GALAXY_FPS, type GalaxySceneModel, type GalaxyTransient } from "../../lib/galaxy-scene";

export type GalaxyOrbitalVisualProps = {
  model: GalaxySceneModel;
  transients: GalaxyTransient[];
  frame: number;
  reducedMotion: boolean;
  durationInFrames: number;
  ambient?: boolean;
};

const STAR_FIELD = Array.from({ length: 54 }, (_, index) => ({
  x: (index * 47 + 19) % GALAXY_VIEW.width,
  y: (index * 71 + 11) % GALAXY_VIEW.height,
  r: index % 11 === 0 ? 1.15 : index % 4 === 0 ? 0.75 : 0.45,
  warm: index % 3 === 0,
}));

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

function orbitPoint(orbit: GalaxyOrbitDef, angle: number, seconds: number, reducedMotion: boolean, travel = 0): GalaxyProjectedPoint {
  const parallax = reducedMotion ? 0 : Math.sin(seconds * 0.11) * 0.028;
  const theta = reducedMotion ? angle : angle + seconds * orbit.speed * travel;
  return projectOrbitPoint(orbit, theta, parallax);
}

function lerpPoint(from: GalaxyProjectedPoint, to: GalaxyProjectedPoint, amount: number): GalaxyProjectedPoint {
  return {
    x: from.x + (to.x - from.x) * amount,
    y: from.y + (to.y - from.y) * amount,
    z: from.z + (to.z - from.z) * amount,
    scale: from.scale + (to.scale - from.scale) * amount,
  };
}

export function GalaxyOrbitalVisual({
  model,
  transients,
  frame,
  reducedMotion,
  durationInFrames,
  ambient = false,
}: GalaxyOrbitalVisualProps) {
  const seconds = frame / GALAXY_FPS;
  const parallax = reducedMotion ? 0 : Math.sin(seconds * 0.11) * 0.028;
  const slots = buildGalaxyVisualSlots(model);
  const decisionEvent = transients.find((event) => event.kind === "decision");
  const riskEvent = transients.find((event) => event.kind === "risk-accept" || event.kind === "risk-veto");
  const marketEvent = transients.find((event) => event.kind === "market");
  const settlementEvent = transients.find((event) => event.kind === "settlement");
  const triggerEvent = transients.find((event) => event.kind === "trigger");
  const executionEvent = transients.find((event) => event.kind === "order" || event.kind === "fill" || event.kind === "cancel" || event.kind === "execution-error");
  const referenceEvent = transients.find((event) => event.kind === "reference");
  const corePulse = eventWindow(frame, decisionEvent, durationInFrames);
  const riskPulse = eventWindow(frame, riskEvent, durationInFrames);
  const marketPulse = eventWindow(frame, marketEvent, durationInFrames);
  const settlementPulse = eventWindow(frame, settlementEvent, durationInFrames);
  const triggerPulse = eventWindow(frame, triggerEvent, durationInFrames);
  const coreScale = 1
    + (reducedMotion ? 0 : Math.sin(seconds * 0.82) * 0.018)
    + (corePulse < 78 ? ramp(corePulse, [0, 12, 78], [0, 0.16, 0]) : 0);
  const core = slots.find((slot) => slot.kind === "core")!;
  const executionSlot = slots.find((slot) => slot.kind === "execution")!;
  const executionPoint = orbitPoint(galaxyOrbit("execution"), executionSlot.angle, seconds, reducedMotion, 0.12);
  const corePoint: GalaxyProjectedPoint = { x: GALAXY_VIEW.cx, y: GALAXY_VIEW.cy, z: 80, scale: 1 };
  const paintId = ambient ? "fallback" : "live";
  const nodes = slots
    .filter((slot) => slot.kind !== "core")
    .map((slot) => ({ slot, point: orbitPoint(galaxyOrbit(slot.orbitId === "core" ? "halo" : slot.orbitId), slot.angle, seconds, reducedMotion, slot.size === "tick" ? 0.35 : 0.08) }))
    .sort((left, right) => left.point.z - right.point.z);
  const satelliteProps = { model, transients, frame, durationInFrames, settlementPulse, triggerPulse, marketPulse };

  return <div
    className={`galaxyOrbital${ambient ? " galaxyOrbital--ambient" : ""}`}
    aria-label="Living Galaxy system architecture driven by the current authoritative snapshot"
  >
    <div className="galaxyOrbitalWash" />
    <div className="galaxyOrbitalNebula galaxyOrbitalNebula--far" style={{ translate: reducedMotion || ambient ? "0px 0px" : `${Math.sin(seconds / 7) * 12}px ${Math.cos(seconds / 9) * 7}px` }} />
    <div className="galaxyOrbitalNebula galaxyOrbitalNebula--near" style={{ translate: reducedMotion || ambient ? "0px 0px" : `${Math.cos(seconds / 8) * 9}px ${Math.sin(seconds / 6) * 6}px` }} />
    <svg className="galaxyOrbitalField" viewBox={`0 0 ${GALAXY_VIEW.width} ${GALAXY_VIEW.height}`} role="img" aria-hidden="true">
      <defs>
        <radialGradient id={`galaxy-core-fill-${paintId}`} cx="38%" cy="32%" r="68%">
          <stop offset="0%" stopColor="#fff6d8" />
          <stop offset="18%" stopColor="#ffd27a" />
          <stop offset="42%" stopColor="#f39b3c" />
          <stop offset="70%" stopColor="#b85a18" />
          <stop offset="100%" stopColor="#3a1a0c" />
        </radialGradient>
        <radialGradient id={`galaxy-disk-${paintId}`} cx="50%" cy="50%" r="50%">
          <stop offset="0%" stopColor="rgba(255,176,82,.10)" />
          <stop offset="42%" stopColor="rgba(180,110,40,.05)" />
          <stop offset="100%" stopColor="rgba(0,0,0,0)" />
        </radialGradient>
      </defs>

      {STAR_FIELD.map((star, index) => <circle
        key={`star-${index}`}
        cx={star.x}
        cy={star.y}
        r={star.r}
        fill={star.warm ? "#f3ddb0" : "#d8d2c4"}
        opacity={reducedMotion ? 0.42 : 0.2 + 0.38 * ((Math.sin((frame + index * 13) / 36) + 1) / 2)}
      />)}

      <ellipse cx={GALAXY_VIEW.cx} cy={GALAXY_VIEW.cy + 10} rx="520" ry="210" fill={`url(#galaxy-disk-${paintId})`} opacity="0.95" />

      {GALAXY_ORBITS.map((orbit) => {
        const path = orbitPolyline(orbit, 80, parallax);
        const depth = orbitDepthPaths(orbit, 80, parallax);
        const active = transients.some((event) => eventOrbitId(event.kind) === orbit.id && elapsedFrames(frame, event, durationInFrames) < 70);
        return <g key={`far-${orbit.id}`}>
          <path d={path} fill="none" stroke="rgba(255,214,140,.05)" strokeWidth="7" />
          {depth.far.map((segment, index) => <path className={`galaxyOrbitStroke galaxyOrbitStroke--${orbit.id}`} d={segment} fill="none" stroke={orbit.stroke} strokeWidth={active ? 1.2 : 0.85} opacity={active ? 0.42 : 0.28} key={`${orbit.id}-far-${index}`} />)}
        </g>;
      })}

      {GALAXY_ORBITS.flatMap((orbit) => [0, 1].flatMap((lane) => {
        const trail = Array.from({ length: reducedMotion ? 1 : 5 }, (_, ghost) => {
          const angle = orbit.phase + lane * 2.05 + seconds * orbit.speed + ghost * -0.09;
          const point = projectOrbitPoint(orbit, reducedMotion ? orbit.phase + lane * 2.05 : angle, parallax);
          return { key: `${orbit.id}-${lane}-${ghost}`, point, ghost, orbit, lane };
        });
        return trail;
      })).sort((left, right) => left.point.z - right.point.z).map(({ key, point, ghost, orbit }) => {
        const hot = (orbit.id === "trigger" && triggerPulse < 64)
          || (orbit.id === "market" && marketPulse < 64)
          || (orbit.id === "reference" && referenceEvent && elapsedFrames(frame, referenceEvent, durationInFrames) < 64);
        return <circle
          key={key}
          cx={point.x}
          cy={point.y}
          r={(ghost === 0 ? 2.15 : 1.35) * point.scale * (hot && ghost === 0 ? 1.35 : 1)}
          fill={hot ? "#ffe7b0" : "#ffd27a"}
          opacity={reducedMotion ? 0.28 : 0.82 - ghost * 0.18}
        />;
      })}

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
        <circle r="118" fill="rgba(255,168,64,.10)" />
        <circle r="86" fill="rgba(255,176,72,.16)" />
        <circle r="52" fill={`url(#galaxy-core-fill-${paintId})`} stroke="rgba(255,236,196,.82)" strokeWidth="1.4" />
        <circle r="18" fill="#fff8e4" opacity="0.96" />
      </g>
      <text x={GALAXY_VIEW.cx} y={GALAXY_VIEW.cy - 10} textAnchor="middle" className="galaxyCoreKicker">ASTRA</text>
      <text x={GALAXY_VIEW.cx} y={GALAXY_VIEW.cy + 6} textAnchor="middle" className="galaxyCoreTitle">DECISION CORE</text>
      <text x={GALAXY_VIEW.cx} y={GALAXY_VIEW.cy + 22} textAnchor="middle" className="galaxyCoreDetail">{core.detail}</text>

      {GALAXY_ORBITS.map((orbit) => {
        const depth = orbitDepthPaths(orbit, 80, parallax);
        const active = transients.some((event) => eventOrbitId(event.kind) === orbit.id && elapsedFrames(frame, event, durationInFrames) < 70);
        return <g key={`near-${orbit.id}`}>
          {depth.near.map((segment, index) => <path className={`galaxyOrbitStroke galaxyOrbitStroke--${orbit.id}`} d={segment} fill="none" stroke={orbit.stroke} strokeWidth={active ? 1.8 : 1.2} opacity={active ? 0.95 : 0.78} key={`${orbit.id}-near-${index}`} />)}
        </g>;
      })}

      {nodes.filter(({ point }) => point.z >= 36).map(({ slot, point }) => <Satellite key={`front-${slot.id}`} slot={slot} point={point} {...satelliteProps} />)}
    </svg>

    <div className="galaxyOrbitalHud" aria-hidden="true">
      <span><i>DIR</i><b>{core.detail}</b></span>
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
  triggerPulse,
  marketPulse,
}: {
  slot: GalaxyVisualSlot;
  point: GalaxyProjectedPoint;
  model: GalaxySceneModel;
  transients: GalaxyTransient[];
  frame: number;
  durationInFrames: number;
  settlementPulse: number;
  triggerPulse: number;
  marketPulse: number;
}) {
  const label = labelAnchor(point);
  const radius = slot.size === "primary" ? 5.4 : slot.size === "secondary" ? 3.6 : 1.7;
  const activity = transients.find((event) => {
    if (slot.kind === "reference") return event.kind === "reference" && event.asset === slot.label;
    if (slot.kind === "position") return event.kind === "settlement";
    return eventOrbitId(event.kind) === slot.orbitId;
  });
  const activityFrame = activity ? elapsedFrames(frame, activity, durationInFrames) : 999;
  const settling = slot.kind === "position" && settlementPulse < 82;
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
    {slot.size !== "tick" ? <>
      <text x={label.x} y={label.y} textAnchor="middle" className="galaxySatelliteLabel">{slot.label}</text>
      <text x={label.x} y={label.y + 11} textAnchor="middle" className="galaxySatelliteDetail">{slot.detail}</text>
    </> : null}
    {slot.kind === "market" ? <title>{`${model.market.status} · ${model.market.outcomes} outcomes`}</title> : null}
  </g>;
}

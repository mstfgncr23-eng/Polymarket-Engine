import type { CSSProperties } from "react";
import { AbsoluteFill, Easing, interpolate, useCurrentFrame } from "remotion";
import {
  GALAXY_FPS,
  type GalaxyMotionKind,
  type GalaxySceneModel,
  type GalaxyTransient,
} from "../../lib/galaxy-scene";

export type GalaxyRemotionSceneProps = {
  model: GalaxySceneModel;
  transients: GalaxyTransient[];
  reducedMotion: boolean;
  durationInFrames: number;
};

const stars = Array.from({ length: 76 }, (_, index) => ({
  left: (index * 47 + 13) % 100,
  top: (index * 71 + 7) % 100,
  size: index % 13 === 0 ? 2.2 : index % 5 === 0 ? 1.5 : 1,
  phase: (index * 17) % 90,
}));

const routes: Partial<Record<GalaxyMotionKind, string>> = {
  trigger: "M 386 516 C 430 470, 470 425, 525 394",
  "risk-accept": "M 672 370 C 730 355, 765 350, 819 354",
  "risk-veto": "M 672 370 C 730 355, 765 350, 819 354",
  order: "M 865 386 C 900 420, 925 452, 970 485",
  fill: "M 1008 517 C 1045 548, 1061 580, 1085 609",
  cancel: "M 865 386 C 900 420, 925 452, 970 485",
  "execution-error": "M 865 386 C 900 420, 925 452, 970 485",
};

function toneColor(tone: string): string {
  if (tone === "healthy") return "#68e0b5";
  if (tone === "warning") return "#efc56a";
  if (tone === "critical") return "#f16f84";
  return "#71839e";
}

function eventColor(kind: GalaxyMotionKind): string {
  if (kind === "risk-veto" || kind === "execution-error") return "#f16f84";
  if (kind === "cancel") return "#efc56a";
  if (kind === "risk-accept" || kind === "fill" || kind === "settlement") return "#68e0b5";
  if (kind === "reference") return "#9b8cff";
  return "#78d9ff";
}

function elapsedFrames(frame: number, event: GalaxyTransient, durationInFrames: number): number {
  return (frame - event.startFrame + durationInFrames) % durationInFrames;
}

function EventSignals({ frame, durationInFrames, transients }: { frame: number; durationInFrames: number; transients: GalaxyTransient[] }) {
  return <>{transients.map((event) => {
    const elapsed = elapsedFrames(frame, event, durationInFrames);
    const eventDuration = 58 + event.severity * 12;
    if (elapsed > eventDuration) return null;
    const route = routes[event.kind];
    const color = eventColor(event.kind);
    const opacity = interpolate(elapsed, [0, 7, eventDuration * .72, eventDuration], [0, 1, .78, 0], {
      extrapolateLeft: "clamp",
      extrapolateRight: "clamp",
      easing: Easing.bezier(.16, 1, .3, 1),
    });
    if (!route) return null;
    return <div
      className={`galaxySignal galaxySignal--${event.kind}`}
      key={event.id}
      style={{
        "--signal-color": color,
        offsetPath: `path('${route}')`,
        offsetDistance: `${interpolate(elapsed, [0, eventDuration], [0, 100], { extrapolateLeft: "clamp", extrapolateRight: "clamp", easing: Easing.bezier(.2, .7, .2, 1) })}%`,
        opacity,
        scale: interpolate(elapsed, [0, 9, eventDuration], [.5, 1 + event.severity * .08, .3], { extrapolateLeft: "clamp", extrapolateRight: "clamp" }),
      } as CSSProperties}
    />;
  })}</>;
}

function SceneStatus({ label, tone }: { label: string; tone: string }) {
  return <span className="sceneStatus" style={{ "--scene-tone": toneColor(tone) } as CSSProperties}><i />{label}</span>;
}

export function GalaxyRemotionScene({ model, transients, reducedMotion, durationInFrames }: GalaxyRemotionSceneProps) {
  const currentFrame = useCurrentFrame();
  const frame = reducedMotion ? 0 : currentFrame;
  const seconds = frame / GALAXY_FPS;
  const decisionEvent = transients.find((event) => event.kind === "decision");
  const riskEvent = transients.find((event) => event.kind === "risk-accept" || event.kind === "risk-veto");
  const marketEvent = transients.find((event) => event.kind === "market");
  const settlementEvent = transients.find((event) => event.kind === "settlement");
  const corePulse = decisionEvent ? elapsedFrames(frame, decisionEvent, durationInFrames) : 999;
  const riskPulse = riskEvent ? elapsedFrames(frame, riskEvent, durationInFrames) : 999;
  const marketPulse = marketEvent ? elapsedFrames(frame, marketEvent, durationInFrames) : 999;
  const settlementPulse = settlementEvent ? elapsedFrames(frame, settlementEvent, durationInFrames) : 999;

  return <AbsoluteFill className="remotionGalaxyScene" aria-label="Living Galaxy system architecture driven by the current authoritative snapshot">
    <div className="sceneNebula sceneNebula--one" style={{ translate: reducedMotion ? "0 0" : `${Math.sin(seconds / 6) * 15}px ${Math.cos(seconds / 8) * 8}px` }} />
    <div className="sceneNebula sceneNebula--two" style={{ translate: reducedMotion ? "0 0" : `${Math.cos(seconds / 9) * 11}px ${Math.sin(seconds / 7) * 9}px` }} />
    <div className="sceneStars" aria-hidden="true">
      {stars.map((star, index) => <i key={index} style={{
        left: `${star.left}%`,
        top: `${star.top}%`,
        width: star.size,
        height: star.size,
        opacity: reducedMotion ? .46 : .22 + .42 * ((Math.sin((frame + star.phase) / 34) + 1) / 2),
      }} />)}
    </div>

    <svg className="constellationField" viewBox="0 0 1200 760" aria-hidden="true">
      <defs>
        <linearGradient id="route-cyan" x1="0" y1="0" x2="1" y2="0"><stop offset="0" stopColor="#526f94" stopOpacity=".05" /><stop offset=".48" stopColor="#79d8ff" stopOpacity=".52" /><stop offset="1" stopColor="#7ca6d8" stopOpacity=".09" /></linearGradient>
        <linearGradient id="route-violet" x1="0" y1="0" x2="1" y2="0"><stop offset="0" stopColor="#9b8cff" stopOpacity=".16" /><stop offset="1" stopColor="#79d8ff" stopOpacity=".46" /></linearGradient>
        <filter id="route-glow"><feGaussianBlur stdDeviation="3" result="blur" /><feMerge><feMergeNode in="blur" /><feMergeNode in="SourceGraphic" /></feMerge></filter>
      </defs>
      <ellipse className="gravityOrbit gravityOrbit--outer" cx="600" cy="375" rx="475" ry="275" />
      <ellipse className="gravityOrbit gravityOrbit--inner" cx="600" cy="375" rx="322" ry="185" />
      <path className="constellationPath constellationPath--reference" d="M 315 225 C 400 246, 462 300, 528 350" />
      <path className="constellationPath" d="M 220 555 C 300 555, 335 535, 386 516 C 430 470, 470 425, 525 394" />
      <path className="constellationPath" d="M 672 370 C 730 355, 765 350, 819 354" />
      <path className="constellationPath" d="M 865 386 C 900 420, 925 452, 970 485" />
      <path className="constellationPath" d="M 1008 517 C 1045 548, 1061 580, 1085 609" />
      <path className="constellationPath constellationPath--faint" d="M 315 225 C 535 125, 760 140, 825 320" />
    </svg>

    <section className="referenceConstellation" aria-label={`${model.reference.connected} of ${model.reference.feeds.length} reference feeds connected`}>
      <div className="referenceAnchor" style={{ scale: reducedMotion ? 1 : 1 + Math.sin(seconds * .7) * .018 }}>
        <span className="sceneMicroLabel">REFERENCE NETWORK</span>
        <strong>{model.reference.connected}/{model.reference.feeds.length}</strong>
        <small>connected</small>
      </div>
      <div className="referenceOrbitRing" aria-hidden="true" style={{ rotate: reducedMotion ? "-12deg" : `${-12 + seconds * .7}deg` }} />
      <div className="referenceFeeds" role="list">
        {model.reference.feeds.map((feed, index) => {
          const count = Math.max(1, model.reference.feeds.length);
          const angle = -2.72 + (index / count) * Math.PI * 2;
          const x = 205 + Math.cos(angle) * (112 + (index % 2) * 23);
          const y = 225 + Math.sin(angle) * (76 + (index % 3) * 8);
          const activity = transients.find((event) => event.kind === "reference" && event.asset === feed.asset);
          const activityFrame = activity ? elapsedFrames(frame, activity, durationInFrames) : 999;
          return <div
            className={`referenceBody referenceBody--${feed.tone}`}
            key={feed.asset}
            role="listitem"
            style={{
              left: x,
              top: y,
              translate: reducedMotion ? "-50% -50%" : `calc(-50% + ${Math.sin(seconds * .45 + index) * 3}px) calc(-50% + ${Math.cos(seconds * .4 + index) * 3}px)`,
              "--feed-tone": toneColor(feed.tone),
            } as CSSProperties}
          >
            <span className="referenceAura" style={{ scale: feed.state === "CONNECTING" && !reducedMotion ? 1 + Math.sin(seconds * 2.2) * .16 : 1 }} />
            {activityFrame < 70 ? <span className="referenceActivity" style={{ opacity: interpolate(activityFrame, [0, 12, 70], [0, .9, 0], { extrapolateLeft: "clamp", extrapolateRight: "clamp" }), scale: interpolate(activityFrame, [0, 70], [.65, 2.1], { extrapolateLeft: "clamp", extrapolateRight: "clamp" }) }} /> : null}
            <b>{feed.asset}</b><small>{feed.state}</small>
          </div>;
        })}
      </div>
    </section>

    <section className={`sensorNode sensorNode--${model.market.tone}`} aria-label={`Market Scanner ${model.market.status}`}>
      <div className="sensorSweep" aria-hidden="true" style={{ rotate: reducedMotion ? "-28deg" : `${(seconds * 24) % 360}deg` }} />
      {marketPulse < 72 ? <span className="sensorWave" style={{ opacity: interpolate(marketPulse, [0, 12, 72], [0, .7, 0], { extrapolateLeft: "clamp", extrapolateRight: "clamp" }), scale: interpolate(marketPulse, [0, 72], [.75, 1.6], { extrapolateLeft: "clamp", extrapolateRight: "clamp" }) }} /> : null}
      <span className="sceneMicroLabel">MARKET SENSOR</span><strong>{model.market.outcomes}</strong><small>outcomes · {model.market.qualified} qualified</small><SceneStatus label={model.market.status} tone={model.market.tone} />
    </section>

    <section className={`pulsarNode pulsarNode--${model.trigger.tone}`} aria-label={`Trigger Engine ${model.trigger.status}`}>
      <span className="pulsarBeam pulsarBeam--one" aria-hidden="true" style={{ rotate: reducedMotion ? "24deg" : `${24 + Math.sin(seconds) * 4}deg` }} />
      <span className="pulsarBeam pulsarBeam--two" aria-hidden="true" style={{ rotate: reducedMotion ? "-62deg" : `${-62 - Math.sin(seconds) * 4}deg` }} />
      <span className="pulsarCore" aria-hidden="true" />
      <div className="celestialLabel celestialLabel--left"><span className="sceneMicroLabel">TRIGGER RELAY</span><strong>{model.trigger.status}</strong><small>{model.trigger.kind}</small></div>
    </section>

    <section className={`decisionStar decisionStar--${model.core.authorityTone}`} aria-label={`Native Decision Core. Authority ${model.core.authority}. Direction ${model.core.direction}.`}>
      <span className="coreGravity coreGravity--outer" aria-hidden="true" style={{ rotate: reducedMotion ? "17deg" : `${17 + seconds * 2.1}deg` }} />
      <span className="coreGravity coreGravity--inner" aria-hidden="true" style={{ rotate: reducedMotion ? "-28deg" : `${-28 - seconds * 3}deg` }} />
      <span className="coreCorona" aria-hidden="true" style={{ scale: 1 + (reducedMotion ? 0 : Math.sin(seconds * .86) * .025) + (corePulse < 80 ? interpolate(corePulse, [0, 14, 80], [0, .15, 0], { extrapolateLeft: "clamp", extrapolateRight: "clamp" }) : 0) }} />
      <span className="coreSurface" aria-hidden="true" style={{ rotate: reducedMotion ? "0deg" : `${seconds * 2.7}deg` }} />
      <div className="coreReadout">
        <span className="sceneMicroLabel">NATIVE DECISION CORE</span>
        <strong>{model.core.direction}</strong>
        <small>{model.core.confidence} confidence</small>
        <SceneStatus label={model.core.authority} tone={model.core.authorityTone} />
        <div className="coreReadoutGrid"><span><i>FAIR</i><b>{model.core.fairProbability}</b></span><span><i>MARKET</i><b>{model.core.marketProbability}</b></span><span><i>EDGE</i><b>{model.core.estimatedEdge}</b></span></div>
      </div>
    </section>

    <section className={`guardianNode guardianNode--${model.risk.tone}`} aria-label={`Risk Guardian ${model.risk.status}`}>
      <span className="guardianField guardianField--outer" aria-hidden="true" style={{ scale: riskPulse < 72 && riskEvent?.kind === "risk-veto" ? interpolate(riskPulse, [0, 18, 72], [1.12, .83, 1], { extrapolateLeft: "clamp", extrapolateRight: "clamp" }) : 1 }} />
      <span className="guardianField guardianField--inner" aria-hidden="true" />
      <span className="guardianCore" aria-hidden="true" />
      <div className="celestialLabel celestialLabel--right"><span className="sceneMicroLabel">RISK CONTAINMENT</span><strong>{model.risk.status}</strong><small>{model.risk.vetoes} vetoes · {model.risk.detail}</small></div>
    </section>

    <section className={`executionComet executionComet--${model.execution.tone}`} aria-label={`Execution ${model.execution.status}`}>
      <span className="executionTail" aria-hidden="true" style={{ scale: `${reducedMotion ? 1 : 1 + Math.sin(seconds * 1.8) * .06} 1` }} />
      <span className="executionCore" aria-hidden="true" />
      <div className="celestialLabel celestialLabel--right"><span className="sceneMicroLabel">PAPER EXECUTION</span><strong>{model.execution.status}</strong><small>{model.execution.detail}</small></div>
    </section>

    <section className={`portfolioMass portfolioMass--${model.portfolio.tone}`} aria-label={`Portfolio ${model.portfolio.status}, equity ${model.portfolio.equity}`}>
      <span className="portfolioRing" aria-hidden="true" style={{ rotate: reducedMotion ? "8deg" : `${8 + seconds * .8}deg` }} />
      {settlementPulse < 82 ? <span className="portfolioWave" style={{ opacity: interpolate(settlementPulse, [0, 12, 82], [0, .75, 0], { extrapolateLeft: "clamp", extrapolateRight: "clamp" }), scale: interpolate(settlementPulse, [0, 82], [.85, 1.8], { extrapolateLeft: "clamp", extrapolateRight: "clamp" }) }} /> : null}
      <span className="portfolioCore" aria-hidden="true" />
      <div className="portfolioReadout"><span className="sceneMicroLabel">PORTFOLIO LEDGER</span><strong>{model.portfolio.equity}</strong><small>{model.portfolio.positions} positions · {model.portfolio.cash} cash</small></div>
    </section>

    <EventSignals frame={frame} durationInFrames={durationInFrames} transients={transients} />
    <div className="sceneLegend"><span>LIVE ARCHITECTURE</span><i /> gravitational data paths <b>{model.reference.feeds.length} references</b><b>{transients.length} active signals</b></div>
  </AbsoluteFill>;
}

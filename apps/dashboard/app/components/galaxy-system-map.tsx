"use client";

import dynamic from "next/dynamic";
import { Component, useCallback, useState, type ErrorInfo, type ReactNode } from "react";
import type { DashboardSnapshot } from "../../lib/types";
import { GalaxyFallbackScene } from "./galaxy-fallback-scene";
import { Panel } from "./primitives";

const GalaxyRemotionPlayer = dynamic(
  () => import("./galaxy-remotion-player").then((module) => module.GalaxyRemotionPlayer),
  { ssr: false, loading: () => null },
);

class GalaxySceneBoundary extends Component<{ children: ReactNode; fallback: ReactNode; onError: () => void }, { failed: boolean }> {
  state = { failed: false };

  static getDerivedStateFromError() {
    return { failed: true };
  }

  componentDidCatch(_error: Error, _info: ErrorInfo) {
    this.props.onError();
  }

  render() {
    return this.state.failed ? this.props.fallback : this.props.children;
  }
}

export function GalaxySystemMap({ snapshot }: { snapshot: DashboardSnapshot }) {
  const [ready, setReady] = useState(false);
  const [failed, setFailed] = useState(false);
  const markReady = useCallback(() => setReady(true), []);
  const markFailed = useCallback(() => setFailed(true), []);
  const fallback = <GalaxyFallbackScene snapshot={snapshot} />;

  return <Panel
    title="Galaxy System Map"
    kicker="LIVING SYSTEM ARCHITECTURE"
    meta={failed ? "CSS / SVG FALLBACK" : ready ? "REMOTION PLAYER · AUTHORITATIVE STATE" : "CSS / SVG FALLBACK · LOADING PLAYER"}
    className="galaxyPanel"
  >
    {failed ? fallback : <GalaxySceneBoundary fallback={fallback} onError={markFailed}>
      <div className={`galaxySceneStack${ready ? " galaxySceneStack--ready" : ""}`}>
        {!ready ? <div className="galaxyFallbackUnderlay">{fallback}</div> : null}
        <GalaxyRemotionPlayer snapshot={snapshot} onReady={markReady} onError={markFailed} />
      </div>
    </GalaxySceneBoundary>}
  </Panel>;
}

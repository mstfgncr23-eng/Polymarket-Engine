"use client";

import { useEffect, useState } from "react";
import { startEngineLiveTransport } from "../lib/live-transport";
import { formatTime } from "../lib/dashboard-selectors";
import type { DashboardSnapshot } from "../lib/types";
import { DecisionIntelligence } from "./components/decision-intelligence";
import { EmptyState } from "./components/primitives";
import { ExecutionStream } from "./components/execution-stream";
import { ExperimentBar } from "./components/experiment-bar";
import { GalaxySystemMap } from "./components/galaxy-system-map";
import { MarketUniverse } from "./components/market-universe";
import { PortfolioPanel } from "./components/portfolio-panel";
import { RiskPanel } from "./components/risk-panel";
import { SecondaryDock } from "./components/secondary-dock";
import { SystemHealth } from "./components/system-health";

export default function Dashboard({ initial }: { initial: DashboardSnapshot | null }) {
  const [data, setData] = useState(initial);
  const [stream, setStream] = useState<"CONNECTING" | "LIVE" | "DEGRADED">("CONNECTING");

  useEffect(() => {
    let disposed = false;
    if (!initial) {
      fetch("/api/engine/snapshot", { cache: "no-store" })
        .then((response) => response.ok ? response.json() : null)
        .then((value) => {
          if (!disposed && value && typeof value === "object" && !("error" in value)) setData(value as DashboardSnapshot);
        })
        .catch(() => undefined);
    }
    const transport = startEngineLiveTransport({
      onStatus: (status) => { if (!disposed) setStream(status); },
      onSnapshot: (snapshot) => { if (!disposed) setData(snapshot); },
      onCache: (cache) => { if (!disposed) setData((previous) => previous ? { ...previous, cache } : previous); },
    });
    return () => { disposed = true; transport.close(); };
  }, [initial]);

  return <main className="dashboardShell">
    <ExperimentBar snapshot={data} stream={stream} />
    {!data ? <section className="dashboardEmpty"><EmptyState title="ENGINE SNAPSHOT UNAVAILABLE" detail="The dashboard has no cached values to invent. It will recover when the upstream snapshot poll succeeds." /></section> : <>
      <div className="dashboardStage">
        <div className="primaryPlane">
          <aside className="controlRail controlRail--left"><MarketUniverse snapshot={data} /><SystemHealth snapshot={data} /></aside>
          <GalaxySystemMap snapshot={data} />
          <aside className="controlRail controlRail--right"><DecisionIntelligence snapshot={data} /><RiskPanel snapshot={data} /><ExecutionStream snapshot={data} /></aside>
        </div>
        <div className="operationsDock"><PortfolioPanel snapshot={data} /><SecondaryDock snapshot={data} /></div>
      </div>
      <footer className="dashboardFooter"><span>Snapshot {formatTime(data.generatedAt)}</span><span>Paper simulation · persisted engine values only · external research is diagnostic</span></footer>
    </>}
  </main>;
}

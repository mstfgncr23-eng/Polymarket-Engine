"use client";

import { useEffect, useState } from "react";
import { startEngineLiveTransport } from "../lib/live-transport";
import { formatTime } from "../lib/dashboard-selectors";
import type { DashboardSnapshot } from "../lib/types";
import { DecisionIntelligence } from "./components/decision-intelligence";
import { EmptyState } from "./components/primitives";
import { ExecutionStream } from "./components/execution-stream";
import { ExperimentBar } from "./components/experiment-bar";
import { ExternalResearch } from "./components/external-research";
import { GalaxySystemMap } from "./components/galaxy-system-map";
import { MarketUniverse } from "./components/market-universe";
import { MissionTimeline } from "./components/mission-timeline";
import { OpenPositions } from "./components/open-positions";
import { PortfolioPanel } from "./components/portfolio-panel";
import { RiskPanel } from "./components/risk-panel";
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
    });
    return () => { disposed = true; transport.close(); };
  }, [initial]);

  return <main className="dashboardShell">
    <ExperimentBar snapshot={data} stream={stream} />
    {!data ? <section className="dashboardEmpty"><EmptyState title="ENGINE SNAPSHOT UNAVAILABLE" detail="The dashboard has no cached values to invent. It will recover when the private engine stream reconnects." /></section> : <>
      <GalaxySystemMap snapshot={data} />
      <div className="dashboardGrid dashboardGrid--two"><MarketUniverse snapshot={data} /><DecisionIntelligence snapshot={data} /></div>
      <div className="dashboardGrid dashboardGrid--two"><PortfolioPanel snapshot={data} /><ExecutionStream snapshot={data} /></div>
      <div className="dashboardGrid dashboardGrid--two"><RiskPanel snapshot={data} /><SystemHealth snapshot={data} /></div>
      <div className="dashboardGrid dashboardGrid--two"><OpenPositions snapshot={data} /><MissionTimeline snapshot={data} /></div>
      <div className="dashboardGrid dashboardGrid--wide"><ExternalResearch snapshot={data} /></div>
      <footer className="dashboardFooter"><span>Snapshot {formatTime(data.generatedAt)}</span><span>All orders simulated · persisted engine values only · external research is diagnostic</span></footer>
    </>}
  </main>;
}

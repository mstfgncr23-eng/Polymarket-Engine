"use client";

import { useState } from "react";
import type { DashboardSnapshot } from "../../lib/types";
import { ExternalResearch } from "./external-research";
import { MissionTimeline } from "./mission-timeline";
import { OpenPositions } from "./open-positions";

const tabs = ["timeline", "positions", "research"] as const;
type DockTab = typeof tabs[number];

const labels: Record<DockTab, string> = {
  timeline: "Mission Timeline",
  positions: "Open Positions",
  research: "External Research",
};

export function SecondaryDock({ snapshot }: { snapshot: DashboardSnapshot }) {
  const [active, setActive] = useState<DockTab>("timeline");
  return <section className="secondaryDock" aria-label="Secondary operational detail">
    <div className="secondaryDockTabs" role="tablist" aria-label="Operational detail panels">
      {tabs.map((tab) => <button
        aria-controls={`dock-panel-${tab}`}
        aria-selected={active === tab}
        className={active === tab ? "isActive" : ""}
        id={`dock-tab-${tab}`}
        key={tab}
        onClick={() => setActive(tab)}
        role="tab"
        type="button"
      >{labels[tab]}</button>)}
    </div>
    <div className="secondaryDockPanel" id={`dock-panel-${active}`} role="tabpanel" aria-labelledby={`dock-tab-${active}`}>
      {active === "timeline" ? <MissionTimeline snapshot={snapshot} /> : null}
      {active === "positions" ? <OpenPositions snapshot={snapshot} /> : null}
      {active === "research" ? <ExternalResearch snapshot={snapshot} /> : null}
    </div>
  </section>;
}

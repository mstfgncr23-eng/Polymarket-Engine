import { createElement } from "../apps/dashboard/node_modules/react/index.js";
import { renderToStaticMarkup } from "../apps/dashboard/node_modules/react-dom/server.node.js";
import { expect, it } from "vitest";
import { emptyExperimentWsStats, PaperLedger } from "@pm/core";
import { EngineStateStore } from "../apps/engine/src/state.js";
import { SystemHealth } from "../apps/dashboard/app/components/system-health.js";
import type { DashboardSnapshot } from "../apps/dashboard/lib/types.js";
it("renders current experiment zero independently of a nonzero process total",()=>{
  const state=new EngineStateStore(new PaperLedger("1000",new Date()).snapshot(),new Map());
  state.connections.polymarket.reconnects=123;
  state.experimentWs={experimentId:"experiment-new",startedAt:"2026-10-15T13:00:00.000Z",entryClosedAt:null,completedAt:null,stats:emptyExperimentWsStats()};
  const html=renderToStaticMarkup(createElement(SystemHealth,{snapshot:state.snapshot() as unknown as DashboardSnapshot}));
  expect(html).toContain("Experiment reconnects");expect(html).toContain("experiment-new");
  expect(html).toContain("123 PROCESS RECONNECTS");expect(html).toContain("S1: 0 disconnects / 0 reconnects");
  state.experimentWs=null;
  expect(renderToStaticMarkup(createElement(SystemHealth,{snapshot:state.snapshot() as unknown as DashboardSnapshot}))).toContain("UNAVAILABLE");
});

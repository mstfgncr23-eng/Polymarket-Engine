"use client";

import { useEffect, useMemo, useState } from "react";
import type { DashboardSnapshot } from "../lib/types";

const AGENTS = [
  ["luna_market_microstructure", "Microstructure"], ["luna_btc_market", "BTC Market"], ["luna_information", "Information"],
  ["terra_fair_probability", "Fair Probability"], ["terra_risk_hedge", "Risk & Hedge"], ["sol_strategy_supervisor", "Strategy Supervisor"],
] as const;

export default function Dashboard({ initial }: { initial: DashboardSnapshot | null }) {
  const [data, setData] = useState(initial);
  const [stream, setStream] = useState<"CONNECTING" | "LIVE" | "DEGRADED">("CONNECTING");

  useEffect(() => {
    let disposed = false;
    if (!initial) fetch("/api/engine/snapshot", { cache: "no-store" }).then((r) => r.ok ? r.json() : null).then((value) => { if (!disposed && value) setData(value as DashboardSnapshot); }).catch(() => undefined);
    const source = new EventSource("/api/engine/events");
    source.onopen = () => setStream("LIVE");
    source.onerror = () => setStream("DEGRADED");
    source.addEventListener("snapshot", (event) => {
      try { setData(JSON.parse((event as MessageEvent<string>).data) as DashboardSnapshot); setStream("LIVE"); } catch { setStream("DEGRADED"); }
    });
    return () => { disposed = true; source.close(); };
  }, [initial]);

  const equityPoints = useMemo(() => chartPoints(data?.equityHistory ?? []), [data?.equityHistory]);
  const balance = data?.balance;
  const fairOutput = data?.latestAgents.terra_fair_probability?.output;
  const latestTrigger = data?.decisionFeed.find((event) => event.type.startsWith("TRIGGER_LEVEL_"));
  const money = (value: string | number | null | undefined) => value === null || value === undefined ? "—" : `$${Number(value).toFixed(2)}`;

  return <main>
    <header className="topbar">
      <div><p className="eyebrow">RESEARCH CONTROL PLANE</p><h1>Polymarket Event Engine</h1></div>
      <div className="badges"><span className="paper">PAPER MODE · NO LIVE ORDERS</span><Status label={stream} good={stream === "LIVE"} /></div>
    </header>

    {!data && <section className="empty"><h2>Engine unavailable</h2><p>The dashboard has no cached values to invent. It will recover when the private engine stream reconnects.</p></section>}
    {data && <>
      <section className="kpis">
        <Kpi label="Equity" value={money(balance?.equity)} hint={`Cash ${money(balance?.cash)}`} />
        <Kpi label="Net P&L" value={money(balance?.netEconomicPnl)} hint={`Trading ${money(balance?.netTradingPnl)} · DD ${money(balance?.drawdown)}`} />
        <Kpi label="Open positions" value={String(data.positions.length)} hint={`${data.positions.filter((p) => Number(p.unpairedQuantity) > 0).length} unhedged`} />
        <Kpi label="AI spend today" value={money(data.aiUsage.costTodayUsd)} hint={`${data.aiUsage.calls} calls`} />
        <Kpi label="Experiment" value={data.engine.experimentStatus ?? "—"} hint={formatDuration(data.engine.uptimeSeconds)} />
      </section>

      <div className="grid two">
        <Panel title="Market scanner" meta={`${data.scanner.length} outcomes`}><div className="tableWrap"><table><thead><tr><th>Market / outcome</th><th>Bid</th><th>Ask</th><th>Spread</th><th>Depth</th><th>Age</th></tr></thead><tbody>{data.scanner.map((row) => <tr key={`${row.marketId}:${row.outcome}`}><td><strong>{short(row.marketId)}</strong><small>{row.outcome}</small></td><td>{decimal(row.bid)}</td><td>{decimal(row.ask)}</td><td>{row.spread === null ? "—" : `${(row.spread * 10_000).toFixed(0)}bp`}</td><td>{money(row.depthUsd)}</td><td>{age(row.lastMessageAt)}</td></tr>)}</tbody></table>{data.scanner.length === 0 && <EmptyRow text="Waiting for the first real market snapshot" />}</div></Panel>
        <Panel title="Equity & drawdown" meta={`${data.equityHistory.length} persisted points`}><div className="chart">{equityPoints ? <svg role="img" aria-label="Equity history" viewBox="0 0 600 180" preserveAspectRatio="none"><path className="area" d={`${equityPoints} L600,180 L0,180 Z`} /><polyline points={equityPoints} /></svg> : <p className="muted">No equity history yet.</p>}</div><div className="ledger"><span>Gross {money(balance?.grossPnl)}</span><span>Realized {money(balance?.realizedPnl)}</span><span>Unrealized {money(balance?.unrealizedPnl)}</span><span>Fees {money(balance?.fees)}</span><span>Slippage {money(balance?.slippage)}</span><span>AI {money(balance?.aiCost)}</span><span>Infra {money(balance?.infrastructureCost)}</span></div></Panel>
      </div>

      <Panel title="Six-agent deliberation" meta="Exact production topology"><div className="agents">{AGENTS.map(([id, title]) => { const agent = data.latestAgents[id]; return <article className="agent" key={id}><div><p className="eyebrow">{agent?.model ?? "NOT RUN"}</p><h3>{title}</h3></div><Status label={agent ? "COMPLETED" : "IDLE"} good={Boolean(agent)} /><dl><div><dt>Latency</dt><dd>{agent ? `${agent.durationMs}ms` : "—"}</dd></div><div><dt>Tokens</dt><dd>{agent ? agent.usage.inputTokens + agent.usage.outputTokens : "—"}</dd></div><div><dt>Cost</dt><dd>{money(agent?.usage.estimatedCostUsd)}</dd></div></dl><p className="agentOutput">{summarize(agent?.output)}</p></article>; })}</div></Panel>

      <div className="grid two">
        <Panel title="Consensus, edge & deterministic risk" meta={latestTrigger?.type.replace("TRIGGER_LEVEL_", "level ") ?? "no trigger"}><div className="decision"><strong>{data.consensus?.consensusDirection ?? "NO DECISION"}</strong><span>{data.consensus ? `${(data.consensus.consensusConfidence * 100).toFixed(1)}% confidence · ${(data.consensus.estimatedEdge * 10_000).toFixed(0)}bp raw edge · ${(data.consensus.signalFreshness * 100).toFixed(0)}% fresh · ${(data.consensus.agentAgreement * 100).toFixed(0)}% agreement` : "Awaiting qualified trigger"}</span><span>Market {decimal(data.scanner[0]?.ask)} · Fair {percent(numberField(fairOutput, "fairProbability"))} · Fees {money(balance?.fees)} · Slippage {money(balance?.slippage)}</span></div><div className="risk"><Status label={data.risk?.accepted ? "RISK PASS" : data.risk ? "VETO" : "NOT RUN"} good={data.risk?.accepted === true} /><p>{data.risk?.vetoes.join(" · ") || data.consensus?.conflicts.join(" · ") || "No risk evaluation has been persisted."}</p><p>{data.risk ? Object.entries(data.risk.utilization).map(([key, value]) => `${key} ${(value * 100).toFixed(0)}%`).join(" · ") : "Risk utilization unavailable."}</p></div></Panel>
        <Panel title="System health" meta={`event latency ${data.health.eventLatencyMs === null ? "—" : `${data.health.eventLatencyMs}ms`}`}><div className="health"><Health label="Polymarket WS" value={data.health.polymarketWs.state} detail={`${data.health.polymarketWs.reconnects} reconnects`} /><Health label="BTC WS" value={data.health.btcWs.state} detail={`${data.health.btcWs.reconnects} reconnects`} /><Health label="Postgres" value={data.health.database} detail="event journal" /><Health label="Engine" value={data.engine.ready ? "READY" : data.engine.draining ? "DRAINING" : "NOT READY"} detail={`queue ${data.engine.queueDepth}`} /></div></Panel>
      </div>

      <div className="grid two">
        <Panel title="Paper positions" meta={`${data.positions.length} active`}><div className="cards">{data.positions.map((p) => <article className="position" key={p.id}><strong>{short(p.marketId)}</strong><span>UP {Number(p.upQuantity).toFixed(2)} @ {decimal(p.averageUpCost)}</span><span>DOWN {Number(p.downQuantity).toFixed(2)} @ {decimal(p.averageDownCost)}</span><span className={Number(p.unpairedQuantity) > 0 ? "warn" : "ok"}>{Number(p.unpairedQuantity) > 0 ? `${p.unpairedQuantity} ${p.unpairedOutcome} unhedged` : `${p.pairedQuantity} paired`}</span></article>)}{data.positions.length === 0 && <p className="muted">No paper positions.</p>}</div></Panel>
        <Panel title="AI usage by agent" meta={`${data.aiUsage.inputTokens + data.aiUsage.outputTokens} tokens`}><div className="costs">{AGENTS.map(([id, title]) => <div key={id}><span>{title}</span><div className="bar"><i style={{ width: `${costWidth(data.aiUsage.costByAgent[id] ?? 0, data.aiUsage.costByAgent)}%` }} /></div><strong>{money(data.aiUsage.costByAgent[id] ?? 0)}</strong></div>)}</div></Panel>
      </div>

      <Panel title="Decision & audit feed" meta="trace-linked"><div className="feed">{data.decisionFeed.slice(0, 50).map((event) => <article key={event.id}><time>{new Date(event.at).toLocaleTimeString()}</time><strong>{event.type}</strong><span>{event.marketId ? short(event.marketId) : "system"}</span><code>{event.traceId}</code></article>)}{data.decisionFeed.length === 0 && <p className="muted">No events recorded.</p>}</div></Panel>
      <footer><span>Snapshot {new Date(data.generatedAt).toLocaleString()}</span><span>All orders simulated · prices and fees from live public feeds</span></footer>
    </>}
  </main>;
}

function Panel({ title, meta, children }: { title: string; meta: string; children: React.ReactNode }) { return <section className="panel"><div className="panelTitle"><h2>{title}</h2><span>{meta}</span></div>{children}</section>; }
function Kpi({ label, value, hint }: { label: string; value: string; hint: string }) { return <article className="kpi"><span>{label}</span><strong>{value}</strong><small>{hint}</small></article>; }
function Status({ label, good }: { label: string; good: boolean }) { return <span className={`status ${good ? "good" : "bad"}`}><i />{label}</span>; }
function Health({ label, value, detail }: { label: string; value: string; detail: string }) { const good = ["CONNECTED", "UP", "READY"].includes(value); return <article><div><strong>{label}</strong><small>{detail}</small></div><Status label={value} good={good} /></article>; }
function EmptyRow({ text }: { text: string }) { return <p className="muted pad">{text}</p>; }
function short(value: string) { return value.length > 24 ? `${value.slice(0, 12)}…${value.slice(-8)}` : value; }
function decimal(value: string | null | undefined) { return value === null || value === undefined ? "—" : Number(value).toFixed(4); }
function age(value: string) { const seconds = Math.max(0, Math.floor((Date.now() - new Date(value).getTime()) / 1_000)); return seconds < 60 ? `${seconds}s` : `${Math.floor(seconds / 60)}m`; }
function formatDuration(seconds: number) { return `${Math.floor(seconds / 3600)}h ${Math.floor(seconds % 3600 / 60)}m uptime`; }
function summarize(output: Record<string, unknown> | null | undefined) { if (!output) return "No structured output yet."; const candidate = output.summary ?? output.rationale ?? output.recommendation ?? output.action; return Array.isArray(candidate) ? candidate.join(" · ") : String(candidate ?? "Structured output captured."); }
function numberField(output: Record<string, unknown> | null | undefined, key: string) { const value = output?.[key]; return typeof value === "number" ? value : null; }
function percent(value: number | null) { return value === null ? "—" : `${(value * 100).toFixed(1)}%`; }
function costWidth(value: number, all: Record<string, number>) { const max = Math.max(0.000001, ...Object.values(all)); return Math.max(value > 0 ? 3 : 0, value / max * 100); }
function chartPoints(history: DashboardSnapshot["equityHistory"]) { if (history.length < 2) return null; const values = history.slice(-250).map((p) => Number(p.equity)); const min = Math.min(...values), max = Math.max(...values), range = Math.max(max - min, 0.01); return values.map((value, index) => `${index / (values.length - 1) * 600},${165 - (value - min) / range * 145}`).join(" "); }

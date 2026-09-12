import type { DashboardSnapshot } from "../../lib/types";
import { formatAge, formatNumber, formatTime, shortId, toneForState } from "../../lib/dashboard-selectors";
import { DataValue, EmptyState, Metric, Panel, StatusPill } from "./primitives";

export function MarketUniverse({ snapshot }: { snapshot: DashboardSnapshot }) {
  const discovery = snapshot.discovery;
  const qualified = discovery?.qualified ?? [];
  const rejected = discovery?.rejected ?? [];
  const status = snapshot.engine.discoveryStatus ?? snapshot.health.discoveryStatus ?? "UNKNOWN";
  const monitoring = snapshot.engine.marketMonitoring ?? snapshot.health.marketMonitoring ?? false;
  return <Panel title="Market Universe" kicker="DISCOVERY + SCANNER" meta={`${snapshot.scanner.length} scanner rows`}>
    <div className="metricGrid metricGrid--four">
      <Metric label="Qualified" value={formatNumber(snapshot.engine.qualifiedMarketCount ?? qualified.length, 0)} detail="current universe" tone={qualified.length ? "healthy" : "neutral"} />
      <Metric label="Rejected" value={formatNumber(rejected.length, 0)} detail="latest snapshot" tone={rejected.length ? "warning" : "neutral"} />
      <Metric label="Discovery" value={status} detail={snapshot.engine.discoveryLoopActive ? "loop active" : "loop idle"} tone={toneForState(status)} />
      <Metric label="Monitoring" value={monitoring ? "ACTIVE" : "OFF"} detail="market monitor" tone={monitoring ? "healthy" : "warning"} />
    </div>
    <div className="universeSplit">
      <div>
        <div className="sectionLabel">Qualified market set</div>
        {qualified.length === 0 ? <EmptyState title="No qualified markets" detail="The snapshot contains no qualified discovery result." /> : <div className="qualifiedList">{qualified.slice(0, 8).map((market) => <div className="qualifiedItem" key={market.marketId}><div><strong>{market.asset ?? market.slug}</strong><span>{shortId(market.marketId, 24)}</span></div><StatusPill label={`${market.tokens.length} TOKENS`} tone="healthy" /></div>)}</div>}
      </div>
      <div>
        <div className="sectionLabel">Scanner / outcomes</div>
        {snapshot.scanner.length === 0 ? <EmptyState title="Scanner is quiet" detail="No scanner outcomes are present in this snapshot." /> : <div className="tableWrap"><table><thead><tr><th>Market</th><th>Outcome</th><th>Bid / ask</th><th>Spread</th><th>Last message</th></tr></thead><tbody>{snapshot.scanner.slice(0, 8).map((row) => <tr key={`${row.marketId}:${row.outcome}`}><td title={row.marketId}>{shortId(row.marketId, 16)}</td><td>{row.asset ? `${row.asset} · ` : ""}{row.outcome}</td><td>{row.bid ?? "—"} / {row.ask ?? "—"}</td><td>{row.spread === null ? "—" : formatNumber(row.spread, 4)}</td><td>{formatAge(row.lastMessageAt)}</td></tr>)}</tbody></table></div>}
      </div>
    </div>
    <div className="panelFoot"><DataValue label="Snapshot generated" value={formatTime(snapshot.generatedAt)} /><DataValue label="Scanner rows shown" value={`${Math.min(snapshot.scanner.length, 8)} / ${snapshot.scanner.length}`} /></div>
  </Panel>;
}

import type { DashboardSnapshot } from "../../lib/types";
import { equityPoints, formatMoney, formatNumber } from "../../lib/dashboard-selectors";
import { DataValue, EmptyState, Metric, Panel } from "./primitives";

const ledgerFields = [
  ["Starting balance", "startingBalance"], ["Cash", "cash"], ["Reserved cash", "reservedCash"], ["Position value", "positionValue"], ["High water mark", "highWaterMark"], ["Realized PnL", "realizedPnl"],
  ["Unrealized PnL", "unrealizedPnl"], ["Gross PnL", "grossPnl"], ["Fees", "fees"], ["Slippage", "slippage"],
  ["Net Trading PnL", "netTradingPnl"],
] as const;

export function PortfolioPanel({ snapshot }: { snapshot: DashboardSnapshot }) {
  const points = equityPoints(snapshot.equityHistory);
  return <Panel title="Portfolio / Equity" kicker="PAPER LEDGER" meta={`${snapshot.positions.length} open positions`}>
    <div className="metricGrid metricGrid--four">
      <Metric label="Equity" value={formatMoney(snapshot.balance.equity)} detail="current ledger" />
      <Metric label="Drawdown" value={formatMoney(snapshot.balance.drawdown)} detail="ledger drawdown" tone={Number(snapshot.balance.drawdown) > 0 ? "warning" : "healthy"} />
      <Metric label="Net Trading PnL" value={formatMoney(snapshot.balance.netTradingPnl)} detail="realized + unrealized, less fees" />
      <Metric label="Gross PnL" value={formatMoney(snapshot.balance.grossPnl)} detail="realized + unrealized" />
    </div>
    <div className="metricGrid metricGrid--four" aria-label="Current experiment results">
      <Metric label="Completed" value={snapshot.tradingResults ? formatNumber(snapshot.tradingResults.completed, 0) : "—"} detail="settled / closed episodes" />
      <Metric label="Wins" value={snapshot.tradingResults ? formatNumber(snapshot.tradingResults.wins, 0) : "—"} detail="after fees" />
      <Metric label="Losses" value={snapshot.tradingResults ? formatNumber(snapshot.tradingResults.losses, 0) : "—"} detail="after fees" />
      <Metric label="Win Rate" value={snapshot.tradingResults?.winRate != null ? `${(snapshot.tradingResults.winRate * 100).toFixed(1)}%` : "—"} detail="wins / completed" />
    </div>
    {snapshot.tradingResults ? <DataValue label="Breakeven" value={formatNumber(snapshot.tradingResults.breakeven, 0)} /> : <p className="panelFoot">Current experiment results unavailable from retained accounting history.</p>}
    <div className="metricGrid metricGrid--four" aria-label="Actual dollar results">
      <Metric label="Completed net PnL" value={snapshot.economicResults ? formatMoney(snapshot.economicResults.completedNetPnl) : "—"} detail="closed trade cashflows after fees" />
      <Metric label="Meteors" value={snapshot.economicResults ? String(snapshot.economicResults.meteors) : "—"} detail="closed trade net PnL ≤ −$2" />
      <Metric label="Largest trade loss" value={snapshot.economicResults ? formatMoney(snapshot.economicResults.worstTradeNetPnl) : "—"} detail="closed trades after fees" />
      <Metric label="Net economic PnL" value={formatMoney(snapshot.balance.netEconomicPnl)} detail="net trading less operating costs" />
    </div>
    {points ? <div className="equityChart"><div className="chartLabel"><span>Equity history</span><span>{snapshot.equityHistory.length} points</span></div><svg viewBox="0 0 600 165" role="img" aria-label="Equity history line chart"><polyline points={points} fill="none" /><line x1="0" y1="164" x2="600" y2="164" /></svg></div> : <EmptyState title="No equity curve" detail="At least two valid equity history points are required." />}
    <div className="ledgerGrid">{ledgerFields.map(([label, key]) => <DataValue key={key} label={label} value={formatMoney(snapshot.balance[key])} />)}</div>
    <div className="panelFoot"><DataValue label="Paired / unpaired" value={`${snapshot.positions.reduce((sum, position) => sum + Number(position.pairedQuantity), 0).toFixed(2)} / ${snapshot.positions.reduce((sum, position) => sum + Number(position.unpairedQuantity), 0).toFixed(2)}`} /><DataValue label="Updated" value={snapshot.balance.updatedAt} /></div>
  </Panel>;
}

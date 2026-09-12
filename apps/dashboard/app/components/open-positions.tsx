import type { DashboardSnapshot } from "../../lib/types";
import { formatMoney, formatNumber, formatTime, shortId } from "../../lib/dashboard-selectors";
import { EmptyState, Panel, StatusPill } from "./primitives";

export function OpenPositions({ snapshot }: { snapshot: DashboardSnapshot }) {
  return <Panel title="Open Positions" kicker="POSITION LEDGER" meta={`${snapshot.positions.length} active`}>
    {snapshot.positions.length === 0 ? <EmptyState title="NO OPEN POSITIONS" detail="No paper position is present in the current ledger snapshot." /> : <div className="positionTable tableWrap"><table><thead><tr><th>Market</th><th>UP qty / avg</th><th>DOWN qty / avg</th><th>Paired</th><th>Unpaired</th><th>Opened</th></tr></thead><tbody>{snapshot.positions.map((position) => { const unpaired = Number(position.unpairedQuantity) > 0; return <tr key={position.id}><td title={position.marketId}><strong>{shortId(position.marketId, 19)}</strong><small>{shortId(position.id, 19)}</small></td><td>{formatNumber(position.upQuantity)} / {formatMoney(position.averageUpCost)}</td><td>{formatNumber(position.downQuantity)} / {formatMoney(position.averageDownCost)}</td><td>{formatNumber(position.pairedQuantity)}</td><td><StatusPill label={unpaired ? `${formatNumber(position.unpairedQuantity)} ${position.unpairedOutcome ?? "UNPAIRED"}` : "NONE"} tone={unpaired ? "warning" : "healthy"} /></td><td>{formatTime(position.openedAt)}</td></tr>; })}</tbody></table></div>}
  </Panel>;
}

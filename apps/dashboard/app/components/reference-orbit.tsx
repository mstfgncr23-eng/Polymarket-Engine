import type { CSSProperties } from "react";
import type { DashboardSnapshot } from "../../lib/types";
import { formatAge, referenceTone, toneForState } from "../../lib/dashboard-selectors";
import { StatusPill } from "./primitives";

export function ReferenceOrbit({ feeds, overallTone }: { feeds: NonNullable<DashboardSnapshot["health"]["referenceFeeds"]>; overallTone: ReturnType<typeof referenceTone> }) {
  return <article className={`referenceNode systemNode--${overallTone}`} aria-labelledby="reference-node-title">
    <div className="nodeTopline"><span className="nodeEyebrow">MARKET / REFERENCE DATA</span><StatusPill label={feeds.length ? `${feeds.length} FEEDS` : "NO FEEDS"} tone={overallTone} /></div>
    <h3 id="reference-node-title">Reference Galaxy</h3>
    <p className="nodeDetail">Connection state only · prices and momentum are not exposed in the dashboard snapshot.</p>
    <div className="referencePlanets" role="list" aria-label="Active reference feeds">
      {feeds.map((feed, index) => <div className={`referencePlanet referencePlanet--${toneForState(feed.state)}`} role="listitem" key={feed.asset} style={{ "--planet-index": index } as CSSProperties} title={`${feed.asset} ${feed.state}`}><span>{feed.asset}</span><small>{feed.state}</small><em>{formatAge(feed.lastMessageAt)} · {feed.reconnects} reconnects</em></div>)}
      {feeds.length === 0 && <span className="muted">Awaiting feed connection data</span>}
    </div>
    {feeds.length > 0 && <div className="referenceFoot"><span>{feeds.filter((feed) => feed.state === "CONNECTED").length} connected</span><span>{feeds.reduce((sum, feed) => sum + feed.reconnects, 0)} reconnects</span></div>}
  </article>;
}

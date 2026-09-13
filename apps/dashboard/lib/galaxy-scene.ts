import {
  decisionAuthorityLabel,
  decisionAuthorityTone,
  formatBasisPoints,
  formatMoney,
  formatPercent,
  formatTime,
  latest,
  referenceTone,
  shortId,
  toneForState,
  triggerEvents,
  type Tone,
} from "./dashboard-selectors";
import type { DashboardSnapshot } from "./types";

export const GALAXY_FPS = 30;
export const GALAXY_DURATION_IN_FRAMES = GALAXY_FPS * 60 * 10;
export const MAX_GALAXY_TRANSIENTS = 12;
export const MAX_GALAXY_POSITION_SATELLITES = 8;
export const GALAXY_TRANSIENT_LIFETIME_MS = 4_000;
const MAX_SEEN_EVENT_IDS = 64;
const MAX_TRACKED_REFERENCES = 32;
const MAX_TRACKED_MARKETS = 160;
const REFERENCE_PULSE_COALESCE_MS = 1_200;
const MARKET_PULSE_COALESCE_MS = 900;

export type GalaxyMotionKind =
  | "trigger"
  | "decision"
  | "risk-accept"
  | "risk-veto"
  | "order"
  | "fill"
  | "cancel"
  | "execution-error"
  | "settlement"
  | "reference"
  | "market";

export interface GalaxyTransient {
  id: string;
  kind: GalaxyMotionKind;
  startFrame: number;
  expiresAtMs: number;
  severity: number;
  asset: string | null;
}

export interface GalaxyMotionState {
  seenEventIds: string[];
  referenceTimestamps: Record<string, string | null>;
  scannerTimestamps: Record<string, string>;
  transients: GalaxyTransient[];
  lastReferencePulseAtMs: number;
  lastMarketPulseAtMs: number;
}

export interface GalaxySceneModel {
  generatedAt: string;
  reference: {
    tone: Tone;
    connected: number;
    feeds: Array<{
      asset: string;
      state: string;
      tone: Tone;
      lastMessageAt: string | null;
      reconnects: number;
    }>;
  };
  market: {
    status: string;
    tone: Tone;
    qualified: number;
    outcomes: number;
    latestMessageAt: string | null;
  };
  trigger: {
    status: string;
    tone: Tone;
    severity: number;
    kind: string;
    detail: string;
  };
  core: {
    available: boolean;
    authority: string;
    authorityTone: Tone;
    direction: string;
    confidence: string;
    fairProbability: string;
    marketProbability: string;
    estimatedEdge: string;
    calculatedAt: string;
  };
  risk: {
    status: string;
    tone: Tone;
    vetoes: number;
    detail: string;
  };
  execution: {
    status: string;
    tone: Tone;
    detail: string;
  };
  portfolio: {
    status: string;
    tone: Tone;
    equity: string;
    cash: string;
    positions: number;
    satellites: Array<{ id: string; label: string }>;
  };
}

const semanticKinds: Partial<Record<string, GalaxyMotionKind>> = {
  NATIVE_DECISION_COMPLETE: "decision",
  RISK_ACCEPT: "risk-accept",
  RISK_VETO: "risk-veto",
  PAPER_ORDER_CREATED: "order",
  PAPER_FILL: "fill",
  PAPER_ORDER_CANCELLED: "cancel",
  PAPER_EXECUTION_ERROR: "execution-error",
  POSITION_SETTLED: "settlement",
};

function semanticKind(type: string): GalaxyMotionKind | null {
  if (type.startsWith("TRIGGER_LEVEL_")) return "trigger";
  return semanticKinds[type] ?? null;
}

function eventSeverity(type: string): number {
  if (!type.startsWith("TRIGGER_LEVEL_")) return type === "RISK_VETO" || type === "PAPER_EXECUTION_ERROR" ? 3 : 1;
  const parsed = Number(type.split("_").at(-1));
  return Number.isFinite(parsed) ? Math.max(1, Math.min(3, parsed)) : 1;
}

function semanticEvents(snapshot: DashboardSnapshot) {
  return snapshot.decisionFeed.filter((event) => semanticKind(event.type)).slice(0, MAX_SEEN_EVENT_IDS);
}

function referenceTimestampMap(snapshot: DashboardSnapshot): Record<string, string | null> {
  return Object.fromEntries((snapshot.health.referenceFeeds ?? []).slice(0, MAX_TRACKED_REFERENCES).map((feed) => [feed.asset, feed.lastMessageAt]));
}

function scannerTimestampMap(snapshot: DashboardSnapshot): Record<string, string> {
  return Object.fromEntries(snapshot.scanner.slice(0, MAX_TRACKED_MARKETS).map((row) => [`${row.marketId}:${row.outcome}`, row.lastMessageAt]));
}

function transientFromEvent(event: DashboardSnapshot["decisionFeed"][number], startFrame: number, nowMs: number): GalaxyTransient | null {
  const kind = semanticKind(event.type);
  if (!kind) return null;
  return {
    id: event.id,
    kind,
    startFrame,
    expiresAtMs: nowMs + GALAXY_TRANSIENT_LIFETIME_MS,
    severity: eventSeverity(event.type),
    asset: event.asset ?? null,
  };
}

export function initializeGalaxyMotionState(snapshot: DashboardSnapshot, _startFrame: number, _nowMs: number): GalaxyMotionState {
  const events = semanticEvents(snapshot);
  return {
    seenEventIds: events.map((event) => event.id),
    referenceTimestamps: referenceTimestampMap(snapshot),
    scannerTimestamps: scannerTimestampMap(snapshot),
    transients: [],
    lastReferencePulseAtMs: 0,
    lastMarketPulseAtMs: 0,
  };
}

export function expireGalaxyMotionState(state: GalaxyMotionState, nowMs: number): GalaxyMotionState {
  const transients = state.transients.filter((event) => event.expiresAtMs > nowMs);
  return transients.length === state.transients.length ? state : { ...state, transients };
}

export function advanceGalaxyMotionState(
  state: GalaxyMotionState,
  snapshot: DashboardSnapshot,
  startFrame: number,
  nowMs: number,
): GalaxyMotionState {
  const active = state.transients.filter((event) => event.expiresAtMs > nowMs);
  const events = semanticEvents(snapshot);
  const seen = new Set(state.seenEventIds);
  const additions = events
    .filter((event) => !seen.has(event.id))
    .slice(0, 4)
    .map((event) => transientFromEvent(event, startFrame, nowMs))
    .filter((event): event is GalaxyTransient => Boolean(event));

  const references = referenceTimestampMap(snapshot);
  const changedReference = Object.entries(references).find(([asset, timestamp]) => timestamp && state.referenceTimestamps[asset] !== undefined && state.referenceTimestamps[asset] !== timestamp);
  const referenceReady = changedReference && nowMs - state.lastReferencePulseAtMs >= REFERENCE_PULSE_COALESCE_MS;
  if (referenceReady) {
    additions.push({
      id: `reference:${changedReference[0]}:${changedReference[1]}`,
      kind: "reference",
      startFrame,
      expiresAtMs: nowMs + GALAXY_TRANSIENT_LIFETIME_MS,
      severity: 1,
      asset: changedReference[0],
    });
  }

  const scanner = scannerTimestampMap(snapshot);
  const changedMarket = Object.entries(scanner).find(([key, timestamp]) => state.scannerTimestamps[key] !== undefined && state.scannerTimestamps[key] !== timestamp);
  const marketReady = changedMarket && nowMs - state.lastMarketPulseAtMs >= MARKET_PULSE_COALESCE_MS;
  if (marketReady) {
    additions.push({
      id: `market:${changedMarket[0]}:${changedMarket[1]}`,
      kind: "market",
      startFrame,
      expiresAtMs: nowMs + GALAXY_TRANSIENT_LIFETIME_MS,
      severity: 1,
      asset: null,
    });
  }

  const unique = new Map<string, GalaxyTransient>();
  for (const event of [...additions, ...active]) unique.set(event.id, event);
  return {
    seenEventIds: events.map((event) => event.id),
    referenceTimestamps: references,
    scannerTimestamps: scanner,
    transients: [...unique.values()].slice(0, MAX_GALAXY_TRANSIENTS),
    lastReferencePulseAtMs: referenceReady ? nowMs : state.lastReferencePulseAtMs,
    lastMarketPulseAtMs: marketReady ? nowMs : state.lastMarketPulseAtMs,
  };
}

export function buildGalaxySceneModel(snapshot: DashboardSnapshot): GalaxySceneModel {
  const feeds = snapshot.health.referenceFeeds ?? [];
  const triggerEvent = latest(triggerEvents(snapshot));
  const trigger = latest(snapshot.recentTriggers);
  const risk = snapshot.risk;
  const execution = snapshot.decisionFeed.find((event) => event.type.startsWith("PAPER_") || event.type === "POSITION_SETTLED");
  const marketTone = snapshot.engine.marketMonitoring ? "healthy" : snapshot.engine.discoveryStatus === "RECONCILING" ? "warning" : "neutral";
  const triggerSeverity = trigger?.severity ?? (triggerEvent ? eventSeverity(triggerEvent.type) : 0);
  const decision = snapshot.consensus;
  return {
    generatedAt: snapshot.generatedAt,
    reference: {
      tone: referenceTone(snapshot),
      connected: feeds.filter((feed) => feed.state === "CONNECTED").length,
      feeds: feeds.map((feed) => ({ ...feed, tone: toneForState(feed.state) })),
    },
    market: {
      status: snapshot.engine.marketMonitoring ? "MONITORING" : snapshot.engine.discoveryStatus ?? "WAITING",
      tone: marketTone,
      qualified: snapshot.engine.qualifiedMarketCount ?? 0,
      outcomes: snapshot.scanner.length,
      latestMessageAt: snapshot.scanner.reduce<string | null>((latestAt, row) => !latestAt || row.lastMessageAt > latestAt ? row.lastMessageAt : latestAt, null),
    },
    trigger: {
      status: trigger || triggerEvent ? `LEVEL ${triggerSeverity}` : "QUIET",
      tone: triggerSeverity >= 3 ? "critical" : trigger || triggerEvent ? "healthy" : "neutral",
      severity: triggerSeverity,
      kind: trigger?.kind ?? (typeof triggerEvent?.detail.kind === "string" ? triggerEvent.detail.kind : "Awaiting trigger"),
      detail: trigger ? `${trigger.asset ?? "system"} · ${trigger.marketId ?? "unscoped"}` : triggerEvent ? `${triggerEvent.asset ?? "system"} · ${triggerEvent.marketId ?? "unscoped"}` : "No trigger event in feed",
    },
    core: {
      available: Boolean(decision),
      authority: decisionAuthorityLabel(decision),
      authorityTone: decisionAuthorityTone(decision),
      direction: decision?.consensusDirection ?? "AWAITING",
      confidence: formatPercent(decision?.consensusConfidence),
      fairProbability: formatPercent(decision?.fairProbability),
      marketProbability: formatPercent(decision?.marketProbability),
      estimatedEdge: formatBasisPoints(decision?.estimatedEdge),
      calculatedAt: formatTime(decision?.calculatedAt),
    },
    risk: {
      status: risk ? risk.accepted ? "PASS" : "VETO" : "NOT RUN",
      tone: risk ? risk.accepted ? "healthy" : "critical" : "neutral",
      vetoes: risk?.vetoes.length ?? 0,
      detail: risk ? `evaluated ${formatTime(risk.evaluatedAt)}` : "No risk evaluation persisted",
    },
    execution: {
      status: execution?.type === "PAPER_EXECUTION_ERROR" ? "ERROR" : execution ? execution.type.replace("PAPER_", "") : "IDLE",
      tone: execution?.type === "PAPER_EXECUTION_ERROR" ? "critical" : execution?.type === "PAPER_ORDER_CANCELLED" ? "warning" : execution ? "healthy" : "neutral",
      detail: execution?.marketId ?? "No execution event in feed",
    },
    portfolio: {
      status: snapshot.positions.length ? "TRACKING" : "EMPTY",
      tone: snapshot.positions.length ? "healthy" : "neutral",
      equity: formatMoney(snapshot.balance.equity),
      cash: formatMoney(snapshot.balance.cash),
      positions: snapshot.positions.length,
      satellites: snapshot.positions.slice(0, MAX_GALAXY_POSITION_SATELLITES).map((position) => ({
        id: position.id,
        label: shortId(position.marketId, 12),
      })),
    },
  };
}

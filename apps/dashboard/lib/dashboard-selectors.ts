import type { DashboardSnapshot } from "./types";

export type Tone = "healthy" | "warning" | "critical" | "neutral";
export const NATIVE_AUTHORITY = "NATIVE_DETERMINISTIC_V1";
export const ASTRA_AUTHORITY = "ASTRA_BRAIN_V1";

export const AGENT_ROLES = [
  ["market_microstructure_scanner", "Market Microstructure"],
  ["crypto_market_scanner", "Crypto Market"],
  ["news_x_intelligence_scanner", "News / X Intelligence"],
  ["fair_probability_analyst", "Fair Probability"],
  ["risk_hedge_analyst", "Risk & Hedge"],
  ["strategy_supervisor", "Strategy Supervisor"],
] as const;

export const EXECUTION_EVENT_TYPES = [
  "PAPER_ORDER_CREATED",
  "PAPER_FILL",
  "PAPER_ORDER_CANCELLED",
  "PAPER_EXECUTION_ERROR",
] as const;

export const DECISION_ACTIVITY_EVENT_TYPES = [
  "NATIVE_DECISION_EVALUATION",
  "NATIVE_DECISION_COMPLETE",
  "CONSENSUS_DECISION",
  "ASTRA_MODEL_INVOCATION",
  "ASTRA_DECISION",
  "ASTRA_DEEP_REVIEW",
  "ASTRA_FAILURE",
  "ASTRA_BUDGET_BLOCK",
  "BOOK_REVALIDATION_REJECTED",
] as const;

export const MISSION_EVENT_TYPES = [
  "TRIGGER_LEVEL_1", "TRIGGER_LEVEL_2", "TRIGGER_LEVEL_3",
  ...DECISION_ACTIVITY_EVENT_TYPES,
  "RISK_ACCEPT", "RISK_VETO", "PAPER_ORDER_CREATED", "PAPER_FILL",
  "PAPER_ORDER_CANCELLED", "PAPER_EXECUTION_ERROR", "POSITION_SETTLED",
] as const;

export type DashboardEvent = DashboardSnapshot["decisionFeed"][number];

export function triggerEvents(snapshot: DashboardSnapshot): DashboardEvent[] {
  return snapshot.decisionFeed.filter((event) => event.type.startsWith("TRIGGER_LEVEL_"));
}

export function nativeDecisionEvents(snapshot: DashboardSnapshot): DashboardEvent[] {
  return snapshot.decisionFeed.filter((event) => event.type === "NATIVE_DECISION_COMPLETE");
}

export function decisionActivityEvents(snapshot: DashboardSnapshot): DashboardEvent[] {
  return snapshot.decisionFeed.filter((event) => (DECISION_ACTIVITY_EVENT_TYPES as readonly string[]).includes(event.type));
}

export function executionEvents(snapshot: DashboardSnapshot): DashboardEvent[] {
  return snapshot.decisionFeed.filter((event) => (EXECUTION_EVENT_TYPES as readonly string[]).includes(event.type));
}

export function missionEvents(snapshot: DashboardSnapshot): DashboardEvent[] {
  return snapshot.decisionFeed.filter((event) => (MISSION_EVENT_TYPES as readonly string[]).includes(event.type));
}

export function latest<T>(items: T[]): T | null {
  return items[0] ?? null;
}

export function toneForState(state: string | null | undefined): Tone {
  switch (state) {
    case "CONNECTED":
    case "UP":
    case "READY":
    case "ALIVE":
    case "ACTIVE":
    case "MONITORING":
    case "NORMAL":
      return "healthy";
    case "CONNECTING":
    case "RECONCILING":
    case "IDLE":
    case "WAITING":
      return "warning";
    case "STALE":
      return "warning";
    case "DISCONNECTED":
    case "DOWN":
    case "STOPPED":
    case "DRAINING":
    case "FAIL_CLOSED":
    case "FAILED":
      return "critical";
    default:
      return "neutral";
  }
}

export function referenceTone(snapshot: DashboardSnapshot): Tone {
  const feeds = snapshot.health.referenceFeeds ?? [];
  if (feeds.length === 0) return "neutral";
  if (feeds.some((feed) => toneForState(feed.state) === "critical")) return "critical";
  if (feeds.some((feed) => toneForState(feed.state) === "warning")) return "warning";
  return "healthy";
}

function isHealthyAuthority(authority: string | null | undefined): boolean {
  return authority === NATIVE_AUTHORITY || authority === ASTRA_AUTHORITY;
}

export function decisionAuthorityLabel(consensus: DashboardSnapshot["consensus"]): string {
  if (!consensus) return "—";
  if (consensus.authority === NATIVE_AUTHORITY) return NATIVE_AUTHORITY;
  if (consensus.authority) return consensus.authority;
  return "AUTHORITY UNAVAILABLE";
}

export function decisionAuthorityTone(consensus: DashboardSnapshot["consensus"]): Tone {
  if (!consensus) return "neutral";
  return isHealthyAuthority(consensus.authority) ? "healthy" : "warning";
}

export function decisionActivityLabel(event: DashboardEvent | null): string {
  if (!event) return "AWAITING DECISION";
  switch (event.type) {
    case "ASTRA_MODEL_INVOCATION": return "ASTRA MODEL INVOKED";
    case "ASTRA_DECISION": return event.detail.accepted === false ? "ASTRA GATE REJECTED" : "ASTRA DECISION";
    case "ASTRA_DEEP_REVIEW": return "ASTRA DEEP REVIEW";
    case "ASTRA_FAILURE": return "ASTRA FAILURE";
    case "ASTRA_BUDGET_BLOCK": return "ASTRA BUDGET BLOCK";
    case "BOOK_REVALIDATION_REJECTED": return "BOOK REVALIDATION REJECTED";
    case "NATIVE_DECISION_EVALUATION": return "NATIVE EVALUATION";
    case "NATIVE_DECISION_COMPLETE": return "NATIVE DECISION";
    case "CONSENSUS_DECISION": return "CONSENSUS DECISION";
    default: return event.type;
  }
}

export function decisionActivityTone(event: DashboardEvent | null): Tone {
  if (!event) return "neutral";
  if (["ASTRA_FAILURE", "ASTRA_BUDGET_BLOCK", "BOOK_REVALIDATION_REJECTED"].includes(event.type)) return "critical";
  if (event.type === "ASTRA_DECISION" && event.detail.accepted === false) return "warning";
  if (event.type === "NATIVE_DECISION_EVALUATION" || event.type === "ASTRA_MODEL_INVOCATION" || event.type === "ASTRA_DEEP_REVIEW") return "warning";
  return "healthy";
}

export function formatMoney(value: string | number | null | undefined): string {
  if (value === null || value === undefined || value === "") return "—";
  const number = Number(value);
  return Number.isFinite(number) ? `$${number.toFixed(2)}` : "—";
}

export function formatNumber(value: string | number | null | undefined, digits = 2): string {
  if (value === null || value === undefined || value === "") return "—";
  const number = Number(value);
  return Number.isFinite(number) ? number.toFixed(digits) : "—";
}

export function formatPercent(value: number | null | undefined, digits = 1): string {
  return typeof value === "number" && Number.isFinite(value) ? `${(value * 100).toFixed(digits)}%` : "—";
}

export function formatBasisPoints(value: number | null | undefined): string {
  return typeof value === "number" && Number.isFinite(value) ? `${(value * 10_000).toFixed(0)}bp` : "—";
}

export function formatTime(value: string | null | undefined): string {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isFinite(date.getTime()) ? date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }) : "—";
}

export function formatAge(value: string | null | undefined, relativeTo?: string | number | Date): string {
  if (!value) return "—";
  const timestamp = new Date(value).getTime();
  if (!Number.isFinite(timestamp)) return "—";
  const referenceTime = relativeTo === undefined ? Date.now() : new Date(relativeTo).getTime();
  if (!Number.isFinite(referenceTime)) return "—";
  const seconds = Math.max(0, Math.floor((referenceTime - timestamp) / 1_000));
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3_600) return `${Math.floor(seconds / 60)}m ago`;
  return `${Math.floor(seconds / 3_600)}h ago`;
}

export function shortId(value: string | null | undefined, length = 18): string {
  if (!value) return "—";
  return value.length > length ? `${value.slice(0, Math.floor(length * 0.55))}…${value.slice(-Math.floor(length * 0.3))}` : value;
}

export function eventSummary(event: DashboardEvent): string {
  const detail = event.detail;
  const parts: string[] = [];
  if (typeof detail.kind === "string") parts.push(detail.kind);
  if (typeof detail.stage === "string") parts.push(detail.stage);
  if (typeof detail.direction === "string") parts.push(detail.direction);
  if (detail.accepted === false) parts.push("rejected");
  if (typeof detail.gateReason === "string") parts.push(detail.gateReason);
  if (typeof detail.reason === "string") parts.push(detail.reason);
  if (typeof detail.orderId === "string") parts.push(`order ${shortId(detail.orderId)}`);
  if (typeof detail.fillId === "string") parts.push(`fill ${shortId(detail.fillId)}`);
  if (typeof detail.price === "string") parts.push(`@ ${detail.price}`);
  if (typeof detail.quantity === "string") parts.push(`qty ${detail.quantity}`);
  if (typeof detail.role === "string") parts.push(detail.role);
  if (Array.isArray(detail.reasons)) parts.push(detail.reasons.join(", "));
  return parts.join(" · ") || (event.marketId ? shortId(event.marketId) : "System event");
}

/** Semantic lifecycle pulses only; high-frequency market/reference traffic is deliberately excluded in V1. */
export function pulseToken(snapshot: DashboardSnapshot, node: "trigger" | "core" | "risk" | "execution" | "portfolio"): string | null {
  const event = snapshot.decisionFeed.find((candidate) => {
    const type = candidate.type;
    if (node === "trigger") return type.startsWith("TRIGGER_LEVEL_");
    if (node === "core") return (DECISION_ACTIVITY_EVENT_TYPES as readonly string[]).includes(type);
    if (node === "risk") return type === "RISK_ACCEPT" || type === "RISK_VETO";
    if (node === "execution") return (EXECUTION_EVENT_TYPES as readonly string[]).includes(type);
    if (node === "portfolio") return type === "PAPER_FILL" || type === "POSITION_SETTLED";
    return false;
  });
  return event?.id ?? null;
}

export function equityPoints(history: DashboardSnapshot["equityHistory"]): string | null {
  if (history.length < 2) return null;
  const values = history.slice(-160).map((point) => Number(point.equity));
  if (values.some((value) => !Number.isFinite(value))) return null;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = Math.max(max - min, 0.01);
  return values.map((value, index) => `${(index / (values.length - 1)) * 600},${165 - ((value - min) / range) * 145}`).join(" ");
}

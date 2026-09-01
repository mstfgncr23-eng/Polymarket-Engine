export const DECISION_EVENT_TYPES = [
  "ENGINE_STARTED", "ENGINE_DRAINING", "ENGINE_STOPPED", "STATE_RECOVERED",
  "DISCOVERY_RECONCILED",
  "TRIGGER_EVENT", "TRIGGER_LEVEL_1", "TRIGGER_LEVEL_2", "TRIGGER_LEVEL_3",
  "GROK_RESEARCH_AVAILABLE", "GROK_ACTIVATION_DECISION", "GROK_ACTIVATION_RESULT", "GROK_SIGNAL_ACCEPTED",
  "CONSENSUS_DECISION", "SEVERE_DISAGREEMENT",
  "RISK_EVALUATION", "RISK_VETO", "RISK_ACCEPT",
  "PAPER_ORDER_CREATED", "PAPER_FILL", "PAPER_EXECUTION_ERROR", "PAPER_MAKER_RESERVED", "PAPER_MAKER_RELEASED", "PAPER_ORDER_CANCELLED",
  "POSITION_UPDATED", "POSITION_SETTLED", "POSITION_WIN", "POSITION_LOSS", "POSITION_BREAKEVEN", "PAIR_REALIZED", "PNL_SNAPSHOT",
  "MARKET_RESOLUTION_OBSERVED", "HEDGE_COMPLETE", "FAILED_HEDGE", "FIRST_LEG_FILLED",
  "EXPERIMENT_STARTED", "EXPERIMENT_ENTRY_CLOSED", "EXPERIMENT_COMPLETED", "EXPERIMENT_REPORT_GENERATED", "EXPERIMENT_PREFLIGHT",
  "REFERENCE_FEED_STARTED", "REFERENCE_FEED_STOPPED", "REFERENCE_UNSUPPORTED", "REFERENCE_PROBE_TRANSIENT", "REFERENCE_PROVIDER_BLOCKED",
  "SYSTEM_HEALTH_EVENT", "MCP_INTERACTION", "STRATEGY_CHANGE_PROPOSED",
] as const;

export const TELEMETRY_EVENT_TYPES = ["MARKET_SNAPSHOT", "REFERENCE_TICK", "MARKET_EVENT"] as const;

export function isTelemetryEventType(type: string): boolean {
  return (TELEMETRY_EVENT_TYPES as readonly string[]).includes(type);
}

export interface JournalQuery {
  types?: readonly string[];
  experimentId?: string;
  from?: Date;
  to?: Date;
  limit?: number;
  offset?: number;
  /** asc = earliest first (required for multi-page scans). desc = newest-first window. */
  order?: "asc" | "desc";
}

/** Bounded page size for full-experiment scans. Do not raise a silent truncation cap. */
export const EVENT_PAGE_SIZE = 500;

/** Safety ceiling for paged scans. Hitting it MUST mark the scan incomplete — never complete=true. */
export const EVENT_SCAN_CEILING = 2_000_000;

export function compareJournalEventOrder(
  a: { occurredAt: string; id: string },
  b: { occurredAt: string; id: string },
): number {
  const byTime = a.occurredAt.localeCompare(b.occurredAt);
  if (byTime !== 0) return byTime;
  return a.id.localeCompare(b.id);
}

export function sortJournalEventsChronologically<T extends { occurredAt: string; id: string }>(events: readonly T[]): T[] {
  return [...events].sort(compareJournalEventOrder);
}

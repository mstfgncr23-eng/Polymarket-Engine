import { DECISION_EVENT_TYPES, TELEMETRY_EVENT_TYPES } from "./journal-types.js";

/** Grok submission timestamp skew accepted by submitGrokSignal (5 minutes). */
export const GROK_SIGNAL_MAX_SKEW_MS = 5 * 60_000;

/** Default trigger TTL. Signal/trigger acceptance cannot outlive this plus output expiresAt. */
export const DEFAULT_TRIGGER_TTL_MS = 30_000;

/** Extra margin beyond the longest acceptance horizon before an idempotency key may be deleted. */
export const MCP_IDEMPOTENCY_TTL_MARGIN_MS = 60 * 60_000;

export const MCP_IDEMPOTENCY_MIN_TTL_MS =
  GROK_SIGNAL_MAX_SKEW_MS + DEFAULT_TRIGGER_TTL_MS + MCP_IDEMPOTENCY_TTL_MARGIN_MS;

export const RETENTION_DEFAULTS = {
  telemetryTtlMs: 6 * 60 * 60_000,
  monitorTtlMs: 24 * 60 * 60_000,
  completedGraceMs: 7 * 24 * 60 * 60_000,
  mcpIdempotencyTtlMs: 24 * 60 * 60_000,
  checkpointKeep: 8,
  batchSize: 200,
  cycleBudgetMs: 250,
  maxBatchesPerCycle: 20,
  maxRowsPerCycle: 4_000,
  enabled: false,
} as const;

export const RETENTION_BATCH_SIZE_MAX = 500;

export type RetentionClass =
  | "recovery_authoritative"
  | "checkpoint"
  | "mcp_idempotency"
  | "raw_telemetry"
  | "monitor_control"
  | "long_lived_audit"
  | "unknown_retain";

export const AUDIT_CRITICAL_EVENT_TYPES = [
  "ENGINE_STARTED", "ENGINE_DRAINING", "ENGINE_STOPPED", "STATE_RECOVERED",
  "EXPERIMENT_STARTED", "EXPERIMENT_START_VALIDATED", "EXPERIMENT_START_ABORTED", "EXPERIMENT_ENTRY_CLOSED", "EXPERIMENT_COMPLETED",
  "EXPERIMENT_REPORT_GENERATED", "EXPERIMENT_SOURCE_SEALED", "EXPERIMENT_PREFLIGHT",
  "PAPER_ORDER_CREATED", "PAPER_FILL", "PAPER_EXECUTION_ERROR",
  "PAPER_MAKER_RESERVED", "PAPER_MAKER_RELEASED", "PAPER_ORDER_CANCELLED",
  "POSITION_UPDATED", "POSITION_SETTLED", "POSITION_WIN", "POSITION_LOSS", "POSITION_BREAKEVEN",
  "PAIR_REALIZED", "PNL_SNAPSHOT",
  "RISK_EVALUATION", "RISK_VETO", "RISK_ACCEPT",
  "CONSENSUS_DECISION", "SEVERE_DISAGREEMENT",
  "ASTRA_MODEL_INVOCATION", "ASTRA_DECISION", "ASTRA_DEEP_REVIEW", "ASTRA_LEARNING_UPDATE",
  "ASTRA_FAILURE", "ASTRA_BUDGET_BLOCK", "BOOK_REVALIDATION_REJECTED", "ASTRA_COUNTERFACTUAL",
  "MARKET_RESOLUTION_OBSERVED", "HEDGE_COMPLETE", "FAILED_HEDGE", "FIRST_LEG_FILLED",
  "STRATEGY_CHANGE_PROPOSED",
] as const;

export const MONITOR_CONTROL_EVENT_TYPES = [
  "TRIGGER_EVENT", "TRIGGER_LEVEL_1", "TRIGGER_LEVEL_2", "TRIGGER_LEVEL_3",
  "GROK_RESEARCH_AVAILABLE", "GROK_ACTIVATION_DECISION", "GROK_ACTIVATION_RESULT", "GROK_SIGNAL_ACCEPTED",
  "MCP_INTERACTION", "SYSTEM_HEALTH_EVENT", "DISCOVERY_RECONCILED",
  "REFERENCE_FEED_STARTED", "REFERENCE_FEED_STOPPED", "REFERENCE_UNSUPPORTED",
  "REFERENCE_PROBE_TRANSIENT", "REFERENCE_PROVIDER_BLOCKED",
] as const;

const AUDIT_SET = new Set<string>(AUDIT_CRITICAL_EVENT_TYPES);
const MONITOR_SET = new Set<string>(MONITOR_CONTROL_EVENT_TYPES);
const TELEMETRY_SET = new Set<string>(TELEMETRY_EVENT_TYPES);

export function classifyJournalRetention(type: string): RetentionClass {
  if (TELEMETRY_SET.has(type)) return "raw_telemetry";
  if (AUDIT_SET.has(type)) return "long_lived_audit";
  if (MONITOR_SET.has(type)) return "monitor_control";
  return "unknown_retain";
}

export function isProtectedJournalType(type: string): boolean {
  const cls = classifyJournalRetention(type);
  return cls === "long_lived_audit" || cls === "unknown_retain";
}

/**
 * Canonical experiment-source types. These enter the complete-report hash /
 * sourceEventCount and must have fail-closed or sticky integrity semantics.
 * Intentionally narrower than DECISION_EVENT_TYPES (dashboard / SSE list).
 */
export const CANONICAL_REPORT_INPUT_EVENT_TYPES = [
  "TRIGGER_EVENT", "TRIGGER_LEVEL_1", "TRIGGER_LEVEL_2", "TRIGGER_LEVEL_3",
  "GROK_RESEARCH_AVAILABLE", "GROK_ACTIVATION_DECISION", "GROK_ACTIVATION_RESULT", "GROK_SIGNAL_ACCEPTED",
  "CONSENSUS_DECISION", "SEVERE_DISAGREEMENT",
  "ASTRA_MODEL_INVOCATION", "ASTRA_DECISION", "ASTRA_DEEP_REVIEW", "ASTRA_LEARNING_UPDATE",
  "ASTRA_FAILURE", "ASTRA_BUDGET_BLOCK", "BOOK_REVALIDATION_REJECTED", "ASTRA_COUNTERFACTUAL",
  "RISK_EVALUATION", "RISK_VETO", "RISK_ACCEPT",
  "PAPER_ORDER_CREATED", "PAPER_FILL", "PAPER_EXECUTION_ERROR", "PAPER_MAKER_RESERVED", "PAPER_MAKER_RELEASED", "PAPER_ORDER_CANCELLED",
  "POSITION_UPDATED", "POSITION_SETTLED", "POSITION_WIN", "POSITION_LOSS", "POSITION_BREAKEVEN", "PAIR_REALIZED", "PNL_SNAPSHOT",
  "MARKET_RESOLUTION_OBSERVED", "HEDGE_COMPLETE", "FAILED_HEDGE", "FIRST_LEG_FILLED",
  "EXPERIMENT_STARTED", "EXPERIMENT_ENTRY_CLOSED", "EXPERIMENT_COMPLETED", "EXPERIMENT_PREFLIGHT",
  "MCP_INTERACTION", "STRATEGY_CHANGE_PROPOSED",
] as const;

/** Dashboard / monitor / control-plane types. Not part of the canonical source hash. */
export const OPERATIONAL_MONITOR_EVENT_TYPES = [
  "ENGINE_STARTED", "ENGINE_DRAINING", "ENGINE_STOPPED", "STATE_RECOVERED",
  "DISCOVERY_RECONCILED",
  "EXPERIMENT_START_ABORTED", "EXPERIMENT_START_VALIDATED",
  "REFERENCE_FEED_STARTED", "REFERENCE_FEED_STOPPED", "REFERENCE_UNSUPPORTED",
  "REFERENCE_PROBE_TRANSIENT", "REFERENCE_PROVIDER_BLOCKED",
  "SYSTEM_HEALTH_EVENT",
  "EXPERIMENT_REPORT_GENERATED", "EXPERIMENT_SOURCE_SEALED",
] as const;

export type ReportInputClass = "canonical" | "operational";

export function classifyReportInput(type: string): ReportInputClass | null {
  if ((CANONICAL_REPORT_INPUT_EVENT_TYPES as readonly string[]).includes(type)) return "canonical";
  if ((OPERATIONAL_MONITOR_EVENT_TYPES as readonly string[]).includes(type)) return "operational";
  return null;
}

/** @deprecated Use CANONICAL_REPORT_INPUT_EVENT_TYPES. Alias kept for seal/writer/admission. */
export const REPORT_INPUT_EVENT_TYPES = CANONICAL_REPORT_INPUT_EVENT_TYPES;

export function isReportInputEventType(type: string): boolean {
  return (CANONICAL_REPORT_INPUT_EVENT_TYPES as readonly string[]).includes(type);
}

export function isOperationalMonitorEventType(type: string): boolean {
  return (OPERATIONAL_MONITOR_EVENT_TYPES as readonly string[]).includes(type);
}

export function assertMcpIdempotencyTtlSafe(ttlMs: number): void {
  if (ttlMs < MCP_IDEMPOTENCY_MIN_TTL_MS) {
    throw new Error(`MCP_IDEMPOTENCY_TTL_TOO_SHORT: ${ttlMs} < ${MCP_IDEMPOTENCY_MIN_TTL_MS}`);
  }
}

export function clampRetentionBatchSize(size: number): number {
  return Math.max(1, Math.min(Math.floor(size), RETENTION_BATCH_SIZE_MAX));
}

if (RETENTION_DEFAULTS.mcpIdempotencyTtlMs < MCP_IDEMPOTENCY_MIN_TTL_MS) {
  throw new Error("RETENTION_DEFAULTS.mcpIdempotencyTtlMs below proven acceptance horizon");
}

function assertReportInputInventory(): void {
  const classified = new Set<string>();
  for (const type of CANONICAL_REPORT_INPUT_EVENT_TYPES) {
    if (classified.has(type)) throw new Error(`duplicate canonical report-input type ${type}`);
    classified.add(type);
  }
  for (const type of OPERATIONAL_MONITOR_EVENT_TYPES) {
    if (classified.has(type)) throw new Error(`report-input type in both classes: ${type}`);
    classified.add(type);
  }
  for (const type of DECISION_EVENT_TYPES) {
    if (classifyReportInput(type) == null) {
      throw new Error(`DECISION type missing canonical/operational class: ${type}`);
    }
  }
}
assertReportInputInventory();

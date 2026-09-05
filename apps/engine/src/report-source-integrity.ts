import { isReportInputEventType, isTelemetryEventType, type JournalEvent } from "@pm/core";
import { createJournalEvent, type EventJournal } from "@pm/db";
import { isSourceSealExemptType } from "./source-seal.js";

export const REPORT_SOURCE_INTEGRITY_FAULT = "REPORT_SOURCE_INTEGRITY_FAULT";
export const REPORT_SOURCE_INTEGRITY_REPAIRED = "REPORT_SOURCE_INTEGRITY_REPAIRED";

/** Accepted experiment facts whose durable loss must refuse a complete final. */
const LATCH_TYPES = new Set([
  "TRIGGER_EVENT", "TRIGGER_LEVEL_1", "TRIGGER_LEVEL_2", "TRIGGER_LEVEL_3",
  "GROK_RESEARCH_AVAILABLE", "GROK_ACTIVATION_DECISION", "GROK_ACTIVATION_RESULT", "GROK_SIGNAL_ACCEPTED",
  "CONSENSUS_DECISION", "SEVERE_DISAGREEMENT",
  "RISK_EVALUATION", "RISK_VETO", "RISK_ACCEPT",
  "PAPER_ORDER_CREATED", "PAPER_FILL", "PAPER_EXECUTION_ERROR", "PAPER_MAKER_RESERVED", "PAPER_MAKER_RELEASED", "PAPER_ORDER_CANCELLED",
  "POSITION_UPDATED", "POSITION_SETTLED", "POSITION_WIN", "POSITION_LOSS", "POSITION_BREAKEVEN", "PAIR_REALIZED", "PNL_SNAPSHOT",
  "MARKET_RESOLUTION_OBSERVED", "HEDGE_COMPLETE", "FAILED_HEDGE", "FIRST_LEG_FILLED",
  "STRATEGY_CHANGE_PROPOSED", "MCP_INTERACTION",
]);

export interface ReportSourceIntegrityFault {
  experimentId: string;
  eventType: string;
  eventId: string;
  reason: string;
  at: string;
}

export function shouldLatchReportSourceIntegrity(event: { type: string }, experiment?: { id?: string; status?: string } | null): boolean {
  if (!experiment?.id) return false;
  if (experiment.status === "IDLE" || experiment.status === "MONITOR_ONLY") return false;
  if (!isReportInputEventType(event.type)) return false;
  if (isTelemetryEventType(event.type)) return false;
  if (isSourceSealExemptType(event.type)) return false;
  return LATCH_TYPES.has(event.type);
}

export function integrityFaultEventId(experimentId: string, eventId: string): string {
  return `rsrcfault:${experimentId}:${eventId}`;
}

export function integrityRepairEventId(experimentId: string, eventId: string): string {
  return `rsrcrepair:${experimentId}:${eventId}`;
}

export function createIntegrityFaultEvent(fault: ReportSourceIntegrityFault): JournalEvent {
  return createJournalEvent({
    id: integrityFaultEventId(fault.experimentId, fault.eventId),
    traceId: fault.experimentId,
    type: REPORT_SOURCE_INTEGRITY_FAULT,
    aggregateType: "experiment",
    aggregateId: fault.experimentId,
    payload: { ...fault, unresolved: true },
  });
}

export function createIntegrityRepairEvent(fault: ReportSourceIntegrityFault): JournalEvent {
  return createJournalEvent({
    id: integrityRepairEventId(fault.experimentId, fault.eventId),
    traceId: fault.experimentId,
    type: REPORT_SOURCE_INTEGRITY_REPAIRED,
    aggregateType: "experiment",
    aggregateId: fault.experimentId,
    payload: { ...fault, unresolved: false, repaired: true },
  });
}

const INTEGRITY_PAGE = 256;
const INTEGRITY_SCAN_CEILING = 4_096;

export async function pageJournalEventsByTypes(
  journal: EventJournal,
  types: readonly string[],
  experimentId?: string,
  ceiling = INTEGRITY_SCAN_CEILING,
): Promise<{ events: JournalEvent[]; overflow: boolean }> {
  const events: JournalEvent[] = [];
  let after: { occurredAt: string; id: string } | undefined;
  for (;;) {
    const page = await journal.queryEventsKeyset({
      types: [...types],
      experimentId,
      limit: INTEGRITY_PAGE,
      after,
    });
    events.push(...page);
    if (events.length > ceiling) return { events: events.slice(0, ceiling), overflow: true };
    if (page.length < INTEGRITY_PAGE) return { events, overflow: false };
    const last = page[page.length - 1];
    if (!last) return { events, overflow: false };
    after = { occurredAt: last.occurredAt, id: last.id };
  }
}

export async function loadIntegrityRepairIds(
  journal: EventJournal,
  experimentId: string,
): Promise<{ ids: Set<string>; overflow: boolean }> {
  if (!experimentId) return { ids: new Set(), overflow: false };
  const { events, overflow } = await pageJournalEventsByTypes(journal, [REPORT_SOURCE_INTEGRITY_REPAIRED], experimentId);
  return {
    ids: new Set(events.map((event) => String((event.payload as { eventId?: unknown }).eventId ?? "")).filter(Boolean)),
    overflow,
  };
}

export async function loadUnresolvedReportSourceIntegrity(
  journal: EventJournal,
  experimentId: string,
): Promise<ReportSourceIntegrityFault[]> {
  if (!experimentId) return [];
  const faultsPage = await pageJournalEventsByTypes(journal, [REPORT_SOURCE_INTEGRITY_FAULT], experimentId);
  const repairs = await loadIntegrityRepairIds(journal, experimentId);
  if (faultsPage.overflow || repairs.overflow) {
    return [{
      experimentId,
      eventType: "OVERFLOW",
      eventId: "integrity-scan-overflow",
      reason: "INTEGRITY_SCAN_OVERFLOW",
      at: new Date().toISOString(),
    }];
  }
  return faultsPage.events.flatMap((event) => {
    const payload = event.payload as Partial<ReportSourceIntegrityFault>;
    const eventId = String(payload.eventId ?? "");
    if (!eventId || repairs.ids.has(eventId)) return [];
    return [{
      experimentId,
      eventType: String(payload.eventType ?? event.type),
      eventId,
      reason: String(payload.reason ?? "REPORT_INPUT_PERSIST_FAILED"),
      at: payload.at ?? event.occurredAt,
    }];
  });
}

export function parseCheckpointIntegrityFaults(payload: Record<string, unknown>): ReportSourceIntegrityFault[] {
  const raw = payload.reportSourceIntegrity;
  if (!Array.isArray(raw)) return [];
  return raw.flatMap((item) => {
    if (!item || typeof item !== "object") return [];
    const row = item as Record<string, unknown>;
    if (typeof row.experimentId !== "string" || typeof row.eventId !== "string") return [];
    return [{
      experimentId: row.experimentId,
      eventType: typeof row.eventType === "string" ? row.eventType : "UNKNOWN",
      eventId: row.eventId,
      reason: typeof row.reason === "string" ? row.reason : "REPORT_INPUT_PERSIST_FAILED",
      at: typeof row.at === "string" ? row.at : new Date().toISOString(),
    }];
  });
}

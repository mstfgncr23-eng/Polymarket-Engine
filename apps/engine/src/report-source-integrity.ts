import { isReportInputEventType, isTelemetryEventType, type JournalEvent } from "@pm/core";
import { createJournalEvent, type DurableTransition, type EventJournal } from "@pm/db";
import { isSourceSealExemptType } from "./source-seal.js";

export const REPORT_SOURCE_INTEGRITY_FAULT = "REPORT_SOURCE_INTEGRITY_FAULT";
export const REPORT_SOURCE_INTEGRITY_REPAIRED = "REPORT_SOURCE_INTEGRITY_REPAIRED";

/** Hard cap on retained per-experiment fault identities. One overflow sentinel is enough to refuse finality. */
export const REPORT_SOURCE_INTEGRITY_HARD_CAP = 64;
export const INTEGRITY_OVERFLOW_EVENT_ID = "integrity-overflow";

export interface ReportSourceIntegrityFault {
  experimentId: string;
  eventType: string;
  eventId: string;
  reason: string;
  at: string;
}

export function shouldLatchReportSourceIntegrity(
  event: { type: string; payload?: Record<string, unknown> },
  experiment?: { id?: string; status?: string } | null,
): boolean {
  if (!experiment?.id) return false;
  if (experiment.status === "IDLE") return false;
  if (!isReportInputEventType(event.type)) return false;
  if (isTelemetryEventType(event.type)) return false;
  if (isSourceSealExemptType(event.type)) return false;
  const windowOpen = experiment.status === "RUNNING" || experiment.status === "ENTRY_CLOSED";
  if (windowOpen) return true;
  const stamped = typeof event.payload?.experimentId === "string" ? event.payload.experimentId : undefined;
  return stamped === experiment.id;
}

export function integrityFaultEventId(experimentId: string, eventId: string): string {
  return `rsrcfault:${experimentId}:${eventId}`;
}

export function integrityRepairEventId(experimentId: string, eventId: string): string {
  return `rsrcrepair:${experimentId}:${eventId}`;
}

export function integrityDurableOperationKey(kind: "fault" | "repair", experimentId: string, eventId: string): string {
  return kind === "fault"
    ? `rsrcfault:${experimentId}:${eventId}`
    : `rsrcrepair:${experimentId}:${eventId}`;
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

export function overflowIntegrityFault(experimentId: string, at = new Date().toISOString()): ReportSourceIntegrityFault {
  return {
    experimentId,
    eventType: "OVERFLOW",
    eventId: INTEGRITY_OVERFLOW_EVENT_ID,
    reason: "REPORT_SOURCE_INTEGRITY_OVERFLOW",
    at,
  };
}

export function boundIntegrityFaults(faults: readonly ReportSourceIntegrityFault[], experimentId: string): ReportSourceIntegrityFault[] {
  const overflow = faults.find((fault) => fault.eventId === INTEGRITY_OVERFLOW_EVENT_ID || fault.reason.includes("OVERFLOW"));
  if (overflow || faults.length > REPORT_SOURCE_INTEGRITY_HARD_CAP) {
    return [overflow ?? overflowIntegrityFault(experimentId)];
  }
  return [...faults];
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

export async function pageDurableTransitionsByTypes(
  journal: EventJournal,
  types: readonly string[],
  ceiling = INTEGRITY_SCAN_CEILING,
): Promise<{ rows: DurableTransition[]; overflow: boolean }> {
  const rows: DurableTransition[] = [];
  let afterSequence = 0;
  for (;;) {
    const page = await journal.queryDurableTransitions({ types: [...types], afterSequence, limit: INTEGRITY_PAGE });
    rows.push(...page);
    if (rows.length > ceiling) return { rows: rows.slice(0, ceiling), overflow: true };
    if (page.length < INTEGRITY_PAGE) return { rows, overflow: false };
    const last = page[page.length - 1];
    if (!last) return { rows, overflow: false };
    afterSequence = last.sequence;
  }
}

function faultFromRecord(experimentId: string, payload: Record<string, unknown>, fallbackAt: string): ReportSourceIntegrityFault | null {
  const eventId = typeof payload.eventId === "string" ? payload.eventId : "";
  if (!eventId) return null;
  return {
    experimentId: typeof payload.experimentId === "string" ? payload.experimentId : experimentId,
    eventType: typeof payload.eventType === "string" ? payload.eventType : "UNKNOWN",
    eventId,
    reason: typeof payload.reason === "string" ? payload.reason : "REPORT_INPUT_PERSIST_FAILED",
    at: typeof payload.at === "string" ? payload.at : fallbackAt,
  };
}

export async function loadIntegrityRepairIds(
  journal: EventJournal,
  experimentId: string,
): Promise<{ ids: Set<string>; overflow: boolean }> {
  if (!experimentId) return { ids: new Set(), overflow: false };
  const journalRepairs = await pageJournalEventsByTypes(journal, [REPORT_SOURCE_INTEGRITY_REPAIRED], experimentId);
  const durable = await pageDurableTransitionsByTypes(journal, [REPORT_SOURCE_INTEGRITY_REPAIRED]);
  if (journalRepairs.overflow || durable.overflow) return { ids: new Set(), overflow: true };
  const ids = new Set(
    journalRepairs.events.map((event) => String((event.payload as { eventId?: unknown }).eventId ?? "")).filter(Boolean),
  );
  for (const row of durable.rows) {
    if (row.aggregateId !== experimentId && (row.payload as { experimentId?: unknown }).experimentId !== experimentId) continue;
    const eventId = String((row.payload as { eventId?: unknown }).eventId ?? "");
    if (eventId) ids.add(eventId);
  }
  return { ids, overflow: false };
}

export async function loadUnresolvedReportSourceIntegrity(
  journal: EventJournal,
  experimentId: string,
): Promise<ReportSourceIntegrityFault[]> {
  if (!experimentId) return [];
  const journalFaults = await pageJournalEventsByTypes(journal, [REPORT_SOURCE_INTEGRITY_FAULT], experimentId);
  const journalRepairs = await loadIntegrityRepairIds(journal, experimentId);
  const durable = await pageDurableTransitionsByTypes(journal, [REPORT_SOURCE_INTEGRITY_FAULT, REPORT_SOURCE_INTEGRITY_REPAIRED]);
  if (journalFaults.overflow || journalRepairs.overflow || durable.overflow) {
    return [overflowIntegrityFault(experimentId)];
  }
  const repaired = new Set(journalRepairs.ids);
  for (const row of durable.rows) {
    if (row.type !== REPORT_SOURCE_INTEGRITY_REPAIRED) continue;
    if (row.aggregateId !== experimentId && (row.payload as { experimentId?: unknown }).experimentId !== experimentId) continue;
    const eventId = String((row.payload as { eventId?: unknown }).eventId ?? "");
    if (eventId) repaired.add(eventId);
  }
  const byId = new Map<string, ReportSourceIntegrityFault>();
  for (const event of journalFaults.events) {
    const fault = faultFromRecord(experimentId, event.payload, event.occurredAt);
    if (fault && !repaired.has(fault.eventId)) byId.set(fault.eventId, fault);
  }
  for (const row of durable.rows) {
    if (row.type !== REPORT_SOURCE_INTEGRITY_FAULT) continue;
    if (row.aggregateId !== experimentId && (row.payload as { experimentId?: unknown }).experimentId !== experimentId) continue;
    const fault = faultFromRecord(experimentId, row.payload, row.occurredAt);
    if (fault && !repaired.has(fault.eventId)) byId.set(fault.eventId, fault);
  }
  return boundIntegrityFaults([...byId.values()], experimentId);
}

export function parseCheckpointIntegrityFaults(payload: Record<string, unknown>): ReportSourceIntegrityFault[] {
  const raw = payload.reportSourceIntegrity;
  if (!Array.isArray(raw)) return [];
  const experimentId = typeof (payload.experiment as { id?: unknown } | undefined)?.id === "string"
    ? (payload.experiment as { id: string }).id
    : "";
  const faults = raw.flatMap((item) => {
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
  return boundIntegrityFaults(faults, experimentId || faults[0]?.experimentId || "unknown");
}

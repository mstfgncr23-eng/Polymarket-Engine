import type { JournalEvent } from "@pm/core";
import { createJournalEvent, type EventJournal } from "@pm/db";

export const EXPERIMENT_SOURCE_SEALED = "EXPERIMENT_SOURCE_SEALED";
export const SOURCE_SEAL_TIMEOUT_MS = 5_000;

export function remainingSealMs(deadlineAt: number, now = Date.now()): number {
  return Math.max(0, deadlineAt - now);
}

export function sourceSealEventId(experimentId: string): string {
  return `sourceseal:${experimentId}`;
}

export function isSourceSealExemptType(type: string): boolean {
  return type === EXPERIMENT_SOURCE_SEALED
    || type === "EXPERIMENT_REPORT_GENERATED"
    || type === "EXPERIMENT_COMPLETED"
    || type === "EXPERIMENT_ENTRY_CLOSED"
    || type === "EXPERIMENT_STARTED"
    || type === "EXPERIMENT_PREFLIGHT";
}

export async function loadSourceSeal(journal: EventJournal, experimentId: string): Promise<JournalEvent | null> {
  if (!experimentId) return null;
  const rows = await journal.queryEventsKeyset({
    types: [EXPERIMENT_SOURCE_SEALED],
    experimentId,
    limit: 8,
  });
  return rows.find((event) => event.id === sourceSealEventId(experimentId)
    || event.aggregateId === experimentId
    || (event.payload as { experimentId?: unknown }).experimentId === experimentId) ?? null;
}

export function createSourceSealEvent(input: {
  experimentId: string;
  startedAt?: string | null;
  completedAt?: string | null;
  occurredAt?: Date;
  persistAttempt?: number;
}): JournalEvent {
  return createJournalEvent({
    id: sourceSealEventId(input.experimentId),
    traceId: input.experimentId,
    type: EXPERIMENT_SOURCE_SEALED,
    aggregateType: "experiment",
    aggregateId: input.experimentId,
    payload: {
      experimentId: input.experimentId,
      startedAt: input.startedAt ?? null,
      completedAt: input.completedAt ?? null,
      sealed: true,
      persistAttempt: input.persistAttempt ?? null,
    },
    occurredAt: input.occurredAt,
  });
}

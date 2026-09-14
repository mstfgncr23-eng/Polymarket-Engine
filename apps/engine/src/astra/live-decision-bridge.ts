import { AsyncLocalStorage } from "node:async_hooks";
import { ASTRA_BRAIN_AUTHORITY, type JournalEvent } from "@pm/core";
import type { EventJournal, JournalAppendOptions } from "@pm/db";

export interface LiveDecisionFeedItem {
  id: string;
  at: string;
  type: string;
  traceId: string;
  marketId: string | null;
  asset?: string | null;
  detail: Record<string, unknown>;
}

export interface LiveDecisionTriggerContext {
  id: string;
  traceId: string;
  marketId: string | null;
  asset?: string | null;
}

export interface AstraLiveDecisionSink {
  recentTriggers(): readonly LiveDecisionTriggerContext[];
  publish(item: LiveDecisionFeedItem): void;
  onError?(error: unknown, event: JournalEvent): void;
}

const LIVE_ASTRA_TYPES = new Set([
  "ASTRA_MODEL_INVOCATION",
  "ASTRA_DECISION",
  "ASTRA_DEEP_REVIEW",
  "ASTRA_FAILURE",
  "ASTRA_BUDGET_BLOCK",
  "BOOK_REVALIDATION_REJECTED",
]);

const PUBLISHED_ID_CAP = 1_024;
const DETAIL_SCALARS = [
  "authority",
  "stage",
  "accepted",
  "gateReason",
  "reason",
  "failureReason",
  "decisionSource",
  "bypass",
  "status",
  "model",
  "latencyMs",
  "triggerId",
  "uncertainty",
  "rationaleCode",
  "direction",
  "confidence",
  "action",
  "estimatedEdge",
] as const;

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function scalar(value: unknown): string | number | boolean | null | undefined {
  return typeof value === "string" || typeof value === "number" || typeof value === "boolean" || value === null
    ? value
    : undefined;
}

function stringValue(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function boundedStrings(value: unknown, limit = 8): string[] | undefined {
  if (!Array.isArray(value)) return undefined;
  const values = value.filter((item): item is string => typeof item === "string").slice(0, limit);
  return values.length > 0 ? values : undefined;
}

function eventAt(event: JournalEvent): string {
  const parsed = new Date(String(event.occurredAt));
  return Number.isFinite(parsed.getTime()) ? parsed.toISOString() : String(event.occurredAt);
}

/**
 * Convert one already-persisted canonical Astra fact into a bounded live UI
 * projection. This is observability only: it never creates authority and it
 * intentionally excludes feature packets, memory, prompts, token usage, and
 * other bulky/provider-specific payloads.
 */
export function projectCanonicalAstraDecisionEvent(
  event: JournalEvent,
  recentTriggers: readonly LiveDecisionTriggerContext[],
): LiveDecisionFeedItem | null {
  const payload = event.payload as Record<string, unknown>;
  const authority = stringValue(payload.authority);
  const astraConsensus = event.type === "CONSENSUS_DECISION" && authority === ASTRA_BRAIN_AUTHORITY;
  if (!LIVE_ASTRA_TYPES.has(event.type) && !astraConsensus) return null;
  if (payload.stage === "BATCH") return null;

  const triggerId = stringValue(payload.triggerId)
    ?? stringValue(record(payload.featurePacket)?.triggerId)
    ?? stringValue(record(payload.decision)?.triggerId);
  const trigger = recentTriggers.find((item) => item.traceId === event.traceId)
    ?? (triggerId ? recentTriggers.find((item) => item.id === triggerId) : undefined);
  const decision = record(payload.decision);

  const detail: Record<string, unknown> = {};
  for (const key of DETAIL_SCALARS) {
    const value = scalar(payload[key]);
    if (value !== undefined) detail[key] = value;
  }
  if (triggerId && detail.triggerId === undefined) detail.triggerId = triggerId;

  const nestedDirection = scalar(decision?.direction);
  const nestedConfidence = scalar(decision?.confidence);
  const nestedAction = scalar(decision?.action);
  const nestedUncertainty = scalar(decision?.uncertainty);
  const nestedRationale = scalar(decision?.rationaleCode);
  if (detail.direction === undefined && nestedDirection !== undefined) detail.direction = nestedDirection;
  if (detail.confidence === undefined && nestedConfidence !== undefined) detail.confidence = nestedConfidence;
  if (detail.action === undefined && nestedAction !== undefined) detail.action = nestedAction;
  if (detail.uncertainty === undefined && nestedUncertainty !== undefined) detail.uncertainty = nestedUncertainty;
  if (detail.rationaleCode === undefined && nestedRationale !== undefined) detail.rationaleCode = nestedRationale;

  if (astraConsensus) {
    const direction = scalar(payload.consensusDirection);
    const confidence = scalar(payload.consensusConfidence);
    const edge = scalar(payload.estimatedEdge);
    if (detail.direction === undefined && direction !== undefined) detail.direction = direction;
    if (detail.confidence === undefined && confidence !== undefined) detail.confidence = confidence;
    if (detail.estimatedEdge === undefined && edge !== undefined) detail.estimatedEdge = edge;
  }

  const stages = boundedStrings(payload.stages, 4);
  const reasonCodes = boundedStrings(decision?.reasonCodes, 8) ?? boundedStrings(payload.reasonCodes, 8);
  if (stages) detail.stages = stages;
  if (reasonCodes) detail.reasonCodes = reasonCodes;
  if (detail.reason === undefined) {
    const reason = scalar(payload.failureReason) ?? scalar(decision?.rationaleCode);
    if (reason !== undefined) detail.reason = reason;
  }

  return {
    id: event.id,
    at: eventAt(event),
    type: event.type,
    traceId: event.traceId,
    marketId: stringValue(payload.marketId) ?? trigger?.marketId ?? null,
    asset: stringValue(payload.asset) ?? trigger?.asset ?? null,
    detail,
  };
}

/**
 * Decorate one journal instance at the composition root. Projection happens
 * only after append/appendMany resolves successfully, so the live dashboard
 * can never outrun the canonical persistence boundary. The wrapper is
 * process-local, bounded, and does not query the database.
 */
export function installAstraLiveDecisionBridge(
  journal: EventJournal,
  sink: AstraLiveDecisionSink,
): () => void {
  const originalAppend = journal.append;
  const originalAppendMany = journal.appendMany;
  const batchContext = new AsyncLocalStorage<boolean>();
  const publishedIds = new Set<string>();

  const remember = (id: string) => {
    publishedIds.add(id);
    while (publishedIds.size > PUBLISHED_ID_CAP) {
      const oldest = publishedIds.values().next().value as string | undefined;
      if (!oldest) break;
      publishedIds.delete(oldest);
    }
  };

  const publish = (event: JournalEvent) => {
    if (publishedIds.has(event.id)) return;
    const item = projectCanonicalAstraDecisionEvent(event, sink.recentTriggers());
    if (!item) return;
    try {
      sink.publish(item);
      remember(event.id);
    } catch (error) {
      // Persistence has already succeeded. Observability must never turn a
      // committed fact into an economic failure or trigger a retry.
      sink.onError?.(error, event);
    }
  };

  journal.append = async (event: JournalEvent, options?: JournalAppendOptions): Promise<void> => {
    await originalAppend.call(journal, event, options);
    if (!batchContext.getStore()) publish(event);
  };

  journal.appendMany = async (events: readonly JournalEvent[]): Promise<void> => {
    await batchContext.run(true, () => originalAppendMany.call(journal, events));
    for (const event of events) publish(event);
  };

  return () => {
    journal.append = originalAppend;
    journal.appendMany = originalAppendMany;
  };
}

import type { EventJournal } from "@pm/db";
import { pageJournalEventsByTypes } from "./report-source-integrity.js";

export const CLEANUP_LIFECYCLE_STATES = [
  "PROVEN_NO_ACTIVE_EXPERIMENT",
  "ACTIVE_RUNNING",
  "ACTIVE_ENTRY_CLOSED",
  "COMPLETED",
  "FAILED",
  "IDLE",
  "UNKNOWN",
  "CONFLICTING",
] as const;

export type CleanupLifecycleState = (typeof CLEANUP_LIFECYCLE_STATES)[number];

export interface CleanupLifecycleHints {
  experimentId?: string | null;
  status?: string | null;
  startedAt?: string | null;
}

export interface CleanupLifecycle {
  state: CleanupLifecycleState;
  source: "durable_journal" | "hint_verified" | "unresolved";
  experimentId: string | null;
  status: string | null;
  startedAt: string | null;
  completedAt: string | null;
  monitorDeleteAllowed: boolean;
  reason: string;
  hintMismatch: boolean;
  /** Projection passed into retention. null only when PROVEN_NO_ACTIVE_EXPERIMENT. */
  experiment: { id: string; status: string; startedAt?: string | null } | null;
}

interface ExperimentRow {
  id: string;
  startedAt: string | null;
  entryClosedAt: string | null;
  completedAt: string | null;
}

function experimentIdFrom(event: { aggregateId: string; payload: Record<string, unknown> }): string | null {
  const fromPayload = event.payload.experimentId;
  if (typeof fromPayload === "string" && fromPayload) return fromPayload;
  const fromAggregate = event.payload.aggregateId;
  if (typeof fromAggregate === "string" && fromAggregate) return fromAggregate;
  return event.aggregateId || null;
}

async function loadExperimentRows(journal: EventJournal): Promise<ExperimentRow[] | null> {
  try {
    const startedPage = await pageJournalEventsByTypes(journal, ["EXPERIMENT_STARTED"]);
    const closedPage = await pageJournalEventsByTypes(journal, ["EXPERIMENT_ENTRY_CLOSED"]);
    const completedPage = await pageJournalEventsByTypes(journal, ["EXPERIMENT_COMPLETED"]);
    if (startedPage.overflow || closedPage.overflow || completedPage.overflow) return null;
    const started = startedPage.events;
    const closed = closedPage.events;
    const completed = completedPage.events;
    const byId = new Map<string, ExperimentRow>();
    for (const event of started) {
      const id = experimentIdFrom(event);
      if (!id) continue;
      const current = byId.get(id) ?? { id, startedAt: null, entryClosedAt: null, completedAt: null };
      current.startedAt = current.startedAt ?? event.occurredAt;
      byId.set(id, current);
    }
    for (const event of closed) {
      const id = experimentIdFrom(event);
      if (!id) continue;
      const current = byId.get(id) ?? { id, startedAt: null, entryClosedAt: null, completedAt: null };
      current.entryClosedAt = current.entryClosedAt ?? event.occurredAt;
      byId.set(id, current);
    }
    for (const event of completed) {
      const id = experimentIdFrom(event);
      if (!id) continue;
      const current = byId.get(id) ?? { id, startedAt: null, entryClosedAt: null, completedAt: null };
      current.completedAt = current.completedAt ?? event.occurredAt;
      byId.set(id, current);
    }
    return [...byId.values()];
  } catch {
    return null;
  }
}

function stateFor(row: ExperimentRow): Exclude<CleanupLifecycleState, "PROVEN_NO_ACTIVE_EXPERIMENT" | "UNKNOWN" | "CONFLICTING"> {
  if (row.completedAt) return "COMPLETED";
  if (row.entryClosedAt) return "ACTIVE_ENTRY_CLOSED";
  if (row.startedAt) return "ACTIVE_RUNNING";
  return "IDLE";
}

function monitorAllowed(state: CleanupLifecycleState, startedAt: string | null): boolean {
  if (state === "PROVEN_NO_ACTIVE_EXPERIMENT") return true;
  if (state === "IDLE") return true;
  if (state === "UNKNOWN" || state === "CONFLICTING") return false;
  if (state === "ACTIVE_RUNNING" || state === "ACTIVE_ENTRY_CLOSED") return Boolean(startedAt);
  if (state === "COMPLETED" || state === "FAILED") return false;
  return false;
}

function reasonFor(state: CleanupLifecycleState, startedAt: string | null): string {
  switch (state) {
    case "PROVEN_NO_ACTIVE_EXPERIMENT":
      return "No experiment lifecycle rows in durable journal; monitor TTL eligible.";
    case "ACTIVE_RUNNING":
      return startedAt
        ? "Authoritative RUNNING experiment; in-window monitor/report-input protected."
        : "RUNNING experiment missing startedAt; monitor delete forbidden.";
    case "ACTIVE_ENTRY_CLOSED":
      return startedAt
        ? "Authoritative ENTRY_CLOSED experiment; in-window monitor/report-input protected."
        : "ENTRY_CLOSED experiment missing startedAt; monitor delete forbidden.";
    case "COMPLETED":
      return "COMPLETED experiment: monitor delete requires canonical complete+non-truncated final plus grace.";
    case "FAILED":
      return "FAILED experiment: monitor delete forbidden until an operator-proven no-active state.";
    case "IDLE":
      return "IDLE experiment identity with no start; monitor TTL eligible.";
    case "CONFLICTING":
      return "Conflicting experiment identities in durable journal; monitor delete forbidden.";
    default:
      return "Experiment lifecycle could not be proven; UNKNOWN is never treated as no-active. Monitor delete forbidden.";
  }
}

export async function resolveCleanupLifecycle(
  journal: EventJournal,
  hints: CleanupLifecycleHints = {},
): Promise<CleanupLifecycle> {
  const rows = await loadExperimentRows(journal);
  if (rows == null) {
    return {
      state: "UNKNOWN",
      source: "unresolved",
      experimentId: hints.experimentId ?? null,
      status: "UNKNOWN",
      startedAt: null,
      completedAt: null,
      monitorDeleteAllowed: false,
      reason: reasonFor("UNKNOWN", null),
      hintMismatch: false,
      experiment: { id: hints.experimentId || "unknown", status: "UNKNOWN", startedAt: null },
    };
  }

  const active = rows.filter((row) => !row.completedAt && row.startedAt);
  const hintId = hints.experimentId?.trim() || null;

  let chosen: ExperimentRow | null = null;
  let state: CleanupLifecycleState;
  let hintMismatch = false;

  if (active.length > 1) {
    state = "CONFLICTING";
    chosen = hintId ? active.find((row) => row.id === hintId) ?? null : null;
  } else if (active.length === 1) {
    chosen = active[0]!;
    state = stateFor(chosen);
  } else if (rows.length === 0) {
    state = "PROVEN_NO_ACTIVE_EXPERIMENT";
    chosen = null;
  } else {
    chosen = rows.reduce((latest, row) => {
      const t = Date.parse(row.completedAt ?? row.entryClosedAt ?? row.startedAt ?? "") || 0;
      const lt = Date.parse(latest.completedAt ?? latest.entryClosedAt ?? latest.startedAt ?? "") || 0;
      return t >= lt ? row : latest;
    });
    state = stateFor(chosen);
  }

  if (hintId) {
    const hinted = rows.find((row) => row.id === hintId) ?? null;
    if (!hinted && state === "PROVEN_NO_ACTIVE_EXPERIMENT") {
      hintMismatch = true;
      state = "UNKNOWN";
    } else if (hinted && chosen && hinted.id !== chosen.id && (state === "ACTIVE_RUNNING" || state === "ACTIVE_ENTRY_CLOSED")) {
      hintMismatch = true;
      state = "CONFLICTING";
    } else if (hinted) {
      chosen = hinted;
      if (state !== "CONFLICTING" && state !== "UNKNOWN") state = stateFor(hinted);
    } else if (chosen && chosen.id !== hintId) {
      hintMismatch = true;
      state = "CONFLICTING";
    }
    if (hints.status && hints.status !== "UNKNOWN" && chosen) {
      const actual = stateFor(chosen) === "ACTIVE_RUNNING" ? "RUNNING"
        : stateFor(chosen) === "ACTIVE_ENTRY_CLOSED" ? "ENTRY_CLOSED"
          : stateFor(chosen);
      if (hints.status !== actual) {
        hintMismatch = true;
        state = "CONFLICTING";
      }
    }
    if (hints.startedAt && chosen?.startedAt && hints.startedAt !== chosen.startedAt) {
      hintMismatch = true;
      state = "CONFLICTING";
    }
  }

  if (hintMismatch && state !== "CONFLICTING") state = "UNKNOWN";

  const status = state === "ACTIVE_RUNNING" ? "RUNNING"
    : state === "ACTIVE_ENTRY_CLOSED" ? "ENTRY_CLOSED"
      : state === "PROVEN_NO_ACTIVE_EXPERIMENT" ? null
        : state;
  const startedAt = chosen?.startedAt ?? null;
  const completedAt = chosen?.completedAt ?? null;
  const monitorDeleteAllowed = !hintMismatch && monitorAllowed(state, startedAt);
  const experiment = state === "PROVEN_NO_ACTIVE_EXPERIMENT"
    ? null
    : {
      id: chosen?.id ?? hintId ?? "unknown",
      status: status ?? "UNKNOWN",
      startedAt,
    };

  return {
    state,
    source: rows === null ? "unresolved" : hintId && !hintMismatch ? "hint_verified" : "durable_journal",
    experimentId: chosen?.id ?? (state === "PROVEN_NO_ACTIVE_EXPERIMENT" ? null : hintId),
    status,
    startedAt,
    completedAt,
    monitorDeleteAllowed,
    reason: hintMismatch
      ? "Cleanup hint does not match authoritative durable experiment lifecycle; fail closed."
      : reasonFor(state, startedAt),
    hintMismatch,
    experiment,
  };
}

export function lifecycleForbidsMonitorDelete(lifecycle: CleanupLifecycle): boolean {
  return !lifecycle.monitorDeleteAllowed || lifecycle.hintMismatch || lifecycle.state === "UNKNOWN" || lifecycle.state === "CONFLICTING";
}

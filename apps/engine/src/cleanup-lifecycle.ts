import type { EventJournal } from "@pm/db";
import { pageDurableTransitionsByTypes, pageJournalEventsByTypes } from "./report-source-integrity.js";

export const CLEANUP_LIFECYCLE_STATES = [
  "PROVEN_NO_ACTIVE_EXPERIMENT",
  "ACTIVE_RUNNING",
  "ACTIVE_ENTRY_CLOSED",
  "COMPLETED",
  "FAILED",
  "ABORTED_START",
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
  source: "durable_transitions" | "verified_snapshot" | "hint_verified" | "unresolved" | "legacy_journal";
  experimentId: string | null;
  status: string | null;
  startedAt: string | null;
  completedAt: string | null;
  monitorDeleteAllowed: boolean;
  reason: string;
  hintMismatch: boolean;
  journalCrossCheck: "match" | "missing_audit" | "legacy_only" | "conflict" | "none";
  /** Projection passed into retention. null only when PROVEN_NO_ACTIVE_EXPERIMENT. */
  experiment: { id: string; status: string; startedAt?: string | null } | null;
}

interface ExperimentRow {
  id: string;
  startedAt: string | null;
  entryClosedAt: string | null;
  completedAt: string | null;
  abortedAt: string | null;
  validatedAt: string | null;
}

const LIFECYCLE_TYPES = ["EXPERIMENT_STARTED", "EXPERIMENT_START_VALIDATED", "EXPERIMENT_START_ABORTED", "EXPERIMENT_ENTRY_CLOSED", "EXPERIMENT_COMPLETED"] as const;

function experimentIdFromPayload(payload: Record<string, unknown>, aggregateId: string): string | null {
  const nested = payload.experiment;
  if (nested && typeof nested === "object" && typeof (nested as { id?: unknown }).id === "string") {
    return (nested as { id: string }).id;
  }
  if (typeof payload.experimentId === "string" && payload.experimentId) return payload.experimentId;
  if (typeof payload.aggregateId === "string" && payload.aggregateId) return payload.aggregateId;
  return aggregateId || null;
}

function startedAtFrom(payload: Record<string, unknown>, occurredAt: string): string | null {
  const nested = payload.experiment;
  if (nested && typeof nested === "object" && typeof (nested as { startedAt?: unknown }).startedAt === "string") {
    return (nested as { startedAt: string }).startedAt;
  }
  return occurredAt;
}

function applyLifecycleEvent(byId: Map<string, ExperimentRow>, type: string, id: string, occurredAt: string, payload: Record<string, unknown>): void {
  const current = byId.get(id) ?? { id, startedAt: null, entryClosedAt: null, completedAt: null, abortedAt: null, validatedAt: null };
  if (type === "EXPERIMENT_STARTED") current.startedAt = current.startedAt ?? startedAtFrom(payload, occurredAt);
  if (type === "EXPERIMENT_START_VALIDATED") current.validatedAt = current.validatedAt ?? occurredAt;
  if (type === "EXPERIMENT_ENTRY_CLOSED") current.entryClosedAt = current.entryClosedAt ?? occurredAt;
  if (type === "EXPERIMENT_COMPLETED") current.completedAt = current.completedAt ?? occurredAt;
  if (type === "EXPERIMENT_START_ABORTED") current.abortedAt = current.abortedAt ?? occurredAt;
  byId.set(id, current);
}

async function loadDurableExperimentRows(journal: EventJournal): Promise<{ rows: ExperimentRow[]; source: "durable_transitions" | "verified_snapshot" } | null> {
  try {
    // Once the historical lifecycle prefix is compacted, the latest verified
    // recovery snapshot plus its durable tail remains authoritative.
    if (journal.listVerifiedCheckpoints) {
      const checkpoints = await journal.listVerifiedCheckpoints();
      const checkpoint = checkpoints[0];
      if (checkpoint) {
        const payload = checkpoint.payload;
        const raw = payload.experiment;
        const experiment = raw && typeof raw === "object" ? raw as Record<string, unknown> : null;
        const id = typeof experiment?.id === "string" ? experiment.id : checkpoint.experimentId;
        const byId = new Map<string, ExperimentRow>();
        if (id) {
          const status = typeof experiment?.status === "string" ? experiment.status : "";
          const startedAt = typeof experiment?.startedAt === "string" ? experiment.startedAt : null;
          const entryClosedAt = status === "ENTRY_CLOSED" && typeof experiment?.entryClosedAt === "string" ? experiment.entryClosedAt : null;
          const completedAt = status === "COMPLETED" && typeof experiment?.completedAt === "string" ? experiment.completedAt : null;
          const abortedAt = status === "ABORTED_START" && typeof experiment?.abortedAt === "string" ? experiment.abortedAt : null;
          const validatedAt = status !== "ABORTED_START" && status !== "IDLE" ? (typeof experiment?.startValidatedAt === "string" ? experiment.startValidatedAt : startedAt) : null;
          byId.set(id, { id, startedAt, entryClosedAt, completedAt, abortedAt, validatedAt });
        }
        const cursorValue = payload.durableCursor;
        const cursor = typeof cursorValue === "number" && Number.isInteger(cursorValue) ? cursorValue : checkpoint.sequence;
        const highWater = await journal.latestDurableSequence();
        const safetyCeiling = 100_000;
        let after = cursor;
        let pages = 0;
        while (after < highWater) {
          if (++pages > safetyCeiling) return null;
          const tail = await journal.readDurableTransitionsAfter(after, 1_000);
          if (tail.length === 0) return null;
          for (const row of tail) {
            const rowId = experimentIdFromPayload(row.payload, row.aggregateId);
            if (rowId) applyLifecycleEvent(byId, row.type, rowId, row.occurredAt, row.payload);
            after = Math.max(after, row.sequence);
          }
        }
        return { rows: [...byId.values()], source: "verified_snapshot" };
      }
    }
    const page = await pageDurableTransitionsByTypes(journal, LIFECYCLE_TYPES);
    if (page.overflow) return null;
    const byId = new Map<string, ExperimentRow>();
    for (const row of page.rows) {
      const id = experimentIdFromPayload(row.payload, row.aggregateId);
      if (!id) continue;
      applyLifecycleEvent(byId, row.type, id, row.occurredAt, row.payload);
    }
    return { rows: [...byId.values()], source: "durable_transitions" };
  } catch {
    return null;
  }
}

async function loadJournalExperimentRows(journal: EventJournal): Promise<ExperimentRow[] | null> {
  try {
    const startedPage = await pageJournalEventsByTypes(journal, ["EXPERIMENT_STARTED", "EXPERIMENT_START_VALIDATED", "EXPERIMENT_START_ABORTED"]);
    const closedPage = await pageJournalEventsByTypes(journal, ["EXPERIMENT_ENTRY_CLOSED"]);
    const completedPage = await pageJournalEventsByTypes(journal, ["EXPERIMENT_COMPLETED"]);
    if (startedPage.overflow || closedPage.overflow || completedPage.overflow) return null;
    const byId = new Map<string, ExperimentRow>();
    for (const event of [...startedPage.events, ...closedPage.events, ...completedPage.events]) {
      const id = experimentIdFromPayload(event.payload, event.aggregateId);
      if (!id) continue;
      applyLifecycleEvent(byId, event.type, id, event.occurredAt, event.payload);
    }
    return [...byId.values()];
  } catch {
    return null;
  }
}

function stateFor(row: ExperimentRow): Exclude<CleanupLifecycleState, "PROVEN_NO_ACTIVE_EXPERIMENT" | "UNKNOWN" | "CONFLICTING"> {
  if (row.abortedAt) return "ABORTED_START";
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
  if (state === "COMPLETED" || state === "FAILED" || state === "ABORTED_START") return true;
  return false;
}

function reasonFor(state: CleanupLifecycleState, startedAt: string | null, source: CleanupLifecycle["source"]): string {
  switch (state) {
    case "PROVEN_NO_ACTIVE_EXPERIMENT":
      return "No experiment lifecycle in durable_transitions and no legacy journal rows; monitor TTL eligible.";
    case "ACTIVE_RUNNING":
      return startedAt
        ? "Authoritative RUNNING experiment from durable_transitions; in-window monitor/report-input protected."
        : "RUNNING experiment missing startedAt; monitor delete forbidden.";
    case "ACTIVE_ENTRY_CLOSED":
      return startedAt
        ? "Authoritative ENTRY_CLOSED experiment from durable_transitions; in-window monitor/report-input protected."
        : "ENTRY_CLOSED experiment missing startedAt; monitor delete forbidden.";
    case "COMPLETED":
      return "COMPLETED experiment from durable_transitions: monitor delete requires canonical complete+non-truncated final plus grace.";
    case "FAILED":
      return "FAILED experiment: monitor delete forbidden until an operator-proven no-active state.";
    case "ABORTED_START":
      return "Feed-invalidated experiment start is durably terminal; no active experiment identity remains.";
    case "IDLE":
      return "IDLE experiment identity with no start; monitor TTL eligible.";
    case "CONFLICTING":
      return "Conflicting experiment identities in durable_transitions; monitor delete forbidden.";
    default:
      return source === "legacy_journal"
        ? "Legacy journal lifecycle rows exist without durable_transitions authority; UNKNOWN is never no-active. Monitor delete forbidden."
        : "Experiment lifecycle could not be proven from durable_transitions; UNKNOWN is never treated as no-active. Monitor delete forbidden.";
  }
}

function chooseRow(rows: ExperimentRow[], hintId: string | null): { chosen: ExperimentRow | null; state: CleanupLifecycleState } {
  const active = rows.filter((row) => !row.completedAt && !row.abortedAt && row.startedAt);
  if (active.length > 1) {
    return { state: "CONFLICTING", chosen: hintId ? active.find((row) => row.id === hintId) ?? null : null };
  }
  if (active.length === 1) return { chosen: active[0]!, state: stateFor(active[0]!) };
  if (rows.length === 0) return { chosen: null, state: "PROVEN_NO_ACTIVE_EXPERIMENT" };
  const chosen = rows.reduce((latest, row) => {
    const t = Date.parse(row.completedAt ?? row.entryClosedAt ?? row.startedAt ?? "") || 0;
    const lt = Date.parse(latest.completedAt ?? latest.entryClosedAt ?? latest.startedAt ?? "") || 0;
    return t >= lt ? row : latest;
  });
  return { chosen, state: stateFor(chosen) };
}

function applyHints(
  rows: ExperimentRow[],
  chosen: ExperimentRow | null,
  state: CleanupLifecycleState,
  hints: CleanupLifecycleHints,
): { chosen: ExperimentRow | null; state: CleanupLifecycleState; hintMismatch: boolean } {
  const hintId = hints.experimentId?.trim() || null;
  let hintMismatch = false;
  if (!hintId) return { chosen, state, hintMismatch };
  const hinted = rows.find((row) => row.id === hintId) ?? null;
  if (!hinted && state === "PROVEN_NO_ACTIVE_EXPERIMENT") {
    return { chosen, state: "UNKNOWN", hintMismatch: true };
  }
  if (hinted && chosen && hinted.id !== chosen.id && (state === "ACTIVE_RUNNING" || state === "ACTIVE_ENTRY_CLOSED")) {
    return { chosen: hinted, state: "CONFLICTING", hintMismatch: true };
  }
  if (hinted) {
    chosen = hinted;
    if (state !== "CONFLICTING") state = stateFor(hinted);
  } else if (chosen && chosen.id !== hintId) {
    return { chosen, state: "CONFLICTING", hintMismatch: true };
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
  if (hintMismatch && state !== "CONFLICTING") state = "UNKNOWN";
  return { chosen, state, hintMismatch };
}

function journalCrossCheck(durable: ExperimentRow[], journal: ExperimentRow[] | null, chosen: ExperimentRow | null, state: CleanupLifecycleState): CleanupLifecycle["journalCrossCheck"] {
  if (state === "PROVEN_NO_ACTIVE_EXPERIMENT") return "none";
  if (!journal) return "none";
  if (durable.length === 0 && journal.length > 0) return "legacy_only";
  if (!chosen) return "none";
  const journalActive = journal.filter((row) => !row.completedAt && !row.abortedAt && row.startedAt && row.id !== chosen.id);
  if (journalActive.length > 0) return "conflict";
  const journalRow = journal.find((row) => row.id === chosen.id);
  if (!journalRow) return "missing_audit";
  return "match";
}

export async function resolveCleanupLifecycle(
  journal: EventJournal,
  hints: CleanupLifecycleHints = {},
): Promise<CleanupLifecycle> {
  const durableResult = await loadDurableExperimentRows(journal);
  const durableRows = durableResult?.rows ?? null;
  const journalRows = await loadJournalExperimentRows(journal);
  if (durableRows == null) {
    return {
      state: "UNKNOWN",
      source: "unresolved",
      experimentId: hints.experimentId ?? null,
      status: "UNKNOWN",
      startedAt: null,
      completedAt: null,
      monitorDeleteAllowed: false,
      reason: reasonFor("UNKNOWN", null, "unresolved"),
      hintMismatch: false,
      journalCrossCheck: "none",
      experiment: { id: hints.experimentId || "unknown", status: "UNKNOWN", startedAt: null },
    };
  }

  const hintId = hints.experimentId?.trim() || null;

  if (durableRows.length === 0) {
    if (journalRows == null) {
      return {
        state: "UNKNOWN",
        source: "unresolved",
        experimentId: hintId,
        status: "UNKNOWN",
        startedAt: null,
        completedAt: null,
        monitorDeleteAllowed: false,
        reason: "Legacy journal lifecycle could not be proven absent; UNKNOWN is never no-active. Monitor delete forbidden.",
        hintMismatch: false,
        journalCrossCheck: "none",
        experiment: { id: hintId || "unknown", status: "UNKNOWN", startedAt: null },
      };
    }
    if (journalRows.length > 0) {
      const hinted = applyHints(journalRows, null, "UNKNOWN", hints);
      return {
        state: "UNKNOWN",
        source: "legacy_journal",
        experimentId: hinted.chosen?.id ?? hintId,
        status: "UNKNOWN",
        startedAt: hinted.chosen?.startedAt ?? null,
        completedAt: hinted.chosen?.completedAt ?? null,
        monitorDeleteAllowed: false,
        reason: reasonFor("UNKNOWN", null, "legacy_journal"),
        hintMismatch: hinted.hintMismatch,
        journalCrossCheck: "legacy_only",
        experiment: { id: hinted.chosen?.id ?? hintId ?? "unknown", status: "UNKNOWN", startedAt: hinted.chosen?.startedAt ?? null },
      };
    }
    const hinted = applyHints([], null, "PROVEN_NO_ACTIVE_EXPERIMENT", hints);
    if (hinted.hintMismatch) {
      return {
        state: "UNKNOWN",
        source: "unresolved",
        experimentId: hintId,
        status: "UNKNOWN",
        startedAt: null,
        completedAt: null,
        monitorDeleteAllowed: false,
        reason: "Cleanup hint does not match authoritative durable experiment lifecycle; fail closed.",
        hintMismatch: true,
        journalCrossCheck: "none",
        experiment: { id: hintId || "unknown", status: "UNKNOWN", startedAt: null },
      };
    }
    return {
      state: "PROVEN_NO_ACTIVE_EXPERIMENT",
    source: "durable_transitions",
      experimentId: null,
      status: null,
      startedAt: null,
      completedAt: null,
      monitorDeleteAllowed: true,
      reason: reasonFor("PROVEN_NO_ACTIVE_EXPERIMENT", null, "durable_transitions"),
      hintMismatch: false,
      journalCrossCheck: "none",
      experiment: null,
    };
  }

  const selected = chooseRow(durableRows, hintId);
  const hinted = applyHints(durableRows, selected.chosen, selected.state, hints);
  let state = hinted.state;
  const chosen = hinted.chosen;
  if (hinted.hintMismatch && state !== "CONFLICTING") state = "UNKNOWN";
  const cross = journalCrossCheck(durableRows, journalRows, chosen, state);
  // Journal is diagnostic only. Extra journal identities must not override
  // recovery-authoritative durable_transitions (including to CONFLICTING).

  const status = state === "ACTIVE_RUNNING" ? "RUNNING"
    : state === "ACTIVE_ENTRY_CLOSED" ? "ENTRY_CLOSED"
      : state === "ABORTED_START" ? "ABORTED_START"
      : state === "PROVEN_NO_ACTIVE_EXPERIMENT" ? null
        : state;
  const startedAt = chosen?.startedAt ?? null;
  const completedAt = chosen?.completedAt ?? null;
  const monitorDeleteAllowed = !hinted.hintMismatch && monitorAllowed(state, startedAt);
  const experiment = state === "PROVEN_NO_ACTIVE_EXPERIMENT"
    ? null
    : {
      id: chosen?.id ?? hintId ?? "unknown",
      status: status ?? "UNKNOWN",
      startedAt,
    };

  return {
    state,
    source: hinted.hintMismatch ? "unresolved" : hintId && !hinted.hintMismatch ? "hint_verified" : (durableResult?.source ?? "durable_transitions"),
    experimentId: chosen?.id ?? (state === "PROVEN_NO_ACTIVE_EXPERIMENT" ? null : hintId),
    status,
    startedAt,
    completedAt,
    monitorDeleteAllowed,
    reason: hinted.hintMismatch
      ? "Cleanup hint does not match authoritative durable experiment lifecycle; fail closed."
      : reasonFor(state, startedAt, "durable_transitions"),
    hintMismatch: hinted.hintMismatch,
    journalCrossCheck: cross,
    experiment,
  };
}

export function lifecycleForbidsMonitorDelete(lifecycle: CleanupLifecycle): boolean {
  return !lifecycle.monitorDeleteAllowed || lifecycle.hintMismatch || lifecycle.state === "UNKNOWN" || lifecycle.state === "CONFLICTING";
}

export function seedDurableLifecycleInput(input: {
  id: string;
  startedAt?: string | null;
  entryClosedAt?: string | null;
  completedAt?: string | null;
}): Array<{ operationKey: string; type: string; aggregateId: string; payload: Record<string, unknown>; occurredAt: Date }> {
  const rows = [];
  if (input.startedAt) {
    rows.push({
      operationKey: `experiment:${input.id}:started`,
      type: "EXPERIMENT_STARTED",
      aggregateId: input.id,
      payload: { experimentId: input.id, experiment: { id: input.id, status: "RUNNING", startedAt: input.startedAt } },
      occurredAt: new Date(input.startedAt),
    });
  }
  if (input.entryClosedAt) {
    rows.push({
      operationKey: `experiment:${input.id}:entry_closed`,
      type: "EXPERIMENT_ENTRY_CLOSED",
      aggregateId: input.id,
      payload: { experimentId: input.id, experiment: { id: input.id, status: "ENTRY_CLOSED", startedAt: input.startedAt ?? input.entryClosedAt } },
      occurredAt: new Date(input.entryClosedAt),
    });
  }
  if (input.completedAt) {
    rows.push({
      operationKey: `experiment:${input.id}:completed`,
      type: "EXPERIMENT_COMPLETED",
      aggregateId: input.id,
      payload: { experimentId: input.id, experiment: { id: input.id, status: "COMPLETED", startedAt: input.startedAt ?? null, completedAt: input.completedAt } },
      occurredAt: new Date(input.completedAt),
    });
  }
  return rows;
}

export async function seedDurableLifecycle(
  journal: EventJournal,
  input: Parameters<typeof seedDurableLifecycleInput>[0],
): Promise<void> {
  let previous = await journal.latestDurableSequence();
  for (const row of seedDurableLifecycleInput(input)) {
    const committed = await journal.commitDurableTransition({
      operationKey: row.operationKey,
      type: row.type,
      aggregateType: "experiment",
      aggregateId: row.aggregateId,
      payload: row.payload,
      occurredAt: row.occurredAt,
    }, previous);
    previous = committed.sequence;
  }
}

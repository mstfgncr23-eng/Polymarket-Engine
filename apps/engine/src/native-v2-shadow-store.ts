import type {
  NativeDecision,
  NativeV2ShadowFairProbability,
  NativeV2ShadowFeatureVector,
  NativeV2ShadowOpportunityCandidate,
  NativeV2ShadowOpportunityRanking,
  Outcome,
} from "@pm/core";

export const NATIVE_V2_SHADOW_EXPORT_SCHEMA_VERSION = 1 as const;
export const NATIVE_V2_SHADOW_RECORD_LIMIT = 5_000;

export interface NativeV2ShadowExecutionRecord {
  sequence: number;
  recordedAt: string;
  experimentId: string | null;
  experimentStatus: string | null;
  gitSha: string | null;
  triggerId: string;
  traceId: string;
  marketId: string;
  asset: string | null;
  outcome: Outcome;
  v1: Pick<NativeDecision, "direction" | "confidence" | "fairProbability" | "marketProbability">;
  featureVector: NativeV2ShadowFeatureVector;
  fairProbability: NativeV2ShadowFairProbability;
  opportunity: {
    marketRank: number;
    rankedMarketCount: number;
    actionableMarketCount: number;
    selected: NativeV2ShadowOpportunityCandidate;
    globalTop: NativeV2ShadowOpportunityRanking["top"];
  };
  execution: Record<string, unknown>;
}

interface NativeV2ShadowSummary {
  observed: number;
  evaluated: number;
  wouldSubmit: number;
  vetoed: number;
  preflightBlocked: number;
  droppedBusy: number;
  retained: number;
  capacity: number;
  topRiskVetoes: Record<string, number>;
  topPreflightBlockers: Record<string, number>;
}

export class NativeV2ShadowStore {
  private readonly slots: Array<NativeV2ShadowExecutionRecord | undefined>;
  private cursor = 0;
  private sizeValue = 0;
  private sequence = 0;
  private observed = 0;
  private evaluated = 0;
  private wouldSubmit = 0;
  private vetoed = 0;
  private preflightBlocked = 0;
  private droppedBusy = 0;
  private readonly riskVetoes = new Map<string, number>();
  private readonly preflightBlockers = new Map<string, number>();

  constructor(readonly capacity = NATIVE_V2_SHADOW_RECORD_LIMIT) {
    if (!Number.isInteger(capacity) || capacity <= 0) throw new Error("INVALID_NATIVE_V2_SHADOW_CAPACITY");
    this.slots = new Array<NativeV2ShadowExecutionRecord | undefined>(capacity);
  }

  reset(): void {
    this.slots.fill(undefined);
    this.cursor = 0;
    this.sizeValue = 0;
    this.sequence = 0;
    this.observed = 0;
    this.evaluated = 0;
    this.wouldSubmit = 0;
    this.vetoed = 0;
    this.preflightBlocked = 0;
    this.droppedBusy = 0;
    this.riskVetoes.clear();
    this.preflightBlockers.clear();
  }

  noteObserved(): void {
    this.observed += 1;
  }

  noteDroppedBusy(): void {
    this.droppedBusy += 1;
  }

  append(record: Omit<NativeV2ShadowExecutionRecord, "sequence">): NativeV2ShadowExecutionRecord {
    const stored: NativeV2ShadowExecutionRecord = {
      ...structuredClone(record),
      sequence: ++this.sequence,
    };
    this.slots[this.cursor] = stored;
    this.cursor = (this.cursor + 1) % this.capacity;
    this.sizeValue = Math.min(this.sizeValue + 1, this.capacity);
    this.evaluated += 1;

    const blockers = stringArray(stored.execution.preflightBlockers);
    const vetoes = stringArray(stored.execution.riskVetoes);
    const wouldSubmit = stored.execution.wouldSubmitOrder === true;
    if (wouldSubmit) this.wouldSubmit += 1;
    else if (blockers.length > 0) this.preflightBlocked += 1;
    else this.vetoed += 1;
    for (const blocker of blockers) increment(this.preflightBlockers, blocker);
    for (const veto of vetoes) increment(this.riskVetoes, veto);
    return structuredClone(stored);
  }

  snapshot(): {
    schemaVersion: typeof NATIVE_V2_SHADOW_EXPORT_SCHEMA_VERSION;
    summary: NativeV2ShadowSummary;
    latest: NativeV2ShadowExecutionRecord | null;
  } {
    const records = this.orderedRecords();
    return {
      schemaVersion: NATIVE_V2_SHADOW_EXPORT_SCHEMA_VERSION,
      summary: this.summary(),
      latest: records.at(-1) ?? null,
    };
  }

  export(input: {
    generatedAt: string;
    experimentId: string | null;
    experimentStatus: string | null;
    gitSha: string | null;
  }): {
    schemaVersion: typeof NATIVE_V2_SHADOW_EXPORT_SCHEMA_VERSION;
    generatedAt: string;
    experimentId: string | null;
    experimentStatus: string | null;
    gitSha: string | null;
    summary: NativeV2ShadowSummary;
    evaluations: NativeV2ShadowExecutionRecord[];
  } {
    return {
      schemaVersion: NATIVE_V2_SHADOW_EXPORT_SCHEMA_VERSION,
      generatedAt: input.generatedAt,
      experimentId: input.experimentId,
      experimentStatus: input.experimentStatus,
      gitSha: input.gitSha,
      summary: this.summary(),
      evaluations: this.orderedRecords(),
    };
  }

  private summary(): NativeV2ShadowSummary {
    return {
      observed: this.observed,
      evaluated: this.evaluated,
      wouldSubmit: this.wouldSubmit,
      vetoed: this.vetoed,
      preflightBlocked: this.preflightBlocked,
      droppedBusy: this.droppedBusy,
      retained: this.sizeValue,
      capacity: this.capacity,
      topRiskVetoes: sortedCounts(this.riskVetoes),
      topPreflightBlockers: sortedCounts(this.preflightBlockers),
    };
  }

  private orderedRecords(): NativeV2ShadowExecutionRecord[] {
    if (this.sizeValue === 0) return [];
    const start = this.sizeValue < this.capacity ? 0 : this.cursor;
    const records: NativeV2ShadowExecutionRecord[] = [];
    for (let i = 0; i < this.sizeValue; i += 1) {
      const item = this.slots[(start + i) % this.capacity];
      if (item) records.push(structuredClone(item));
    }
    return records;
  }
}

function increment(target: Map<string, number>, key: string): void {
  target.set(key, (target.get(key) ?? 0) + 1);
}

function sortedCounts(source: ReadonlyMap<string, number>): Record<string, number> {
  return Object.fromEntries(
    [...source.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])),
  );
}

function stringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === "string") : [];
}

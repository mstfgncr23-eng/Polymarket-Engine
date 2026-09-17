import {
  appendFileSync,
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  renameSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";
import type { NativeDecision } from "./native-decision.js";
import type { NativeV2ShadowFairProbability } from "./native-v2-shadow-fair-probability.js";
import {
  NATIVE_V2_SHADOW_FEATURE_VERSION,
  type NativeV2ShadowFeatureVector,
} from "./native-v2-shadow-features.js";
import type { Outcome } from "./domain.js";
import { isAuthoritativeBinaryMapping, parsePublicBinaryResolution, type BinarySettlementLookup } from "./settlement.js";
import {
  LEARNING_SAMPLE_SCHEMA_VERSION,
  buildLearningSampleV1,
  learningSampleId,
  parseLearningCheckpointSeconds,
  selectCrossedCheckpoints,
  type LearningSampleV1,
  type LearningSettlementLabel,
} from "./learning-sample.js";

export const LEARNING_MANIFEST_VERSION = 1 as const;
/** Default max durable learning bytes — operational default, not a capacity requirement. */
export const DEFAULT_LEARNING_MAX_BYTES = 256_000_000;
export const DEFAULT_LEARNING_DATA_DIR = "/data/learning";
export const DEFAULT_LEARNING_SEGMENT_MAX_BYTES = 8_000_000;
/** Fraction of maxBytes reserved so settlement labels remain writable near capacity. */
export const DEFAULT_LEARNING_LABEL_RESERVE_RATIO = 0.05;
/** Floor reserve so tiny test caps still leave room for labels. */
export const DEFAULT_LEARNING_LABEL_RESERVE_FLOOR_BYTES = 4_096;

export interface LearningDatasetManifest {
  manifestVersion: typeof LEARNING_MANIFEST_VERSION;
  schemaVersion: typeof LEARNING_SAMPLE_SCHEMA_VERSION;
  featureSchemaVersion: string;
  createdAt: string;
  updatedAt: string;
  gitSha: string | null;
  sampleCount: number;
  labeledCount: number;
  unresolvedMarketCount: number;
  segmentCount: number;
  bytes: number;
  maxBytes: number;
  checkpointSeconds: number[];
  degraded: boolean;
  lastError: string | null;
  lastErrorAt: string | null;
}

export interface LearningStoreSummary {
  enabled: boolean;
  dataDir: string;
  sampleCount: number;
  labeledCount: number;
  unresolvedMarketCount: number;
  segmentCount: number;
  bytes: number;
  maxBytes: number;
  degraded: boolean;
  lastError: string | null;
  writeFailures: number;
  observeAttempts: number;
  observeAccepted: number;
  observeSkipped: number;
  labelsApplied: number;
}

export interface LearningStoreOptions {
  dataDir?: string;
  maxBytes?: number;
  segmentMaxBytes?: number;
  checkpointSeconds?: readonly number[] | string;
  gitSha?: string | null;
  enabled?: boolean;
}

export interface LearningObserveInput {
  marketId: string;
  asset: string | null;
  now: Date;
  featureVector: NativeV2ShadowFeatureVector;
  v2FairProbability: NativeV2ShadowFairProbability;
  native?: Pick<NativeDecision, "fairProbability" | "marketProbability" | "direction" | "confidence"> | null;
  experimentId?: string | null;
  gitSha?: string | null;
  /** Optional full market horizon (end - start). Invalid checkpoints above it are skipped. */
  horizonMs?: number | null;
  /**
   * Authoritative settlement lookup captured when the learning sample is accepted.
   * Required so unresolved labels remain labelable after restart when discovery drops the market.
   */
  settlementLookup?: BinarySettlementLookup;
}

type SegmentRow =
  | { kind: "sample"; sample: LearningSampleV1; settlementLookup?: BinarySettlementLookup }
  | { kind: "label"; marketId: string; sampleId: string; settlement: LearningSettlementLabel; at: string }
  | { kind: "settlement_lookup"; lookup: BinarySettlementLookup; at: string };

/**
 * Append-friendly segmented JSONL learning memory.
 * Failures degrade learning only — callers must catch and continue trading.
 */
export class LearningSampleStore {
  readonly dataDir: string;
  readonly maxBytes: number;
  readonly segmentMaxBytes: number;
  readonly checkpointSeconds: number[];
  readonly enabled: boolean;

  private readonly sampleIds = new Set<string>();
  private readonly checkpointIdsByMarket = new Map<string, Set<string>>();
  private readonly unresolvedMarkets = new Set<string>();
  private readonly labeledSampleIds = new Set<string>();
  private readonly samplesByMarket = new Map<string, Set<string>>();
  private readonly settlementLookups = new Map<string, BinarySettlementLookup>();

  private manifest: LearningDatasetManifest;
  private currentSegmentPath: string | null = null;
  private currentSegmentBytes = 0;
  private currentSegmentIndex = 0;
  private initialized = false;
  private degraded = false;
  private lastError: string | null = null;
  private lastErrorAt: string | null = null;
  private writeFailures = 0;
  private observeAttempts = 0;
  private observeAccepted = 0;
  private observeSkipped = 0;
  private labelsApplied = 0;
  private gitSha: string | null;

  constructor(options: LearningStoreOptions = {}) {
    this.enabled = options.enabled !== false;
    this.dataDir = options.dataDir ?? DEFAULT_LEARNING_DATA_DIR;
    this.maxBytes = Number.isFinite(options.maxBytes) && (options.maxBytes ?? 0) > 0
      ? Math.trunc(options.maxBytes!)
      : DEFAULT_LEARNING_MAX_BYTES;
    this.segmentMaxBytes = Number.isFinite(options.segmentMaxBytes) && (options.segmentMaxBytes ?? 0) > 0
      ? Math.trunc(options.segmentMaxBytes!)
      : DEFAULT_LEARNING_SEGMENT_MAX_BYTES;
    this.checkpointSeconds = typeof options.checkpointSeconds === "string" || options.checkpointSeconds == null
      ? parseLearningCheckpointSeconds(typeof options.checkpointSeconds === "string" ? options.checkpointSeconds : null)
      : parseLearningCheckpointSeconds([...options.checkpointSeconds].join(","));
    this.gitSha = options.gitSha ?? null;
    const now = new Date().toISOString();
    this.manifest = {
      manifestVersion: LEARNING_MANIFEST_VERSION,
      schemaVersion: LEARNING_SAMPLE_SCHEMA_VERSION,
      featureSchemaVersion: NATIVE_V2_SHADOW_FEATURE_VERSION,
      createdAt: now,
      updatedAt: now,
      gitSha: this.gitSha,
      sampleCount: 0,
      labeledCount: 0,
      unresolvedMarketCount: 0,
      segmentCount: 0,
      bytes: 0,
      maxBytes: this.maxBytes,
      checkpointSeconds: [...this.checkpointSeconds],
      degraded: false,
      lastError: null,
      lastErrorAt: null,
    };
  }

  /**
   * Rebuild dedupe index from durable segments.
   * Fail-soft: filesystem / segment failures degrade learning only — never throw.
   */
  init(): void {
    if (!this.enabled || this.initialized) return;
    try {
      this.rebuildFromDisk();
    } catch (error) {
      this.noteFailure(error);
      // Mark initialized so callers do not re-enter a throwing rebuild loop.
      this.initialized = true;
    }
  }

  /**
   * Fail-soft observe entry used by the engine. Never throws.
   * Returns accepted samples (may be empty).
   */
  tryObserve(input: LearningObserveInput): LearningSampleV1[] {
    this.observeAttempts += 1;
    if (!this.enabled) {
      this.observeSkipped += 1;
      return [];
    }
    try {
      const accepted = this.observe(input);
      if (accepted.length === 0) this.observeSkipped += 1;
      else this.observeAccepted += accepted.length;
      return accepted;
    } catch (error) {
      this.noteFailure(error);
      this.observeSkipped += 1;
      return [];
    }
  }

  observe(input: LearningObserveInput): LearningSampleV1[] {
    if (!this.enabled) return [];
    this.init();

    const timeToExpiryMs = input.featureVector.timing.timeToExpiryMs;
    if (timeToExpiryMs === null || !Number.isFinite(timeToExpiryMs)) return [];

    const already = this.checkpointIdsByMarket.get(input.marketId) ?? new Set<string>();
    const crossed = selectCrossedCheckpoints({
      timeToExpiryMs,
      checkpointSeconds: this.checkpointSeconds,
      alreadySampledCheckpointIds: already,
      ...(input.horizonMs !== undefined ? { horizonMs: input.horizonMs } : {}),
    });
    if (crossed.length === 0) return [];

    // Authoritative accept-time lookup is serialized ON the sample row (same append
    // boundary). Split state — durable sample without required lookup — is impossible.
    const authoritativeLookup =
      input.settlementLookup
      && input.settlementLookup.marketId === input.marketId
      && isAuthoritativeBinaryMapping(input.settlementLookup.yesMapsTo, input.settlementLookup.noMapsTo)
        ? structuredClone(input.settlementLookup)
        : undefined;

    const accepted: LearningSampleV1[] = [];
    for (const checkpoint of crossed) {
      const sampleId = learningSampleId(input.marketId, checkpoint.checkpointId);
      if (this.sampleIds.has(sampleId)) continue;

      const sample = buildLearningSampleV1({
        marketId: input.marketId,
        checkpointId: checkpoint.checkpointId,
        checkpointSeconds: checkpoint.checkpointSeconds,
        asset: input.asset,
        observedAt: input.now,
        timeToExpiryMs,
        featureVector: input.featureVector,
        v2FairProbability: input.v2FairProbability,
        ...(input.native !== undefined ? { native: input.native } : {}),
        ...(input.experimentId !== undefined ? { experimentId: input.experimentId } : {}),
        gitSha: input.gitSha ?? this.gitSha,
      });

      const row: SegmentRow = authoritativeLookup
        ? { kind: "sample", sample, settlementLookup: structuredClone(authoritativeLookup) }
        : { kind: "sample", sample };
      const line = `${JSON.stringify(row)}\n`;
      const lineBytes = Buffer.byteLength(line, "utf8");
      if (this.manifest.bytes + lineBytes > this.sampleWriteCeiling()) {
        this.noteFailure(new Error("LEARNING_CAPACITY_EXCEEDED"));
        break;
      }

      this.appendLine(line, lineBytes);
      this.indexSample(sample);
      if (authoritativeLookup) {
        this.settlementLookups.set(authoritativeLookup.marketId, structuredClone(authoritativeLookup));
      }
      accepted.push(sample);
    }
    if (accepted.length > 0) this.persistManifest();
    return accepted;
  }

  /**
   * Non-economic settlement labeling. Applies official resolution to unresolved
   * learning samples for a market. Does not touch ledger / settleMarket.
   */
  labelMarket(
    marketId: string,
    raw: Record<string, unknown>,
    yesMapsTo: Outcome = "UP",
    noMapsTo: Outcome = "DOWN",
    labeledAt: Date = new Date(),
  ): number {
    if (!this.enabled) return 0;
    this.init();
    const parsed = parsePublicBinaryResolution(raw, yesMapsTo, noMapsTo);
    if (parsed.status !== "resolved" || !parsed.winner) return 0;

    const sampleIds = this.samplesByMarket.get(marketId);
    if (!sampleIds || sampleIds.size === 0) {
      this.unresolvedMarkets.delete(marketId);
      this.syncManifestCounts();
      this.persistManifest();
      return 0;
    }

    const settlement: LearningSettlementLabel = {
      status: "resolved",
      outcome: parsed.winner,
      labeledAt: labeledAt.toISOString(),
      source: parsed.source,
      reason: parsed.reason,
    };

    let applied = 0;
    for (const sampleId of [...sampleIds]) {
      if (this.labeledSampleIds.has(sampleId)) continue;
      const line = `${JSON.stringify({
        kind: "label",
        marketId,
        sampleId,
        settlement,
        at: settlement.labeledAt!,
      } satisfies SegmentRow)}\n`;
      const lineBytes = Buffer.byteLength(line, "utf8");
      // Labels may consume the reserved label headroom past the sample ceiling.
      if (this.manifest.bytes + lineBytes > this.maxBytes) {
        this.noteFailure(new Error("LEARNING_CAPACITY_EXCEEDED"));
        break;
      }
      try {
        this.appendLine(line, lineBytes);
      } catch (error) {
        this.noteFailure(error);
        break;
      }
      this.labeledSampleIds.add(sampleId);
      applied += 1;
      this.labelsApplied += 1;
    }
    // Keep market unresolved until every related sample has an authoritative label.
    const allLabeled = [...sampleIds].every((id) => this.labeledSampleIds.has(id));
    if (allLabeled) this.unresolvedMarkets.delete(marketId);
    else this.unresolvedMarkets.add(marketId);
    this.syncManifestCounts();
    try {
      this.persistManifest();
    } catch (error) {
      this.noteFailure(error);
      // Partial durable labels still leave the market retryable.
      this.unresolvedMarkets.add(marketId);
    }
    return applied;
  }

  /** Markets with at least one unresolved learning sample (separate from ledger positions). */
  unresolvedLearningMarketIds(): string[] {
    return [...this.unresolvedMarkets].sort();
  }

  /**
   * Restart-safe settlement lookup for unresolved learning markets.
   * Returns null when no authoritative mapping was durably captured — do not invent mappings.
   */
  settlementLookupFor(marketId: string): BinarySettlementLookup | null {
    this.init();
    return this.settlementLookups.get(marketId) ?? null;
  }

  summary(): LearningStoreSummary {
    return {
      enabled: this.enabled,
      dataDir: this.dataDir,
      sampleCount: this.sampleIds.size,
      labeledCount: this.labeledSampleIds.size,
      unresolvedMarketCount: this.unresolvedMarkets.size,
      segmentCount: this.manifest.segmentCount,
      bytes: this.manifest.bytes,
      maxBytes: this.maxBytes,
      degraded: this.degraded,
      lastError: this.lastError,
      writeFailures: this.writeFailures,
      observeAttempts: this.observeAttempts,
      observeAccepted: this.observeAccepted,
      observeSkipped: this.observeSkipped,
      labelsApplied: this.labelsApplied,
    };
  }

  getManifest(): LearningDatasetManifest {
    return structuredClone(this.manifest);
  }

  /** Test helper: read all sample payloads with latest labels applied. */
  readSamplesForTest(): LearningSampleV1[] {
    this.init();
    const byId = new Map<string, LearningSampleV1>();
    for (const segmentPath of this.listSegmentPaths()) {
      const text = readFileSync(segmentPath, "utf8");
      for (const line of text.split("\n")) {
        if (!line.trim()) continue;
        const row = JSON.parse(line) as SegmentRow;
        if (row.kind === "sample") {
          byId.set(row.sample.sampleId, structuredClone(row.sample));
        } else if (row.kind === "label") {
          const sample = byId.get(row.sampleId);
          if (sample) sample.settlement = structuredClone(row.settlement);
        }
      }
    }
    return [...byId.values()].sort((a, b) => a.sampleId.localeCompare(b.sampleId));
  }

  /** Test helper: raw segment rows for atomic accept-boundary assertions. */
  readSegmentRowsForTest(): SegmentRow[] {
    this.init();
    const rows: SegmentRow[] = [];
    for (const segmentPath of this.listSegmentPaths()) {
      const text = readFileSync(segmentPath, "utf8");
      for (const line of text.split("\n")) {
        if (!line.trim()) continue;
        rows.push(JSON.parse(line) as SegmentRow);
      }
    }
    return rows;
  }

  private rebuildFromDisk(): void {
    mkdirSync(this.segmentsDir(), { recursive: true });
    this.sampleIds.clear();
    this.checkpointIdsByMarket.clear();
    this.unresolvedMarkets.clear();
    this.labeledSampleIds.clear();
    this.samplesByMarket.clear();
    this.settlementLookups.clear();

    const manifestPath = this.manifestPath();
    if (existsSync(manifestPath)) {
      try {
        const parsed = JSON.parse(readFileSync(manifestPath, "utf8")) as LearningDatasetManifest;
        this.manifest = {
          ...this.manifest,
          ...parsed,
          maxBytes: this.maxBytes,
          checkpointSeconds: [...this.checkpointSeconds],
          featureSchemaVersion: NATIVE_V2_SHADOW_FEATURE_VERSION,
        };
      } catch {
        // rebuild from segments only
      }
    }

    let bytes = 0;
    const segments = this.listSegmentPaths();
    for (const segmentPath of segments) {
      const text = readFileSync(segmentPath, "utf8");
      bytes += Buffer.byteLength(text, "utf8");
      for (const line of text.split("\n")) {
        if (!line.trim()) continue;
        try {
          const row = JSON.parse(line) as SegmentRow;
          if (row.kind === "sample") {
            this.indexSample(row.sample, false);
            if (row.settlementLookup?.marketId) {
              this.settlementLookups.set(row.settlementLookup.marketId, structuredClone(row.settlementLookup));
            }
          }
          else if (row.kind === "label") {
            this.labeledSampleIds.add(row.sampleId);
            this.unresolvedMarkets.delete(row.marketId);
          }
          else if (row.kind === "settlement_lookup") {
            if (row.lookup?.marketId) {
              this.settlementLookups.set(row.lookup.marketId, structuredClone(row.lookup));
            }
          }
        } catch {
          this.noteFailure(new Error(`LEARNING_SEGMENT_CORRUPT:${path.basename(segmentPath)}`));
        }
      }
    }

    this.unresolvedMarkets.clear();
    for (const [marketId, sampleIds] of this.samplesByMarket) {
      for (const sampleId of sampleIds) {
        if (!this.labeledSampleIds.has(sampleId)) {
          this.unresolvedMarkets.add(marketId);
          break;
        }
      }
    }

    this.currentSegmentIndex = segments.length > 0
      ? Math.max(...segments.map((p) => segmentIndexFromPath(p)), 0)
      : 0;
    if (segments.length > 0) {
      this.currentSegmentPath = segments[segments.length - 1]!;
      this.currentSegmentBytes = Buffer.byteLength(readFileSync(this.currentSegmentPath, "utf8"), "utf8");
    } else {
      this.currentSegmentPath = null;
      this.currentSegmentBytes = 0;
    }

    this.manifest.bytes = bytes;
    this.manifest.segmentCount = segments.length;
    this.syncManifestCounts();
    if (!existsSync(manifestPath)) {
      this.manifest.createdAt = new Date().toISOString();
    }
    this.persistManifest();
    this.initialized = true;
  }

  private indexSample(sample: LearningSampleV1, bumpUnresolved = true): void {
    this.sampleIds.add(sample.sampleId);
    const checkpoints = this.checkpointIdsByMarket.get(sample.marketId) ?? new Set<string>();
    checkpoints.add(sample.checkpointId);
    this.checkpointIdsByMarket.set(sample.marketId, checkpoints);
    const samples = this.samplesByMarket.get(sample.marketId) ?? new Set<string>();
    samples.add(sample.sampleId);
    this.samplesByMarket.set(sample.marketId, samples);
    if (bumpUnresolved && sample.settlement.status === "unresolved" && !this.labeledSampleIds.has(sample.sampleId)) {
      this.unresolvedMarkets.add(sample.marketId);
    }
    this.syncManifestCounts();
  }

  private syncManifestCounts(): void {
    this.manifest.sampleCount = this.sampleIds.size;
    this.manifest.labeledCount = this.labeledSampleIds.size;
    this.manifest.unresolvedMarketCount = this.unresolvedMarkets.size;
    this.manifest.updatedAt = new Date().toISOString();
    this.manifest.degraded = this.degraded;
    this.manifest.lastError = this.lastError;
    this.manifest.lastErrorAt = this.lastErrorAt;
    this.manifest.gitSha = this.gitSha;
  }

  private appendLine(line: string, lineBytes: number): void {
    mkdirSync(this.segmentsDir(), { recursive: true });
    if (
      !this.currentSegmentPath
      || this.currentSegmentBytes + lineBytes > this.segmentMaxBytes
    ) {
      this.currentSegmentIndex += 1;
      this.currentSegmentPath = path.join(
        this.segmentsDir(),
        `segment-${String(this.currentSegmentIndex).padStart(5, "0")}.jsonl`,
      );
      this.currentSegmentBytes = 0;
      this.manifest.segmentCount = Math.max(this.manifest.segmentCount, this.currentSegmentIndex);
    }
    appendFileSync(this.currentSegmentPath, line, "utf8");
    this.currentSegmentBytes += lineBytes;
    this.manifest.bytes += lineBytes;
  }

  private persistManifest(): void {
    mkdirSync(this.dataDir, { recursive: true });
    this.syncManifestCounts();
    const tmp = `${this.manifestPath()}.tmp`;
    writeFileSync(tmp, `${JSON.stringify(this.manifest, null, 2)}\n`, "utf8");
    renameSync(tmp, this.manifestPath());
  }

  private listSegmentPaths(): string[] {
    const dir = this.segmentsDir();
    if (!existsSync(dir)) return [];
    return readdirSync(dir)
      .filter((name) => /^segment-\d+\.jsonl$/.test(name))
      .sort()
      .map((name) => path.join(dir, name));
  }

  /** Bytes reserved for settlement labels near the sample capacity ceiling. */
  private labelReserveBytes(): number {
    const ratioReserve = Math.floor(this.maxBytes * DEFAULT_LEARNING_LABEL_RESERVE_RATIO);
    const reserve = Math.max(ratioReserve, DEFAULT_LEARNING_LABEL_RESERVE_FLOOR_BYTES);
    return Math.min(reserve, Math.floor(this.maxBytes / 2));
  }

  /** Sample writes stop before this ceiling so labels retain bounded headroom. */
  private sampleWriteCeiling(): number {
    return Math.max(0, this.maxBytes - this.labelReserveBytes());
  }



  private noteFailure(error: unknown): void {
    this.degraded = true;
    this.writeFailures += 1;
    this.lastError = error instanceof Error ? error.message : String(error);
    this.lastErrorAt = new Date().toISOString();
    this.syncManifestCounts();
    try {
      this.persistManifest();
    } catch {
      // last resort — still do not throw into trading
    }
  }

  private segmentsDir(): string {
    return path.join(this.dataDir, "segments");
  }

  private manifestPath(): string {
    return path.join(this.dataDir, "manifest.json");
  }
}

function segmentIndexFromPath(segmentPath: string): number {
  const match = path.basename(segmentPath).match(/^segment-(\d+)\.jsonl$/);
  return match ? Number(match[1]) : 0;
}

/**
 * Engine-facing helper: observe learning samples after V2 fair probability.
 * Never throws into the trading path.
 */
export function tryObserveLearningSample(
  store: LearningSampleStore | null | undefined,
  input: LearningObserveInput,
): LearningSampleV1[] {
  if (!store) return [];
  try {
    return store.tryObserve(input);
  } catch {
    return [];
  }
}

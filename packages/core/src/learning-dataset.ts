import { createHash } from "node:crypto";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";
import {
  LEARNING_SAMPLE_SCHEMA_VERSION,
  type LearningSampleV1,
} from "./learning-sample.js";
import { NATIVE_V2_SHADOW_FEATURE_VERSION } from "./native-v2-shadow-features.js";

/** Offline finalized dataset schema (PR4). Paper-only — never runtime authority. */
export const LEARNING_DATASET_VERSION = "learning-dataset-v1" as const;
export const LEARNING_TRAINING_ROW_SCHEMA_VERSION = "learning-training-row-v1" as const;

export type LearningBinaryLabel = 0 | 1;

export interface LearningTrainingRowV1 {
  schemaVersion: typeof LEARNING_TRAINING_ROW_SCHEMA_VERSION;
  sampleId: string;
  marketId: string;
  checkpointId: string;
  checkpointSeconds: number;
  asset: string | null;
  observedAt: string;
  timeToExpiryMs: number;
  featureSchemaVersion: string;
  /** Official UP=1 / DOWN=0 from settlement — never inferred. */
  labelUp: LearningBinaryLabel;
  settlementOutcome: "UP" | "DOWN";
  labeledAt: string | null;
  marketPriorUp: number;
  v2FairProbabilityUp: number;
  v1MarketProbabilityUp: number | null;
  v1FairProbabilityUp: number | null;
  /** Approximate executable UP ask from mid + spread/2 (top-of-book ask is not stored on the sample). */
  approximateExecutableAskUp: number;
  approximateExecutableBidUp: number;
  pairAskCost: number;
  featureVector: LearningSampleV1["featureVector"];
  v2FairProbability: LearningSampleV1["v2FairProbability"];
}

export interface LearningDatasetManifestV1 {
  datasetVersion: typeof LEARNING_DATASET_VERSION;
  sampleSchemaVersion: typeof LEARNING_SAMPLE_SCHEMA_VERSION;
  featureSchemaVersion: string;
  trainingRowSchemaVersion: typeof LEARNING_TRAINING_ROW_SCHEMA_VERSION;
  createdAt: string;
  rowCount: number;
  marketCount: number;
  assetBreakdown: Record<string, number>;
  checkpointBreakdown: Record<string, number>;
  /** Max rows retained per market after deterministic downsample (dominance control). */
  maxRowsPerMarket: number | null;
  ordering: "observedAt_asc_sampleId_asc";
  artifactFormat: "jsonl";
  artifactFormatRationale: string;
  rowsChecksumSha256: string;
  gitSha: string | null;
}

export interface BuildLearningDatasetResult {
  rows: LearningTrainingRowV1[];
  manifest: LearningDatasetManifestV1;
}

export interface BuildLearningDatasetOptions {
  /** Required feature schema; incompatible samples fail clearly. */
  expectedFeatureSchemaVersion?: string;
  /** Cap rows per market after stable sort (null = no extra cap). */
  maxRowsPerMarket?: number | null;
  gitSha?: string | null;
  now?: Date;
}

const ARTIFACT_FORMAT_RATIONALE =
  "JSONL matches the existing LearningSampleStore segmented JSONL layout and requires no new native deps; Parquet would add an ML/IO stack not otherwise present in @pm/core.";

/**
 * Build a finalized, reproducible offline dataset from learning samples.
 * Only authoritative resolved settlements are accepted.
 */
export function buildLearningDataset(
  samples: readonly LearningSampleV1[],
  options: BuildLearningDatasetOptions = {},
): BuildLearningDatasetResult {
  const expectedFeatureSchema =
    options.expectedFeatureSchemaVersion ?? NATIVE_V2_SHADOW_FEATURE_VERSION;
  const maxRowsPerMarket =
    options.maxRowsPerMarket === undefined ? null : options.maxRowsPerMarket;

  const finalized: LearningTrainingRowV1[] = [];
  const seenSampleIds = new Set<string>();

  for (const sample of samples) {
    if (sample.schemaVersion !== LEARNING_SAMPLE_SCHEMA_VERSION) {
      throw new Error(
        `LEARNING_DATASET_INCOMPATIBLE_SAMPLE_SCHEMA: expected ${LEARNING_SAMPLE_SCHEMA_VERSION}, got ${String(sample.schemaVersion)}`,
      );
    }
    if (sample.featureSchemaVersion !== expectedFeatureSchema) {
      throw new Error(
        `LEARNING_DATASET_INCOMPATIBLE_FEATURE_SCHEMA: expected ${expectedFeatureSchema}, got ${String(sample.featureSchemaVersion)}`,
      );
    }
    if (sample.featureVector.version !== expectedFeatureSchema) {
      throw new Error(
        `LEARNING_DATASET_INCOMPATIBLE_FEATURE_VECTOR_VERSION: expected ${expectedFeatureSchema}, got ${String(sample.featureVector.version)}`,
      );
    }
    if (sample.settlement.status !== "resolved") continue;
    if (sample.settlement.outcome !== "UP" && sample.settlement.outcome !== "DOWN") {
      throw new Error(
        `LEARNING_DATASET_INVALID_SETTLEMENT_OUTCOME: sample ${sample.sampleId} has status=resolved but outcome=${String(sample.settlement.outcome)}`,
      );
    }
    if (seenSampleIds.has(sample.sampleId)) {
      throw new Error(`LEARNING_DATASET_DUPLICATE_SAMPLE_ID: ${sample.sampleId}`);
    }
    seenSampleIds.add(sample.sampleId);

    const marketPriorUp = finiteProbability(sample.featureVector.pair.marketPriorUp);
    const v2Up = finiteProbability(sample.v2FairProbability.fairProbabilityUp);
    if (marketPriorUp === null || v2Up === null) {
      throw new Error(`LEARNING_DATASET_INVALID_PROBABILITY: sample ${sample.sampleId}`);
    }

    finalized.push({
      schemaVersion: LEARNING_TRAINING_ROW_SCHEMA_VERSION,
      sampleId: sample.sampleId,
      marketId: sample.marketId,
      checkpointId: sample.checkpointId,
      checkpointSeconds: sample.checkpointSeconds,
      asset: sample.asset,
      observedAt: sample.observedAt,
      timeToExpiryMs: sample.timeToExpiryMs,
      featureSchemaVersion: sample.featureSchemaVersion,
      labelUp: sample.settlement.outcome === "UP" ? 1 : 0,
      settlementOutcome: sample.settlement.outcome,
      labeledAt: sample.settlement.labeledAt,
      marketPriorUp,
      v2FairProbabilityUp: v2Up,
      v1MarketProbabilityUp: finiteProbability(sample.v1.marketProbability),
      v1FairProbabilityUp: finiteProbability(sample.v1.fairProbability),
      approximateExecutableAskUp: approximateAsk(sample.featureVector.up.mid, sample.featureVector.up.spread),
      approximateExecutableBidUp: approximateBid(sample.featureVector.up.mid, sample.featureVector.up.spread),
      pairAskCost: sample.featureVector.pair.pairAskCost,
      featureVector: structuredClone(sample.featureVector),
      v2FairProbability: structuredClone(sample.v2FairProbability),
    });
  }

  finalized.sort(compareTrainingRows);

  const capped =
    maxRowsPerMarket === null || maxRowsPerMarket === undefined
      ? finalized
      : downsamplePerMarket(finalized, maxRowsPerMarket);

  const marketCheckpoint = new Set<string>();
  for (const row of capped) {
    const key = `${row.marketId}::${row.checkpointId}`;
    if (marketCheckpoint.has(key)) {
      throw new Error(`LEARNING_DATASET_DUPLICATE_MARKET_CHECKPOINT: ${key}`);
    }
    marketCheckpoint.add(key);
  }

  const assetBreakdown: Record<string, number> = {};
  const checkpointBreakdown: Record<string, number> = {};
  const markets = new Set<string>();
  for (const row of capped) {
    markets.add(row.marketId);
    const assetKey = row.asset ?? "UNKNOWN";
    assetBreakdown[assetKey] = (assetBreakdown[assetKey] ?? 0) + 1;
    checkpointBreakdown[row.checkpointId] = (checkpointBreakdown[row.checkpointId] ?? 0) + 1;
  }

  const manifest: LearningDatasetManifestV1 = {
    datasetVersion: LEARNING_DATASET_VERSION,
    sampleSchemaVersion: LEARNING_SAMPLE_SCHEMA_VERSION,
    featureSchemaVersion: expectedFeatureSchema,
    trainingRowSchemaVersion: LEARNING_TRAINING_ROW_SCHEMA_VERSION,
    createdAt: (options.now ?? new Date()).toISOString(),
    rowCount: capped.length,
    marketCount: markets.size,
    assetBreakdown,
    checkpointBreakdown,
    maxRowsPerMarket,
    ordering: "observedAt_asc_sampleId_asc",
    artifactFormat: "jsonl",
    artifactFormatRationale: ARTIFACT_FORMAT_RATIONALE,
    rowsChecksumSha256: checksumTrainingRows(capped),
    gitSha: options.gitSha ?? null,
  };

  return { rows: capped, manifest };
}

export function writeLearningDatasetArtifact(
  outputDir: string,
  result: BuildLearningDatasetResult,
): { rowsPath: string; manifestPath: string } {
  mkdirSync(outputDir, { recursive: true });
  const rowsPath = path.join(outputDir, "dataset.jsonl");
  const manifestPath = path.join(outputDir, "dataset.manifest.json");
  const body =
    result.rows.map((row) => JSON.stringify(row)).join("\n") + (result.rows.length > 0 ? "\n" : "");
  writeFileSync(rowsPath, body, "utf8");
  writeFileSync(manifestPath, `${JSON.stringify(result.manifest, null, 2)}\n`, "utf8");
  return { rowsPath, manifestPath };
}

/** Load samples from a LearningSampleStore dataDir (segments/*.jsonl), applying labels. */
export function loadLearningSamplesFromStoreDir(dataDir: string): LearningSampleV1[] {
  const segmentsDir = path.join(dataDir, "segments");
  if (!existsSync(segmentsDir)) return [];
  const byId = new Map<string, LearningSampleV1>();
  const segments = readdirSync(segmentsDir)
    .filter((name) => /^segment-\d+\.jsonl$/.test(name))
    .sort()
    .map((name) => path.join(segmentsDir, name));
  for (const segmentPath of segments) {
    const text = readFileSync(segmentPath, "utf8");
    for (const line of text.split("\n")) {
      if (!line.trim()) continue;
      const row = JSON.parse(line) as
        | { kind: "sample"; sample: LearningSampleV1 }
        | { kind: "label"; sampleId: string; settlement: LearningSampleV1["settlement"] };
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

export function readLearningDatasetJsonlSync(rowsPath: string): LearningTrainingRowV1[] {
  if (!existsSync(rowsPath)) return [];
  const text = readFileSync(rowsPath, "utf8");
  const rows: LearningTrainingRowV1[] = [];
  for (const line of text.split("\n")) {
    if (!line.trim()) continue;
    rows.push(JSON.parse(line) as LearningTrainingRowV1);
  }
  return rows;
}

export function checksumTrainingRows(rows: readonly LearningTrainingRowV1[]): string {
  const hash = createHash("sha256");
  for (const row of rows) {
    hash.update(JSON.stringify(row));
    hash.update("\n");
  }
  return hash.digest("hex");
}

function downsamplePerMarket(
  rows: readonly LearningTrainingRowV1[],
  maxRowsPerMarket: number,
): LearningTrainingRowV1[] {
  if (!Number.isInteger(maxRowsPerMarket) || maxRowsPerMarket <= 0) {
    throw new Error(`LEARNING_DATASET_INVALID_MAX_ROWS_PER_MARKET: ${maxRowsPerMarket}`);
  }
  const counts = new Map<string, number>();
  const out: LearningTrainingRowV1[] = [];
  for (const row of rows) {
    const n = counts.get(row.marketId) ?? 0;
    if (n >= maxRowsPerMarket) continue;
    counts.set(row.marketId, n + 1);
    out.push(row);
  }
  return out;
}

function compareTrainingRows(a: LearningTrainingRowV1, b: LearningTrainingRowV1): number {
  const byTime = a.observedAt.localeCompare(b.observedAt);
  if (byTime !== 0) return byTime;
  return a.sampleId.localeCompare(b.sampleId);
}

function approximateAsk(mid: number, spread: number): number {
  return clamp01(mid + spread / 2);
}

function approximateBid(mid: number, spread: number): number {
  return clamp01(mid - spread / 2);
}

function finiteProbability(value: number | null | undefined): number | null {
  if (value === null || value === undefined) return null;
  return Number.isFinite(value) && value >= 0 && value <= 1 ? value : null;
}

function clamp01(value: number): number {
  if (!Number.isFinite(value)) return 0.5;
  return Math.min(1, Math.max(0, value));
}

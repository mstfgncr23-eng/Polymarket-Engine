#!/usr/bin/env tsx
/**
 * Paper-only offline learning train/validate CLI (PR4).
 * Reads finalized LearningSampleStore JSONL (or an in-memory path), builds a
 * walk-forward dataset, fits residual logistic, scores baselines, and writes a
 * versioned candidate artifact. Never touches runtime trading authority.
 *
 * Usage:
 *   pnpm learning:train-validate -- --data-dir /data/learning --out ./artifacts/pr4
 */
import { existsSync } from "node:fs";
import path from "node:path";
import {
  loadLearningSamplesFromStoreDir,
  learningModelsDir,
  registerLearningCandidate,
  runLearningOfflinePipeline,
} from "@pm/core";

function arg(name: string, fallback?: string): string | undefined {
  const idx = process.argv.indexOf(name);
  if (idx >= 0 && process.argv[idx + 1]) return process.argv[idx + 1];
  return fallback;
}

function hasFlag(name: string): boolean {
  return process.argv.includes(name);
}

async function main(): Promise<void> {
  if (hasFlag("--help") || hasFlag("-h")) {
    console.log(`Paper-only offline Native V2 learning train/validate (PR4)

Options:
  --data-dir <path>   LearningSampleStore directory (manifest + segments/)
  --out <path>        Output directory for dataset.jsonl + candidate.json
  --git-sha <sha>     Optional git SHA stamped into artifacts
  --val-fraction <n>  Validation market fraction (default 0.3)
  --max-rows-per-market <n>  Optional dominance cap
  --assumed-fee <n>   Optional assumed fee/share for net-edge reporting
  --register-candidate  Register output as CANDIDATE in LEARNING_DATA_DIR/models
                        (never auto-promotes to SHADOW/PAPER_APPROVED)
`);
    return;
  }

  const dataDir = arg("--data-dir");
  const outDir = arg("--out", path.resolve("artifacts/learning-pr4"));
  const gitSha = arg("--git-sha", process.env.GIT_SHA ?? process.env.GITHUB_SHA ?? null) ?? null;
  const valFraction = Number(arg("--val-fraction", "0.3"));
  const maxRowsRaw = arg("--max-rows-per-market");
  const assumedFeeRaw = arg("--assumed-fee");

  if (!dataDir) {
    console.error("ERROR: --data-dir is required");
    process.exitCode = 1;
    return;
  }
  if (!existsSync(dataDir)) {
    console.error(`ERROR: data dir not found: ${dataDir}`);
    process.exitCode = 1;
    return;
  }

  const samples = loadLearningSamplesFromStoreDir(dataDir);
  const resolved = samples.filter((s) => s.settlement.status === "resolved");
  console.log(`Loaded ${samples.length} samples (${resolved.length} resolved) from ${dataDir}`);

  if (resolved.length < 4) {
    console.error("ERROR: need at least 4 resolved samples across >=2 markets for walk-forward");
    process.exitCode = 1;
    return;
  }

  const result = runLearningOfflinePipeline(samples, {
    gitSha,
    outputDir: outDir!,
    dataset: {
      gitSha,
      ...(maxRowsRaw ? { maxRowsPerMarket: Number(maxRowsRaw) } : {}),
    },
    walkForward: { validationMarketFraction: valFraction },
    residual: { maxIterations: 400, l2Lambda: 0.01 },
    economics: {
      minExpectedGrossEdge: 0.01,
      ...(assumedFeeRaw ? { assumedFeePerShare: Number(assumedFeeRaw) } : {}),
    },
  });

  let registered: { modelId: string; status: string } | null = null;
  if (hasFlag("--register-candidate")) {
    if (!result.output?.candidatePath) {
      console.error("ERROR: no candidate path to register");
      process.exitCode = 1;
      return;
    }
    const entry = registerLearningCandidate(learningModelsDir(dataDir), {
      artifactPath: result.output.candidatePath,
      candidate: result.candidate,
    });
    registered = { modelId: entry.modelId, status: entry.status };
    if (entry.status !== "CANDIDATE") {
      console.error("ERROR: register must produce CANDIDATE only");
      process.exitCode = 1;
      return;
    }
  }

  console.log(JSON.stringify({
    paperOnly: true,
    runtimeAuthority: false,
    autoPromotion: false,
    datasetRows: result.dataset.rows.length,
    trainMarkets: result.split.trainMarketIds.length,
    validationMarkets: result.split.validationMarketIds.length,
    modelType: result.candidate.modelType,
    modelVersion: result.candidate.modelVersion,
    featureSchemaVersion: result.candidate.featureSchemaVersion,
    datasetVersion: result.candidate.datasetVersion,
    checksumSha256: result.candidate.checksumSha256,
    metrics: result.candidate.metrics,
    economic: {
      tradeCount: result.candidate.economicEvaluation.tradeCount,
      totalRealizedPnlPerShare: result.candidate.economicEvaluation.totalRealizedPnlPerShare,
      capabilities: result.candidate.economicEvaluation.capabilities,
      unavailable: result.candidate.economicEvaluation.unavailable,
    },
    output: result.output,
    registered,
  }, null, 2));
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack ?? error.message : error);
  process.exitCode = 1;
});

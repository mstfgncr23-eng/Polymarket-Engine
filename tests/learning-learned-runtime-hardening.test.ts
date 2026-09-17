import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  LearningRuntime,
  LearningScorecardStore,
  buildLearningScorecardRecord,
  evaluateLearnedV2Shadow,
  learningModelsDir,
  learningScorecardDir,
} from "@pm/core";

const dirs: string[] = [];

afterEach(() => {
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

function tempDir(): string {
  const dir = mkdtempSync(join(tmpdir(), "pm-learning-pr5-hardening-"));
  dirs.push(dir);
  return dir;
}

describe("PR5 learned runtime fail-soft boundary", () => {
  it("corrupt registry cannot escape init into engine startup", () => {
    const dataDir = tempDir();
    const modelsDir = learningModelsDir(dataDir);
    mkdirSync(modelsDir, { recursive: true });
    writeFileSync(join(modelsDir, "registry.json"), "{not-json", "utf8");

    const runtime = new LearningRuntime({
      shadowEnabled: true,
      learningDataDir: dataDir,
      modelArtifactPath: null,
      modelId: "missing-after-corrupt-registry",
      paperAuthority: { paperAuthorityEnabled: false, approvedModelId: null },
    });

    expect(() => runtime.init()).not.toThrow();
    const summary = runtime.summary();
    expect(summary.modelLoaded).toBe(false);
    expect(summary.loadFailureReason).toBe("RUNTIME_INIT_ERROR");

    const evaluation = runtime.evaluateShadow({
      featureVector: null,
      deterministicV2: null,
      marketId: "m-corrupt-registry",
      recordScorecard: false,
    });
    expect(evaluation.status).toBe("fail_safe");
  });
});

describe("PR5 scorecard durable byte cap", () => {
  it("replays existing bytes and refuses growth beyond cap after restart", () => {
    const dataDir = tempDir();
    const scorecardDir = learningScorecardDir(dataDir);
    const evaluation = evaluateLearnedV2Shadow({
      enabled: false,
      loaded: null,
      featureVector: null,
      deterministicV2: null,
      now: new Date("2026-09-17T00:00:00.000Z"),
    });
    const record = buildLearningScorecardRecord({
      evaluation,
      marketId: "m-cap",
      recordId: "cap-record",
    });
    const row1 = { ...record, recordId: "cap-1" };
    const row2 = { ...record, recordId: "cap-2" };
    const maxBytes = Buffer.byteLength(`${JSON.stringify(row1)}\n`, "utf8")
      + Buffer.byteLength(`${JSON.stringify(row2)}\n`, "utf8");

    const first = new LearningScorecardStore({ dataDir: scorecardDir, maxBytes });
    first.init();
    first.append(row1);
    first.append(row2);
    expect(first.summary().bytes).toBe(maxBytes);
    expect(() => first.append({ ...record, recordId: "cap-3" })).toThrow(/SCORECARD_MAX_BYTES/);

    const segmentPath = join(scorecardDir, "segments", "segment-00001.jsonl");
    const beforeRestart = readFileSync(segmentPath, "utf8");

    const restarted = new LearningScorecardStore({ dataDir: scorecardDir, maxBytes });
    restarted.init();
    expect(restarted.summary().bytes).toBe(maxBytes);
    expect(restarted.summary().degraded).toBe(true);
    expect(() => restarted.append({ ...record, recordId: "cap-4" })).toThrow(/SCORECARD_MAX_BYTES/);
    expect(readFileSync(segmentPath, "utf8")).toBe(beforeRestart);
  });
});

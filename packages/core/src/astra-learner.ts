import { z } from "zod";
import { astraLessonSchema, type AstraLesson } from "./astra.js";
import { updateAstraCalibration, type AstraCalibrationState, type AstraSettlementObservation } from "./astra-learning.js";

const astraCalibrationStateSchema = z.object({ samples: z.number().int().nonnegative(), brierSum: z.number().finite().nonnegative(), reliability: z.number().min(0).max(1) }).strict();
const astraSettlementObservationSchema = z.object({ predictedProbability: z.number().min(0).max(1), outcome: z.union([z.literal(0), z.literal(1)]) }).strict();
const astraLearningPendingSchema = z.object({ id: z.string().min(1).max(160), observation: astraSettlementObservationSchema }).strict();
export const astraLearningStateSchema = z.object({
  calibration: astraCalibrationStateSchema,
  processedSettlementIds: z.array(z.string().min(1).max(160)).max(1_024),
  pending: z.array(astraLearningPendingSchema).max(1_024).default([]),
  lessons: z.array(astraLessonSchema).max(8).default([]),
}).strict();

export interface AstraLearningState {
  calibration: AstraCalibrationState;
  processedSettlementIds: string[];
  pending: Array<{ id: string; observation: AstraSettlementObservation }>;
  lessons: AstraLesson[];
}

export class AstraBatchLearner {
  private calibration: AstraCalibrationState = { samples: 0, brierSum: 0, reliability: 1 };
  private readonly processed = new Set<string>();
  private pending: Array<{ id: string; observation: AstraSettlementObservation }> = [];
  private lessons: AstraLesson[] = [];
  constructor(private readonly maxProcessed = 1_024) {}
  observe(id: string, observation: AstraSettlementObservation): void {
    if (this.processed.has(id)) return;
    this.processed.add(id);
    this.pending.push({ id, observation });
    while (this.pending.length > this.maxProcessed) this.pending.shift();
    while (this.processed.size > this.maxProcessed) this.processed.delete(this.processed.values().next().value as string);
  }
  flush(threshold: number): { calibration: AstraCalibrationState; sampleIds: string[] } | null {
    if (this.pending.length < Math.max(1, threshold)) return null;
    const batch = this.pending.splice(0);
    const sampleIds = batch.map((item) => item.id);
    for (const item of batch) this.calibration = updateAstraCalibration(this.calibration, item.observation);
    return { calibration: { ...this.calibration }, sampleIds };
  }
  applyLessons(lessons: readonly AstraLesson[]): void {
    const parsed = lessons.map((lesson) => astraLessonSchema.parse(lesson));
    this.lessons = parsed.slice(-8);
  }
  snapshot(): AstraLearningState { return { calibration: { ...this.calibration }, processedSettlementIds: [...this.processed], pending: structuredClone(this.pending), lessons: structuredClone(this.lessons) }; }
  restore(state: AstraLearningState): void {
    const parsed = astraLearningStateSchema.parse(state);
    this.calibration = { ...parsed.calibration };
    this.processed.clear();
    for (const id of parsed.processedSettlementIds.slice(-this.maxProcessed)) this.processed.add(id);
    this.pending = parsed.pending.slice(-this.maxProcessed).map((item) => ({ id: item.id, observation: { ...item.observation } }));
    this.lessons = parsed.lessons.slice(-8).map((lesson) => ({ ...lesson }));
  }
}

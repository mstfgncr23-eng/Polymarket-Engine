import type { z } from "zod";
import type { bookLevelSchema, feeScheduleSchema, marketSnapshotSchema, positionSchema } from "./domain.js";

export type BookLevel = z.infer<typeof bookLevelSchema>;
export type FeeSchedule = z.infer<typeof feeScheduleSchema>;
export type MarketSnapshot = z.infer<typeof marketSnapshotSchema>;
export type Position = z.infer<typeof positionSchema>;

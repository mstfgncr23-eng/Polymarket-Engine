export { createId } from "./domain.js";
export type { BtcTick, MarketSnapshot, TriggerEvent } from "./domain.js";
import type { z } from "zod";
import type { triggerKindSchema } from "./domain.js";
export type TriggerKind = z.infer<typeof triggerKindSchema>;

import { z } from "zod";
export const QUARANTINE_TRANSITION = "EXPERIMENT_NON_FINAL_QUARANTINED";
export const quarantineOperationKey = (experimentId: string) => "experiment-quarantine:" + experimentId;
export class ExperimentQuarantineRejectedError extends Error {
  readonly code = "EXPERIMENT_QUARANTINE_REJECTED";
  constructor(readonly blocker: string) { super(blocker); this.name = "ExperimentQuarantineRejectedError"; }
}
export const quarantinePayloadSchema = z.object({
  experimentId: z.string().min(1).max(120),
  experimentStatus: z.literal("COMPLETED"),
  disposition: z.literal("QUARANTINED_NON_FINAL"),
  quarantinedAt: z.string().datetime({ offset: true }),
  operatorId: z.string().min(1).max(80),
  requestId: z.string().min(1).max(80),
  reasonCode: z.literal("CANONICAL_SOURCE_INTEGRITY_UNRESOLVED"),
  canonicalFinalReportPresent: z.literal(false),
  integrityFaultCount: z.number().int().min(1).max(64),
  faultReferences: z.array(z.object({
    experimentId: z.string().min(1).max(120), eventId: z.string().min(1).max(160),
    eventType: z.string().min(1).max(80), reason: z.string().max(512),
    at: z.string().datetime({ offset: true }),
  })).min(1).max(64),
  durableCursorBefore: z.number().int().nonnegative(),
  validationAuthority: z.literal("DURABLE_REPLAY_VALID"),
  economicTerminality: z.object({
    openPositions: z.literal(0), restingOrders: z.literal(0), reservations: z.literal(0),
    reservedCash: z.literal("0"), riskReductionOwners: z.literal(0), pendingEconomicWork: z.literal(0),
  }),
}).superRefine((value, ctx) => {
  if (value.faultReferences.length !== value.integrityFaultCount
    || value.faultReferences.some(f => f.experimentId !== value.experimentId)
    || new Set(value.faultReferences.map(f => f.eventId)).size !== value.faultReferences.length) {
    ctx.addIssue({ code: "custom", message: "quarantine fault evidence identity mismatch" });
  }
});
export type QuarantinePayload = z.infer<typeof quarantinePayloadSchema>;
export const quarantineReceiptSchema = quarantinePayloadSchema.and(z.object({ sequence: z.number().int().positive() }));
export type QuarantineReceipt = z.infer<typeof quarantineReceiptSchema>;

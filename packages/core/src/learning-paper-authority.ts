import type { LearningModelStatus } from "./learning-model-registry.js";
import { statusGrantsPaperAuthority } from "./learning-model-registry.js";

/**
 * Controlled PAPER authority gate (PR5 infrastructure only).
 *
 * IMPORTANT — execution wiring is INTENTIONALLY ABSENT in this PR:
 * V1 authority is evaluateNativeDecision → evaluateTrade with hardcoded native
 * fair/market probabilities and deterministic risk. There is no pluggable
 * probability provider or learned-authority fork. Until such a hook exists
 * without inventing architecture, this gate is advisory documentation + tests
 * only and must never be wired to submit orders.
 *
 * Requirements that would be necessary IF a future PR adds a safe hook:
 * 1. LEARNING_PAPER_AUTHORITY_ENABLED === true (default false)
 * 2. registry status === PAPER_APPROVED (never CANDIDATE/SHADOW auto)
 * 3. paperApprovedByOperator === true
 * 4. explicit operator model id match
 * 5. deterministic risk still mandatory (never bypassed)
 * 6. all existing execution gates still mandatory
 * 7. TRADING_MODE remains paper (no live)
 */
export interface ControlledPaperAuthorityConfig {
  /** Default false. Must be explicitly enabled by operator. */
  paperAuthorityEnabled: boolean;
  /** Explicit model id the operator approved for a future controlled path. */
  approvedModelId: string | null;
}

export interface ControlledPaperAuthorityInput {
  config: ControlledPaperAuthorityConfig;
  modelId: string | null;
  registryStatus: LearningModelStatus | null;
  paperApprovedByOperator: boolean;
  /** Deterministic risk still in the call chain (must be true for any future path). */
  deterministicRiskMandatory: boolean;
  /** All existing execution gates still apply. */
  executionGatesMandatory: boolean;
  tradingMode: "paper" | "live" | string;
}

export type ControlledPaperAuthorityBlockReason =
  | "PAPER_AUTHORITY_DISABLED"
  | "STATUS_NOT_PAPER_APPROVED"
  | "OPERATOR_FLAG_MISSING"
  | "MODEL_ID_MISMATCH"
  | "MODEL_ID_REQUIRED"
  | "DETERMINISTIC_RISK_NOT_MANDATORY"
  | "EXECUTION_GATES_NOT_MANDATORY"
  | "LIVE_TRADING_FORBIDDEN"
  | "ENGINE_HOOK_NOT_IMPLEMENTED";

export interface ControlledPaperAuthorityDecision {
  /**
   * Would the *future* controlled path be allowed to even be considered?
   * In PR5 this never unlocks order submission — see engineHookImplemented.
   */
  configAllows: boolean;
  /** Always false in PR5 — no safe engine integration proven. */
  maySubmitOrders: boolean;
  engineHookImplemented: false;
  blockReasons: ControlledPaperAuthorityBlockReason[];
}

export const LEARNING_PAPER_AUTHORITY_BLOCKER =
  "V1 decision/execution path has no pluggable fair-probability or learned-authority hook; wiring learned models into evaluateTrade would invent architecture. Controlled PAPER execution path deferred." as const;

/**
 * Evaluate whether a model could enter a *future* controlled PAPER path.
 * CANDIDATE never passes. PR5 never returns maySubmitOrders=true.
 */
export function evaluateControlledPaperAuthorityGate(
  input: ControlledPaperAuthorityInput,
): ControlledPaperAuthorityDecision {
  const blockReasons: ControlledPaperAuthorityBlockReason[] = [];

  if (!input.config.paperAuthorityEnabled) {
    blockReasons.push("PAPER_AUTHORITY_DISABLED");
  }
  if (!input.registryStatus || !statusGrantsPaperAuthority(input.registryStatus)) {
    blockReasons.push("STATUS_NOT_PAPER_APPROVED");
  }
  if (!input.paperApprovedByOperator) {
    blockReasons.push("OPERATOR_FLAG_MISSING");
  }
  if (!input.config.approvedModelId) {
    blockReasons.push("MODEL_ID_REQUIRED");
  } else if (!input.modelId || input.modelId !== input.config.approvedModelId) {
    blockReasons.push("MODEL_ID_MISMATCH");
  }
  if (!input.deterministicRiskMandatory) {
    blockReasons.push("DETERMINISTIC_RISK_NOT_MANDATORY");
  }
  if (!input.executionGatesMandatory) {
    blockReasons.push("EXECUTION_GATES_NOT_MANDATORY");
  }
  if (input.tradingMode !== "paper") {
    blockReasons.push("LIVE_TRADING_FORBIDDEN");
  }

  // Hard blocker for this PR: no engine hook.
  blockReasons.push("ENGINE_HOOK_NOT_IMPLEMENTED");

  const configAllows =
    input.config.paperAuthorityEnabled
    && input.registryStatus !== null
    && statusGrantsPaperAuthority(input.registryStatus)
    && input.paperApprovedByOperator
    && !!input.config.approvedModelId
    && input.modelId === input.config.approvedModelId
    && input.deterministicRiskMandatory
    && input.executionGatesMandatory
    && input.tradingMode === "paper";

  return {
    configAllows,
    maySubmitOrders: false,
    engineHookImplemented: false,
    blockReasons,
  };
}

/** Convenience: CANDIDATE (or anything except PAPER_APPROVED) cannot obtain PAPER authority. */
export function candidateCannotObtainPaperAuthority(status: LearningModelStatus): boolean {
  return !statusGrantsPaperAuthority(status);
}

import type { z } from "zod";
import {
  cryptoMarketOutputSchema,
  externalAgentRoleIdSchema,
  fairProbabilityOutputSchema,
  grokSignalSubmissionSchema,
  marketMicrostructureOutputSchema,
  newsXIntelligenceOutputSchema,
  riskHedgeOutputSchema,
  strategySupervisorOutputSchema,
  type ExternalAgentRoleId,
  type GrokSignalSubmission,
} from "./domain.js";

interface ExternalRoleDefinition<T extends z.ZodType> {
  id: ExternalAgentRoleId;
  name: string;
  schema: T;
  instructions: string;
}

/** Logical Grok Bot identities. These contracts expose no order, trade, or wallet capability. */
export const externalAgentRoleDefinitions = [
  {
    id: "market_microstructure_scanner", name: "Market Microstructure Scanner",
    schema: marketMicrostructureOutputSchema,
    instructions: "Analyze supplied order books and market snapshots. Submit structured research only; never place or approve an order.",
  },
  {
    id: "crypto_market_scanner", name: "Crypto Market Scanner",
    schema: cryptoMarketOutputSchema,
    instructions: "Analyze supplied crypto market state and submit structured research only; never place or approve an order.",
  },
  {
    id: "news_x_intelligence_scanner", name: "News/X Intelligence Scanner",
    schema: newsXIntelligenceOutputSchema,
    instructions: "Classify relevant, sourced news/X intelligence and submit structured research only; never place or approve an order.",
  },
  {
    id: "fair_probability_analyst", name: "Market/Fair Probability Analyst",
    schema: fairProbabilityOutputSchema,
    instructions: "Estimate fair probability and edge from supplied evidence. Submit a recommendation only; never place or approve an order.",
  },
  {
    id: "risk_hedge_analyst", name: "Risk & Hedge Analyst",
    schema: riskHedgeOutputSchema,
    instructions: "Assess inventory and hedge risk. You may recommend a veto, but cannot approve risk or place an order.",
  },
  {
    id: "strategy_supervisor", name: "Strategy Supervisor",
    schema: strategySupervisorOutputSchema,
    instructions: "Diagnose strategy behavior. Parameter changes must remain PROPOSED and can never be activated through MCP.",
  },
] as const satisfies readonly ExternalRoleDefinition<z.ZodType>[];

if (externalAgentRoleDefinitions.length !== 6 || new Set(externalAgentRoleDefinitions.map((role) => role.id)).size !== 6) {
  throw new Error("EXACTLY_SIX_EXTERNAL_GROK_ROLES_REQUIRED");
}

export const grokWriteToolNames = ["submit_signal", "submit_analysis", "recommend_hedge", "recommend_veto"] as const;
export type GrokWriteToolName = (typeof grokWriteToolNames)[number];

/** Authenticated MCP clientId must be this role ID; only these write tools are legal for that token. */
export const grokRoleWriteTools = {
  market_microstructure_scanner: ["submit_signal"],
  crypto_market_scanner: ["submit_signal"],
  news_x_intelligence_scanner: ["submit_signal"],
  fair_probability_analyst: ["submit_analysis"],
  risk_hedge_analyst: ["recommend_hedge", "recommend_veto"],
  strategy_supervisor: ["submit_signal"],
} as const satisfies Record<ExternalAgentRoleId, readonly GrokWriteToolName[]>;

export class GrokSignalValidationError extends Error {
  constructor(readonly code: string, message = code) {
    super(message);
    this.name = "GrokSignalValidationError";
  }
}

export function writeToolsForClient(clientId: string): readonly GrokWriteToolName[] {
  const role = externalAgentRoleIdSchema.safeParse(clientId);
  return role.success ? grokRoleWriteTools[role.data] : [];
}

export function clientMayCallWriteTool(clientId: string, tool: string): boolean {
  return (writeToolsForClient(clientId) as readonly string[]).includes(tool);
}

export function assertGrokWriteAuthorization(clientId: string, tool: string, submission: GrokSignalSubmission): void {
  if (submission.agentId !== submission.role) {
    throw new GrokSignalValidationError("IDENTITY_ROLE_MISMATCH", "agentId and role must be identical");
  }
  if (clientId !== submission.agentId) {
    throw new GrokSignalValidationError(
      "IDENTITY_CLIENT_MISMATCH",
      `authenticated clientId ${clientId} cannot submit as ${submission.agentId}`,
    );
  }
  if (!clientMayCallWriteTool(clientId, tool)) {
    throw new GrokSignalValidationError("WRITE_TOOL_FORBIDDEN", `${clientId} cannot call ${tool}`);
  }
}

export function validateExternalAgentRoleId(value: unknown): ExternalAgentRoleId {
  return externalAgentRoleIdSchema.parse(value);
}

export function validateGrokSignalSubmission(value: unknown): GrokSignalSubmission {
  return grokSignalSubmissionSchema.parse(value);
}

/** Maps the four limited MCP write tools onto the same Zod-validated inbox payload. */
export function parseGrokWritePayload(tool: string, raw: unknown, clientId: string): GrokSignalSubmission {
  if (!(grokWriteToolNames as readonly string[]).includes(tool)) {
    throw new GrokSignalValidationError("UNKNOWN_WRITE_TOOL", `Unsupported write tool: ${tool}`);
  }
  const parsed = grokSignalSubmissionSchema.safeParse(raw);
  if (!parsed.success) throw parsed.error;
  const submission = parsed.data;
  assertGrokWriteAuthorization(clientId, tool, submission);
  if (tool === "recommend_veto" && submission.agentId === "risk_hedge_analyst" && submission.output.recommendVeto !== true) {
    throw new GrokSignalValidationError("VETO_RECOMMENDATION_REQUIRED");
  }
  if (tool === "recommend_hedge" && submission.agentId === "risk_hedge_analyst" && submission.output.recommendVeto === true) {
    throw new GrokSignalValidationError("HEDGE_TOOL_CANNOT_VETO");
  }
  return submission;
}

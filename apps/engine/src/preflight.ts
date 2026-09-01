import type { EngineEnv } from "@pm/core";

export interface PreflightResult {
  ok: boolean;
  blockers: string[];
  warnings: string[];
  checks: Record<string, unknown>;
}

export function evaluateExperimentPreflight(input: {
  env: EngineEnv;
  databaseUp: boolean;
  discoveryStatus: string;
  qualifiedOrManaged: number;
  reference: Array<{ asset: string; status: string; detail?: string }>;
  mcpRoleTokens: string[];
  bootstrapFailed: boolean;
}): PreflightResult {
  const blockers: string[] = [];
  const warnings: string[] = [];
  if (input.env.TRADING_MODE !== "paper") blockers.push("TRADING_MODE_NOT_PAPER");
  if (!input.databaseUp) blockers.push("POSTGRES_UNHEALTHY");
  if (input.bootstrapFailed) blockers.push("UNRESOLVED_BOOTSTRAP_FAILURE");
  if (input.qualifiedOrManaged <= 0 && input.discoveryStatus === "ZERO_QUALIFIED") {
    warnings.push("NO_QUALIFIED_OR_MANAGED_MARKETS");
  }
  const requiredRoles = [
    "market_microstructure_scanner", "crypto_market_scanner", "news_x_intelligence_scanner",
    "fair_probability_analyst", "risk_hedge_analyst", "strategy_supervisor",
  ];
  const tokens = input.env.GROK_MCP_BEARER_TOKENS.split(",").map((entry) => entry.trim().split(":")[0]).filter(Boolean);
  const missingRoles = requiredRoles.filter((role) => !tokens.includes(role));
  if (input.env.EXPERIMENT_REQUIRES_GROK) {
    if (input.env.GROK_ACTIVATION_PROVIDER === "noop") blockers.push("GROK_ACTIVATION_NOOP");
    if (missingRoles.length > 0) blockers.push("GROK_MCP_ROLES_INCOMPLETE");
    if (input.env.GROK_ACTIVATION_PROVIDER === "slack" && (!input.env.SLACK_BOT_TOKEN || !input.env.SLACK_CHANNEL_ID)) {
      blockers.push("SLACK_ACTIVATION_UNCONFIGURED");
    }
  } else if (input.env.GROK_ACTIVATION_PROVIDER === "noop") {
    warnings.push("GROK_WAKE_NOT_VALIDATED");
  }
  const blocked = input.reference.filter((item) => item.status === "PROVIDER_BLOCKED" || item.status === "REGION_BLOCKED");
  if (blocked.length > 0) warnings.push("REFERENCE_PROVIDER_BLOCKED");
  return {
    ok: blockers.length === 0,
    blockers,
    warnings,
    checks: {
      paperMode: input.env.TRADING_MODE === "paper",
      liveTradingDisabled: true,
      databaseUp: input.databaseUp,
      discoveryStatus: input.discoveryStatus,
      qualifiedOrManaged: input.qualifiedOrManaged,
      activationProvider: input.env.GROK_ACTIVATION_PROVIDER,
      grokWakeValidated: false,
      grokResearchRequired: input.env.EXPERIMENT_REQUIRES_GROK,
      mcpRoles: tokens,
      missingRoles,
      reference: input.reference,
    },
  };
}

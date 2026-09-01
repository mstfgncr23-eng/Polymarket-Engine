import type { EngineEnv } from "@pm/core";

export type ExperimentActivationMode = "MONITOR_ONLY" | "48H_GROK_PAPER_EXPERIMENT";

export interface PreflightEntryMarket {
  marketId: string;
  asset: string | null;
  referenceRequired: boolean;
  blockedStatus: string | null;
}

export interface PreflightResult {
  ok: boolean;
  blockers: string[];
  warnings: string[];
  checks: Record<string, unknown>;
}

const REQUIRED_ROLES = [
  "market_microstructure_scanner", "crypto_market_scanner", "news_x_intelligence_scanner",
  "fair_probability_analyst", "risk_hedge_analyst", "strategy_supervisor",
] as const;

function isProviderBlocked(status: string | null | undefined): boolean {
  return status === "PROVIDER_BLOCKED" || status === "REGION_BLOCKED";
}

export function evaluateExperimentPreflight(input: {
  env: EngineEnv;
  databaseUp: boolean;
  discoveryStatus: string;
  qualifiedOrManaged: number;
  tokenConfiguredPath?: boolean;
  entryMarkets?: PreflightEntryMarket[];
  reference: Array<{ asset: string; status: string; detail?: string }>;
  mcpRoleTokens: string[];
  bootstrapFailed: boolean;
  mode?: ExperimentActivationMode;
}): PreflightResult {
  const mode = input.mode ?? "MONITOR_ONLY";
  const grokPaper = mode === "48H_GROK_PAPER_EXPERIMENT";
  const blockers: string[] = [];
  const warnings: string[] = [];
  if (input.env.TRADING_MODE !== "paper") blockers.push("TRADING_MODE_NOT_PAPER");
  if (!input.databaseUp) blockers.push("POSTGRES_UNHEALTHY");
  if (input.bootstrapFailed) blockers.push("UNRESOLVED_BOOTSTRAP_FAILURE");
  const hasMarketPath = input.qualifiedOrManaged > 0 || input.tokenConfiguredPath === true;
  if (!hasMarketPath) {
    (grokPaper ? blockers : warnings).push("NO_QUALIFIED_OR_MANAGED_MARKETS");
  }
  const tokens = input.env.GROK_MCP_BEARER_TOKENS.split(",").map((entry) => entry.trim().split(":")[0]).filter(Boolean);
  const missingRoles = REQUIRED_ROLES.filter((role) => !tokens.includes(role));
  const requireGrok = grokPaper || input.env.EXPERIMENT_REQUIRES_GROK;
  if (requireGrok) {
    if (input.env.GROK_ACTIVATION_PROVIDER === "noop") blockers.push("GROK_ACTIVATION_NOOP");
    if (missingRoles.length > 0) blockers.push("GROK_MCP_ROLES_INCOMPLETE");
    if (input.env.GROK_ACTIVATION_PROVIDER === "slack" && (!input.env.SLACK_BOT_TOKEN || !input.env.SLACK_CHANNEL_ID)) {
      blockers.push("SLACK_ACTIVATION_UNCONFIGURED");
    }
  } else if (input.env.GROK_ACTIVATION_PROVIDER === "noop") {
    warnings.push("GROK_WAKE_NOT_VALIDATED");
  }
  const blocked = input.reference.filter((item) => isProviderBlocked(item.status));
  if (blocked.length > 0) warnings.push("REFERENCE_PROVIDER_BLOCKED");
  const entries = input.entryMarkets ?? [];
  if (grokPaper && entries.length > 0) {
    const tradable = entries.filter((entry) => !entry.referenceRequired || !isProviderBlocked(entry.blockedStatus));
    if (tradable.length === 0 && entries.some((entry) => entry.referenceRequired && isProviderBlocked(entry.blockedStatus))) {
      blockers.push("REFERENCE_REGION_BLOCKED");
    }
  }
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
      tokenConfiguredPath: input.tokenConfiguredPath === true,
      activationProvider: input.env.GROK_ACTIVATION_PROVIDER,
      grokWakeValidated: false,
      grokResearchRequired: requireGrok,
      experimentMode: mode,
      mcpRoles: tokens,
      missingRoles,
      reference: input.reference,
      entryMarkets: entries,
    },
  };
}

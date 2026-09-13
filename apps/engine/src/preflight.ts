import type { EngineEnv } from "@pm/core";

export type ExperimentActivationMode = "MONITOR_ONLY" | "48H_GROK_PAPER_EXPERIMENT" | "48H_NATIVE_PAPER_EXPERIMENT" | "48H_ASTRA_PAPER_EXPERIMENT";

export interface PreflightEntryMarket {
  marketId: string;
  asset: string | null;
  referenceRequired: boolean;
  blockedStatus: string | null;
  yesTokenId?: string | null;
  noTokenId?: string | null;
  endDate?: string | null;
  feeScheduleKnown?: boolean;
  binarySafe?: boolean;
  monitored?: boolean;
  /** Provided by the real feed path; synthetic adapters may omit it. */
  referenceFresh?: boolean;
}

export function isGenuineEntryQualifiedMarket(market: PreflightEntryMarket): boolean {
  if (!market.binarySafe) return false;
  if (!market.yesTokenId || !market.noTokenId || market.yesTokenId === market.noTokenId) return false;
  if (!market.asset) return false;
  if (!market.endDate) return false;
  if (market.feeScheduleKnown !== true) return false;
  if (market.monitored !== true) return false;
  if (market.referenceRequired && isProviderBlocked(market.blockedStatus)) return false;
  if (market.referenceRequired && market.referenceFresh === false) return false;
  return true;
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
  overloadFailClosed?: boolean;
}): PreflightResult {
  const mode = input.mode ?? "MONITOR_ONLY";
  const grokPaper = mode === "48H_GROK_PAPER_EXPERIMENT";
  const nativePaper = mode === "48H_NATIVE_PAPER_EXPERIMENT";
  const astraPaper = mode === "48H_ASTRA_PAPER_EXPERIMENT";
  const blockers: string[] = [];
  const warnings: string[] = [];
  if (input.env.TRADING_MODE !== "paper") blockers.push("TRADING_MODE_NOT_PAPER");
  if (!input.databaseUp) blockers.push("POSTGRES_UNHEALTHY");
  if (input.overloadFailClosed) blockers.push("OVERLOAD_FAIL_CLOSED");
  if (input.bootstrapFailed) blockers.push("UNRESOLVED_BOOTSTRAP_FAILURE");
  const entries = input.entryMarkets ?? [];
  const genuineEntries = entries.filter(isGenuineEntryQualifiedMarket);
  if (grokPaper || nativePaper || astraPaper) {
    if (genuineEntries.length === 0) blockers.push("NO_ENTRY_QUALIFIED_MARKET");
  } else {
    const hasMarketPath = genuineEntries.length > 0 || input.qualifiedOrManaged > 0 || input.tokenConfiguredPath === true;
    if (!hasMarketPath) warnings.push("NO_QUALIFIED_OR_MANAGED_MARKETS");
  }
  const tokens = input.env.GROK_MCP_BEARER_TOKENS.split(",").map((entry) => entry.trim().split(":")[0]).filter(Boolean);
  const missingRoles = REQUIRED_ROLES.filter((role) => !tokens.includes(role));
  // Native authority is deliberately independent of the legacy Grok/MCP
  // control plane, even when old compatibility flags remain configured.
  const requireGrok = !nativePaper && !astraPaper && (grokPaper || input.env.EXPERIMENT_REQUIRES_GROK);
  if (requireGrok) {
    if (input.env.GROK_ACTIVATION_PROVIDER === "noop") blockers.push("GROK_ACTIVATION_NOOP");
    // The in-process xAI runtime uses the EngineService binding directly;
    // MCP bearer roles are required only for the Slack/MCP activation path.
    if (input.env.GROK_ACTIVATION_PROVIDER !== "xai" && missingRoles.length > 0) blockers.push("GROK_MCP_ROLES_INCOMPLETE");
    if (input.env.GROK_ACTIVATION_PROVIDER === "slack" && (!input.env.SLACK_BOT_TOKEN || !input.env.SLACK_CHANNEL_ID)) {
      blockers.push("SLACK_ACTIVATION_UNCONFIGURED");
    }
    if (input.env.GROK_ACTIVATION_PROVIDER === "xai" && (!input.env.XAI_API_KEY || !input.env.XAI_MODEL)) {
      blockers.push("XAI_ACTIVATION_UNCONFIGURED");
    }
  } else if (!nativePaper && !astraPaper && input.env.GROK_ACTIVATION_PROVIDER === "noop") {
    warnings.push("GROK_WAKE_NOT_VALIDATED");
  }
  const blocked = input.reference.filter((item) => isProviderBlocked(item.status));
  if (blocked.length > 0) warnings.push("REFERENCE_PROVIDER_BLOCKED");
  if (grokPaper && genuineEntries.length === 0 && entries.length > 0) {
    const tradable = entries.filter((entry) => !entry.referenceRequired || !isProviderBlocked(entry.blockedStatus));
    if (tradable.length === 0 && entries.some((entry) => entry.referenceRequired && isProviderBlocked(entry.blockedStatus))) {
      blockers.push("REFERENCE_REGION_BLOCKED");
    }
  }
  if (grokPaper && genuineEntries.length > 0) {
    const tradable = genuineEntries.filter((entry) => !entry.referenceRequired || !isProviderBlocked(entry.blockedStatus));
    if (tradable.length === 0) blockers.push("REFERENCE_REGION_BLOCKED");
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
      genuineEntryQualified: genuineEntries.length,
      manualTokensMonitorOnly: input.tokenConfiguredPath === true && genuineEntries.length === 0,
      activationProvider: astraPaper ? "openai" : nativePaper ? "native_deterministic" : input.env.GROK_ACTIVATION_PROVIDER,
      decisionAuthority: astraPaper ? "ASTRA_BRAIN_V1" : nativePaper ? "NATIVE_DETERMINISTIC_V1" : "LEGACY_EXTERNAL",
      localExecution: astraPaper ? false : nativePaper,
      externalInference: astraPaper ? true : !nativePaper,
      grokDisabled: astraPaper || nativePaper,
      grokWakeValidated: false,
      grokResearchRequired: requireGrok,
      experimentMode: mode,
      overloadFailClosed: input.overloadFailClosed === true,
      mcpRoles: astraPaper || nativePaper ? [] : tokens,
      missingRoles: astraPaper || nativePaper ? [] : missingRoles,
      reference: input.reference,
      entryMarkets: entries,
    },
  };
}

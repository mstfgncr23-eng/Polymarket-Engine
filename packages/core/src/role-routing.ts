import type { ExternalAgentRoleId, TriggerKind } from "./domain.js";

const MICROSTRUCTURE: readonly TriggerKind[] = [
  "BOOK_IMBALANCE",
  "SPREAD_CHANGE",
  "DEPTH_CHANGE",
  "REPRICING_LAG",
  "PAIR_COST_OPPORTUNITY",
  "EDGE_CHANGE",
  "NEARING_EXPIRY",
];

const CRYPTO_REFERENCE: readonly TriggerKind[] = [
  "BTC_RAPID_MOVE",
  "BTC_VOLATILITY_SPIKE",
  "BTC_VOLUME_SPIKE",
  "CRYPTO_REFERENCE_RAPID_MOVE",
  "CRYPTO_REFERENCE_VOLATILITY_SPIKE",
  "CRYPTO_REFERENCE_VOLUME_SPIKE",
];

const NEWS: readonly TriggerKind[] = ["NEWS_INFORMATION"];

const HEDGE_RISK: readonly TriggerKind[] = [
  "FAILED_HEDGE",
  "DIRECTIONAL_EXPOSURE",
  "LOSS_CLUSTER",
];

const ESCALATION: readonly TriggerKind[] = [
  "SEVERE_DISAGREEMENT",
  "REGIME_ESCALATION",
  "SCHEDULED_REVIEW",
];

const MICROSTRUCTURE_ROLES: readonly ExternalAgentRoleId[] = [
  "market_microstructure_scanner",
  "crypto_market_scanner",
  "fair_probability_analyst",
];

const REFERENCE_ROLES: readonly ExternalAgentRoleId[] = [
  "crypto_market_scanner",
  "fair_probability_analyst",
];

const NEWS_ROLES: readonly ExternalAgentRoleId[] = [
  "news_x_intelligence_scanner",
  "fair_probability_analyst",
];

const HEDGE_ROLES: readonly ExternalAgentRoleId[] = ["risk_hedge_analyst"];

const SUPERVISOR_ROLES: readonly ExternalAgentRoleId[] = ["strategy_supervisor"];

export function rolesForTriggerKind(kind: TriggerKind, severity: 0 | 1 | 2 | 3 = 1): ExternalAgentRoleId[] {
  const roles = new Set<ExternalAgentRoleId>();
  if ((MICROSTRUCTURE as readonly string[]).includes(kind)) MICROSTRUCTURE_ROLES.forEach((role) => roles.add(role));
  if ((CRYPTO_REFERENCE as readonly string[]).includes(kind)) REFERENCE_ROLES.forEach((role) => roles.add(role));
  if ((NEWS as readonly string[]).includes(kind)) NEWS_ROLES.forEach((role) => roles.add(role));
  if ((HEDGE_RISK as readonly string[]).includes(kind)) HEDGE_ROLES.forEach((role) => roles.add(role));
  if ((ESCALATION as readonly string[]).includes(kind) || severity >= 3) SUPERVISOR_ROLES.forEach((role) => roles.add(role));
  return [...roles];
}

export function isCryptoReferenceKind(kind: TriggerKind): boolean {
  return (CRYPTO_REFERENCE as readonly string[]).includes(kind);
}

export function isMicrostructureKind(kind: TriggerKind): boolean {
  return (MICROSTRUCTURE as readonly string[]).includes(kind);
}

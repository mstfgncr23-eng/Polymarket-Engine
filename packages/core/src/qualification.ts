import { d } from "./calculations.js";
import { qualificationConfigSchema, type QualificationConfig } from "./config.js";
import type { FeeSchedule, QualificationReasonCode } from "./domain.js";

export interface QualificationMarketInput {
  id: string;
  slug: string;
  title: string;
  tags: readonly string[];
  active: boolean;
  closed: boolean;
  acceptingOrders: boolean;
  endDate: string | null;
  yesTokenId: string | null;
  noTokenId: string | null;
  yesPrice: string | null;
  noPrice: string | null;
  liquidityUsd: string | null;
  spread: number | null;
  feeSchedule: FeeSchedule | null;
  feesUnknown: boolean;
  yesLabel?: string | null;
  noLabel?: string | null;
}

export interface BinaryNormalization {
  safe: boolean;
  yesOutcome: "UP" | "DOWN" | null;
  noOutcome: "UP" | "DOWN" | null;
  reason: QualificationReasonCode | "QUALIFIED";
}

export interface QualificationResult {
  qualified: boolean;
  reasonCodes: QualificationReasonCode[];
  asset: string | null;
  crypto: boolean;
  binary: BinaryNormalization;
  duplicateOf: string | null;
}

const CRYPTO_CATEGORY_TAG = /^(crypto|cryptocurrency|crypto-prices|crypto-price|altcoins|altcoin|defi|web3|digital-assets|digital-asset)$/i;
const DIRECTIONAL_SLUG = /(?:^|-)([a-z0-9]{2,16})-(?:updown|up-or-down|updown-\d+m)(?:-|$)/i;
const DIRECTIONAL_TEXT = /\b(up\s*or\s*down|updown|higher\s+or\s+lower)\b/i;

export function extractUnderlyingAsset(slug: string, title: string, tags: readonly string[]): string | null {
  const slugMatch = slug.match(DIRECTIONAL_SLUG);
  if (slugMatch?.[1]) return slugMatch[1].toUpperCase();
  const tagged = tags.map((tag) => tag.trim()).find((tag) => /^[A-Z0-9]{2,12}$/.test(tag) && !CRYPTO_CATEGORY_TAG.test(tag));
  if (tagged) return tagged.toUpperCase();
  return null;
}

export function isCryptoCategory(tags: readonly string[], slug: string, title: string): boolean {
  if (tags.some((tag) => CRYPTO_CATEGORY_TAG.test(tag))) return true;
  if (DIRECTIONAL_SLUG.test(slug)) return true;
  if (DIRECTIONAL_TEXT.test(`${slug} ${title}`) && extractUnderlyingAsset(slug, title, tags)) return true;
  return false;
}

function labeledOutcome(label: string | null | undefined): "UP" | "DOWN" | null {
  const value = (label ?? "").trim().toLowerCase();
  if (value === "up" || value === "higher") return "UP";
  if (value === "down" || value === "lower") return "DOWN";
  return null;
}

export function normalizeBinarySemantics(input: QualificationMarketInput): BinaryNormalization {
  if (!input.yesTokenId || !input.noTokenId) {
    return { safe: false, yesOutcome: null, noOutcome: null, reason: "MISSING_TOKEN_IDS" };
  }
  const yesFromLabel = labeledOutcome(input.yesLabel);
  const noFromLabel = labeledOutcome(input.noLabel);
  if (yesFromLabel && noFromLabel && yesFromLabel !== noFromLabel) {
    return { safe: true, yesOutcome: yesFromLabel, noOutcome: noFromLabel, reason: "QUALIFIED" };
  }
  const text = `${input.slug} ${input.title}`;
  const directional = DIRECTIONAL_TEXT.test(text) || DIRECTIONAL_SLUG.test(input.slug);
  const unlabeledOrYesNo = !input.yesLabel && !input.noLabel
    || /^(yes)?$/i.test((input.yesLabel ?? "").trim()) && /^(no)?$/i.test((input.noLabel ?? "").trim());
  if (directional && unlabeledOrYesNo) {
    return { safe: true, yesOutcome: "UP", noOutcome: "DOWN", reason: "QUALIFIED" };
  }
  return { safe: false, yesOutcome: null, noOutcome: null, reason: "UNSUPPORTED_BINARY_SEMANTICS" };
}

export function qualifyMarket(
  input: QualificationMarketInput,
  seen: ReadonlyMap<string, string>,
  now = new Date(),
  configInput: Partial<QualificationConfig> = {},
): QualificationResult {
  const config = qualificationConfigSchema.parse(configInput);
  const reasonCodes: QualificationReasonCode[] = [];
  const crypto = isCryptoCategory(input.tags, input.slug, input.title);
  const asset = extractUnderlyingAsset(input.slug, input.title, input.tags);
  const binary = normalizeBinarySemantics(input);

  if (!crypto) reasonCodes.push("NOT_CRYPTO");
  if (config.requireUnderlyingAsset && !asset) reasonCodes.push("UNKNOWN_UNDERLYING_ASSET");
  if (input.closed || !input.active) reasonCodes.push(input.closed ? "MARKET_CLOSED" : "MARKET_INACTIVE");
  if (!input.acceptingOrders) reasonCodes.push("MARKET_INACTIVE");
  if (!input.yesTokenId || !input.noTokenId) reasonCodes.push("MISSING_TOKEN_IDS");
  if (!binary.safe) reasonCodes.push(binary.reason === "MISSING_TOKEN_IDS" ? "MISSING_TOKEN_IDS" : "UNSUPPORTED_BINARY_SEMANTICS");
  if (input.feesUnknown || input.feeSchedule === null) reasonCodes.push("UNKNOWN_FEE_SCHEDULE");

  if (input.endDate) {
    const remaining = new Date(input.endDate).getTime() - now.getTime();
    if (!Number.isFinite(remaining) || remaining < config.minRemainingDurationMs || remaining > config.maxRemainingDurationMs) {
      reasonCodes.push("DURATION_OUT_OF_RANGE");
    }
  } else {
    reasonCodes.push("DURATION_OUT_OF_RANGE");
  }

  if (input.liquidityUsd !== null && d(input.liquidityUsd).lt(config.minLiquidityUsd)) reasonCodes.push("INSUFFICIENT_LIQUIDITY");
  if (input.spread !== null && input.spread > config.maxSpread) reasonCodes.push("SPREAD_TOO_WIDE");

  const yes = input.yesPrice === null ? null : Number(input.yesPrice);
  const no = input.noPrice === null ? null : Number(input.noPrice);
  if ((yes !== null && (!Number.isFinite(yes) || yes < 0 || yes > 1)) || (no !== null && (!Number.isFinite(no) || no < 0 || no > 1))) {
    reasonCodes.push("INVALID_PRICE");
  }

  const tokenKey = [input.yesTokenId, input.noTokenId].filter(Boolean).sort().join(":");
  const duplicateOf = tokenKey && seen.has(tokenKey) ? seen.get(tokenKey) ?? null : seen.get(input.id) ?? null;
  if (duplicateOf && duplicateOf !== input.id) reasonCodes.push("DUPLICATE_MARKET");

  const unique = [...new Set(reasonCodes)];
  return {
    qualified: unique.length === 0,
    reasonCodes: unique.length === 0 ? ["QUALIFIED"] : unique,
    asset,
    crypto,
    binary,
    duplicateOf,
  };
}

export function impliedSpread(yesPrice: string | null, noPrice: string | null): number | null {
  if (yesPrice === null || noPrice === null) return null;
  const total = d(yesPrice).plus(noPrice);
  if (total.lte(0)) return null;
  return d(1).minus(total).abs().toNumber();
}

import { isBookDerived, type BookDerived, type CompactBookLevel } from "@pm/core";

export const MARKET_DERIVATION_WORKER_READY = "ready" as const;
export const MARKET_DERIVATION_WORKER_DERIVE = "derive" as const;
export const MARKET_DERIVATION_WORKER_DERIVED = "derived" as const;
export const MARKET_DERIVATION_WORKER_ERROR = "error" as const;

export type MarketDerivationWorkerMode =
  | "throw-on-start"
  | "exit-on-start"
  | "throw-on-message"
  | "malformed"
  | "hang"
  | "hang-on-start";

export interface MarketDerivationWorkerData {
  mode?: MarketDerivationWorkerMode;
  delayMs?: number;
}

export interface MarketDerivationRequest {
  type: typeof MARKET_DERIVATION_WORKER_DERIVE;
  id: number;
  bids: CompactBookLevel[];
  asks: CompactBookLevel[];
}

export interface MarketDerivationReady {
  type: typeof MARKET_DERIVATION_WORKER_READY;
}

export interface MarketDerivationSuccess {
  type: typeof MARKET_DERIVATION_WORKER_DERIVED;
  id: number;
  derived: BookDerived;
}

export interface MarketDerivationFailure {
  type: typeof MARKET_DERIVATION_WORKER_ERROR;
  id: number;
  message: string;
}

export type MarketDerivationResponse =
  | MarketDerivationReady
  | MarketDerivationSuccess
  | MarketDerivationFailure;

export function isMarketDerivationRequest(value: unknown): value is MarketDerivationRequest {
  if (!value || typeof value !== "object") return false;
  const record = value as Record<string, unknown>;
  return record.type === MARKET_DERIVATION_WORKER_DERIVE
    && typeof record.id === "number"
    && Number.isInteger(record.id)
    && Array.isArray(record.bids)
    && Array.isArray(record.asks);
}

export function isMarketDerivationSuccess(value: unknown): value is MarketDerivationSuccess {
  if (!value || typeof value !== "object") return false;
  const record = value as Record<string, unknown>;
  return record.type === MARKET_DERIVATION_WORKER_DERIVED
    && typeof record.id === "number"
    && isBookDerived(record.derived);
}

export function isMarketDerivationFailure(value: unknown): value is MarketDerivationFailure {
  if (!value || typeof value !== "object") return false;
  const record = value as Record<string, unknown>;
  return record.type === MARKET_DERIVATION_WORKER_ERROR
    && typeof record.id === "number"
    && typeof record.message === "string";
}

export function isMarketDerivationReady(value: unknown): value is MarketDerivationReady {
  return Boolean(value) && typeof value === "object" && (value as { type?: unknown }).type === MARKET_DERIVATION_WORKER_READY;
}

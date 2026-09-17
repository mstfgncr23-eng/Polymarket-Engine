import {
  appendFileSync,
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  renameSync,
  truncateSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";
import { PaperLedger } from "./accounting.js";
import type { LedgerState, MarketSnapshot, Outcome, PaperFill, PaperOrder, Position } from "./domain.js";
import { executePaperTaker } from "./execution.js";
import type { LearningSettlementLabel } from "./learning-sample.js";
import { isAuthoritativeBinaryMapping, type BinarySettlementLookup } from "./settlement.js";
import { DEFAULT_LEARNING_DATA_DIR, DEFAULT_LEARNING_MAX_BYTES } from "./learning-store.js";

export const NATIVE_V2_COUNTERFACTUAL_MANIFEST_VERSION = 1 as const;
export const NATIVE_V2_COUNTERFACTUAL_STATE_BASIS = "DECISION_TIME_V2_COUNTERFACTUAL_STATE" as const;
export const DEFAULT_COUNTERFACTUAL_SEGMENT_MAX_BYTES = 8_000_000;
/** Total CF event-log byte cap — same operational default as learning volume contract. */
export const DEFAULT_COUNTERFACTUAL_MAX_BYTES = DEFAULT_LEARNING_MAX_BYTES;

/**
 * @deprecated Prefer {@link counterfactualSettlementObligationBytes} — per-open
 * reservation is the exact serialized identity cost + clamped non-identity overhead,
 * not a fixed global constant. Kept as a soft documentation ceiling for typical
 * ASCII marketIds; worst-case JSON-escaped idSchema values exceed this.
 */
export const MAX_COUNTERFACTUAL_SETTLEMENT_ROW_BYTES = 1024;

/**
 * Max UTF-8 bytes of the JSON string literal (including quotes / escapes) retained
 * for settlement source metadata (non-identity). Char-count clamps are NOT byte-safe.
 */
export const MAX_COUNTERFACTUAL_SETTLEMENT_SOURCE_JSON_BYTES = 66;
/** Max UTF-8 JSON string literal bytes for settlement reason metadata (non-identity). */
export const MAX_COUNTERFACTUAL_SETTLEMENT_REASON_JSON_BYTES = 66;
/** Max UTF-8 JSON string literal bytes for settlement decimal delta strings (non-identity). */
export const MAX_COUNTERFACTUAL_SETTLEMENT_DECIMAL_JSON_BYTES = 66;
/** Fixed ISO-8601 millisecond timestamp JSON string literal byte budget (incl. quotes). */
export const COUNTERFACTUAL_SETTLEMENT_AT_JSON_BYTES = 26;

/** @deprecated Use MAX_COUNTERFACTUAL_SETTLEMENT_SOURCE_JSON_BYTES */
export const MAX_COUNTERFACTUAL_SETTLEMENT_SOURCE_CHARS = 64;
/** @deprecated Use MAX_COUNTERFACTUAL_SETTLEMENT_REASON_JSON_BYTES */
export const MAX_COUNTERFACTUAL_SETTLEMENT_REASON_CHARS = 64;
/** @deprecated Use MAX_COUNTERFACTUAL_SETTLEMENT_DECIMAL_JSON_BYTES */
export const MAX_COUNTERFACTUAL_SETTLEMENT_DECIMAL_CHARS = 64;
/** @deprecated Use COUNTERFACTUAL_SETTLEMENT_AT_JSON_BYTES */
export const COUNTERFACTUAL_SETTLEMENT_AT_CHARS = 24;

export type NativeV2CounterfactualIntent = "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG";

export type CounterfactualRecoveryStatus = "not_initialized" | "ready" | "unavailable";

export function nativeV2CounterfactualActionKey(
  marketId: string,
  outcome: Outcome,
  intent: NativeV2CounterfactualIntent = "OPEN_FIRST_LEG",
): string {
  return `${marketId}:${outcome}:${intent}`;
}

export function counterfactualDataDir(learningDataDir = DEFAULT_LEARNING_DATA_DIR): string {
  return path.join(learningDataDir, "counterfactual");
}

/**
 * Exact UTF-8 byte length of `marketId` as a JSON string literal (including quotes
 * and any JSON escapes). This is the durable identity cost of a settlement row.
 */
export function counterfactualSettlementIdentityJsonBytes(marketId: string): number {
  return Buffer.byteLength(JSON.stringify(marketId), "utf8");
}

/**
 * Settlement capacity obligation for one open CF market: exact serialized identity
 * byte cost + bounded non-identity overhead (clamped source/reason/deltas + fixed at).
 * Summed across actual open market IDs on fill admission and reconstructed on restart.
 */
export function counterfactualSettlementObligationBytes(marketId: string): number {
  // Worst-case non-identity payloads that still fit the JSON-byte clamps.
  const row = buildBoundedCounterfactualSettlementRow({
    marketId,
    upPayoff: "1",
    downPayoff: "0",
    cashDelta: "9".repeat(256),
    realizedDelta: "9".repeat(256),
    at: "2".repeat(64),
    source: "x".repeat(256),
    reason: "y".repeat(256),
  });
  return Buffer.byteLength(`${JSON.stringify(row)}\n`, "utf8");
}

export interface NativeV2CounterfactualManifest {
  manifestVersion: typeof NATIVE_V2_COUNTERFACTUAL_MANIFEST_VERSION;
  createdAt: string;
  updatedAt: string;
  startingBalance: string;
  fillCount: number;
  settlementCount: number;
  openPositionCount: number;
  realizedPnl: string;
  fees: string;
  segmentCount: number;
  bytes: number;
  maxBytes: number;
  degraded: boolean;
  lastError: string | null;
  lastErrorAt: string | null;
  recoveryStatus: CounterfactualRecoveryStatus;
}

export interface NativeV2CounterfactualSummary {
  dataDir: string;
  startingBalance: string;
  availableCashUsd: string;
  cash: string;
  reservedCash: string;
  realizedPnl: string;
  fees: string;
  openPositionCount: number;
  fillCount: number;
  settlementCount: number;
  actionKeyCount: number;
  settledMarketCount: number;
  bytes: number;
  maxBytes: number;
  degraded: boolean;
  lastError: string | null;
  lastFillAt: string | null;
  recoveryStatus: CounterfactualRecoveryStatus;
  recoveryReady: boolean;
}

export interface NativeV2CounterfactualDecisionState {
  stateBasis: typeof NATIVE_V2_COUNTERFACTUAL_STATE_BASIS;
  positions: Position[];
  ledger: LedgerState;
  availableCashUsd: string;
  lastStrategyTradeAt: string | null;
  recoveryReady: boolean;
  recoveryStatus: CounterfactualRecoveryStatus;
}

export interface CounterfactualWouldSubmitInput {
  actionKey: string;
  order: PaperOrder;
  snapshot: MarketSnapshot;
  /** Paper taker latency model; Phase-4 shadow uses decision-time book (0 is valid). */
  measuredLatencyMs?: number;
  at?: Date;
  /**
   * Authoritative settlement lookup captured at accept time. Required for
   * restart-safe official resolution when the market leaves active discovery.
   * Do not invent YES/NO mappings later.
   */
  settlementLookup?: BinarySettlementLookup;
}

export type CounterfactualCommitResult =
  | { applied: true; fill: PaperFill; actionKey: string }
  | {
    applied: false;
    reason:
      | "DUPLICATE_ACTION"
      | "NO_ORDER"
      | "EXECUTION_ERROR"
      | "CAPACITY_EXCEEDED"
      | "DISABLED"
      | "RECOVERY_UNAVAILABLE"
      | "MARKET_SETTLED"
      | "MISSING_SETTLEMENT_LOOKUP";
    detail?: string;
  };

export type CounterfactualSettlementResult =
  | { applied: true; marketId: string; cashDelta: string; realizedDelta: string }
  | {
    applied: false;
    reason:
      | "NO_POSITION"
      | "UNRESOLVED_LABEL"
      | "EXECUTION_ERROR"
      | "CAPACITY_EXCEEDED"
      | "DISABLED"
      | "RECOVERY_UNAVAILABLE";
    detail?: string;
  };

type SegmentRow =
  | {
    kind: "fill";
    actionKey: string;
    fill: PaperFill;
    at: string;
    settlementLookup?: BinarySettlementLookup;
  }
  | {
    kind: "settlement";
    marketId: string;
    upPayoff: "0" | "1";
    downPayoff: "0" | "1";
    cashDelta: string;
    realizedDelta: string;
    at: string;
    source: string;
    reason: string | null;
  };

/**
 * Truncate a string so JSON.stringify(value) UTF-8 byte length stays within maxJsonBytes
 * (including the surrounding quotes and any JSON escapes). Escape-aware / UTF-8 safe.
 */
export function clampJsonStringUtf8Bytes(value: string, maxJsonBytes: number): string {
  if (!Number.isFinite(maxJsonBytes) || maxJsonBytes < 2) return "";
  if (Buffer.byteLength(JSON.stringify(value), "utf8") <= maxJsonBytes) return value;
  let lo = 0;
  let hi = value.length;
  while (lo < hi) {
    const mid = Math.ceil((lo + hi) / 2);
    if (Buffer.byteLength(JSON.stringify(value.slice(0, mid)), "utf8") <= maxJsonBytes) lo = mid;
    else hi = mid - 1;
  }
  return value.slice(0, lo);
}

/**
 * Build a settlement row with exact marketId identity and byte-bounded non-identity metadata.
 * Row byte size is identity-driven (JSON-escaped marketId) + bounded overhead — never
 * truncate marketId. Non-identity fields are clamped to JSON UTF-8 byte budgets.
 */
export function buildBoundedCounterfactualSettlementRow(input: {
  marketId: string;
  upPayoff: "0" | "1";
  downPayoff: "0" | "1";
  cashDelta: string;
  realizedDelta: string;
  at: string;
  source: string;
  reason: string | null;
}): Extract<SegmentRow, { kind: "settlement" }> {
  const marketId = input.marketId;
  const source = clampJsonStringUtf8Bytes(input.source, MAX_COUNTERFACTUAL_SETTLEMENT_SOURCE_JSON_BYTES);
  const reason =
    input.reason === null
      ? null
      : clampJsonStringUtf8Bytes(input.reason, MAX_COUNTERFACTUAL_SETTLEMENT_REASON_JSON_BYTES);
  const cashDelta = clampJsonStringUtf8Bytes(input.cashDelta, MAX_COUNTERFACTUAL_SETTLEMENT_DECIMAL_JSON_BYTES);
  const realizedDelta = clampJsonStringUtf8Bytes(input.realizedDelta, MAX_COUNTERFACTUAL_SETTLEMENT_DECIMAL_JSON_BYTES);
  const at = clampJsonStringUtf8Bytes(input.at, COUNTERFACTUAL_SETTLEMENT_AT_JSON_BYTES);

  return {
    kind: "settlement",
    marketId,
    upPayoff: input.upPayoff,
    downPayoff: input.downPayoff,
    cashDelta,
    realizedDelta,
    at,
    source,
    reason,
  };
}

export interface NativeV2CounterfactualStoreOptions {
  dataDir?: string;
  startingBalance?: string | number;
  segmentMaxBytes?: number;
  maxBytes?: number;
  enabled?: boolean;
  /** When false, keep in-memory state only (still isolated from V1). */
  durable?: boolean;
}

/**
 * Isolated Native V2 counterfactual portfolio.
 *
 * Reuses PaperLedger + executePaperTaker + settlement math. Never mutates the
 * authoritative V1 paper ledger, reserved cash, or execution authority.
 *
 * Durable recovery is fail-closed: corrupt/unreadable economic rows, startingBalance
 * mismatch, or unsafe maxBytes decrease mark CF recovery unavailable and reject
 * further CF mutation until clean reinit/restart. V1 remains fail-soft elsewhere.
 */
export class NativeV2CounterfactualStore {
  readonly dataDir: string;
  readonly enabled: boolean;
  readonly durable: boolean;
  readonly segmentMaxBytes: number;

  /** Constructor-requested starting balance (compared to durable manifest on recovery). */
  private readonly requestedStartingBalance: string;
  /** Constructor-requested maxBytes (merged with durable rules on recovery). */
  private readonly requestedMaxBytes: number;

  /** Authoritative durable starting balance after successful recovery / first persist. */
  private startingBalanceValue: string;
  private maxBytesValue: number;

  private readonly ledger: PaperLedger;
  private readonly actionKeys = new Set<string>();
  /** Terminal settled-market tombstones (reconstructed from durable settlement rows). */
  private readonly settledMarketIds = new Set<string>();
  /** Authoritative settlement lookup metadata captured at CF fill accept time. */
  private readonly settlementLookups = new Map<string, BinarySettlementLookup>();
  private fillCount = 0;
  private settlementCount = 0;
  private lastFillAt: string | null = null;
  private initialized = false;
  private recoveryStatus: CounterfactualRecoveryStatus = "not_initialized";
  private degraded = false;
  private lastError: string | null = null;
  private lastErrorAt: string | null = null;
  private currentSegmentPath: string | null = null;
  private currentSegmentBytes = 0;
  private currentSegmentIndex = 0;
  private bytes = 0;
  private segmentCount = 0;
  private createdAt: string;
  /** Test-only: next durable segment append throws once, then clears. */
  private appendFaultForTest: Error | null = null;

  constructor(options: NativeV2CounterfactualStoreOptions = {}) {
    this.enabled = options.enabled !== false;
    this.durable = options.durable !== false;
    this.dataDir = options.dataDir ?? counterfactualDataDir();
    this.requestedStartingBalance = String(options.startingBalance ?? 1000);
    this.startingBalanceValue = this.requestedStartingBalance;
    this.segmentMaxBytes = Number.isFinite(options.segmentMaxBytes) && (options.segmentMaxBytes ?? 0) > 0
      ? Math.trunc(options.segmentMaxBytes!)
      : DEFAULT_COUNTERFACTUAL_SEGMENT_MAX_BYTES;
    this.requestedMaxBytes = Number.isFinite(options.maxBytes) && (options.maxBytes ?? 0) > 0
      ? Math.trunc(options.maxBytes!)
      : DEFAULT_COUNTERFACTUAL_MAX_BYTES;
    this.maxBytesValue = this.requestedMaxBytes;
    this.createdAt = new Date().toISOString();
    this.ledger = new PaperLedger(this.startingBalanceValue, new Date(this.createdAt));
  }

  get startingBalance(): string {
    return this.startingBalanceValue;
  }

  get maxBytes(): number {
    return this.maxBytesValue;
  }

  /**
   * Rebuild / open durable segments.
   * Fail-closed for CF: untrustworthy durable recovery marks recovery unavailable
   * and rejects subsequent CF mutation. Never throws into the engine trading path.
   */
  init(): void {
    if (!this.enabled || this.initialized) return;
    try {
      if (this.durable) this.rebuildFromDisk();
      this.recoveryStatus = "ready";
      this.initialized = true;
    } catch (error) {
      this.markRecoveryUnavailable(error);
      this.initialized = true;
    }
  }

  isRecoveryReady(): boolean {
    this.init();
    return this.enabled && this.recoveryStatus === "ready";
  }

  getRecoveryStatus(): CounterfactualRecoveryStatus {
    this.init();
    return this.recoveryStatus;
  }

  /** Decision-time portfolio view for Phase-4 shadow risk/execution (V2-only). */
  decisionState(): NativeV2CounterfactualDecisionState {
    this.init();
    const ready = this.recoveryStatus === "ready";
    // When recovery is unavailable, do not present partial/corrupt state as authority.
    if (!ready) {
      const empty = new PaperLedger(this.startingBalanceValue, new Date(this.createdAt));
      return {
        stateBasis: NATIVE_V2_COUNTERFACTUAL_STATE_BASIS,
        positions: [],
        ledger: empty.snapshot(),
        availableCashUsd: empty.availableCash().toFixed(),
        lastStrategyTradeAt: null,
        recoveryReady: false,
        recoveryStatus: this.recoveryStatus,
      };
    }
    return {
      stateBasis: NATIVE_V2_COUNTERFACTUAL_STATE_BASIS,
      positions: structuredClone([...this.ledger.positions.values()]),
      ledger: this.ledger.snapshot(),
      availableCashUsd: this.ledger.availableCash().toFixed(),
      lastStrategyTradeAt: this.lastFillAt,
      recoveryReady: true,
      recoveryStatus: this.recoveryStatus,
    };
  }

  hasAction(actionKey: string): boolean {
    this.init();
    if (this.recoveryStatus !== "ready") return false;
    return this.actionKeys.has(actionKey);
  }

  hasUnpairedPosition(marketId: string): boolean {
    this.init();
    if (this.recoveryStatus !== "ready") return false;
    return Boolean(this.ledger.positions.get(marketId)?.unpairedOutcome);
  }

  hasSettledMarket(marketId: string): boolean {
    this.init();
    if (this.recoveryStatus !== "ready") return false;
    return this.settledMarketIds.has(marketId);
  }

  openPositionMarketIds(): string[] {
    this.init();
    if (this.recoveryStatus !== "ready") return [];
    return [...this.ledger.positions.keys()]
      .filter((marketId) => {
        const position = this.ledger.positions.get(marketId);
        if (!position) return false;
        return Number(position.upQuantity) !== 0 || Number(position.downQuantity) !== 0;
      })
      .sort();
  }

  /**
   * Restart-safe settlement lookup for an open (or previously accepted) CF market.
   * Returns null when no authoritative mapping was durably captured — callers must
   * NOT invent YES/NO → UP/DOWN mappings.
   */
  settlementLookupFor(marketId: string): BinarySettlementLookup | null {
    this.init();
    if (this.recoveryStatus !== "ready") return null;
    return this.settlementLookups.get(marketId) ?? null;
  }

  /** Open CF markets that still have durable settlement lookup metadata. */
  openSettlementLookups(): BinarySettlementLookup[] {
    this.init();
    if (this.recoveryStatus !== "ready") return [];
    return this.openPositionMarketIds()
      .map((marketId) => this.settlementLookups.get(marketId))
      .filter((row): row is BinarySettlementLookup => row != null);
  }

  settledMarketIdsForTest(): string[] {
    this.init();
    return [...this.settledMarketIds].sort();
  }

  /**
   * Advance CF state only for a real would-submit under the shadow policy.
   * Duplicate action keys are idempotent (no second fill).
   * Settled markets are terminal (V1-style) — never reopen.
   */
  tryCommitWouldSubmit(input: CounterfactualWouldSubmitInput): CounterfactualCommitResult {
    if (!this.enabled) return { applied: false, reason: "DISABLED" };
    this.init();
    if (this.recoveryStatus !== "ready") {
      return {
        applied: false,
        reason: "RECOVERY_UNAVAILABLE",
        detail: this.lastError ?? "CF_RECOVERY_UNAVAILABLE",
      };
    }
    if (this.settledMarketIds.has(input.order.marketId)) {
      return {
        applied: false,
        reason: "MARKET_SETTLED",
        detail: `CF market ${input.order.marketId} is terminally settled`,
      };
    }
    if (this.actionKeys.has(input.actionKey)) {
      return { applied: false, reason: "DUPLICATE_ACTION" };
    }
    // Re-evaluations against an already-open unpaired CF book are not new actions.
    if (this.hasUnpairedPosition(input.order.marketId)) {
      return { applied: false, reason: "DUPLICATE_ACTION", detail: "EXISTING_UNPAIRED_POSITION" };
    }
    if (!input.order) return { applied: false, reason: "NO_ORDER" };

    // Durable CF fills require restart-safe authoritative settlement lookup at accept time.
    // Do not invent YES→UP / NO→DOWN after discovery drops the market.
    if (this.durable) {
      const lookup = input.settlementLookup;
      if (
        !lookup
        || lookup.marketId !== input.order.marketId
        || !isAuthoritativeBinaryMapping(lookup.yesMapsTo, lookup.noMapsTo)
      ) {
        return {
          applied: false,
          reason: "MISSING_SETTLEMENT_LOOKUP",
          detail: "CF durable would-submit requires authoritative settlementLookup at accept time",
        };
      }
    }

    try {
      const fill = executePaperTaker({
        order: input.order,
        snapshotAfterLatency: input.snapshot,
        measuredLatencyMs: input.measuredLatencyMs ?? 0,
      });
      const at = input.at ?? new Date(fill.timestamp);
      // Stage on a temporary ledger first so durable failure cannot diverge memory.
      const staged = this.cloneLedger();
      staged.applyFill(fill);

      if (this.durable) {
        const row: SegmentRow = {
          kind: "fill",
          actionKey: input.actionKey,
          fill: structuredClone(fill),
          at: at.toISOString(),
          ...(input.settlementLookup
            ? { settlementLookup: structuredClone(input.settlementLookup) }
            : {}),
        };
        try {
          this.appendRow(row, "fill", {
            openMarketIdsAfter: [...staged.positions.keys()].filter((marketId) => {
              const position = staged.positions.get(marketId);
              if (!position) return false;
              return Number(position.upQuantity) !== 0 || Number(position.downQuantity) !== 0;
            }),
          });
        } catch (error) {
          // Economically required would-submit could not be durably appended — fail CF closed
          // for subsequent shadow decisions. V1 remains unaffected. Durable log unchanged.
          this.markRecoveryUnavailable(error);
          const message = error instanceof Error ? error.message : String(error);
          return {
            applied: false,
            reason: message.includes("CAPACITY_EXCEEDED") ? "CAPACITY_EXCEEDED" : "EXECUTION_ERROR",
            detail: message,
          };
        }
        // Durable event is authoritative — apply memory, then best-effort manifest.
        this.applyFillInternal(input.actionKey, fill, at, false);
        if (input.settlementLookup) {
          this.settlementLookups.set(input.settlementLookup.marketId, structuredClone(input.settlementLookup));
        }
        try {
          this.persistManifest();
        } catch (error) {
          // Manifest failure AFTER durable segment append must NOT roll back the economic event
          // or unnecessarily invalidate a recoverable economic log.
          this.noteFailure(error);
        }
      } else {
        this.applyFillInternal(input.actionKey, fill, at, false);
        if (input.settlementLookup) {
          this.settlementLookups.set(input.settlementLookup.marketId, structuredClone(input.settlementLookup));
        }
      }
      return { applied: true, fill, actionKey: input.actionKey };
    } catch (error) {
      this.noteFailure(error);
      return {
        applied: false,
        reason: "EXECUTION_ERROR",
        detail: error instanceof Error ? error.message : String(error),
      };
    }
  }

  trySettleMarket(
    marketId: string,
    upPayoff: "0" | "1",
    downPayoff: "0" | "1",
    meta: { at?: Date; source?: string; reason?: string | null } = {},
  ): CounterfactualSettlementResult {
    if (!this.enabled) return { applied: false, reason: "DISABLED" };
    this.init();
    if (this.recoveryStatus !== "ready") {
      return {
        applied: false,
        reason: "RECOVERY_UNAVAILABLE",
        detail: this.lastError ?? "CF_RECOVERY_UNAVAILABLE",
      };
    }
    if (!this.ledger.positions.has(marketId)) {
      return { applied: false, reason: "NO_POSITION" };
    }
    const at = meta.at ?? new Date();
    // Stage settlement on a temporary ledger; only mutate live state after durable append.
    const staged = this.cloneLedger();
    const settled = staged.settlePosition(marketId, upPayoff, downPayoff, at);
    if (!settled) return { applied: false, reason: "NO_POSITION" };

    if (this.durable) {
      const row = buildBoundedCounterfactualSettlementRow({
        marketId,
        upPayoff,
        downPayoff,
        cashDelta: settled.cashDelta,
        realizedDelta: settled.realizedDelta,
        at: at.toISOString(),
        source: meta.source ?? "counterfactual_settlement",
        reason: meta.reason ?? null,
      });
      try {
        this.appendRow(row, "settlement");
      } catch (error) {
        this.noteFailure(error);
        const message = error instanceof Error ? error.message : String(error);
        return {
          applied: false,
          reason: message.includes("CAPACITY_EXCEEDED") ? "CAPACITY_EXCEEDED" : "EXECUTION_ERROR",
          detail: message,
        };
      }
      const live = this.ledger.settlePosition(marketId, upPayoff, downPayoff, at);
      if (!live) {
        // Durable settlement exists; memory must match — force-apply via restore from staged.
        this.ledger.restore(staged.snapshot(), [...staged.positions.values()]);
      }
      this.settlementCount += 1;
      this.settledMarketIds.add(marketId);
      this.settlementLookups.delete(marketId);
      this.clearActionKeysForMarket(marketId);
      try {
        this.persistManifest();
      } catch (error) {
        // Manifest failure AFTER durable segment append must NOT roll back the economic event.
        this.noteFailure(error);
      }
      return {
        applied: true,
        marketId,
        cashDelta: settled.cashDelta,
        realizedDelta: settled.realizedDelta,
      };
    }

    const live = this.ledger.settlePosition(marketId, upPayoff, downPayoff, at);
    if (!live) return { applied: false, reason: "NO_POSITION" };
    this.settlementCount += 1;
    this.settledMarketIds.add(marketId);
    this.settlementLookups.delete(marketId);
    this.clearActionKeysForMarket(marketId);
    return {
      applied: true,
      marketId,
      cashDelta: live.cashDelta,
      realizedDelta: live.realizedDelta,
    };
  }

  trySettleFromLearningLabel(
    marketId: string,
    label: LearningSettlementLabel,
    at: Date = new Date(),
  ): CounterfactualSettlementResult {
    if (label.status !== "resolved" || !label.outcome) {
      return { applied: false, reason: "UNRESOLVED_LABEL" };
    }
    const upPayoff: "0" | "1" = label.outcome === "UP" ? "1" : "0";
    const downPayoff: "0" | "1" = label.outcome === "DOWN" ? "1" : "0";
    return this.trySettleMarket(marketId, upPayoff, downPayoff, {
      at,
      source: label.source ?? "learning_settlement_label",
      reason: label.reason,
    });
  }

  summary(): NativeV2CounterfactualSummary {
    this.init();
    return this.buildSummary();
  }

  getManifest(): NativeV2CounterfactualManifest {
    this.init();
    return this.buildManifest();
  }

  private buildSummary(): NativeV2CounterfactualSummary {
    const ledger = this.ledger.snapshot();
    const ready = this.recoveryStatus === "ready";
    return {
      dataDir: this.dataDir,
      startingBalance: this.startingBalanceValue,
      availableCashUsd: ready ? this.ledger.availableCash().toFixed() : this.startingBalanceValue,
      cash: ready ? ledger.cash : this.startingBalanceValue,
      reservedCash: ready ? ledger.reservedCash : "0",
      realizedPnl: ready ? ledger.realizedPnl : "0",
      fees: ready ? ledger.fees : "0",
      openPositionCount: ready ? this.ledger.positions.size : 0,
      fillCount: ready ? this.fillCount : 0,
      settlementCount: ready ? this.settlementCount : 0,
      actionKeyCount: ready ? this.actionKeys.size : 0,
      settledMarketCount: ready ? this.settledMarketIds.size : 0,
      bytes: this.bytes,
      maxBytes: this.maxBytesValue,
      degraded: this.degraded,
      lastError: this.lastError,
      lastFillAt: ready ? this.lastFillAt : null,
      recoveryStatus: this.recoveryStatus,
      recoveryReady: ready,
    };
  }

  private buildManifest(): NativeV2CounterfactualManifest {
    const ledger = this.ledger.snapshot();
    return {
      manifestVersion: NATIVE_V2_COUNTERFACTUAL_MANIFEST_VERSION,
      createdAt: this.createdAt,
      updatedAt: new Date().toISOString(),
      startingBalance: this.startingBalanceValue,
      fillCount: this.fillCount,
      settlementCount: this.settlementCount,
      openPositionCount: this.ledger.positions.size,
      realizedPnl: ledger.realizedPnl,
      fees: ledger.fees,
      segmentCount: this.segmentCount,
      bytes: this.bytes,
      maxBytes: this.maxBytesValue,
      degraded: this.degraded,
      lastError: this.lastError,
      lastErrorAt: this.lastErrorAt,
      recoveryStatus: this.recoveryStatus,
    };
  }

  /** Test helper — does not expose mutation of V1. */
  ledgerForTest(): PaperLedger {
    this.init();
    return this.ledger;
  }

  private applyFillInternal(actionKey: string, fill: PaperFill, at: Date, persist: boolean): void {
    this.ledger.applyFill(fill);
    this.actionKeys.add(actionKey);
    this.fillCount += 1;
    this.lastFillAt = at.toISOString();
    if (persist && this.durable) {
      const row: SegmentRow = { kind: "fill", actionKey, fill: structuredClone(fill), at: this.lastFillAt };
      this.appendRow(row);
      this.persistManifest();
    }
  }

  private rebuildFromDisk(): void {
    mkdirSync(this.segmentsDir(), { recursive: true });
    this.actionKeys.clear();
    this.settledMarketIds.clear();
    this.settlementLookups.clear();
    this.fillCount = 0;
    this.settlementCount = 0;
    this.lastFillAt = null;
    this.bytes = 0;
    this.segmentCount = 0;

    let persistedStartingBalance: string | null = null;
    let persistedMaxBytes: number | null = null;
    const manifestPath = this.manifestPath();
    if (existsSync(manifestPath)) {
      let parsed: NativeV2CounterfactualManifest;
      try {
        parsed = JSON.parse(readFileSync(manifestPath, "utf8")) as NativeV2CounterfactualManifest;
      } catch (error) {
        throw new Error(
          `CF_MANIFEST_CORRUPT:${error instanceof Error ? error.message : String(error)}`,
        );
      }
      this.createdAt = parsed.createdAt ?? this.createdAt;
      if (parsed.startingBalance != null && parsed.startingBalance !== "") {
        persistedStartingBalance = String(parsed.startingBalance);
      }
      if (Number.isFinite(parsed.maxBytes) && parsed.maxBytes > 0) {
        persistedMaxBytes = Math.trunc(parsed.maxBytes);
      }
    }

    // Durable startingBalance is authoritative when present — never silently reinterpret.
    if (persistedStartingBalance != null) {
      if (persistedStartingBalance !== this.requestedStartingBalance) {
        throw new Error(
          `CF_STARTING_BALANCE_MISMATCH: durable=${persistedStartingBalance} requested=${this.requestedStartingBalance}`,
        );
      }
      this.startingBalanceValue = persistedStartingBalance;
    } else {
      this.startingBalanceValue = this.requestedStartingBalance;
    }

    // Fresh isolated ledger then replay durable events (deterministic recovery).
    const fresh = new PaperLedger(this.startingBalanceValue, new Date(this.createdAt));

    const segments = this.listSegmentPaths();
    for (let segmentIdx = 0; segmentIdx < segments.length; segmentIdx += 1) {
      const segmentPath = segments[segmentIdx]!;
      const isLastSegment = segmentIdx === segments.length - 1;
      let text: string;
      try {
        text = readFileSync(segmentPath, "utf8");
      } catch (error) {
        throw new Error(
          `CF_SEGMENT_UNREADABLE:${path.basename(segmentPath)}:${error instanceof Error ? error.message : String(error)}`,
        );
      }
      const endsWithNewline = text.endsWith("\n");
      const rawLines = text.split("\n");
      // split() yields a trailing empty string when file ends with \n — drop it.
      if (rawLines.length > 0 && rawLines[rawLines.length - 1] === "") {
        rawLines.pop();
      }

      const committedLines: string[] = [];
      let tornTailSkipped = false;

      for (let lineIdx = 0; lineIdx < rawLines.length; lineIdx += 1) {
        const line = rawLines[lineIdx]!;
        if (!line.trim()) continue;
        const isLastLine = lineIdx === rawLines.length - 1;
        try {
          const row = JSON.parse(line) as SegmentRow;
          if (row.kind === "fill") {
            if (!row.actionKey || !row.fill) {
              throw new Error("CF_FILL_ROW_INVALID");
            }
            if (this.actionKeys.has(row.actionKey)) {
              committedLines.push(line);
              continue;
            }
            fresh.applyFill(row.fill);
            this.actionKeys.add(row.actionKey);
            this.fillCount += 1;
            this.lastFillAt = row.at;
            if (row.settlementLookup && row.settlementLookup.marketId) {
              this.settlementLookups.set(
                row.settlementLookup.marketId,
                structuredClone(row.settlementLookup),
              );
            }
            committedLines.push(line);
          } else if (row.kind === "settlement") {
            if (typeof row.marketId !== "string" || !row.marketId) {
              throw new Error("CF_SETTLEMENT_ROW_INVALID");
            }
            // Terminal tombstone: settlement identity persists even if position already gone.
            this.settledMarketIds.add(row.marketId);
            if (fresh.positions.has(row.marketId)) {
              fresh.settlePosition(row.marketId, row.upPayoff, row.downPayoff, new Date(row.at));
            }
            this.settlementCount += 1;
            this.clearActionKeysForMarket(row.marketId);
            this.settlementLookups.delete(row.marketId);
            committedLines.push(line);
          } else {
            throw new Error(`CF_SEGMENT_UNKNOWN_KIND:${String((row as { kind?: unknown }).kind)}`);
          }
        } catch (error) {
          // Torn/truncated tail of the last segment: deterministic skip (incomplete write).
          if (isLastSegment && isLastLine && !endsWithNewline) {
            tornTailSkipped = true;
            continue;
          }
          // Any other corrupt economic row → fail CF closed (do not skip / continue).
          throw new Error(
            `CF_SEGMENT_CORRUPT:${path.basename(segmentPath)}:line=${lineIdx + 1}:${error instanceof Error ? error.message : String(error)}`,
          );
        }
      }

      // Physically repair last segment so the next append cannot concatenate onto a
      // torn/missing-newline tail. Truncate to last committed newline-terminated content.
      if (isLastSegment && (tornTailSkipped || !endsWithNewline)) {
        const repaired = committedLines.length > 0 ? `${committedLines.join("\n")}\n` : "";
        writeFileSync(segmentPath, repaired, "utf8");
        text = repaired;
      }

      this.bytes += Buffer.byteLength(text, "utf8");
    }

    this.ledger.restore(fresh.snapshot(), [...fresh.positions.values()]);

    // maxBytes rules after replay: allow safe increase; reject unsafe decrease.
    const outstandingReserve = this.outstandingSettlementReserveBytes(this.openPositionMarketIdsUnlocked());
    if (persistedMaxBytes != null) {
      if (this.requestedMaxBytes >= persistedMaxBytes) {
        this.maxBytesValue = this.requestedMaxBytes;
      } else if (this.requestedMaxBytes >= this.bytes + outstandingReserve) {
        // Safe decrease: still covers committed bytes + settlement obligations.
        this.maxBytesValue = this.requestedMaxBytes;
      } else {
        throw new Error(
          `CF_MAX_BYTES_UNSAFE_DECREASE: durable=${persistedMaxBytes} requested=${this.requestedMaxBytes} need>=${this.bytes + outstandingReserve}`,
        );
      }
    } else {
      this.maxBytesValue = this.requestedMaxBytes;
    }

    this.currentSegmentIndex = segments.length > 0
      ? Math.max(...segments.map((p) => segmentIndexFromPath(p)), 0)
      : 0;
    if (segments.length > 0) {
      this.currentSegmentPath = segments[segments.length - 1]!;
      this.currentSegmentBytes = Buffer.byteLength(readFileSync(this.currentSegmentPath, "utf8"), "utf8");
      this.segmentCount = segments.length;
    } else {
      this.currentSegmentPath = null;
      this.currentSegmentBytes = 0;
      this.segmentCount = 0;
    }
    this.persistManifest();
  }

  private openPositionMarketIdsUnlocked(): string[] {
    return [...this.ledger.positions.keys()]
      .filter((marketId) => {
        const position = this.ledger.positions.get(marketId);
        if (!position) return false;
        return Number(position.upQuantity) !== 0 || Number(position.downQuantity) !== 0;
      })
      .sort();
  }

  private clearActionKeysForMarket(marketId: string): void {
    const prefix = `${marketId}:`;
    for (const key of [...this.actionKeys]) {
      if (key.startsWith(prefix)) this.actionKeys.delete(key);
    }
  }

  /** Test helper: force the next durable segment append to fail once. */
  faultNextAppendForTest(error: Error = new Error("FORCED_APPEND_FAILURE")): void {
    this.appendFaultForTest = error;
  }

  private cloneLedger(): PaperLedger {
    const clone = new PaperLedger(this.startingBalanceValue, new Date(this.createdAt));
    clone.restore(this.ledger.snapshot(), [...this.ledger.positions.values()]);
    return clone;
  }

  private outstandingSettlementReserveBytes(openMarketIds: readonly string[]): number {
    let total = 0;
    for (const marketId of openMarketIds) {
      total += counterfactualSettlementObligationBytes(marketId);
    }
    return total;
  }

  /**
   * Append a durable CF event.
   * Fills are admitted only when bytes + fillRow + sum(exact settlement obligations for open IDs after) <= maxBytes.
   * Settlements may consume reserved headroom up to maxBytes.
   */
  private appendRow(
    row: SegmentRow,
    klass: "fill" | "settlement" = row.kind === "settlement" ? "settlement" : "fill",
    opts: { openMarketIdsAfter?: readonly string[] } = {},
  ): void {
    if (this.appendFaultForTest) {
      const err = this.appendFaultForTest;
      this.appendFaultForTest = null;
      throw err;
    }
    const line = `${JSON.stringify(row)}\n`;
    const lineBytes = Buffer.byteLength(line, "utf8");
    const openAfter = opts.openMarketIdsAfter ?? this.openPositionMarketIdsUnlocked();
    const reservedAfter = klass === "fill" ? this.outstandingSettlementReserveBytes(openAfter) : 0;
    const ceiling = this.maxBytesValue - reservedAfter;
    if (this.bytes + lineBytes > ceiling) {
      throw new Error(
        `CAPACITY_EXCEEDED: counterfactual store at ${this.bytes}+${lineBytes}+reserve${reservedAfter} bytes exceeds maxBytes=${this.maxBytesValue} (${klass})`,
      );
    }
    mkdirSync(this.segmentsDir(), { recursive: true });
    if (!this.currentSegmentPath || this.currentSegmentBytes + lineBytes > this.segmentMaxBytes) {
      this.currentSegmentIndex += 1;
      this.currentSegmentPath = path.join(
        this.segmentsDir(),
        `segment-${String(this.currentSegmentIndex).padStart(5, "0")}.jsonl`,
      );
      this.currentSegmentBytes = 0;
      this.segmentCount = Math.max(this.segmentCount, this.currentSegmentIndex);
    }
    appendFileSync(this.currentSegmentPath, line, "utf8");
    this.currentSegmentBytes += lineBytes;
    this.bytes += lineBytes;
  }

  private persistManifest(): void {
    if (!this.durable) return;
    mkdirSync(this.dataDir, { recursive: true });
    const tmp = `${this.manifestPath()}.tmp`;
    writeFileSync(tmp, `${JSON.stringify(this.buildManifest(), null, 2)}\n`, "utf8");
    renameSync(tmp, this.manifestPath());
  }

  private listSegmentPaths(): string[] {
    const dir = this.segmentsDir();
    if (!existsSync(dir)) return [];
    return readdirSync(dir)
      .filter((name) => /^segment-\d+\.jsonl$/.test(name))
      .sort()
      .map((name) => path.join(dir, name));
  }

  private markRecoveryUnavailable(error: unknown): void {
    this.recoveryStatus = "unavailable";
    this.degraded = true;
    this.lastError = error instanceof Error ? error.message : String(error);
    this.lastErrorAt = new Date().toISOString();
    // Clear any partial in-memory economic state — CF must not act as authority.
    // Do NOT rewrite durable manifest/segments: preserve evidence so a clean restart can retry.
    this.actionKeys.clear();
    this.settledMarketIds.clear();
    this.settlementLookups.clear();
    this.fillCount = 0;
    this.settlementCount = 0;
    this.lastFillAt = null;
    const empty = new PaperLedger(this.startingBalanceValue, new Date(this.createdAt));
    this.ledger.restore(empty.snapshot(), []);
  }

  private noteFailure(error: unknown): void {
    this.degraded = true;
    this.lastError = error instanceof Error ? error.message : String(error);
    this.lastErrorAt = new Date().toISOString();
    try {
      this.persistManifest();
    } catch {
      // never throw into trading / shadow path
    }
  }

  private segmentsDir(): string {
    return path.join(this.dataDir, "segments");
  }

  private manifestPath(): string {
    return path.join(this.dataDir, "manifest.json");
  }
}

function segmentIndexFromPath(segmentPath: string): number {
  const match = path.basename(segmentPath).match(/^segment-(\d+)\.jsonl$/);
  return match ? Number(match[1]) : 0;
}

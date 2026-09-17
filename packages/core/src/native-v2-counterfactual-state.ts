import {
  appendFileSync,
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  renameSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";
import { PaperLedger } from "./accounting.js";
import type { LedgerState, MarketSnapshot, Outcome, PaperFill, PaperOrder, Position } from "./domain.js";
import { executePaperTaker } from "./execution.js";
import type { LearningSettlementLabel } from "./learning-sample.js";
import { DEFAULT_LEARNING_DATA_DIR, DEFAULT_LEARNING_MAX_BYTES } from "./learning-store.js";

export const NATIVE_V2_COUNTERFACTUAL_MANIFEST_VERSION = 1 as const;
export const NATIVE_V2_COUNTERFACTUAL_STATE_BASIS = "DECISION_TIME_V2_COUNTERFACTUAL_STATE" as const;
export const DEFAULT_COUNTERFACTUAL_SEGMENT_MAX_BYTES = 8_000_000;
/** Total CF event-log byte cap — same operational default as learning volume contract. */
export const DEFAULT_COUNTERFACTUAL_MAX_BYTES = DEFAULT_LEARNING_MAX_BYTES;
/**
 * Hard upper bound for one durable settlement JSONL row (including trailing newline).
 * Fill admission reserves this many bytes per accepted open CF position so every
 * open position can still append its settlement without exceeding maxBytes.
 * Settlement metadata is clamped so serialized rows cannot exceed this bound.
 */
export const MAX_COUNTERFACTUAL_SETTLEMENT_ROW_BYTES = 768;
/** Max UTF-16 code units retained for settlement marketId in the durable row. */
export const MAX_COUNTERFACTUAL_SETTLEMENT_MARKET_ID_CHARS = 128;
/** Max UTF-16 code units retained for settlement source metadata. */
export const MAX_COUNTERFACTUAL_SETTLEMENT_SOURCE_CHARS = 64;
/** Max UTF-16 code units retained for settlement reason metadata. */
export const MAX_COUNTERFACTUAL_SETTLEMENT_REASON_CHARS = 64;
/** Max UTF-16 code units retained for settlement decimal delta strings. */
export const MAX_COUNTERFACTUAL_SETTLEMENT_DECIMAL_CHARS = 64;

export type NativeV2CounterfactualIntent = "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG";

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
  bytes: number;
  maxBytes: number;
  degraded: boolean;
  lastError: string | null;
  lastFillAt: string | null;
}

export interface NativeV2CounterfactualDecisionState {
  stateBasis: typeof NATIVE_V2_COUNTERFACTUAL_STATE_BASIS;
  positions: Position[];
  ledger: LedgerState;
  availableCashUsd: string;
  lastStrategyTradeAt: string | null;
}

export interface CounterfactualWouldSubmitInput {
  actionKey: string;
  order: PaperOrder;
  snapshot: MarketSnapshot;
  /** Paper taker latency model; Phase-4 shadow uses decision-time book (0 is valid). */
  measuredLatencyMs?: number;
  at?: Date;
}

export type CounterfactualCommitResult =
  | { applied: true; fill: PaperFill; actionKey: string }
  | { applied: false; reason: "DUPLICATE_ACTION" | "NO_ORDER" | "EXECUTION_ERROR" | "CAPACITY_EXCEEDED" | "DISABLED"; detail?: string };

export type CounterfactualSettlementResult =
  | { applied: true; marketId: string; cashDelta: string; realizedDelta: string }
  | { applied: false; reason: "NO_POSITION" | "UNRESOLVED_LABEL" | "EXECUTION_ERROR" | "CAPACITY_EXCEEDED" | "DISABLED"; detail?: string };

type SegmentRow =
  | { kind: "fill"; actionKey: string; fill: PaperFill; at: string }
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

function clampSettlementChars(value: string, maxChars: number): string {
  return value.length <= maxChars ? value : value.slice(0, maxChars);
}

/** Build a settlement row whose serialized size is guaranteed <= MAX_COUNTERFACTUAL_SETTLEMENT_ROW_BYTES. */
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
  let source = clampSettlementChars(input.source, MAX_COUNTERFACTUAL_SETTLEMENT_SOURCE_CHARS);
  let reason =
    input.reason === null
      ? null
      : clampSettlementChars(input.reason, MAX_COUNTERFACTUAL_SETTLEMENT_REASON_CHARS);
  let marketId = clampSettlementChars(input.marketId, MAX_COUNTERFACTUAL_SETTLEMENT_MARKET_ID_CHARS);
  let cashDelta = clampSettlementChars(input.cashDelta, MAX_COUNTERFACTUAL_SETTLEMENT_DECIMAL_CHARS);
  let realizedDelta = clampSettlementChars(input.realizedDelta, MAX_COUNTERFACTUAL_SETTLEMENT_DECIMAL_CHARS);

  const build = (): Extract<SegmentRow, { kind: "settlement" }> => ({
    kind: "settlement",
    marketId,
    upPayoff: input.upPayoff,
    downPayoff: input.downPayoff,
    cashDelta,
    realizedDelta,
    at: input.at,
    source,
    reason,
  });

  let row = build();
  let line = `${JSON.stringify(row)}\n`;
  // Shrink optional metadata first if a pathological payload still exceeds the hard bound.
  while (Buffer.byteLength(line, "utf8") > MAX_COUNTERFACTUAL_SETTLEMENT_ROW_BYTES) {
    if (reason && reason.length > 0) {
      reason = reason.slice(0, Math.max(0, reason.length - 16));
      if (reason.length === 0) reason = null;
    } else if (source.length > 8) {
      source = source.slice(0, Math.max(8, source.length - 16));
    } else if (marketId.length > 16) {
      marketId = marketId.slice(0, Math.max(16, marketId.length - 16));
    } else {
      throw new Error(
        `CAPACITY_EXCEEDED: settlement row exceeds MAX_COUNTERFACTUAL_SETTLEMENT_ROW_BYTES=${MAX_COUNTERFACTUAL_SETTLEMENT_ROW_BYTES}`,
      );
    }
    row = build();
    line = `${JSON.stringify(row)}\n`;
  }
  return row;
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
 */
export class NativeV2CounterfactualStore {
  readonly dataDir: string;
  readonly startingBalance: string;
  readonly enabled: boolean;
  readonly durable: boolean;
  readonly segmentMaxBytes: number;
  readonly maxBytes: number;

  private readonly ledger: PaperLedger;
  private readonly actionKeys = new Set<string>();
  private fillCount = 0;
  private settlementCount = 0;
  private lastFillAt: string | null = null;
  private initialized = false;
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
    this.startingBalance = String(options.startingBalance ?? 1000);
    this.segmentMaxBytes = Number.isFinite(options.segmentMaxBytes) && (options.segmentMaxBytes ?? 0) > 0
      ? Math.trunc(options.segmentMaxBytes!)
      : DEFAULT_COUNTERFACTUAL_SEGMENT_MAX_BYTES;
    this.maxBytes = Number.isFinite(options.maxBytes) && (options.maxBytes ?? 0) > 0
      ? Math.trunc(options.maxBytes!)
      : DEFAULT_COUNTERFACTUAL_MAX_BYTES;
    this.createdAt = new Date().toISOString();
    this.ledger = new PaperLedger(this.startingBalance, new Date(this.createdAt));
  }

  /**
   * Rebuild / open durable segments.
   * Fail-soft: filesystem failures degrade counterfactual persistence only - never throw.
   */
  init(): void {
    if (!this.enabled || this.initialized) return;
    try {
      if (this.durable) this.rebuildFromDisk();
      this.initialized = true;
    } catch (error) {
      this.noteFailure(error);
      // Mark initialized so callers do not re-enter a throwing rebuild loop.
      this.initialized = true;
    }
  }

  /** Decision-time portfolio view for Phase-4 shadow risk/execution (V2-only). */
  decisionState(): NativeV2CounterfactualDecisionState {
    this.init();
    return {
      stateBasis: NATIVE_V2_COUNTERFACTUAL_STATE_BASIS,
      positions: structuredClone([...this.ledger.positions.values()]),
      ledger: this.ledger.snapshot(),
      availableCashUsd: this.ledger.availableCash().toFixed(),
      lastStrategyTradeAt: this.lastFillAt,
    };
  }

  hasAction(actionKey: string): boolean {
    this.init();
    return this.actionKeys.has(actionKey);
  }

  hasUnpairedPosition(marketId: string): boolean {
    this.init();
    return Boolean(this.ledger.positions.get(marketId)?.unpairedOutcome);
  }

  openPositionMarketIds(): string[] {
    this.init();
    return [...this.ledger.positions.keys()]
      .filter((marketId) => {
        const position = this.ledger.positions.get(marketId);
        if (!position) return false;
        return Number(position.upQuantity) !== 0 || Number(position.downQuantity) !== 0;
      })
      .sort();
  }

  /**
   * Advance CF state only for a real would-submit under the shadow policy.
   * Duplicate action keys are idempotent (no second fill).
   */
  tryCommitWouldSubmit(input: CounterfactualWouldSubmitInput): CounterfactualCommitResult {
    if (!this.enabled) return { applied: false, reason: "DISABLED" };
    this.init();
    if (this.actionKeys.has(input.actionKey)) {
      return { applied: false, reason: "DUPLICATE_ACTION" };
    }
    // Re-evaluations against an already-open unpaired CF book are not new actions.
    if (this.hasUnpairedPosition(input.order.marketId)) {
      return { applied: false, reason: "DUPLICATE_ACTION", detail: "EXISTING_UNPAIRED_POSITION" };
    }
    if (!input.order) return { applied: false, reason: "NO_ORDER" };

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
        };
        try {
          this.appendRow(row, "fill", { openPositionsAfter: staged.positions.size });
        } catch (error) {
          this.noteFailure(error);
          const message = error instanceof Error ? error.message : String(error);
          return {
            applied: false,
            reason: message.includes("CAPACITY_EXCEEDED") ? "CAPACITY_EXCEEDED" : "EXECUTION_ERROR",
            detail: message,
          };
        }
        // Durable event is authoritative — apply memory, then best-effort manifest.
        this.applyFillInternal(input.actionKey, fill, at, false);
        try {
          this.persistManifest();
        } catch (error) {
          // Manifest failure AFTER durable segment append must NOT roll back the economic event.
          this.noteFailure(error);
        }
      } else {
        this.applyFillInternal(input.actionKey, fill, at, false);
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
    return {
      dataDir: this.dataDir,
      startingBalance: this.startingBalance,
      availableCashUsd: this.ledger.availableCash().toFixed(),
      cash: ledger.cash,
      reservedCash: ledger.reservedCash,
      realizedPnl: ledger.realizedPnl,
      fees: ledger.fees,
      openPositionCount: this.ledger.positions.size,
      fillCount: this.fillCount,
      settlementCount: this.settlementCount,
      actionKeyCount: this.actionKeys.size,
      bytes: this.bytes,
      maxBytes: this.maxBytes,
      degraded: this.degraded,
      lastError: this.lastError,
      lastFillAt: this.lastFillAt,
    };
  }

  private buildManifest(): NativeV2CounterfactualManifest {
    const ledger = this.ledger.snapshot();
    return {
      manifestVersion: NATIVE_V2_COUNTERFACTUAL_MANIFEST_VERSION,
      createdAt: this.createdAt,
      updatedAt: new Date().toISOString(),
      startingBalance: this.startingBalance,
      fillCount: this.fillCount,
      settlementCount: this.settlementCount,
      openPositionCount: this.ledger.positions.size,
      realizedPnl: ledger.realizedPnl,
      fees: ledger.fees,
      segmentCount: this.segmentCount,
      bytes: this.bytes,
      maxBytes: this.maxBytes,
      degraded: this.degraded,
      lastError: this.lastError,
      lastErrorAt: this.lastErrorAt,
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
    this.fillCount = 0;
    this.settlementCount = 0;
    this.lastFillAt = null;
    this.bytes = 0;
    this.segmentCount = 0;

    const manifestPath = this.manifestPath();
    if (existsSync(manifestPath)) {
      try {
        const parsed = JSON.parse(readFileSync(manifestPath, "utf8")) as NativeV2CounterfactualManifest;
        this.createdAt = parsed.createdAt ?? this.createdAt;
      } catch {
        // rebuild from segments only
      }
    }

    // Fresh isolated ledger then replay durable events (deterministic recovery).
    const fresh = new PaperLedger(this.startingBalance, new Date(this.createdAt));

    const segments = this.listSegmentPaths();
    for (const segmentPath of segments) {
      const text = readFileSync(segmentPath, "utf8");
      this.bytes += Buffer.byteLength(text, "utf8");
      for (const line of text.split("\n")) {
        if (!line.trim()) continue;
        try {
          const row = JSON.parse(line) as SegmentRow;
          if (row.kind === "fill") {
            if (this.actionKeys.has(row.actionKey)) continue;
            fresh.applyFill(row.fill);
            this.actionKeys.add(row.actionKey);
            this.fillCount += 1;
            this.lastFillAt = row.at;
          } else if (row.kind === "settlement") {
            if (fresh.positions.has(row.marketId)) {
              fresh.settlePosition(row.marketId, row.upPayoff, row.downPayoff, new Date(row.at));
              this.settlementCount += 1;
            }
            this.clearActionKeysForMarket(row.marketId);
          }
        } catch (error) {
          this.noteFailure(error instanceof Error ? error : new Error(`CF_SEGMENT_CORRUPT:${path.basename(segmentPath)}`));
        }
      }
    }

    this.ledger.restore(fresh.snapshot(), [...fresh.positions.values()]);

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
    const clone = new PaperLedger(this.startingBalance, new Date(this.createdAt));
    clone.restore(this.ledger.snapshot(), [...this.ledger.positions.values()]);
    return clone;
  }

  private outstandingSettlementReserveBytes(openPositionCount = this.ledger.positions.size): number {
    return Math.max(0, openPositionCount) * MAX_COUNTERFACTUAL_SETTLEMENT_ROW_BYTES;
  }

  /**
   * Append a durable CF event.
   * Fills are admitted only when bytes + fillRow + outstandingSettlementReserve(after) <= maxBytes.
   * Settlements may consume reserved headroom up to maxBytes; row size is hard-bounded.
   */
  private appendRow(
    row: SegmentRow,
    klass: "fill" | "settlement" = row.kind === "settlement" ? "settlement" : "fill",
    opts: { openPositionsAfter?: number } = {},
  ): void {
    if (this.appendFaultForTest) {
      const err = this.appendFaultForTest;
      this.appendFaultForTest = null;
      throw err;
    }
    const line = `${JSON.stringify(row)}\n`;
    const lineBytes = Buffer.byteLength(line, "utf8");
    if (klass === "settlement" && lineBytes > MAX_COUNTERFACTUAL_SETTLEMENT_ROW_BYTES) {
      throw new Error(
        `CAPACITY_EXCEEDED: settlement row ${lineBytes} bytes exceeds MAX_COUNTERFACTUAL_SETTLEMENT_ROW_BYTES=${MAX_COUNTERFACTUAL_SETTLEMENT_ROW_BYTES}`,
      );
    }
    const openAfter = opts.openPositionsAfter ?? this.ledger.positions.size;
    const reservedAfter = klass === "fill" ? this.outstandingSettlementReserveBytes(openAfter) : 0;
    const ceiling = this.maxBytes - reservedAfter;
    if (this.bytes + lineBytes > ceiling) {
      throw new Error(
        `CAPACITY_EXCEEDED: counterfactual store at ${this.bytes}+${lineBytes}+reserve${reservedAfter} bytes exceeds maxBytes=${this.maxBytes} (${klass})`,
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

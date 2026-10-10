import { d, marketSnapshotSchema, type MarketSnapshot, type Outcome } from "@pm/core";

/**
 * G2H CLOSED channel contract. Sequence/generation are assigned by a trusted
 * local producer/transport, NOT inferred from the Polymarket WebSocket hash or
 * timestamp. Only normalized COMPLETE books may enter. No socket, orders or
 * process is created here.
 */
export interface GuardianBookChannel {
  readonly marketId: string;
  readonly tokenIds: Readonly<Record<Outcome, string>>;
  readonly generation: number;
  readonly ingressSequence: number;
  readonly status: "DISCONNECTED" | "AWAITING_BOOKS" | "ACTIVE" | "HALTED";
  readonly books: Readonly<Partial<Record<Outcome, MarketSnapshot>>>;
  readonly haltReason: string | null;
}
export type GuardianBookFrame =
  | { kind: "FULL_BOOK"; generation: number; ingressSequence: number;
      synchronized: true; snapshot: MarketSnapshot }
  | { kind: "INVALIDATE"; generation: number; ingressSequence: number;
      reason: "DISCONNECT" | "SOURCE_GAP" | "TOKEN_ROTATION" | "UNSYNC" | "OVERLOAD" };
export interface GuardianBookFreshness {
  nowMs: number;
  maxAgeMs: number;
  maxSkewMs: number;
}
export type GuardianPairedBookResult =
  | { status: "BLOCKED"; reason: "DISCONNECTED" | "HALTED" | "MISSING_SIDE" | "STALE_OR_FUTURE" | "SKEW" | "INVALID_LIMITS" }
  | { status: "READY"; up: MarketSnapshot; down: MarketSnapshot; generation: number; ingressSequence: number };

const MAX_BOOK_LEVELS = 512;
function validId(s: string): boolean { return typeof s === "string" && s.length > 0 && s.length <= 512; }
function halted(state: GuardianBookChannel, reason: string): GuardianBookChannel {
  return { ...state, status: "HALTED", books: {}, haltReason: reason };
}

export function createGuardianBookChannel(input: {
  marketId: string; upTokenId: string; downTokenId: string;
}): GuardianBookChannel {
  if (!validId(input.marketId) || !validId(input.upTokenId) || !validId(input.downTokenId)
    || input.upTokenId === input.downTokenId) throw new Error("INVALID_GUARDIAN_BOOK_IDENTITY");
  return { marketId: input.marketId, tokenIds: { UP: input.upTokenId, DOWN: input.downTokenId },
    generation: 0, ingressSequence: 0, status: "DISCONNECTED", books: {}, haltReason: null };
}

/** Every reconnect rotates the local generation and discards BOTH old books. */
export function beginGuardianBookGeneration(state: GuardianBookChannel): GuardianBookChannel {
  if (!Number.isSafeInteger(state.generation) || state.generation >= Number.MAX_SAFE_INTEGER) {
    return halted(state, "GENERATION_EXHAUSTED");
  }
  return { ...state, generation: state.generation + 1, ingressSequence: 0,
    status: "AWAITING_BOOKS", books: {}, haltReason: null };
}

function validLadder(book: MarketSnapshot): boolean {
  if (book.bids.length < 1 || book.asks.length < 1
    || book.bids.length > MAX_BOOK_LEVELS || book.asks.length > MAX_BOOK_LEVELS) return false;
  const side = (rows: MarketSnapshot["bids"], descending: boolean): boolean => {
    let previous: ReturnType<typeof d> | null = null;
    for (const row of rows) {
      const price = d(row.price), qty = d(row.quantity);
      if (!price.isFinite() || !qty.isFinite() || price.lt(0) || price.gt(1) || qty.lte(0)) return false;
      if (previous && (descending ? price.gte(previous) : price.lte(previous))) return false;
      previous = price;
    }
    return true;
  };
  return side(book.bids, true) && side(book.asks, false)
    && d(book.bids[0]!.price).lte(book.asks[0]!.price);
}

/** Bad frames poison this local generation; stale data cannot remain READY. */
export function advanceGuardianBookChannel(state: GuardianBookChannel, frame: GuardianBookFrame): GuardianBookChannel {
  if (state.status === "HALTED" || state.status === "DISCONNECTED") return state;
  if (!Number.isSafeInteger(frame.generation) || frame.generation !== state.generation
    || !Number.isSafeInteger(frame.ingressSequence)
    || frame.ingressSequence !== state.ingressSequence + 1) return halted(state, "GENERATION_OR_SEQUENCE");
  if (frame.kind === "INVALIDATE") return {
    ...state, ingressSequence: frame.ingressSequence, status: "DISCONNECTED",
    books: {}, haltReason: frame.reason,
  };
  if (frame.kind !== "FULL_BOOK" || frame.synchronized !== true) return halted(state, "UNSYNCHRONIZED_FRAME");
  const parse = marketSnapshotSchema.safeParse(frame.snapshot);
  if (!parse.success) return halted(state, "INVALID_BOOK_SCHEMA");
  const book = parse.data;
  if (book.marketId !== state.marketId || book.tokenId !== state.tokenIds[book.outcome])
    return halted(state, "TOKEN_OR_MARKET_IDENTITY");
  if (!validLadder(book)) return halted(state, "INVALID_BOOK_DEPTH");
  const next = { ...state.books, [book.outcome]: structuredClone(book) };
  return { ...state, ingressSequence: frame.ingressSequence,
    books: next, status: next.UP && next.DOWN ? "ACTIVE" : "AWAITING_BOOKS" };
}

/**
 * READY means *observationally usable*, NEVER executable depth/fill authority.
 * The future single executor revalidates both sides, fees, depth, residual
 * quantity, maker/taker owner and token epoch before any PAPER order.
 */
export function readGuardianPairedBook(state: GuardianBookChannel, limits: GuardianBookFreshness): GuardianPairedBookResult {
  if (state.status === "HALTED") return { status: "BLOCKED", reason: "HALTED" };
  if (state.status === "DISCONNECTED") return { status: "BLOCKED", reason: "DISCONNECTED" };
  if (!Number.isSafeInteger(limits.nowMs) || !Number.isSafeInteger(limits.maxAgeMs)
    || !Number.isSafeInteger(limits.maxSkewMs) || limits.maxAgeMs < 1
    || limits.maxAgeMs > 60_000 || limits.maxSkewMs < 0
    || limits.maxSkewMs > limits.maxAgeMs) return { status: "BLOCKED", reason: "INVALID_LIMITS" };
  const up = state.books.UP, down = state.books.DOWN;
  if (!up || !down) return { status: "BLOCKED", reason: "MISSING_SIDE" };
  const times = [up, down].map(book => ({
    received: Date.parse(book.receivedAt), source: Date.parse(book.sourceTimestamp),
  }));
  if (times.some(({received,source})=>!Number.isFinite(received) || !Number.isFinite(source)
    || received > limits.nowMs || source > received
    || limits.nowMs - received > limits.maxAgeMs || limits.nowMs - source > limits.maxAgeMs)) {
    return { status: "BLOCKED", reason: "STALE_OR_FUTURE" };
  }
  if (Math.abs(times[0]!.received - times[1]!.received) > limits.maxSkewMs
    || Math.abs(times[0]!.source - times[1]!.source) > limits.maxSkewMs) {
    return { status: "BLOCKED", reason: "SKEW" };
  }
  // Do not expose retained books as mutable references. G2I must measure copy cost.
  return { status: "READY", up: structuredClone(up), down: structuredClone(down),
    generation: state.generation, ingressSequence: state.ingressSequence };
}

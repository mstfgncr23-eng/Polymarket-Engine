import {
  EVENT_PAGE_SIZE,
  EVENT_SCAN_CEILING,
  type JournalEvent,
  type JournalKeysetCursor,
  type JournalKeysetQuery,
  type JournalQuery,
} from "@pm/core";

export interface EventJournalKeyset {
  queryEventsKeyset(query: JournalKeysetQuery): Promise<JournalEvent[]>;
  peekLatestKeyset(query: Omit<JournalKeysetQuery, "limit" | "after" | "through">): Promise<JournalKeysetCursor | null>;
}

export interface SourceFence {
  through: JournalKeysetCursor | null;
  recordedAtTo: Date;
  windowStart: Date | null;
  windowEnd: Date | null;
}

export interface ScanStats {
  complete: boolean;
  truncated: boolean;
  interrupted: boolean;
  scanned: number;
  pages: number;
  durationMs: number;
  maxPageRetained: number;
  fence: SourceFence;
  truncatedReason: string | null;
}

export interface ScanOptions {
  pageSize?: number;
  ceiling?: number;
  signal?: AbortSignal;
  onPage?(page: readonly JournalEvent[], pageIndex: number): void | Promise<void>;
}

export async function captureSourceFence(
  journal: EventJournalKeyset,
  query: Omit<JournalQuery, "limit" | "offset" | "order">,
  now = new Date(),
): Promise<SourceFence> {
  const peek: Omit<JournalKeysetQuery, "limit" | "after" | "through"> = { recordedAtTo: now };
  if (query.types) peek.types = query.types;
  if (query.experimentId) peek.experimentId = query.experimentId;
  if (query.from) peek.from = query.from;
  if (query.to) peek.to = query.to;
  const through = await journal.peekLatestKeyset(peek);
  return {
    through,
    recordedAtTo: now,
    windowStart: query.from ?? null,
    windowEnd: query.to ?? null,
  };
}

export async function scanJournalEvents(
  journal: EventJournalKeyset,
  query: Omit<JournalQuery, "limit" | "offset" | "order">,
  options: ScanOptions = {},
): Promise<ScanStats> {
  const started = Date.now();
  const pageSize = Math.max(1, Math.min(options.pageSize ?? EVENT_PAGE_SIZE, 20_000));
  const ceiling = Math.max(1, options.ceiling ?? EVENT_SCAN_CEILING);
  const fence = await captureSourceFence(journal, query);
  let after: JournalKeysetCursor | undefined;
  let scanned = 0;
  let pages = 0;
  let maxPageRetained = 0;
  let truncated = false;
  let interrupted = false;
  let truncatedReason: string | null = null;
  try {
    for (;;) {
      if (options.signal?.aborted) {
        interrupted = true;
        truncated = true;
        truncatedReason = "SCAN_INTERRUPTED";
        break;
      }
      if (scanned >= ceiling) {
        truncated = true;
        truncatedReason = "SCAN_CEILING";
        break;
      }
      const limit = Math.min(pageSize, ceiling - scanned);
      const pageQuery: JournalKeysetQuery = {
        recordedAtTo: fence.recordedAtTo,
        limit,
      };
      if (query.types) pageQuery.types = query.types;
      if (query.experimentId) pageQuery.experimentId = query.experimentId;
      if (query.from) pageQuery.from = query.from;
      if (query.to) pageQuery.to = query.to;
      if (after) pageQuery.after = after;
      if (fence.through) pageQuery.through = fence.through;
      const page = await journal.queryEventsKeyset(pageQuery);
      if (page.length === 0) break;
      pages += 1;
      maxPageRetained = Math.max(maxPageRetained, page.length);
      scanned += page.length;
      if (options.onPage) await options.onPage(page, pages - 1);
      const last = page[page.length - 1]!;
      after = { occurredAt: last.occurredAt, id: last.id };
      if (page.length < limit) break;
    }
  } catch (error) {
    interrupted = true;
    truncated = true;
    truncatedReason = truncatedReason ?? "SCAN_FAILED";
    throw Object.assign(error instanceof Error ? error : new Error(String(error)), {
      scanStats: {
        complete: false,
        truncated: true,
        interrupted: true,
        scanned,
        pages,
        durationMs: Date.now() - started,
        maxPageRetained,
        fence,
        truncatedReason,
      } satisfies ScanStats,
    });
  }
  return {
    complete: !truncated && !interrupted,
    truncated,
    interrupted,
    scanned,
    pages,
    durationMs: Date.now() - started,
    maxPageRetained,
    fence,
    truncatedReason,
  };
}

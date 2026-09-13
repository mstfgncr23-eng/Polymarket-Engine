import { isReportInputEventType, isTelemetryEventType, type JournalEvent } from "@pm/core";
import { createJournalEvent, type EventJournal } from "@pm/db";
import type { EngineMetrics } from "./metrics.js";

export interface TelemetryStats {
  queued: number;
  dropped: number;
  sampled: number;
  flushed: number;
  pendingWrites: number;
}

export interface CanonicalBatchStats { batches: number; events: number; }

export class BoundedJournalWriter {
  private readonly queue: JournalEvent[] = [];
  private pendingWrites = 0;
  private dropped = 0;
  private sampled = 0;
  private flushed = 0;
  private flushing = false;
  private sampleCounter = 0;
  private canonicalBatches = 0;
  private canonicalEvents = 0;

  constructor(
    private readonly journal: EventJournal,
    private readonly onDatabase: (up: boolean) => void,
    private readonly logger: { error(bindings: object, message: string): void },
    private readonly options: {
      queueLimit: number;
      batchSize: number;
      sampleEvery: number;
      maxConcurrent: number;
      onTelemetryDegraded?: (degraded: boolean) => void;
    } = {
      queueLimit: 2_000, batchSize: 50, sampleEvery: 20, maxConcurrent: 2,
    },
    private readonly metrics?: EngineMetrics,
  ) {}

  stats(): TelemetryStats {
    return { queued: this.queue.length, dropped: this.dropped, sampled: this.sampled, flushed: this.flushed, pendingWrites: this.pendingWrites };
  }

  canonicalStats(): CanonicalBatchStats { return { batches: this.canonicalBatches, events: this.canonicalEvents }; }

  /**
   * Writes a bounded canonical batch behind the same caller-owned admission
   * barrier as the individual decision path. Economic callers await this
   * method before an order can be submitted; it is never sampled or dropped.
   */
  async persistCanonicalBatch(events: readonly JournalEvent[]): Promise<void> {
    if (events.length === 0) return;
    if (events.some((event) => !isReportInputEventType(event.type))) throw new Error("CANONICAL_BATCH_CONTAINS_NON_REPORT_INPUT");
    await this.journal.appendMany(events);
    this.canonicalBatches += 1;
    this.canonicalEvents += events.length;
  }

  async persist(event: JournalEvent): Promise<void> {
    // Raw market/reference observations are live inputs, never durable
    // telemetry. Keep this guard at the writer boundary so a future producer
    // cannot accidentally reintroduce permanent rows through this helper.
    if (event.type === "MARKET_SNAPSHOT" || event.type === "REFERENCE_TICK") {
      this.sampled += 1;
      return;
    }
    if (!isTelemetryEventType(event.type)) {
      await this.writeDecision(event);
      return;
    }
    this.sampleCounter += 1;
    if (this.sampleCounter % this.options.sampleEvery !== 0) {
      this.sampled += 1;
      return;
    }
    if (this.queue.length >= this.options.queueLimit) {
      this.queue.shift();
      this.dropped += 1;
      this.metrics?.telemetryEventsDropped.inc();
    }
    this.queue.push(event);
    this.metrics?.telemetryQueueDepth.set(this.queue.length);
    void this.flush();
  }

  async persistDecision(type: string, input: Parameters<typeof createJournalEvent>[0]): Promise<void> {
    await this.writeDecision(createJournalEvent({ ...input, type }));
  }

  async flush(): Promise<void> {
    if (this.flushing || this.queue.length === 0) return;
    if (this.pendingWrites >= this.options.maxConcurrent) return;
    this.flushing = true;
    const batch = this.queue.splice(0, this.options.batchSize);
    this.metrics?.telemetryQueueDepth.set(this.queue.length);
    this.metrics?.telemetryBatchSize.observe(batch.length);
    this.pendingWrites += 1;
    const started = performance.now();
    try {
      await this.journal.appendMany(batch);
      this.flushed += batch.length;
      this.options.onTelemetryDegraded?.(false);
    } catch (error) {
      this.options.onTelemetryDegraded?.(true);
      this.dropped += batch.length;
      this.metrics?.telemetryEventsDropped.inc(batch.length);
      this.metrics?.telemetryBatchFailures.inc();
      this.logger.error({ err: error, count: batch.length }, "telemetry batch failed");
    } finally {
      this.metrics?.telemetryBatchFlushDuration.observe((performance.now() - started) / 1000);
      this.pendingWrites -= 1;
      this.flushing = false;
      if (this.queue.length > 0) void this.flush();
    }
  }

  private async writeDecision(event: JournalEvent): Promise<void> {
    if (isReportInputEventType(event.type)) {
      throw new Error("REPORT_INPUT_MUST_USE_TRACKED_ADMISSION");
    }
    const write = async () => {
      try {
        await this.journal.append(event);
        this.onDatabase(true);
      } catch (error) {
        this.onDatabase(false);
        this.logger.error({ err: error, traceId: event.traceId, type: event.type }, "journal append failed");
        throw error;
      }
    };
    if (!this.metrics) {
      await write().catch(() => undefined);
      return;
    }
    try {
      await this.metrics.observeDirectJournal(event.type, write);
    } catch {
      // Persistence semantics unchanged: decision write failures are logged, not retried.
    }
  }
}

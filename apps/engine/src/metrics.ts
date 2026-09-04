import { performance } from "node:perf_hooks";
import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from "prom-client";
import { triggerKindSchema, type TriggerEmitObserver } from "@pm/core";
import { REJECTION_REASONS, WORK_LANES, type RejectionReason, type WorkLane } from "./runtime-supervisor.js";

export const JOURNAL_METRIC_CLASSES = ["trigger", "activation", "consensus", "execution", "system"] as const;
export type JournalMetricClass = (typeof JOURNAL_METRIC_CLASSES)[number];

export const TRIGGER_KIND_VALUES = triggerKindSchema.options;
export const TRIGGER_SEVERITY_VALUES = ["0", "1", "2", "3"] as const;
export const FORBIDDEN_METRIC_LABEL_NAMES = ["marketId", "tokenId", "traceId", "slug", "eventId", "market_id", "token_id", "trace_id"] as const;

const DURATION_BUCKETS = [0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1, 5];
const BYTE_BUCKETS = [1_000, 10_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 2_000_000];
const LEVEL_BUCKETS = [1, 5, 10, 25, 50, 100, 250, 500, 1_000];
const COUNT_BUCKETS = [1, 2, 5, 10, 25, 50, 100, 250, 800];

/** Bounded journal class only — never event/trace/market/token identifiers. */
export function classifyJournalMetricClass(type: string): JournalMetricClass {
  if (type.startsWith("TRIGGER_") || type === "GROK_RESEARCH_AVAILABLE") return "trigger";
  if (type.startsWith("GROK_ACTIVATION") || type === "GROK_SIGNAL_ACCEPTED") return "activation";
  if (type === "CONSENSUS_DECISION" || type === "SEVERE_DISAGREEMENT") return "consensus";
  if (
    type.startsWith("PAPER_")
    || type.startsWith("POSITION_")
    || type.startsWith("RISK_")
    || type === "PNL_SNAPSHOT"
    || type === "HEDGE_COMPLETE"
    || type === "FAILED_HEDGE"
    || type === "FIRST_LEG_FILLED"
    || type === "PAIR_REALIZED"
    || type === "MARKET_RESOLUTION_OBSERVED"
  ) return "execution";
  return "system";
}

export type { TriggerEmitObserver };

export interface MarketNormalizeObserver {
  onNormalizedSnapshot(input: { durationSeconds: number; bidLevels: number; askLevels: number }): void;
  onPriceChangeEvent(entriesInPayload: number, processed: number): void;
}

export class EngineMetrics {
  readonly registry = new Registry();
  readonly marketMessages = new Counter({ name: "market_messages_total", help: "Normalized market messages", registers: [this.registry] });
  readonly wsReconnects = new Counter({ name: "ws_reconnects_total", help: "WebSocket reconnects", labelNames: ["feed"], registers: [this.registry] });
  readonly triggers = new Counter({ name: "triggers_total", help: "Triggers", labelNames: ["kind", "severity"], registers: [this.registry] });
  readonly mcpInteractions = new Counter({ name: "grok_mcp_interactions_total", help: "Authenticated and rejected Grok MCP interactions", labelNames: ["status", "method"], registers: [this.registry] });
  readonly grokSignals = new Counter({ name: "grok_signals_total", help: "Grok Bot signal submissions", labelNames: ["agent", "status"], registers: [this.registry] });
  readonly paperOrders = new Counter({ name: "paper_orders_total", help: "Paper orders", labelNames: ["type"], registers: [this.registry] });
  readonly paperFills = new Counter({ name: "paper_fills_total", help: "Paper fills", labelNames: ["role"], registers: [this.registry] });
  readonly riskVetoes = new Counter({ name: "risk_veto_total", help: "Risk vetoes", labelNames: ["reason"], registers: [this.registry] });
  readonly openPositions = new Gauge({ name: "open_positions", help: "Open positions", registers: [this.registry] });
  readonly unhedgedPositions = new Gauge({ name: "unhedged_positions", help: "Unhedged positions", registers: [this.registry] });
  readonly eventLatency = new Histogram({ name: "event_processing_latency", help: "Event processing latency seconds", buckets: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5], registers: [this.registry] });
  readonly mcpLatency = new Histogram({ name: "grok_mcp_latency_seconds", help: "Grok MCP request latency seconds", labelNames: ["method"], buckets: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5], registers: [this.registry] });
  readonly queueDepth = new Gauge({ name: "engine_queue_depth", help: "Pending decision count", registers: [this.registry] });
  readonly activations = new Counter({ name: "grok_activation_total", help: "Grok activation adapter attempts (not observed Bot wakes)", labelNames: ["provider", "status"], registers: [this.registry] });
  readonly staleEventsDropped = new Counter({ name: "polymarket_stale_events_dropped_total", help: "Out-of-order Polymarket token events dropped without reconnect", registers: [this.registry] });
  readonly malformedMarketEvents = new Counter({ name: "polymarket_malformed_events_dropped_total", help: "Malformed Polymarket token events dropped without reconnect", registers: [this.registry] });
  readonly staleEventRegression = new Histogram({ name: "polymarket_stale_event_regression_ms", help: "How far behind last accepted timestamp a dropped stale event was", buckets: [1, 2, 5, 10, 50, 100, 1_000, 5_000, 60_000], registers: [this.registry] });
  readonly referenceCloses = new Counter({ name: "reference_ws_close_total", help: "Reference WebSocket closes by classified kind", labelNames: ["kind"], registers: [this.registry] });

  readonly normalizedMarketSnapshots = new Counter({
    name: "normalized_market_snapshots_total",
    help: "Complete bid/ask snapshots produced by market-data normalization (book or price_change)",
    registers: [this.registry],
  });
  readonly priceChangeEntries = new Counter({
    name: "price_change_entries_total",
    help: "Price-change entries that produced a normalized snapshot",
    registers: [this.registry],
  });
  readonly priceChangeEntriesPerEvent = new Histogram({
    name: "price_change_entries_per_event",
    help: "Price-change entries present on each source event payload",
    buckets: COUNT_BUCKETS,
    registers: [this.registry],
  });
  readonly marketBookBidLevels = new Histogram({
    name: "market_book_bid_levels",
    help: "Bid-side book level count at normalization time (map size, no extra scan)",
    buckets: LEVEL_BUCKETS,
    registers: [this.registry],
  });
  readonly marketBookAskLevels = new Histogram({
    name: "market_book_ask_levels",
    help: "Ask-side book level count at normalization time (map size, no extra scan)",
    buckets: LEVEL_BUCKETS,
    registers: [this.registry],
  });
  readonly marketNormalizationDuration = new Histogram({
    name: "market_normalization_duration_seconds",
    help: "Time to convert one book state into a MarketSnapshot, including ordered-book construction or incremental level update",
    buckets: DURATION_BUCKETS,
    registers: [this.registry],
  });

  readonly sseSubscribers = new Gauge({
    name: "sse_subscribers",
    help: "Current dashboard SSE subscriber count",
    registers: [this.registry],
  });
  readonly sseSnapshotBuildDuration = new Histogram({
    name: "sse_snapshot_build_duration_seconds",
    help: "Time spent in state.snapshot() for an SSE snapshot",
    buckets: DURATION_BUCKETS,
    registers: [this.registry],
  });
  readonly sseSnapshotSerializeDuration = new Histogram({
    name: "sse_snapshot_serialize_duration_seconds",
    help: "Time spent JSON.stringify-ing an SSE snapshot that is already being serialized",
    buckets: DURATION_BUCKETS,
    registers: [this.registry],
  });
  readonly sseSnapshotPayloadBytes = new Histogram({
    name: "sse_snapshot_payload_bytes",
    help: "Serialized SSE snapshot JSON payload size in bytes",
    buckets: BYTE_BUCKETS,
    registers: [this.registry],
  });
  readonly sseSnapshotBroadcasts = new Counter({
    name: "sse_snapshot_broadcasts_total",
    help: "SSE snapshot frames actually encoded and written",
    registers: [this.registry],
  });
  readonly sseSnapshotsScheduledZeroSubscribers = new Counter({
    name: "sse_snapshots_scheduled_zero_subscribers_total",
    help: "Snapshot publishes scheduled while no SSE subscribers were connected",
    registers: [this.registry],
  });
  readonly sseSnapshotsSkippedZeroSubscribers = new Counter({
    name: "sse_snapshots_skipped_zero_subscribers_total",
    help: "Snapshot publish attempts skipped before scheduling/building because there were zero subscribers",
    registers: [this.registry],
  });
  readonly sseSubscriberBackpressure = new Counter({
    name: "sse_subscriber_backpressure_total",
    help: "Subscriber write() returned false (Node backpressure); observed only, not handled",
    registers: [this.registry],
  });

  readonly triggerCandidates = new Counter({
    name: "trigger_candidates_total",
    help: "Trigger candidates considered before cooldown/dedupe",
    registers: [this.registry],
  });
  readonly triggersEmitted = new Counter({
    name: "triggers_emitted_total",
    help: "Triggers emitted after cooldown/dedupe (bounded kind/severity labels)",
    labelNames: ["kind", "severity"],
    registers: [this.registry],
  });
  readonly triggersSuppressed = new Counter({
    name: "triggers_suppressed_total",
    help: "Trigger candidates suppressed by cooldown/dedupe (bounded kind/severity labels)",
    labelNames: ["kind", "severity"],
    registers: [this.registry],
  });
  readonly processTriggerInFlight = new Gauge({
    name: "process_trigger_in_flight",
    help: "Current concurrent processTrigger() invocations (bounded by RuntimeSupervisor workers)",
    registers: [this.registry],
  });
  readonly processTriggerInFlightPeak = new Gauge({
    name: "process_trigger_in_flight_peak",
    help: "Peak concurrent processTrigger() invocations since process start",
    registers: [this.registry],
  });
  readonly processTriggerDuration = new Histogram({
    name: "process_trigger_duration_seconds",
    help: "End-to-end processTrigger() duration including IDLE journal writes",
    buckets: DURATION_BUCKETS,
    registers: [this.registry],
  });
  readonly idleActivationBookkeepingDuration = new Histogram({
    name: "idle_activation_latency_bookkeeping_duration_seconds",
    help: "IDLE/inactive experiment activation-latency bookkeeping including durable journal write",
    buckets: DURATION_BUCKETS,
    registers: [this.registry],
  });

  readonly journalDirectWrites = new Counter({
    name: "journal_direct_writes_total",
    help: "Direct decision-event journal appends (not telemetry batches)",
    labelNames: ["class"],
    registers: [this.registry],
  });
  readonly journalDirectInFlight = new Gauge({
    name: "journal_direct_in_flight",
    help: "In-flight direct decision journal appends",
    registers: [this.registry],
  });
  readonly journalDirectLatency = new Histogram({
    name: "journal_direct_latency_seconds",
    help: "Direct decision journal append latency",
    labelNames: ["class"],
    buckets: DURATION_BUCKETS,
    registers: [this.registry],
  });
  readonly journalDirectFailures = new Counter({
    name: "journal_direct_failures_total",
    help: "Direct decision journal append failures",
    labelNames: ["class"],
    registers: [this.registry],
  });

  readonly referenceTicks = new Counter({
    name: "reference_ticks_total",
    help: "Reference feed ticks accepted for processing",
    registers: [this.registry],
  });
  readonly referenceTickProcessingDuration = new Histogram({
    name: "reference_tick_processing_duration_seconds",
    help: "Reference tick ingest duration including telemetry persist and trigger ingest",
    buckets: DURATION_BUCKETS,
    registers: [this.registry],
  });
  readonly referenceWindowLength = new Histogram({
    name: "reference_window_length",
    help: "Retained reference-tick window length after ingest (no per-asset label; asset set is not closed)",
    buckets: LEVEL_BUCKETS,
    registers: [this.registry],
  });

  readonly discoveryRefreshAttempts = new Counter({
    name: "discovery_refresh_attempts_total",
    help: "Discovery refresh attempts (single-flight; dirty reruns count as additional attempts)",
    registers: [this.registry],
  });
  readonly discoveryRefreshDuration = new Histogram({
    name: "discovery_refresh_duration_seconds",
    help: "Discovery refresh wall time",
    buckets: [0.05, 0.1, 0.25, 0.5, 1, 2, 5, 15, 30],
    registers: [this.registry],
  });
  readonly discoveryRefreshInFlight = new Gauge({
    name: "discovery_refresh_in_flight",
    help: "Currently running discovery refreshes (single-flight: 0 or 1)",
    registers: [this.registry],
  });
  readonly discoveryRefreshOverlap = new Counter({
    name: "discovery_refresh_overlap_total",
    help: "Discovery refreshes started while another refresh was already in flight (regression: should stay 0 after single-flight)",
    registers: [this.registry],
  });
  readonly discoverySingleflightCoalesced = new Counter({
    name: "discovery_singleflight_coalesced_total",
    help: "Discovery refresh requests collapsed onto the in-flight run plus at most one dirty rerun",
    registers: [this.registry],
  });
  readonly discoverySingleflightDirty = new Gauge({
    name: "discovery_singleflight_dirty",
    help: "Discovery dirty/pending-rerun bit (0 or 1)",
    registers: [this.registry],
  });
  readonly maintenanceSingleflightCoalesced = new Counter({
    name: "maintenance_singleflight_coalesced_total",
    help: "Maintenance ticks collapsed onto the in-flight run plus at most one dirty rerun",
    registers: [this.registry],
  });
  readonly maintenanceSingleflightDirty = new Gauge({
    name: "maintenance_singleflight_dirty",
    help: "Maintenance dirty/pending-rerun bit (0 or 1)",
    registers: [this.registry],
  });

  readonly runtimeWorkQueueDepth = new Gauge({
    name: "runtime_work_queue_depth",
    help: "Admitted trigger work waiting for a RuntimeSupervisor worker",
    labelNames: ["lane"],
    registers: [this.registry],
  });
  readonly runtimeWorkInFlight = new Gauge({
    name: "runtime_work_in_flight",
    help: "Trigger work currently executing on a RuntimeSupervisor worker",
    labelNames: ["lane"],
    registers: [this.registry],
  });
  readonly runtimeWorkRejected = new Counter({
    name: "runtime_work_rejected_total",
    help: "Trigger work rejected by admission/overload policy (finite lane and reason labels)",
    labelNames: ["lane", "reason"],
    registers: [this.registry],
  });
  readonly runtimeWorkOldestAge = new Gauge({
    name: "runtime_work_oldest_age_seconds",
    help: "Age of the oldest queued trigger work item per lane",
    labelNames: ["lane"],
    registers: [this.registry],
  });
  readonly runtimeWorkQueueBytes = new Gauge({
    name: "runtime_work_queue_bytes",
    help: "Conservative queued trigger byte estimate per lane",
    labelNames: ["lane"],
    registers: [this.registry],
  });
  readonly runtimeWorkFailures = new Counter({
    name: "runtime_work_failures_total",
    help: "RuntimeSupervisor task failures by lane (no error text in labels)",
    labelNames: ["lane"],
    registers: [this.registry],
  });

  readonly journalDirectWaiting = new Gauge({
    name: "journal_direct_waiting",
    help: "Direct journal appends waiting in the application admission queue (not PG pool waiters)",
    registers: [this.registry],
  });
  readonly journalDirectActive = new Gauge({
    name: "journal_direct_active",
    help: "Direct journal appends admitted and currently executing (capped below PG pool max)",
    registers: [this.registry],
  });
  readonly journalDirectActiveCritical = new Gauge({
    name: "journal_direct_active_critical",
    help: "Critical direct journal appends currently executing",
    registers: [this.registry],
  });
  readonly journalDirectActiveNoncritical = new Gauge({
    name: "journal_direct_active_noncritical",
    help: "Noncritical direct journal appends currently executing (capped below total active)",
    registers: [this.registry],
  });
  readonly journalDirectAdmissionRejected = new Counter({
    name: "journal_direct_admission_rejected_total",
    help: "Direct journal appends rejected by application admission before a PG waiter is created",
    labelNames: ["class"],
    registers: [this.registry],
  });
  readonly journalDirectQueueOldestAge = new Gauge({
    name: "journal_direct_queue_oldest_age_seconds",
    help: "Age of the oldest waiting direct journal admission",
    registers: [this.registry],
  });

  readonly sseSlowClientDisconnects = new Counter({
    name: "sse_slow_client_disconnects_total",
    help: "SSE subscribers disconnected for slow-client backpressure (finite reasons, no per-client labels)",
    labelNames: ["reason"],
    registers: [this.registry],
  });
  readonly discoveryMarketsInspected = new Histogram({
    name: "discovery_markets_inspected",
    help: "Markets inspected during one discovery refresh (report.discovered.length)",
    buckets: COUNT_BUCKETS,
    registers: [this.registry],
  });
  readonly discoveryQualified = new Histogram({
    name: "discovery_qualified",
    help: "Qualified markets produced by one discovery refresh (report.qualified.length)",
    buckets: COUNT_BUCKETS,
    registers: [this.registry],
  });

  readonly eventLoopUtilization = new Gauge({
    name: "nodejs_eventloop_utilization",
    help: "Node.js event-loop utilization since previous scrape (0-1); complements prom-client event-loop lag",
    registers: [this.registry],
    collect: () => {
      const next = performance.eventLoopUtilization(this.eventLoopUtilizationPrevious);
      this.eventLoopUtilizationPrevious = performance.eventLoopUtilization();
      this.eventLoopUtilization.set(next.utilization);
    },
  });

  readonly telemetryQueueDepth = new Gauge({
    name: "telemetry_queue_depth",
    help: "Telemetry writer in-memory queue depth",
    registers: [this.registry],
  });
  readonly telemetryEventsDropped = new Counter({
    name: "telemetry_events_dropped_total",
    help: "Telemetry events dropped by sampling overflow or failed batch flush",
    registers: [this.registry],
  });
  readonly telemetryBatchFlushDuration = new Histogram({
    name: "telemetry_batch_flush_duration_seconds",
    help: "Telemetry batch flush duration",
    buckets: DURATION_BUCKETS,
    registers: [this.registry],
  });
  readonly telemetryBatchFailures = new Counter({
    name: "telemetry_batch_failures_total",
    help: "Telemetry batch flush failures",
    registers: [this.registry],
  });
  readonly telemetryBatchSize = new Histogram({
    name: "telemetry_batch_size",
    help: "Telemetry events included in each flush batch",
    buckets: [1, 5, 10, 25, 50, 100],
    registers: [this.registry],
  });

  private eventLoopUtilizationPrevious = performance.eventLoopUtilization();
  private processTriggerInFlightCount = 0;
  private processTriggerPeakCount = 0;
  private journalDirectInFlightCount = 0;

  constructor() {
    collectDefaultMetrics({ register: this.registry, prefix: "polymarket_" });
    for (const lane of WORK_LANES) {
      this.runtimeWorkQueueDepth.set({ lane }, 0);
      this.runtimeWorkInFlight.set({ lane }, 0);
      this.runtimeWorkOldestAge.set({ lane }, 0);
      this.runtimeWorkQueueBytes.set({ lane }, 0);
      this.runtimeWorkFailures.inc({ lane }, 0);
      for (const reason of REJECTION_REASONS) this.runtimeWorkRejected.inc({ lane, reason }, 0);
    }
    for (const journalClass of JOURNAL_METRIC_CLASSES) {
      this.journalDirectWrites.inc({ class: journalClass }, 0);
      this.journalDirectFailures.inc({ class: journalClass }, 0);
      this.journalDirectAdmissionRejected.inc({ class: journalClass }, 0);
    }
    this.sseSlowClientDisconnects.inc({ reason: "byte_threshold" }, 0);
    this.sseSlowClientDisconnects.inc({ reason: "persistent_backpressure" }, 0);
    this.discoverySingleflightDirty.set(0);
    this.maintenanceSingleflightDirty.set(0);
    this.journalDirectWaiting.set(0);
    this.journalDirectActive.set(0);
    this.journalDirectActiveCritical.set(0);
    this.journalDirectActiveNoncritical.set(0);
    this.journalDirectQueueOldestAge.set(0);
  }

  triggerObserver(): TriggerEmitObserver {
    return {
      onCandidates: (count) => { if (count > 0) this.triggerCandidates.inc(count); },
      onEmitted: (kind, severity) => this.triggersEmitted.inc({ kind, severity: String(severity) }),
      onSuppressed: (kind, severity) => this.triggersSuppressed.inc({ kind, severity: String(severity) }),
      onReferenceWindow: (length) => this.referenceWindowLength.observe(length),
    };
  }

  normalizeObserver(): MarketNormalizeObserver {
    return {
      onNormalizedSnapshot: (input) => {
        this.normalizedMarketSnapshots.inc();
        this.marketNormalizationDuration.observe(input.durationSeconds);
        this.marketBookBidLevels.observe(input.bidLevels);
        this.marketBookAskLevels.observe(input.askLevels);
      },
      onPriceChangeEvent: (entriesInPayload, processed) => {
        this.priceChangeEntriesPerEvent.observe(entriesInPayload);
        if (processed > 0) this.priceChangeEntries.inc(processed);
      },
    };
  }

  noteProcessTriggerStart(): void {
    this.processTriggerInFlightCount += 1;
    this.processTriggerInFlight.set(this.processTriggerInFlightCount);
    if (this.processTriggerInFlightCount > this.processTriggerPeakCount) {
      this.processTriggerPeakCount = this.processTriggerInFlightCount;
      this.processTriggerInFlightPeak.set(this.processTriggerPeakCount);
    }
  }

  noteProcessTriggerEnd(startedMs: number): void {
    this.processTriggerDuration.observe((performance.now() - startedMs) / 1000);
    this.processTriggerInFlightCount = Math.max(0, this.processTriggerInFlightCount - 1);
    this.processTriggerInFlight.set(this.processTriggerInFlightCount);
  }

  async observeDirectJournal<T>(type: string, work: () => Promise<T>): Promise<T> {
    const journalClass = classifyJournalMetricClass(type);
    this.journalDirectInFlightCount += 1;
    this.journalDirectInFlight.set(this.journalDirectInFlightCount);
    const started = performance.now();
    try {
      const result = await work();
      this.journalDirectWrites.inc({ class: journalClass });
      return result;
    } catch (error) {
      this.journalDirectFailures.inc({ class: journalClass });
      throw error;
    } finally {
      this.journalDirectLatency.observe({ class: journalClass }, (performance.now() - started) / 1000);
      this.journalDirectInFlightCount = Math.max(0, this.journalDirectInFlightCount - 1);
      this.journalDirectInFlight.set(this.journalDirectInFlightCount);
    }
  }

  runtimeWorkMetrics() {
    return {
      setQueueDepth: (lane: WorkLane, value: number) => this.runtimeWorkQueueDepth.set({ lane }, value),
      setInFlight: (lane: WorkLane, value: number) => this.runtimeWorkInFlight.set({ lane }, value),
      setQueueBytes: (lane: WorkLane, value: number) => this.runtimeWorkQueueBytes.set({ lane }, value),
      setOldestAge: (lane: WorkLane, seconds: number) => this.runtimeWorkOldestAge.set({ lane }, seconds),
      incRejected: (lane: WorkLane, reason: RejectionReason) => this.runtimeWorkRejected.inc({ lane, reason }),
      incFailed: (lane: WorkLane) => this.runtimeWorkFailures.inc({ lane }),
    };
  }

  journalAdmissionMetrics() {
    return {
      setWaiting: (value: number) => this.journalDirectWaiting.set(value),
      setActive: (value: number) => this.journalDirectActive.set(value),
      setActiveCritical: (value: number) => this.journalDirectActiveCritical.set(value),
      setActiveNoncritical: (value: number) => this.journalDirectActiveNoncritical.set(value),
      setOldestAge: (seconds: number) => this.journalDirectQueueOldestAge.set(seconds),
      incRejected: (journalClass: string) => this.journalDirectAdmissionRejected.inc({ class: journalClass }),
    };
  }
}

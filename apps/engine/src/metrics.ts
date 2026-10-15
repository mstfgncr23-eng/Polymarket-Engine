import { ScalarHistogram } from "./scalar-histogram.js";
import { performance } from "node:perf_hooks";
import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from "prom-client";
import { triggerKindSchema, type TriggerEmitObserver } from "@pm/core";
import { INGRESS_GAP_REASONS, type MinOrderSizeHydrationObserver } from "@pm/market-data";
import { DURABLE_METRIC_TYPES, type DurableMetricType } from "@pm/db";
import { RECOVERY_FAILURE_REASONS, type RecoveryFailureReason } from "./recovery.js";
import { REJECTION_REASONS, WORK_LANES, type RejectionReason, type WorkLane } from "./runtime-supervisor.js";

export const JOURNAL_METRIC_CLASSES = ["trigger", "activation", "consensus", "execution", "system"] as const;
export type JournalMetricClass = (typeof JOURNAL_METRIC_CLASSES)[number];

export const TRIGGER_KIND_VALUES = triggerKindSchema.options;
export const TRIGGER_SEVERITY_VALUES = ["0", "1", "2", "3"] as const;
export const FORBIDDEN_METRIC_LABEL_NAMES = [
  "marketId", "tokenId", "traceId", "slug", "eventId", "market_id", "token_id", "trace_id",
  "operationKey", "positionId", "orderId", "fillId", "experimentId", "rawError",
  "asset", "event_id", "token_id",
] as const;

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
  onSourceEventNormalized?(durationSeconds: number): void;
}

export class EngineMetrics {
  readonly registry = new Registry();
  readonly sourceEventNormalization = new ScalarHistogram({ name: "market_source_event_normalization_seconds", help: "Complete source event normalization including validation and copy-on-write, before Engine dispatch", buckets: DURATION_BUCKETS, registers: [this.registry] });
  readonly ingressAccepted = new Gauge({ name: "market_ingress_accepted_events", help: "Cumulative raw source events admitted in this process; never reset per experiment", registers: [this.registry] });
  readonly ingressProcessed = new Gauge({ name: "market_ingress_processed_events", help: "Cumulative raw source events dequeued in this process; never reset per experiment", registers: [this.registry] });
  readonly ingressCapacityEvents = new Gauge({name:"market_ingress_capacity_events",help:"Fixed total event quota across owned shards",registers:[this.registry]});
  readonly ingressCapacityBytes = new Gauge({name:"market_ingress_capacity_bytes",help:"Fixed total approximate-byte quota across owned shards",registers:[this.registry]});
  readonly ingressBorrowedEvents = new Gauge({name:"market_ingress_borrowed_events",help:"Hot shard queued events borrowing idle quota; total cap remains fixed",registers:[this.registry]});
  readonly shardIngress = new Gauge({ name: "market_shard_ingress", help: "Bounded per-shard queue, cumulative source counts and subscription load", labelNames: ["shard", "measure"], registers: [this.registry] });
  readonly marketMessages = new Counter({ name: "market_messages_total", help: "Normalized market messages", registers: [this.registry] });
  readonly makerPublicTrades = new Counter({name:'maker_first_public_trades_total',help:'Validated public trade events delivered on the existing feed while Maker-First is enabled; process scoped',registers:[this.registry]});
  readonly bookSourceToIngress = new ScalarHistogram({ name: "book_source_to_ingress_seconds", help: "Source to owned WebSocket callback; absent when callback provenance is unavailable", buckets: DURATION_BUCKETS, registers: [this.registry] });
  readonly bookIngressToReceived = new ScalarHistogram({ name: "book_ingress_to_received_seconds", help: "Owned WebSocket callback to Engine received timestamp", buckets: DURATION_BUCKETS, registers: [this.registry] });
  readonly bookReceivedToProcessing = new ScalarHistogram({ name: "book_received_to_processing_seconds", help: "Engine received timestamp to processing; separate from source delay", buckets: DURATION_BUCKETS, registers: [this.registry] });
  // MinOrderSize hydration observability: low-cardinality, no token/market labels.
  readonly minOrderSizeHydrationAttempts = new Counter({
    name: "polymarket_min_order_size_hydration_attempt_total",
    help: "REST min order size hydration attempts",
    registers: [this.registry],
  });
  readonly minOrderSizeHydrationSuccesses = new Counter({
    name: "polymarket_min_order_size_hydration_success_total",
    help: "Successful REST min order size hydration responses",
    registers: [this.registry],
  });
  readonly minOrderSizeHydrationFailures = new Counter({
    name: "polymarket_min_order_size_hydration_failure_total",
    help: "Failed REST min order size hydration responses (throw/invalid/null)",
    registers: [this.registry],
  });
  readonly minOrderSizeHydrationStale = new Counter({
    name: "polymarket_min_order_size_hydration_stale_total",
    help: "Hydration completions dropped as stale (lifecycle/epoch mismatch)",
    registers: [this.registry],
  });
  readonly minOrderSizeHydrationInFlight = new Gauge({
    name: "polymarket_min_order_size_hydration_in_flight",
    help: "Current physically in-flight min order size hydration requests",
    registers: [this.registry],
  });
  readonly minOrderSizeMetadataApplied = new Counter({
    name: "polymarket_min_order_size_metadata_applied_total",
    help: "Min order size metadata applied to consumers",
    registers: [this.registry],
  });
  readonly wsReconnects = new Counter({ name: "ws_reconnects_total", help: "WebSocket reconnects", labelNames: ["feed"], registers: [this.registry] });
  readonly triggers = new Counter({ name: "triggers_total", help: "Triggers", labelNames: ["kind", "severity"], registers: [this.registry] });
  readonly mcpInteractions = new Counter({ name: "grok_mcp_interactions_total", help: "Authenticated and rejected Grok MCP interactions", labelNames: ["status", "method"], registers: [this.registry] });
  readonly grokSignals = new Counter({ name: "grok_signals_total", help: "Grok Bot signal submissions", labelNames: ["agent", "status"], registers: [this.registry] });
  readonly paperOrders = new Counter({ name: "paper_orders_total", help: "Paper orders", labelNames: ["type"], registers: [this.registry] });
  readonly paperFills = new Counter({ name: "paper_fills_total", help: "Paper fills", labelNames: ["role"], registers: [this.registry] });
  readonly riskVetoes = new Counter({ name: "risk_veto_total", help: "Risk vetoes", labelNames: ["reason"], registers: [this.registry] });
  readonly openPositions = new Gauge({ name: "open_positions", help: "Open positions", registers: [this.registry] });
  readonly unhedgedPositions = new Gauge({ name: "unhedged_positions", help: "Unhedged positions", registers: [this.registry] });
  readonly eventLatency = new ScalarHistogram({ name: "event_processing_latency", help: "Event processing latency seconds", buckets: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5], registers: [this.registry] });
  readonly mcpLatency = new Histogram({ name: "grok_mcp_latency_seconds", help: "Grok MCP request latency seconds", labelNames: ["method"], buckets: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5], registers: [this.registry] });
  readonly queueDepth = new Gauge({ name: "engine_queue_depth", help: "Pending decision count", registers: [this.registry] });
  readonly activations = new Counter({ name: "grok_activation_total", help: "Grok activation adapter attempts (not observed Bot wakes)", labelNames: ["provider", "status"], registers: [this.registry] });
  readonly scoutRefreshSkipped = new Counter({ name: "grok_scout_refresh_skipped_total", help: "Bounded scout refreshes skipped before model dispatch", labelNames: ["reason"], registers: [this.registry] });
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
  readonly priceChangeEntriesPerEvent = new ScalarHistogram({
    name: "price_change_entries_per_event",
    help: "Price-change entries present on each source event payload",
    buckets: COUNT_BUCKETS,
    registers: [this.registry],
  });
  readonly marketBookBidLevels = new ScalarHistogram({
    name: "market_book_bid_levels",
    help: "Bid-side book level count at normalization time (array length, no extra scan)",
    buckets: LEVEL_BUCKETS,
    registers: [this.registry],
  });
  readonly marketBookAskLevels = new ScalarHistogram({
    name: "market_book_ask_levels",
    help: "Ask-side book level count at normalization time (array length, no extra scan)",
    buckets: LEVEL_BUCKETS,
    registers: [this.registry],
  });
  readonly marketNormalizationDuration = new ScalarHistogram({
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
  readonly referenceTickProcessingDuration = new ScalarHistogram({
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
  readonly triggerMarketIngestDuration = new ScalarHistogram({
    name: "trigger_market_ingest_duration_seconds",
    help: "TriggerEngine.ingestMarket duration (one histogram for CPU attribution)",
    buckets: DURATION_BUCKETS,
    registers: [this.registry],
  });
  readonly triggerReferenceIngestDuration = new ScalarHistogram({
    name: "trigger_reference_ingest_duration_seconds",
    help: "TriggerEngine.ingestReference duration (one histogram for CPU attribution)",
    buckets: DURATION_BUCKETS,
    registers: [this.registry],
  });
  readonly triggerDedupeEntries = new Gauge({
    name: "trigger_dedupe_entries",
    help: "Retained trigger cooldown/dedupe entries (stable market+asset+kind scope)",
    registers: [this.registry],
  });
  readonly marketIngressQueueDepth = new Gauge({
    name: "market_ingress_queue_depth",
    help: "Owned market ingress event-count depth",
    registers: [this.registry],
  });
  readonly marketIngressQueueBytes = new Gauge({
    name: "market_ingress_queue_bytes",
    help: "Owned market ingress approximate queued bytes",
    registers: [this.registry],
  });
  readonly marketIngressHighWater = new Gauge({
    name: "market_ingress_high_water_total",
    help: "High-water event count of the owned market ingress queue",
    registers: [this.registry],
  });
  readonly marketIngressHighWaterBytes = new Gauge({
    name: "market_ingress_high_water_bytes",
    help: "High-water approximate bytes of the owned market ingress queue",
    registers: [this.registry],
  });
  readonly marketIngressGap = new Counter({
    name: "market_ingress_gap_total",
    help: "Market ingress gaps that invalidate incremental books (finite reasons)",
    labelNames: ["reason"],
    registers: [this.registry],
  });
  readonly marketIngressResync = new Counter({
    name: "market_ingress_resync_total",
    help: "Successful full-book resyncs after a gap or await (finite reasons)",
    labelNames: ["reason"],
    registers: [this.registry],
  });
  readonly marketUnsynchronizedTokens = new Gauge({
    name: "market_unsynchronized_tokens",
    help: "Tokens not in SYNCED (AWAITING_BOOK or GAPPED)",
    registers: [this.registry],
  });
  readonly referenceWindowEntries = new Gauge({
    name: "reference_window_entries",
    help: "Total retained reference-window ticks across all assets",
    registers: [this.registry],
  });
  readonly referenceWindowOverflow = new Counter({
    name: "reference_window_overflow_total",
    help: "Reference window overflows that mark the window GAPPED (finite reasons)",
    labelNames: ["reason"],
    registers: [this.registry],
  });
  readonly restingOrderTokenIndexEntries = new Gauge({
    name: "resting_order_token_index_entries",
    help: "Derived token→resting-order index mappings",
    registers: [this.registry],
  });
  readonly assetMarketIndexEntries = new Gauge({
    name: "asset_market_index_entries",
    help: "Derived asset→market index mappings",
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

  readonly retentionDeleted = new Counter({
    name: "retention_deleted_total",
    help: "Journal/idempotency/checkpoint rows deleted by the bounded retention manager",
    labelNames: ["retention_class"],
    registers: [this.registry],
  });
  readonly retentionFailures = new Counter({
    name: "retention_failures_total",
    help: "Retention cycle failures (trading state is not mutated by retention)",
    registers: [this.registry],
  });
  readonly retentionRuns = new Counter({
    name: "retention_runs_total",
    help: "Retention manager cycles that were not skipped for overlap",
    registers: [this.registry],
  });
  readonly retentionLastSuccessUnix = new Gauge({
    name: "retention_last_success_unix_seconds",
    help: "Unix timestamp of the last successful retention cycle",
    registers: [this.registry],
  });
  readonly retentionBatchSize = new Histogram({
    name: "retention_batch_size",
    help: "Rows deleted in one bounded retention batch",
    buckets: [1, 5, 10, 25, 50, 100, 200, 500],
    registers: [this.registry],
  });
  readonly retentionBatchDuration = new Histogram({
    name: "retention_batch_duration_seconds",
    help: "Wall time of one bounded retention batch",
    buckets: DURATION_BUCKETS,
    registers: [this.registry],
  });
  readonly reportScanPages = new Histogram({
    name: "experiment_report_scan_pages",
    help: "Keyset pages visited while building a streamed experiment summary",
    buckets: [1, 2, 5, 10, 25, 50, 100, 250, 500],
    registers: [this.registry],
  });
  readonly reportScanEvents = new Histogram({
    name: "experiment_report_scan_events",
    help: "Events visited while building a streamed experiment summary (logical, not retained)",
    buckets: [1, 10, 100, 1_000, 10_000, 50_000, 100_000, 500_000],
    registers: [this.registry],
  });
  readonly reportScanDuration = new Histogram({
    name: "experiment_report_scan_duration_seconds",
    help: "Streamed experiment summary scan duration",
    buckets: DURATION_BUCKETS,
    registers: [this.registry],
  });
  readonly reportAccumulatorHighWaterBytes = new Gauge({
    name: "experiment_report_accumulator_high_water_bytes",
    help: "Estimated bounded accumulator high-water mark for the last complete-scope scan",
    registers: [this.registry],
  });
  readonly finalReportPersistFailures = new Counter({
    name: "experiment_report_persist_failures_total",
    help: "Canonical experiment report persist failures (filesystem export failures are separate)",
    registers: [this.registry],
  });
  readonly finalReportFsExportFailures = new Counter({
    name: "experiment_report_fs_export_failures_total",
    help: "Optional /app/reports filesystem export failures after a successful canonical persist",
    registers: [this.registry],
  });
  readonly completeMetricsCoalesced = new Counter({
    name: "strategy_metrics_complete_coalesced_total",
    help: "Concurrent get_strategy_metrics(scope=complete) callers that shared one scan",
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
  readonly runtimeWorkExpired = new Counter({
    name: "runtime_work_expired_total",
    help: "Admitted trigger work discarded from the queue after its economic deadline",
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

  readonly durableTransitionCommits = new Counter({
    name: "durable_transition_commits_total",
    help: "Replay-authoritative durable transition inserts",
    labelNames: ["type"],
    registers: [this.registry],
  });
  readonly durableTransitionIdempotentHits = new Counter({
    name: "durable_transition_idempotent_hits_total",
    help: "Same-key same-payload durable commit hits",
    labelNames: ["type"],
    registers: [this.registry],
  });
  readonly durableTransitionConflicts = new Counter({
    name: "durable_transition_conflicts_total",
    help: "Same-key different-payload/type durable conflicts",
    labelNames: ["type"],
    registers: [this.registry],
  });
  readonly durableTransitionCommitFailures = new Counter({
    name: "durable_transition_commit_failures_total",
    help: "Durable transition commit failures (no retry tree)",
    labelNames: ["type"],
    registers: [this.registry],
  });
  readonly durableTransitionLastAppliedSequence = new Gauge({
    name: "durable_transition_last_applied_sequence",
    help: "Last applied durable commit sequence in this process",
    registers: [this.registry],
  });
  readonly recoveryReplayedTransitions = new Counter({
    name: "recovery_replayed_transitions_total",
    help: "Durable transitions applied during startup journal-tail replay",
    labelNames: ["type"],
    registers: [this.registry],
  });
  readonly recoveryDuration = new Histogram({
    name: "recovery_duration_seconds",
    help: "Startup checkpoint restore plus journal-tail replay duration",
    buckets: DURATION_BUCKETS,
    registers: [this.registry],
  });
  readonly recoveryFailures = new Counter({
    name: "recovery_failures_total",
    help: "Startup recovery FAIL_CLOSED outcomes (finite reasons only)",
    labelNames: ["reason"],
    registers: [this.registry],
  });
  readonly checkpointDurableCursor = new Gauge({
    name: "checkpoint_durable_cursor",
    help: "Durable journal cursor stored on the latest successful checkpoint",
    registers: [this.registry],
  });
  readonly checkpointWrites = new Counter({
    name: "checkpoint_writes_total",
    help: "Checkpoint write attempts by finite status",
    labelNames: ["status"],
    registers: [this.registry],
  });

  readonly journalTransientRetry = new Counter({
    name: "journal_transient_retry_total", help: "Bounded persistence transport second attempts",
    labelNames: ["operation_class"], registers: [this.registry],
  });
  readonly journalTransientRetryRecovered = new Counter({
    name: "journal_transient_retry_recovered_total", help: "Second attempts resolved successfully",
    labelNames: ["operation_class"], registers: [this.registry],
  });
  readonly journalTransientRetryExhausted = new Counter({
    name: "journal_transient_retry_exhausted_total", help: "Unresolved or cancelled transport recovery",
    labelNames: ["operation_class"], registers: [this.registry],
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
      this.runtimeWorkExpired.inc({ lane }, 0);
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
    for (const cls of ["raw_telemetry", "monitor_control", "mcp_idempotency", "checkpoint"] as const) {
      this.retentionDeleted.inc({ retention_class: cls }, 0);
    }
    this.retentionLastSuccessUnix.set(0);
    this.reportAccumulatorHighWaterBytes.set(0);
    this.finalReportPersistFailures.inc(0);
    this.finalReportFsExportFailures.inc(0);
    this.completeMetricsCoalesced.inc(0);
    this.journalDirectWaiting.set(0);
    this.journalDirectActive.set(0);
    this.journalDirectActiveCritical.set(0);
    this.journalDirectActiveNoncritical.set(0);
    this.journalDirectQueueOldestAge.set(0);
    for (const type of DURABLE_METRIC_TYPES) {
      this.durableTransitionCommits.inc({ type }, 0);
      this.durableTransitionIdempotentHits.inc({ type }, 0);
      this.durableTransitionConflicts.inc({ type }, 0);
      this.durableTransitionCommitFailures.inc({ type }, 0);
      this.recoveryReplayedTransitions.inc({ type }, 0);
    }
    for (const reason of RECOVERY_FAILURE_REASONS) this.recoveryFailures.inc({ reason }, 0);
    this.checkpointWrites.inc({ status: "success" }, 0);
    this.checkpointWrites.inc({ status: "fence" }, 0);
    this.checkpointWrites.inc({ status: "failed" }, 0);
    this.durableTransitionLastAppliedSequence.set(0);
    this.checkpointDurableCursor.set(0);
    for (const reason of INGRESS_GAP_REASONS) {
      this.marketIngressGap.inc({ reason }, 0);
      this.marketIngressResync.inc({ reason }, 0);
    }
    this.marketIngressResync.inc({ reason: "full_book" }, 0);
    this.referenceWindowOverflow.inc({ reason: "reference_window_limit" }, 0);
    this.triggerDedupeEntries.set(0);
    this.marketIngressQueueDepth.set(0);
    this.marketIngressQueueBytes.set(0);
    this.marketIngressHighWater.set(0);
    this.marketUnsynchronizedTokens.set(0);
    this.referenceWindowEntries.set(0);
    this.restingOrderTokenIndexEntries.set(0);
    this.assetMarketIndexEntries.set(0);
  }

  noteDurableCommit(type: string, inserted: boolean): void {
    const label = { type: type as DurableMetricType };
    const bounded = DURABLE_METRIC_TYPES.includes(label.type) ? label : { type: "other" as const };
    if (inserted) this.durableTransitionCommits.inc(bounded);
    else this.durableTransitionIdempotentHits.inc(bounded);
  }

  noteDurableConflict(type: string): void {
    const bounded = DURABLE_METRIC_TYPES.includes(type as DurableMetricType) ? { type: type as DurableMetricType } : { type: "other" as const };
    this.durableTransitionConflicts.inc(bounded);
  }

  noteDurableCommitFailure(type: string): void {
    const bounded = DURABLE_METRIC_TYPES.includes(type as DurableMetricType) ? { type: type as DurableMetricType } : { type: "other" as const };
    this.durableTransitionCommitFailures.inc(bounded);
  }

  noteRecoveryReplay(type: string): void {
    const bounded = DURABLE_METRIC_TYPES.includes(type as DurableMetricType) ? { type: type as DurableMetricType } : { type: "other" as const };
    this.recoveryReplayedTransitions.inc(bounded);
  }

  noteRecoveryFailure(reason: RecoveryFailureReason): void {
    this.recoveryFailures.inc({ reason });
  }

  triggerObserver(): TriggerEmitObserver {
    return {
      onCandidates: (count) => { if (count > 0) this.triggerCandidates.inc(count); },
      onEmitted: (kind, severity) => this.triggersEmitted.inc({ kind, severity: String(severity) }),
      onSuppressed: (kind, severity) => this.triggersSuppressed.inc({ kind, severity: String(severity) }),
      onReferenceWindow: (length) => this.referenceWindowLength.observe(length),
      onMarketIngest: (durationSeconds) => this.triggerMarketIngestDuration.observe(durationSeconds),
      onReferenceIngest: (durationSeconds) => this.triggerReferenceIngestDuration.observe(durationSeconds),
      onReferenceWindowOverflow: () => this.referenceWindowOverflow.inc({ reason: "reference_window_limit" }),
    };
  }

  noteIngressSnapshot(input: { depth: number; bytes: number; highWaterEvents: number; highWaterBytes: number; accepted?: number; processed?: number; poolMaxEvents?:number;poolMaxBytes?:number;borrowedEvents?:number }): void {
    this.marketIngressQueueDepth.set(input.depth);
    this.marketIngressQueueBytes.set(input.bytes);
    this.marketIngressHighWater.set(input.highWaterEvents);
    this.marketIngressHighWaterBytes.set(input.highWaterBytes);
    if (input.accepted !== undefined) this.ingressAccepted.set(input.accepted);
    if (input.processed !== undefined) this.ingressProcessed.set(input.processed);
    if(input.poolMaxEvents!==undefined)this.ingressCapacityEvents.set(input.poolMaxEvents);
    if(input.poolMaxBytes!==undefined)this.ingressCapacityBytes.set(input.poolMaxBytes);
    this.ingressBorrowedEvents.set(input.borrowedEvents??0);
  }

  noteShardSnapshots(input: readonly { shard: number; tokenCount: number; unsynchronizedTokens: number;
    ingress: { depth: number; bytes: number; highWaterEvents: number; highWaterBytes: number; accepted: number; processed: number } }[]): void {
    for (const s of input) for (const [measure, value] of Object.entries({ ...s.ingress, tokenCount: s.tokenCount, unsynchronizedTokens: s.unsynchronizedTokens })) {
      this.shardIngress.set({ shard: String(s.shard), measure }, value);
    }
  }

  noteIngressGap(reason: string): void {
    const bounded = (INGRESS_GAP_REASONS as readonly string[]).includes(reason) ? reason : "malformed";
    this.marketIngressGap.inc({ reason: bounded });
  }

  noteIngressResync(reason: string): void {
    const allowed = [...INGRESS_GAP_REASONS, "full_book"];
    const bounded = allowed.includes(reason) ? reason : "full_book";
    this.marketIngressResync.inc({ reason: bounded });
  }

  normalizeObserver(): MarketNormalizeObserver {
    return {
      onSourceEventNormalized: (seconds: number) => this.sourceEventNormalization.observe(seconds),
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

  minOrderSizeHydrationObserver(): MinOrderSizeHydrationObserver {
    return {
      onHydrationAttempt: () => this.minOrderSizeHydrationAttempts.inc(),
      onHydrationSuccess: () => this.minOrderSizeHydrationSuccesses.inc(),
      onHydrationFailure: () => this.minOrderSizeHydrationFailures.inc(),
      onHydrationStale: () => this.minOrderSizeHydrationStale.inc(),
      onHydrationApplied: () => this.minOrderSizeMetadataApplied.inc(),
      onHydrationInFlightChanged: (count) => this.minOrderSizeHydrationInFlight.set(count),
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
      incExpired: (lane: WorkLane) => this.runtimeWorkExpired.inc({ lane }),
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

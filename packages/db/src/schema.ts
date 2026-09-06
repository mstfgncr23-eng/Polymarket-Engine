import { bigint, boolean, index, integer, jsonb, numeric, pgTable, primaryKey, text, timestamp, uniqueIndex } from "drizzle-orm/pg-core";

const tracedColumns = {
  id: text("id").primaryKey(),
  traceId: text("trace_id").notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  payload: jsonb("payload").notNull(),
};

export const markets = pgTable("markets", {
  id: text("id").primaryKey(), slug: text("slug").notNull(), question: text("question").notNull(),
  conditionId: text("condition_id").notNull(), active: boolean("active").notNull(),
  payload: jsonb("payload").notNull(), updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => [uniqueIndex("markets_slug_idx").on(table.slug), index("markets_active_idx").on(table.active)]);

export const marketSnapshots = pgTable("market_snapshots", {
  ...tracedColumns, marketId: text("market_id").notNull(), tokenId: text("token_id").notNull(),
  sourceTimestamp: timestamp("source_timestamp", { withTimezone: true }).notNull(), sequence: integer("sequence").notNull(),
}, (table) => [index("market_snapshots_market_time_idx").on(table.marketId, table.sourceTimestamp)]);

export const triggerEvents = pgTable("trigger_events", {
  ...tracedColumns, marketId: text("market_id"), kind: text("kind").notNull(), severity: integer("severity").notNull(),
  dedupeKey: text("dedupe_key").notNull(), expiresAt: timestamp("expires_at", { withTimezone: true }).notNull(),
}, (table) => [index("trigger_events_time_idx").on(table.createdAt), index("trigger_events_market_idx").on(table.marketId)]);

export const agentInvocations = pgTable("agent_invocations", {
  ...tracedColumns, triggerId: text("trigger_id").notNull(), agentId: text("agent_id").notNull(), model: text("model").notNull(),
  status: text("status").notNull(), durationMs: integer("duration_ms"), estimatedCostUsd: numeric("estimated_cost_usd", { precision: 20, scale: 10 }),
}, (table) => [index("agent_invocations_agent_time_idx").on(table.agentId, table.createdAt)]);

export const agentOutputs = pgTable("agent_outputs", {
  ...tracedColumns, invocationId: text("invocation_id").notNull(), agentId: text("agent_id").notNull(), expiresAt: timestamp("expires_at", { withTimezone: true }),
}, (table) => [uniqueIndex("agent_outputs_invocation_idx").on(table.invocationId)]);

export const consensusDecisions = pgTable("consensus_decisions", {
  ...tracedColumns, triggerId: text("trigger_id").notNull(), direction: text("direction").notNull(), confidence: numeric("confidence", { precision: 12, scale: 10 }).notNull(),
}, (table) => [index("consensus_trigger_idx").on(table.triggerId)]);

export const riskEvaluations = pgTable("risk_evaluations", {
  ...tracedColumns, accepted: boolean("accepted").notNull(), reasonCodes: jsonb("reason_codes").notNull(),
}, (table) => [index("risk_evaluations_accepted_time_idx").on(table.accepted, table.createdAt)]);

export const paperOrders = pgTable("paper_orders", {
  ...tracedColumns, triggerId: text("trigger_id").notNull(), marketId: text("market_id").notNull(), tokenId: text("token_id").notNull(), status: text("status").notNull(),
}, (table) => [index("paper_orders_market_time_idx").on(table.marketId, table.createdAt)]);

export const paperFills = pgTable("paper_fills", {
  ...tracedColumns, orderId: text("order_id").notNull(), marketId: text("market_id").notNull(), sourceMarketEventId: text("source_market_event_id").notNull(),
  quantity: numeric("quantity", { precision: 38, scale: 18 }).notNull(), fillPrice: numeric("fill_price", { precision: 38, scale: 18 }).notNull(),
}, (table) => [index("paper_fills_order_idx").on(table.orderId)]);

export const positions = pgTable("positions", {
  id: text("id").primaryKey(), traceId: text("trace_id").notNull(), marketId: text("market_id").notNull(),
  payload: jsonb("payload").notNull(), updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => [uniqueIndex("positions_market_idx").on(table.marketId)]);

export const hedges = pgTable("hedges", {
  ...tracedColumns, marketId: text("market_id").notNull(), firstLegFillId: text("first_leg_fill_id").notNull(), secondLegFillId: text("second_leg_fill_id"), status: text("status").notNull(),
});

export const pnlSnapshots = pgTable("pnl_snapshots", {
  ...tracedColumns, experimentId: text("experiment_id").notNull(), equity: numeric("equity", { precision: 28, scale: 10 }).notNull(), drawdown: numeric("drawdown", { precision: 28, scale: 10 }).notNull(),
}, (table) => [index("pnl_experiment_time_idx").on(table.experimentId, table.createdAt)]);

export const aiCosts = pgTable("ai_costs", {
  ...tracedColumns, invocationId: text("invocation_id").notNull(), agentId: text("agent_id").notNull(), model: text("model").notNull(),
  inputTokens: integer("input_tokens").notNull(), outputTokens: integer("output_tokens").notNull(), estimatedCostUsd: numeric("estimated_cost_usd", { precision: 20, scale: 10 }),
}, (table) => [index("ai_costs_agent_time_idx").on(table.agentId, table.createdAt)]);

export const systemHealthEvents = pgTable("system_health_events", {
  ...tracedColumns, component: text("component").notNull(), status: text("status").notNull(),
}, (table) => [index("health_component_time_idx").on(table.component, table.createdAt)]);

export const strategyVersions = pgTable("strategy_versions", {
  id: text("id").primaryKey(), version: text("version").notNull(), status: text("status").notNull(),
  configuration: jsonb("configuration").notNull(), createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => [uniqueIndex("strategy_version_idx").on(table.version)]);

export const proposedStrategyChanges = pgTable("proposed_strategy_changes", {
  ...tracedColumns, strategyVersionId: text("strategy_version_id").notNull(), status: text("status").notNull().default("PROPOSED"), acceptedAt: timestamp("accepted_at", { withTimezone: true }),
});

export const experiments = pgTable("experiments", {
  id: text("id").primaryKey(), traceId: text("trace_id").notNull(), status: text("status").notNull(),
  startedAt: timestamp("started_at", { withTimezone: true }).notNull(), entryClosesAt: timestamp("entry_closes_at", { withTimezone: true }).notNull(),
  completedAt: timestamp("completed_at", { withTimezone: true }), payload: jsonb("payload").notNull(),
});

export const journalEvents = pgTable("journal_events", {
  id: text("id").primaryKey(), traceId: text("trace_id").notNull(), correlationId: text("correlation_id").notNull(),
  type: text("type").notNull(), aggregateType: text("aggregate_type").notNull(), aggregateId: text("aggregate_id").notNull(),
  payload: jsonb("payload").notNull(), occurredAt: timestamp("occurred_at", { withTimezone: true }).notNull(),
  recordedAt: timestamp("recorded_at", { withTimezone: true }).notNull().defaultNow(),
  /** Database-persistent admission/commit fence. Never inferred from recorded_at. */
  commitSeq: bigint("commit_seq", { mode: "number" }).notNull().default(0),
  sourceEventId: text("source_event_id"),
}, (table) => [index("journal_trace_idx").on(table.traceId), index("journal_aggregate_idx").on(table.aggregateType, table.aggregateId, table.occurredAt), index("journal_type_time_idx").on(table.type, table.occurredAt)]);

/** Singleton commit fence for journal_events. Updated in the same transaction as each event. */
export const journalCommitMetadata = pgTable("journal_commit_metadata", {
  id: integer("id").primaryKey(),
  highWater: bigint("high_water", { mode: "number" }).notNull().default(0),
  updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
});

export const engineCheckpoints = pgTable("engine_checkpoints", {
  id: text("id").primaryKey(), experimentId: text("experiment_id"), sequence: integer("sequence").notNull(),
  payload: jsonb("payload").notNull(), createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => [index("checkpoint_sequence_idx").on(table.sequence)]);

export const mcpIdempotencyKeys = pgTable("mcp_idempotency_keys", {
  scope: text("scope").notNull(), key: text("key").notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => [uniqueIndex("mcp_idempotency_scope_key_idx").on(table.scope, table.key)]);

export const experimentReports = pgTable("experiment_reports", {
  experimentId: text("experiment_id").notNull(),
  reportKind: text("report_kind").notNull(),
  schemaVersion: integer("schema_version").notNull(),
  generatedAt: timestamp("generated_at", { withTimezone: true }).notNull(),
  complete: boolean("complete").notNull(),
  truncated: boolean("truncated").notNull(),
  sourceEventCount: integer("source_event_count").notNull(),
  sourceWindowStart: timestamp("source_window_start", { withTimezone: true }),
  sourceWindowEnd: timestamp("source_window_end", { withTimezone: true }),
  sourceFenceOccurredAt: timestamp("source_fence_occurred_at", { withTimezone: true }),
  sourceFenceId: text("source_fence_id"),
  earliestIncludedAt: timestamp("earliest_included_at", { withTimezone: true }),
  latestIncludedAt: timestamp("latest_included_at", { withTimezone: true }),
  canonicalSummary: jsonb("canonical_summary").notNull(),
  renderedMarkdown: text("rendered_markdown").notNull(),
  contentHash: text("content_hash").notNull(),
  persistedAt: timestamp("persisted_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => [
  primaryKey({ name: "experiment_reports_pkey", columns: [table.experimentId, table.reportKind] }),
  index("experiment_reports_hash_idx").on(table.experimentId, table.contentHash),
  index("experiment_reports_complete_idx").on(table.complete, table.truncated, table.persistedAt),
]);

export const durableTransitions = pgTable("durable_transitions", {
  sequence: bigint("sequence", { mode: "number" }).primaryKey(),
  operationKey: text("operation_key").notNull(),
  type: text("type").notNull(),
  aggregateType: text("aggregate_type").notNull(),
  aggregateId: text("aggregate_id").notNull(),
  payloadHash: text("payload_hash").notNull(),
  payload: jsonb("payload").notNull(),
  occurredAt: timestamp("occurred_at", { withTimezone: true }).notNull(),
  recordedAt: timestamp("recorded_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => [
  uniqueIndex("durable_transitions_operation_key_uidx").on(table.operationKey),
  index("durable_transitions_sequence_idx").on(table.sequence),
]);

/** Singleton monotonic high-water and bounded compaction-key index. */
export const durableSequenceMetadata = pgTable("durable_sequence_metadata", {
  id: integer("id").primaryKey(),
  highWater: bigint("high_water", { mode: "number" }).notNull().default(0),
  compactedOperationKeys: jsonb("compacted_operation_keys").notNull().default({}),
  updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
});

/** Indexed replay identities; payloads are intentionally excluded. */
export const durableReplayKeys = pgTable("durable_replay_keys", {
  operationKey: text("operation_key").primaryKey(),
  type: text("type").notNull(),
  aggregateType: text("aggregate_type").notNull(),
  aggregateId: text("aggregate_id").notNull(),
  payloadHash: text("payload_hash").notNull(),
  sequence: bigint("sequence", { mode: "number" }).notNull(),
  occurredAt: timestamp("occurred_at", { withTimezone: true }).notNull(),
  recordedAt: timestamp("recorded_at", { withTimezone: true }).notNull(),
  expiresAt: timestamp("expires_at", { withTimezone: true }).notNull(),
  lifecycleState: text("lifecycle_state").notNull().default("closed"),
}, (table) => [index("durable_replay_keys_sequence_idx").on(table.sequence), index("durable_replay_keys_expiry_idx").on(table.expiresAt)]);

/** Versioned, verified analytics accumulator; raw source may be deleted only after this row is durable. */
export const experimentReportRollups = pgTable("experiment_report_rollups", {
  experimentId: text("experiment_id").notNull(),
  rollupKind: text("rollup_kind").notNull(),
  schemaVersion: integer("schema_version").notNull(),
  sourceCursor: bigint("source_cursor", { mode: "number" }).notNull(),
  sourceFenceId: text("source_fence_id"),
  sourceFenceOccurredAt: timestamp("source_fence_occurred_at", { withTimezone: true }),
  sourceEventCount: integer("source_event_count").notNull().default(0),
  sourceCommitSeq: bigint("source_commit_seq", { mode: "number" }).notNull().default(0),
  sourceRecordedAt: timestamp("source_recorded_at", { withTimezone: true }),
  stableRead: boolean("stable_read").notNull().default(false),
  previousContentHash: text("previous_content_hash"),
  coverageHash: text("coverage_hash"),
  accumulator: jsonb("accumulator").notNull(),
  contentHash: text("content_hash").notNull(),
  verified: boolean("verified").notNull().default(false),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => [
  primaryKey({ name: "experiment_report_rollups_pkey", columns: [table.experimentId, table.rollupKind] }),
  index("experiment_report_rollups_cursor_idx").on(table.sourceCursor),
]);

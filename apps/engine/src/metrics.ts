import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from "prom-client";

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

  constructor() { collectDefaultMetrics({ register: this.registry, prefix: "polymarket_" }); }
}

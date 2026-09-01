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

  constructor() { collectDefaultMetrics({ register: this.registry, prefix: "polymarket_" }); }
}

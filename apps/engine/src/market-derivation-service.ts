import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Worker } from "node:worker_threads";
import {
  compactBookSidesForDerivation,
  deriveBook,
  deriveBookFromSides,
  type BookDerived,
  type CompactBookSides,
  type MarketSnapshot,
} from "@pm/core";
import {
  MARKET_DERIVATION_WORKER_DERIVE,
  isMarketDerivationFailure,
  isMarketDerivationReady,
  isMarketDerivationSuccess,
  type MarketDerivationWorkerData,
} from "./market-derivation-protocol.js";

export const MARKET_DERIVATION_WORKER_STATUSES = ["success", "fallback", "timeout", "error"] as const;
export type MarketDerivationWorkerStatus = (typeof MARKET_DERIVATION_WORKER_STATUSES)[number];

export interface MarketDerivationWorkerObserver {
  onRequest(status: MarketDerivationWorkerStatus, latencySeconds: number): void;
  onInFlight(count: number): void;
}

export interface MarketDerivationResult {
  derived: BookDerived;
  status: MarketDerivationWorkerStatus;
}

export interface MarketDerivationWorkerServiceOptions {
  timeoutMs?: number;
  startTimeoutMs?: number;
  workerData?: MarketDerivationWorkerData;
  filename?: string;
  execArgv?: string[];
  observer?: MarketDerivationWorkerObserver;
}

interface PendingRequest {
  id: number;
  resolve: (result: MarketDerivationResult) => void;
  timer: NodeJS.Timeout;
  sides: CompactBookSides;
  snapshot: MarketSnapshot | null;
  started: number;
}

export function resolveMarketDerivationWorkerPath(fromUrl = import.meta.url): { filename: string; execArgv?: string[] } {
  const dir = dirname(fileURLToPath(fromUrl));
  const js = join(dir, "market-derivation-worker.js");
  if (existsSync(js)) return { filename: js };
  const ts = join(dir, "market-derivation-worker.ts");
  if (existsSync(ts)) return { filename: compileWorkerForTests(ts) };
  throw new Error("MARKET_DERIVATION_WORKER_NOT_FOUND");
}

let compiledWorkerPath: string | null = null;

function compileWorkerForTests(tsPath: string): string {
  if (compiledWorkerPath && existsSync(compiledWorkerPath)) return compiledWorkerPath;
  const outfile = join(tmpdir(), `pm-market-derivation-worker-${process.pid}.mjs`);
  let esbuild: { buildSync: (options: Record<string, unknown>) => void };
  try {
    esbuild = createRequire(import.meta.url)("esbuild") as { buildSync: (options: Record<string, unknown>) => void };
  } catch {
    throw new Error("MARKET_DERIVATION_WORKER_JS_MISSING");
  }
  esbuild.buildSync({
    entryPoints: [tsPath],
    bundle: true,
    platform: "node",
    target: "node22",
    format: "esm",
    outfile,
    sourcemap: false,
    logLevel: "silent",
    banner: {
      js: "import { createRequire as __createRequire } from 'node:module'; const require = __createRequire(import.meta.url);",
    },
  });
  compiledWorkerPath = outfile;
  return outfile;
}

/**
 * One worker, one in-flight request. Snapshot application remains serialized;
 * this service never pipelines or reorders BookDerived requests.
 */
export class MarketDerivationWorkerService {
  private worker: Worker | null = null;
  private nextId = 1;
  private pending: PendingRequest | null = null;
  private chain: Promise<void> = Promise.resolve();
  private started = false;
  private available = false;
  private disabled = false;
  private readonly timeoutMs: number;
  private readonly startTimeoutMs: number;
  private readonly observer?: MarketDerivationWorkerObserver;
  private readonly options: MarketDerivationWorkerServiceOptions;
  private ignoredLateResponses = 0;
  private readyResolve: (() => void) | null = null;
  private readyReject: ((error: Error) => void) | null = null;
  private killFlight: Promise<void> = Promise.resolve();

  constructor(options: MarketDerivationWorkerServiceOptions = {}) {
    this.options = options;
    this.timeoutMs = options.timeoutMs ?? 50;
    this.startTimeoutMs = options.startTimeoutMs ?? Math.max(this.timeoutMs, 2_000);
    this.observer = options.observer;
  }

  get isAvailable(): boolean {
    return this.available && !this.disabled && this.worker !== null;
  }

  get lateResponseCount(): number {
    return this.ignoredLateResponses;
  }

  async start(): Promise<boolean> {
    if (this.started) return this.isAvailable;
    this.started = true;
    const ready = this.waitReady();
    try {
      const resolved = this.options.filename
        ? { filename: this.options.filename, execArgv: this.options.execArgv }
        : resolveMarketDerivationWorkerPath();
      this.worker = new Worker(resolved.filename, {
        workerData: this.options.workerData,
        execArgv: this.options.execArgv ?? resolved.execArgv,
      });
      this.attach(this.worker);
      await ready;
      this.available = true;
      return true;
    } catch (error) {
      this.failReady(error instanceof Error ? error : new Error("MARKET_DERIVATION_WORKER_START_FAILED"));
      await ready.catch(() => undefined);
      this.available = false;
      this.disabled = true;
      await this.killQuiet();
      return false;
    }
  }

  async deriveSnapshot(snapshot: MarketSnapshot): Promise<MarketDerivationResult> {
    return this.enqueue(() => this.deriveOne(compactBookSidesForDerivation(snapshot), snapshot));
  }

  async deriveSides(sides: CompactBookSides): Promise<MarketDerivationResult> {
    return this.enqueue(() => this.deriveOne(sides, null));
  }

  async stop(): Promise<void> {
    this.disabled = true;
    this.available = false;
    const pending = this.pending;
    if (pending) this.finish(pending, "fallback", this.fallback(pending));
    await this.killQuiet();
    this.started = false;
  }

  private enqueue(work: () => Promise<MarketDerivationResult>): Promise<MarketDerivationResult> {
    const run = this.chain.then(work, work);
    this.chain = run.then(() => undefined, () => undefined);
    return run;
  }

  private async deriveOne(sides: CompactBookSides, snapshot: MarketSnapshot | null): Promise<MarketDerivationResult> {
    const started = performance.now();
    if (!this.isAvailable || !this.worker) {
      return this.observe("fallback", started, this.syncDerived(sides, snapshot));
    }
    const id = this.nextId++;
    this.observer?.onInFlight(1);
    return await new Promise<MarketDerivationResult>((resolve) => {
      const pending: PendingRequest = {
        id,
        resolve,
        sides,
        snapshot,
        started,
        timer: setTimeout(() => {
          if (this.pending?.id !== id) return;
          this.disableBroken("timeout");
        }, this.timeoutMs),
      };
      pending.timer.unref();
      this.pending = pending;
      try {
        this.worker!.postMessage({
          type: MARKET_DERIVATION_WORKER_DERIVE,
          id,
          bids: sides.bids,
          asks: sides.asks,
        });
      } catch {
        this.disableBroken("error");
      }
    });
  }

  private attach(worker: Worker): void {
    worker.on("message", (raw: unknown) => this.onMessage(raw));
    worker.on("error", (error) => {
      const wrapped = error instanceof Error ? error : new Error("MARKET_DERIVATION_WORKER_ERROR");
      if (this.readyReject) this.failReady(wrapped);
      else this.onWorkerBroken("error");
    });
    worker.on("exit", (code) => {
      if (this.readyReject) this.failReady(new Error(`MARKET_DERIVATION_WORKER_EXIT_${code}`));
      else this.onWorkerBroken("error");
    });
  }

  private waitReady(): Promise<void> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.failReady(new Error("MARKET_DERIVATION_WORKER_START_TIMEOUT"));
      }, this.startTimeoutMs);
      this.readyResolve = () => {
        clearTimeout(timer);
        this.readyResolve = null;
        this.readyReject = null;
        resolve();
      };
      this.readyReject = (error) => {
        clearTimeout(timer);
        this.readyResolve = null;
        this.readyReject = null;
        reject(error);
      };
    });
  }

  private failReady(error: Error): void {
    this.readyReject?.(error);
  }

  private onMessage(raw: unknown): void {
    if (isMarketDerivationReady(raw)) {
      this.readyResolve?.();
      return;
    }
    const pending = this.pending;
    if (!pending) {
      this.ignoredLateResponses += 1;
      return;
    }
    if (isMarketDerivationSuccess(raw)) {
      if (raw.id !== pending.id) {
        this.ignoredLateResponses += 1;
        return;
      }
      this.finish(pending, "success", raw.derived);
      return;
    }
    if (isMarketDerivationFailure(raw)) {
      if (raw.id !== pending.id) {
        this.ignoredLateResponses += 1;
        return;
      }
      this.finish(pending, "error", this.fallback(pending));
      return;
    }
    this.disableBroken("error");
  }

  private onWorkerBroken(status: "error" | "fallback"): void {
    this.disableBroken(status);
  }

  private disableBroken(status: MarketDerivationWorkerStatus): void {
    this.disabled = true;
    this.available = false;
    const pending = this.pending;
    if (pending) this.finish(pending, status, this.fallback(pending));
    void this.killQuiet();
  }

  private finish(pending: PendingRequest, status: MarketDerivationWorkerStatus, derived: BookDerived): void {
    if (this.pending?.id !== pending.id) return;
    this.pending = null;
    clearTimeout(pending.timer);
    this.observer?.onInFlight(0);
    pending.resolve(this.observe(status, pending.started, derived));
  }

  private observe(status: MarketDerivationWorkerStatus, started: number, derived: BookDerived): MarketDerivationResult {
    this.observer?.onRequest(status, (performance.now() - started) / 1000);
    return { derived, status };
  }

  private fallback(pending: PendingRequest): BookDerived {
    return this.syncDerived(pending.sides, pending.snapshot);
  }

  private syncDerived(sides: CompactBookSides, snapshot: MarketSnapshot | null): BookDerived {
    return snapshot ? deriveBook(snapshot) : deriveBookFromSides(sides);
  }

  private async killQuiet(): Promise<void> {
    const worker = this.worker;
    this.worker = null;
    if (!worker) return this.killFlight;
    const terminating = worker.terminate().then(
      () => { worker.removeAllListeners(); },
      () => { worker.removeAllListeners(); },
    );
    this.killFlight = this.killFlight.then(() => terminating, () => terminating);
    return this.killFlight;
  }
}

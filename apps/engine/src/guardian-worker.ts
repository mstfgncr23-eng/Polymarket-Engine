import { performance, monitorEventLoopDelay } from "node:perf_hooks";
import {
  evaluateGuardianV2, projectGuardianBookSignal, sameReversalEpisode, d,
  type GuardianV2State, type MarketSnapshot,
} from "@pm/core";
import { guardianRpc, type GuardianView, type GuardianUpdate, type GuardianMarket } from "./guardian-link.js";

export class GuardianMonitor {
  private states = new Map<string, GuardianV2State>();
  private persisted = new Map<string, number>();
  private source = "";

  evaluate(view: GuardianView, nowMs = Date.now()) {
    const source = `${view.engineStartedAt}:${view.experimentId}`;
    if (source !== this.source) { this.states.clear(); this.persisted.clear(); this.source = source; }
    const open = new Set(view.markets.map(m => m.position.marketId));
    for (const key of this.states.keys()) if (!open.has(key)) { this.states.delete(key); this.persisted.delete(key); }
    const updates: GuardianUpdate[] = [];
    const markets: Array<{ marketId: string; positionId: string; reason: string; priceAgeMs: number | null; phase: string | null }> = [];
    for (const market of view.markets) {
      const p = market.position;
      let reason = "OBSERVING", priceAgeMs: number | null = null;
      try {
        const episode = { marketId: p.marketId, positionId: p.id,
          unhedgedStartedAt: p.unhedgedStartedAt ?? p.openedAt, heldOutcome: p.unpairedOutcome! };
        let state = this.states.get(p.marketId) ?? null;
        if (state && !sameReversalEpisode(state, episode)) { state = null; this.persisted.delete(p.marketId); }
        if (market.state && sameReversalEpisode(market.state, episode)
          && (!state || market.state.revision > state.revision)) state = market.state;
        if (state) this.states.set(p.marketId, state);
        const ages = [market.up, market.down].map(b => b ? nowMs - Date.parse(b.receivedAt) : null);
        priceAgeMs = ages.every(a => a !== null) ? Math.max(...ages as number[]) : null;
        const validBook = (book: MarketSnapshot | null, outcome: string) => !!book
          && book.marketId === p.marketId && book.outcome === outcome
          && Number.isFinite(Date.parse(book.receivedAt)) && Number.isFinite(Date.parse(book.sourceTimestamp))
          && Date.parse(book.receivedAt) <= nowMs && Date.parse(book.sourceTimestamp) <= nowMs
          && nowMs - Date.parse(book.receivedAt) <= view.staleMs
          && nowMs - Date.parse(book.sourceTimestamp) <= view.staleMs;
        if (!view.enabled || !view.generation || !view.experimentId) reason = "ENGINE_UNAVAILABLE";
        else if (!p.unpairedOutcome || !d(p.unpairedQuantity).gt(0)) reason = "NO_RESIDUAL";
        else if (!market.up || !market.down) reason = "BOOK_MISSING";
        else if (!market.synchronized) reason = "BOOK_UNSYNCHRONIZED";
        else if (!validBook(market.up, "UP") || !validBook(market.down, "DOWN")) reason = "BOOK_STALE_OR_FUTURE";
        else if (market.up.tokenId === market.down.tokenId) reason = "BOOK_IDENTITY_INVALID";
        else {
          const observation = projectGuardianBookSignal(market.up, market.down, p.unpairedOutcome);
          if (!observation) reason = "BOOK_INVALID";
          else {
            const result = evaluateGuardianV2({ generation: view.generation!, state, episode,
              entryPrice: p.unpairedOutcome === "UP" ? p.averageUpCost : p.averageDownCost,
              now: new Date(nowMs), observation,
              observationGap: !!state?.lastObservationAt && nowMs - Date.parse(state.lastObservationAt) > view.staleMs,
              ...(market.consensus ? { consensus: market.consensus } : {}) });
            state = result.nextState;
            if (state) {
              this.states.set(p.marketId, state);
              reason = state.confirmedAt ? "PROTECTION_REQUEST" : state.phase;
              // Semantic transitions immediately; bounded durable recovery checkpoints otherwise.
              const last = this.persisted.get(p.marketId) ?? 0;
              if (result.events.length || nowMs - last >= 1000 || state.confirmedAt) {
                updates.push({ experimentId: view.experimentId!, state, events: result.events });
              }
            }
          }
        }
        markets.push({ marketId: p.marketId, positionId: p.id, reason, priceAgeMs, phase: state?.phase ?? null });
      } catch (error) {
        markets.push({ marketId: p.marketId, positionId: p.id, reason: `INVALID_MARKET_DATA:${(error as Error).message}`,
          priceAgeMs, phase: null });
      }
    }
    return { updates, markets };
  }

  acknowledged(marketId: string, nowMs: number) { this.persisted.set(marketId, nowMs); }
}

export async function runGuardian(path: string, signal: AbortSignal): Promise<void> {
  const monitor = new GuardianMonitor();
  const loop = monitorEventLoopDelay({ resolution: 20 }); loop.enable();
  let cycles = 0, errors = 0, lastLog = 0, lastCpu = process.cpuUsage(), lastAt = performance.now();
  const samples: Array<{ cycleMs: number; viewMs: number; evaluateMs: number; updateMs: number; engineWaitMs: number | null; handlerMs: number | null }> = [];
  const log = (x: object) => console.log(JSON.stringify({ service: "guardian", at: new Date().toISOString(), pid: process.pid, ...x }));
  log({ event: "STARTED", socket: path, mode: "paper" });
  while (!signal.aborted) {
    const started = performance.now();
    try {
      const view = await guardianRpc<GuardianView & { timing?: { engineWaitMs: number; handlerMs: number } }>(path, { kind: "VIEW" });
      const viewMs = performance.now() - started;
      if (view.mode !== "paper" || Date.parse(view.now) > Date.now() + 1000) throw new Error("INVALID_ENGINE_VIEW");
      const evaluationAt = performance.now();
      const result = monitor.evaluate(view);
      const evaluateMs = performance.now() - evaluationAt, updatesAt = performance.now();
      for (const update of result.updates) {
        const reply = await guardianRpc<{ status: string; reason?: string }>(path, { kind: "UPDATE", ...update });
        if (reply.status === "PERSISTED" || reply.status === "REPLAY" || reply.status === "QUEUED") monitor.acknowledged(update.state.marketId, Date.now());
        if (update.state.confirmedAt || reply.status === "REJECTED") log({ event: "REQUEST_RESULT", marketId: update.state.marketId,
          revision: update.state.revision, protectionReason: update.state.protectionReason, ...reply });
      }
      cycles++;
      samples.push({ cycleMs: performance.now() - started, viewMs, evaluateMs, updateMs: performance.now() - updatesAt,
        engineWaitMs: view.timing?.engineWaitMs ?? null, handlerMs: view.timing?.handlerMs ?? null });
      if (Date.now() - lastLog >= 5000) {
        const cpu = process.cpuUsage(), at = performance.now();
        log({ event: "CYCLE", cycles, errors, enginePid: view.enginePid, experimentId: view.experimentId,
          feed: view.feed, markets: result.markets, cycleMs: performance.now() - started,
          cpuPercent: (cpu.user + cpu.system - lastCpu.user - lastCpu.system) / ((at - lastAt) * 10),
          eventLoopP99Ms: loop.percentile(99) / 1e6, timingSamples: samples.splice(0) });
        lastCpu = cpu; lastAt = at; lastLog = Date.now(); loop.reset();
      }
    } catch (error) {
      errors++;
      if (Date.now() - lastLog >= 5000) { log({ event: "DISCONNECTED", cycles, errors, reason: (error as Error).message }); lastLog = Date.now(); }
    }
    await new Promise<void>(resolve => {
      const done = () => { clearTimeout(timer); signal.removeEventListener("abort", done); resolve(); };
      const timer = setTimeout(done, Math.max(0, 250 - (performance.now() - started)));
      signal.addEventListener("abort", done, { once: true });
      if (signal.aborted) done();
    });
  }
  loop.disable(); log({ event: "STOPPED", cycles, errors });
}

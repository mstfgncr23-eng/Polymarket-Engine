import { performance, monitorEventLoopDelay } from "node:perf_hooks";
import {
  evaluateGuardianV2, projectGuardianBookSignal, sameReversalEpisode, d,
  type GuardianV2State, type MarketSnapshot,
} from "@pm/core";
import { guardianRpc, currentGuardianConsensus, type GuardianView, type GuardianUpdate } from "./guardian-link.js";

export class GuardianMonitor {
  private states = new Map<string, GuardianV2State>();
  private persisted = new Map<string, number>();
  private unconfirmed = new Map<string, GuardianV2State>();
  private source = "";

  evaluate(view: GuardianView, nowMs = Date.now()) {
    const source = `${view.engineStartedAt}:${view.experimentId}`;
    if (source !== this.source) { this.states.clear(); this.persisted.clear(); this.unconfirmed.clear(); this.source = source; }
    const open = new Set(view.markets.map(m => m.position.marketId));
    for (const key of this.states.keys()) if (!open.has(key)) { this.states.delete(key); this.persisted.delete(key); this.unconfirmed.delete(key); }
    const updates: GuardianUpdate[] = [];
    const markets: Array<{ marketId: string; positionId: string; reason: string; priceAgeMs: number | null; phase: string | null }> = [];
    for (const market of view.markets) {
      const p = market.position;
      let reason = "OBSERVING", priceAgeMs: number | null = null;
      try {
        const episode = { marketId: p.marketId, positionId: p.id,
          unhedgedStartedAt: p.unhedgedStartedAt ?? p.openedAt, heldOutcome: p.unpairedOutcome! };
        let state = this.states.get(p.marketId) ?? null;
        if (state && !sameReversalEpisode(state, episode)) { state = null; this.persisted.delete(p.marketId); this.unconfirmed.delete(p.marketId); }
        const watch = this.unconfirmed.get(p.marketId);
        if (market.state && !market.state.confirmedAt && sameReversalEpisode(market.state, episode)
          && (!watch || market.state.revision > watch.revision)) this.unconfirmed.set(p.marketId, market.state);
        if (market.state && sameReversalEpisode(market.state, episode)
          && (!state || market.state.revision > state.revision)) state = market.state;
        // Admission is recovery context, not durable execution authority.
        if (market.admission && sameReversalEpisode(market.admission, episode)
          && (!state || market.admission.revision > state.revision)) state = market.admission;
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
            if (state && !state.confirmedAt) this.unconfirmed.set(p.marketId, state);
            const result = evaluateGuardianV2({ generation: view.generation!, state, episode,
              entryPrice: p.unpairedOutcome === "UP" ? p.averageUpCost : p.averageDownCost,
              now: new Date(nowMs), observation,
              observationGap: !!state?.lastObservationAt && nowMs - Date.parse(state.lastObservationAt) > view.staleMs,
              consensus: currentGuardianConsensus(market.consensus, nowMs, view.staleMs) });
            state = result.nextState;
            if (state) {
              this.states.set(p.marketId, state);
              reason = state.confirmedAt ? "PROTECTION_REQUEST" : state.phase;
              // Semantic transitions immediately; bounded durable recovery checkpoints otherwise.
              const last = this.persisted.get(p.marketId) ?? 0;
              if ((!market.state || market.state.revision < state.revision)
                && (result.events.length || nowMs - last >= 1000 || state.confirmedAt)) {
                const times = (book: MarketSnapshot) => ({ tokenId: book.tokenId, eventId: book.eventId, sequence: book.sequence,
                  sourceTimestamp: book.sourceTimestamp, receivedAt: book.receivedAt });
                updates.push({ experimentId: view.experimentId!, state, events: result.events,
                  observed: { up: times(market.up), down: times(market.down) } });
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

  rejected(update: GuardianUpdate, reason: string) {
    // Only a definitive Engine rejection proves this request was NOT admitted.
    // A transport timeout must retain the original identity for receipt/REPLAY.
    if (reason !== "STATE_STALE_OR_FUTURE") return;
    const state = this.states.get(update.state.marketId), watch = this.unconfirmed.get(update.state.marketId);
    if (state?.confirmedAt && state.revision === update.state.revision && watch
      && sameReversalEpisode(state, watch)) this.states.set(state.marketId, watch);
  }
}

/** Bounded local transport only. Critical semantic updates keep FIFO order;
 * ordinary observations coalesce. One socket per market, eight globally. */
export class GuardianUpdateLane {
  private queued = new Map<string, GuardianUpdate[]>();
  private active = new Map<string, Promise<void>>();
  private retry = new Map<string, { update: GuardianUpdate; nextAt: number; delayMs: number }>();
  private source = "";
  constructor(private readonly send: (update: GuardianUpdate) => Promise<{ status: string; reason?: string }>,
    private readonly monitor: GuardianMonitor, private readonly log: (x: object) => void,
    private readonly now: () => number = () => performance.now()) {}
  reconcile(view: GuardianView) {
    const source = `${view.engineStartedAt}:${view.experimentId}`;
    if (source !== this.source) { this.queued.clear(); this.retry.clear(); this.source = source; }
    for (const [id, queue] of this.queued) {
      const market = view.markets.find(m => m.position.marketId === id);
      const episode = market && { marketId: id, positionId: market.position.id,
        unhedgedStartedAt: market.position.unhedgedStartedAt ?? market.position.openedAt, heldOutcome: market.position.unpairedOutcome! };
      const remaining = queue.filter(u => episode && sameReversalEpisode(u.state, episode)
        && (!market!.state || market!.state.revision < u.state.revision));
      if (remaining.length) this.queued.set(id, remaining); else this.queued.delete(id);
      if (this.retry.get(id)?.update !== remaining[0]) this.retry.delete(id);
    }
  }
  offer(update: GuardianUpdate) {
    const id = update.state.marketId, queue = this.queued.get(id) ?? [];
    if (queue.some(u => sameReversalEpisode(u.state, update.state) && u.state.revision >= update.state.revision)) return;
    const last = queue.at(-1);
    if (last && !last.events.length && !last.state.confirmedAt && last.state.revision !== 1
      && !update.events.length && !update.state.confirmedAt
      && sameReversalEpisode(last.state, update.state) && !(this.active.has(id) && queue.length === 1)) queue.pop();
    const bytes = [...this.queued.values()].reduce((n, q) => n + Buffer.byteLength(JSON.stringify(q)), 0);
    if (queue.length >= 8 || [...this.queued.values()].reduce((n, q) => n + q.length, 0) >= 64
      || bytes + Buffer.byteLength(JSON.stringify(update)) > 4 * 1024 * 1024)
      throw new Error("GUARDIAN_UPDATE_BACKPRESSURE");
    queue.push(update); this.queued.set(id, queue);
  }
  pump() {
    for (const [id, queue] of this.queued) {
      if (this.active.size >= 8) break;
      if (this.active.has(id) || !queue.length) continue;
      const retry = this.retry.get(id);
      if (retry && retry.update === queue[0] && this.now() < retry.nextAt) continue;
      const update = queue[0]!, source = this.source, sent = performance.now();
      const task = this.send(update).then(reply => {
        if (source !== this.source || this.queued.get(id)?.[0] !== update) return;
        if (["PERSISTED", "REPLAY", "QUEUED"].includes(reply.status)) {
          queue.shift(); this.retry.delete(id); this.monitor.acknowledged(id, Date.now());
        } else if (reply.status === "REJECTED") {
          queue.shift(); this.retry.delete(id); this.monitor.rejected(update, reply.reason ?? "");
          if (reply.reason === "STATE_STALE_OR_FUTURE" && update.state.confirmedAt) this.queued.delete(id);
        } else this.defer(id, update);
        if (!queue.length && this.queued.get(id) === queue) this.queued.delete(id);
        if (update.state.confirmedAt || reply.status === "REJECTED") this.log({ event: "REQUEST_RESULT", marketId: id,
          positionId: update.state.positionId, unhedgedStartedAt: update.state.unhedgedStartedAt,
          revision: update.state.revision, updatedAt: update.state.updatedAt, confirmedAt: update.state.confirmedAt,
          observed: update.observed, protectionReason: update.state.protectionReason, rpcMs: performance.now() - sent, ...reply });
      }).catch(error => {
        if (source === this.source && this.queued.get(id)?.[0] === update) this.defer(id, update);
        this.log({ event: "REQUEST_UNRESOLVED", marketId: id, revision: update.state.revision,
          rpcMs: performance.now() - sent, reason: (error as Error).message });
      })
        .finally(() => { this.active.delete(id); });
      this.active.set(id, task);
    }
  }
  private defer(id: string, update: GuardianUpdate) {
    const previous = this.retry.get(id);
    const delayMs = previous?.update === update ? Math.min(5000, previous.delayMs * 2) : 500;
    // Probe the same request identity; monitoring/VIEW reconciliation continue
    // at 250ms and immediately remove an observed durable receipt.
    this.retry.set(id, { update, delayMs, nextAt: this.now() + delayMs });
  }
  async drain() { await Promise.allSettled(this.active.values()); }
}

export async function runGuardian(path: string, signal: AbortSignal): Promise<void> {
  const monitor = new GuardianMonitor();
  const loop = monitorEventLoopDelay({ resolution: 20 }); loop.enable();
  let cycles = 0, errors = 0, lastLog = 0, lastCpu = process.cpuUsage(), lastAt = performance.now();
  const samples: Array<{ cycleMs: number; viewMs: number; evaluateMs: number; updateMs: number; engineWaitMs: number | null; handlerMs: number | null }> = [];
  const log = (x: object) => console.log(JSON.stringify({ service: "guardian", at: new Date().toISOString(), pid: process.pid, ...x }));
  const lane = new GuardianUpdateLane(update => guardianRpc(path, { kind: "UPDATE", ...update }), monitor, log);
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
      lane.reconcile(view);
      for (const update of result.updates) lane.offer(update);
      lane.pump();
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
  await lane.drain(); loop.disable(); log({ event: "STOPPED", cycles, errors });
}

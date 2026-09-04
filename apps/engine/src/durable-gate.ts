import { AsyncLocalStorage } from "node:async_hooks";

/**
 * Serializes replay-authoritative state transitions and checkpoint fencing.
 * Ordinary journal admission (PR #12) stays concurrent for telemetry/research.
 * Re-entrant for the same async context so commit+apply+checkpoint can nest.
 */
export class DurableMutationGate {
  private chain = Promise.resolve();
  private readonly held = new AsyncLocalStorage<true>();

  async run<T>(work: () => Promise<T>): Promise<T> {
    if (this.held.getStore()) return work();
    const previous = this.chain;
    let release: () => void = () => undefined;
    this.chain = new Promise<void>((resolve) => { release = resolve; });
    await previous;
    try {
      return await this.held.run(true, work);
    } finally {
      release();
    }
  }
}

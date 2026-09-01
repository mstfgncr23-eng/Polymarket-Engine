export interface ReconnectOptions {
  initialDelayMs: number;
  maxDelayMs: number;
  multiplier: number;
  jitterRatio: number;
}

export class ExponentialBackoff {
  private attempt = 0;
  constructor(
    private readonly options: ReconnectOptions,
    private readonly random: () => number = Math.random,
  ) {}

  nextDelay(): number {
    const base = Math.min(this.options.maxDelayMs, this.options.initialDelayMs * this.options.multiplier ** this.attempt++);
    const jitter = base * this.options.jitterRatio * (this.random() * 2 - 1);
    return Math.max(0, Math.round(base + jitter));
  }

  reset(): void { this.attempt = 0; }
}

export function abortableDelay(ms: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.reject(signal.reason);
  return new Promise((resolve, reject) => {
    const timer = setTimeout(resolve, ms);
    signal.addEventListener("abort", () => { clearTimeout(timer); reject(signal.reason); }, { once: true });
  });
}

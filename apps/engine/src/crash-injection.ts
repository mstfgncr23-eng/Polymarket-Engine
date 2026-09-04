export const CRASH_INJECTION_POINTS = [
  "BEFORE_DURABLE_COMMIT",
  "AFTER_DURABLE_COMMIT_BEFORE_APPLY",
  "AFTER_APPLY_BEFORE_DERIVED_AUDIT",
  "AFTER_DERIVED_AUDIT_BEFORE_CHECKPOINT",
  "AFTER_CHECKPOINT",
] as const;

export type CrashInjectionPoint = (typeof CRASH_INJECTION_POINTS)[number];

export class CrashInjectedError extends Error {
  readonly code = "CRASH_INJECTED";
  constructor(readonly point: CrashInjectionPoint, readonly operationKey?: string) {
    super(`CRASH_INJECTED:${point}${operationKey ? `:${operationKey}` : ""}`);
    this.name = "CrashInjectedError";
  }
}

export interface CrashInjectionHook {
  point: CrashInjectionPoint;
  remaining: number;
  operationKey?: string;
}

/** Test-only. Production must never install hooks. */
export class CrashInjector {
  private hooks: CrashInjectionHook[] = [];

  arm(point: CrashInjectionPoint, options: { times?: number; operationKey?: string } = {}): void {
    this.hooks.push({
      point,
      remaining: options.times ?? 1,
      operationKey: options.operationKey,
    });
  }

  maybeCrash(point: CrashInjectionPoint, operationKey?: string): void {
    const index = this.hooks.findIndex((hook) => (
      hook.point === point
      && hook.remaining > 0
      && (hook.operationKey == null || hook.operationKey === operationKey)
    ));
    if (index < 0) return;
    const hook = this.hooks[index]!;
    hook.remaining -= 1;
    if (hook.remaining <= 0) this.hooks.splice(index, 1);
    throw new CrashInjectedError(point, operationKey);
  }

  clear(): void {
    this.hooks = [];
  }
}

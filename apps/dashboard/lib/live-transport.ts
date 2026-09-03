import type { DashboardSnapshot } from "./types";

export type StreamStatus = "CONNECTING" | "LIVE" | "DEGRADED";

export interface EngineEventSource {
  onopen: ((ev: Event) => void) | null;
  onerror: ((ev: Event) => void) | null;
  addEventListener(type: string, listener: (event: MessageEvent<string>) => void): void;
  close(): void;
}

export interface LiveTransportHandlers {
  onStatus: (status: StreamStatus) => void;
  onSnapshot: (snapshot: DashboardSnapshot) => void;
}

export interface LiveTransportOptions {
  eventsUrl?: string;
  snapshotUrl?: string;
  eventSourceCtor?: new (url: string) => EngineEventSource;
  fetchImpl?: typeof fetch;
}

function asSnapshot(value: unknown): DashboardSnapshot | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  if ("error" in value) return null;
  return value as DashboardSnapshot;
}

/**
 * Single EventSource to `/api/engine/events` plus one bounded snapshot recovery per disconnect.
 * Relies on native EventSource reconnect; does not open a second source or poll.
 * Recovery is scoped to a generation so a late response cannot overwrite a newer SSE snapshot.
 */
export function startEngineLiveTransport(
  handlers: LiveTransportHandlers,
  options: LiveTransportOptions = {},
): { close: () => void } {
  const eventsUrl = options.eventsUrl ?? "/api/engine/events";
  const snapshotUrl = options.snapshotUrl ?? "/api/engine/snapshot";
  const EventSourceImpl = options.eventSourceCtor ?? EventSource;
  const doFetch = options.fetchImpl ?? fetch;

  let disposed = false;
  let generation = 0;
  let recoveredGeneration = -1;
  let recoveryAbort: AbortController | undefined;
  const source = new EventSourceImpl(eventsUrl);

  const invalidateRecovery = () => {
    generation += 1;
    const pending = recoveryAbort;
    recoveryAbort = undefined;
    pending?.abort();
  };

  const recoverSnapshot = () => {
    if (disposed || recoveryAbort || recoveredGeneration === generation) return;
    recoveredGeneration = generation;
    const myGeneration = generation;
    const abort = new AbortController();
    recoveryAbort = abort;
    void doFetch(snapshotUrl, { cache: "no-store", signal: abort.signal })
      .then(async (response) => (response.ok ? asSnapshot(await response.json()) : null))
      .then((snapshot) => {
        if (disposed || myGeneration !== generation || abort.signal.aborted) return;
        if (snapshot) handlers.onSnapshot(snapshot);
      })
      .catch(() => undefined)
      .finally(() => {
        if (recoveryAbort === abort) recoveryAbort = undefined;
      });
  };

  source.onopen = () => {
    if (disposed) return;
    invalidateRecovery();
    handlers.onStatus("LIVE");
  };
  source.onerror = () => {
    if (disposed) return;
    handlers.onStatus("DEGRADED");
    recoverSnapshot();
  };
  source.addEventListener("snapshot", (event) => {
    if (disposed) return;
    try {
      const snapshot = asSnapshot(JSON.parse(event.data) as unknown);
      if (!snapshot) {
        handlers.onStatus("DEGRADED");
        return;
      }
      invalidateRecovery();
      handlers.onSnapshot(snapshot);
      handlers.onStatus("LIVE");
    } catch {
      handlers.onStatus("DEGRADED");
    }
  });

  return {
    close() {
      disposed = true;
      invalidateRecovery();
      source.close();
    },
  };
}

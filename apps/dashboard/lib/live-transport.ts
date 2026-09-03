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
  let recovering = false;
  let recoveryAttempted = false;
  const source = new EventSourceImpl(eventsUrl);

  const recoverSnapshot = () => {
    if (disposed || recovering || recoveryAttempted) return;
    recovering = true;
    recoveryAttempted = true;
    void doFetch(snapshotUrl, { cache: "no-store" })
      .then(async (response) => (response.ok ? asSnapshot(await response.json()) : null))
      .then((snapshot) => {
        if (!disposed && snapshot) handlers.onSnapshot(snapshot);
      })
      .catch(() => undefined)
      .finally(() => {
        recovering = false;
      });
  };

  source.onopen = () => {
    if (disposed) return;
    recoveryAttempted = false;
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
      handlers.onSnapshot(snapshot);
      handlers.onStatus("LIVE");
    } catch {
      handlers.onStatus("DEGRADED");
    }
  });

  return {
    close() {
      disposed = true;
      source.close();
    },
  };
}

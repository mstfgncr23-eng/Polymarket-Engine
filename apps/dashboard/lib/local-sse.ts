import { dashboardCache, type SnapshotCache } from "./snapshot-cache";

/** Local transport only. Slow/disconnected clients release their subscription. */
export function localSnapshotSse(request: Request, cache: SnapshotCache = dashboardCache()): Response {
  const encoder = new TextEncoder();
  let cleanup = () => {};
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      let closed = false;
      let unsubscribe = () => {};
      let heartbeat: ReturnType<typeof setInterval> | undefined;
      let lastSync: string | null = null;
      cleanup = () => {
        if (closed) return;
        closed = true;
        unsubscribe();
        clearInterval(heartbeat);
        request.signal.removeEventListener("abort", cleanup);
        try { controller.close(); } catch { /* Already cancelled. */ }
      };
      const send = (event: string, value: unknown) => {
        if (closed) return;
        if ((controller.desiredSize ?? 0) <= 0) { cleanup(); return; }
        controller.enqueue(encoder.encode(`event: ${event}\ndata: ${JSON.stringify(value)}\n\n`));
      };
      const snapshot = (value: NonNullable<ReturnType<SnapshotCache["peek"]>>) => {
        if (lastSync === value.cache!.lastUpstreamSync) return;
        lastSync = value.cache!.lastUpstreamSync;
        send("snapshot", value);
      };
      if (request.signal.aborted) { cleanup(); return; }
      request.signal.addEventListener("abort", cleanup, { once: true });
      unsubscribe = cache.subscribe(snapshot);
      void cache.read().then((value) => { if (value) snapshot(value); });
      heartbeat = setInterval(() => { send("cache", cache.metadata()); }, 5_000);
      heartbeat.unref?.();
    },
    cancel() { cleanup(); },
  }, { highWaterMark: 2 });
  return new Response(body, { headers: {
    "content-type": "text/event-stream", "cache-control": "no-cache, no-transform",
    connection: "keep-alive", "x-accel-buffering": "no",
  } });
}

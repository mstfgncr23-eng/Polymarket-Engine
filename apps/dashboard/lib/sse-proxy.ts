import { engineInternalUrl, engineReadHeaders, engineUnreachableResponse } from "./engine";

/** Default SSE proxy lifetime. Covers Fly connect + streaming, with margin under Vercel's ~300s kill. */
export const DEFAULT_SSE_PROXY_MAX_MS = 240_000;

/** Hard cap for env/override. Must stay below the observed 300s Vercel runtime kill. */
export const SSE_PROXY_HARD_CAP_MS = 270_000;

const SSE_DOWNSTREAM_HEADERS = {
  "content-type": "text/event-stream",
  "cache-control": "no-cache, no-transform",
  connection: "keep-alive",
  "x-accel-buffering": "no",
} as const;

function clampSseProxyMaxMs(value: number): number {
  if (!Number.isFinite(value) || value <= 0) return DEFAULT_SSE_PROXY_MAX_MS;
  return Math.min(Math.floor(value), SSE_PROXY_HARD_CAP_MS);
}

export function resolveSseProxyMaxMs(env: NodeJS.ProcessEnv = process.env, override?: number): number {
  if (typeof override === "number") return clampSseProxyMaxMs(override);
  const raw = env.SSE_PROXY_MAX_MS;
  if (raw) {
    const parsed = Number(raw);
    if (Number.isFinite(parsed) && parsed > 0) return clampSseProxyMaxMs(parsed);
  }
  return DEFAULT_SSE_PROXY_MAX_MS;
}

export interface ProxyEngineSseOptions {
  maxMs?: number;
  fetchImpl?: typeof fetch;
}

/**
 * Authenticated Fly `/events` proxy with a deterministic lifetime that starts before
 * the upstream fetch (DNS/TLS/headers + streaming) and pull-driven backpressure.
 */
export async function proxyEngineSse(request: Request, options: ProxyEngineSseOptions = {}): Promise<Response> {
  const maxMs = resolveSseProxyMaxMs(process.env, options.maxMs);
  const doFetch = options.fetchImpl ?? fetch;
  const rotation = new AbortController();
  const signal = AbortSignal.any([request.signal, rotation.signal]);
  let timer: ReturnType<typeof setTimeout> | undefined;
  let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  let upstreamBody: ReadableStream<Uint8Array> | undefined;
  let downstream: ReadableStreamDefaultController<Uint8Array> | undefined;
  let closed = false;
  let cleaned = false;

  const closeDownstream = (comment?: string) => {
    if (closed || !downstream) return;
    closed = true;
    if (comment) {
      try {
        downstream.enqueue(new TextEncoder().encode(`: ${comment}\n\n`));
      } catch {
        // controller already closed
      }
    }
    try {
      downstream.close();
    } catch {
      // controller already closed
    }
  };

  const onClientAbort = () => {
    cleanup();
  };

  const cleanup = () => {
    if (cleaned) return;
    cleaned = true;
    if (timer !== undefined) {
      clearTimeout(timer);
      timer = undefined;
    }
    request.signal.removeEventListener("abort", onClientAbort);
    const pendingReader = reader;
    reader = undefined;
    const pendingBody = upstreamBody;
    upstreamBody = undefined;
    void pendingReader?.cancel("sse-proxy-cleanup").catch(() => undefined);
    if (!pendingReader) void pendingBody?.cancel("sse-proxy-cleanup").catch(() => undefined);
    if (!rotation.signal.aborted) rotation.abort();
  };

  if (request.signal.aborted) {
    cleanup();
    return engineUnreachableResponse();
  }
  request.signal.addEventListener("abort", onClientAbort);

  timer = setTimeout(() => {
    console.info(JSON.stringify({ component: "dashboard-sse-proxy", event: "rotate", maxMs }));
    closeDownstream("sse-proxy-rotation");
    cleanup();
  }, maxMs);

  try {
    const upstream = await doFetch(`${engineInternalUrl()}/events`, {
      headers: engineReadHeaders({
        accept: "text/event-stream",
        "last-event-id": request.headers.get("last-event-id") ?? "",
      }),
      cache: "no-store",
      signal,
    });
    if (!upstream.ok || !upstream.body || signal.aborted) {
      cleanup();
      return engineUnreachableResponse();
    }

    upstreamBody = upstream.body;
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        downstream = controller;
        if (signal.aborted) {
          closeDownstream("sse-proxy-rotation");
          cleanup();
        }
      },
      async pull(controller) {
        if (closed || signal.aborted) {
          closeDownstream(signal.aborted ? "sse-proxy-rotation" : undefined);
          cleanup();
          return;
        }
        try {
          reader ??= upstreamBody?.getReader();
          if (!reader) {
            closeDownstream();
            cleanup();
            return;
          }
          const { done, value } = await reader.read();
          if (closed) return;
          if (done) {
            closeDownstream();
            cleanup();
            return;
          }
          if (value) controller.enqueue(value);
        } catch {
          if (closed) return;
          if (signal.aborted) closeDownstream("sse-proxy-rotation");
          else {
            closed = true;
            try {
              controller.error(new Error("upstream_sse_failed"));
            } catch {
              // controller already closed
            }
          }
          cleanup();
        }
      },
      cancel() {
        cleanup();
      },
    }, { highWaterMark: 1 });

    return new Response(stream, {
      headers: {
        ...SSE_DOWNSTREAM_HEADERS,
        "x-sse-proxy-max-ms": String(maxMs),
      },
    });
  } catch {
    cleanup();
    return engineUnreachableResponse();
  }
}

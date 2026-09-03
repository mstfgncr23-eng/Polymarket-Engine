import { engineInternalUrl, engineReadHeaders, engineUnreachableResponse } from "./engine";

/** Default SSE proxy lifetime. Leaves a 60s margin under Vercel's observed 300s runtime kill. */
export const DEFAULT_SSE_PROXY_MAX_MS = 240_000;

const SSE_DOWNSTREAM_HEADERS = {
  "content-type": "text/event-stream",
  "cache-control": "no-cache, no-transform",
  connection: "keep-alive",
  "x-accel-buffering": "no",
} as const;

export function resolveSseProxyMaxMs(env: NodeJS.ProcessEnv = process.env, override?: number): number {
  if (typeof override === "number" && Number.isFinite(override) && override > 0) return override;
  const raw = env.SSE_PROXY_MAX_MS;
  if (raw) {
    const parsed = Number(raw);
    if (Number.isFinite(parsed) && parsed > 0) return parsed;
  }
  return DEFAULT_SSE_PROXY_MAX_MS;
}

export interface ProxyEngineSseOptions {
  maxMs?: number;
  fetchImpl?: typeof fetch;
}

/**
 * Authenticated Fly `/events` proxy with a deterministic downstream lifetime.
 * Closes the stream and aborts the upstream reader before Vercel runtime termination.
 */
export async function proxyEngineSse(request: Request, options: ProxyEngineSseOptions = {}): Promise<Response> {
  const maxMs = resolveSseProxyMaxMs(process.env, options.maxMs);
  const doFetch = options.fetchImpl ?? fetch;
  const rotation = new AbortController();
  const signal = AbortSignal.any([request.signal, rotation.signal]);
  let timer: ReturnType<typeof setTimeout> | undefined;
  let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  let cleaned = false;

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
    // Cancel the upstream reader before aborting fetch so the source `cancel()`
    // algorithm runs instead of losing it to an already-errored stream.
    void pendingReader?.cancel("sse-proxy-cleanup").catch(() => undefined);
    if (!rotation.signal.aborted) rotation.abort();
  };

  if (request.signal.aborted) {
    cleanup();
    return engineUnreachableResponse();
  }
  request.signal.addEventListener("abort", onClientAbort);

  try {
    const upstream = await doFetch(`${engineInternalUrl()}/events`, {
      headers: engineReadHeaders({
        accept: "text/event-stream",
        "last-event-id": request.headers.get("last-event-id") ?? "",
      }),
      cache: "no-store",
      signal,
    });
    if (!upstream.ok || !upstream.body) {
      cleanup();
      return engineUnreachableResponse();
    }

    const body = upstream.body;
    const stream = new ReadableStream<Uint8Array>({
      async start(controller) {
        let closed = false;
        const closeDownstream = (comment?: string) => {
          if (closed) return;
          closed = true;
          if (comment) {
            try {
              controller.enqueue(new TextEncoder().encode(`: ${comment}\n\n`));
            } catch {
              // controller already closed
            }
          }
          try {
            controller.close();
          } catch {
            // controller already closed
          }
        };

        reader = body.getReader();
        timer = setTimeout(() => {
          console.info(JSON.stringify({ component: "dashboard-sse-proxy", event: "rotate", maxMs }));
          closeDownstream("sse-proxy-rotation");
          cleanup();
        }, maxMs);

        try {
          while (!signal.aborted && !closed) {
            const { done, value } = await reader.read();
            if (done || closed) break;
            if (value) controller.enqueue(value);
          }
          closeDownstream();
        } catch {
          if (signal.aborted) closeDownstream();
          else {
            closed = true;
            try {
              controller.error(new Error("upstream_sse_failed"));
            } catch {
              // controller already closed
            }
          }
        } finally {
          cleanup();
        }
      },
      cancel() {
        cleanup();
      },
    });

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

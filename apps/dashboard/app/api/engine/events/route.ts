import { engineInternalUrl, engineReadHeaders, engineUnreachableResponse } from "../../../../lib/engine";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET(request: Request) {
  const base = engineInternalUrl();
  try {
    const upstream = await fetch(`${base}/events`, {
      headers: engineReadHeaders({
        accept: "text/event-stream",
        "last-event-id": request.headers.get("last-event-id") ?? "",
      }),
      cache: "no-store",
      signal: request.signal,
    });
    if (!upstream.ok || !upstream.body) return engineUnreachableResponse();
    return new Response(upstream.body, {
      headers: {
        "content-type": "text/event-stream",
        "cache-control": "no-cache, no-transform",
        connection: "keep-alive",
        "x-accel-buffering": "no",
      },
    });
  } catch {
    return engineUnreachableResponse();
  }
}

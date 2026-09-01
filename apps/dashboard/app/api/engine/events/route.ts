export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET(request: Request) {
  const base = process.env.ENGINE_INTERNAL_URL ?? "http://127.0.0.1:8080";
  try {
    const upstream = await fetch(`${base}/events`, { headers: { accept: "text/event-stream", "last-event-id": request.headers.get("last-event-id") ?? "" }, cache: "no-store", signal: request.signal });
    if (!upstream.ok || !upstream.body) return new Response("Engine event stream unavailable", { status: 503 });
    return new Response(upstream.body, { headers: { "content-type": "text/event-stream", "cache-control": "no-cache, no-transform", connection: "keep-alive", "x-accel-buffering": "no" } });
  } catch {
    return new Response("Engine event stream unavailable", { status: 503 });
  }
}

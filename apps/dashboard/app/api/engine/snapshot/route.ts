export const dynamic = "force-dynamic";
export const runtime = "nodejs";

function engineHeaders(): HeadersInit {
  const token = process.env.ENGINE_READ_TOKEN;
  return token ? { authorization: `Bearer ${token}` } : {};
}

export async function GET() {
  const base = process.env.ENGINE_INTERNAL_URL ?? "http://127.0.0.1:8080";
  try {
    const upstream = await fetch(`${base}/api/dashboard`, {
      cache: "no-store",
      signal: AbortSignal.timeout(4_000),
      headers: engineHeaders(),
    });
    return new Response(await upstream.text(), { status: upstream.status, headers: { "content-type": "application/json", "cache-control": "no-store" } });
  } catch {
    return Response.json({ error: "ENGINE_UNREACHABLE" }, { status: 503, headers: { "cache-control": "no-store" } });
  }
}

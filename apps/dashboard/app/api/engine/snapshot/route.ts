import { engineInternalUrl, engineReadHeaders, engineUnreachableResponse } from "../../../../lib/engine";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET() {
  const base = engineInternalUrl();
  try {
    const upstream = await fetch(`${base}/api/dashboard`, {
      cache: "no-store",
      signal: AbortSignal.timeout(4_000),
      headers: engineReadHeaders(),
    });
    return new Response(await upstream.text(), { status: upstream.status, headers: { "content-type": "application/json", "cache-control": "no-store" } });
  } catch {
    return engineUnreachableResponse();
  }
}

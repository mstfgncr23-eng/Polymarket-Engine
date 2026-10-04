import { dashboardCache } from "../../lib/snapshot-cache";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET() {
  const cache = dashboardCache();
  const snapshot = await cache.read();
  const metadata = cache.metadata();
  return Response.json({ status: snapshot && !metadata.stale ? "ready" : "not_ready", cache: metadata }, {
    status: snapshot && !metadata.stale ? 200 : 503, headers: { "cache-control": "no-store" },
  });
}

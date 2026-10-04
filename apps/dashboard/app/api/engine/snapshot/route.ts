import { engineUnreachableResponse } from "../../../../lib/engine";
import { dashboardCache } from "../../../../lib/snapshot-cache";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET() {
  const snapshot = await dashboardCache().read();
  return snapshot ? Response.json(snapshot, { headers: { "cache-control": "no-store" } }) : engineUnreachableResponse();
}

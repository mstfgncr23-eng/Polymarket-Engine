import { localSnapshotSse } from "../../../../lib/local-sse";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET(request: Request) {
  return localSnapshotSse(request);
}

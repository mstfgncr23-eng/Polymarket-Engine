import { proxyEngineSse } from "../../../../lib/sse-proxy";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET(request: Request) {
  return proxyEngineSse(request);
}

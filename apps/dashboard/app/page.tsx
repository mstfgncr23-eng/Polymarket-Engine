import Dashboard from "./dashboard";
import { engineInternalUrl, engineReadHeaders } from "../lib/engine";
import type { DashboardSnapshot } from "../lib/types";

export const dynamic = "force-dynamic";

async function initialSnapshot(): Promise<DashboardSnapshot | null> {
  const base = engineInternalUrl();
  try {
    const response = await fetch(`${base}/api/dashboard`, {
      cache: "no-store",
      signal: AbortSignal.timeout(3_000),
      headers: engineReadHeaders(),
    });
    return response.ok ? await response.json() as DashboardSnapshot : null;
  } catch {
    return null;
  }
}

export default async function Page() {
  return <Dashboard initial={await initialSnapshot()} />;
}

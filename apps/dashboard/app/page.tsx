import Dashboard from "./dashboard";
import type { DashboardSnapshot } from "../lib/types";

export const dynamic = "force-dynamic";

async function initialSnapshot(): Promise<DashboardSnapshot | null> {
  const base = process.env.ENGINE_INTERNAL_URL ?? "http://127.0.0.1:8080";
  try {
    const response = await fetch(`${base}/api/dashboard`, { cache: "no-store", signal: AbortSignal.timeout(3_000) });
    return response.ok ? await response.json() as DashboardSnapshot : null;
  } catch {
    return null;
  }
}

export default async function Page() {
  return <Dashboard initial={await initialSnapshot()} />;
}

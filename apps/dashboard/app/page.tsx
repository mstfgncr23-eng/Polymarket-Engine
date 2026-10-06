import Dashboard from "./dashboard";
import { initialSnapshot } from "../lib/snapshot-cache";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export default async function Page() {
  return <Dashboard initial={await initialSnapshot()} />;
}

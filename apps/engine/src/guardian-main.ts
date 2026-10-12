import { runGuardian } from "./guardian-worker.js";

if (process.env.TRADING_MODE !== "paper") throw new Error("GUARDIAN_PAPER_ONLY");
const path = process.env.GUARDIAN_SOCKET_PATH;
if (!path) throw new Error("GUARDIAN_SOCKET_PATH_REQUIRED");
const stop = new AbortController();
process.once("SIGINT", () => stop.abort());
process.once("SIGTERM", () => stop.abort());
await runGuardian(path, stop.signal);

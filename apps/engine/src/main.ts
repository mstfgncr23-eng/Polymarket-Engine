import { startRuntime } from "./runtime.js";

const runtime = await startRuntime();
let shuttingDown = false;

async function shutdown(signal: string): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  runtime.server.log.info({ signal }, "graceful shutdown requested");
  const deadline = setTimeout(() => {
    runtime.server.log.error("graceful shutdown deadline exceeded");
    process.exitCode = 1;
  }, 55_000);
  deadline.unref();
  await runtime.stop();
  clearTimeout(deadline);
}

process.once("SIGTERM", () => void shutdown("SIGTERM"));
process.once("SIGINT", () => void shutdown("SIGINT"));

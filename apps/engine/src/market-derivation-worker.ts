import { parentPort, workerData } from "node:worker_threads";
import { deriveBookFromSides } from "@pm/core";
import {
  MARKET_DERIVATION_WORKER_DERIVED,
  MARKET_DERIVATION_WORKER_ERROR,
  MARKET_DERIVATION_WORKER_READY,
  isMarketDerivationRequest,
  type MarketDerivationWorkerData,
} from "./market-derivation-protocol.js";

if (!parentPort) throw new Error("MARKET_DERIVATION_WORKER_NO_PARENT_PORT");

const data = (workerData ?? {}) as MarketDerivationWorkerData;
const mode = data.mode;
const delayMs = typeof data.delayMs === "number" && data.delayMs > 0 ? data.delayMs : 0;

if (mode === "throw-on-start") throw new Error("MARKET_DERIVATION_WORKER_TEST_THROW");
if (mode === "exit-on-start") process.exit(2);
if (mode === "hang-on-start") {
  /* Intentionally never ready. */
} else {
  parentPort.postMessage({ type: MARKET_DERIVATION_WORKER_READY });
}

parentPort.on("message", (raw: unknown) => {
  if (mode === "throw-on-message") throw new Error("MARKET_DERIVATION_WORKER_TEST_THROW_ON_MESSAGE");
  if (mode === "hang" || mode === "hang-on-start") return;
  if (mode === "malformed") {
    parentPort!.postMessage({ garbage: true });
    return;
  }
  if (!isMarketDerivationRequest(raw)) {
    parentPort!.postMessage({
      type: MARKET_DERIVATION_WORKER_ERROR,
      id: typeof raw === "object" && raw && "id" in raw && typeof (raw as { id: unknown }).id === "number"
        ? (raw as { id: number }).id
        : -1,
      message: "MARKET_DERIVATION_WORKER_MALFORMED_REQUEST",
    });
    return;
  }
  const respond = (): void => {
    try {
      const derived = deriveBookFromSides({ bids: raw.bids, asks: raw.asks });
      parentPort!.postMessage({ type: MARKET_DERIVATION_WORKER_DERIVED, id: raw.id, derived });
    } catch (error) {
      parentPort!.postMessage({
        type: MARKET_DERIVATION_WORKER_ERROR,
        id: raw.id,
        message: error instanceof Error ? error.message : "MARKET_DERIVATION_WORKER_DERIVE_FAILED",
      });
    }
  };
  if (delayMs > 0) setTimeout(respond, delayMs);
  else respond();
});

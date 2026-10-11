import { afterEach, describe, expect, it } from "vitest";
import { fork, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import { guardianProtectionRequestSchema } from "@pm/core";
import { snapshot } from "./fixtures.js";

const childFile = fileURLToPath(new URL("../apps/engine/src/guardian-offline-child.ts", import.meta.url));
const t0 = Date.parse("2026-08-31T12:00:00.000Z");
const active = new Set<ChildProcess>();
const boot = (open = true) => ({
  version: 1 as const, mode: "PAPER" as const,
  authorityId: "offline-test-authority", experimentId: "offline-test-experiment",
  checkpointContentHash: "a".repeat(64), cursor: 44,
  open: open ? [{ episode: {
    marketId: "g2i-market", positionId: "g2i-position",
    unhedgedStartedAt: new Date(t0).toISOString(), heldOutcome: "UP" as const,
  }, residualQty: "5", entryPrice: "0.9" }] : [],
  settledMarkets: [] as string[],
});
function observed(id: number, afterMs: number, heldBid: string, heldAsk: string,
  opts: { seq?: number; open?: boolean; delayUpMs?: number; upToken?: string } = {}) {
  const at = new Date(t0 + afterMs).toISOString();
  const up = snapshot({
    marketId: "g2i-market", tokenId: opts.upToken ?? "up-token",
    outcome: "UP", eventId: "u-" + id,
    sourceTimestamp: new Date(t0 + afterMs - (opts.delayUpMs ?? 0)).toISOString(),
    receivedAt: new Date(t0 + afterMs - (opts.delayUpMs ?? 0)).toISOString(),
    bids: [{ price: heldBid, quantity: "10" }],
    asks: [{ price: heldAsk, quantity: "90" }],
  });
  const down = snapshot({
    marketId: "g2i-market", tokenId: "down-token", outcome: "DOWN", eventId: "d-" + id,
    sourceTimestamp: at, receivedAt: at,
    bids: [{ price: "0.87", quantity: "90" }],
    asks: [{ price: "0.91", quantity: "10" }],
  });
  return {
    kind: "OBSERVE" as const, id, bootstrap: boot(opts.open ?? true),
    paired: { status: "READY" as const, generation: 1,
      ingressSequence: opts.seq ?? id, up, down },
    now: at, maxAgeMs: 2_000, maxSkewMs: 1_000,
  };
}
type Message = { kind: string; id?: number; status: string; pid: number;
  reason?: string; request?: unknown; events?: string[] };
function nextMessage(child: ChildProcess, send?: unknown): Promise<Message> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => { cleanup(); reject(Error("GUARDIAN_CHILD_RESPONSE_TIMEOUT")); }, 12_000);
    const cleanup = () => {
      clearTimeout(timeout); child.off("message", onMessage);
      child.off("exit", onExit); child.off("error", onError);
    };
    const onMessage = (value: unknown) => { cleanup(); resolve(value as Message); };
    const onExit = (code: number | null) => { cleanup(); reject(Error("GUARDIAN_CHILD_EXIT_" + code)); };
    const onError = (error: Error) => { cleanup(); reject(error); };
    child.once("message", onMessage);
    child.once("exit", onExit);
    child.once("error", onError);
    if (send !== undefined) child.send(send, error => { if (error) { cleanup(); reject(error); } });
  });
}
async function launch() {
  const child = fork(childFile, [], {
    cwd: process.cwd(), execArgv: ["--import", "tsx"],
    env: { ...process.env, PM_GUARDIAN_OFFLINE_CHILD: "1" },
    stdio: ["ignore", "ignore", "pipe", "ipc"],
  });
  active.add(child);
  let errors = "";
  child.stderr?.on("data", data => { errors += String(data); });
  const ready = await nextMessage(child).catch(error => { throw Error(String(error) + "\n" + errors); });
  expect(ready).toMatchObject({ kind: "READY", status: "OFFLINE_ONLY" });
  expect(ready.pid).not.toBe(process.pid);
  return child;
}
afterEach(async () => {
  await Promise.all([...active].map(async child => {
    if (child.exitCode === null) {
      const stopped = new Promise<void>(resolve => child.once("exit", () => resolve()));
      child.kill(); await stopped;
    }
    active.delete(child);
  }));
});
describe("G2I first truly separate OFFLINE Guardian process", () => {
  it("evaluates existing V2 in a different PID; NO_ACTION then typed REQUEST, never an order", async () => {
    const child = await launch();
    const first = await nextMessage(child, observed(1, 0, "0.50", "0.60"));
    expect(first).toMatchObject({ id: 1, status: "NO_ACTION" });
    const second = await nextMessage(child, observed(2, 11_000, "0.09", "0.11"));
    expect(second.status).toBe("REQUEST");
    expect(second.pid).not.toBe(process.pid);
    const request = guardianProtectionRequestSchema.parse(second.request);
    expect(request).toMatchObject({
      mode: "PAPER", policyGeneration: 3, experimentId: "offline-test-experiment",
      expectedResidualQty: "5", sourceDurableCursor: 44,
      decisionReason: "EARLY_EMERGENCY", requestedKind: "PROTECTIVE_SELL",
      episode: { marketId: "g2i-market", positionId: "g2i-position" },
    });
    expect(second.events).toContain("EARLY_EMERGENCY_PROTECT");
    const repeated = await nextMessage(child, observed(3, 12_000, "0.09", "0.11"));
    expect(repeated.status).toBe("NO_ACTION");
  });

  it("keeps unverified-stale books from causing any risk decision", async () => {
    const child = await launch();
    const result = await nextMessage(child, observed(1, 11_000, "0.09", "0.11",
      { delayUpMs: 4_000 }));
    expect(result).toMatchObject({ status: "NO_ACTION", reason: "BOOK_STALE_OR_FUTURE" });
  });

  it("latches fail-closed after duplicate IPC sequence or token rotation", async () => {
    const child = await launch();
    expect((await nextMessage(child, observed(1, 0, "0.50", "0.60"))).status).toBe("NO_ACTION");
    expect((await nextMessage(child, observed(2, 11_000, "0.09", "0.11",
      { seq: 1 }))).status).toBe("HALTED");
    expect((await nextMessage(child, observed(3, 12_000, "0.09", "0.11"))).status).toBe("HALTED");
  });

  it("will not request protection without an actual open directional position", async () => {
    const child = await launch();
    expect(await nextMessage(child, observed(1, 0, "0.09", "0.11",
      { open: false }))).toMatchObject({ status: "NO_ACTION", reason: "NO_OPEN_POSITION" });
  });

  it("is a child that can terminate and start cleanly, not an Engine worker thread", async () => {
    const first = await launch();
    const a = await nextMessage(first, observed(1, 0, "0.50", "0.60"));
    first.kill();
    await new Promise<void>(resolve => first.once("exit", () => resolve()));
    active.delete(first);
    const second = await launch();
    const b = await nextMessage(second, observed(1, 0, "0.50", "0.60"));
    expect(b.status).toBe("NO_ACTION");
    expect(b.pid).not.toBe(a.pid);
  });
});

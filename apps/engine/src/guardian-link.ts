import { createConnection, createServer, type Server, type Socket } from "node:net";
import { chmod, mkdir, unlink } from "node:fs/promises";
import { dirname } from "node:path";
import type { GuardianGeneration, GuardianV2Event, GuardianV2State, MarketSnapshot, Position } from "@pm/core";

export interface GuardianMarket {
  position: Position;
  state: GuardianV2State | null;
  up: MarketSnapshot | null;
  down: MarketSnapshot | null;
  synchronized: boolean;
  admission?: GuardianV2State;
  consensus?: { eventId: string; direction: "UP" | "DOWN" | "FLAT"; confidence: number; estimatedEdge: number;
    calculatedAt?: string; expiresAt?: string };
}
export interface GuardianView {
  mode: "paper";
  enginePid: number;
  engineStartedAt: string;
  experimentId: string | null;
  now: string;
  enabled: boolean;
  generation: GuardianGeneration | null;
  staleMs: number;
  feed: { state: string; lastMessageAt: string | null; reconnects: number };
  markets: GuardianMarket[];
}
export interface GuardianUpdate {
  experimentId: string;
  state: GuardianV2State;
  events: GuardianV2Event[];
  observed?: Record<"up" | "down", { tokenId: string; eventId: string; sequence: number; sourceTimestamp: string; receivedAt: string }>;
}

export function currentGuardianConsensus(consensus: GuardianMarket["consensus"], nowMs: number, staleMs: number) {
  return consensus && Number.isFinite(Date.parse(consensus.calculatedAt ?? ""))
    && Date.parse(consensus.calculatedAt!) <= nowMs && nowMs - Date.parse(consensus.calculatedAt!) <= staleMs
    && Date.parse(consensus.expiresAt ?? "") > nowMs ? consensus : undefined;
}

const MAX_BYTES = 4 * 1024 * 1024;
/** One local request/reply socket; no external feed or economic writer here. */
export function guardianRpc<T>(path: string, request: unknown): Promise<T> {
  return new Promise((resolve, reject) => {
    const socket = createConnection(path);
    let buffer = "", complete = false;
    const fail = (error: Error) => { if (!complete) { complete = true; socket.destroy(); reject(error); } };
    socket.setTimeout(5000, () => fail(new Error("GUARDIAN_SOCKET_TIMEOUT")));
    socket.on("error", fail);
    socket.on("close", () => { if (!complete) fail(new Error("GUARDIAN_SOCKET_CLOSED")); });
    socket.on("connect", () => socket.write(JSON.stringify({ ...(request as object), sentAtMs: Date.now() }) + "\n"));
    socket.setEncoding("utf8");
    socket.on("data", chunk => {
      buffer += chunk;
      if (Buffer.byteLength(buffer) > MAX_BYTES) return fail(new Error("GUARDIAN_FRAME_TOO_LARGE"));
      if (!buffer.includes("\n")) return;
      try {
        const reply = JSON.parse(buffer.slice(0, buffer.indexOf("\n")));
        if (reply.error) return fail(new Error(reply.error));
        complete = true; socket.destroy(); resolve(reply as T);
      } catch (error) { fail(error as Error); }
    });
  });
}

export async function startGuardianSocket(path: string, handler: (input: unknown) => Promise<unknown>): Promise<{
  stop(): Promise<void>;
}> {
  const sockets = new Set<Socket>();
  if (process.platform !== "win32") {
    await mkdir(dirname(path), { recursive: true });
    // Refuse a live listener; remove only a stale socket left by a stopped Engine.
    try { await guardianRpc(path, { kind: "VIEW" }); throw new Error("GUARDIAN_SOCKET_ALREADY_IN_USE"); }
    catch (error) {
      const code = (error as NodeJS.ErrnoException).code;
      if (code !== "ENOENT" && code !== "ECONNREFUSED") throw error;
    }
    await unlink(path).catch(error => { if (error.code !== "ENOENT") throw error; });
  }
  const server: Server = createServer(socket => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    socket.on("error", () => undefined);
    socket.setTimeout(10000, () => socket.destroy());
    socket.setEncoding("utf8");
    let buffer = "", handled = false;
    socket.on("data", chunk => {
      if (handled) return;
      buffer += chunk;
      if (Buffer.byteLength(buffer) > MAX_BYTES) { socket.destroy(); return; }
      if (!buffer.includes("\n")) return;
      handled = true;
      void (async () => {
        try {
          const input = JSON.parse(buffer.slice(0, buffer.indexOf("\n")));
          const started = performance.now();
          const engineWaitMs = typeof input.sentAtMs === "number" ? Math.max(0, Date.now() - input.sentAtMs) : null;
          const reply = await handler(input);
          socket.end(JSON.stringify({ ...(reply as object), timing: { engineWaitMs, handlerMs: performance.now() - started } }) + "\n");
        }
        catch (error) { socket.end(JSON.stringify({ error: (error as Error).message }) + "\n"); }
      })();
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(path, () => { server.off("error", reject); resolve(); });
  });
  if (process.platform !== "win32") await chmod(path, 0o660);
  return { async stop() {
    for (const socket of sockets) socket.destroy();
    await new Promise<void>(resolve => server.close(() => resolve()));
    if (process.platform !== "win32") await unlink(path).catch(() => undefined);
  } };
}

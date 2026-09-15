import { existsSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { Worker } from "node:worker_threads";
import { describe, expect, it } from "vitest";
import { deriveBook, type BookDerived } from "@pm/core";
import { snapshot } from "./fixtures.js";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");

describe("PR52 worker build artifact", () => {
  it("emits a dedicated production worker entry next to dist/main.js", () => {
    const build = readFileSync(join(repoRoot, "apps/engine/build.mjs"), "utf8");
    expect(build).toMatch(/market-derivation-worker\.ts/);
    expect(build).toMatch(/market-derivation-worker\.js/);
    expect(build).toMatch(/src\/main\.ts/);
    const dockerfile = readFileSync(join(repoRoot, "Dockerfile.engine"), "utf8");
    expect(dockerfile).toMatch(/COPY --from=build[^\n]*\/workspace\/apps\/engine\/dist \.\/dist/);
    expect(dockerfile).toMatch(/CMD \["node", "dist\/main\.js"\]/);
    expect(dockerfile).not.toMatch(/tsx/);
  });

  it("can launch the built worker artifact when dist exists", async () => {
    const filename = join(repoRoot, "apps/engine/dist/market-derivation-worker.js");
    if (!existsSync(filename)) return;
    const worker = new Worker(filename);
    const ready = await new Promise<boolean>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("BUILT_WORKER_READY_TIMEOUT")), 2_000);
      worker.once("message", (raw: { type?: string }) => {
        clearTimeout(timer);
        resolve(raw?.type === "ready");
      });
      worker.once("error", reject);
    });
    expect(ready).toBe(true);
    const book = snapshot({ bids: [{ price: "0.40", quantity: "10" }], asks: [{ price: "0.50", quantity: "2" }] });
    const derived = await new Promise<BookDerived>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("BUILT_WORKER_DERIVE_TIMEOUT")), 2_000);
      worker.once("message", (raw: { type?: string; derived?: BookDerived }) => {
        clearTimeout(timer);
        if (raw?.type === "derived" && raw.derived) resolve(raw.derived);
        else reject(new Error("BUILT_WORKER_BAD_RESPONSE"));
      });
      worker.postMessage({
        type: "derive",
        id: 1,
        bids: book.bids.map((level) => ({ price: level.price, quantity: level.quantity })),
        asks: book.asks.map((level) => ({ price: level.price, quantity: level.quantity })),
      });
    });
    expect(derived).toEqual(deriveBook(book));
    await worker.terminate();
  });
});

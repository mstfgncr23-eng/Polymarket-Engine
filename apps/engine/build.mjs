import { build } from "esbuild";
import { fileURLToPath } from "node:url";

const banner = {
  js: "import { createRequire as __createRequire } from 'node:module'; const require = __createRequire(import.meta.url);",
};

const shared = {
  bundle: true,
  platform: "node",
  target: "node22",
  format: "esm",
  banner,
  sourcemap: true,
};

await Promise.all([
  build({
    ...shared,
    entryPoints: [fileURLToPath(new URL("./src/main.ts", import.meta.url))],
    outfile: fileURLToPath(new URL("./dist/main.js", import.meta.url)),
  }),
  build({
    ...shared,
    entryPoints: [fileURLToPath(new URL("./src/market-derivation-worker.ts", import.meta.url))],
    outfile: fileURLToPath(new URL("./dist/market-derivation-worker.js", import.meta.url)),
  }),
]);

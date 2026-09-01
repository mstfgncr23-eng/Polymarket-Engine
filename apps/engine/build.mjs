import { build } from "esbuild";
import { fileURLToPath } from "node:url";

await build({
  entryPoints: [fileURLToPath(new URL("./src/main.ts", import.meta.url))],
  bundle: true,
  platform: "node",
  target: "node22",
  format: "esm",
  banner: { js: "import { createRequire as __createRequire } from 'node:module'; const require = __createRequire(import.meta.url);" },
  outfile: fileURLToPath(new URL("./dist/main.js", import.meta.url)),
  sourcemap: true,
});

import { defineConfig } from "vitest/config";
import { fileURLToPath } from "node:url";

export default defineConfig({
  resolve: {
    alias: {
      "@pm/core": fileURLToPath(new URL("./packages/core/src/index.ts", import.meta.url)),
      "@pm/db": fileURLToPath(new URL("./packages/db/src/index.ts", import.meta.url)),
      "@pm/market-data": fileURLToPath(new URL("./packages/market-data/src/index.ts", import.meta.url)),
    },
  },
  test: {
    include: ["packages/**/*.test.ts", "apps/**/*.test.ts", "tests/**/*.test.ts"],
    environment: "node",
    coverage: { reporter: ["text", "json", "html"] },
  },
});

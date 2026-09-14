/**
 * Thin ESM wrapper ? authoritative runner is galaxy-runtime-proof.cjs
 * (Playwright + standalone Next harness at /galaxy-harness).
 */
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
require("./galaxy-runtime-proof.cjs");

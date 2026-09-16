import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, "../..");
const manifest = JSON.parse(readFileSync(path.join(here, "manifest.json"), "utf8"));

function writeConcat(destRel, parts, sha256, encoding) {
  const chunks = parts.map((p) => readFileSync(path.join(here, p), "utf8"));
  let text;
  if (encoding === "base64") {
    text = Buffer.concat(chunks.map((c) => Buffer.from(c.trim(), "base64"))).toString("utf8");
  } else {
    text = chunks.join("");
  }
  const sha = createHash("sha256").update(text).digest("hex");
  if (sha !== sha256) throw new Error(`SHA mismatch for ${destRel}: ${sha}`);
  const dest = path.join(root, destRel);
  mkdirSync(path.dirname(dest), { recursive: true });
  writeFileSync(dest, text);
  console.log(`wrote ${destRel} (${Buffer.byteLength(text, "utf8")} bytes)`);
}

for (const file of manifest.files) {
  writeConcat(file.dest, file.parts, file.sha256, file.encoding || "utf8");
}

const patchPath = path.join(here, manifest.enginePatch);
try {
  execFileSync("git", ["apply", "--check", "--whitespace=nowarn", patchPath], { cwd: root, stdio: "pipe" });
  execFileSync("git", ["apply", "--whitespace=nowarn", patchPath], { cwd: root, stdio: "inherit" });
} catch (e) {
  // If already applied, accept; else try patch -p1 --forward
  try {
    execFileSync("patch", ["-p1", "--forward", "--batch", "-i", patchPath], { cwd: root, stdio: "inherit" });
  } catch (e2) {
    // Check if key markers already present
    const engine = readFileSync(path.join(root, "apps/engine/src/engine.ts"), "utf8");
    if (!engine.includes("tryObserveLearningSample") || !engine.includes("pollLearningSettlements")) {
      throw e2;
    }
    console.log("engine learning patch already applied — continuing");
  }
}
console.log("assemble ok");

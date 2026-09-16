import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, "../..");
const manifest = JSON.parse(readFileSync(path.join(here, "manifest.json"), "utf8"));

for (const [name, meta] of Object.entries(manifest.files)) {
  const b64 = meta.parts.map((p) => readFileSync(path.join(here, p), "utf8").trim()).join("");
  const buf = Buffer.from(b64, "base64");
  const sha = createHash("sha256").update(buf).digest("hex");
  if (sha !== meta.sha256) throw new Error(`SHA mismatch for ${name}: ${sha}`);
  const dest = path.join(root, meta.dest);
  mkdirSync(path.dirname(dest), { recursive: true });
  writeFileSync(dest, buf);
  console.log(`wrote ${meta.dest} (${buf.length} bytes)`);
}

const patchPath = path.join(here, manifest.enginePatch);
try {
  execFileSync("git", ["apply", "--whitespace=nowarn", patchPath], { cwd: root, stdio: "inherit" });
  console.log("applied engine-learning.patch");
} catch (error) {
  // Fallback: patch(1)
  execFileSync("patch", ["-p1", "--forward", "--batch", "-i", patchPath], { cwd: root, stdio: "inherit" });
  console.log("applied engine-learning.patch via patch(1)");
}
console.log("assemble ok");

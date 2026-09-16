import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, "../..");
const manifest = JSON.parse(readFileSync(path.join(here, "manifest.json"), "utf8"));

function writeConcat(destRel, parts, sha256) {
  const text = parts.map((p) => readFileSync(path.join(here, p), "utf8")).join("");
  const sha = createHash("sha256").update(text).digest("hex");
  if (sha !== sha256) throw new Error(`SHA mismatch for ${destRel}: ${sha}`);
  const dest = path.join(root, destRel);
  mkdirSync(path.dirname(dest), { recursive: true });
  writeFileSync(dest, text);
  console.log(`wrote ${destRel} (${Buffer.byteLength(text, "utf8")} bytes)`);
}

for (const file of manifest.files) {
  writeConcat(file.dest, file.parts, file.sha256);
}

const patchPath = path.join(here, manifest.enginePatch);
try {
  execFileSync("git", ["apply", "--whitespace=nowarn", patchPath], { cwd: root, stdio: "inherit" });
} catch {
  execFileSync("patch", ["-p1", "--forward", "--batch", "-i", patchPath], { cwd: root, stdio: "inherit" });
}
console.log("assemble ok");

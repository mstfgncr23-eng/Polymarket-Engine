import { execFileSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, "../..");
const patchPath = path.join(here, "engine-learning.patch");
try {
  execFileSync("git", ["apply", "--whitespace=nowarn", patchPath], { cwd: root, stdio: "inherit" });
  console.log("applied engine-learning.patch");
} catch {
  execFileSync("patch", ["-p1", "--forward", "--batch", "-i", patchPath], { cwd: root, stdio: "inherit" });
  console.log("applied engine-learning.patch via patch(1)");
}
console.log("assemble ok");

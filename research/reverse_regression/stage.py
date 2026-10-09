#!/usr/bin/env python3
"""Recreate a tested compiled-source stage, never touches a running service."""
import argparse
import hashlib
import json
import os
import pathlib
import posixpath
import sys

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parent.parent
MAP_DIR = HERE / "maps"
STAGES = [
    "00_safe", "01_step2", "02_step3", "03_identity", "04_mapopt",
    "05_cpu_v1", "06_cpu_v2", "07_cpu_v3", "08_current",
]

def source_files(stage):
    path = MAP_DIR / (stage + ".json")
    if not path.is_file():
        raise FileNotFoundError(f"Missing source map: {path}")
    bundle = json.loads(path.read_text(encoding="utf-8"))
    content = {}
    for src, body in zip(bundle["sources"], bundle["sourcesContent"]):
        if body is None:
            continue
        mapped = posixpath.normpath(posixpath.join("apps/engine/dist", src))
        if mapped.startswith("apps/engine/src/") or (
            mapped.startswith("packages/") and "/src/" in mapped
        ):
            if mapped in content and content[mapped] != body:
                raise ValueError("Conflicting source map contents: " + mapped)
            content[mapped] = body
    # Auxiliary index.ts is tree-shaken out of main.js.map.
    # Exact original content unavailable in the map; reconstruct exports separately.
    db_index = "".join('export * from "./' + name + '.js";\n' for name in (
        "access", "durable", "journal", "reports", "scan", "schema", "schema-verify"
    ))
    if STAGES.index(stage) >= STAGES.index("03_identity"):
        db_index += 'export * from "./quarantine.js";\n'
    content["packages/db/src/index.ts"] = db_index
    return content

def all_managed():
    results = set()
    for name in STAGES:
        results.update(source_files(name))
    return sorted(results)

def fingerprint(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def run():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--list", action="store_true", help="Image-to-image diff counts")
    ap.add_argument("--stage", choices=STAGES)
    ap.add_argument("--apply", action="store_true", help="Overwrite only managed image source files")
    ap.add_argument("--verify", action="store_true", help="Verify image source bytes match worktree")
    ap.add_argument("--manifest", action="store_true", help="Write hashes to the local evidence directory")
    args = ap.parse_args()
    if args.list:
        previous = None
        for name in STAGES:
            current = source_files(name)
            changed = [] if previous is None else [
                p for p in set(previous) | set(current) if previous.get(p) != current.get(p)
            ]
            print(f"{name:>13}: mapped={len(current):>3}, previous_delta={len(changed):>3}")
            for p in sorted(changed):
                print("  " + p)
            previous = current
        return 0
    if not args.stage:
        ap.error("Choose --list or --stage NAME")
    if args.apply and args.verify:
        ap.error("--apply and --verify are mutually exclusive")
    target = source_files(args.stage)
    changed = []
    missing = []
    mismatched = []
    removed = []
    for relative in all_managed():
        output = ROOT / relative
        wanted = target.get(relative)
        existing = output.read_text(encoding="utf-8") if output.exists() else None
        if existing == wanted:
            continue
        if wanted is None:
            removed.append(relative)
        elif existing is None:
            missing.append(relative)
        else:
            mismatched.append(relative)
        changed.append(relative)
        if args.apply:
            if wanted is None:
                if output.is_file():
                    output.unlink()
            else:
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_bytes(wanted.encode("utf-8"))
    print(f"STAGE={args.stage} managed={len(all_managed())} different={len(changed)}"
          f" created={len(missing)} replaced={len(mismatched)} removed={len(removed)}"
          f" mode={'APPLIED' if args.apply else 'READ_ONLY'}")
    for name in changed[:110]:
        print((" APPLIED " if args.apply else " DIFF ") + name)
    if args.manifest:
        manifest = {
            "stage": args.stage,
            "sha256_by_source": {p: fingerprint(v) for p, v in sorted(target.items())},
            "map_sha256": hashlib.sha256((MAP_DIR / (args.stage + ".json")).read_bytes()).hexdigest(),
            "source_count": len(target),
        }
        dest = HERE / "evidence" / (args.stage + ".manifest.json")
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        print("MANIFEST", dest)
    if args.verify and changed:
        print("VERIFICATION FAILED", file=sys.stderr)
        return 2
    return 0

if __name__ == "__main__":
    raise SystemExit(run())

#!/usr/bin/env python3
"""Independent OVH readback: stdin JSON, stdout one short signed-SSH-channel result.
No modification, provisioning, delete, retention enable or secret material.
Run over pinned SSH connection on the receiving OVH, not on the Engine host.
"""
from hashlib import sha256
from pathlib import Path
import json
import os
import stat
import sys

PINNED_PREFIX = Path("/data/polymarket-archive/g4-independent-paper/archive")
FILES = {
    "experiment_canonical": ("raw-manifest.json", "canonical-events.jsonl",
                             "final-report.json", "canonical-rollup.json"),
    "journal_bucket": ("manifest.json", "events.jsonl"),
    "engine_recovery": ("manifest.json", "checkpoints.json", "durable-transitions.jsonl"),
}
def deny(code):
    print("G4_OVH_READBACK_" + code, file=sys.stderr)
    raise SystemExit(78)

def verify(request, pinned_prefix=PINNED_PREFIX):
    if not isinstance(request, dict) or set(request) not in ({"directory", "kind", "files", "source"}, {"directory", "kind", "files", "source", "readFile"}):
        deny("REQUEST_SCOPE")
    kind = request["kind"]
    if kind not in FILES or request["source"] not in ("polymarket_g4_paper_20261012", "polymarket"):
        deny("SOURCE_SCOPE")
    if pinned_prefix.is_symlink():
        deny("SYMLINKED_ARCHIVE_ROOT")
    root = pinned_prefix.resolve(strict=False)
    folder = Path(request["directory"])
    if not folder.is_absolute() or folder == root or root not in folder.parents:
        deny("DESTINATION_SCOPE")
    if request["source"] == "polymarket" and root / "simple-engine" not in folder.parents:
        deny("ENGINE_DESTINATION_SCOPE")
    # Check every path component. resolve() alone follows untrusted symlinks.
    path = folder
    while path != root:
        if not path.exists() or path.is_symlink() or not path.is_dir():
            deny("UNSAFE_DIRECTORY")
        path = path.parent
    if root.is_symlink() or not root.is_dir():
        deny("UNSAFE_PREFIX")
    required = FILES[kind]
    if not isinstance(request["files"], dict) or set(request["files"]) != set(required):
        deny("FILE_LIST_SCOPE")
    total = 0
    for name in required:
        expected = request["files"][name]
        if (not isinstance(expected, dict) or set(expected) != {"bytes", "sha256"}
                or type(expected["bytes"]) is not int or expected["bytes"] < 0
                or expected["bytes"] > 16 * 1024**3
                or not isinstance(expected["sha256"], str)
                or len(expected["sha256"]) != 64
                or any(x not in "0123456789abcdef" for x in expected["sha256"])):
            deny("EXPECTED_DIGEST")
        file = folder / name
        if file.is_symlink() or not file.exists():
            deny("MISSING_OR_LINKED_FILE")
        st = file.lstat()
        if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1 or st.st_size != expected["bytes"]:
            deny("FILE_SIZE_OR_TYPE")
        digest = sha256()
        with file.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        if digest.hexdigest() != expected["sha256"]:
            deny("CHECKSUM_MISMATCH")
        total += st.st_size
    if request["source"] == "polymarket":
        verify_engine_scope(folder, kind, request["files"])
    if "readFile" in request and request["readFile"] not in required:
        deny("READ_FILE_SCOPE")
    return {"status": "G4_OVH_REMOTE_READBACK_VERIFIED", "source": request["source"],
            "fileCount": len(required), "totalBytes": total}

def verify_engine_scope(folder, kind, files):
    """Verify the current Engine manifest and row/cursor scope after byte hashes.
    Economic payload hashes and replay semantics are checked by the Engine restore test.
    """
    manifest_name = "raw-manifest.json" if kind == "experiment_canonical" else "manifest.json"
    manifest = json.loads((folder / manifest_name).read_text())
    if manifest.get("kind") != kind:
        deny("MANIFEST_KIND")
    def descriptor(key):
        desc = manifest[key]
        if desc["file"] not in files or any(desc[k] != files[desc["file"]][k] for k in ("bytes", "sha256")):
            deny("MANIFEST_DIGEST")
        return desc
    if kind == "engine_recovery":
        descriptor("checkpoints")
        rows_file = descriptor("durableTransitions")
        start, through, prefix = (manifest[k] for k in ("replayFromCursor", "throughCursor", "prefixThroughCursor"))
        if any(type(n) is not int or n < 0 for n in (start, through, prefix)) or not prefix <= start <= through:
            deny("RECOVERY_CURSOR")
        checkpoints = json.loads((folder / "checkpoints.json").read_text())
        retained = [c for c in checkpoints if c["payload"].get("durableCursor", -1) >= start
                    and c["payload"].get("durableCursor", through + 1) <= through
                    and c["payload"].get("checkpointContentHash")]
        if len(retained) < 2 or min(c["payload"]["durableCursor"] for c in retained) != start:
            deny("RECOVERY_CHECKPOINTS")
        cursor, last_prefix = start, 0
        def check(row):
            nonlocal cursor, last_prefix
            seq = row["sequence"]
            if seq <= prefix:
                if seq <= last_prefix or not row.get("payloadHash"):
                    deny("RECOVERY_PREFIX")
                last_prefix = seq
            else:
                if seq != cursor + 1 or not row.get("operationKey") or not row.get("payloadHash"):
                    deny("RECOVERY_REPLAY_GAP")
                cursor = seq
        count = check_lines(folder / rows_file["file"], check)
        if cursor != through or last_prefix != prefix or count != rows_file["eventCount"]:
            deny("RECOVERY_COUNT")
    else:
        raw = descriptor("rawEvents")
        if kind == "experiment_canonical":
            descriptor("finalReport"); descriptor("canonicalRollup")
            exp = manifest["experimentId"]
            report = json.loads((folder / "final-report.json").read_text())
            rollup = json.loads((folder / "canonical-rollup.json").read_text())
            if (folder.name != exp or report["experimentId"] != exp or rollup["experimentId"] != exp
                or report["contentHash"] != manifest["reportContentHash"]
                or rollup["contentHash"] != manifest["rollupContentHash"]
                or raw["eventCount"] != manifest["sourceEventCount"]):
                deny("EXPERIMENT_MANIFEST_SCOPE")
            def check(row):
                if (row["payload"].get("experimentId") != exp or row["occurredAt"] >= manifest["sourceFenceOccurredAt"]
                    or row.get("commitSeq", 0) > manifest["sourceCommitSeq"]):
                    deny("EXPERIMENT_EVENT_SCOPE")
        else:
            if folder.parent.name != manifest["retentionClass"]:
                deny("BUCKET_CLASS_SCOPE")
            def check(row):
                if (row["type"] not in manifest["eventTypes"]
                    or not manifest["occurredFrom"] <= row["occurredAt"] < manifest["occurredBefore"]
                    or row.get("commitSeq", 0) > manifest["commitSeqTo"]
                    or (manifest["unattributedOnly"] and row["payload"].get("experimentId"))):
                    deny("BUCKET_EVENT_SCOPE")
        if check_lines(folder / raw["file"], check) != raw["eventCount"]:
            deny("EVENT_COUNT")

def check_lines(path, check):
    count = 0
    with path.open() as stream:
        for line in stream:
            check(json.loads(line)); count += 1
    return count

if __name__ == "__main__":
    try:
        request = json.load(sys.stdin)
        result = verify(request)
        if "readFile" in request:
            with (Path(request["directory"]) / request["readFile"]).open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    sys.stdout.buffer.write(chunk)
        else:
            print(json.dumps(result, sort_keys=True))
    except SystemExit:
        raise
    except Exception:
        deny("UNVERIFIED")

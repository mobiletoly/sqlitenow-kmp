#!/usr/bin/env python3
"""Compare a dirty-worktree baseline with an immutable path allowlist.

Read-only checks cover tracked/non-ignored untracked files and explicitly allowed
ignored files. They do not prove semantic isolation, scan ignored directories,
or grant authorization. Snapshot writes only the requested new baseline file.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import stat
import subprocess
import sys


class ScopeError(RuntimeError):
    pass


def git(root: Path, *args: str) -> bytes:
    result = subprocess.run(
        ["git", "-C", str(root), *args], capture_output=True, check=False
    )
    if result.returncode:
        raise ScopeError(result.stderr.decode(errors="replace").strip())
    return result.stdout


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def valid_allow(path: str) -> str:
    parts = PurePosixPath(path).parts
    if (not parts or path.startswith("/") or ".." in parts
            or any(c in path for c in "\x00\\*?[]")
            or PurePosixPath(path).as_posix() != path.rstrip("/")):
        raise ScopeError(f"allow must be an exact relative file or directory prefix: {path!r}")
    return path


def fingerprint(path: Path) -> dict:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return {"kind": "missing"}
    if stat.S_ISLNK(info.st_mode):
        kind, raw = "symlink", os.fsencode(os.readlink(path))
    elif stat.S_ISREG(info.st_mode):
        kind, raw = "file", path.read_bytes()
    else:
        raise ScopeError(f"cannot certify non-file path (including submodules): {path}")
    return {"kind": kind, "mode": stat.S_IMODE(info.st_mode), "sha256": digest(raw)}


def inventory(root: Path, allowed: list[str]) -> dict:
    raw = git(root, "ls-files", "--cached", "--others", "--exclude-standard", "-z")
    paths = set(os.fsdecode(raw).split("\0")) - {""}
    paths.update(p for p in allowed if not p.endswith("/"))
    return {p: fingerprint(root / p) for p in sorted(paths)}


def identity(root: Path) -> dict:
    return {
        "root": str(root),
        "head": git(root, "rev-parse", "HEAD").decode().strip(),
        "branch": git(root, "branch", "--show-current").decode().strip(),
        "indexSHA256": digest(git(root, "ls-files", "--stage", "-z")),
    }


def snapshot(root: Path, allowed: list[str], test_only: bool) -> dict:
    if not allowed:
        raise ScopeError("snapshot requires at least one --allow path")
    allowed = sorted({valid_allow(p) for p in allowed})
    return {
        "version": 1, **identity(root),
        "capturedUTC": datetime.now(timezone.utc).isoformat(),
        "allowed": allowed, "testOnly": test_only,
        "files": inventory(root, allowed),
    }


def product_source(path: str) -> bool:
    parts = PurePosixPath(path).parts
    if "Sources" in parts:
        return True
    if "src" not in parts:
        return False
    position = parts.index("src") + 1
    source_set = parts[position] if position < len(parts) else ""
    return source_set not in {"test", "tests", "testFixtures"} and not source_set.endswith("Test")


def check(root: Path, baseline: dict) -> dict:
    if (not isinstance(baseline, dict) or baseline.get("version") != 1
            or not isinstance(baseline.get("allowed"), list)
            or not baseline["allowed"] or not isinstance(baseline.get("testOnly"), bool)
            or not isinstance(baseline.get("files"), dict)):
        raise ScopeError("invalid baseline schema")
    allowed = [valid_allow(p) for p in baseline["allowed"]]
    current_identity = identity(root)
    drift = [key for key, value in current_identity.items() if baseline.get(key) != value]
    current = inventory(root, allowed)
    changed = sorted(p for p in current.keys() | baseline["files"].keys()
                     if current.get(p) != baseline["files"].get(p))
    outside = [p for p in changed if not any(
        p.startswith(a) if a.endswith("/") else p == a for a in allowed
    )]
    production = [p for p in changed if baseline["testOnly"] and product_source(p)]
    wiring = [p for p in changed if PurePosixPath(p).name in {
        "build.gradle", "build.gradle.kts", "settings.gradle", "settings.gradle.kts", "Package.swift"
    }]
    return {
        "status": "FAIL" if drift or outside or production else "PASS",
        "identityDrift": drift, "changedPaths": changed,
        "outsideAllowedPaths": outside, "productionSourceChanges": production,
        "buildWiringRequiresReview": wiring,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["snapshot", "check"])
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--allow", action="append", default=[])
    parser.add_argument("--test-only", action="store_true")
    args = parser.parse_args()
    try:
        root = args.root.resolve()
        if Path(os.fsdecode(git(root, "rev-parse", "--show-toplevel")).strip()).resolve() != root:
            raise ScopeError("--root must be the repository root")
        if args.action == "snapshot":
            baseline = args.baseline.resolve()
            if baseline.is_relative_to(root):
                result = subprocess.run(
                    ["git", "-C", str(root), "check-ignore", "-q", "--", str(baseline)], check=False
                )
                if result.returncode:
                    raise ScopeError("baseline must be ignored or outside the checkout")
            record = snapshot(root, args.allow, args.test_only)
            with baseline.open("x", encoding="utf-8") as output:
                json.dump(record, output, indent=2)
                output.write("\n")
            print(f"Captured {len(record['files'])} paths; baseline will not be overwritten.")
            return 0
        if args.allow or args.test_only:
            raise ScopeError("check uses the original baseline scope; --allow/--test-only cannot override it")
        report = check(root, json.loads(args.baseline.read_text(encoding="utf-8")))
        print(json.dumps(report, indent=2))
        return 0 if report["status"] == "PASS" else 1
    except (ScopeError, OSError, ValueError, TypeError) as error:
        print(f"scope check error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())

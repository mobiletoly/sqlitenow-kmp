#!/usr/bin/env python3
"""Reject disposable specification dependencies and temporary milestone names."""

from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
EXCEPTIONS_REPOSITORY_PATH = "tools/repository-hygiene-exceptions.json"
EXCLUDED_ROOTS = {"spec", "specs"}
TOKEN_PATTERNS = (
    re.compile(r"(?<![A-Za-z0-9])[Mm]\d{4}(?=$|[^A-Za-z0-9]|[A-Z])"),
    re.compile(r"(?<![A-Za-z0-9])(?i:se)-?0?3(?=$|[^A-Za-z0-9]|[A-Z])"),
    re.compile(r"(?<![A-Za-z0-9])(?i:qp)(?:0|1|-01)(?=$|[^A-Za-z0-9]|[A-Z])"),
    re.compile(r"(?<![A-Za-z0-9])[Mm]9(?=[A-Z_-])"),
    re.compile(r"(?:(?<![A-Za-z0-9])[Rr]0[1-8](?=[A-Za-z_-])|(?<=[a-z])R0[1-8](?=[A-Z]))"),
    re.compile(r"(?<![A-Za-z0-9])(?i:phase)[\s_-]*(?:\d+[A-Za-z]?|[Xx])(?=$|[^A-Za-z0-9]|[A-Z])"),
)
CONCRETE_SPEC_PATH = re.compile(
    r"(?<![A-Za-z0-9_.-])(?:spec|specs)/[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*"
)


class HygieneError(RuntimeError):
    pass


def git(root: Path, *arguments: str) -> bytes:
    completed = subprocess.run(
        ["git", "-C", str(root), *arguments],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode:
        raise HygieneError(completed.stderr.decode(errors="replace").strip())
    return completed.stdout


def nul_paths(raw: bytes) -> list[str]:
    return sorted(set(item for item in raw.decode(errors="surrogateescape").split("\0") if item))


def is_excluded_spec_path(path: str) -> bool:
    parts = PurePosixPath(path).parts
    return bool(parts) and parts[0] in EXCLUDED_ROOTS


def candidate_paths(root: Path, revision: str | None) -> tuple[list[str], list[str]]:
    if revision:
        git(root, "rev-parse", "--verify", f"{revision}^{{commit}}")
        tracked = nul_paths(git(root, "ls-tree", "-rz", "--name-only", revision))
        return tracked, tracked
    tracked = nul_paths(git(root, "ls-files", "-z"))
    untracked = nul_paths(git(root, "ls-files", "--others", "--exclude-standard", "-z"))
    return tracked, sorted(set(tracked + untracked))


def read_candidate(root: Path, path: str, revision: str | None) -> bytes:
    if revision:
        return git(root, "show", f"{revision}:{path}")
    candidate = root / path
    if candidate.is_symlink() or not candidate.is_file():
        return b""
    return candidate.read_bytes()


def token_matches(value: str) -> Iterable[str]:
    for pattern in TOKEN_PATTERNS:
        for match in pattern.finditer(value):
            yield match.group(0)


def parse_exceptions(raw: bytes, source: str) -> list[dict[str, object]]:
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as error:
        raise HygieneError(f"cannot read exceptions from {source}: {error}") from error
    if not isinstance(value, list):
        raise HygieneError("exceptions must be a JSON array")
    seen: set[tuple[str, str]] = set()
    for entry in value:
        if not isinstance(entry, dict) or set(entry) != {"path", "token", "count", "reason"}:
            raise HygieneError("each exception needs exactly path, token, count, and reason")
        key = (entry.get("path"), entry.get("token"))
        if (
            not all(isinstance(entry.get(field), str) and entry[field] for field in ("path", "token", "reason"))
            or isinstance(entry.get("count"), bool)
            or not isinstance(entry.get("count"), int)
            or entry["count"] <= 0
            or key in seen
        ):
            raise HygieneError("exception values must be unique, non-empty, and have a positive count")
        seen.add(key)
    return value


def load_exceptions(path: Path) -> list[dict[str, object]]:
    try:
        raw = path.read_bytes()
    except OSError as error:
        raise HygieneError(f"cannot read exceptions from {path}: {error}") from error
    return parse_exceptions(raw, str(path))


def candidate_exceptions(
    root: Path, revision: str | None, exceptions_path: Path
) -> list[dict[str, object]]:
    if not revision:
        return load_exceptions(exceptions_path)
    repository_path = root / EXCEPTIONS_REPOSITORY_PATH
    if exceptions_path.resolve() != repository_path.resolve():
        raise HygieneError(
            "--exceptions cannot override the committed exception registry in revision mode"
        )
    try:
        raw = read_candidate(root, EXCEPTIONS_REPOSITORY_PATH, revision)
    except HygieneError as error:
        raise HygieneError(
            f"cannot read exceptions from {revision}:{EXCEPTIONS_REPOSITORY_PATH}: {error}"
        ) from error
    return parse_exceptions(raw, f"{revision}:{EXCEPTIONS_REPOSITORY_PATH}")


def violations(root: Path, revision: str | None = None) -> list[tuple[str, int, str, str]]:
    tracked, candidates = candidate_paths(root, revision)
    found: list[tuple[str, int, str, str]] = []
    for path in tracked:
        if is_excluded_spec_path(path):
            found.append((path, 0, path, "root specification is tracked"))
    for path in candidates:
        if is_excluded_spec_path(path):
            continue
        if not revision:
            candidate = root / path
            if candidate.is_symlink() or not candidate.is_file():
                # Ordinary pending deletions are not violations; indexed specifications
                # were already reported above even when their local file is missing.
                continue
        for token in token_matches(path):
            found.append((path, 0, token, "temporary milestone token in path"))
        # The exact-token exception registry necessarily quotes the tokens it governs.
        if path == "tools/repository-hygiene-exceptions.json":
            continue
        raw = read_candidate(root, path, revision)
        if not raw or b"\0" in raw:
            continue
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            continue
        for line_number, line in enumerate(text.splitlines(), start=1):
            for token in token_matches(line):
                found.append((path, line_number, token, "temporary milestone token in text"))
            for match in CONCRETE_SPEC_PATH.finditer(line):
                found.append((path, line_number, match.group(0), "concrete dependency or link into root specs"))
    return sorted(found, key=lambda item: (item[0], item[1], item[2], item[3]))


def apply_exceptions(
    found: list[tuple[str, int, str, str]], exceptions: list[dict[str, object]]
) -> tuple[list[tuple[str, int, str, str]], list[str]]:
    expected = {(str(item["path"]), str(item["token"])): int(item["count"]) for item in exceptions}
    actual = Counter((path, token) for path, _line, token, _reason in found)
    problems: list[str] = []
    for key, count in sorted(expected.items()):
        observed = actual.get(key, 0)
        if observed != count:
            problems.append(
                f"exception {key[0]} token {key[1]!r} expected {count} occurrence(s), observed {observed}"
            )
    remaining = [item for item in found if (item[0], item[2]) not in expected]
    return remaining, problems


def check(root: Path, revision: str | None, exceptions_path: Path) -> list[str]:
    found = violations(root, revision)
    remaining, exception_problems = apply_exceptions(
        found, candidate_exceptions(root, revision, exceptions_path)
    )
    messages = list(exception_problems)
    for path, line, token, reason in remaining:
        location = f"{path}:{line}" if line else path
        messages.append(f"{location}: {reason}: {token}")
    return sorted(messages)


def main(arguments: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision")
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--exceptions", type=Path)
    options = parser.parse_args(arguments)
    root = options.root.resolve()
    exceptions = options.exceptions or root / EXCEPTIONS_REPOSITORY_PATH
    try:
        messages = check(root, options.revision, exceptions)
    except (HygieneError, OSError) as error:
        print(f"repository hygiene unavailable: {error}", file=sys.stderr)
        return 2
    if messages:
        for message in messages:
            print(message)
        print(f"repository hygiene failed: {len(messages)} violation(s)")
        return 1
    mode = f"revision {options.revision}" if options.revision else "worktree and index"
    print(f"repository hygiene passed: {mode}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

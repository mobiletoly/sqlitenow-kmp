#!/usr/bin/env python3
"""Deterministic advisory qualification planner and runner."""

from __future__ import annotations

import argparse
import datetime as dt
import errno
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import signal
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from typing import Any, Iterable


MANIFEST_SCHEMA_VERSION = 3
PLAN_SCHEMA_VERSION = 3
ATTEMPT_RECORD_SCHEMA_VERSION = 2
DEFAULT_TIMEOUT_SECONDS = 1800
DEFAULT_MANIFEST = Path(__file__).with_name("lanes.json")
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
DISALLOWED_SHELL_TOKENS = ("&&", "||", ";", "|", chr(96), "$(", ">", "<")
STARTED_FIELDS = {
    "schema_version", "event", "attempt_id", "lane_id", "plan_sha256", "manifest_sha256",
    "argv", "cwd", "repository", "environment", "toolchains", "relevant_input_sha256",
    "dependency_digests", "repositories_before", "timeout_seconds", "start_epoch_seconds", "start_utc", "log",
}
FINISHED_FIELDS = {
    "schema_version", "event", "attempt_id", "lane_id", "end_epoch_seconds", "end_utc",
    "duration_seconds", "exit_code", "signal", "timed_out", "harness_error", "log",
    "log_bytes", "log_sha256", "relevant_input_sha256_after", "repositories_after",
}


class QualificationError(Exception):
    def __init__(self, exit_code: int, code: str, message: str) -> None:
        super().__init__(message)
        self.exit_code = exit_code
        self.code = code


class RunnerInterrupted(Exception):
    def __init__(self, signum: int) -> None:
        super().__init__(f"runner interrupted by signal {signum}")
        self.signum = signum


def canonical_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _unique(items: Iterable[str], label: str) -> dict[str, str]:
    seen: dict[str, str] = {}
    for item in items:
        if not isinstance(item, str) or not item:
            raise QualificationError(3, "manifest_invalid_id", f"{label} IDs must be non-empty strings")
        if item in seen:
            raise QualificationError(3, "manifest_duplicate_id", f"duplicate {label} ID: {item}")
        seen[item] = item
    return seen


def _positive_seconds(value: Any, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise QualificationError(3, "manifest_timeout", f"{label} must be a positive integer")


def _mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise QualificationError(3, "manifest_schema", f"{label} must be an object")
    return value


def normalize_relative_path(value: str, label: str = "path") -> str:
    if not isinstance(value, str) or not value or "\x00" in value or "\\" in value:
        raise QualificationError(3, "manifest_invalid_path", f"invalid {label}: {value!r}")
    if value != "." and any(part == "" for part in value.split("/")):
        raise QualificationError(3, "manifest_invalid_path", f"invalid {label}: {value!r}")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in ("", "..") for part in path.parts):
        raise QualificationError(3, "manifest_invalid_path", f"invalid {label}: {value!r}")
    return path.as_posix()


def _glob_regex(pattern: str) -> re.Pattern[str]:
    normalize_relative_path(pattern, "glob")
    result = ""
    index = 0
    while index < len(pattern):
        character = pattern[index]
        if character == "*":
            if index + 1 < len(pattern) and pattern[index + 1] == "*":
                index += 2
                if index < len(pattern) and pattern[index] == "/":
                    result += "(?:.*/)?"
                    index += 1
                else:
                    result += ".*"
                continue
            result += "[^/]*"
        elif character == "?":
            result += "[^/]"
        else:
            result += re.escape(character)
        index += 1
    return re.compile(f"^{result}$")


def glob_matches(pattern: str, path: str) -> bool:
    return bool(_glob_regex(pattern).match(path))


def load_manifest(path: Path = DEFAULT_MANIFEST) -> tuple[dict[str, Any], str]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise QualificationError(3, "manifest_unreadable", f"cannot read manifest {path}: {error}") from error
    validate_manifest(data)
    return data, sha256_bytes(canonical_bytes(data))


def _check_graph(graph: dict[str, list[str]], label: str) -> None:
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node: str, chain: list[str]) -> None:
        if node in visiting:
            raise QualificationError(3, "manifest_cycle", f"{label} cycle: {' -> '.join(chain + [node])}")
        if node in visited:
            return
        visiting.add(node)
        for child in graph[node]:
            visit(child, chain + [node])
        visiting.remove(node)
        visited.add(node)

    for node in graph:
        visit(node, [])


def validate_manifest(manifest: dict[str, Any]) -> None:
    if not isinstance(manifest, dict) or manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise QualificationError(
            3,
            "manifest_schema",
            f"manifest schema_version must be {MANIFEST_SCHEMA_VERSION}",
        )
    _positive_seconds(manifest.get("default_timeout_seconds"), "default_timeout_seconds")
    required = (
        "repositories", "toolchains", "lanes", "path_rules",
        "fallback_lane_ids", "forbidden_planning_validation_prefixes", "resources",
    )
    for field in required:
        if field not in manifest or (field != "resources" and not isinstance(manifest[field], list)):
            raise QualificationError(3, "manifest_missing_field", f"manifest field {field} has the wrong type")

    resources = _mapping(manifest["resources"], "resources")
    if not resources or any(
        not isinstance(name, str) or not name or isinstance(capacity, bool)
        or not isinstance(capacity, int) or capacity <= 0
        for name, capacity in resources.items()
    ):
        raise QualificationError(3, "manifest_resources", "resources must map non-empty names to positive capacities")
    maximum_workers = manifest.get("campaign_max_workers", 1)
    if isinstance(maximum_workers, bool) or not isinstance(maximum_workers, int) or maximum_workers <= 0:
        raise QualificationError(3, "manifest_workers", "campaign_max_workers must be a positive integer")

    repositories = manifest["repositories"]
    repo_ids = _unique((_mapping(entry, "repository").get("id") for entry in repositories), "repository")
    if set(repo_ids) != {"sqlite-now", "go-oversync"}:
        raise QualificationError(3, "manifest_repositories", "repositories must be exactly sqlite-now and go-oversync")
    for entry in repositories:
        _mapping(entry, "repository")
        normalize_relative_path(entry.get("path"), "repository path")
        if (
            not isinstance(entry.get("read_only"), bool)
            or not isinstance(entry.get("roles"), list)
            or not entry["roles"]
            or not all(isinstance(role, str) and role for role in entry["roles"])
        ):
            raise QualificationError(3, "manifest_repository_fields", f"incomplete repository entry: {entry.get('id')}")
    if not next(item for item in repositories if item["id"] == "go-oversync")["read_only"]:
        raise QualificationError(3, "manifest_go_mutable", "go-oversync must remain read-only")

    toolchains = manifest["toolchains"]
    toolchain_ids = _unique((_mapping(entry, "toolchain").get("id") for entry in toolchains), "toolchain")
    for entry in toolchains:
        _mapping(entry, "toolchain")
        argv = entry.get("argv")
        if not isinstance(argv, list) or not argv or not all(isinstance(arg, str) and arg for arg in argv):
            raise QualificationError(3, "manifest_toolchain_argv", f"toolchain {entry.get('id')} needs argv")
        normalize_relative_path(entry.get("cwd"), "toolchain cwd")
        if entry.get("repository", "sqlite-now") not in repo_ids:
            raise QualificationError(3, "manifest_toolchain_repository", f"toolchain {entry.get('id')} has unknown repository")

    lanes = manifest["lanes"]
    lane_ids = _unique((entry.get("id") for entry in lanes), "lane")
    required_lane_fields = {
        "id", "description", "repository", "cwd", "argv", "environment_allowlist", "environment",
        "toolchains", "environment_class", "classification", "live", "heavy", "static_inputs",
        "depends_on", "subsumes", "resource_locks", "scheduling_class", "memory_class", "campaign_eligible",
    }
    dependency_graph: dict[str, list[str]] = {}
    subsumption_graph: dict[str, list[str]] = {}
    for lane in lanes:
        _mapping(lane, "lane")
        missing = required_lane_fields - set(lane)
        if missing:
            raise QualificationError(3, "manifest_lane_fields", f"lane {lane.get('id')} missing {sorted(missing)}")
        if lane["repository"] not in repo_ids:
            raise QualificationError(3, "manifest_lane_repository", f"lane {lane['id']} has unknown repository")
        normalize_relative_path(lane["cwd"], "lane cwd")
        if not isinstance(lane["argv"], list) or not lane["argv"] or not all(isinstance(arg, str) and arg for arg in lane["argv"]):
            raise QualificationError(3, "manifest_lane_argv", f"lane {lane['id']} has missing argv")
        if any(any(token in arg for token in DISALLOWED_SHELL_TOKENS) for arg in lane["argv"]):
            raise QualificationError(3, "manifest_shell_composition", f"lane {lane['id']} contains shell composition")
        for field in ("environment_allowlist", "toolchains", "static_inputs", "depends_on", "subsumes"):
            if not isinstance(lane[field], list) or not all(isinstance(value, str) for value in lane[field]):
                raise QualificationError(3, "manifest_lane_fields", f"lane {lane['id']} field {field} must be strings")
        if not isinstance(lane.get("ignored_static_inputs", []), list) or not all(
            isinstance(value, str) and value for value in lane.get("ignored_static_inputs", [])
        ):
            raise QualificationError(3, "manifest_lane_fields", f"lane {lane['id']} ignored_static_inputs must be strings")
        for pattern in lane.get("ignored_static_inputs", []):
            _glob_regex(pattern)
        if not isinstance(lane["environment"], dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in lane["environment"].items()):
            raise QualificationError(3, "manifest_lane_environment", f"lane {lane['id']} has invalid environment")
        if not set(lane["environment"]).issubset(set(lane["environment_allowlist"])):
            raise QualificationError(3, "manifest_lane_environment", f"lane {lane['id']} overrides non-allow-listed environment")
        if not isinstance(lane["environment_class"], str) or not lane["environment_class"]:
            raise QualificationError(3, "manifest_lane_environment", f"lane {lane['id']} has invalid environment class")
        if lane["classification"] not in ("product", "infrastructure") or not isinstance(lane["live"], bool) or not isinstance(lane["heavy"], bool):
            raise QualificationError(3, "manifest_lane_classification", f"lane {lane['id']} has invalid classification flags")
        if (
            not isinstance(lane["resource_locks"], list) or not lane["resource_locks"]
            or len(set(lane["resource_locks"])) != len(lane["resource_locks"])
            or any(lock not in resources for lock in lane["resource_locks"])
            or not isinstance(lane["scheduling_class"], str) or not lane["scheduling_class"]
            or lane["memory_class"] not in ("light", "normal", "heavy", "measurement-exclusive")
            or not isinstance(lane["campaign_eligible"], bool)
        ):
            raise QualificationError(3, "manifest_scheduling", f"lane {lane['id']} has unsafe scheduling metadata")
        if lane["memory_class"] == "measurement-exclusive" and "measurement-exclusive" not in lane["resource_locks"]:
            raise QualificationError(3, "manifest_scheduling", f"measurement lane {lane['id']} lacks measurement-exclusive lock")
        if lane["repository"] == "go-oversync" and lane["campaign_eligible"]:
            raise QualificationError(3, "manifest_go_campaign", "Go-Oversync lanes cannot be campaign eligible")
        if lane["classification"] == "product" and not lane["resource_locks"]:
            raise QualificationError(3, "manifest_scheduling", f"product lane {lane['id']} lacks resource locks")
        if "timeout_seconds" in lane:
            _positive_seconds(lane["timeout_seconds"], f"lane {lane['id']} timeout_seconds")
        for pattern in lane["static_inputs"]:
            _glob_regex(pattern)
        for toolchain in lane["toolchains"]:
            if toolchain not in toolchain_ids:
                raise QualificationError(3, "manifest_unknown_toolchain", f"lane {lane['id']} references {toolchain}")
        for relation in lane["depends_on"] + lane["subsumes"]:
            if relation not in lane_ids:
                raise QualificationError(3, "manifest_unknown_lane", f"lane {lane['id']} references {relation}")
            if relation == lane["id"]:
                raise QualificationError(3, "manifest_self_relation", f"lane {lane['id']} references itself")
        dependency_graph[lane["id"]] = list(lane["depends_on"])
        subsumption_graph[lane["id"]] = list(lane["subsumes"])
    lanes_by_id = {lane["id"]: lane for lane in lanes}
    cross_repo = lanes_by_id.get("cross-repo-conformance")
    if cross_repo is not None:
        if "GO_OVERSYNC_REPO" in cross_repo["environment_allowlist"] or "GO_OVERSYNC_REPO" in cross_repo["environment"]:
            raise QualificationError(3, "manifest_cross_repo_override", "cross-repo-conformance must use the canonical Go-Oversync sibling")
        if cross_repo["environment"].get("GOTOOLCHAIN") != "go1.25.12" or "GOTOOLCHAIN" not in cross_repo["environment_allowlist"]:
            raise QualificationError(3, "manifest_cross_repo_toolchain", "cross-repo-conformance must pin GOTOOLCHAIN=go1.25.12")
        if any(argument == "-PgoOversyncRepo" or argument.startswith("-PgoOversyncRepo=") for argument in cross_repo["argv"]):
            raise QualificationError(3, "manifest_cross_repo_override", "cross-repo-conformance cannot override Go-Oversync repository path")
    _check_graph(dependency_graph, "dependency")
    _check_graph(subsumption_graph, "subsumption")
    for lane in lanes:
        sensitive_children = [
            child
            for child in _transitive(subsumption_graph, lane["id"])
            if lanes_by_id[child]["heavy"] or "resource" in lanes_by_id[child]["environment_class"]
        ]
        if sensitive_children:
            raise QualificationError(
                3,
                "manifest_sensitive_subsumption",
                f"lane {lane['id']} cannot subsume heavy/resource lanes: {sorted(sensitive_children)}",
            )

    _unique((_mapping(entry, "path rule").get("id") for entry in manifest["path_rules"]), "path rule")
    allowed_classes = {
        "ordinary-docs", "governing-docs", "source", "test", "fixture-contract",
        "generated-support", "build-toolchain", "planner",
    }
    for rule in manifest["path_rules"]:
        _mapping(rule, "path rule")
        required_rule = {
            "id", "repository", "includes", "excludes", "lanes", "reason_code", "reason", "class",
            "validation_paths",
        }
        if required_rule - set(rule):
            raise QualificationError(3, "manifest_rule_fields", f"path rule {rule.get('id')} is incomplete")
        if rule["repository"] not in repo_ids or rule["class"] not in allowed_classes:
            raise QualificationError(3, "manifest_rule_fields", f"path rule {rule['id']} has invalid owner or class")
        for field in ("includes", "excludes", "lanes", "validation_paths"):
            if not isinstance(rule[field], list) or not all(isinstance(value, str) and value for value in rule[field]):
                raise QualificationError(3, "manifest_rule_fields", f"path rule {rule['id']} field {field} must be strings")
        if not rule["includes"] or not rule["lanes"] or not rule["validation_paths"]:
            raise QualificationError(3, "manifest_uncovered_rule", f"path rule {rule['id']} has no includes or lanes")
        for pattern in rule["includes"] + rule["excludes"]:
            _glob_regex(pattern)
        for validation_path in rule["validation_paths"]:
            normalized = normalize_relative_path(validation_path, "rule validation path")
            if not any(glob_matches(pattern, normalized) for pattern in rule["includes"]):
                raise QualificationError(3, "manifest_unreachable_rule", f"validation path does not reach rule {rule['id']}")
            if any(glob_matches(pattern, normalized) for pattern in rule["excludes"]):
                raise QualificationError(3, "manifest_unreachable_rule", f"validation path is excluded from rule {rule['id']}")
        for lane in rule["lanes"]:
            if lane not in lane_ids:
                raise QualificationError(3, "manifest_unknown_lane", f"path rule {rule['id']} references {lane}")
    if not manifest["fallback_lane_ids"]:
        raise QualificationError(3, "manifest_no_fallback", "fallback_lane_ids must not be empty")
    for lane in manifest["fallback_lane_ids"]:
        if lane not in lane_ids:
            raise QualificationError(3, "manifest_unknown_lane", f"fallback references {lane}")
    for prefix in manifest["forbidden_planning_validation_prefixes"]:
        if not isinstance(prefix, list) or not prefix or not all(isinstance(arg, str) and arg for arg in prefix):
            raise QualificationError(3, "manifest_forbidden_prefix", "forbidden prefixes must be argv arrays")
    for lane in lanes:
        if lane["classification"] == "product" and not any(
            lane["argv"][: len(prefix)] == prefix
            for prefix in manifest["forbidden_planning_validation_prefixes"]
        ):
            raise QualificationError(3, "manifest_product_containment", f"product lane {lane['id']} lacks a planning refusal prefix")


def repository_roots(manifest: dict[str, Any], workspace_root: Path = WORKSPACE_ROOT) -> dict[str, Path]:
    workspace = workspace_root.resolve()
    roots = {entry["id"]: (workspace / entry["path"]).resolve() for entry in manifest["repositories"]}
    for repo_id, root in roots.items():
        if root != workspace and workspace not in root.parents:
            raise QualificationError(3, "repository_root_escapes_workspace", f"repository {repo_id} escapes workspace: {root}")
        if not root.is_dir() or not (root / ".git").exists():
            raise QualificationError(5, "repository_unavailable", f"repository {repo_id} is unavailable at {root}")
    resolved = list(roots.items())
    for index, (left_id, left) in enumerate(resolved):
        for right_id, right in resolved[index + 1 :]:
            if left == right or left in right.parents or right in left.parents:
                raise QualificationError(3, "repository_roots_overlap", f"repository roots overlap: {left_id}, {right_id}")
    return roots


def git(root: Path, *arguments: str, check: bool = True) -> bytes:
    process = subprocess.run(
        ["git", "-C", str(root), *arguments],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if check and process.returncode:
        message = process.stderr.decode(errors="replace").strip()
        raise QualificationError(5, "git_failed", f"git {' '.join(arguments)} failed in {root}: {message}")
    return process.stdout


def _index_entries(root: Path) -> dict[str, tuple[str, str]]:
    entries: dict[str, tuple[str, str]] = {}
    raw = git(root, "ls-files", "-s", "-z")
    for value in filter(None, raw.decode(errors="surrogateescape").split("\x00")):
        header, path = value.split("\t", 1)
        fields = header.split()
        if len(fields) == 3:
            if fields[2] != "0" or path in entries:
                entries[path] = ("conflict", "")
            elif entries.get(path) != ("conflict", ""):
                entries[path] = (fields[0], fields[1])
    conflicts = sorted(path for path, entry in entries.items() if entry[0] == "conflict")
    if conflicts:
        raise QualificationError(5, "git_index_unmerged", f"repository has unmerged index entries: {', '.join(conflicts)}")
    return entries


def repository_identity(root: Path, index: dict[str, tuple[str, str]] | None = None) -> dict[str, Any]:
    branch = git(root, "branch", "--show-current").decode().strip()
    head = git(root, "rev-parse", "HEAD").decode().strip()
    tree = git(root, "rev-parse", "HEAD^{tree}").decode().strip()
    porcelain = git(root, "status", "--porcelain=v2", "-z", "--untracked-files=all")
    untracked_raw = git(root, "ls-files", "--others", "--exclude-standard", "-z")
    untracked: list[dict[str, Any]] = []
    for raw_path in sorted(filter(None, untracked_raw.decode(errors="surrogateescape").split("\x00"))):
        path = root / raw_path
        resolved = path.resolve(strict=False)
        if root != resolved and root not in resolved.parents:
            raise QualificationError(5, "input_escapes_repository", f"input {raw_path} escapes repository")
        if path.is_symlink():
            content = os.readlink(path).encode(errors="surrogateescape")
        elif path.is_file():
            resolved = path.resolve(strict=False)
            if root != resolved and root not in resolved.parents:
                raise QualificationError(5, "input_escapes_repository", f"input {raw_path} escapes repository")
            content = path.read_bytes()
        else:
            continue
        untracked.append({"path": raw_path, "sha256": sha256_bytes(content), "bytes": len(content)})
    changed_raw = git(root, "diff", "--name-only", "-z", "HEAD", "--")
    worktree_paths = set(filter(None, changed_raw.decode(errors="surrogateescape").split("\x00")))
    worktree_paths.update(item["path"] for item in untracked)
    index = index if index is not None else _index_entries(root)
    worktree_manifest = [
        _file_identity(root, path, index)
        for path in sorted(worktree_paths)
    ]
    return {
        "branch": branch,
        "head": head,
        "head_tree": tree,
        "porcelain_v2_sha256": sha256_bytes(porcelain),
        "untracked_content_manifest_sha256": sha256_bytes(canonical_bytes(untracked)),
        "index_manifest_sha256": sha256_bytes(git(root, "ls-files", "-s", "-z")),
        "worktree_content_manifest_sha256": sha256_bytes(canonical_bytes(worktree_manifest)),
    }


def changed_paths(root: Path, base: str) -> list[dict[str, str]]:
    raw = git(root, "diff", "--name-status", "-z", "-M", "-C", base, "--")
    values = raw.decode(errors="surrogateescape").split("\x00")
    changes: list[dict[str, str]] = []
    index = 0
    while index < len(values) and values[index]:
        status = values[index]
        index += 1
        if status.startswith(("R", "C")):
            old_path, new_path = values[index], values[index + 1]
            index += 2
            changes.extend((
                {"path": old_path, "status": f"{status}:old"},
                {"path": new_path, "status": f"{status}:new"},
            ))
        else:
            changes.append({"path": values[index], "status": status})
            index += 1
    tracked = {entry["path"] for entry in changes}
    untracked = git(root, "ls-files", "--others", "--exclude-standard", "-z")
    for path in filter(None, untracked.decode(errors="surrogateescape").split("\x00")):
        if path not in tracked:
            changes.append({"path": path, "status": "untracked"})
    return sorted(changes, key=lambda entry: (entry["path"], entry["status"]))


def _git_files(root: Path, arguments: tuple[str, ...], patterns: Iterable[str] = ()) -> list[str]:
    command = list(arguments) + ["-z"]
    normalized_patterns = [f":(glob){pattern}" for pattern in sorted(set(patterns))]
    if normalized_patterns:
        command.extend(["--", *normalized_patterns])
    raw = git(root, *command)
    return sorted(set(filter(None, raw.decode(errors="surrogateescape").split("\x00"))))


def _listed_files(root: Path, patterns: Iterable[str] = ()) -> list[str]:
    return _git_files(root, ("ls-files", "-co", "--exclude-standard"), patterns)


def _ignored_files(root: Path, patterns: Iterable[str] = ()) -> list[str]:
    return _git_files(root, ("ls-files", "--others", "--ignored", "--exclude-standard"), patterns)


def _filesystem_matches(root: Path, pattern: str, inventory: Iterable[str]) -> list[str]:
    matches: list[str] = []
    try:
        for relative in sorted(set(inventory)):
            if not glob_matches(pattern, relative) or relative == ".git" or relative.startswith(".git/"):
                continue
            candidate = root / relative
            if candidate.is_symlink():
                resolved = candidate.resolve()
                if root != resolved and root not in resolved.parents:
                    raise QualificationError(5, "input_escapes_repository", f"input {relative} escapes repository")
                matches.append(relative)
            elif candidate.is_file():
                resolved = candidate.resolve()
                if root != resolved and root not in resolved.parents:
                    raise QualificationError(5, "input_escapes_repository", f"input {relative} escapes repository")
                matches.append(relative)
    except OSError as error:
        raise QualificationError(5, "input_unreadable", f"cannot enumerate {pattern} in {root}: {error}") from error
    return sorted(set(matches))


def _file_identity(
    root: Path, path_value: str, index: dict[str, tuple[str, str]] | None = None
) -> dict[str, Any]:
    path = root / path_value
    head_mode: str | None = None
    head_object: str | None = None
    try:
        index_entry = (index or {}).get(path_value)
        if index is None:
            index_raw = git(root, "ls-files", "-s", "--", path_value, check=False).decode().strip()
            index_parts = index_raw.split(maxsplit=3)
            index_entry = (index_parts[0], index_parts[1]) if len(index_parts) == 4 else None
        index_mode, index_object = index_entry or (None, None)
        if index_mode == "160000":
            content = index_object.encode("ascii")
            state = "gitlink"
            mode = index_mode
        elif path.is_symlink():
            resolved = path.resolve(strict=False)
            if root != resolved and root not in resolved.parents:
                raise QualificationError(5, "input_escapes_repository", f"input {path_value} escapes repository")
            content = os.readlink(path).encode(errors="surrogateescape")
            state = "symlink"
            mode = "120000"
        elif path.is_file():
            content = path.read_bytes()
            state = "file"
            mode = f"{path.stat().st_mode & 0o777777:o}"
        else:
            content = b""
            if path.exists():
                raise QualificationError(5, "input_unreadable", f"unsupported filesystem entry {path_value} in {root}")
            if not index_mode:
                head_raw = git(root, "ls-tree", "-r", "--full-tree", "HEAD", "--", path_value, check=False)
                fields = head_raw.decode(errors="surrogateescape").splitlines()[0].split() if head_raw else []
                if len(fields) >= 3:
                    head_mode, head_object = fields[0], fields[2]
            state = "deleted" if index_mode or head_mode else "missing"
            mode = index_mode or head_mode or "000000"
    except OSError as error:
        raise QualificationError(5, "input_unreadable", f"cannot read {path_value} in {root}: {error}") from error
    return {
        "path": path_value,
        "state": state,
        "mode": mode,
        "index_mode": index_mode,
        "index_object": index_object,
        "head_mode": head_mode,
        "head_object": head_object,
        "bytes": len(content),
        "sha256": sha256_bytes(content),
    }


def governing_input_identities(
    manifest: dict[str, Any], roots: dict[str, Path], inventory: dict[str, list[str]] | None = None,
    indexes: dict[str, dict[str, tuple[str, str]]] | None = None,
) -> list[dict[str, Any]]:
    """Hash governing inputs directly, including ignored files not visible to Git."""
    paths_by_repository: dict[str, set[str]] = {repo_id: set() for repo_id in roots}
    inventory = inventory or {
        repository: sorted(set(_listed_files(root) + _ignored_files(root)))
        for repository, root in roots.items()
    }
    for rule in manifest["path_rules"]:
        if rule["class"] != "governing-docs":
            continue
        repository = rule["repository"]
        root = roots[repository]
        for pattern in rule["includes"]:
            if pattern == "AGENTS.md":
                continue
            if any(character in pattern for character in "*?"):
                paths_by_repository[repository].update(_filesystem_matches(root, pattern, inventory[repository]))
            else:
                paths_by_repository[repository].add(pattern)
    identities: list[dict[str, Any]] = []
    for repository in sorted(paths_by_repository):
        root = roots[repository]
        for path in sorted(paths_by_repository[repository]):
            try:
                identity = _file_identity(root, path, (indexes or {}).get(repository))
            except QualificationError as error:
                raise QualificationError(
                    5,
                    "governing_input_unavailable",
                    f"governing input {repository}:{path} cannot be read: {error}",
                ) from error
            if identity["state"] != "file":
                raise QualificationError(
                    5,
                    "governing_input_unavailable",
                    f"governing input {repository}:{path} is {identity['state']}, not a readable file",
                )
            identities.append({"repository": repository, **identity})
    return identities


def selected_governing_input_identities(
    selections: list[str] | None,
    roots: dict[str, Path],
    indexes: dict[str, dict[str, tuple[str, str]]],
) -> list[dict[str, Any]]:
    normalized: set[tuple[str, str]] = set()
    for selection in selections or []:
        if not isinstance(selection, str) or ":" not in selection:
            raise QualificationError(
                2,
                "governing_input_invalid",
                "--governing-input requires repository-id:relative-path",
            )
        repository, raw_path = selection.split(":", 1)
        if repository not in roots:
            raise QualificationError(
                2,
                "governing_input_invalid",
                f"unknown governing-input repository: {repository}",
            )
        normalized.add((repository, normalize_relative_path(raw_path, "governing input")))

    identities: list[dict[str, Any]] = []
    for repository, path in sorted(normalized):
        identity = _file_identity(roots[repository], path, indexes[repository])
        if identity["state"] != "file":
            raise QualificationError(
                5,
                "governing_input_unavailable",
                f"governing input {repository}:{path} is {identity['state']}, not a readable regular file",
            )
        identities.append({"repository": repository, **identity})
    return identities


def instruction_input_identities(
    roots: dict[str, Path],
    indexes: dict[str, dict[str, tuple[str, str]]],
) -> list[dict[str, Any]]:
    identities: list[dict[str, Any]] = []
    for repository in sorted(roots):
        identity = _file_identity(roots[repository], "AGENTS.md", indexes[repository])
        if identity["state"] == "file":
            identities.append({"repository": repository, **identity})
        elif identity["state"] == "missing":
            identities.append({"repository": repository, "path": "AGENTS.md", "state": "absent"})
        else:
            raise QualificationError(
                5,
                "instruction_input_unavailable",
                f"instruction input {repository}:AGENTS.md is {identity['state']}",
            )
    return identities


def effective_lane_environment(lane: dict[str, Any]) -> dict[str, str | None]:
    environment = {name: os.environ.get(name) for name in sorted(lane["environment_allowlist"])}
    environment.update(lane["environment"])
    return environment


def _remove_temporary_state(root: Path) -> None:
    """Remove a run-owned probe home, including read-only toolchain caches."""
    root = root.absolute()

    def retry_readonly(operation: Any, raw_path: str, exception: tuple[type[BaseException], BaseException, Any]) -> None:
        error = exception[1]
        path = Path(raw_path).absolute()
        if not isinstance(error, PermissionError) or (path != root and root not in path.parents):
            raise error
        for candidate in (path.parent, path):
            if candidate != root and root not in candidate.parents:
                continue
            try:
                if not candidate.is_symlink():
                    os.chmod(candidate, os.stat(candidate).st_mode | stat.S_IWUSR | stat.S_IXUSR)
            except OSError:
                pass
        operation(raw_path)

    for attempt in range(20):
        try:
            shutil.rmtree(root, onerror=retry_readonly)
            return
        except OSError as error:
            if error.errno != errno.ENOTEMPTY or attempt == 19:
                raise
            time.sleep(0.05)


def toolchain_identity(
    toolchain: dict[str, Any],
    repo_root: Path,
    environment: dict[str, str | None] | None = None,
) -> dict[str, Any]:
    cwd = (repo_root / toolchain["cwd"]).resolve()
    if cwd != repo_root and repo_root not in cwd.parents:
        raise QualificationError(5, "cwd_escapes_repository", f"toolchain {toolchain['id']} cwd escapes repository")
    effective_environment = os.environ.copy() if environment is None else dict(environment)
    process_environment = {
        key: value for key, value in effective_environment.items() if value is not None
    }
    environment_sha256 = sha256_bytes(canonical_bytes(effective_environment))
    temporary_state_root: Path | None = None
    if "HOME" not in process_environment or "XDG_CONFIG_HOME" not in process_environment:
        temporary_state_root = Path(tempfile.mkdtemp(prefix="planner-toolchain-home-", dir="/tmp"))
        if "HOME" not in process_environment:
            process_environment["HOME"] = str(temporary_state_root / "home")
        if "XDG_CONFIG_HOME" not in process_environment:
            process_environment["XDG_CONFIG_HOME"] = str(temporary_state_root / "config")
    try:
        try:
            process = subprocess.run(
                toolchain["argv"],
                cwd=cwd,
                env=process_environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                check=False,
                timeout=30,
            )
            result = {
                "id": toolchain["id"],
                "argv": toolchain["argv"],
                "cwd": toolchain["cwd"],
                "environment_sha256": environment_sha256,
                "exit_code": process.returncode,
                "output_sha256": sha256_bytes(process.stdout),
            }
        except (OSError, subprocess.TimeoutExpired) as error:
            result = {
                "id": toolchain["id"],
                "argv": toolchain["argv"],
                "cwd": toolchain["cwd"],
                "environment_sha256": environment_sha256,
                "exit_code": None,
                "error": type(error).__name__,
            }
    finally:
        if temporary_state_root is not None:
            try:
                _remove_temporary_state(temporary_state_root)
            except OSError as error:
                raise QualificationError(5, "toolchain_probe_cleanup_failed", str(error)) from error
    return result


def _lane_digest(
    lane: dict[str, Any],
    roots: dict[str, Path],
    listed: dict[str, list[str]],
    changed: list[dict[str, Any]],
    manifest_digest: str,
    toolchains: dict[str, dict[str, dict[str, Any]]],
    dependency_digests: dict[str, str],
    governing_inputs: list[dict[str, Any]],
    explicit_governing_inputs: list[dict[str, Any]],
    instruction_inputs: list[dict[str, Any]],
    static_matches: dict[tuple[str, str], list[str]],
    indexes: dict[str, dict[str, tuple[str, str]]],
) -> tuple[str, dict[str, Any]]:
    root = roots[lane["repository"]]
    input_paths_by_repo: dict[str, set[str]] = {repo_id: set() for repo_id in roots}
    changed_inputs = []
    for entry in changed:
        if lane["id"] in entry.get("matched_lanes", []):
            input_paths_by_repo[entry["repository"]].add(entry["path"])
            changed_inputs.append({
                "repository": entry["repository"],
                "path": entry["path"],
                "status": entry["status"],
            })
    for pattern in lane["static_inputs"] + lane.get("ignored_static_inputs", []):
        matched = sorted(set(static_matches.get((lane["repository"], pattern), [])))
        if not matched and not any(character in pattern for character in "*?") and (root / pattern).exists():
            matched = [pattern]
        input_paths_by_repo[lane["repository"]].update(matched)
    files = [
        {"repository": repo_id, **_file_identity(roots[repo_id], path, indexes[repo_id])}
        for repo_id in sorted(input_paths_by_repo)
        for path in sorted(input_paths_by_repo[repo_id])
    ]
    allowed_environment = effective_lane_environment(lane)
    payload = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "manifest_sha256": manifest_digest,
        "lane": lane,
        "environment": allowed_environment,
        "toolchains": [toolchains[lane["id"]][item] for item in lane["toolchains"]],
        "changed_inputs": sorted(changed_inputs, key=lambda item: (item["repository"], item["path"], item["status"])),
        "files": files,
        "dependency_digests": {item: dependency_digests[item] for item in lane["depends_on"]},
        "governing_inputs": governing_inputs,
        "explicit_governing_inputs": explicit_governing_inputs,
        "instruction_inputs": instruction_inputs,
    }
    return sha256_bytes(canonical_bytes(payload)), payload


def _transitive(graph: dict[str, list[str]], start: str) -> set[str]:
    result: set[str] = set()
    pending = list(graph[start])
    while pending:
        item = pending.pop()
        if item not in result:
            result.add(item)
            pending.extend(graph[item])
    return result


def _relation_path(graph: dict[str, list[str]], start: str, target: str) -> list[str] | None:
    if start == target:
        return [start]
    for child in graph[start]:
        suffix = _relation_path(graph, child, target)
        if suffix:
            return [start, *suffix]
    return None


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and bool(re.fullmatch(r"[0-9a-f]{64}", value))


def _is_finite_number(value: Any, minimum: float = 0.0) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
        and value >= minimum
    )


def _is_utc_timestamp(value: Any) -> bool:
    if not isinstance(value, str) or not value:
        return False
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError:
        return False
    return (
        parsed.tzinfo is not None
        and parsed.utcoffset() == dt.timedelta(0)
        and value == parsed.isoformat()
    )


def _timestamp_matches_epoch(epoch: Any, timestamp: Any) -> bool:
    if not _is_finite_number(epoch):
        return False
    if not _is_utc_timestamp(timestamp):
        return False
    try:
        parsed = dt.datetime.fromisoformat(timestamp)
    except ValueError:
        return False
    return math.isclose(parsed.timestamp(), float(epoch), rel_tol=0.0, abs_tol=0.000001)


def _strict_record_json(line: bytes) -> Any:
    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    def reject_nonfinite(value: str) -> Any:
        raise ValueError(f"non-finite JSON number: {value}")

    value = json.loads(
        line.decode("utf-8"),
        object_pairs_hook=reject_duplicate_keys,
        parse_constant=reject_nonfinite,
    )
    if canonical_bytes(value) != line + b"\n":
        raise ValueError("record JSON is not canonical")
    return value


def _is_git_object_id(value: Any) -> bool:
    return isinstance(value, str) and bool(re.fullmatch(r"[0-9a-f]{40}", value))


REPOSITORY_IDENTITY_FIELDS = {
    "branch", "head", "head_tree", "porcelain_v2_sha256",
    "untracked_content_manifest_sha256", "index_manifest_sha256",
    "worktree_content_manifest_sha256", "base",
}


def _validate_repository_identity(identity: Any, label: str) -> str | None:
    if not isinstance(identity, dict) or set(identity) != REPOSITORY_IDENTITY_FIELDS:
        return f"{label} has an invalid field set"
    if not isinstance(identity["branch"], str):
        return f"{label} branch is invalid"
    for field in ("head", "head_tree", "base"):
        if not _is_git_object_id(identity[field]):
            return f"{label} {field} is invalid"
    for field in (
        "porcelain_v2_sha256", "untracked_content_manifest_sha256",
        "index_manifest_sha256", "worktree_content_manifest_sha256",
    ):
        if not _is_sha256(identity[field]):
            return f"{label} {field} is invalid"
    return None


def _validate_repository_identities(
    identities: Any, expected_ids: set[str] | None, label: str
) -> str | None:
    if not isinstance(identities, dict) or any(not isinstance(key, str) for key in identities):
        return f"{label} is invalid"
    if expected_ids is not None and set(identities) != expected_ids:
        return f"{label} repository set is invalid"
    for repository, identity in identities.items():
        error = _validate_repository_identity(identity, f"{label}.{repository}")
        if error:
            return error
    return None


def _validate_toolchain_records(toolchains: Any) -> str | None:
    if not isinstance(toolchains, list):
        return "record toolchains must be an array"
    seen: set[str] = set()
    for item in toolchains:
        if not isinstance(item, dict):
            return "record toolchain is not an object"
        required = {"id", "argv", "cwd", "environment_sha256", "exit_code"}
        optional_result = {"output_sha256", "error"}
        if not required.issubset(item) or set(item) - required - optional_result:
            return "record toolchain fields are invalid"
        if set(item).intersection(optional_result) not in ({"output_sha256"}, {"error"}):
            return "record toolchain result is invalid"
        if not isinstance(item["id"], str) or not item["id"] or item["id"] in seen:
            return "record toolchain ID is invalid"
        seen.add(item["id"])
        if (
            not isinstance(item["argv"], list)
            or not item["argv"]
            or not all(isinstance(value, str) and value for value in item["argv"])
            or not isinstance(item["cwd"], str)
            or not item["cwd"]
            or not _is_sha256(item["environment_sha256"])
        ):
            return "record toolchain fields are invalid"
        try:
            normalize_relative_path(item["cwd"], "record toolchain cwd")
        except QualificationError:
            return "record toolchain cwd is invalid"
        exit_code = item["exit_code"]
        if exit_code is not None and (isinstance(exit_code, bool) or not isinstance(exit_code, int)):
            return "record toolchain exit_code is invalid"
        if "output_sha256" in item and not _is_sha256(item["output_sha256"]):
            return "record toolchain output hash is invalid"
        if "error" in item and (not isinstance(item["error"], str) or not item["error"]):
            return "record toolchain error is invalid"
        if exit_code is None and "error" not in item:
            return "record unavailable toolchain is missing its error"
        if exit_code is not None and "output_sha256" not in item:
            return "record available toolchain is missing its output hash"
    return None


def _record_log_path(records: Path, value: Any) -> Path:
    if not isinstance(value, str) or not value or "\\" in value:
        raise QualificationError(6, "log_invalid", "record log path is invalid")
    relative = PurePosixPath(value)
    if (
        relative.is_absolute()
        or len(relative.parts) != 1
        or relative.parts[0] in ("", ".", "..")
        or relative.suffix != ".log"
    ):
        raise QualificationError(6, "log_invalid", "record log path escapes records")
    path = records / relative.as_posix()
    resolved_records = records.resolve()
    resolved_path = path.resolve(strict=False)
    if resolved_path != resolved_records and resolved_records not in resolved_path.parents:
        raise QualificationError(6, "log_invalid", "record log path escapes records")
    if path.is_symlink():
        raise QualificationError(6, "log_invalid", "record log path is a symlink")
    return path


def _validate_record_event(
    event: Any, expected: str, expected_repository_ids: set[str] | None = None
) -> str | None:
    if not isinstance(event, dict):
        return "record event must be an object"
    if event.get("schema_version") != ATTEMPT_RECORD_SCHEMA_VERSION:
        return f"record schema version is not {ATTEMPT_RECORD_SCHEMA_VERSION}"
    if event.get("event") != expected:
        return f"record event is not {expected}"
    required = STARTED_FIELDS if expected == "started" else FINISHED_FIELDS
    if set(event) != required:
        missing = sorted(required - set(event))
        extra = sorted(set(event) - required)
        return f"record fields mismatch; missing={missing}, extra={extra}"
    if not isinstance(event["attempt_id"], str) or not event["attempt_id"]:
        return "record attempt_id is invalid"
    if not isinstance(event["lane_id"], str) or not event["lane_id"]:
        return "record lane_id is invalid"
    if not isinstance(event["log"], str) or not event["log"]:
        return "record log is invalid"
    if expected == "started":
        if (
            not isinstance(event["repository"], str)
            or not isinstance(event["cwd"], str)
            or not event["cwd"]
            or not isinstance(event["environment"], dict)
            or not all(
                isinstance(key, str) and key and (value is None or isinstance(value, str))
                for key, value in event["environment"].items()
            )
        ):
            return "started record repository, cwd, or environment is invalid"
        try:
            normalize_relative_path(event["cwd"], "started record cwd")
        except QualificationError:
            return "started record cwd is invalid"
        if (
            not _is_sha256(event["plan_sha256"])
            or not _is_sha256(event["manifest_sha256"])
            or not _is_sha256(event["relevant_input_sha256"])
        ):
            return "started record hash is invalid"
        if not isinstance(event["argv"], list) or not all(isinstance(item, str) and item for item in event["argv"]):
            return "started record argv is invalid"
        toolchain_error = _validate_toolchain_records(event["toolchains"])
        if toolchain_error:
            return f"started {toolchain_error}"
        if (
            not isinstance(event["dependency_digests"], dict)
            or any(not isinstance(key, str) or not key or not _is_sha256(value)
                   for key, value in event["dependency_digests"].items())
        ):
            return "started record dependency digests are invalid"
        repository_error = _validate_repository_identities(
            event["repositories_before"], expected_repository_ids, "started repositories_before"
        )
        if repository_error:
            return repository_error
        if isinstance(event["timeout_seconds"], bool) or not isinstance(event["timeout_seconds"], int) or event["timeout_seconds"] <= 0:
            return "started record timeout is invalid"
        if not _timestamp_matches_epoch(event["start_epoch_seconds"], event["start_utc"]):
            return "started record timestamp is invalid"
    else:
        if event["exit_code"] is not None and (isinstance(event["exit_code"], bool) or not isinstance(event["exit_code"], int)):
            return "finished record exit_code is invalid"
        if event["signal"] is not None and (isinstance(event["signal"], bool) or not isinstance(event["signal"], int)):
            return "finished record signal is invalid"
        if event["exit_code"] is None and event["signal"] is not None:
            return "finished record signal is invalid"
        if event["exit_code"] is not None and event["exit_code"] < 0:
            if event["signal"] != -event["exit_code"]:
                return "finished record signal does not match exit_code"
        elif event["signal"] is not None:
            return "finished record signal does not match exit_code"
        if not isinstance(event["timed_out"], bool) or isinstance(event["log_bytes"], bool) or not isinstance(event["log_bytes"], int) or event["log_bytes"] < 0:
            return "finished record timeout or log size is invalid"
        repository_error = _validate_repository_identities(
            event["repositories_after"], expected_repository_ids, "finished repositories_after"
        )
        if repository_error:
            return repository_error
        if not isinstance(event["harness_error"], (str, type(None))):
            return "finished record harness error is invalid"
        if not _timestamp_matches_epoch(event["end_epoch_seconds"], event["end_utc"]):
            return "finished record timestamp is invalid"
        if event["relevant_input_sha256_after"] is not None and not _is_sha256(event["relevant_input_sha256_after"]):
            return "finished record relevant-input hash is invalid"
        if not _is_sha256(event["log_sha256"]):
            return "finished record log hash is invalid"
        if not _is_finite_number(event["duration_seconds"]):
            return "finished record duration is invalid"
    return None


def _validate_record_pair(started: dict[str, Any], finished: dict[str, Any]) -> str | None:
    for field in ("attempt_id", "lane_id", "log"):
        if started.get(field) != finished.get(field):
            return f"started and finished {field} do not match"
    if float(finished["end_epoch_seconds"]) < float(started["start_epoch_seconds"]):
        return "finished timestamp precedes started timestamp"
    if finished["timed_out"] and finished["exit_code"] == 0:
        return "timed-out record has a successful exit code"
    return None


def _read_attempt_pairs(
    records: Path, expected_repository_ids: set[str], allow_incomplete: bool = False
) -> tuple[dict[str, dict[str, dict[str, Any]]], dict[str, str] | None]:
    """Read the shared attempt stream while permitting concurrent interleaving."""
    events_path = records / "attempts.jsonl"
    if events_path.is_symlink():
        return {}, {"reason_code": "records_path_invalid", "reason": "attempts.jsonl is a symlink"}
    if not events_path.is_file():
        return {}, {"reason_code": "records_missing", "reason": "append-only records are unavailable"}
    attempts: dict[str, dict[str, dict[str, Any]]] = {}
    open_attempts: set[str] = set()
    try:
        raw_lines = events_path.read_bytes().split(b"\n")
        if raw_lines and raw_lines[-1] == b"":
            raw_lines.pop()
        if not raw_lines:
            return {}, {"reason_code": "records_empty", "reason": "append-only records are empty"}
        for number, line in enumerate(raw_lines, 1):
            if not line.strip():
                return {}, {"reason_code": "records_malformed", "reason": f"blank record at line {number}"}
            event = _strict_record_json(line)
            if not isinstance(event, dict):
                return {}, {"reason_code": "records_malformed", "reason": f"record at line {number} is not an object"}
            kind = event.get("event")
            if kind not in ("started", "finished"):
                return {}, {"reason_code": "records_malformed", "reason": f"unknown event at line {number}"}
            validation_error = _validate_record_event(event, kind, expected_repository_ids)
            if validation_error:
                reason_code = (
                    "records_schema_stale"
                    if event.get("schema_version") != ATTEMPT_RECORD_SCHEMA_VERSION
                    else "records_malformed"
                )
                return {}, {"reason_code": reason_code, "reason": f"line {number}: {validation_error}"}
            attempt_id = event["attempt_id"]
            if kind == "started":
                if attempt_id in attempts:
                    return {}, {"reason_code": "duplicate_event", "reason": f"invalid started event at line {number}"}
                attempts[attempt_id] = {"started": event}
                open_attempts.add(attempt_id)
            else:
                if attempt_id not in open_attempts or "finished" in attempts.get(attempt_id, {}):
                    return {}, {"reason_code": "event_order", "reason": f"invalid finished event at line {number}"}
                attempts[attempt_id]["finished"] = event
                open_attempts.remove(attempt_id)
    except (OSError, UnicodeError, json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
        return {}, {"reason_code": "records_malformed", "reason": str(error)}
    if open_attempts:
        attempt_id = min(open_attempts)
        if allow_incomplete:
            return attempts, None
        return {}, {"reason_code": "attempt_incomplete", "reason": f"attempt {attempt_id} is incomplete"}
    return attempts, None


def _record_git_anchors_resolve(
    roots: dict[str, Path], identities: dict[str, Any], label: str
) -> str | None:
    for repository, identity in identities.items():
        root = roots.get(repository)
        if root is None:
            return f"{label}.{repository} has no configured root"
        resolved_head = git(root, "rev-parse", "--verify", f"{identity['head']}^{{commit}}", check=False).decode().strip()
        resolved_tree = git(root, "rev-parse", "--verify", f"{identity['head']}^{{tree}}", check=False).decode().strip()
        if resolved_head != identity["head"] or resolved_tree != identity["head_tree"]:
            return f"{label}.{repository} Git anchors do not resolve"
    return None


def _stable_repository_anchors_match(
    recorded: dict[str, Any], current: dict[str, Any]
) -> bool:
    return all(
        recorded[repository][field] == current[repository][field]
        for repository in current
        for field in ("branch", "head", "head_tree", "base")
    )


def _prior_reuse(
    records: Path | None,
    lane: dict[str, Any],
    digest: str,
    manifest_digest: str,
    payload: dict[str, Any],
    repositories: dict[str, Any],
    roots: dict[str, Path],
) -> tuple[bool, list[dict[str, str]], str | None]:
    rejected: list[dict[str, str]] = []
    if records is None:
        return False, rejected, None
    try:
        _ensure_no_symlink_ancestors(records, 6, "records path")
    except QualificationError as error:
        return False, [{"reason_code": "records_path_invalid", "reason": str(error)}], None
    if records.is_symlink() or (records.exists() and not records.is_dir()):
        return False, [{"reason_code": "records_path_invalid", "reason": "records path is not a real directory"}], None
    attempts, record_error = _read_attempt_pairs(records, set(repositories))
    if record_error:
        return False, [record_error], None
    for attempt_id, events in reversed(list(attempts.items())):
        started, finished = events.get("started"), events.get("finished")
        if not started or not finished:
            return False, [{"reason_code": "attempt_incomplete", "reason": f"attempt {attempt_id} is incomplete"}], None
        pair_error = _validate_record_pair(started, finished)
        if pair_error:
            return False, [{"reason_code": "records_malformed", "reason": f"attempt {attempt_id}: {pair_error}"}], None
        anchor_error = _record_git_anchors_resolve(roots, started["repositories_before"], "started repositories_before")
        if anchor_error:
            rejected.append({"reason_code": "repository_identity_invalid", "reason": f"attempt {attempt_id}: {anchor_error}"})
            continue
        anchor_error = _record_git_anchors_resolve(roots, finished["repositories_after"], "finished repositories_after")
        if anchor_error:
            rejected.append({"reason_code": "repository_identity_invalid", "reason": f"attempt {attempt_id}: {anchor_error}"})
            continue
        toolchains_available = all(
            item.get("exit_code") == 0
            and _is_sha256(item.get("environment_sha256"))
            and _is_sha256(item.get("output_sha256"))
            for item in payload["toolchains"]
        )
        conditions = (
            toolchains_available,
            started.get("lane_id") == lane["id"],
            started.get("manifest_sha256") == manifest_digest,
            started.get("relevant_input_sha256") == digest,
            started.get("argv") == lane["argv"],
            started.get("cwd") == lane["cwd"],
            started.get("repository") == lane["repository"],
            started.get("environment") == payload["environment"],
            started.get("toolchains") == payload["toolchains"],
            started.get("dependency_digests") == payload["dependency_digests"],
            finished.get("exit_code") == 0,
            finished.get("signal") is None,
            not finished.get("timed_out"),
            finished.get("harness_error") is None,
            finished.get("relevant_input_sha256_after") == digest,
            started.get("repositories_before") == finished.get("repositories_after"),
            set(finished.get("repositories_after", {})) == set(repositories),
            _stable_repository_anchors_match(started["repositories_before"], repositories),
            started["repositories_before"].get("go-oversync") == repositories.get("go-oversync"),
            finished.get("lane_id") == started.get("lane_id"),
            finished.get("log") == started.get("log"),
        )
        if not all(conditions):
            rejected.append({"reason_code": "attempt_inapplicable", "reason": f"attempt {attempt_id} does not match current relevant inputs"})
            continue
        try:
            log = _record_log_path(records, finished["log"])
        except QualificationError as error:
            rejected.append({"reason_code": error.code, "reason": f"attempt {attempt_id}: {error}"})
            continue
        if (
            not log.is_file()
            or log.is_symlink()
            or log.stat().st_size != finished.get("log_bytes")
            or sha256_file(log) != finished.get("log_sha256")
        ):
            rejected.append({"reason_code": "log_invalid", "reason": f"attempt {attempt_id} log is missing or changed"})
            continue
        return True, rejected, attempt_id
    return False, rejected, None


def build_plan(
    manifest: dict[str, Any],
    manifest_digest: str,
    base: str,
    repo_filter: list[str] | None = None,
    injected_paths: list[str] | None = None,
    records: Path | None = None,
    workspace_root: Path = WORKSPACE_ROOT,
    explicit_governing_input_selections: list[str] | None = None,
) -> dict[str, Any]:
    roots = repository_roots(manifest, workspace_root)
    selected_repos = repo_filter or [entry["id"] for entry in manifest["repositories"]]
    if any(repo not in roots for repo in selected_repos):
        raise QualificationError(2, "unknown_repository", "--repo names an unknown repository")
    indexes = {repo: _index_entries(root) for repo, root in roots.items()}
    identities = {repo: repository_identity(root, indexes[repo]) for repo, root in roots.items()}
    for repo in roots:
        identities[repo]["base"] = git(roots[repo], "rev-parse", base).decode().strip()

    discovered: list[dict[str, str]] = []
    if injected_paths:
        for value in injected_paths:
            if ":" not in value:
                raise QualificationError(4, "changed_path_unclassified", f"injected path needs repository prefix: {value}")
            repo, path = value.split(":", 1)
            if repo not in selected_repos:
                raise QualificationError(4, "changed_path_unclassified", f"unknown or unselected repository: {repo}")
            discovered.append({
                "repository": repo,
                "path": normalize_relative_path(path, "changed path"),
                "status": "injected",
            })
    else:
        for repo in selected_repos:
            discovered.extend({"repository": repo, **entry} for entry in changed_paths(roots[repo], base))

    selected: set[str] = set()
    classified: list[dict[str, Any]] = []
    unknown_paths: list[dict[str, str]] = []
    for entry in sorted(discovered, key=lambda item: (item["repository"], item["path"], item["status"])):
        matched_rules: list[dict[str, Any]] = []
        for rule in manifest["path_rules"]:
            if rule["repository"] != entry["repository"]:
                continue
            included = any(glob_matches(pattern, entry["path"]) for pattern in rule["includes"])
            excluded = any(glob_matches(pattern, entry["path"]) for pattern in rule["excludes"])
            if included and not excluded:
                matched_rules.append(rule)
        if matched_rules:
            matched_lanes = sorted({lane for rule in matched_rules for lane in rule["lanes"]})
            selected.update(matched_lanes)
            classified.append({
                **entry,
                "matched_rules": [rule["id"] for rule in matched_rules],
                "matched_lanes": matched_lanes,
                "fallback": False,
            })
        else:
            fallback = list(manifest["fallback_lane_ids"])
            if not fallback:
                raise QualificationError(4, "changed_path_unclassified", f"no fallback for {entry['repository']}:{entry['path']}")
            selected.update(fallback)
            unknown_paths.append(entry)
            classified.append({
                **entry,
                "matched_rules": [],
                "matched_lanes": fallback,
                "fallback": True,
            })

    lane_by_id = {lane["id"]: lane for lane in manifest["lanes"]}
    dependency_graph = {lane["id"]: lane["depends_on"] for lane in manifest["lanes"]}
    for lane_id in list(selected):
        selected.update(_transitive(dependency_graph, lane_id))

    input_patterns: dict[str, set[str]] = {repo: set() for repo in roots}
    ignored_patterns: dict[str, set[str]] = {repo: set() for repo in roots}
    for rule in manifest["path_rules"]:
        if rule["class"] == "governing-docs":
            input_patterns[rule["repository"]].update(rule["includes"])
            ignored_patterns[rule["repository"]].update(rule["includes"])
    for lane_id in selected:
        lane = lane_by_id[lane_id]
        input_patterns[lane["repository"]].update(lane["static_inputs"])
        ignored_patterns[lane["repository"]].update(lane.get("ignored_static_inputs", []))
    listed = {
        repo: sorted(set(
            _listed_files(root, input_patterns[repo])
            + _ignored_files(root, ignored_patterns[repo])
        ))
        for repo, root in roots.items()
    }
    governing_inputs = governing_input_identities(manifest, roots, listed, indexes)
    explicit_governing_inputs = selected_governing_input_identities(
        explicit_governing_input_selections,
        roots,
        indexes,
    )
    instruction_inputs = instruction_input_identities(roots, indexes)
    static_matches = {
        (repo, pattern): _filesystem_matches(roots[repo], pattern, listed[repo])
        for repo, patterns in {
            repository: input_patterns[repository] | ignored_patterns[repository]
            for repository in roots
        }.items()
        for pattern in patterns
    }

    subsumption_graph = {lane["id"]: lane["subsumes"] for lane in manifest["lanes"]}
    subsumed_paths: dict[str, list[str]] = {}
    for aggregate in manifest["lanes"]:
        if aggregate["id"] not in selected:
            continue
        for child in _transitive(subsumption_graph, aggregate["id"]):
            if child in selected:
                path = _relation_path(subsumption_graph, aggregate["id"], child)
                if path and len(path) > len(subsumed_paths.get(child, [])):
                    subsumed_paths[child] = path

    selected_aggregates = [
        lane["id"] for lane in manifest["lanes"]
        if lane["id"] in selected and lane["subsumes"]
    ]
    aggregate_overlaps: list[dict[str, Any]] = []
    for index, left in enumerate(selected_aggregates):
        left_children = _transitive(subsumption_graph, left)
        for right in selected_aggregates[index + 1:]:
            right_children = _transitive(subsumption_graph, right)
            if right in left_children or left in right_children:
                continue
            common_children = sorted(left_children.intersection(right_children))
            if common_children:
                aggregate_overlaps.append({
                    "aggregates": [left, right],
                    "common_children": common_children,
                })

    # A rule in one selected repository may select a lane owned by the sibling.
    toolchain_by_id = {entry["id"]: entry for entry in manifest["toolchains"]}
    toolchain_results: dict[str, dict[str, dict[str, Any]]] = {}
    toolchain_cache: dict[tuple[str, str], dict[str, Any]] = {}
    for lane_id in (lane["id"] for lane in manifest["lanes"] if lane["id"] in selected):
        lane = lane_by_id[lane_id]
        lane_environment = effective_lane_environment(lane)
        environment_key = sha256_bytes(canonical_bytes(lane_environment))
        toolchain_results[lane_id] = {}
        for toolchain_id in lane["toolchains"]:
            cache_key = (toolchain_id, environment_key)
            if cache_key not in toolchain_cache:
                toolchain = toolchain_by_id[toolchain_id]
                owner = toolchain.get("repository", "sqlite-now")
                toolchain_cache[cache_key] = toolchain_identity(
                    toolchain,
                    roots[owner],
                    lane_environment,
                )
            toolchain_results[lane_id][toolchain_id] = toolchain_cache[cache_key]

    dependency_digests: dict[str, str] = {}
    payloads: dict[str, dict[str, Any]] = {}
    pending = [lane["id"] for lane in manifest["lanes"] if lane["id"] in selected]
    while pending:
        progress = False
        for lane_id in list(pending):
            lane = lane_by_id[lane_id]
            if all(dependency in dependency_digests for dependency in lane["depends_on"]):
                digest, payload = _lane_digest(
                    lane, roots, listed, classified, manifest_digest,
                    toolchain_results,
                    dependency_digests,
                    governing_inputs,
                    explicit_governing_inputs,
                    instruction_inputs,
                    static_matches,
                    indexes,
                )
                dependency_digests[lane_id] = digest
                payloads[lane_id] = payload
                pending.remove(lane_id)
                progress = True
        if not progress:
            raise QualificationError(3, "manifest_cycle", "cannot order lane dependencies")

    lane_results: list[dict[str, Any]] = []
    for lane in manifest["lanes"]:
        lane_id = lane["id"]
        reasons: list[dict[str, str]] = []
        for path in classified:
            if lane_id in path["matched_lanes"]:
                if path["fallback"]:
                    reasons.append({
                        "reason_code": "unknown_path_fallback",
                        "reason": f"conservative fallback for {path['repository']}:{path['path']}",
                    })
                else:
                    for rule_id in path["matched_rules"]:
                        rule = next(item for item in manifest["path_rules"] if item["id"] == rule_id)
                        if lane_id in rule["lanes"]:
                            reasons.append({"reason_code": rule["reason_code"], "reason": rule["reason"]})
        if lane_id in selected and not reasons:
            parents = [parent for parent in selected if lane_id in _transitive(dependency_graph, parent)]
            reasons.append({
                "reason_code": "lane_dependency",
                "reason": f"required by {', '.join(sorted(parents))}",
            })
        if lane_id not in selected:
            disposition = "excluded"
            reasons = [{"reason_code": "no_relevant_change", "reason": "no changed input selected this lane"}]
            digest = None
            reuse_attempt = None
            rejected: list[dict[str, str]] = []
        elif lane_id in subsumed_paths:
            disposition = "subsumed"
            reasons.append({
                "reason_code": "aggregate_subsumption",
                "reason": f"covered by {' -> '.join(subsumed_paths[lane_id])}",
            })
            digest = dependency_digests[lane_id]
            reuse_attempt = None
            rejected = []
        else:
            digest = dependency_digests[lane_id]
            reusable, rejected, reuse_attempt = _prior_reuse(
                records, lane, digest, manifest_digest, payloads[lane_id], identities, roots,
            )
            if reusable:
                disposition = "reuse_recommended"
                reasons.append({
                    "reason_code": "reuse_recommended",
                    "reason": "matching prior attempt may be reused; reviewer confirmation required",
                })
            else:
                disposition = "invalidated" if records is not None and rejected else "selected"
                reasons.append({"reason_code": "lane_selected", "reason": "lane selected for advisory execution"})
        lane_results.append({
            "id": lane_id,
            "classification": lane["classification"],
            "argv": lane["argv"],
            "cwd": lane["cwd"],
            "repository": lane["repository"],
            "depends_on": lane["depends_on"],
            "resource_locks": lane["resource_locks"],
            "scheduling_class": lane["scheduling_class"],
            "memory_class": lane["memory_class"],
            "campaign_eligible": lane["campaign_eligible"],
            "selected": lane_id in selected,
            "disposition": disposition,
            "subsumed_by": subsumed_paths.get(lane_id, [None])[0],
            "subsumption_path": subsumed_paths.get(lane_id, []),
            "relevant_input_sha256": digest,
            "toolchains": payloads.get(lane_id, {}).get("toolchains", []),
            "reasons": reasons,
            "reuse_attempt_id": reuse_attempt,
            "rejected_prior_evidence": rejected,
        })

    plan: dict[str, Any] = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "authority": "advisory_only_reviewer_confirmation_required",
        "manifest_sha256": manifest_digest,
        "planning_request": {
            "base": base,
            "repositories": selected_repos,
            "injected_paths": injected_paths or [],
            "explicit_governing_inputs": sorted(set(explicit_governing_input_selections or [])),
        },
        "repositories": identities,
        "governing_inputs": governing_inputs,
        "explicit_governing_inputs": explicit_governing_inputs,
        "instruction_inputs": instruction_inputs,
        "changed_paths": classified,
        "unknown_paths": unknown_paths,
        "aggregate_overlaps": aggregate_overlaps,
        "lanes": lane_results,
    }
    plan["plan_sha256"] = sha256_bytes(canonical_bytes(plan))
    return plan


def concise_report(plan: dict[str, Any]) -> str:
    lines = ["Qualification advisory plan — reviewer confirmation required"]
    for lane in plan["lanes"]:
        if lane["disposition"] != "excluded":
            suffix = f" by {lane['subsumed_by']}" if lane["subsumed_by"] else ""
            command = " ".join(lane["argv"])
            lines.append(f"{lane['disposition']}: {lane['id']}{suffix} :: {command}")
    if not any(lane["selected"] for lane in plan["lanes"]):
        lines.append("selected: none")
    for path in plan["unknown_paths"]:
        lines.append(f"unknown_path_fallback: {path['repository']}:{path['path']}")
    for overlap in plan.get("aggregate_overlaps", []):
        lines.append(
            f"aggregate_overlap: {', '.join(overlap['aggregates'])} -> "
            f"{', '.join(overlap['common_children'])}"
        )
    excluded = [lane["id"] for lane in plan["lanes"] if lane["disposition"] == "excluded"]
    if excluded:
        lines.append(f"excluded ({len(excluded)}): {', '.join(excluded)}")
    return "\n".join(lines) + "\n"


def _ensure_no_symlink_ancestors(path: Path, code: int, label: str) -> None:
    absolute = path.absolute()
    current = Path(absolute.anchor)
    for index, part in enumerate(absolute.parts[1:], 1):
        current /= part
        # macOS commonly exposes /tmp and /var as system-level symlinks. Reject
        # symlinks below that root anchor, where a caller-controlled ancestor
        # could redirect records or output outside the requested hierarchy.
        if current.is_symlink() and index > 1:
            raise QualificationError(code, "path_symlink", f"{label} contains a symlink: {current}")
        if current != absolute and current.exists() and not current.is_dir():
            raise QualificationError(code, "path_not_directory", f"{label} ancestor is not a directory: {current}")


def _write_json(data: bytes, output: str, label: str) -> None:
    if output == "-":
        sys.stdout.buffer.write(data)
        return
    target = Path(output)
    _ensure_no_symlink_ancestors(target, 5, label)
    if target.exists() and not target.is_file():
        raise QualificationError(5, "plan_output_failed", f"{label} is not a regular file: {target}")
    descriptor: int | None = None
    temporary: Path | None = None
    for attempt in range(100):
        candidate = target.with_name(f".{target.name}.tmp-{os.getpid()}-{attempt}")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(candidate, flags, 0o644)
            temporary = candidate
            break
        except FileExistsError:
            continue
        except OSError as error:
            raise QualificationError(5, "plan_output_failed", str(error)) from error
    if descriptor is None or temporary is None:
        raise QualificationError(5, "plan_output_failed", f"cannot allocate a unique temporary path beside {target}")
    try:
        written = 0
        while written < len(data):
            written += os.write(descriptor, data[written:])
        os.fsync(descriptor)
    except OSError as error:
        raise QualificationError(5, "plan_output_failed", str(error)) from error
    finally:
        os.close(descriptor)
    try:
        os.replace(temporary, target)
        directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        directory_descriptor = os.open(target.parent, directory_flags)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    except OSError as error:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise QualificationError(5, "plan_output_failed", str(error)) from error


def write_json(data: bytes, output: str) -> None:
    _write_json(data, output, "JSON output path")


def write_plan(plan: dict[str, Any], output: str) -> None:
    _write_json(canonical_bytes(plan), output, "plan output path")
    if output == "-":
        sys.stderr.write(concise_report(plan))
        return
    sys.stdout.write(concise_report(plan))


def _append_event(path: Path, event: dict[str, Any]) -> None:
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise OSError(f"append-only record path is not a regular file: {path}")
    line = canonical_bytes(event)
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o644)
    try:
        written = 0
        while written < len(line):
            written += os.write(descriptor, line[written:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _now() -> tuple[float, str]:
    epoch = time.time()
    return epoch, dt.datetime.fromtimestamp(epoch, tz=dt.timezone.utc).isoformat()


def _effective_timeout(manifest: dict[str, Any], lane: dict[str, Any]) -> int:
    return lane.get("timeout_seconds", manifest["default_timeout_seconds"])


def _records_directory(records: Path) -> Path:
    if records.is_symlink():
        raise QualificationError(12, "records_path_invalid", "records directory is a symlink")
    _ensure_no_symlink_ancestors(records, 12, "records path")
    try:
        records.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise QualificationError(12, "records_path_invalid", f"cannot create records directory: {error}") from error
    if not records.is_dir():
        raise QualificationError(12, "records_path_invalid", "records path is not a directory")
    _ensure_no_symlink_ancestors(records, 12, "records path")
    return records.resolve()


def _terminate_process_group(process: subprocess.Popen[bytes]) -> int | None:
    if process.poll() is not None:
        return process.returncode
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return process.poll()
    try:
        return process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        return process.wait()


def _current_repositories(roots: dict[str, Path], fallback: dict[str, Any]) -> dict[str, Any]:
    try:
        current = {repository: repository_identity(root) for repository, root in roots.items()}
        for repository, identity in current.items():
            if isinstance(fallback.get(repository), dict) and "base" in fallback[repository]:
                identity["base"] = fallback[repository]["base"]
        return current
    except QualificationError:
        return fallback


def _validate_execution_snapshot(
    saved_plan: dict[str, Any],
    current: dict[str, Any],
    manifest_digest: str,
) -> None:
    saved_repositories = saved_plan["repositories"]
    if set(saved_repositories) != set(current["repositories"]):
        raise QualificationError(5, "plan_stale", "saved repository inventory does not match current state")
    for repository, identity in current["repositories"].items():
        saved_identity = saved_repositories.get(repository)
        if not isinstance(saved_identity, dict) or saved_identity.get("branch") != identity.get("branch"):
            raise QualificationError(5, "repository_branch_stale", f"repository branch for {repository} changed")
        if repository == "go-oversync" and saved_identity != identity:
            raise QualificationError(5, "read_only_repository_stale", "read-only repository Git identity changed")
    if current["governing_inputs"] != saved_plan.get("governing_inputs"):
        raise QualificationError(5, "governing_input_stale", "a governing input changed")
    if current["explicit_governing_inputs"] != saved_plan.get("explicit_governing_inputs"):
        raise QualificationError(5, "governing_input_stale", "an explicitly selected governing input changed")
    if current["instruction_inputs"] != saved_plan.get("instruction_inputs"):
        raise QualificationError(5, "instruction_input_stale", "an instruction input changed presence or content")
    saved_lanes = {lane["id"]: lane for lane in saved_plan["lanes"]}
    current_lanes = {lane["id"]: lane for lane in current["lanes"]}
    if set(saved_lanes) != set(current_lanes):
        raise QualificationError(5, "plan_stale", "saved lane inventory does not match current state")
    for lane_id, saved in saved_lanes.items():
        current_lane = current_lanes[lane_id]
        for field in (
            "id", "classification", "argv", "cwd", "repository", "depends_on", "selected",
            "subsumed_by", "subsumption_path", "toolchains", "resource_locks", "scheduling_class",
            "memory_class", "campaign_eligible",
        ):
            if saved.get(field) != current_lane.get(field):
                raise QualificationError(5, "plan_stale", f"saved lane {lane_id} field {field} changed")
        if saved.get("selected") and current_lane.get("relevant_input_sha256") != saved.get("relevant_input_sha256"):
            raise QualificationError(5, "relevant_input_stale", f"lane {lane_id} relevant inputs changed")
    if saved_plan.get("manifest_sha256") != manifest_digest:
        raise QualificationError(5, "manifest_stale", "saved plan manifest digest does not match current manifest")


def _refresh_execution_snapshot(
    manifest_path: Path,
    saved_plan: dict[str, Any],
    request: dict[str, Any],
    expected_manifest_digest: str,
    workspace_root: Path,
) -> tuple[dict[str, Any], str, dict[str, Any]]:
    manifest, manifest_digest = load_manifest(manifest_path)
    if manifest_digest != expected_manifest_digest:
        raise QualificationError(5, "manifest_stale", "manifest changed during execution")
    current = build_plan(
        manifest,
        manifest_digest,
        request.get("base", "HEAD"),
        request.get("repositories"),
        request.get("injected_paths"),
        None,
        workspace_root,
        request.get("explicit_governing_inputs"),
    )
    _validate_execution_snapshot(saved_plan, current, manifest_digest)
    return manifest, manifest_digest, current


def run_plan(
    plan_path: Path,
    manifest_path: Path,
    lane_ids: list[str],
    accept_selected: bool,
    records: Path,
    continue_on_failure: bool,
    workspace_root: Path = WORKSPACE_ROOT,
    authorize_product: bool = False,
) -> int:
    manifest, manifest_digest = load_manifest(manifest_path)
    try:
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise QualificationError(5, "plan_unreadable", f"cannot read saved plan: {error}") from error
    if not isinstance(plan, dict):
        raise QualificationError(5, "plan_stale", "saved plan is not an object")
    if plan.get("schema_version") != PLAN_SCHEMA_VERSION:
        raise QualificationError(5, "plan_stale", f"saved plan schema version is not {PLAN_SCHEMA_VERSION}")
    supplied_digest = plan.get("plan_sha256")
    without_digest = dict(plan)
    without_digest.pop("plan_sha256", None)
    if supplied_digest != sha256_bytes(canonical_bytes(without_digest)) or plan.get("manifest_sha256") != manifest_digest:
        raise QualificationError(5, "plan_stale", "saved plan or manifest digest does not match")
    request = plan.get("planning_request", {})
    if (
        not isinstance(request, dict)
        or not isinstance(request.get("base"), str)
        or not isinstance(request.get("repositories"), list)
        or not all(isinstance(repository, str) for repository in request.get("repositories", []))
        or not isinstance(request.get("injected_paths"), list)
        or not all(isinstance(path, str) for path in request.get("injected_paths", []))
        or not isinstance(request.get("explicit_governing_inputs"), list)
        or not all(isinstance(path, str) for path in request.get("explicit_governing_inputs", []))
        or not isinstance(plan.get("lanes"), list)
        or not isinstance(plan.get("repositories"), dict)
        or not isinstance(plan.get("governing_inputs"), list)
        or not isinstance(plan.get("explicit_governing_inputs"), list)
        or not isinstance(plan.get("instruction_inputs"), list)
        or not all(isinstance(lane, dict) and isinstance(lane.get("id"), str) for lane in plan["lanes"])
    ):
        raise QualificationError(5, "plan_stale", "saved plan has an invalid structure")
    current = build_plan(
        manifest,
        manifest_digest,
        request.get("base", "HEAD"),
        request.get("repositories"),
        request.get("injected_paths"),
        None,
        workspace_root,
        request.get("explicit_governing_inputs"),
    )
    _validate_execution_snapshot(plan, current, manifest_digest)
    current_lanes = {lane["id"]: lane for lane in current["lanes"]}
    saved_lanes = {lane["id"]: lane for lane in plan["lanes"]}
    if bool(lane_ids) == bool(accept_selected):
        raise QualificationError(2, "execution_selection_required", "use either --lane or --accept-selected")
    if accept_selected:
        chosen = [
            lane["id"] for lane in plan["lanes"]
            if lane["disposition"] in ("selected", "invalidated") and not lane["subsumed_by"]
        ]
    else:
        chosen = lane_ids
    if not chosen:
        raise QualificationError(2, "execution_selection_required", "no lanes were explicitly accepted")
    lane_by_id = {lane["id"]: lane for lane in manifest["lanes"]}
    for lane_id in chosen:
        if lane_id not in saved_lanes or not saved_lanes[lane_id].get("selected") or saved_lanes[lane_id].get("subsumed_by"):
            raise QualificationError(5, "lane_not_executable", f"lane {lane_id} is unknown, unselected, or subsumed")
        if lane_by_id[lane_id]["repository"] == "go-oversync":
            raise QualificationError(5, "read_only_execution_refused", "Go-Oversync lanes are planning-only")
        if lane_by_id[lane_id]["classification"] == "product" and not authorize_product:
            raise QualificationError(2, "product_authorization_required", f"lane {lane_id} requires --authorize-product")
        unavailable = [
            toolchain["id"]
            for toolchain in current_lanes[lane_id].get("toolchains", [])
            if toolchain.get("exit_code") != 0
        ]
        if unavailable:
            raise QualificationError(5, "toolchain_unavailable", f"lane {lane_id} toolchains unavailable: {', '.join(unavailable)}")
    graph = {lane["id"]: lane["subsumes"] for lane in manifest["lanes"]}
    for aggregate_id in chosen:
        overlap = _transitive(graph, aggregate_id).intersection(chosen)
        if overlap:
            raise QualificationError(5, "redundant_execution", f"lane {aggregate_id} subsumes {sorted(overlap)}")

    roots = repository_roots(manifest, workspace_root)
    records = _records_directory(records)
    events_path = records / "attempts.jsonl"
    any_failure = False
    for ordinal, lane_id in enumerate(chosen, 1):
        manifest, manifest_digest, current = _refresh_execution_snapshot(
            manifest_path,
            plan,
            request,
            manifest_digest,
            workspace_root,
        )
        current_lanes = {lane["id"]: lane for lane in current["lanes"]}
        lane = lane_by_id[lane_id]
        saved = saved_lanes[lane_id]
        lane_cwd = (roots[lane["repository"]] / lane["cwd"]).resolve()
        if lane_cwd != roots[lane["repository"]] and roots[lane["repository"]] not in lane_cwd.parents:
            raise QualificationError(5, "cwd_escapes_repository", f"lane {lane_id} cwd escapes repository")
        attempt_id = f"{time.time_ns()}-{os.getpid()}-{ordinal:03d}-{lane_id}"
        log_name = f"{attempt_id}.log"
        log_path = records / log_name
        try:
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
            log_descriptor = os.open(log_path, flags, 0o644)
        except OSError as error:
            raise QualificationError(12, "record_log_create_failed", str(error)) from error
        environment = effective_lane_environment(lane)
        process_environment = {
            name: value for name, value in environment.items() if value is not None
        }
        epoch, utc = _now()
        toolchains = current_lanes[lane_id]["toolchains"]
        started = {
            "schema_version": ATTEMPT_RECORD_SCHEMA_VERSION,
            "event": "started",
            "attempt_id": attempt_id,
            "lane_id": lane_id,
            "plan_sha256": supplied_digest,
            "manifest_sha256": manifest_digest,
            "argv": lane["argv"],
            "cwd": lane["cwd"],
            "repository": lane["repository"],
            "environment": environment,
            "toolchains": toolchains,
            "relevant_input_sha256": saved["relevant_input_sha256"],
            "dependency_digests": {
                dependency: saved_lanes[dependency]["relevant_input_sha256"]
                for dependency in lane["depends_on"]
            },
            "repositories_before": current["repositories"],
            "timeout_seconds": _effective_timeout(manifest, lane),
            "start_epoch_seconds": epoch,
            "start_utc": utc,
            "log": log_name,
        }
        try:
            _append_event(events_path, started)
        except OSError as error:
            os.close(log_descriptor)
            raise QualificationError(12, "record_append_failed", str(error)) from error
        start_monotonic = time.monotonic()
        exit_code: int | None = None
        timed_out = False
        harness_error: str | None = None
        previous_handlers: dict[int, Any] = {}
        process: subprocess.Popen[bytes] | None = None
        try:
            def interrupt_handler(signum: int, frame: Any) -> None:
                del frame
                raise RunnerInterrupted(signum)

            for signum in (signal.SIGINT, signal.SIGTERM):
                previous_handlers[signum] = signal.getsignal(signum)
                signal.signal(signum, interrupt_handler)
            with os.fdopen(log_descriptor, "wb", closefd=True) as log_file:
                process = subprocess.Popen(
                    lane["argv"],
                    cwd=lane_cwd,
                    env=process_environment,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
                try:
                    exit_code = process.wait(timeout=_effective_timeout(manifest, lane))
                except subprocess.TimeoutExpired:
                    timed_out = True
                    exit_code = _terminate_process_group(process)
                except RunnerInterrupted as interruption:
                    harness_error = str(interruption)
                    exit_code = _terminate_process_group(process)
                    if exit_code is None:
                        exit_code = -interruption.signum
        except RunnerInterrupted as interruption:
            harness_error = str(interruption)
            if process is not None:
                exit_code = _terminate_process_group(process)
            if exit_code is None:
                exit_code = -interruption.signum
        except OSError as error:
            harness_error = f"{type(error).__name__}: {error}"
        finally:
            for signum, handler in previous_handlers.items():
                signal.signal(signum, handler)
        duration = time.monotonic() - start_monotonic
        end_epoch, end_utc = _now()
        after = current
        after_lane: dict[str, Any] = {"relevant_input_sha256": None}
        postflight_error: QualificationError | None = None
        try:
            manifest, manifest_digest, after = _refresh_execution_snapshot(
                manifest_path,
                plan,
                request,
                manifest_digest,
                workspace_root,
            )
            after_lane = next(item for item in after["lanes"] if item["id"] == lane_id)
        except QualificationError as error:
            postflight_error = error
            harness_error = harness_error or f"postflight {error.code}: {error}"
            after = {"repositories": _current_repositories(roots, current["repositories"])}
        changed_read_only = (
            after["repositories"]["go-oversync"] != current["repositories"]["go-oversync"]
        )
        if changed_read_only:
            harness_error = "read-only repository Git identity changed during execution"
        finished = {
            "schema_version": ATTEMPT_RECORD_SCHEMA_VERSION,
            "event": "finished",
            "attempt_id": attempt_id,
            "lane_id": lane_id,
            "end_epoch_seconds": end_epoch,
            "end_utc": end_utc,
            "duration_seconds": round(duration, 6),
            "exit_code": exit_code,
            "signal": -exit_code if exit_code is not None and exit_code < 0 else None,
            "timed_out": timed_out,
            "harness_error": harness_error,
            "log": log_name,
            "log_bytes": log_path.stat().st_size,
            "log_sha256": sha256_file(log_path),
            "relevant_input_sha256_after": after_lane["relevant_input_sha256"],
            "repositories_after": after["repositories"],
        }
        try:
            _append_event(events_path, finished)
        except OSError as error:
            raise QualificationError(12, "record_append_failed", str(error)) from error
        failed = (
            exit_code != 0
            or timed_out
            or harness_error is not None
            or after_lane["relevant_input_sha256"] != saved["relevant_input_sha256"]
        )
        any_failure = any_failure or failed
        current = after
        if postflight_error is not None:
            raise QualificationError(5, "execution_identity_changed", f"lane {lane_id}: {postflight_error}") from postflight_error
        if failed and not continue_on_failure:
            return 10
    return 11 if any_failure else 0


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    subparsers = result.add_subparsers(dest="command", required=True)
    validate = subparsers.add_parser("validate-manifest")
    validate.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    plan = subparsers.add_parser("plan")
    plan.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    plan.add_argument("--base", required=True)
    plan.add_argument("--repo", action="append")
    plan.add_argument("--changed-path", action="append")
    plan.add_argument("--governing-input", action="append")
    plan.add_argument("--records", type=Path)
    plan.add_argument("--output", default="-")
    run = subparsers.add_parser("run")
    run.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    run.add_argument("--plan", type=Path, required=True)
    run.add_argument("--lane", action="append", default=[])
    run.add_argument("--accept-selected", action="store_true")
    run.add_argument("--records", type=Path, required=True)
    run.add_argument("--continue-on-failure", action="store_true")
    run.add_argument("--authorize-product", action="store_true")
    return result


def main(arguments: list[str] | None = None) -> int:
    values = list(sys.argv[1:] if arguments is None else arguments)
    if not values or values[0] not in ("plan", "run", "validate-manifest", "-h", "--help"):
        values.insert(0, "plan")
    try:
        options = parser().parse_args(values)
        if options.command == "validate-manifest":
            manifest, digest = load_manifest(options.manifest)
            print(f"manifest valid: {len(manifest['lanes'])} lanes, sha256={digest}")
            return 0
        if options.command == "plan":
            manifest, digest = load_manifest(options.manifest)
            plan = build_plan(
                manifest,
                digest,
                options.base,
                options.repo,
                options.changed_path,
                options.records,
                explicit_governing_input_selections=options.governing_input,
            )
            write_plan(plan, options.output)
            return 0
        return run_plan(
            options.plan,
            options.manifest,
            options.lane,
            options.accept_selected,
            options.records,
            options.continue_on_failure,
            authorize_product=options.authorize_product,
        )
    except QualificationError as error:
        print(f"QUALIFICATION-{error.code}: {error}", file=sys.stderr)
        return error.exit_code


if __name__ == "__main__":
    raise SystemExit(main())

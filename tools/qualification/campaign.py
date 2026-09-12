#!/usr/bin/env python3
"""Local qualification campaign orchestrator.

This is deliberately a local, standard-library scheduler.  It consumes a saved
plan-version-3 plan and records orchestration observations; it does not accept
product evidence or qualify a candidate.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid
from typing import Any

try:  # Supports both ``python campaign.py`` and package imports in tests.
    from tools.qualification import planner
except ModuleNotFoundError:  # pragma: no cover - direct-script import path
    import planner  # type: ignore[no-redef]


DEFAULT_MANIFEST = Path(__file__).with_name("lanes.json")
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
CAMPAIGN_SCHEMA_VERSION = 2
CAMPAIGN_EVENTS = {
    "campaign_started", "lane_queued", "lane_acquired", "lane_started", "lane_finished",
    "lane_blocked", "lane_cancelled", "lane_cleanup", "resume_retained", "campaign_finished",
}


def _campaign_directory(records: Path, create: bool = False) -> Path:
    if create:
        root = planner._records_directory(records)
    else:
        planner._ensure_no_symlink_ancestors(records, 5, "campaign records path")
        if records.is_symlink() or not records.is_dir():
            raise planner.QualificationError(5, "campaign_unreadable", "campaign records directory is unavailable")
        root = records.resolve()
    directory = root / "campaigns"
    planner._ensure_no_symlink_ancestors(directory, 12 if create else 5, "campaign records path")
    if directory.is_symlink():
        raise planner.QualificationError(12 if create else 5, "campaign_records_path_invalid", "campaign records directory is a symlink")
    if create:
        try:
            directory.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            raise planner.QualificationError(12, "campaign_records_path_invalid", str(error)) from error
    if not directory.is_dir():
        raise planner.QualificationError(12 if create else 5, "campaign_records_path_invalid", "campaign records path is not a directory")
    return directory


def _campaign_path(records: Path, campaign_id: str, create: bool = False) -> Path:
    if not campaign_id or any(character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for character in campaign_id):
        raise planner.QualificationError(2, "campaign_id_invalid", "campaign ID contains unsafe characters")
    return _campaign_directory(records, create=create) / f"{campaign_id}.jsonl"


def _append(path: Path, event: dict[str, Any]) -> None:
    if event.get("event") not in CAMPAIGN_EVENTS:
        raise planner.QualificationError(12, "campaign_record_invalid", "unknown campaign event")
    event = {"schema_version": CAMPAIGN_SCHEMA_VERSION, **event}
    try:
        planner._append_event(path, event)
    except OSError as error:
        raise planner.QualificationError(12, "campaign_record_append_failed", str(error)) from error


def _load_plan(path: Path, manifest_digest: str) -> dict[str, Any]:
    try:
        plan = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise planner.QualificationError(5, "plan_unreadable", f"cannot read saved plan: {error}") from error
    if not isinstance(plan, dict) or plan.get("schema_version") != planner.PLAN_SCHEMA_VERSION:
        raise planner.QualificationError(5, "plan_stale", "saved plan is not a plan-version-3 object")
    supplied = plan.get("plan_sha256")
    unsigned = dict(plan)
    unsigned.pop("plan_sha256", None)
    if supplied != planner.sha256_bytes(planner.canonical_bytes(unsigned)):
        raise planner.QualificationError(5, "plan_stale", "saved plan digest does not match")
    if plan.get("manifest_sha256") != manifest_digest:
        raise planner.QualificationError(5, "manifest_stale", "saved plan manifest digest does not match")
    if not isinstance(plan.get("lanes"), list) or not isinstance(plan.get("planning_request"), dict):
        raise planner.QualificationError(5, "plan_stale", "saved plan lacks lanes or planning request")
    return plan


def _selected(plan: dict[str, Any], manifest: dict[str, Any], lane_ids: list[str], accept_selected: bool) -> list[str]:
    if bool(lane_ids) == bool(accept_selected):
        raise planner.QualificationError(2, "campaign_selection_required", "use either --lane or --accept-selected")
    saved = {lane["id"]: lane for lane in plan["lanes"] if isinstance(lane, dict) and isinstance(lane.get("id"), str)}
    order = [lane["id"] for lane in manifest["lanes"]]
    chosen = [
        lane_id for lane_id in order
        if saved.get(lane_id, {}).get("disposition") in ("selected", "invalidated")
        and not saved[lane_id].get("subsumed_by")
    ] if accept_selected else lane_ids
    if not chosen or len(set(chosen)) != len(chosen):
        raise planner.QualificationError(2, "campaign_selection_required", "selected lanes must be non-empty and unique")
    manifest_lanes = {lane["id"]: lane for lane in manifest["lanes"]}
    for lane_id in chosen:
        lane = manifest_lanes.get(lane_id)
        if not lane or not saved.get(lane_id, {}).get("selected") or saved[lane_id].get("subsumed_by"):
            raise planner.QualificationError(5, "lane_not_executable", f"lane {lane_id} is unknown, unselected, or subsumed")
        if lane["repository"] == "go-oversync":
            raise planner.QualificationError(5, "read_only_execution_refused", "Go-Oversync lanes are planning-only")
        if not lane.get("campaign_eligible", False):
            raise planner.QualificationError(5, "campaign_ineligible", f"lane {lane_id} is not campaign eligible")
        missing = [dependency for dependency in lane["depends_on"] if dependency not in chosen]
        if missing:
            raise planner.QualificationError(5, "dependency_not_selected", f"lane {lane_id} requires selected dependencies: {', '.join(missing)}")
    graph = {lane["id"]: lane["subsumes"] for lane in manifest["lanes"]}
    for lane_id in chosen:
        overlap = planner._transitive(graph, lane_id).intersection(chosen)
        if overlap:
            raise planner.QualificationError(5, "redundant_execution", f"lane {lane_id} subsumes {sorted(overlap)}")
    return chosen


def _preflight(manifest_path: Path, plan: dict[str, Any], expected_manifest: str, workspace_root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    request = plan["planning_request"]
    manifest, digest, current = planner._refresh_execution_snapshot(manifest_path, plan, request, expected_manifest, workspace_root)
    if digest != expected_manifest:
        raise planner.QualificationError(5, "manifest_stale", "campaign manifest changed")
    return manifest, current


def _topological(manifest: dict[str, Any], chosen: list[str]) -> list[str]:
    lane_map = {lane["id"]: lane for lane in manifest["lanes"]}
    pending = set(chosen)
    output: list[str] = []
    while pending:
        ready = [lane_id for lane_id in chosen if lane_id in pending and all(dependency in output for dependency in lane_map[lane_id]["depends_on"])]
        if not ready:
            raise planner.QualificationError(3, "campaign_dependency_cycle", "selected lanes cannot be topologically scheduled")
        output.extend(ready)
        pending.difference_update(ready)
    return output


def _event_time() -> tuple[float, str]:
    return planner._now()


def _attempt_started(
    records: Path,
    lane: dict[str, Any],
    saved_lane: dict[str, Any],
    saved_lanes: dict[str, dict[str, Any]],
    current: dict[str, Any],
    plan_sha256: str,
    manifest_digest: str,
    timeout_seconds: int,
    ordinal: int,
) -> tuple[str, Path, float]:
    attempt_id = f"{time.time_ns()}-{os.getpid()}-{ordinal:03d}-{lane['id']}"
    log_name = f"{attempt_id}.log"
    log_path = records / log_name
    descriptor = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o644)
    os.close(descriptor)
    epoch, utc = _event_time()
    environment = planner.effective_lane_environment(lane)
    started = {
        "schema_version": planner.ATTEMPT_RECORD_SCHEMA_VERSION, "event": "started", "attempt_id": attempt_id,
        "lane_id": lane["id"], "plan_sha256": plan_sha256, "manifest_sha256": manifest_digest,
        "argv": lane["argv"], "cwd": lane["cwd"], "repository": lane["repository"], "environment": environment,
        "toolchains": next(item for item in current["lanes"] if item["id"] == lane["id"])["toolchains"],
        "relevant_input_sha256": saved_lane["relevant_input_sha256"],
        "dependency_digests": {
            dependency: saved_lanes[dependency]["relevant_input_sha256"]
            for dependency in lane["depends_on"]
        },
        "repositories_before": current["repositories"], "timeout_seconds": timeout_seconds,
        "start_epoch_seconds": epoch, "start_utc": utc, "log": log_name,
    }
    # The attempt-record schema deliberately owns this stream; campaign linkage lives in its own JSONL.
    try:
        planner._append_event(records / "attempts.jsonl", started)
    except OSError as error:
        raise planner.QualificationError(12, "record_append_failed", str(error)) from error
    return attempt_id, log_path, time.monotonic()


def _finish_attempt(
    records: Path,
    attempt_id: str,
    lane: dict[str, Any],
    log_path: Path,
    started_at: float,
    exit_code: int | None,
    timed_out: bool,
    harness_error: str | None,
    relevant_input_after: str | None,
    repositories_after: dict[str, Any],
) -> dict[str, Any]:
    epoch, utc = _event_time()
    finished = {
        "schema_version": planner.ATTEMPT_RECORD_SCHEMA_VERSION, "event": "finished", "attempt_id": attempt_id, "lane_id": lane["id"],
        "end_epoch_seconds": epoch, "end_utc": utc, "duration_seconds": round(time.monotonic() - started_at, 6),
        "exit_code": exit_code, "signal": -exit_code if exit_code is not None and exit_code < 0 else None,
        "timed_out": timed_out, "harness_error": harness_error, "log": log_path.name,
        "log_bytes": log_path.stat().st_size, "log_sha256": planner.sha256_file(log_path),
        "relevant_input_sha256_after": relevant_input_after, "repositories_after": repositories_after,
    }
    try:
        planner._append_event(records / "attempts.jsonl", finished)
    except OSError as error:
        raise planner.QualificationError(12, "record_append_failed", str(error)) from error
    return finished


def _kill(process: subprocess.Popen[bytes]) -> tuple[int | None, bool]:
    try:
        return planner._terminate_process_group(process), False
    except (OSError, subprocess.SubprocessError):
        return process.poll(), True


def _start_process(argv: list[str], cwd: Path, environment: dict[str, str], log: Any) -> subprocess.Popen[bytes]:
    """Single auditable launch seam used by focused containment tests."""
    return subprocess.Popen(
        argv, cwd=cwd, env=environment, stdout=log,
        stderr=subprocess.STDOUT, start_new_session=True,
    )


def _read_events(path: Path) -> list[dict[str, Any]]:
    try:
        raw = path.read_bytes()
    except OSError as error:
        raise planner.QualificationError(5, "campaign_unreadable", str(error)) from error
    if not raw or not raw.endswith(b"\n"):
        raise planner.QualificationError(5, "campaign_corrupt", "campaign record is empty or lacks its final newline")
    lines = raw.split(b"\n")[:-1]
    events: list[dict[str, Any]] = []
    campaign_id: str | None = None
    selected: set[str] = set()
    stages: dict[str, str] = {}
    for number, line in enumerate(lines, 1):
        try:
            event = planner._strict_record_json(line)
        except ValueError as error:
            raise planner.QualificationError(5, "campaign_corrupt", str(error)) from error
        if not isinstance(event, dict) or event.get("schema_version") != CAMPAIGN_SCHEMA_VERSION or event.get("event") not in CAMPAIGN_EVENTS:
            raise planner.QualificationError(5, "campaign_corrupt", f"campaign record line {number} has an invalid event")
        if number == 1:
            campaign_id = event.get("campaign_id")
            lane_ids = event.get("selected_lane_ids")
            if event.get("event") != "campaign_started" or not isinstance(campaign_id, str) or not campaign_id:
                raise planner.QualificationError(5, "campaign_corrupt", "campaign record lacks a valid campaign_started event")
            if not isinstance(lane_ids, list) or not lane_ids or not all(isinstance(item, str) and item for item in lane_ids) or len(set(lane_ids)) != len(lane_ids):
                raise planner.QualificationError(5, "campaign_corrupt", "campaign selected lanes are invalid")
            resource_values = event.get("resources")
            if not isinstance(event.get("plan"), dict) or not isinstance(resource_values, dict) or any(
                not isinstance(name, str) or not name or isinstance(capacity, bool)
                or not isinstance(capacity, int) or capacity <= 0
                for name, capacity in resource_values.items()
            ):
                raise planner.QualificationError(5, "campaign_corrupt", "campaign start has invalid retained inputs")
            selected = set(lane_ids)
            stages = {lane_id: "pending" for lane_id in lane_ids}
        elif event.get("campaign_id") != campaign_id or event.get("event") == "campaign_started":
            raise planner.QualificationError(5, "campaign_corrupt", f"campaign record line {number} has an invalid campaign identity")
        lane_id = event.get("lane_id")
        if lane_id is not None and lane_id not in selected:
            raise planner.QualificationError(5, "campaign_corrupt", f"campaign record line {number} names an unselected lane")
        kind = event["event"]
        numeric_fields = {
            "campaign_started": ("start_epoch_seconds",),
            "lane_queued": ("queue_epoch_seconds",),
            "lane_acquired": ("acquired_epoch_seconds", "lock_wait_seconds"),
            "lane_started": ("start_epoch_seconds", "queue_seconds", "lock_wait_seconds"),
            "lane_finished": ("duration_seconds",),
            "lane_cleanup": ("cleanup_duration_seconds", "resource_hold_seconds", "released_epoch_seconds"),
            "campaign_finished": ("duration_seconds", "end_epoch_seconds"),
        }.get(kind, ())
        if any(field in event and not planner._is_finite_number(event[field]) for field in numeric_fields):
            raise planner.QualificationError(5, "campaign_corrupt", f"campaign record line {number} has an invalid measurement")
        if kind == "lane_queued":
            if stages[lane_id] != "pending":
                raise planner.QualificationError(5, "campaign_corrupt", f"lane {lane_id} was queued out of order")
            stages[lane_id] = "queued"
        elif kind == "lane_acquired":
            if stages[lane_id] != "queued":
                raise planner.QualificationError(5, "campaign_corrupt", f"lane {lane_id} acquired resources out of order")
            stages[lane_id] = "acquired"
        elif kind == "lane_started":
            if stages[lane_id] != "acquired" or not isinstance(event.get("attempt_id"), str):
                raise planner.QualificationError(5, "campaign_corrupt", f"lane {lane_id} started out of order")
            stages[lane_id] = "running"
        elif kind == "lane_finished":
            if stages[lane_id] != "running" or not isinstance(event.get("attempt_id"), str) or event.get("outcome") not in ("passed", "failed", "timed_out", "cancelled", "cleanup_failed"):
                raise planner.QualificationError(5, "campaign_corrupt", f"lane {lane_id} finished out of order")
            stages[lane_id] = str(event.get("outcome"))
        elif kind == "lane_cleanup":
            if stages[lane_id] not in ("passed", "failed", "timed_out", "cancelled", "cleanup_failed") or event.get("outcome") not in ("ok", "failed"):
                raise planner.QualificationError(5, "campaign_corrupt", f"lane {lane_id} cleanup is out of order")
            stages[lane_id] = f"cleaned:{stages[lane_id]}"
        elif kind == "lane_blocked":
            if stages[lane_id] != "pending":
                raise planner.QualificationError(5, "campaign_corrupt", f"lane {lane_id} was blocked out of order")
            stages[lane_id] = "blocked_dependency"
        elif kind == "lane_cancelled":
            if stages[lane_id] not in ("pending", "queued", "acquired"):
                raise planner.QualificationError(5, "campaign_corrupt", f"lane {lane_id} was cancelled out of order")
            stages[lane_id] = "cancelled"
        elif kind == "resume_retained":
            if not stages[lane_id].startswith("cleaned:passed"):
                raise planner.QualificationError(5, "campaign_corrupt", f"lane {lane_id} cannot be retained")
        elif kind == "campaign_finished":
            states = event.get("states")
            if not isinstance(states, dict) or set(states) != selected or not all(isinstance(value, str) for value in states.values()):
                raise planner.QualificationError(5, "campaign_corrupt", f"campaign record line {number} has invalid final states")
        events.append(event)
    if not events:
        raise planner.QualificationError(5, "campaign_corrupt", "campaign record is empty")
    return events


def _retained_attempts(
    events: list[dict[str, Any]],
    records: Path,
    plan: dict[str, Any],
    manifest: dict[str, Any],
    current: dict[str, Any],
    chosen: list[str],
) -> tuple[dict[str, str], set[str]]:
    attempts, record_error = planner._read_attempt_pairs(records, set(current["repositories"]), allow_incomplete=True)
    if record_error:
        raise planner.QualificationError(5, "campaign_attempts_corrupt", record_error["reason"])
    lane_map = {lane["id"]: lane for lane in manifest["lanes"]}
    saved_lanes = {lane["id"]: lane for lane in plan["lanes"]}
    current_lanes = {lane["id"]: lane for lane in current["lanes"]}
    passed: dict[str, dict[str, Any]] = {}
    cleanup: dict[str, str] = {}
    unresolved: set[str] = set()
    touched: set[str] = set()
    terminal: set[str] = set()
    for event in events:
        lane_id = event.get("lane_id")
        if lane_id not in chosen:
            continue
        if event["event"] in ("lane_queued", "lane_acquired", "lane_started"):
            touched.add(lane_id)
        if event["event"] == "lane_finished":
            terminal.add(lane_id)
            if event.get("outcome") == "passed":
                passed[lane_id] = event
            else:
                unresolved.add(lane_id)
        elif event["event"] == "lane_cleanup":
            cleanup[lane_id] = str(event.get("outcome"))
        elif event["event"] in ("lane_cancelled", "lane_blocked"):
            terminal.add(lane_id)
            unresolved.add(lane_id)
    unresolved.update(touched - terminal)
    retained: dict[str, str] = {}
    for lane_id, lane_event in passed.items():
        if lane_id in unresolved or cleanup.get(lane_id) != "ok":
            unresolved.add(lane_id)
            continue
        attempt_id = lane_event.get("attempt_id")
        pair = attempts.get(attempt_id, {})
        started, finished = pair.get("started"), pair.get("finished")
        if not started or not finished:
            raise planner.QualificationError(5, "campaign_attempts_corrupt", f"passed lane {lane_id} lacks a complete attempt")
        pair_error = planner._validate_record_pair(started, finished)
        if pair_error:
            raise planner.QualificationError(5, "campaign_attempts_corrupt", f"attempt {attempt_id}: {pair_error}")
        lane = lane_map[lane_id]
        saved = saved_lanes[lane_id]
        now = current_lanes[lane_id]
        expected_dependencies = {
            dependency: saved_lanes[dependency]["relevant_input_sha256"]
            for dependency in lane["depends_on"]
        }
        applicable = (
            started["plan_sha256"] == plan["plan_sha256"]
            and started["manifest_sha256"] == plan["manifest_sha256"]
            and started["lane_id"] == lane_id
            and started["argv"] == lane["argv"]
            and started["cwd"] == lane["cwd"]
            and started["repository"] == lane["repository"]
            and started["environment"] == planner.effective_lane_environment(lane)
            and started["toolchains"] == now["toolchains"]
            and started["relevant_input_sha256"] == saved["relevant_input_sha256"]
            and started["dependency_digests"] == expected_dependencies
            and started["repositories_before"] == finished["repositories_after"]
            and planner._stable_repository_anchors_match(started["repositories_before"], current["repositories"])
            and finished["exit_code"] == 0
            and finished["signal"] is None
            and not finished["timed_out"]
            and finished["harness_error"] is None
            and finished["relevant_input_sha256_after"] == saved["relevant_input_sha256"]
            and lane_event.get("log_sha256") == finished["log_sha256"]
        )
        if not applicable:
            raise planner.QualificationError(5, "campaign_attempt_stale", f"passed attempt {attempt_id} is no longer applicable")
        log = planner._record_log_path(records, finished["log"])
        if not log.is_file() or log.is_symlink() or log.stat().st_size != finished["log_bytes"] or planner.sha256_file(log) != finished["log_sha256"]:
            raise planner.QualificationError(5, "campaign_log_invalid", f"attempt {attempt_id} log is missing or changed")
        retained[lane_id] = attempt_id
    return retained, unresolved


def _run(plan: dict[str, Any], manifest_path: Path, manifest_digest: str, records: Path, chosen: list[str], campaign_id: str, max_workers: int | None, continue_on_failure: bool, authorize_product: bool, workspace_root: Path, resume: bool) -> int:
    records = planner._records_directory(records)
    manifest, current = _preflight(manifest_path, plan, manifest_digest, workspace_root)
    lane_map = {lane["id"]: lane for lane in manifest["lanes"]}
    saved_lanes = {lane["id"]: lane for lane in plan["lanes"]}
    if any(lane_map[lane_id]["classification"] == "product" for lane_id in chosen) and not authorize_product:
        raise planner.QualificationError(2, "product_authorization_required", "selected product lanes require --authorize-product")
    resources = manifest["resources"]
    maximum = manifest.get("campaign_max_workers", 1)
    workers = maximum if max_workers is None else max_workers
    if not isinstance(workers, int) or workers <= 0 or workers > maximum:
        raise planner.QualificationError(2, "worker_limit_invalid", f"max workers must be 1..{maximum}")
    campaign_path = _campaign_path(records, campaign_id, create=True)
    order = _topological(manifest, chosen)
    retained: dict[str, str] = {}
    unresolved: set[str] = set()
    if resume:
        events = _read_events(campaign_path)
        retained, unresolved = _retained_attempts(events, records, plan, manifest, current, chosen)
        for lane_id in order:
            if lane_id in retained:
                _append(campaign_path, {"event": "resume_retained", "campaign_id": campaign_id, "lane_id": lane_id, "attempt_id": retained[lane_id]})
    else:
        epoch, utc = _event_time()
        _append(campaign_path, {
            "event": "campaign_started", "campaign_id": campaign_id, "plan_sha256": plan["plan_sha256"],
            "plan": plan, "manifest_sha256": manifest_digest, "selected_lane_ids": chosen,
            "continue_on_failure": continue_on_failure, "authorize_product": authorize_product,
            "max_workers": workers, "resources": resources, "repositories_before": current["repositories"],
            "start_epoch_seconds": epoch, "start_utc": utc,
        })
    state = {lane_id: "retained" if lane_id in retained else "unresolved" if lane_id in unresolved else "pending" for lane_id in chosen}
    held = {name: 0 for name in resources}
    running: dict[str, dict[str, Any]] = {}
    queue_started: dict[str, float] = {}
    stop = False
    abort_running = False
    interrupted_signal: int | None = None
    cleanup_failed = False
    orchestrator_error: str | None = None
    primary_error: BaseException | None = None
    campaign_started_at = time.monotonic()
    old_handlers: dict[int, Any] = {}
    ordinal = 0
    roots = planner.repository_roots(manifest, workspace_root)

    def interrupt(signum: int, frame: Any) -> None:
        del frame
        nonlocal interrupted_signal, stop, abort_running, orchestrator_error
        interrupted_signal = signum
        stop = True
        abort_running = True
        orchestrator_error = f"signal {signum}"

    def finish_lane(lane_id: str, run: dict[str, Any], *, forced: bool, timed_out: bool, harness_error: str | None) -> tuple[str, str | None]:
        nonlocal cleanup_failed, current, manifest
        cleanup_started = time.monotonic()
        process: subprocess.Popen[bytes] | None = run.get("process")
        cleanup_error = False
        exit_code: int | None = process.poll() if process is not None else None
        if process is not None and exit_code is None and (forced or timed_out):
            exit_code, cleanup_error = _kill(process)
        if process is not None and exit_code is None:
            raise RuntimeError(f"attempted to finish running lane {lane_id}")
        after = current
        after_relevant: str | None = None
        postflight_error: str | None = None
        try:
            manifest, current = _preflight(manifest_path, plan, manifest_digest, workspace_root)
            after = current
            after_relevant = next(item for item in current["lanes"] if item["id"] == lane_id)["relevant_input_sha256"]
        except planner.QualificationError as error:
            postflight_error = f"postflight {error.code}: {error}"
            harness_error = harness_error or postflight_error
            after = {"repositories": planner._current_repositories(roots, current["repositories"])}
        if after["repositories"].get("go-oversync") != run["repositories_before"].get("go-oversync"):
            harness_error = "read-only repository Git identity changed during execution"
            postflight_error = postflight_error or harness_error
        finished = _finish_attempt(
            records, run["attempt_id"], lane_map[lane_id], run["log_path"], run["started_monotonic"],
            exit_code, timed_out, harness_error, after_relevant, after["repositories"],
        )
        if cleanup_error:
            outcome = "cleanup_failed"
        elif timed_out:
            outcome = "timed_out"
        elif forced:
            outcome = "cancelled"
        elif exit_code == 0 and harness_error is None and after_relevant == saved_lanes[lane_id]["relevant_input_sha256"]:
            outcome = "passed"
        else:
            outcome = "failed"
        for resource in run["resources"]:
            held[resource] -= 1
        running.pop(lane_id, None)
        state[lane_id] = outcome
        cleanup_seconds = round(time.monotonic() - cleanup_started, 6)
        hold_seconds = round(time.monotonic() - run["acquired_monotonic"], 6)
        _append(campaign_path, {
            "event": "lane_finished", "campaign_id": campaign_id, "lane_id": lane_id,
            "attempt_id": run["attempt_id"], "outcome": outcome, "exit_code": exit_code,
            "timed_out": timed_out, "duration_seconds": finished["duration_seconds"],
            "log_sha256": finished["log_sha256"],
            "failure_kind": "orchestrator" if harness_error else "lane" if outcome == "failed" else None,
        })
        epoch, utc = _event_time()
        _append(campaign_path, {
            "event": "lane_cleanup", "campaign_id": campaign_id, "lane_id": lane_id,
            "outcome": "failed" if cleanup_error else "ok", "cleanup_duration_seconds": cleanup_seconds,
            "resource_hold_seconds": hold_seconds, "released_epoch_seconds": epoch, "released_utc": utc,
            "resources": run["resources"],
        })
        cleanup_failed = cleanup_failed or cleanup_error
        return outcome, postflight_error

    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            old_handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, interrupt)
        while True:
            for lane_id, run in list(running.items()):
                process: subprocess.Popen[bytes] = run["process"]
                timeout = planner._effective_timeout(manifest, lane_map[lane_id])
                timed_out = time.monotonic() - run["started_monotonic"] >= timeout
                if process.poll() is None and not timed_out and not abort_running:
                    continue
                outcome, postflight_error = finish_lane(
                    lane_id, run, forced=abort_running and not timed_out, timed_out=timed_out,
                    harness_error="campaign interrupted" if abort_running and not timed_out else None,
                )
                if postflight_error:
                    stop = True
                    abort_running = True
                    orchestrator_error = postflight_error
                elif outcome in ("failed", "timed_out", "cleanup_failed") and not continue_on_failure:
                    stop = True
            for lane_id in order:
                if state[lane_id] != "pending":
                    continue
                dependencies = lane_map[lane_id]["depends_on"]
                if any(state[dependency] in ("failed", "timed_out", "cleanup_failed", "cancelled", "unresolved", "blocked_dependency") for dependency in dependencies):
                    state[lane_id] = "blocked_dependency"
                    _append(campaign_path, {"event": "lane_blocked", "campaign_id": campaign_id, "lane_id": lane_id, "reason": "blocked_dependency"})
                elif not stop and all(state[dependency] in ("passed", "retained") for dependency in dependencies):
                    epoch, utc = _event_time()
                    state[lane_id] = "queued"
                    queue_started[lane_id] = time.monotonic()
                    _append(campaign_path, {
                        "event": "lane_queued", "campaign_id": campaign_id, "lane_id": lane_id,
                        "queue_epoch_seconds": epoch, "queue_utc": utc,
                        "resources": sorted(lane_map[lane_id]["resource_locks"]),
                    })
            if not stop:
                for lane_id in order:
                    if len(running) >= workers or state[lane_id] != "queued":
                        continue
                    measurement_running = any(lane_map[active]["memory_class"] == "measurement-exclusive" for active in running)
                    if lane_map[lane_id]["memory_class"] == "measurement-exclusive" and any(lane_map[active]["classification"] == "product" for active in running):
                        continue
                    if measurement_running and lane_map[lane_id]["classification"] == "product":
                        continue
                    lock_names = sorted(lane_map[lane_id]["resource_locks"])
                    if any(held[name] >= resources[name] for name in lock_names):
                        continue
                    # Identity capture is serialized by this single scheduler thread,
                    # but is not held for the duration of independent child commands.
                    manifest, current = _preflight(manifest_path, plan, manifest_digest, workspace_root)
                    lane = lane_map[lane_id]
                    root = roots[lane["repository"]]
                    cwd = (root / lane["cwd"]).resolve()
                    if cwd != root and root not in cwd.parents:
                        raise planner.QualificationError(5, "cwd_escapes_repository", f"lane {lane_id} cwd escapes repository")
                    acquired_at = time.monotonic()
                    for name in lock_names:
                        held[name] += 1
                    epoch, utc = _event_time()
                    lock_wait = round(acquired_at - queue_started[lane_id], 6)
                    _append(campaign_path, {
                        "event": "lane_acquired", "campaign_id": campaign_id, "lane_id": lane_id,
                        "acquired_epoch_seconds": epoch, "acquired_utc": utc,
                        "lock_wait_seconds": lock_wait, "resources": lock_names,
                    })
                    ordinal += 1
                    try:
                        attempt_id, log_path, started_at = _attempt_started(
                            records, lane, saved_lanes[lane_id], saved_lanes, current,
                            plan["plan_sha256"], manifest_digest, planner._effective_timeout(manifest, lane), ordinal,
                        )
                        run = {
                            "process": None, "attempt_id": attempt_id, "log_path": log_path,
                            "started_monotonic": started_at, "acquired_monotonic": acquired_at,
                            "resources": lock_names, "repositories_before": current["repositories"],
                        }
                        running[lane_id] = run
                        state[lane_id] = "running"
                        started_epoch, started_utc = _event_time()
                        _append(campaign_path, {
                            "event": "lane_started", "campaign_id": campaign_id, "lane_id": lane_id,
                            "attempt_id": attempt_id, "start_epoch_seconds": started_epoch,
                            "start_utc": started_utc,
                            "queue_seconds": round(time.monotonic() - queue_started[lane_id], 6),
                            "lock_wait_seconds": lock_wait,
                        })
                        environment = {name: value for name, value in planner.effective_lane_environment(lane).items() if value is not None}
                        with log_path.open("ab") as log:
                            run["process"] = _start_process(lane["argv"], cwd, environment, log)
                    except (OSError, subprocess.SubprocessError) as error:
                        orchestrator_error = f"launch {lane_id}: {type(error).__name__}: {error}"
                        if lane_id in running:
                            finish_lane(lane_id, running[lane_id], forced=False, timed_out=False, harness_error=orchestrator_error)
                        else:
                            for name in lock_names:
                                held[name] -= 1
                        stop = True
                        abort_running = True
                        break
            if not running and all(value not in ("pending", "queued") for value in state.values()):
                break
            if not running and stop:
                break
            time.sleep(0.02)
    except BaseException as error:
        primary_error = error
        stop = True
        abort_running = True
        orchestrator_error = orchestrator_error or f"{type(error).__name__}: {error}"
    finally:
        for lane_id, run in list(running.items()):
            try:
                finish_lane(lane_id, run, forced=True, timed_out=False, harness_error=orchestrator_error or "campaign aborted")
            except BaseException as cleanup_error:
                cleanup_failed = True
                if primary_error is None:
                    primary_error = cleanup_error
        for lane_id in order:
            if state[lane_id] in ("pending", "queued"):
                state[lane_id] = "cancelled"
                try:
                    _append(campaign_path, {"event": "lane_cancelled", "campaign_id": campaign_id, "lane_id": lane_id, "outcome": "cancelled"})
                except BaseException as append_error:
                    if primary_error is None:
                        primary_error = append_error
        failed = any(value in ("failed", "timed_out", "cleanup_failed", "cancelled", "unresolved", "blocked_dependency") for value in state.values()) or cleanup_failed or interrupted_signal is not None or orchestrator_error is not None
        epoch, utc = _event_time()
        try:
            _append(campaign_path, {
                "event": "campaign_finished", "campaign_id": campaign_id,
                "outcome": "unsuccessful" if failed else "passed",
                "states": {lane_id: state[lane_id] for lane_id in order},
                "duration_seconds": round(time.monotonic() - campaign_started_at, 6),
                "end_epoch_seconds": epoch, "end_utc": utc, "cleanup_failed": cleanup_failed,
                "orchestrator_error": orchestrator_error,
                "repositories_after": planner._current_repositories(roots, current["repositories"]),
            })
        except BaseException as append_error:
            if primary_error is None:
                primary_error = append_error
        for signum, handler in old_handlers.items():
            signal.signal(signum, handler)
    if primary_error is not None:
        raise primary_error
    return 11 if failed else 0


def run_campaign(plan_path: Path, manifest_path: Path, records: Path, lane_ids: list[str], accept_selected: bool, continue_on_failure: bool = False, max_workers: int | None = None, authorize_product: bool = False, workspace_root: Path = WORKSPACE_ROOT) -> tuple[str, int]:
    manifest, digest = planner.load_manifest(manifest_path)
    plan = _load_plan(plan_path, digest)
    chosen = _selected(plan, manifest, lane_ids, accept_selected)
    campaign_id = f"campaign-{uuid.uuid4().hex}"
    return campaign_id, _run(plan, manifest_path, digest, records, chosen, campaign_id, max_workers, continue_on_failure, authorize_product, workspace_root, False)


def resume_campaign(campaign_id: str, manifest_path: Path, records: Path, continue_on_failure: bool = False, max_workers: int | None = None, authorize_product: bool = False, workspace_root: Path = WORKSPACE_ROOT) -> int:
    path = _campaign_path(records, campaign_id)
    events = _read_events(path)
    start = events[0]
    plan = start.get("plan")
    if not isinstance(plan, dict) or start.get("campaign_id") != campaign_id:
        raise planner.QualificationError(5, "campaign_corrupt", "campaign start does not retain the exact plan")
    unsigned = dict(plan)
    supplied = unsigned.pop("plan_sha256", None)
    if supplied != planner.sha256_bytes(planner.canonical_bytes(unsigned)):
        raise planner.QualificationError(5, "campaign_corrupt", "retained plan digest does not match its content")
    manifest, digest = planner.load_manifest(manifest_path)
    if start.get("manifest_sha256") != digest or start.get("plan_sha256") != plan.get("plan_sha256"):
        raise planner.QualificationError(5, "campaign_stale", "campaign plan or manifest changed")
    chosen = start.get("selected_lane_ids")
    if not isinstance(chosen, list) or not all(isinstance(lane_id, str) for lane_id in chosen):
        raise planner.QualificationError(5, "campaign_corrupt", "campaign selected lanes are invalid")
    _selected(plan, manifest, chosen, False)
    return _run(plan, manifest_path, digest, records, chosen, campaign_id, max_workers, continue_on_failure, authorize_product, workspace_root, True)


def campaign_report(campaign_id: str, records: Path) -> dict[str, Any]:
    events = _read_events(_campaign_path(records, campaign_id))
    start = events[0]
    if start.get("campaign_id") != campaign_id:
        raise planner.QualificationError(5, "campaign_corrupt", "campaign path and record identity differ")
    plan = start.get("plan")
    if not isinstance(plan, dict):
        raise planner.QualificationError(5, "campaign_corrupt", "campaign start lacks its retained plan")
    unsigned = dict(plan)
    supplied = unsigned.pop("plan_sha256", None)
    if supplied != planner.sha256_bytes(planner.canonical_bytes(unsigned)) or supplied != start.get("plan_sha256"):
        raise planner.QualificationError(5, "campaign_corrupt", "retained plan digest does not match its content")
    selected = start["selected_lane_ids"]
    outcomes: dict[str, str] = {lane_id: "pending" for lane_id in selected}
    attempts: dict[str, list[str]] = {lane_id: [] for lane_id in selected}
    active_seconds: dict[str, float] = {lane_id: 0.0 for lane_id in selected}
    queue_seconds: dict[str, float] = {lane_id: 0.0 for lane_id in selected}
    lock_wait_seconds: dict[str, float] = {lane_id: 0.0 for lane_id in selected}
    cleanup_seconds: dict[str, float] = {lane_id: 0.0 for lane_id in selected}
    resource_hold_seconds: dict[str, float] = {name: 0.0 for name in start["resources"]}
    failures: list[dict[str, Any]] = []
    max_active = active = 0
    for event in events:
        lane_id = event.get("lane_id")
        if event["event"] == "lane_started":
            active += 1; max_active = max(max_active, active)
            queue_seconds[lane_id] += float(event.get("queue_seconds", 0.0))
            lock_wait_seconds[lane_id] += float(event.get("lock_wait_seconds", 0.0))
        elif event["event"] == "lane_finished":
            active = max(0, active - 1); outcomes[lane_id] = event["outcome"]; attempts[lane_id].append(event["attempt_id"])
            active_seconds[lane_id] += float(event.get("duration_seconds", 0.0))
            if event["outcome"] != "passed": failures.append(event)
        elif event["event"] == "lane_cleanup":
            cleanup_seconds[lane_id] += float(event.get("cleanup_duration_seconds", 0.0))
            for resource in event.get("resources", []):
                resource_hold_seconds[resource] = resource_hold_seconds.get(resource, 0.0) + float(event.get("resource_hold_seconds", 0.0))
        elif event["event"] == "lane_blocked":
            outcomes[lane_id] = "blocked_dependency"
            failures.append(event)
        elif event["event"] == "lane_cancelled":
            outcomes[lane_id] = "cancelled"
            failures.append(event)
        elif event["event"] == "resume_retained": outcomes[lane_id] = "retained"
        elif event["event"] == "campaign_finished" and isinstance(event.get("states"), dict):
            for selected_lane, outcome in event["states"].items():
                if selected_lane in outcomes and isinstance(outcome, str):
                    outcomes[selected_lane] = outcome
    recorded_failure_lanes = {event.get("lane_id") for event in failures}
    for lane_id in selected:
        if outcomes[lane_id] in ("unresolved", "pending") and lane_id not in recorded_failure_lanes:
            failures.append({"event": "incomplete", "lane_id": lane_id, "outcome": outcomes[lane_id]})
    lanes = []
    source_lanes = {lane["id"]: lane for lane in start["plan"]["lanes"]}
    for lane_id in selected:
        lane = source_lanes[lane_id]
        lanes.append({
            "lane_id": lane_id, "outcome": outcomes[lane_id], "attempt_ids": attempts[lane_id],
            "dependencies": lane["depends_on"], "resources": lane.get("resource_locks", []),
            "argv": lane["argv"], "queue_seconds": round(queue_seconds[lane_id], 6),
            "lock_wait_seconds": round(lock_wait_seconds[lane_id], 6),
            "active_seconds": round(active_seconds[lane_id], 6),
            "cleanup_seconds": round(cleanup_seconds[lane_id], 6),
        })
    counts = {name: sum(1 for value in outcomes.values() if value == name) for name in ("passed", "failed", "blocked_dependency", "cancelled", "retained", "pending", "timed_out", "cleanup_failed", "unresolved")}
    best: dict[str, tuple[float, list[str]]] = {}
    for lane in lanes:
        lane_id = lane["lane_id"]
        parents = [best[dependency] for dependency in lane["dependencies"] if dependency in best]
        prior_seconds, prior_path = max(parents, default=(0.0, []), key=lambda item: (item[0], item[1]))
        best[lane_id] = (
            prior_seconds + queue_seconds[lane_id] + active_seconds[lane_id] + cleanup_seconds[lane_id],
            prior_path + [lane_id],
        )
    critical_seconds, critical_path = max(best.values(), default=(0.0, []), key=lambda item: (item[0], item[1]))
    finished_events = [event for event in events if event["event"] == "campaign_finished"]
    campaign_duration = sum(float(event.get("duration_seconds", 0.0)) for event in finished_events)
    utilization = {
        name: {
            "hold_seconds": round(seconds, 6),
            "capacity": start["resources"][name],
            "utilization_fraction": round(seconds / (campaign_duration * start["resources"][name]), 6)
            if campaign_duration > 0 else 0.0,
        }
        for name, seconds in sorted(resource_hold_seconds.items())
    }
    return {
        "schema_version": CAMPAIGN_SCHEMA_VERSION, "campaign_id": campaign_id,
        "authority": "orchestration_evidence_only_not_product_qualification_or_reuse_acceptance",
        "selected_lane_count": len(selected), "counts": counts,
        "maximum_observed_concurrency": max_active, "lanes": lanes, "failures": failures,
        "resource_utilization": utilization, "critical_path_observation": critical_path,
        "critical_path_observed_seconds": round(critical_seconds, 6),
        "campaign_duration_seconds": round(campaign_duration, 6),
        "events": events,
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    subs = result.add_subparsers(dest="command", required=True)
    run = subs.add_parser("run")
    run.add_argument("--plan", type=Path, required=True); run.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    run.add_argument("--records", type=Path, required=True); run.add_argument("--lane", action="append", default=[]); run.add_argument("--accept-selected", action="store_true")
    run.add_argument("--authorize-product", action="store_true"); run.add_argument("--max-workers", type=int); run.add_argument("--continue-on-failure", action="store_true")
    resume = subs.add_parser("resume")
    resume.add_argument("--campaign", required=True); resume.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST); resume.add_argument("--records", type=Path, required=True)
    resume.add_argument("--authorize-product", action="store_true"); resume.add_argument("--max-workers", type=int); resume.add_argument("--continue-on-failure", action="store_true")
    report = subs.add_parser("report")
    report.add_argument("--campaign", required=True); report.add_argument("--records", type=Path, required=True); report.add_argument("--output", type=Path)
    return result


def main(arguments: list[str] | None = None) -> int:
    try:
        options = parser().parse_args(arguments)
        if options.command == "run":
            campaign_id, code = run_campaign(options.plan, options.manifest, options.records, options.lane, options.accept_selected, options.continue_on_failure, options.max_workers, options.authorize_product)
            print(f"campaign={campaign_id} exit={code}")
            return code
        if options.command == "resume":
            return resume_campaign(options.campaign, options.manifest, options.records, options.continue_on_failure, options.max_workers, options.authorize_product)
        report = campaign_report(options.campaign, options.records)
        data = planner.canonical_bytes(report)
        if options.output:
            planner.write_json(data, str(options.output))
        else:
            sys.stdout.buffer.write(data)
        return 0
    except planner.QualificationError as error:
        print(f"CAMPAIGN-{error.code}: {error}", file=sys.stderr)
        return error.exit_code


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Own PostgreSQL databases and schema-evolution authorities for one bounded selector run."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import sys
import time
from urllib.error import URLError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[2]
GO_ROOT = ROOT.parent / "go-oversync"
MODES = ("additive", "sparse", "retired")
SELECTOR_MODES = {
    "additive-upgrade": ("additive",),
    "sparse-upgrade": ("sparse",),
    "paused-target": ("additive",),
    "retired-generation": ("additive", "retired"),
    "action-required-recovery": ("additive", "retired"),
    "checkpoint-recovery": ("additive", "sparse"),
    "nullable-relationships": ("additive",),
    "swift": ("additive",),
    "all": MODES,
}


def database_url(admin_url: str, name: str) -> str:
    parsed = urlsplit(admin_url)
    return urlunsplit((parsed.scheme, parsed.netloc, f"/{name}", parsed.query, parsed.fragment))


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_status(
    base_url: str,
    mode: str,
    process: subprocess.Popen[bytes],
    control_token: str,
) -> dict[str, object]:
    deadline = time.monotonic() + 90
    last_error = "authority did not start"
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"{mode} authority exited with {process.returncode}")
        try:
            request = Request(
                f"{base_url}/schema-evolution/status",
                headers={"X-Schema-Evolution-Control-Token": control_token},
            )
            with urlopen(request, timeout=2) as response:
                status = json.load(response)
            if status.get("mode") != mode:
                raise RuntimeError(f"expected {mode}, received {status.get('mode')}")
            return status
        except (OSError, URLError, json.JSONDecodeError, RuntimeError) as error:
            last_error = str(error)
            time.sleep(0.2)
    raise RuntimeError(f"timed out waiting for {mode}: {last_error}")


def default_commands(selector: str) -> list[list[str]]:
    if selector == "swift":
        return [["./gradlew", ":swift:fixtures:package:sync:swiftTest", "--no-daemon"]]
    test_name = {
        "additive-upgrade": "generationOneToTwoDrainsOldWriterAndPreservesDrafts",
        "sparse-upgrade": "sparseGenerationOneToThreeHasNoGenerationTwoTraffic",
        "paused-target": "pausedTargetKeepsOldWriterAvailable",
        "retired-generation": "retiredGenerationReplacesDirectlyWithoutOldUpload",
        "action-required-recovery": "actionRequiredStatesRemainDurableAcrossReopen",
        "checkpoint-recovery": "checkpointAndPersistentInterruptionRepresentatives",
        "nullable-relationships": "nullableForeignKeysAndScopeIsolationHold",
    }.get(selector)
    if selector == "checkpoint-recovery":
        return [
            [
                "./gradlew", ":platform-oversqlite-test:composeApp:jvmTest",
                "--tests", "*SchemaEvolutionLiveAuthorityJvmTest.checkpointAndPersistentInterruptionRepresentatives",
                "--no-daemon",
            ],
            [
                "./gradlew", ":library-oversqlite:jvmTest",
                "--tests", "*OversqliteOpeningCrashRecoveryJvmTest.additiveAndSparseProcessesRecoverBeforeAndAfterEveryU0ThroughU8Commit",
                "--tests", "*OversqliteOpeningCrashRecoveryJvmTest.localOnlyReadyProcessDiesBeforeGenerationOpeningAndNextProcessCompletesEdge",
                "--no-daemon",
            ],
            [
                "./gradlew", ":platform-oversqlite-test:composeApp:jsBrowserTest",
                "--tests", "*SchemaEvolutionBrowserOwnershipRecoveryTest.*", "--no-daemon",
            ],
            [
                "./gradlew", ":platform-oversqlite-test:composeApp:wasmJsBrowserTest",
                "--tests", "*SchemaEvolutionBrowserOwnershipRecoveryTest.*", "--no-daemon",
            ],
        ]
    if test_name:
        return [[
            "./gradlew", ":platform-oversqlite-test:composeApp:jvmTest",
            "--tests", f"*SchemaEvolutionLiveAuthorityJvmTest.{test_name}", "--no-daemon",
        ]]
    return [[
        "./gradlew", ":platform-oversqlite-test:composeApp:jvmTest",
        "--tests", "*SchemaEvolutionLiveAuthorityJvmTest", "--no-daemon",
    ]]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--selector", choices=tuple(SELECTOR_MODES), default="all")
    parser.add_argument("--admin-url", default=os.getenv(
        "SCHEMA_EVOLUTION_POSTGRES_ADMIN_URL",
        "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable",
    ))
    parser.add_argument("--artifact-dir", type=Path)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    custom_command = bool(command)
    commands = [command] if command else default_commands(args.selector)

    run_id = f"{int(time.time())}_{os.getpid()}_{secrets.token_hex(3)}"
    artifact_dir = (args.artifact_dir or ROOT / "build" / "schema-evolution-authority" / run_id).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=False)
    jwt_secret = f"schema-evolution-jwt-{secrets.token_urlsafe(24)}"
    processes: dict[str, subprocess.Popen[bytes]] = {}
    logs: dict[str, object] = {}
    databases: list[str] = []
    status: dict[str, object] = {}
    started = time.time()
    result_code = 1
    try:
        child_env = os.environ.copy()
        if args.selector == "checkpoint-recovery":
            child_env["OVERSQLITE_PLATFORM_TESTS"] = "true"

        def stop_mode(mode: str) -> None:
            process = processes.pop(mode)
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            logs.pop(mode).close()

        def launch(mode: str, name: str) -> None:
            if not name.replace("_", "").isalnum():
                raise RuntimeError("generated database name is unsafe")
            port = free_port()
            control_token = f"schema-evolution-control-{secrets.token_urlsafe(24)}"
            base_url = f"http://127.0.0.1:{port}"
            log_file = (artifact_dir / f"authority-{mode}.log").open("wb")
            logs[mode] = log_file
            authority_env = child_env | {
                "DATABASE_URL": database_url(args.admin_url, name),
                "JWT_SECRET": jwt_secret,
                "SCHEMA_EVOLUTION_AUTHORITY_ADDR": f"127.0.0.1:{port}",
                "SCHEMA_EVOLUTION_AUTHORITY_MODE": mode,
                "SCHEMA_EVOLUTION_CONTROL_TOKEN": control_token,
            }
            process = subprocess.Popen(
                ["go", "run", "./integration/schema_evolution_authority"],
                cwd=GO_ROOT,
                env=authority_env,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            processes[mode] = process
            status[mode] = wait_status(base_url, mode, process, control_token)
            key = mode.upper()
            child_env[f"SCHEMA_EVOLUTION_{key}_BASE_URL"] = base_url
            child_env[f"SCHEMA_EVOLUTION_{key}_CONTROL_TOKEN"] = control_token

        transition_selector = (
            args.selector
            if args.selector in {"retired-generation", "action-required-recovery"} and not custom_command
            else None
        )
        modes = ("additive",) if transition_selector else SELECTOR_MODES[args.selector]
        shared_name = f"schema_evolution_transition_{run_id}" if transition_selector else ""
        for mode in modes:
            name = shared_name or f"schema_evolution_{mode}_{run_id}"
            subprocess.run(["createdb", "--maintenance-db", args.admin_url, name], check=True)
            databases.append(name)
            launch(mode, name)
        child_env["SCHEMA_EVOLUTION_AUTHORITY_JWT_SECRET"] = jwt_secret
        if transition_selector == "retired-generation":
            child_env["SCHEMA_EVOLUTION_RETIRED_SQLITE_PATH"] = str(artifact_dir / "retired-client.sqlite3")
            child_env["SCHEMA_EVOLUTION_RETIRED_USER"] = f"schema-evolution-retired-{run_id}"
            seed = subprocess.run([
                "./gradlew", ":platform-oversqlite-test:composeApp:jvmTest", "--tests",
                "*SchemaEvolutionLiveAuthorityJvmTest.seedRetiredGeneration", "--no-daemon",
            ], cwd=ROOT, env=child_env)
            if seed.returncode != 0:
                result_code = seed.returncode
                return result_code
            stop_mode("additive")
            launch("retired", shared_name)
        elif transition_selector == "action-required-recovery":
            child_env["SCHEMA_EVOLUTION_ACTION_REQUIRED_DIR"] = str(artifact_dir / "action-required-clients")
            child_env["SCHEMA_EVOLUTION_ACTION_REQUIRED_RUN"] = f"schema-evolution-action-required-{run_id}"
            seed = subprocess.run([
                "./gradlew", ":platform-oversqlite-test:composeApp:jvmTest", "--tests",
                "*SchemaEvolutionLiveAuthorityJvmTest.seedActionRequiredVariants", "--no-daemon",
            ], cwd=ROOT, env=child_env)
            if seed.returncode != 0:
                result_code = seed.returncode
                return result_code
            stop_mode("additive")
            launch("retired", shared_name)
        (artifact_dir / "launch.json").write_text(json.dumps({
            "run_id": run_id,
            "selector": args.selector,
            "commands": commands,
            "database_names": databases,
            "authority_status": status,
            "sqlite_now_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
            "go_oversync_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=GO_ROOT, text=True).strip(),
        }, indent=2, sort_keys=True) + "\n")
        for selected_command in commands:
            completed = subprocess.run(selected_command, cwd=ROOT, env=child_env)
            result_code = completed.returncode
            if result_code != 0:
                break
        return result_code
    finally:
        for process in processes.values():
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
        for process in processes.values():
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        for log_file in logs.values():
            log_file.close()
        cleanup_errors: list[str] = []
        for name in reversed(databases):
            last_error = ""
            for attempt in range(10):
                dropped = subprocess.run(
                    ["dropdb", "--maintenance-db", args.admin_url, "--if-exists", name],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                if dropped.returncode == 0:
                    last_error = ""
                    break
                last_error = dropped.stderr.strip()
                time.sleep(0.2 * (attempt + 1))
            if last_error:
                cleanup_errors.append(f"{name}: {last_error}")
        (artifact_dir / "result.json").write_text(json.dumps({
            "finished_at_epoch": time.time(),
            "started_at_epoch": started,
            "exit_code": result_code,
            "cleanup_errors": cleanup_errors,
        }, indent=2, sort_keys=True) + "\n")
        if cleanup_errors:
            print("schema-evolution launcher cleanup failed: " + "; ".join(cleanup_errors), file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())

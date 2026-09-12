#!/usr/bin/env python3
"""Run bounded mixed-generation JVM/Go pairings on one owned PostgreSQL database.

This is a test-only launcher. It never targets a developer database by name, and it removes only
the database and subprocesses it creates.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import signal
import shutil
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from urllib.parse import urlsplit, urlunsplit


ROOT = Path(__file__).resolve().parents[2]
GO_ROOT = ROOT.parent / "go-oversync"
HELPER = "TestMixedSchemaProcessHelper"
TEST_CLASS = "MixedGenerationJvmTest"
MANIFEST_PATH = Path(__file__).with_name("mixed-generation-manifest.json")
DDL = """
CREATE SCHEMA mixed;
CREATE TABLE mixed.projects(_sync_scope_id text NOT NULL,id uuid NOT NULL,title text NOT NULL,PRIMARY KEY(_sync_scope_id,id));
CREATE TABLE mixed.people(_sync_scope_id text NOT NULL,id uuid NOT NULL,name text NOT NULL,note text DEFAULT 'server-default',project_id uuid,PRIMARY KEY(_sync_scope_id,id),FOREIGN KEY(_sync_scope_id,project_id) REFERENCES mixed.projects(_sync_scope_id,id) DEFERRABLE INITIALLY DEFERRED);
CREATE TABLE mixed.notes(_sync_scope_id text NOT NULL,id uuid NOT NULL,body text NOT NULL,PRIMARY KEY(_sync_scope_id,id));
CREATE TABLE mixed.effects(id uuid PRIMARY KEY, n bigint NOT NULL);
CREATE FUNCTION mixed.count_effect() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO mixed.effects VALUES(NEW.id,1) ON CONFLICT(id) DO UPDATE SET n=mixed.effects.n+1; RETURN NEW; END $$;
CREATE TRIGGER mixed_effect AFTER INSERT OR UPDATE ON mixed.people FOR EACH ROW EXECUTE FUNCTION mixed.count_effect();
"""


def database_url(admin_url: str, database: str) -> str:
    parsed = urlsplit(admin_url)
    return urlunsplit((parsed.scheme, parsed.netloc, f"/{database}", parsed.query, parsed.fragment))


def command_log(path: Path, argv: list[str], *, cwd: Path, env: dict[str, str]) -> None:
    path.write_text(json.dumps({"argv": argv, "cwd": str(cwd), "env": env}, indent=2) + "\n")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def git_output(root: Path, *args: str) -> bytes:
    return subprocess.run(
        ["git", *args],
        cwd=root,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ).stdout


def candidate_identity(root: Path) -> dict[str, object]:
    untracked_paths = [
        item.decode()
        for item in git_output(root, "ls-files", "--others", "--exclude-standard", "-z").split(b"\0")
        if item
    ]
    identity: dict[str, object] = {
        "repo_root": str(root),
        "head": git_output(root, "rev-parse", "HEAD").decode().strip(),
        "index_tree": git_output(root, "write-tree").decode().strip(),
        "status_sha256": sha256(git_output(root, "status", "--porcelain=v1", "-z")),
        "tracked_patch_sha256": sha256(git_output(root, "diff", "--binary", "HEAD")),
        "untracked_files": [
            {"path": path, "sha256": sha256((root / path).read_bytes())}
            for path in untracked_paths
        ],
    }
    identity["identity_sha256"] = sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode())
    return identity


def mixed_generation_candidate_identity() -> dict[str, object]:
    return {"kmp": candidate_identity(ROOT), "go": candidate_identity(GO_ROOT)}


def run_checked(argv: list[str], *, cwd: Path, log: Path, env: dict[str, str] | None = None, timeout: int = 120) -> str:
    completed = subprocess.run(argv, cwd=cwd, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
    log.write_text(completed.stdout)
    if completed.returncode:
        raise RuntimeError(f"command failed ({completed.returncode}): {' '.join(argv)}")
    return completed.stdout


def load_rows(selector: str, only: list[str]) -> list[dict[str, object]]:
    manifest = json.loads(MANIFEST_PATH.read_text())
    rows = manifest["rows"]
    if len(rows) != manifest["required_rows"] or len({row["id"] for row in rows}) != len(rows):
        raise RuntimeError("mixed-generation manifest must contain exactly 24 unique required rows")
    if only:
        requested = set(only)
        available = {str(row["id"]) for row in rows}
        unknown = sorted(requested - available)
        if unknown:
            raise RuntimeError("unknown diagnostic rows: " + ", ".join(unknown))
        return [row for row in rows if row["id"] in requested]
    if selector == "all":
        blocked = [row for row in rows if not row.get("implemented")]
        if blocked:
            raise RuntimeError("required mixed-generation rows are not executable: " + ", ".join(row["id"] for row in blocked))
        return rows
    return [row for row in rows if row.get("implemented")]


def parse_junit(selected: list[str], *, not_before: float, artifact_dir: Path) -> dict[str, object]:
    result_dir = ROOT / "platform-oversqlite-test/composeApp/build/test-results/jvmTest"
    cases: dict[str, ET.Element] = {}
    for report in result_dir.glob("TEST-*.xml"):
        if report.stat().st_mtime + 1 < not_before:
            continue
        for case in ET.parse(report).getroot().iter("testcase"):
            name = case.attrib.get("name", "").removesuffix("[jvm]").removesuffix("()")
            if name in selected:
                if name in cases:
                    raise RuntimeError(f"duplicate JUnit result for {name}")
                cases[name] = case
    missing = sorted(set(selected) - cases.keys())
    failed = sorted(name for name, case in cases.items() if case.find("failure") is not None or case.find("error") is not None)
    skipped = sorted(name for name, case in cases.items() if case.find("skipped") is not None)
    summary = {"expected": len(selected), "executed": len(cases), "passed": len(cases) - len(failed) - len(skipped), "failed": failed, "skipped": skipped, "missing": missing}
    reports = [report for report in result_dir.glob("TEST-*.xml") if report.stat().st_mtime + 1 >= not_before]
    for index, report in enumerate(reports, start=1):
        shutil.copy2(report, artifact_dir / f"junit-{index}.xml")
    if missing or failed or skipped or summary["passed"] != len(selected):
        raise RuntimeError(f"required JUnit reconciliation failed: {json.dumps(summary, sort_keys=True)}")
    return summary


def wait_for_server(process: subprocess.Popen[str], log: Path) -> str:
    deadline = time.monotonic() + 90
    lines: list[str] = []
    while time.monotonic() < deadline:
        line = process.stdout.readline()
        if line:
            lines.append(line)
            if line.startswith("MIXED_GENERATION:http"):
                log.write_text("".join(lines))
                return line.strip().removeprefix("MIXED_GENERATION:")
        elif process.poll() is not None:
            log.write_text("".join(lines))
            raise RuntimeError(f"mixed-generation server exited with {process.returncode}")
    log.write_text("".join(lines))
    raise RuntimeError("timed out waiting for mixed-generation server URL")


def terminate(process: subprocess.Popen[str] | None) -> None:
    if process is None or process.poll() is not None:
        return
    process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=30)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--admin-url", default=os.getenv("MIXED_GENERATION_POSTGRES_ADMIN_URL", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"))
    parser.add_argument("--artifact-dir", type=Path, required=True)
    parser.add_argument("--selector", choices=("implemented", "all"), default="all")
    parser.add_argument("--only", action="append", default=[], help="diagnostic manifest row id; repeat as needed")
    args = parser.parse_args()

    started = time.monotonic()
    args.artifact_dir = args.artifact_dir.resolve()
    args.artifact_dir.mkdir(parents=True, exist_ok=False)
    run_id = f"mixed_generation_{int(time.time())}_{secrets.token_hex(5)}"
    database = run_id.replace("-", "_")
    run_database_url = database_url(args.admin_url, database)
    result: dict[str, object] = {"run_id": run_id, "database": database, "selector": args.selector, "status": "failed"}
    server: subprocess.Popen[str] | None = None
    candidate_before: dict[str, object] | None = None
    exit_code = 1
    try:
        candidate_before = mixed_generation_candidate_identity()
        (args.artifact_dir / "candidate-identity-before.json").write_text(json.dumps(candidate_before, indent=2, sort_keys=True) + "\n")
        result["candidate_identity_before"] = candidate_before
        rows = load_rows(args.selector, args.only)
        runtime = run_checked(
            ["psql", "--no-psqlrc", "--tuples-only", "--no-align", "--dbname", args.admin_url, "-c", "select current_setting('server_version_num'),current_setting('server_version'),current_setting('fsync'),current_setting('synchronous_commit'),current_setting('full_page_writes')"],
            cwd=ROOT, log=args.artifact_dir / "postgres-runtime.log",
        ).strip().split("|")
        if len(runtime) != 5 or int(runtime[0]) // 10000 != 16 or runtime[2:] != ["on", "on", "on"]:
            raise RuntimeError(f"mixed-generation qualification requires PostgreSQL 16 with durability on|on|on, observed {runtime}")
        result["postgresql"] = {"version_num": runtime[0], "version": runtime[1], "fsync": runtime[2], "synchronous_commit": runtime[3], "full_page_writes": runtime[4]}
        run_checked(["psql", "--dbname", args.admin_url, "-v", "ON_ERROR_STOP=1", "-c", f'CREATE DATABASE "{database}"'], cwd=ROOT, log=args.artifact_dir / "create-database.log")
        run_checked(["psql", "--dbname", run_database_url, "-v", "ON_ERROR_STOP=1", "-c", DDL], cwd=ROOT, log=args.artifact_dir / "prepare-schema.log")
        peer_binary = args.artifact_dir / "mixed-generation-go-peer.test"
        run_checked(["env", "GOTOOLCHAIN=go1.25.12", "go", "test", "-c", "-o", str(peer_binary), "./oversqlite_e2e"], cwd=GO_ROOT, log=args.artifact_dir / "go-build.log", timeout=900)
        classpath_file = ROOT / "platform-oversqlite-test/composeApp/build/mixed-generation/jvm-test-classpath.txt"
        run_checked(
            ["./gradlew", ":platform-oversqlite-test:composeApp:writeMixedGenerationClasspath", "--no-daemon"],
            cwd=ROOT,
            log=args.artifact_dir / "kmp-build.log",
            timeout=900,
        )
        kmp_classpath = classpath_file.read_text().strip()
        if not kmp_classpath:
            raise RuntimeError("KMP subprocess classpath task produced an empty classpath")
        server_env = os.environ | {"MIXED_GENERATION_HELPER": "current-test-subprocess-v1", "MIXED_GENERATION_ROLE": "server", "MIXED_GENERATION_DSN": run_database_url, "MIXED_GENERATION_SERVER_TIMEOUT_SECONDS": "600"}
        command_log(args.artifact_dir / "server-command.json", [str(peer_binary), f"-test.run=^{HELPER}$", "-test.v"], cwd=GO_ROOT, env={k: server_env[k] for k in ("MIXED_GENERATION_HELPER", "MIXED_GENERATION_ROLE", "MIXED_GENERATION_DSN", "MIXED_GENERATION_SERVER_TIMEOUT_SECONDS")})
        server = subprocess.Popen([str(peer_binary), f"-test.run=^{HELPER}$", "-test.v"], cwd=GO_ROOT, env=server_env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        base_url = wait_for_server(server, args.artifact_dir / "server.log")
        selected = [str(row["selector"]) for row in rows]
        tests = [f"*{TEST_CLASS}.{name}" for name in selected]
        gradle = ["./gradlew", ":platform-oversqlite-test:composeApp:jvmTest", "--no-daemon"] + sum((["--tests", test] for test in tests), [])
        gradle_env = os.environ | {"MIXED_GENERATION_BASE_URL": base_url, "MIXED_GENERATION_GO_PEER_BINARY": str(peer_binary), "MIXED_GENERATION_DSN": run_database_url, "MIXED_GENERATION_REQUIRED": "1", "MIXED_GENERATION_KMP_CLASSPATH": kmp_classpath}
        command_log(args.artifact_dir / "gradle-command.json", gradle, cwd=ROOT, env={k: gradle_env[k] for k in ("MIXED_GENERATION_BASE_URL", "MIXED_GENERATION_GO_PEER_BINARY", "MIXED_GENERATION_DSN", "MIXED_GENERATION_REQUIRED")})
        test_started = time.time()
        run_checked(gradle, cwd=ROOT, env=gradle_env, log=args.artifact_dir / "gradle.log", timeout=900)
        junit = parse_junit(selected, not_before=test_started, artifact_dir=args.artifact_dir)
        result.update({"status": "passed", "base_url": base_url, "junit": junit})
        exit_code = 0
    except Exception as error:
        result["error"] = str(error)
    finally:
        terminate(server)
        try:
            run_checked(["psql", "--dbname", args.admin_url, "-v", "ON_ERROR_STOP=1", "-c", f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'], cwd=ROOT, log=args.artifact_dir / "drop-database.log")
        except Exception as cleanup_error:
            result["cleanup_error"] = str(cleanup_error)
        if candidate_before is not None:
            candidate_after = mixed_generation_candidate_identity()
            (args.artifact_dir / "candidate-identity-after.json").write_text(json.dumps(candidate_after, indent=2, sort_keys=True) + "\n")
            result["candidate_identity_after"] = candidate_after
            result["candidate_identity_matches"] = candidate_before == candidate_after
            if candidate_before != candidate_after:
                result["status"] = "failed"
                result["error"] = "source candidate changed during qualification"
                exit_code = 1
        result["elapsed_seconds"] = round(time.monotonic() - started, 3)
        (args.artifact_dir / "result.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Explicit live regressions; no campaign selection or qualification authority."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import select
import shlex
import shutil
import signal
import stat
import subprocess
import sys
import threading
import time
from urllib.parse import unquote, urlsplit
import uuid
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[2]
GO_TEST = "TestMobileFlow_DirectSyncDrainsSeveralAdmittedBatchesOnce"
GO_PACKAGE = "github.com/mobiletoly/go-oversync/integration/mobile_flow"
SWIFT_MODULE = "SQLiteNowSyncSupportTests"
SWIFT_CLASS = "SyncCallerCancellationLiveTests"
SWIFT_TEST = "testCancellingCallerDuringSecondPushJoinsOwnedOperation"
SWIFT_STATE_CLASS = "SyncCancellationStateTests"
SWIFT_STATE_TESTS = (
    "testSnapshotAdmissionAndPredispatchCancellationPreserveOwnedState",
    "testConflictAdmissionAndPredispatchCancellationPreservePendingDecision",
    "testPushCancellationRetainsFrozenRequestAcrossRetry",
    "testSnapshotCancellationPreservesStateAndResumes",
)
SWIFT_CONFLICT_TEST = "testReachedConflictCancellationRollsBackOwnedState"
DATABASE_ENV = "OVERSYNC_EXAMPLE_TEST_DATABASE_URL"
PACKAGE_TASK = ":swift:runtime:sync:packageDebugRuntimeXcframework"
FIXTURE = "./integration/nethttp_server/cmd/swift_cancel_fixture"


class SmokeError(Exception):
    pass


def credential_values(env: dict[str, str]) -> set[str]:
    # Include JSON-escaped connection strings and encoded/decoded passwords.
    secrets = set()
    for key, value in env.items():
        if value and any(word in key.upper() for word in ("PASSWORD", "SECRET", "TOKEN", "DATABASE_URL")):
            secrets.update((value, json.dumps(value)[1:-1]))
            if "://" in value:
                try:
                    password = urlsplit(value).password
                except ValueError:
                    password = None
                if password:
                    secrets.update((password, unquote(password)))
    return secrets


class LogRedactor:
    """Hold potential credential prefixes until the next line or end of output."""

    def __init__(self, env: dict[str, str]):
        secrets = sorted(credential_values(env), key=len, reverse=True)
        self.pattern = re.compile("|".join(r"[\r\n]*".join(map(re.escape, s)) for s in secrets)) if secrets else None
        self.prefixes = {s[:i] for s in secrets for i in range(1, len(s))}
        self.longest = max(map(len, secrets), default=0)
        self.pending = ""

    def complete(self, text: str) -> str:
        """Redact complete values in a bounded field without EOF-prefix masking."""
        if self.pattern:
            text = self.pattern.sub("[REDACTED]", text)
        text = re.sub(r"(postgres(?:ql)?://)[^\s/@]+@", r"\1[REDACTED]@", text)
        return re.sub(r"(?i)(password\s*[=:]\s*)(?:'[^']*'|\"[^\"]*\"|[^\s,;]+)", r"\1[REDACTED]", text)

    def feed(self, text: str, *, final: bool = False) -> str:
        text = self.pending + text
        # Find the longest trailing partial credential, ignoring inserted CR/LF.
        cut, suffix = len(text), ""
        for i in range(len(text) - 1, -1, -1):
            if text[i] in "\r\n":
                continue
            suffix = text[i] + suffix
            if len(suffix) >= self.longest:
                break
            if suffix in self.prefixes:
                cut = i
        # Do not split a complete credential because its tail happens to be
        # another credential's prefix. Conversely, retain an incomplete URL
        # before redacting a shorter password inside it.
        if self.pattern:
            for match in self.pattern.finditer(text):
                if match.start() < cut < match.end():
                    cut = match.end()
            if not text[cut:].strip("\r\n"):
                cut = len(text)
        self.pending = text[cut:]
        safe = self.complete(text[:cut])
        if final and self.pending:
            safe += "[REDACTED]"  # An interrupted credential must not leak its prefix.
            self.pending = ""
        return safe


def redact(text: str, env: dict[str, str]) -> str:
    return LogRedactor(env).feed(text, final=True)


def inspect_command(argv: list[str], cwd: Path, env: dict[str, str]) -> str:
    result = subprocess.run(argv, cwd=cwd, env=env, capture_output=True, text=True, timeout=30)
    if result.returncode:
        raise SmokeError(f"inspection failed: {shlex.join(argv)}: {redact(result.stderr, env)}")
    return result.stdout.strip()


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def identity(root: Path, env: dict[str, str]) -> str:
    head = inspect_command(["git", "rev-parse", "HEAD"], root, env)
    status = inspect_command(["git", "status", "--porcelain=v1", "-uall"], root, env)
    diff = inspect_command(["git", "diff", "--binary", "HEAD"], root, env)
    untracked = inspect_command(["git", "ls-files", "--others", "--exclude-standard", "-z"], root, env)
    digest = hashlib.sha256()
    for name in sorted(filter(None, untracked.split("\0"))):
        path = root / name
        if path.is_file():
            digest.update(name.encode() + b"\0" + hashlib.sha256(path.read_bytes()).digest())
    return f"HEAD={head}; status={sha(status.encode())}; diff={sha(diff.encode())}; untracked={digest.hexdigest()}"


def contract_hashes(root: Path, go: Path) -> dict[str, str]:
    paths = [root / "AGENTS.md", go / "AGENTS.md",
             root / "docs-engineering/oversqlite-validation.md",
             root / "specs/oversqlite-protocol-simplification-and-efficiency.md",
             root / "specs/oversqlite-convergent-sync.md",
             root / "specs/oversqlite-platform-gzip-implementation.md",
             go / "specs/p0000-oversqlite-protocol-simplification-and-efficiency.md",
             go / "swagger/two_way_sync.yaml", root / "oversqlite-contracts/MANIFEST.sha256"]
    missing = [str(p) for p in paths if not p.is_file()]
    if missing:
        raise SmokeError("missing governing input: " + ", ".join(missing))
    return {str(p): sha(p.read_bytes()) for p in paths}


class Child:
    """Own one process group and persist its output with credentials redacted."""

    def __init__(self, argv: list[str], cwd: Path, env: dict[str, str], log: Path):
        self.proc = subprocess.Popen(argv, cwd=cwd, env=env, stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT, start_new_session=True)
        self.log = log
        self.error: Exception | None = None
        self.env = env
        self.reader = threading.Thread(target=self._drain, daemon=True)
        self.reader.start()

    def _drain(self) -> None:
        try:
            with self.log.open("w") as output, self.proc.stdout:
                redactor = LogRedactor(self.env)
                for line in self.proc.stdout:
                    output.write(redactor.feed(line.decode(errors="replace")))
                    output.flush()
                output.write(redactor.feed("", final=True))
        except Exception as error:
            self.error = error

    def terminate(self, seconds: float) -> None:
        # Signal only this launcher's process group, including wrapper descendants.
        deadline = time.monotonic() + max(0.01, seconds)
        try:
            os.killpg(self.proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            self.proc.wait(timeout=max(0.01, seconds))
            self.reader.join(timeout=max(0.01, deadline - time.monotonic()))
            if self.reader.is_alive():
                raise subprocess.TimeoutExpired(self.proc.args, seconds)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(self.proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.proc.wait(timeout=2)
            raise SmokeError("owned process required SIGKILL; cleanup is not a pass")
        finally:
            self.reader.join(timeout=2)

    def wait(self, seconds: float) -> int:
        try:
            code = self.proc.wait(timeout=max(0.01, seconds))
        except subprocess.TimeoutExpired as error:
            raise SmokeError("command timed out") from error
        self.reader.join(timeout=2)
        if self.reader.is_alive() or self.error:
            raise SmokeError(f"log capture incomplete: {self.error}")
        return code


class SwiftXMLCapture:
    """Receive native xUnit XML through a pipe and redact it before persistence."""

    def __init__(self, output: Path, env: dict[str, str]):
        self.directory = output / "swift-xml-pipe"
        self.directory.mkdir(mode=0o700)
        self.pipe = self.directory / "xunit"
        os.mkfifo(self.pipe, 0o600)
        # SwiftPM may open the FIFO, but cannot replace it with a regular file.
        self.directory.chmod(0o500)
        self.output = output / "swift-tests.xml"
        self.env = env
        self.data = bytearray()
        self.done = threading.Event()
        self.error: Exception | None = None
        self.reader = threading.Thread(target=self._drain, daemon=True)
        self.reader.start()

    def _drain(self) -> None:
        try:
            fd = os.open(self.pipe, os.O_RDONLY | os.O_NONBLOCK)
            try:
                while True:
                    ready, _, _ = select.select([fd], [], [], 0.05)
                    if ready:
                        chunk = os.read(fd, 65536)
                        if chunk:
                            self.data.extend(chunk)
                        elif self.done.is_set():
                            break
                        else:
                            time.sleep(0.02)
                    elif self.done.is_set():
                        break
            finally:
                os.close(fd)
        except Exception as error:
            self.error = error

    def finish(self) -> None:
        try:
            self.done.set()
            self.reader.join(timeout=2)
            if self.reader.is_alive() or self.error:
                raise SmokeError(f"Swift XML capture incomplete: {self.error}")
            if not stat.S_ISFIFO(self.pipe.stat().st_mode):
                raise SmokeError("Swift XML pipe was replaced")
            if self.data:
                # Parse in memory so XML-escaped credential characters are
                # decoded before redaction and re-escaped only when persisted.
                root = ET.fromstring(self.data)
                redactor = LogRedactor(self.env)
                for element in root.iter():
                    for key, value in element.attrib.items():
                        # Keep native test identity and numeric counts exact.
                        structural = ((element.tag == "testcase" and key in ("classname", "name"))
                                      or key in ("tests", "skipped", "failures", "errors"))
                        element.set(key, redactor.complete(value) if structural else redact(value, self.env))
                    if element.text is not None:
                        element.text = redact(element.text, self.env)
                    if element.tail is not None:
                        element.tail = redact(element.tail, self.env)
                self.output.write_bytes(ET.tostring(root, encoding="utf-8", xml_declaration=True))
        finally:
            self.directory.chmod(0o700)
            self.pipe.unlink(missing_ok=True)
            self.directory.rmdir()


def go_results(path: Path) -> str:
    events = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    if any(not isinstance(e, dict) or "Action" not in e for e in events):
        raise SmokeError("malformed Go test events")
    tests = [e for e in events if e.get("Test") and e["Action"] in ("pass", "fail", "skip")]
    if len(tests) != 1 or tests[0].get("Test") != GO_TEST or tests[0].get("Package") != GO_PACKAGE:
        raise SmokeError("Go selected test did not execute exactly once")
    if tests[0]["Action"] != "pass" or any(e["Action"] in ("fail", "skip") for e in events):
        raise SmokeError("Go test failed or skipped")
    if sum(e["Action"] == "run" and e.get("Test") == GO_TEST for e in events) != 1:
        raise SmokeError("Go test must have exactly one execution event")
    if not any(e["Action"] == "pass" and e.get("Package") == GO_PACKAGE and "Test" not in e for e in events):
        raise SmokeError("Go package did not pass")
    return f"{GO_TEST}: tests=1 passed=1 skipped=0 failed=0"


def swift_results(path: Path, log: Path, test_class: str = SWIFT_CLASS,
                  tests: tuple[str, ...] = (SWIFT_TEST,)) -> str:
    root = ET.parse(path).getroot()
    suites = list(root.iter("testsuite"))
    if root.tag != "testsuites" or not suites:
        raise SmokeError("Swift XML must contain counted test suites")
    cases = list(root.iter("testcase"))
    qualified_class = f"{SWIFT_MODULE}.{test_class}"
    if (len(cases) != len(tests) or sorted(c.get("name", "") for c in cases) != sorted(tests)
            or any(c.get("classname", "") != qualified_class for c in cases)):
        raise SmokeError("Swift selected tests did not execute exactly once each")
    if any(list(root.iter(tag)) for tag in ("skipped", "failure", "error")):
        raise SmokeError("Swift test failed or skipped")
    for suite in suites + list(root.iter("testsuites")):
        count = suite.get("tests")
        if count is None and suite.tag == "testsuite":
            raise SmokeError("Swift suite is missing its tests count")
        if count is not None and (not re.fullmatch(r"[0-9]+", count)
                                  or int(count) != len(list(suite.iter("testcase")))):
            raise SmokeError("Swift suite tests count contradicts its test cases")
        if any(int(suite.get(key, "0")) != 0 for key in ("skipped", "failures", "errors")):
            raise SmokeError("Swift suite reports failures or skips")
    # SwiftPM's XCTest XML describes selected subprocesses, not skip status.
    # Require native XCTest execution/pass lines too; unknown log formats fail closed.
    content = log.read_text()
    for test in tests:
        case = rf"^Test Case '-\[{re.escape(qualified_class)} {re.escape(test)}\]' "
        if (len(re.findall(case + r"started\.$", content, re.M)) != 1
                or len(re.findall(case + r"passed \([0-9.]+ seconds\)\.$", content, re.M)) != 1
                or re.search(case + r"(?:skipped|failed)\b", content, re.M)):
            raise SmokeError("Swift native log must confirm one execution and pass without a skip")
    return f"{test_class}.{','.join(tests)}: tests={len(tests)} passed={len(tests)} skipped=0 failed=0"


def authority_url(child: Child, seconds: float) -> str:
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if child.proc.poll() is not None:
            raise SmokeError("authority exited before readiness")
        content = child.log.read_text() if child.log.exists() else ""
        match = re.search(r"^OVERSQLITE_SWIFT_CONVERGENT_BASE_URL=(http://127\.0\.0\.1:[0-9]+)$", content, re.M)
        if match:
            return match[1]
        time.sleep(0.05)
    raise SmokeError("authority readiness timed out")


def go_argv(go: Path) -> list[str]:
    return [str(go / "scripts/go-test-with-cleanup.sh"), "-json", "-count=1",
            "-timeout=3m30s", "-run", f"^{GO_TEST}$", "."]


class Run:
    def __init__(self, mode: str, root: Path, go: Path, output: Path, env: dict[str, str]):
        self.mode, self.root, self.go, self.output = mode, root, go, output
        self.env = {**env, "GOTOOLCHAIN": "go1.25.12",
                    "OVERSYNC_SMOKE_DATABASE_NAME": "oversync_smoke_" + uuid.uuid4().hex,
                    "OVERSYNC_TEST_RUN_ID": "oversqlite-smoke-" + uuid.uuid4().hex}
        if mode != "go-direct-sync":
            # Select fixture/runtime explicitly; ambient flags must not mix witnesses.
            self.env.pop("OVERSQLITE_A3_SWIFT_TEST_BRIDGE", None)
            self.env.pop("OVERSYNC_SWIFT_A3_FIXTURE", None)
            if mode == "swift-cancellation-state":
                self.env["OVERSYNC_SWIFT_A3_FIXTURE"] = "1"
        self.children: list[Child] = []
        self.notes: list[str] = []
        self.started = time.monotonic()
        self.fixture = output / "swift-cancel-fixture"
        self.authority: Child | None = None
        self.before: dict[str, str] = {}
        self.hashes: dict[str, str] = {}
        self.database_may_exist = False

    def command(self, label: str, argv: list[str], cwd: Path, seconds: float) -> Path:
        if seconds <= 0:
            raise SmokeError(f"{label}: stage budget exhausted before launch")
        self.notes.append(f"- {label}: cwd `{cwd}`; `{shlex.join(argv)}`; budget {seconds:.3f}s")
        start = time.monotonic()
        child = None
        try:
            child = Child(argv, cwd, self.env, self.output / f"{label}.log")
            self.children.append(child)
            code = child.wait(seconds)
            if code:
                raise SmokeError(f"{label}: exit {code}")
            return child.log
        finally:
            self.notes.append(f"  - elapsed {time.monotonic() - start:.3f}s; exit {child.proc.poll() if child else 'not started'}")

    def preflight(self) -> None:
        value = self.env.get(DATABASE_ENV, "")
        if not value:
            raise SmokeError(f"{DATABASE_ENV} must explicitly identify an existing PostgreSQL service")
        if urlsplit(value).scheme not in ("postgres", "postgresql"):
            raise SmokeError(f"{DATABASE_ENV} must be a PostgreSQL URL")
        for tool in ["git", "go"] + (["swift", "java", "xcodebuild"] if self.mode != "go-direct-sync" else []):
            if not shutil.which(tool, path=self.env.get("PATH")):
                raise SmokeError(f"missing prerequisite: {tool}")
        for repo in (self.root, self.go):
            if self.output.is_relative_to(repo):
                result = subprocess.run(["git", "check-ignore", "-q", str(self.output)], cwd=repo)
                if result.returncode:
                    raise SmokeError("output inside a repository must be Git-ignored")
        self.before = {str(p): identity(p, self.env) for p in (self.root, self.go)}
        self.hashes = contract_hashes(self.root, self.go)
        self.notes.extend(f"- Before {p}: {value}" for p, value in self.before.items())
        self.notes.extend(f"- SHA-256 `{p}`: `{value}`" for p, value in self.hashes.items())
        for name in (DATABASE_ENV, "GOTOOLCHAIN", "OVERSYNC_SMOKE_DATABASE_NAME", "OVERSYNC_TEST_RUN_ID",
                     "JAVA_HOME", "DEVELOPER_DIR", "GOFLAGS", "GOWORK", "CGO_ENABLED", "GRADLE_USER_HOME",
                     "GRADLE_OPTS", "JAVA_OPTS", "PATH"):
            self.notes.append(f"- Environment {name}={redact(self.env.get(name, 'unset'), self.env)}")
        for tool, args in [("go", ["version"])] + ([("swift", ["--version"]), ("java", ["-version"]), ("xcodebuild", ["-version"])] if self.mode != "go-direct-sync" else []):
            self.command(f"toolchain-{tool}", [tool, *args], self.root, 30)

    def execute(self) -> None:
        self.preflight()
        deadline = time.monotonic() + (240 if self.mode == "go-direct-sync" else 900)
        self.command("prepare-fixture", ["go", "build", "-o", str(self.fixture), FIXTURE], self.go, deadline - time.monotonic())
        self.notes.append(f"- Authority/cleanup executable SHA-256: {sha(self.fixture.read_bytes())}")
        if self.mode == "go-direct-sync":
            self.database_may_exist = True
            result = self.command("go-tests", go_argv(self.go), self.go / "integration/mobile_flow", deadline - time.monotonic())
            self.notes.append("- Results: " + go_results(result))
            self.notes.append("- Authority startup and in-test cleanup: included in Go command; separate duration unknown")
            return
        self.command("prepare-runtime", [str(self.root / "gradlew"), "-Pa3SwiftTestBridge=false", PACKAGE_TASK],
                     self.root, deadline - time.monotonic())
        self.command("prepare-swift", ["swift", "build", "--build-tests"], self.root / "swift/support", deadline - time.monotonic())
        self.database_may_exist = True
        self.notes.append(f"- authority: `{self.fixture}`; readiness budget 60s")
        start = time.monotonic()
        self.authority = Child([str(self.fixture)], self.go, self.env, self.output / "authority.log")
        self.children.append(self.authority)
        try:
            base_url = authority_url(self.authority, 60)
        finally:
            self.notes.append(f"- Authority startup: {time.monotonic() - start:.3f}s")
        self.env["OVERSQLITE_SWIFT_CONVERGENT_BASE_URL"] = base_url
        if self.mode == "swift-cancellation-state":
            self.swift_cases("swift-state-default", SWIFT_STATE_CLASS, SWIFT_STATE_TESTS)
            self.env["OVERSQLITE_A3_SWIFT_TEST_BRIDGE"] = "1"
            self.command("prepare-bridge-runtime", [str(self.root / "gradlew"),
                         "-Pa3SwiftTestBridge=true", PACKAGE_TASK], self.root, deadline - time.monotonic())
            self.command("prepare-bridge-swift", ["swift", "build", "--build-tests"],
                         self.root / "swift/support", deadline - time.monotonic())
            self.swift_cases("swift-state-conflict", SWIFT_STATE_CLASS, (SWIFT_CONFLICT_TEST,))
        else:
            self.swift_cases("swift-tests", SWIFT_CLASS, (SWIFT_TEST,))

    def swift_cases(self, label: str, test_class: str, tests: tuple[str, ...]) -> None:
        output = self.output if label == "swift-tests" else self.output / label
        if output != self.output:
            output.mkdir()
        selector = (f"{test_class}/{tests[0]}" if len(tests) == 1 else
                    rf"{re.escape(test_class)}/(?:{'|'.join(map(re.escape, tests))})$")
        # SwiftPM emits XCTest XML only through the parallel runner. One worker
        # keeps shared authority barriers serial; verbose retains skip evidence.
        capture = SwiftXMLCapture(output, self.env)
        try:
            log = self.command(label, ["swift", "test", "--skip-build", "--parallel", "--num-workers", "1",
                                               "--verbose", "--disable-swift-testing", "--filter",
                                               selector, "--xunit-output", str(capture.pipe)],
                               self.root / "swift/support", 240)
        finally:
            capture.finish()
        self.notes.append("- Results: " + swift_results(capture.output, log, test_class, tests))

    def cleanup(self) -> None:
        start = time.monotonic()
        errors = []
        for child in reversed(self.children):
            if child.proc.poll() is None or child.reader.is_alive():
                try:
                    child.terminate(min(5, max(0.01, 30 - (time.monotonic() - start))))
                except Exception as error:
                    errors.append(str(error))
        if self.authority is not None and self.authority.proc.poll() != 0:
            errors.append(f"authority exit {self.authority.proc.poll()}")
        if self.database_may_exist:
            try:
                self.command("cleanup-database", [str(self.fixture), "--cleanup"], self.go,
                             max(0.01, 30 - (time.monotonic() - start)))
            except Exception as error:
                errors.append(str(error))
                child = self.children[-1] if self.children else None
                if child is not None and child.proc.poll() is None:
                    try:
                        child.terminate(0.01)
                    except Exception as stop_error:
                        errors.append(str(stop_error))
        self.notes.append(f"- Cleanup: {time.monotonic() - start:.3f}s; {'FAILED' if errors else 'passed (or no resources started)'}")
        if errors:
            raise SmokeError("; ".join(errors))

    def finish(self, failure: str | None) -> int:
        try:
            self.cleanup()
        except Exception as error:
            failure = f"{failure or ''}; {error}".strip("; ")
        try:
            if self.before:
                after = {p: identity(Path(p), self.env) for p in self.before}
                self.notes.extend(f"- After {p}: {value}" for p, value in after.items())
                if after != self.before or contract_hashes(self.root, self.go) != self.hashes:
                    raise SmokeError("repository or governing input changed during smoke")
        except Exception as error:
            failure = f"{failure or ''}; {error}".strip("; ")
        summary = [f"# {self.mode}: {'FAILED' if failure else 'PASSED'}", "",
                   "Focused harness evidence only; not product qualification, independent review or acceptance.", "",
                   f"- Finished UTC: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}",
                   f"- Total invocation elapsed: {time.monotonic() - self.started:.3f}s",
                   f"- Result: {failure or 'required named test and cleanup passed'}", *self.notes]
        (self.output / "summary.md").write_text(redact("\n".join(summary) + "\n", self.env))
        print(f"{'FAILED' if failure else 'PASSED'}: {self.output / 'summary.md'}")
        return 1 if failure else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["swift-cancellation", "swift-cancellation-state", "go-direct-sync"])
    parser.add_argument("--go-repo", type=Path, default=ROOT.parent / "go-oversync")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    output = (args.output or ROOT / ".artifacts/oversqlite-smoke" / (time.strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8])).resolve()
    output.mkdir(parents=True, exist_ok=False)
    run = Run(args.mode, ROOT, args.go_repo.resolve(), output, dict(os.environ))
    run.notes.append(f"- Started UTC: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}")
    previous = {}

    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"interrupted by signal {signum}")

    for sig in (signal.SIGINT, signal.SIGTERM):
        previous[sig] = signal.signal(sig, interrupted)
    failure = None
    try:
        run.execute()
    except (Exception, KeyboardInterrupt) as error:
        failure = f"{type(error).__name__}: {error}"
    finally:
        for sig in previous:
            signal.signal(sig, signal.SIG_IGN)
    try:
        return run.finish(failure)
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


if __name__ == "__main__":
    sys.exit(main())

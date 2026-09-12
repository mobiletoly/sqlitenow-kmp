"""No product children: subprocess coverage uses only controlled Python programs."""

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock
from xml.sax.saxutils import escape

from tools.qualification import oversqlite_smoke as smoke


def go_events(action="pass"):
    return [
        {"Action": "run", "Package": smoke.GO_PACKAGE, "Test": smoke.GO_TEST},
        {"Action": action, "Package": smoke.GO_PACKAGE, "Test": smoke.GO_TEST},
        {"Action": "pass" if action != "fail" else "fail", "Package": smoke.GO_PACKAGE},
    ]


def swift_xml(child="", name=smoke.SWIFT_TEST):
    return f'<testsuites><testsuite tests="1"><testcase classname="SQLiteNowSyncSupportTests.{smoke.SWIFT_CLASS}" name="{name}">{child}</testcase></testsuite></testsuites>'


def swift_log(result="passed", test_class=smoke.SWIFT_CLASS, test=smoke.SWIFT_TEST):
    case = f"Test Case '-[SQLiteNowSyncSupportTests.{test_class} {test}]' "
    return case + "started.\n" + case + result + " (0.194 seconds).\n"


class SmokeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.parent = Path(self.temp.name)
        self.env = {**os.environ, smoke.DATABASE_ENV: "postgres://runner:s%40fe-password@127.0.0.1/test"}

    def make_run(self, mode="go-direct-sync"):
        output = self.parent / str(len(list(self.parent.iterdir())))
        output.mkdir()
        return smoke.Run(mode, self.parent, self.parent, output, self.env)

    def child(self, code, name="child.log"):
        child = smoke.Child([sys.executable, "-c", code], self.parent, self.env, self.parent / name)
        self.addCleanup(lambda: child.terminate(1) if child.proc.poll() is None else None)
        return child

    def test_go_requires_named_fresh_success_and_package_success(self):
        path = self.parent / "go.jsonl"
        path.write_text("\n".join(map(json.dumps, go_events())))
        self.assertIn("passed=1", smoke.go_results(path))
        for events in ([], go_events("skip"), go_events("fail"), go_events()[1:], go_events()[:-1],
                       go_events() + go_events(), [{"Action": "pass", "Package": smoke.GO_PACKAGE}],
                       [{"Action": "pass", "Package": "wrong", "Test": smoke.GO_TEST}]):
            with self.subTest(events=events):
                path.write_text("\n".join(map(json.dumps, events)))
                with self.assertRaises(smoke.SmokeError):
                    smoke.go_results(path)
        path.write_text("not JSON")
        with self.assertRaises(ValueError):
            smoke.go_results(path)
        path.write_text("[]")
        with self.assertRaises(smoke.SmokeError):
            smoke.go_results(path)

    def test_swift_requires_named_execution_and_no_skip_or_failure(self):
        path = self.parent / "swift.xml"
        log = self.parent / "swift.log"
        log.write_text(swift_log())
        path.write_text(swift_xml())
        self.assertIn("passed=1", smoke.swift_results(path, log))
        bare_case = f'<testcase classname="SQLiteNowSyncSupportTests.{smoke.SWIFT_CLASS}" name="{smoke.SWIFT_TEST}"/>'
        for content in ("<testsuites/>", swift_xml("<skipped/>"), swift_xml("<failure/>"),
                        swift_xml("<error/>"), swift_xml(name="unrelated"),
                        bare_case, f"<testsuites>{bare_case}</testsuites>",
                        swift_xml().replace('tests="1"', 'tests="0"'),
                        swift_xml().replace('tests="1"', 'tests="2"'),
                        swift_xml().replace('tests="1"', 'tests="invalid"'),
                        swift_xml().replace('tests="1"', ''),
                        swift_xml().replace('<testsuites>', '<testsuites tests="2">'),
                        swift_xml().replace('tests="1"', 'tests="1" failures="1"')):
            with self.subTest(content=content):
                path.write_text(content)
                with self.assertRaises(smoke.SmokeError):
                    smoke.swift_results(path, log)
        path.write_text("not XML")
        with self.assertRaises(smoke.ET.ParseError):
            smoke.swift_results(path, log)
        path.write_text(swift_xml())
        for content in ("", swift_log("skipped"), swift_log("failed"), swift_log() * 2):
            log.write_text(content)
            with self.assertRaises(smoke.SmokeError):
                smoke.swift_results(path, log)

    def test_credentials_redacted_in_fragmented_child_output(self):
        child = self.child("import os,sys,time; s=os.environ['OVERSYNC_EXAMPLE_TEST_DATABASE_URL']; "
                           "sys.stdout.write(s[:15]); sys.stdout.flush(); time.sleep(.02); print(s[15:]); "
                           "print('password=s@fe-password'); print('s%40fe-password')")
        self.assertEqual(child.wait(2), 0)
        text = child.log.read_text()
        for secret in (self.env[smoke.DATABASE_ENV], "s@fe-password", "s%40fe-password"):
            self.assertNotIn(secret, text)
        self.assertIn("[REDACTED]", text)
        # Even malformed connection input must be reportable without leaking.
        self.assertEqual(smoke.redact("postgres://[broken", {smoke.DATABASE_ENV: "postgres://[broken"}), "[REDACTED]")

    def test_state_results_require_every_exact_case_and_native_pass(self):
        path, log = self.parent / "state.xml", self.parent / "state.log"
        tests = smoke.SWIFT_STATE_TESTS
        cls = smoke.SWIFT_STATE_CLASS

        def xml(names, test_class=cls):
            cases = ''.join(f'<testcase classname="SQLiteNowSyncSupportTests.{test_class}" name="{name}"/>' for name in names)
            return f'<testsuites><testsuite tests="{len(names)}">{cases}</testsuite></testsuites>'

        content = ''.join(swift_log(test_class=cls, test=t) for t in tests)
        path.write_text(xml(tests)); log.write_text(content)
        self.assertIn("passed=4", smoke.swift_results(path, log, cls, tests))
        for invalid in (xml(tests[:-1]), xml(tests + (tests[0],)),
                        xml((tests[0],) * len(tests)), xml(tests, "Not" + cls),
                        xml(tests).replace('/>', '><skipped/></testcase>', 1)):
            path.write_text(invalid)
            with self.assertRaises(smoke.SmokeError):
                smoke.swift_results(path, log, cls, tests)
        path.write_text(xml(tests))
        for invalid in (content.replace('passed', 'skipped', 1),
                        content.replace(swift_log(test_class=cls, test=tests[0]), ''),
                        content + swift_log(test_class=cls, test=tests[0])):
            log.write_text(invalid)
            with self.assertRaises(smoke.SmokeError):
                smoke.swift_results(path, log, cls, tests)

    def test_swift_results_require_exact_module_in_xml_and_native_log(self):
        path, log = self.parent / "module.xml", self.parent / "module.log"
        for cls, test in ((smoke.SWIFT_CLASS, smoke.SWIFT_TEST),
                          (smoke.SWIFT_STATE_CLASS, smoke.SWIFT_STATE_TESTS[0])):
            qualified = f"SQLiteNowSyncSupportTests.{cls}"
            xml = f'<testsuites><testsuite tests="1"><testcase classname="{qualified}" name="{test}"/></testsuite></testsuites>'
            native = swift_log(test_class=cls, test=test)
            path.write_text(xml); log.write_text(native)
            self.assertIn("passed=1", smoke.swift_results(path, log, cls, (test,)))
            for wrong in (f"OtherModule.{cls}", f"Other.{qualified}", cls):
                for source in ("xml", "log", "both"):
                    with self.subTest(cls=cls, wrong=wrong, source=source):
                        path.write_text(xml.replace(qualified, wrong) if source != "log" else xml)
                        log.write_text(native.replace(qualified, wrong) if source != "xml" else native)
                        with self.assertRaises(smoke.SmokeError):
                            smoke.swift_results(path, log, cls, (test,))

    def test_newline_split_credentials_are_withheld_before_disk_write(self):
        ready = self.parent / "credential-prefix-written"
        resume = self.parent / "continue-output"
        code = f'''import os,time
from pathlib import Path
secret = "s@fe-password"
print(secret[:5], flush=True)
Path({str(ready)!r}).touch()
while not Path({str(resume)!r}).exists(): time.sleep(.01)
print(secret[5:], flush=True)
'''
        child = self.child(code)
        deadline = time.monotonic() + 3
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(.01)
        self.assertTrue(ready.exists())
        self.assertNotIn("s@fe-", child.log.read_text() if child.log.exists() else "")
        resume.touch()
        self.assertEqual(child.wait(2), 0)
        self.assertEqual(child.log.read_text(), "[REDACTED]\n")

    def test_redactor_handles_every_credential_split_and_incomplete_eof(self):
        for secret in (self.env[smoke.DATABASE_ENV], "s%40fe-password", "s@fe-password"):
            for split in range(1, len(secret)):
                with self.subTest(secret_length=len(secret), split=split):
                    redactor = smoke.LogRedactor(self.env)
                    prefix = redactor.feed(secret[:split] + "\r\n")
                    self.assertEqual(prefix, "")
                    self.assertEqual(redactor.feed(secret[split:] + "\n", final=True), "[REDACTED]\n")
        redactor = smoke.LogRedactor(self.env)
        self.assertEqual(redactor.feed("s@fe-\n"), "")
        self.assertEqual(redactor.feed("", final=True), "[REDACTED]")

    def test_swift_xml_is_redacted_before_retained_file_write(self):
        output = self.parent / "swift-output"
        output.mkdir()
        env = {**self.env, smoke.DATABASE_ENV: self.env[smoke.DATABASE_ENV] + "?mode=test&source=xml"}
        capture = smoke.SwiftXMLCapture(output, env)
        ready = self.parent / "xml-written"
        resume = self.parent / "continue-xml-child"
        xml = swift_xml(f"<system-out>{escape(env[smoke.DATABASE_ENV])}</system-out>")
        code = f'''from pathlib import Path
import time
Path({str(capture.pipe)!r}).write_text({xml!r})
Path({str(ready)!r}).touch()
while not Path({str(resume)!r}).exists(): time.sleep(.01)
'''
        child = self.child(code)
        try:
            deadline = time.monotonic() + 3
            while not ready.exists() and time.monotonic() < deadline:
                time.sleep(.01)
            self.assertTrue(ready.exists())
            self.assertFalse(capture.output.exists())
            self.assertTrue(smoke.stat.S_ISFIFO(capture.pipe.stat().st_mode))
            resume.touch()
            self.assertEqual(child.wait(2), 0)
        finally:
            capture.finish()
        self.assertNotIn("source=xml", capture.output.read_text())
        self.assertIn("[REDACTED]", capture.output.read_text())
        self.assertFalse(capture.directory.exists())

    def test_missing_prerequisite_starts_no_child(self):
        run = self.make_run()
        del run.env[smoke.DATABASE_ENV]
        with mock.patch.object(smoke, "Child") as child:
            with self.assertRaisesRegex(smoke.SmokeError, smoke.DATABASE_ENV):
                run.execute()
            child.assert_not_called()
        run.env[smoke.DATABASE_ENV] = self.env[smoke.DATABASE_ENV]
        with mock.patch.object(smoke.shutil, "which", return_value=None), mock.patch.object(smoke, "Child") as child:
            with self.assertRaisesRegex(smoke.SmokeError, "missing prerequisite"):
                run.execute()
            child.assert_not_called()

    def test_nonzero_command_retains_status_and_stops(self):
        run = self.make_run()
        with self.assertRaisesRegex(smoke.SmokeError, "exit 7"):
            run.command("failure", [sys.executable, "-c", "print('failed'); raise SystemExit(7)"], self.parent, 2)
        self.assertEqual(run.finish("test failure"), 1)
        self.assertIn("exit 7", (run.output / "summary.md").read_text())

    def test_timeout_cleans_only_owned_child(self):
        unrelated = self.child("import time; time.sleep(20)", "unrelated.log")
        run = self.make_run()
        with self.assertRaisesRegex(smoke.SmokeError, "timed out"):
            run.command("timeout", [sys.executable, "-c", "import time; time.sleep(20)"], self.parent, .05)
        self.assertEqual(run.finish("timeout"), 1)
        self.assertIsNotNone(run.children[0].proc.poll())
        self.assertIsNone(unrelated.proc.poll())

    def test_exhausted_stage_budget_does_not_launch(self):
        with mock.patch.object(smoke, "Child") as child:
            with self.assertRaisesRegex(smoke.SmokeError, "budget exhausted"):
                self.make_run().command("late", ["anything"], self.parent, -1)
            child.assert_not_called()

    def test_authority_failure_and_readiness_timeout(self):
        child = self.child("raise SystemExit(3)")
        child.wait(2)
        with self.assertRaisesRegex(smoke.SmokeError, "exited"):
            smoke.authority_url(child, 1)
        child = self.child("import time; print('not ready', flush=True); time.sleep(10)", "unready.log")
        with self.assertRaisesRegex(smoke.SmokeError, "readiness timed out"):
            smoke.authority_url(child, .05)

    def test_cleanup_failure_overrides_pass(self):
        run = self.make_run()
        run.database_may_exist = True
        with mock.patch.object(run, "command", side_effect=smoke.SmokeError("cleanup denied")):
            self.assertEqual(run.finish(None), 1)
        self.assertIn("FAILED", (run.output / "summary.md").read_text())
        self.assertIn("cleanup denied", (run.output / "summary.md").read_text())

    def test_postflight_runs_even_after_cleanup_failure(self):
        run = self.make_run()
        run.before = {str(self.parent): "before"}
        with mock.patch.object(run, "cleanup", side_effect=smoke.SmokeError("cleanup failed")), \
             mock.patch.object(smoke, "identity", return_value="after") as identity:
            self.assertEqual(run.finish(None), 1)
            identity.assert_called_once()
        self.assertIn("changed during smoke", (run.output / "summary.md").read_text())

    def test_pinned_go_and_anchored_nested_selector(self):
        run = self.make_run()
        self.assertEqual(run.env["GOTOOLCHAIN"], "go1.25.12")
        self.assertRegex(run.env["OVERSYNC_SMOKE_DATABASE_NAME"], r"^oversync_smoke_[0-9a-f]{32}$")
        self.assertEqual(smoke.go_argv(self.parent), [str(self.parent / "scripts/go-test-with-cleanup.sh"),
                         "-json", "-count=1", "-timeout=3m30s", "-run", f"^{smoke.GO_TEST}$", "."])

    def test_both_flows_with_controlled_children_no_product_commands(self):
        actual_child = smoke.Child
        self.env["ORG_GRADLE_PROJECT_a3SwiftTestBridge"] = "true"
        for mode in ("go-direct-sync", "swift-cancellation", "swift-cancellation-state"):
            with self.subTest(mode=mode):
                run = self.make_run(mode)
                observed = []
                swift_environments = []

                def controlled(argv, cwd, env, log):
                    observed.append((argv, cwd))
                    code = "print('prepared')"
                    if argv[:2] == ["go", "build"]:
                        code = f"from pathlib import Path; Path({str(run.fixture)!r}).write_text('stub artifact')"
                    if argv[0].endswith("go-test-with-cleanup.sh"):
                        self.assertEqual(cwd, self.parent / "integration/mobile_flow")
                        code = f"print({chr(10).join(map(json.dumps, go_events()))!r})"
                    elif argv[0] == "swift" and argv[1] == "test":
                        swift_environments.append(dict(env))
                        xml = Path(argv[argv.index("--xunit-output") + 1])
                        names = ((smoke.SWIFT_CONFLICT_TEST,) if env.get("OVERSQLITE_A3_SWIFT_TEST_BRIDGE") else smoke.SWIFT_STATE_TESTS) if mode == "swift-cancellation-state" else (smoke.SWIFT_TEST,)
                        cls = smoke.SWIFT_STATE_CLASS if mode == "swift-cancellation-state" else smoke.SWIFT_CLASS
                        cases = ''.join(f'<testcase classname="SQLiteNowSyncSupportTests.{cls}" name="{name}"/>' for name in names)
                        native_xml = f'<testsuites><testsuite tests="{len(names)}">{cases}</testsuite></testsuites>'
                        native_log = ''.join(swift_log(test_class=cls, test=name) for name in names)
                        selector = argv[argv.index("--filter") + 1]
                        for name in names:
                            self.assertRegex(f"{cls}/{name}", selector)
                        code = f"from pathlib import Path; Path({str(xml)!r}).write_text({native_xml!r}); print({native_log!r})"
                    elif argv == [str(run.fixture)]:
                        code = "import signal,time; signal.signal(signal.SIGTERM,lambda *_:exit(0)); print('OVERSQLITE_SWIFT_CONVERGENT_BASE_URL=http://127.0.0.1:12345',flush=True); time.sleep(20)"
                    elif argv == [str(run.fixture), "--cleanup"]:
                        code = "print('cleanup completed')"
                    return actual_child([sys.executable, "-c", code], self.parent, env, log)

                with mock.patch.object(run, "preflight"), mock.patch.object(smoke, "Child", side_effect=controlled):
                    run.execute()
                    self.assertEqual(run.finish(None), 0)
                flat = [part for argv, _ in observed for part in argv]
                for forbidden in ("./...", "oversqlitePlatformAll", "swiftHybridSupportGate", "oversqliteRealserverAllHeavy", "--rerun-tasks"):
                    self.assertNotIn(forbidden, flat)
                self.assertEqual(sum(argv == [str(run.fixture), "--cleanup"] for argv, _ in observed), 1)
                if mode != "go-direct-sync":
                    preparations = [argv for argv, _ in observed if argv[0] == str(self.parent / "gradlew")]
                    expected = [[str(self.parent / "gradlew"), "-Pa3SwiftTestBridge=false", smoke.PACKAGE_TASK]]
                    if mode == "swift-cancellation-state":
                        expected.append([str(self.parent / "gradlew"), "-Pa3SwiftTestBridge=true", smoke.PACKAGE_TASK])
                    self.assertEqual(preparations, expected)
                if mode == "swift-cancellation":
                    swift = [argv for argv, _ in observed if argv[:2] == ["swift", "test"]]
                    self.assertEqual(len(swift), 1)
                    self.assertIn("--skip-build", swift[0])
                    self.assertIn("--parallel", swift[0])
                    self.assertEqual(swift[0][swift[0].index("--num-workers") + 1], "1")
                    self.assertIn("--verbose", swift[0])
                    self.assertIn("--disable-swift-testing", swift[0])
                    self.assertIn(f"{smoke.SWIFT_CLASS}/{smoke.SWIFT_TEST}", swift[0])
                    self.assertNotIn("OVERSYNC_SWIFT_A3_FIXTURE", swift_environments[0])
                    self.assertNotIn("OVERSQLITE_A3_SWIFT_TEST_BRIDGE", swift_environments[0])
                elif mode == "swift-cancellation-state":
                    self.assertEqual(len(swift_environments), 2)
                    self.assertTrue(all(e["OVERSYNC_SWIFT_A3_FIXTURE"] == "1" for e in swift_environments))
                    self.assertNotIn("OVERSQLITE_A3_SWIFT_TEST_BRIDGE", swift_environments[0])
                    self.assertEqual(swift_environments[1]["OVERSQLITE_A3_SWIFT_TEST_BRIDGE"], "1")
                    self.assertIn("-Pa3SwiftTestBridge=true", flat)
                    self.assertTrue((run.output / "swift-state-default/swift-tests.xml").exists())
                    self.assertTrue((run.output / "swift-state-conflict/swift-tests.xml").exists())

    def test_swift_modes_override_ambient_fixture_and_bridge_flags(self):
        self.env.update(OVERSYNC_SWIFT_A3_FIXTURE="1", OVERSQLITE_A3_SWIFT_TEST_BRIDGE="1")
        second = self.make_run("swift-cancellation")
        state = self.make_run("swift-cancellation-state")
        self.assertNotIn("OVERSYNC_SWIFT_A3_FIXTURE", second.env)
        self.assertNotIn("OVERSQLITE_A3_SWIFT_TEST_BRIDGE", second.env)
        self.assertEqual(state.env["OVERSYNC_SWIFT_A3_FIXTURE"], "1")
        self.assertNotIn("OVERSQLITE_A3_SWIFT_TEST_BRIDGE", state.env)

    def test_descendant_that_ignores_term_is_killed_and_fails_cleanup(self):
        marker = self.parent / "descendant.pid"
        child_code = f"import os,signal,time; from pathlib import Path; signal.signal(signal.SIGTERM,signal.SIG_IGN); Path({str(marker)!r}).write_text(str(os.getpid())); time.sleep(60)"
        parent_code = f"import subprocess,sys,time; subprocess.Popen([sys.executable,'-c',{child_code!r}]); time.sleep(60)"
        child = self.child(parent_code)
        deadline = time.monotonic() + 3
        while not marker.exists() and time.monotonic() < deadline:
            time.sleep(.02)
        self.assertTrue(marker.exists())
        with self.assertRaisesRegex(smoke.SmokeError, "SIGKILL"):
            child.terminate(.1)
        self.assertFalse(child.reader.is_alive())

    def test_cli_interruption_retains_evidence_and_joins_child(self):
        out = self.parent / "interrupted"
        child_pid = self.parent / "child.pid"
        code = f'''from tools.qualification import oversqlite_smoke as s
import sys
from pathlib import Path
def execute(self):
    self.command("controlled", [sys.executable, "-c", "import os,time; from pathlib import Path; Path({str(child_pid)!r}).write_text(str(os.getpid())); time.sleep(60)"], Path({str(self.parent)!r}), 60)
s.Run.execute = execute
raise SystemExit(s.main(["go-direct-sync", "--output", {str(out)!r}]))
'''
        process = subprocess.Popen([sys.executable, "-c", code], cwd=smoke.ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            deadline = time.monotonic() + 5
            while not child_pid.exists() and time.monotonic() < deadline:
                time.sleep(.02)
            self.assertTrue(child_pid.exists())
            process.send_signal(signal.SIGTERM)
            stdout, stderr = process.communicate(timeout=5)
            self.assertEqual(process.returncode, 1, stderr.decode())
            self.assertIn("FAILED", stdout.decode())
            self.assertIn("KeyboardInterrupt", (out / "summary.md").read_text())
            with self.assertRaises(ProcessLookupError):
                os.kill(int(child_pid.read_text()), 0)
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()


if __name__ == "__main__":
    unittest.main()

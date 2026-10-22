"""Fail-closed body accounting and owned-process cleanup controls."""
import json
from pathlib import Path
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from tools.qualification import oversqlite_platform as runner
from tools.qualification import platform_results as results
from tools.qualification.platform_resources import Resources, HTTPObserver, group_alive, read_private_readiness
from tools.qualification.oversqlite_smoke import SmokeError


class BodyAuditTests(unittest.TestCase):
    def replay(self):
        method = results.REPLAY_METHODS[0]
        identity = results.REPLAY_CLASS + "." + method
        cases = [dict(cls=results.REPLAY_CLASS, method=method, seconds=1, failed=False)]
        text = f"LIVE_REPLAY_COMMITTED native {identity} backlog=1000/width=16\nA3_ENTER native {identity}\nA3_SCENARIO native {identity} backlog=1000/width=16\nA3_COMPLETE native {identity}\n"
        return method, cases, text

    def test_replay_requires_exact_native_and_all_witnesses(self):
        method, cases, text = self.replay()
        self.assertEqual(results.audit(cases, text, "replay", "macos", [method])["bodies"], 1)
        lines = text.splitlines()
        web_concatenated = lines[1] + lines[0] + "".join(lines[2:])
        self.assertEqual(results.audit(cases, web_concatenated, "replay", "macos", [method])["bodies"], 1)
        for label, changed_cases, changed_text in (
            ("missing", [], text), ("duplicate", cases * 2, text),
            ("wrong", [dict(cases[0], method="other")], text),
            ("skipped", [dict(cases[0], failed=True)], text),
            ("no-terminal", cases, text.replace("A3_COMPLETE", "OTHER")),
            ("duplicate-terminal", cases, text + text),
            ("guarded", cases, text + "A3_NOT_EXECUTED native ignored opt_in_disabled"),
            ("wrong-runtime", cases, text.replace("native", "jvm")),
            ("wrong-workload", cases, text.replace("1000/width", "10000/width")),
            ("wrong-width-prefix", cases, text.replace("width=16", "width=160")),
        ):
            with self.subTest(label=label), self.assertRaises(SmokeError):
                results.audit(changed_cases, changed_text, "replay", "macos", [method])

    def test_verifier_nonbodies_get_no_execution_credit(self):
        for runtime in results.TASKS:
            cases, pieces = [], []
            nonbodies = results.expected_nonbodies(runtime)
            kind = results.KINDS[runtime]
            for method in results.VERIFIER_METHODS:
                cases.append(dict(cls=results.VERIFIER_CLASS, method=method, seconds=1, failed=False))
                if method in nonbodies:
                    pieces.append(f"F1_NOT_APPLICABLE {kind} {method} {nonbodies[method]}")
                else:
                    pieces.extend(f"{tag} {kind} {method} cleanup=complete" for tag in ("F1_ENTER", "F1_PASS"))
            # Browser reports can concatenate stdout without line separators.
            report = results.audit(cases, "".join(pieces), "verifier", runtime, list(results.VERIFIER_METHODS))
            self.assertEqual(report["bodies"], 9 - len(nonbodies))
            self.assertEqual(report["nonbodies"], len(nonbodies))
            if nonbodies:
                with self.assertRaises(SmokeError):
                    results.audit(cases, "".join(pieces).replace("F1_NOT_APPLICABLE", "OTHER"), "verifier", runtime, list(results.VERIFIER_METHODS))

    def test_native_reports_fail_on_stale_missing_and_normalize_family(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "TEST-control.xml"
            path.write_text('<testsuite><testcase classname="macosArm64Test.dev.test" name="method[macosArm64]" time="1.25"/><system-out>F1_ENTER native method</system-out></testsuite>')
            cases, stdout = results.native_results([path], 0, "macosArm64Test")
            self.assertEqual((cases[0]["cls"], cases[0]["method"], cases[0]["seconds"]), ("dev.test", "method", 1.25))
            self.assertIn("F1_ENTER", stdout)
            with self.assertRaises(SmokeError):
                results.native_results([path], time.time_ns() + 1_000_000, "macosArm64Test")
            with self.assertRaises(SmokeError):
                results.native_results([], 0, "jvmTest")

    def test_android_exact_selection_and_explicit_serial(self):
        env = {"ANDROID_SERIAL": "emulator-5570", "OVERSQLITE_PLATFORM_TESTS": "true"}
        command = runner.native_command(Path("/candidate"), "replay", "android", [results.REPLAY_METHODS[0]], env, "emulator-5570")
        self.assertIn("-Pandroid.testInstrumentationRunnerArguments.class=" + results.REPLAY_CLASS + "#" + results.REPLAY_METHODS[0], command)
        self.assertFalse(any("RunnerArguments.package=" in arg for arg in command))
        with self.assertRaises(SmokeError):
            runner.native_command(Path("/candidate"), "replay", "android", list(results.REPLAY_METHODS), env, "emulator-5570")
        with self.assertRaises(SmokeError):
            runner.native_command(Path("/candidate"), "verifier", "android", list(results.VERIFIER_METHODS), env, "physical-device")

    def test_reject_inherited_authority_and_frozen_drift(self):
        for key in ("OVERSQLITE_REALSERVER_TESTS", "SIMCTL_CHILD_OVERSQLITE_CATEGORIES_REAL_SERVER_SMOKE_BASE_URL", "TEST_DATABASE_URL"):
            with self.assertRaises(SmokeError):
                runner.clean_environment({key: "value"})
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "freeze.json"
            path.write_text('{"files": {"relative": "bad"}}')
            with self.assertRaises(SmokeError):
                runner.verify_freeze(path)


    def test_parent_process_tag_is_rebound_without_admitting_sdk_activation(self):
        env=runner.clean_environment({'PATH':'/declared','OVERSQLITE_PROCESS_OWNER':'parent',
            'SIMCTL_CHILD_OVERSQLITE_PROCESS_OWNER':'parent'})
        self.assertNotIn('OVERSQLITE_PROCESS_OWNER',env)
        self.assertNotIn('SIMCTL_CHILD_OVERSQLITE_PROCESS_OWNER',env)
        with self.assertRaises(SmokeError):runner.clean_environment({'OVERSQLITE_PROCESS_OWNER':'parent','OVERSQLITE_REALSERVER_TESTS':'true'})


class LifecycleTests(unittest.TestCase):
    def test_reaped_pid_cannot_seed_unrelated_process_or_descendant(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);resource=Resources(root,root,root,{})
            resource.children=[SimpleNamespace(proc=SimpleNamespace(pid=4417,poll=lambda:0))]
            protected=next(iter(resource.protected_groups))
            raw=f'4417 1 {protected} unrelated-command\n4418 4417 9000 unrelated-child'
            with patch.object(resource,'inspect',return_value=raw):resource.capture_groups()
            self.assertEqual(resource.groups,set())
            self.assertNotIn('process_census_refusals',resource.record)

    def test_mixed_time_registered_probe_group_is_rechecked(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);resource=Resources(root,root,root,{})
            proc=SimpleNamespace(pid=5254,poll=lambda:None)
            resource.inspections=[SimpleNamespace(proc=proc)]
            protected=next(iter(resource.protected_groups))
            raw=f'5254 4176 {protected} private-command OVERSQLITE_PROCESS_OWNER={resource.run_id}'
            with patch.object(resource,'inspect',return_value=raw),patch('tools.qualification.platform_resources.os.getpgid',return_value=5254):
                resource.capture_groups()
            self.assertEqual(resource.groups,{5254})
            self.assertEqual(resource.record['process_census_rechecks'][0]['snapshot_group'],protected)
            self.assertEqual(resource.record['process_census_rechecks'][0]['current_group'],5254)

    def test_current_protected_group_still_refused_with_redacted_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);resource=Resources(root,root,root,{})
            resource.children=[SimpleNamespace(proc=SimpleNamespace(pid=5254,poll=lambda:None))]
            protected=next(iter(resource.protected_groups))
            raw=f'5254 4176 5254 private-secret OVERSQLITE_PROCESS_OWNER={resource.run_id}'
            with patch.object(resource,'inspect',return_value=raw),patch('tools.qualification.platform_resources.os.getpgid',return_value=protected):
                with self.assertRaisesRegex(SmokeError,'refuse caller/non-owned') as error:resource.capture_groups()
            self.assertNotIn('private-secret',str(error.exception))
            self.assertEqual(resource.record['process_census_refusals'][0]['pid'],5254)
            self.assertTrue(resource.record['process_census_refusals'][0]['registered_live'])
            self.assertNotIn(protected,resource.groups)

    def test_registered_process_exiting_during_group_recheck_is_not_adopted(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);resource=Resources(root,root,root,{})
            states=iter([None,0])
            resource.children=[SimpleNamespace(proc=SimpleNamespace(pid=5254,poll=lambda:next(states)))]
            raw=f'5254 4176 5254 command OVERSQLITE_PROCESS_OWNER={resource.run_id}'
            with patch.object(resource,'inspect',return_value=raw),patch('tools.qualification.platform_resources.os.getpgid',return_value=5254):resource.capture_groups()
            self.assertEqual(resource.groups,set())

    def test_deferred_census_reaps_escaped_child_and_preserves_unowned_group(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            resource = Resources(root, root, root, dict(os.environ))
            outsider = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(30)"], start_new_session=True)
            original = resource.inspect
            def inspect(argv, seconds=10, **kwargs):
                return "" if argv[0] == "docker" else original(argv, seconds, **kwargs)
            try:
                program = "import json,subprocess,sys;from pathlib import Path;p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)'],start_new_session=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);Path(sys.argv[1]).write_text(json.dumps({'pid':p.pid}))"
                child = resource.launch([sys.executable, "-c", program, str(root / "ready.json")], root / "child.log")
                self.assertEqual(resource.wait(child, 5, capture=False), 0)
                escaped = json.loads((root / "ready.json").read_text())["pid"]
                with patch.object(resource, "inspect", side_effect=inspect):
                    cleanup = resource.cleanup()
                self.assertIn(escaped, cleanup["groups"])
                self.assertEqual(cleanup["errors"], [])
                self.assertTrue(cleanup["groups_absent"])
                self.assertIsNone(outsider.poll())
            finally:
                if outsider.poll() is None:
                    outsider.terminate(); outsider.wait(timeout=3)

    def test_process_descendants_join_and_unowned_process_survives(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            resource = Resources(root, root, root, dict(os.environ))
            outsider = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(30)"], start_new_session=True)
            try:
                code = "import subprocess,sys,time;subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)']);time.sleep(30)"
                child = resource.launch([sys.executable, "-c", code], root / "child.log")
                time.sleep(.2)
                resource.capture_groups()
                original = resource.inspect
                def inspect(argv, seconds=10, **kwargs):
                    return "" if argv[0] == "docker" else original(argv, seconds, **kwargs)
                with patch.object(resource, "inspect", side_effect=inspect):
                    receipt = resource.cleanup()
                self.assertEqual(receipt["errors"], [])
                self.assertTrue(receipt["groups_absent"])
                self.assertFalse(group_alive(child.proc.pid))
                self.assertIsNone(outsider.poll())
            finally:
                os.killpg(outsider.pid, signal.SIGTERM)
                outsider.wait(timeout=3)

    def test_detached_reparented_descendant_is_owned_after_parent_exits(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            resource = Resources(root, root, root, dict(os.environ))
            pidfile = root / "detached.pid"
            grandchild = "import time;time.sleep(30)"
            code = f"import subprocess,sys,pathlib;p=subprocess.Popen([sys.executable,'-c',{grandchild!r}],start_new_session=True);pathlib.Path({str(pidfile)!r}).write_text(str(p.pid))"
            child = resource.launch([sys.executable, "-c", code], root / "child.log")
            child.proc.wait(timeout=3)  # Descendant intentionally retains stdout until cleanup.
            pid = int(pidfile.read_text())
            original = resource.inspect
            def inspect(argv, seconds=10, **kwargs):
                return "" if argv[0] == "docker" else original(argv, seconds, **kwargs)
            try:
                with patch.object(resource, "inspect", side_effect=inspect):
                    receipt = resource.cleanup()
                self.assertEqual(receipt["errors"], [])
                self.assertIn(pid, receipt["groups"])
                self.assertFalse(group_alive(pid))
            finally:
                if group_alive(pid):
                    os.killpg(pid, signal.SIGKILL)

    def test_ios_cleanup_deletes_device_then_recovers_escaped_tagged_group(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            resource = Resources(root, root, root, dict(os.environ))
            resource.ios_name = "oversqlite-controlled"
            pidfile = root / "escaped.pid"
            code = f"import subprocess,sys,pathlib;p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)'],start_new_session=True);pathlib.Path({str(pidfile)!r}).write_text(str(p.pid))"
            child = resource.launch([sys.executable, "-c", code], root / "child.log")
            child.proc.wait(timeout=3)
            pid = int(pidfile.read_text())
            deleted = False
            events = []
            original_inspect = resource.inspect
            original_capture = resource.capture_groups
            def inspect(argv, seconds=10, **kwargs):
                nonlocal deleted
                if argv[:3] == ["xcrun", "simctl", "delete"]:
                    deleted = True
                    events.append("delete")
                    return ""
                return "" if argv[0] == "docker" else original_inspect(argv, seconds, **kwargs)
            def capture(deadline=None):
                self.assertTrue(deleted, "global census must follow simulator deletion")
                events.append("census")
                return original_capture(deadline)
            device = {"udid": "controlled-udid", "state": "Shutdown", "name": resource.ios_name}
            try:
                with patch.object(resource, "inspect", side_effect=inspect), patch.object(resource, "capture_groups", side_effect=capture), patch.object(resource, "owned_simulators", side_effect=[[device], []]):
                    receipt = resource.cleanup()
                self.assertEqual(receipt["errors"], [])
                self.assertEqual(events, ["delete", "census", "census"])
                self.assertIn(pid, receipt["groups"])
                self.assertTrue(receipt["groups_absent"])
                self.assertFalse(group_alive(pid))
                self.assertLess(receipt["elapsed_seconds"], 30)
            finally:
                if group_alive(pid):
                    os.killpg(pid, signal.SIGKILL)

    def test_uncertain_simulator_create_reconciles_owned_name(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            resource = Resources(root, root, root, dict(os.environ))
            with patch.object(resource, "inspect", side_effect=subprocess.TimeoutExpired("simctl", 60)):
                with self.assertRaises(subprocess.TimeoutExpired):
                    resource.ios_device()
            self.assertTrue(resource.ios_name.startswith("oversqlite-"))
            self.assertIn(resource.ios_name, (root / "ownership.json").read_text())
            calls = []
            def inspect(argv, seconds=10, **kwargs):
                calls.append(argv)
                return ""
            device = {"udid": "controlled-udid", "state": "Shutdown", "name": resource.ios_name}
            with patch.object(resource, "inspect", side_effect=inspect), patch.object(resource, "capture_groups"), patch.object(resource, "owned_simulators", side_effect=[[device], []]):
                receipt = resource.cleanup()
            self.assertEqual(receipt["errors"], [])
            self.assertIn(["xcrun", "simctl", "delete", "controlled-udid"], calls)
            filters = [argv[argv.index("--filter") + 1] for argv in calls if "--filter" in argv]
            self.assertEqual(filters, ["label=com.mobiletoly.go-oversync.test-run=" + resource.run_id] * 2)

    def test_ios_boot_wait_preserves_device_ownership_without_global_census(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            resource = Resources(root, root, root, dict(os.environ))
            udid = "12345678-1234-1234-1234-123456789ABC"
            with patch.object(resource, "inspect", side_effect=[udid, ""]), patch.object(resource, "command") as command:
                self.assertEqual(resource.ios_device(), udid)
            self.assertEqual(command.call_args.args, (["xcrun", "simctl", "bootstatus", udid, "-b"], 120, "ios-boot"))
            self.assertEqual(command.call_args.kwargs, {"capture": False})
            self.assertIn(resource.ios_name, (root / "ownership.json").read_text())

    def test_process_inspection_cannot_extend_shared_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            resource = Resources(root, root, root, dict(os.environ))
            with patch("tools.qualification.platform_resources.time.monotonic", return_value=100), patch.object(resource, "inspect", return_value="") as inspect:
                with self.assertRaises(SmokeError):
                    resource.capture_groups(99)
                inspect.assert_not_called()
                resource.capture_groups(100.25)
                self.assertEqual(inspect.call_args.args[1], .25)

    def test_signal_interrupt_joins_real_owned_child(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "attempt"
            code = """
import sys,time
from pathlib import Path
from tools.qualification import oversqlite_platform as runner
from tools.qualification.platform_resources import Resources
original = Resources.inspect
def inspect(self, argv, seconds=10, **kwargs):
    return '' if argv[0] == 'docker' else original(self, argv, seconds, **kwargs)
Resources.inspect = inspect
def execute(options, resources, record):
    resources.launch([sys.executable, '-c', 'import time;time.sleep(30)'], resources.output/'child.log')
    (resources.output/'ready').write_text('ready')
    time.sleep(30)
runner.execute = execute
sys.exit(runner.main(['prepare', '--output', sys.argv[1]]))
"""
            with (Path(directory) / "coordinator.log").open("w") as log:
                parent = subprocess.Popen([sys.executable, "-B", "-c", code, str(output)], stdout=log, stderr=subprocess.STDOUT)
                try:
                    deadline = time.monotonic() + 5
                    while not (output / "ready").exists() and parent.poll() is None:
                        self.assertLess(time.monotonic(), deadline)
                        time.sleep(.02)
                    os.kill(parent.pid, signal.SIGTERM)
                    self.assertEqual(parent.wait(timeout=15), 1)
                    receipt = json.loads((output / "receipt.json").read_text())
                    self.assertEqual(receipt["cleanup"]["errors"], [])
                    self.assertTrue(receipt["cleanup"]["groups_absent"])
                    self.assertIn("InterruptedError", receipt["errors"][0])
                finally:
                    if parent.poll() is None:
                        parent.kill(); parent.wait(timeout=3)

    def test_interruption_records_failure_and_runs_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "attempt"
            with patch.object(runner, "execute", side_effect=InterruptedError("controlled interruption")), patch.object(Resources, "cleanup", return_value={"errors": [], "groups_absent": True}) as cleanup:
                code = runner.main(["prepare", "--output", str(output)])
            self.assertEqual(code, 1)
            self.assertEqual(cleanup.call_count, 1)
            self.assertIn("controlled interruption", (output / "receipt.json").read_text())

    def test_cleanup_failure_cannot_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "attempt"
            with patch.object(runner, "execute"), patch.object(Resources, "cleanup", return_value={"errors": ["owned group remains"]}):
                self.assertEqual(runner.main(["prepare", "--output", str(output)]), 1)


class FullEntryAuditTests(unittest.TestCase):
    def test_named_isolation_preparation_preserves_actual_worker_flags(self):
        import argparse
        profile=dict(roots=dict(kotlin='/canonical/kotlin',go='/canonical/go'),schedule=dict(entries={
            'retained-isolation-browser':dict(environment={'OVERSQLITE_REALSERVER_HEAVY':'true','GRADLE_USER_HOME':'binding.worker_browser_gradle_home'})}))
        entry=dict(kind='preparation',argv=['/canonical/go/wrapper','270','--','./gradlew',':compile'],cwd='/canonical/kotlin',
            environment={},environment_contexts={'coordinated-live-browser':{'OVERSQLITE_REALSERVER_HEAVY':'false'}})
        options=argparse.Namespace(root=Path('/canonical/kotlin'),go_repo=Path('/canonical/go'),context='isolation-browser')
        resolved=runner.rebase_profile_entry(entry,profile,options,{})
        self.assertEqual(resolved['environment']['OVERSQLITE_REALSERVER_HEAVY'],'true')
        self.assertEqual(resolved['environment']['GRADLE_USER_HOME'],'binding.worker_browser_gradle_home')
        self.assertIn('-Pkonan.data.dir=binding.worker_browser_konan',resolved['argv'])
        self.assertIn('-Pkotlin.compiler.execution.strategy=in-process',resolved['argv'])

    def test_jvm_preparation_closes_the_correct_runtime_classpaths(self):
        entry=dict(task_ids=[':library-core:compileTestKotlinJvm',':library-core:compileJvmTestJava',
            ':platform-core-test:harness:compileTestKotlinJvm',':library-core:compileTestKotlinJs'])
        self.assertEqual(runner.preparation.jvm_preparation_runtime_tasks(entry),
            (':library-core:jvmTest',':platform-core-test:harness:jvmTest'))

    def test_context_preparation_keeps_owning_tasks_and_all_native_exclusions(self):
        import argparse
        core=':library-core:compileTestKotlinJvm'
        unrelated=':swift:fixture:package'
        profile=dict(roots=dict(kotlin='/canonical/kotlin',go='/canonical/go'),schedule=dict(entries={
            'consumer':dict(context='core-jvm',preparation_tasks=[core])}))
        entry=dict(kind='preparation',task_ids=[core,unrelated],argv=['wrapper','270','--','./gradlew',core,unrelated,'-x',':library-core:jvmTest'],
            cwd='/canonical/kotlin',environment={},environment_contexts={'core-jvm':{}})
        options=argparse.Namespace(root=Path('/canonical/kotlin'),go_repo=Path('/canonical/go'),context='core-jvm')
        resolved=runner.rebase_profile_entry(entry,profile,options,{})
        self.assertEqual(resolved['task_ids'],[core])
        self.assertEqual(resolved['catalog_group_task_ids'],[core,unrelated])
        self.assertNotIn(unrelated,resolved['argv'])
        self.assertEqual(resolved['argv'][-2:],['-x',':library-core:jvmTest'])

    def test_runtime_classpath_metadata_runs_under_existing_owning_help_action(self):
        import tempfile
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory)
            args=runner.preparation.instrument_full(['./gradlew',':library-core:compileTestKotlinJvm'],output,
                prepare_only=True,runtime_tasks=(':library-core:jvmTest',))
            self.assertIn(':library-core:help',args)
            script=(output/'full-preparation.init.gradle').read_text()
            self.assertLess(script.index('runtimeClasspaths[target]'),script.index('gradle.taskGraph.whenReady'))
            self.assertIn("owner.tasks.named('help').configure",script)
            self.assertNotIn('def runtimeClasspaths = []',script)

    def test_node_nonbody_policy_cannot_be_applied_to_browser_or_wasm(self):
        owner='dev.goquick.sqlitenow.core.worker.SqliteWorkerDriverTest.browserLifecycleBusyIsTypedAndWorkerTerminationReleasesOwnership'
        policy={owner:dict(kind='guarded-nonbody',runtime='node',reason='Source browser-runtime guard returns on Node')}
        results.validate_body_policies(dict(runtime='node',expected_source_owners=[owner]),policy)
        for runtime in ('browser','wasm'):
            with self.subTest(runtime=runtime),self.assertRaises(SmokeError):
                results.validate_body_policies(dict(runtime=runtime,expected_source_owners=[owner]),policy)

    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.path=Path(self.temp.name)/'TEST-control.xml'
        self.owner='dev.control.Owner.check'
        self.entry=dict(task=':core:jvmTest',runtime='jvm',expected_source_owners=[self.owner])
        self.policy={self.owner:dict(kind='unconditional',reason='controlled source has no admission guard')}
        self.xml()

    def xml(self,name='check[jvm]',extra='',duration='1'):
        self.path.write_text(f'<testsuite><testcase classname="dev.control.Owner" name="{name}" time="{duration}">{extra}</testcase></testsuite>')

    def test_exact_native_display_suffix_maps_to_the_source_owner(self):
        report=results.audit_full_entry([self.path],0,self.entry,self.policy)
        self.assertEqual(report['bodies'],1)
        self.assertEqual(report['cases'][0]['native_name'],'check[jvm]')
        for name in('check[unknown]','checkMore[jvm]'):
            self.xml(name)
            with self.assertRaises(SmokeError):results.audit_full_entry([self.path],0,self.entry,self.policy)

    def test_swift_capture_binds_exact_module_method_and_new_report(self):
        entry=dict(self.entry,runtime='Swift/macOS')
        policy={self.owner:dict(self.policy[self.owner],native_class='KnownModule.Owner')}
        self.path.write_text('<testsuite><testcase classname="KnownModule.Owner" name="check()" time=".1"/></testsuite>')
        self.assertEqual(results.audit_full_entry([self.path],0,entry,policy)['bodies'],1)
        with self.assertRaises(SmokeError):results.audit_full_entry([self.path],0,entry,self.policy)
        root=Path(self.temp.name);task=':swift:runtime:core:swiftTest';script=root/'capture.gradle'
        args=['wrapper','270','--','./gradlew',task,'--no-parallel']
        got=runner.swift_capture_argv(args,task,root,script)
        self.assertEqual(got[:len(args)],args)
        self.assertIn('-Pqualification.swiftTask='+task,got)
        self.assertNotIn('--filter',got)
        for changed in(args+['-Pqualification.swiftTask=:wrong'],args+['-Pqualification.swiftXunit=/unowned']):
            with self.assertRaises(SmokeError):runner.swift_capture_argv(changed,task,root,script)
        (root/'native-swift.xml').write_text('stale')
        with self.assertRaises(SmokeError):runner.swift_capture_argv(args,task,root,script)

    def test_browser_display_metadata_is_runtime_bound_and_keeps_exact_owner(self):
        for runtime,target in [('browser','js'),('wasm','wasmJs')]:
            entry=dict(self.entry,runtime=runtime,task=':core:'+('jsBrowserTest' if runtime=='browser' else 'wasmJsBrowserTest'))
            raw='check['+target+', browser, ChromeHeadless156.0.0.0, MacOS10.15.7]'
            self.xml(raw)
            got=results.audit_full_entry([self.path],0,entry,self.policy)
            self.assertEqual(got['bodies'],1)
            self.assertEqual(got['cases'][0]['native_name'],raw)
            for bad in (raw.replace(target,'jvm',1),raw.replace('browser','node',1),
                        raw.replace('check[','checkMore['),raw.replace('MacOS10.15.7',''),raw+'extra'):
                self.xml(bad)
                with self.subTest(runtime=runtime,name=bad),self.assertRaises(SmokeError):
                    results.audit_full_entry([self.path],0,entry,self.policy)

    def test_missing_duplicate_failed_skipped_and_unclassified_cannot_pass(self):
        with self.assertRaises(SmokeError):results.audit_full_entry([],0,self.entry,self.policy)
        with self.assertRaises(SmokeError):results.audit_full_entry([self.path,self.path],0,self.entry,self.policy)
        with self.assertRaises(SmokeError):results.audit_full_entry([self.path],0,self.entry,{})
        for child in('<failure/>','<error/>','<skipped/>'):
            self.xml(extra=child)
            with self.assertRaises(SmokeError):results.audit_full_entry([self.path],0,self.entry,self.policy)
        self.xml(duration='NaN')
        with self.assertRaises(SmokeError):results.audit_full_entry([self.path],0,self.entry,self.policy)

    def test_serial_swift_native_events_require_exact_complete_unskipped_cases(self):
        entry=dict(self.entry,id='swift-generated-live-only',runtime='Swift/macOS',task=':swift:fixtures:package:sync:swiftRealserverSmoke')
        policy={self.owner:dict(self.policy[self.owner],native_class='KnownModule.Owner')}
        start="Test Case '-[KnownModule.Owner check]' started."
        end="Test Case '-[KnownModule.Owner check]' passed (0.1 seconds)."
        text='> Task '+entry['task']+'\n'+start+'\n'+end+'\n Executed 1 test, with 0 failures (0 unexpected) in 0.1 seconds\n' + "Test Suite 'Selected tests' passed at 2026-10-27 00:00:00.000.\n"
        report=results.audit_full_entry([],0,entry,policy,native_stdout=text)
        self.assertEqual(report['bodies'],1)
        for bad in (text.replace(end,''),text.replace(start,start+'\n'+start),
                    text.replace('passed','failed'),text.replace('passed','skipped'),
                    text.replace('Executed 1','Executed 2'),text.replace('KnownModule.Owner','OtherModule.Owner'),
                    text.replace(entry['task'],':wrong'),text+text,text.replace("Test Suite 'Selected tests' passed", "Test Suite 'Selected tests' unfinished")):
            with self.subTest(text=bad),self.assertRaises(SmokeError):
                results.audit_full_entry([],0,entry,policy,native_stdout=bad)
        other_start=start.replace(' check]',' other]');other_end=end.replace(' check]',' other]')
        overlapping='> Task '+entry['task']+'\n'+'\n'.join([start,other_start,end,other_end])+'\n Executed 2 tests, with 0 failures (0 unexpected)\n'+"Test Suite 'Selected tests' passed at 2026-10-27 00:00:00.000.\n"
        with self.assertRaisesRegex(SmokeError,'overlapping'):
            results._serial_swift_tree(overlapping,entry['task'])

    def test_explicit_guard_is_retained_with_zero_body_credit(self):
        self.xml(extra='<skipped/>')
        policy={self.owner:dict(kind='guarded-nonbody',reason='opt-in absent in this exact neutral context')}
        report=results.audit_full_entry([self.path],0,self.entry,policy)
        self.assertEqual((report['bodies'],report['nonbodies']),(0,1))

    def test_witness_policy_requires_nonempty_requirements_and_exact_emission(self):
        self.xml()
        policy={self.owner:dict(kind='witness',reason='controlled guarded body needs an execution witness')}
        for markers in(None,[],[''],['ENTER','ENTER']):
            if markers is not None:policy[self.owner]['required_markers']=markers
            with self.assertRaises(SmokeError):results.audit_full_entry([self.path],0,self.entry,policy)
        policy[self.owner]['required_markers']=['CONTROL_ENTER','CONTROL_PASS']
        with self.assertRaises(SmokeError):results.audit_full_entry([self.path],0,self.entry,policy)
        text=self.path.read_text().replace('</testsuite>','<system-out>CONTROL_ENTER CONTROL_PASS</system-out></testsuite>')
        self.path.write_text(text)
        self.assertEqual(results.audit_full_entry([self.path],0,self.entry,policy)['bodies'],1)
        self.path.write_text(text.replace('CONTROL_PASS','CONTROL_PASS CONTROL_PASS'))
        with self.assertRaises(SmokeError):results.audit_full_entry([self.path],0,self.entry,policy)

    def test_go_requires_the_exact_predeclared_owners_and_package_completion(self):
        self.path=Path(self.temp.name)/'go.json'
        owner=dict(package='example/p',test='TestControl',reason='unconditional controlled case')
        events=[dict(Action='run',Package='example/p',Test='TestControl'),dict(Action='pass',Package='example/p',Test='TestControl'),dict(Action='pass',Package='example/p')]
        self.path.write_text('\n'.join(json.dumps(x)for x in events))
        self.assertEqual(results.audit_full_go(self.path,[owner])['bodies'],1)
        for broken in(events[:-1],events+[events[1]],events[1:]):
            self.path.write_text('\n'.join(json.dumps(x)for x in broken))
            with self.assertRaises(SmokeError):results.audit_full_go(self.path,[owner])

    def test_go_fuzz_framework_pass_needs_an_executed_passing_seed(self):
        owner=dict(package='example/p',test='FuzzControl',reason='current source seed corpus')
        events=[dict(Action='run',Package='example/p',Test='FuzzControl'),dict(Action='pass',Package='example/p',Test='FuzzControl'),dict(Action='pass',Package='example/p')]
        self.path.write_text('\n'.join(json.dumps(row)for row in events))
        with self.assertRaises(SmokeError):results.audit_full_go(self.path,[owner])
        events[1:1]=[dict(Action=action,Package='example/p',Test='FuzzControl/seed#0')for action in('run','pass')]
        self.path.write_text('\n'.join(json.dumps(row)for row in events))
        self.assertEqual(results.audit_full_go(self.path,[owner])['bodies'],1)
    def test_expired_resource_budget_does_not_start_a_process(self):
        resource=Resources(Path(self.temp.name),Path(self.temp.name),Path(self.temp.name),{})
        resource.command_deadline=time.monotonic()-1
        with patch.object(runner.smoke,'Child')as child:
            with self.assertRaises(SmokeError):resource.launch(['never'],Path(self.temp.name)/'log')
            self.assertFalse(child.called)

    def test_go_subtest_skip_and_partial_events_cannot_hide_under_a_passing_parent(self):
        owner=dict(package='example/p',test='TestControl',reason='source owns this test')
        child=[dict(Action='run',Package='example/p',Test='TestControl/capability'),
               dict(Action='skip',Package='example/p',Test='TestControl/capability')]
        events=[dict(Action='run',Package='example/p',Test='TestControl'),*child,
                dict(Action='pass',Package='example/p',Test='TestControl'),dict(Action='pass',Package='example/p')]
        self.path.write_text('\n'.join(json.dumps(x)for x in events))
        with self.assertRaises(SmokeError):results.audit_full_go(self.path,[owner])
        declared=dict(owner,subtest_nonbodies={'TestControl/capability':'explicit absent capability in frozen source'})
        audit=results.audit_full_go(self.path,[declared])
        self.assertEqual(audit['bodies'],1)
        self.assertEqual(audit['subtests'][0]['body_credit'],0)
        for broken in(events[:2]+events[3:],events[:1]+events[2:],events+child):
            self.path.write_text('\n'.join(json.dumps(x)for x in broken))
            with self.assertRaises(SmokeError):results.audit_full_go(self.path,[declared])

    def test_actual_go_engine_version_and_layout_are_explicit(self):
        from unittest.mock import Mock
        resource=Mock(env={'PATH':'/declared'})
        resource.inspect.return_value='go1.25.12\n/toolchain/pkg/tool/darwin_arm64'
        with patch.object(runner.shutil,'which',return_value='/launcher/go'):
            files,identity=runner.full_go_runtime(resource)
            self.assertEqual(files[1:],[Path('/toolchain/bin/go'),Path('/toolchain/VERSION')])
            self.assertEqual(identity['version'],'go1.25.12')
            for invalid in('go1.26.0\n/toolchain/pkg/tool/darwin_arm64','go1.25.12\nrelative','go1.25.12','go1.25.12\n/','go1.25.12\n/toolchain/wrong/tools/arm64'):
                resource.inspect.return_value=invalid
                with self.subTest(value=invalid),self.assertRaises(SmokeError):runner.full_go_runtime(resource)

    def test_terminal_build_binding_rejects_cleanup_mutation_absence_and_replacement(self):
        for change in ('none','mutate','remove','symlink','mode'):
            output=Path(self.temp.name)/change
            def execute(options,resource,record):
                binary=resource.output/'built';binary.write_bytes(b'compiled');binary.chmod(0o755)
                record.update(freeze_sha256='frozen',job_deadline=time.monotonic()+30,
                    before={str(x):'stable'for x in(resource.root,resource.go)},
                    build_artifact=dict(path=str(binary),sha256=runner.smoke.sha(binary.read_bytes()),mode=0o755))
            def cleanup(resource):
                binary=resource.output/'built'
                if change=='mutate':binary.write_bytes(b'replaced')
                elif change=='remove':binary.unlink()
                elif change=='symlink':
                    target=resource.output/'elsewhere';binary.rename(target);binary.symlink_to(target)
                elif change=='mode':binary.chmod(0o644)
                return dict(errors=[],groups_absent=True)
            with self.subTest(change=change),patch.object(runner,'execute',side_effect=execute),\
                    patch.object(Resources,'cleanup',autospec=True,side_effect=cleanup),\
                    patch.object(runner,'verify_freeze',return_value='frozen'),\
                    patch.object(runner.smoke,'identity',return_value='stable'):
                code=runner.main(['lane','--profile','/profile','--entry','load-prepare-2',
                    '--freeze','/freeze','--output',str(output)])
                self.assertEqual(code,0 if change=='none'else 1)
                receipt=json.loads((output/'receipt.json').read_text())
                self.assertEqual(receipt['status'],'PASS'if change=='none'else'FAIL')

    def test_expired_parent_budget_stops_terminal_binding_before_inspection(self):
        output=Path(self.temp.name)/'expired-terminal'
        def execute(options,resource,record):
            record.update(freeze_sha256='frozen',job_deadline=time.monotonic()-1,before={})
        with patch.object(runner,'execute',side_effect=execute),\
                patch.object(Resources,'cleanup',return_value=dict(errors=[],groups_absent=True)),\
                patch.object(runner,'verify_freeze')as verify,patch.object(runner,'profile_source_identities')as identities:
            code=runner.main(['lane','--profile','/profile','--entry','control','--freeze','/freeze','--output',str(output)])
        self.assertEqual(code,1);self.assertFalse(verify.called);self.assertFalse(identities.called)

    def test_expired_cleanup_still_signals_registered_groups(self):
        root=Path(self.temp.name)
        owned=subprocess.Popen([sys.executable,'-c','import signal;signal.pause()'],start_new_session=True)
        unrelated=subprocess.Popen([sys.executable,'-c','import signal;signal.pause()'],start_new_session=True)
        try:
            resource=Resources(root,root,root,{})
            resource.groups.add(owned.pid)
            errors=[]
            began=time.monotonic()
            resource.stop_groups(began-1,errors)
            self.assertLess(time.monotonic()-began,1)
            self.assertIn(owned.wait(timeout=2),(-signal.SIGTERM,-signal.SIGKILL))
            self.assertIsNone(unrelated.poll())
        finally:
            for process in(owned,unrelated):
                if process.poll()is None:
                    os.killpg(process.pid,signal.SIGKILL)
                process.wait(timeout=2)

    def test_explicit_inspection_cwd_is_honored(self):
        root=Path(self.temp.name);nested=root/'nested';nested.mkdir()
        resource=Resources(root,root,root,dict(os.environ))
        self.assertEqual(Path(resource.inspect([sys.executable,'-c','import os;print(os.getcwd())'],cwd=nested)).resolve(),nested.resolve())

    def test_inspection_timeout_registers_and_reaps_only_its_own_group(self):
        root=Path(self.temp.name);resource=Resources(root,root,root,dict(os.environ))
        unrelated=subprocess.Popen([sys.executable,'-c','import signal;signal.pause()'],start_new_session=True)
        try:
            began=time.monotonic()
            with self.assertRaises(subprocess.TimeoutExpired):
                resource.inspect([sys.executable,'-c','import signal;signal.signal(signal.SIGTERM,signal.SIG_IGN);signal.pause()'],seconds=.25)
            self.assertLess(time.monotonic()-began,1)
            self.assertEqual(len(resource.inspections),1)
            probe=resource.inspections[0]
            self.assertIn(probe.proc.pid,resource.groups)
            self.assertEqual(probe.proc.wait(timeout=2),-signal.SIGKILL)
            for reader in probe.readers:reader.join(timeout=1)
            self.assertFalse(any(reader.is_alive()for reader in probe.readers))
            self.assertIsNone(unrelated.poll())
        finally:
            if unrelated.poll()is None:os.killpg(unrelated.pid,signal.SIGKILL)
            unrelated.wait(timeout=2)

    def test_inspection_preserves_private_input_and_separates_stderr(self):
        root=Path(self.temp.name);resource=Resources(root,root,root,dict(os.environ))
        source='import sys,json;value=sys.stdin.read();sys.stderr.write("diagnostic\\n");print(json.dumps({"value":value}))'
        self.assertEqual(json.loads(resource.inspect([sys.executable,'-c',source],seconds=2,input_text='private input')),{'value':'private input'})
        self.assertEqual(resource.inspections[0].proc.returncode,0)

    def test_readiness_probe_retains_not_ready_status_but_inspection_rejects_it(self):
        root=Path(self.temp.name);resource=Resources(root,root,root,dict(os.environ))
        argv=[sys.executable,'-c','import sys;sys.stderr.write("not ready\\n");sys.exit(2)']
        probe=resource.probe(argv,seconds=2)
        self.assertEqual(probe.proc.returncode,2)
        self.assertIn(probe.proc.pid,resource.groups)
        with self.assertRaises(SmokeError):resource.inspect(argv,seconds=2)

    def test_inspection_is_registered_before_pipe_start_can_be_interrupted(self):
        root=Path(self.temp.name);resource=Resources(root,root,root,dict(os.environ))
        with patch('tools.qualification.platform_resources.Inspection.start',side_effect=InterruptedError('controlled registration interruption')):
            with self.assertRaises(InterruptedError):resource.inspect([sys.executable,'-c','import signal;signal.pause()'])
        self.assertEqual(len(resource.inspections),1)
        probe=resource.inspections[0]
        self.assertIn(probe.proc.pid,resource.groups)
        self.assertEqual(probe.proc.wait(timeout=2),-signal.SIGKILL)

    def test_census_cannot_adopt_the_launchers_process_group(self):
        root=Path(self.temp.name);resource=Resources(root,root,root,{})
        parent=os.getpgid(os.getppid())
        row=f'12345 1 {parent} command OVERSQLITE_PROCESS_OWNER={resource.run_id}'
        with patch.object(resource,'inspect',return_value=row):
            with self.assertRaises(SmokeError):resource.capture_groups()
        self.assertNotIn(parent,resource.groups)

    def test_private_root_rebinding_is_explicit_and_preserves_adjacent_paths(self):
        import argparse
        root=Path(self.temp.name)
        options=argparse.Namespace(root=root/'worker-kotlin',go_repo=root/'worker-go')
        profile=dict(roots=dict(kotlin='/canonical/kotlin',go='/canonical/go'))
        entry=dict(argv=['/canonical/go/scripts/run-with-timeout.pl','1','--','/canonical/kotlin/gradlew','/canonical/go-adjacent/file'],cwd='/canonical/kotlin',environment={'GO_OVERSYNC_REPO':'/canonical/go'})
        with self.assertRaises(SmokeError):runner.rebase_profile_entry(entry,profile,options,{})
        mapping={original:str((options.root if name=='kotlin'else options.go_repo).resolve())for name,original in profile['roots'].items()}
        got=runner.rebase_profile_entry(entry,profile,options,dict(root_rebindings=mapping))
        self.assertEqual(got['cwd'],str(options.root.resolve()))
        self.assertEqual(got['environment']['GO_OVERSYNC_REPO'],str(options.go_repo.resolve()))
        self.assertEqual(got['argv'][-1],'/canonical/go-adjacent/file')
        entry.update(kind='preparation',environment_contexts={'selected':{'GO_OVERSYNC_REPO':'/canonical/go','OVERSQLITE_CONTRACTS_ROOT':'/canonical/kotlin/oversqlite-contracts'}})
        options.context='selected'
        got=runner.rebase_profile_entry(entry,profile,options,dict(root_rebindings=mapping))
        self.assertEqual(got['environment']['GO_OVERSYNC_REPO'],str(options.go_repo.resolve()))
        self.assertEqual(got['environment']['OVERSQLITE_CONTRACTS_ROOT'],str(options.root.resolve()/'oversqlite-contracts'))
        options.root=Path('/canonical/go/.artifacts/worker/sqlitenow-kmp')
        options.go_repo=Path('/canonical/go/.artifacts/worker/go-oversync')
        mapping={original:str((options.root if name=='kotlin' else options.go_repo).resolve()) for name,original in profile['roots'].items()}
        got=runner.rebase_profile_entry(entry,profile,options,dict(root_rebindings=mapping))
        self.assertEqual(got['cwd'],str(options.root))
        self.assertEqual(got['argv'][0],str(options.go_repo/'scripts/run-with-timeout.pl'))
        self.assertEqual(got['argv'][3],str(options.root/'gradlew'))
        self.assertEqual(got['environment']['OVERSQLITE_CONTRACTS_ROOT'],str(options.root/'oversqlite-contracts'))

    def test_profile_activation_requires_frozen_owner_record_profile_and_coordinator(self):
        import argparse
        root=Path(self.temp.name).resolve();code=root/'tools/qualification';code.mkdir(parents=True)
        files={}
        for name in('oversqlite_platform.py','oversqlite_smoke.py','planner.py','platform_resources.py','platform_preparation.py','platform_results.py'):
            path=code/name;path.write_text('controlled coordinator source')
            files[str(path)]=runner.smoke.sha(path.read_bytes())
            actual=Path(runner.__file__).resolve().with_name(name)
            files[str(actual)]=runner.smoke.sha(actual.read_bytes())
        profile=root/'profile.json';profile.write_text('{"controlled":true}')
        digest=runner.smoke.sha(profile.read_bytes());files[str(profile)]=digest
        owner=root/'owner.json';owner.write_text(json.dumps(dict(stage='E42',profile_sha256=digest,owner_request='controlled authorization fixture; no product execution')))
        files[str(owner)]=runner.smoke.sha(owner.read_bytes())
        frozen=root/'freeze.json';data=dict(files=files,authorization=dict(stage='E42',profile_sha256=digest,record=str(owner),record_sha256=files[str(owner)]));frozen.write_text(json.dumps(data))
        options=argparse.Namespace(root=root,profile=profile,freeze=frozen,bindings=None)
        self.assertEqual(runner.profile_activation(options,{},digest)[1],{})
        options.go_repo=root
        options.expected_freeze_sha256=runner.smoke.sha(frozen.read_bytes())
        options.expected_source_sha256='a'*64
        with patch.object(runner.planner,'profile_source_anchor',return_value='a'*64):
            self.assertEqual(runner.profile_activation(options,{},digest)[1],{})
        with patch.object(runner.planner,'profile_source_anchor',return_value='b'*64):
            with self.assertRaisesRegex(SmokeError,'source census'):runner.profile_activation(options,{},digest)
        data['source_revision']='replacement self-consistent freeze'
        frozen.write_text(json.dumps(data))
        with self.assertRaisesRegex(SmokeError,'freeze differs'):runner.profile_activation(options,{},digest)
        del data['source_revision']
        frozen.write_text(json.dumps(data))
        options.expected_source_sha256=None
        with self.assertRaisesRegex(SmokeError,'both exported'):runner.profile_activation(options,{},digest)
        options.expected_freeze_sha256=None
        for key in('stage','profile_sha256','record_sha256'):
            before=data['authorization'].pop(key);frozen.write_text(json.dumps(data))
            with self.assertRaises(SmokeError):runner.profile_activation(options,{},digest)
            data['authorization'][key]=before
        data['files'].pop(str(profile));frozen.write_text(json.dumps(data))
        with self.assertRaises(SmokeError):runner.profile_activation(options,{},digest)

    def test_missing_or_public_lease_record_is_rejected_before_external_inspection(self):
        root=Path(self.temp.name);resource=Resources(root,root,root,{})
        with patch.object(resource,'inspect')as inspect:
            with self.assertRaises(SmokeError):resource.verify_delegated_lease('lease.rich.url','http://127.0.0.1:1',{}, {})
            self.assertFalse(inspect.called)
        record=root/'public.json';record.write_text('{}');record.chmod(0o644)
        proof=dict(path=str(record),sha256=runner.smoke.sha(record.read_bytes()))
        with self.assertRaises(SmokeError):resource.verify_delegated_lease('lease.rich.url','http://127.0.0.1:1',proof,{str(record):proof['sha256']})



class PrivateReadinessTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.path=Path(self.temp.name)/'ready.json'
        self.started=time.time_ns()-1000000
        self.expected=dict(owner='oversqlite-'+'1'*32,nonce='2'*32,pid=os.getpid(),group=os.getpgrp(),
            profile='categories',database='owned_database',fixture_sha256='3'*64,started_ns=self.started)
        self.value=dict(schema_version=1,owner=self.expected['owner'],nonce=self.expected['nonce'],
            pid=os.getpid(),pgid=os.getpgrp(),profile='categories',database='owned_database',
            fixture_sha256='3'*64,reset_domain=self.expected['nonce'],created_unix_ns=time.time_ns(),
            authority_url='http://127.0.0.1:1234',generation=dict(protocol_version='v1',
                schema_generation=dict(id='1',fingerprint='4'*64,tables=[dict(schema='business',table='categories')]),limits=dict(max_push_rows=200)))
        self.write()

    def write(self):
        self.path.write_text(json.dumps(self.value));self.path.chmod(0o600)

    def read(self):return read_private_readiness(self.path,**self.expected)

    def test_exact_private_frame_identifies_the_registered_producer(self):
        self.assertEqual(self.read(),self.value)

    def test_wrong_owner_nonce_profile_process_database_and_clock_fail(self):
        for key in('owner','nonce','profile','database','fixture_sha256','reset_domain','pid','pgid','schema_version','created_unix_ns'):
            before=self.value[key];self.value[key]='wrong';self.write()
            with self.subTest(field=key),self.assertRaises(SmokeError):self.read()
            self.value[key]=before

    def test_partial_duplicate_public_stale_and_symlink_frames_fail(self):
        for raw in('{','{"nonce":"first","nonce":"second"}','{"nonce":NaN}'):
            self.path.write_text(raw)
            with self.assertRaises(SmokeError):self.read()
        self.write();self.path.chmod(0o644)
        with self.assertRaises(SmokeError):self.read()
        self.write();os.utime(self.path,ns=(self.started-1,self.started-1))
        with self.assertRaises(SmokeError):self.read()
        self.write();original=self.path.with_suffix('.original');self.path.rename(original);self.path.symlink_to(original)
        with self.assertRaises(SmokeError):self.read()

    def test_wrong_listener_or_missing_actual_generation_fails(self):
        for url in('http://example.com:1234','http://secret@127.0.0.1:1234','http://127.0.0.1:1234/path','http://127.0.0.1:1234?route=other'):
            self.value['authority_url']=url;self.write()
            with self.assertRaises(SmokeError):self.read()
        self.value['authority_url']='http://127.0.0.1:1234';self.value['generation']={};self.write()
        with self.assertRaises(SmokeError):self.read()


class ResourceLeaseTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.base=Path(self.temp.name).resolve()

    def resource(self,name):
        output=self.base/name;output.mkdir()
        return Resources(self.base,self.base,output,{})

    def test_exclusive_lock_refuses_a_second_holder_and_cleanup_releases_only_its_fd(self):
        first=self.resource('first');second=self.resource('second');locks=self.base/'locks'
        lease=first.acquire_exclusive('measurement-exclusive',lock_root=locks)
        sentinel=self.base/'unrelated';sentinel.write_text('preserve')
        with self.assertRaises(SmokeError):second.acquire_exclusive('measurement-exclusive',lock_root=locks)
        with patch.object(first,'capture_groups'),patch.object(first,'stop_groups'),patch.object(first,'inspect',return_value=''):
            self.assertEqual(first.cleanup()['errors'],[])
        self.assertTrue(lease['released']);self.assertEqual(sentinel.read_text(),'preserve')
        second.acquire_exclusive('measurement-exclusive',lock_root=locks)
        with patch.object(second,'capture_groups'),patch.object(second,'stop_groups'),patch.object(second,'inspect',return_value=''):
            self.assertEqual(second.cleanup()['errors'],[])

    def test_public_symlink_or_unknown_exclusive_lock_is_refused(self):
        resource=self.resource('owner');locks=self.base/'public';locks.mkdir(mode=0o755)
        with self.assertRaises(SmokeError):resource.acquire_exclusive('measurement-exclusive',lock_root=locks)
        with self.assertRaises(SmokeError):resource.acquire_exclusive('other',lock_root=locks)
        private=self.base/'private';private.mkdir(mode=0o700)
        linked=self.base/'linked';linked.symlink_to(private)
        with self.assertRaises(SmokeError):resource.acquire_exclusive('measurement-exclusive',lock_root=linked)

    def test_existing_foreign_volume_is_refused_before_creation_intent(self):
        resource=self.resource('owner')
        with patch.object(resource,'inspect',return_value=resource.run_id+'-data')as inspect:
            with self.assertRaises(SmokeError):resource.postgres()
        self.assertIsNone(resource.volume);self.assertIsNone(resource.container)
        self.assertEqual(inspect.call_count,1)
        self.assertNotIn('create',inspect.call_args.args[0])

    def test_docker_identity_and_label_are_rechecked_before_destructive_cleanup(self):
        resource=self.resource('owner');resource.container=resource.run_id
        resource.record['container_creation_admitted']=True
        value=dict(Name='/'+resource.run_id,Id='original-id',Config=dict(Labels={'com.mobiletoly.go-oversync.test-run':resource.run_id}))
        with patch.object(resource,'inspect',side_effect=lambda *args,**kwargs:json.dumps(value)):
            self.assertEqual(resource.docker_owned('container',resource.run_id),'original-id')
            value['Id']='replacement-id'
            with self.assertRaises(SmokeError):resource.docker_owned('container',resource.run_id)
        value['Config']['Labels']['com.mobiletoly.go-oversync.test-run']='foreign-owner'
        calls=[]
        def inspect(argv,*args,**kwargs):
            calls.append(argv)
            return json.dumps(value)if 'inspect'in argv else''
        with patch.object(resource,'capture_groups'),patch.object(resource,'stop_groups'),patch.object(resource,'inspect',side_effect=inspect):
            cleanup=resource.cleanup()
        self.assertTrue(cleanup['errors'])
        self.assertFalse(any('rm'in argv for argv in calls))

    def test_readonly_json_observation_rejects_partial_and_extra_results(self):
        resource=self.resource('owner')
        for value,expected in(('BEGIN\n{"bytes":1}\nCOMMIT',{'bytes':1}),
                ('BEGIN\n{"sources":[{"id":1},\n {"id":2}]}\nCOMMIT',{'sources':[{'id':1},{'id':2}]})):
            with patch.object(resource,'sql',return_value=value):
                self.assertEqual(resource.sql_json('query','database'),expected)
        for value in('BEGIN\nCOMMIT','{"bytes":1}\n{"bytes":2}','ERROR\n{"bytes":1}','[]','{"sources":[{"id":1},\n'):
            with patch.object(resource,'sql',return_value=value),self.assertRaises(SmokeError):resource.sql_json('query','database')

    def test_go_fixture_census_binds_wrapper_id_and_refuses_foreign_or_preexisting_storage(self):
        from types import SimpleNamespace
        resource=self.resource('owner')
        resource.env['TEST_DATABASE_URL']='owned-admin-control'
        with patch.object(resource,'inspect',return_value='preexisting'):
            resource.prepare_go_fixtures()
        self.assertEqual(resource.env['OVERSYNC_TEST_RUN_ID'],resource.run_id)
        self.assertEqual(resource.env['OVERSYNC_TEST_ISOLATE_DATABASES'],'1')
        container=dict(Id='a'*64,Name='/native-fixture',Created='created',Image='sha256:'+'1'*64,Config=dict(Labels={'com.mobiletoly.go-oversync.test-run':resource.run_id}),Mounts=[dict(Type='volume',Name='fresh')])
        volume=dict(Name='fresh',CreatedAt='born')
        def probe(argv,*args,**kwargs):
            value=container if argv[1]=='container'else volume
            return SimpleNamespace(proc=SimpleNamespace(returncode=0),output=[json.dumps(value).encode(),b''])
        with patch.object(resource,'inspect',return_value='a'*12),patch.object(resource,'probe',side_effect=probe):
            resource.capture_go_fixtures(time.monotonic()+2)
            self.assertEqual(resource.record['go_fixture_volumes'],{'fresh':'born'})
            container['Mounts'][0]['Name']='preexisting'
            with self.assertRaises(SmokeError):resource.capture_go_fixtures(time.monotonic()+2)
            container['Mounts']=[];container['Config']['Labels']['com.mobiletoly.go-oversync.test-run']='foreign'
            with self.assertRaises(SmokeError):resource.capture_go_fixtures(time.monotonic()+2)
        calls=[]
        with patch.object(resource,'capture_go_fixtures'),patch.object(resource,'inspect_go_fixture',return_value=container),patch.object(resource,'inspect',side_effect=lambda argv,*a,**k:calls.append(argv)or''):
            with self.assertRaises(SmokeError):resource.cleanup_go_fixtures(time.monotonic()+2)
        self.assertFalse(any('rm'in call for call in calls))

    def test_go_cleanup_preserves_foreign_fixture_and_continues_other_owned_resources(self):
        resource=self.resource('owner');resource.record.update(go_fixtures={name:dict(name='/'+name,created='born',image='sha256:'+'1'*64)for name in('foreign','owned')},go_fixture_volumes={})
        def inspect_fixture(kind,name,deadline):
            return dict(Id=name,Name='/'+name,Created='born',Image='sha256:'+'1'*64,Config=dict(Labels={'com.mobiletoly.go-oversync.test-run':resource.run_id if name=='owned'else'foreign'}))
        calls=[]
        with patch.object(resource,'capture_go_fixtures'),patch.object(resource,'inspect_go_fixture',side_effect=inspect_fixture),patch.object(resource,'inspect',side_effect=lambda argv,*args,**kwargs:calls.append(argv)or''):
            with self.assertRaises(SmokeError):resource.cleanup_go_fixtures(time.monotonic()+2)
        self.assertIn(['docker','rm','-fv','owned'],calls)
        self.assertNotIn(['docker','rm','-fv','foreign'],calls)
        self.assertTrue(resource.record['go_fixture_cleanup_errors'])


class LoadControllerTests(unittest.TestCase):
    """Controller-only stubs; no scenario, authority, Docker or product runs."""
    def setUp(self):
        from types import SimpleNamespace
        from unittest.mock import Mock
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.base=Path(self.temp.name).resolve();self.output=self.base/'attempt';self.output.mkdir()
        self.freeze=self.base/'freeze.json';self.freeze.write_text('{"files":{}}')
        self.options=SimpleNamespace(bindings=Path('/private'),freeze=self.freeze)
        self.resource=Mock(root=self.base/'kotlin',go=self.base/'go',output=self.output,env={},record={})
        self.record=dict(coordinator_started=time.monotonic(),errors=[])
        self.profile={'schedule':{'entries':{}}};self.commands=[];self.fail_user=None
        for users,seconds in ((1,240),(5,240),(10,240),(25,300)):
            self.profile['schedule']['entries']['phase-load-'+str(users)]=dict(id='phase-load-'+str(users),
                argv=['wrapper',str(seconds),'--','binding.load_client_binary'],cwd='binding.phase_output',
                environment={'OVERSQLITE_LOAD_USERS':str(users)},internal_only=True,wall_seconds=seconds,native_seconds=seconds)
        self.resource.command.side_effect=self.command
        self.resource.http_observer.return_value.url='http://127.0.0.1:3'
        self.resource.http_observer.return_value.report.side_effect=lambda:{'nonce':'a'*32,'acks':[],
            'requests':[],'physical_samples':[],'errors':[]}
        self.resource.authority.side_effect=self.authority
        self.resource.sql.return_value='0|0';self.resource.sql_json.return_value=dict(database_bytes=100,outcomes=0,sources=2)

    def builds(self,*args):
        artifacts={}
        for name in('load-authority','load-client.test'):
            path=self.output/name;path.write_bytes(b'compiled');path.chmod(0o755)
            artifacts[name]=dict(path=str(path),sha256=runner.smoke.sha(path.read_bytes()),mode=0o755)
            receipt=path.with_suffix('.json');receipt.write_text('{"status":"PASS"}')
            self.record.setdefault('load_preparation',{})[name]=dict(path=str(receipt),sha256=runner.smoke.sha(receipt.read_bytes()))
        return artifacts

    def authority(self,*args,**kwargs):
        self.resource.record['private_readiness']=dict(pid=800,pgid=800,database='owned',nonce='a'*32,
            reset_domain='a'*32,details=dict(history_projection_concurrency=32,product_default=4))
        self.resource.env['OVERSYNC_EXAMPLE_TEST_DATABASE_URL']='postgres://owned@127.0.0.1:1/postgres?sslmode=disable'
        return'http://127.0.0.1:2'

    def command(self,argv,seconds,label,*,cwd):
        users=int(self.resource.env['OVERSQLITE_LOAD_USERS']);self.commands.append(users)
        if self.fail_user==users:raise RuntimeError('controlled member failure')
        (self.output/(label+'.log')).write_text('controlled native')
        report=cwd/'report.json';report.write_text('{"controlled":true}')
        retained=[]
        for ordinal in range(users):
            file=cwd/str(ordinal);file.write_text('closed')
            retained.append(dict(user_id=f'{users}-{ordinal}',path=str(file),files=[dict(path=str(file),sha256=runner.smoke.sha(file.read_bytes()))]))
        self.native=dict(verified_users=users,nonce='a'*32,group=500,retained=retained,
            report=dict(path=str(report),sha256=runner.smoke.sha(report.read_bytes())),native_sha256=runner.smoke.sha(b'controlled native'))

    def execute(self):
        with patch.object(runner.smoke,'identity',return_value='candidate'),\
                patch.object(runner.preparation,'verify_load_prerequisites',return_value={'controlled':True}),\
                patch.object(runner,'load_sequence_builds',side_effect=self.builds),\
                patch.object(runner,'verify_freeze',return_value='frozen'),\
                patch.object(runner.results,'audit_load_phase',side_effect=lambda *args,**kwargs:self.native),\
                patch.object(runner.results,'audit_load_observation',return_value={'controlled':True}),\
                patch.object(runner,'group_alive',return_value=False):
            runner.execute_load_sequence(self.options,self.resource,self.record,self.profile,'profile','frozen',
                {'load_prerequisites':{'path':'/reviewed'},'values':{'binding.java_home':'/java'}})

    def test_one_authority_four_ordered_members_and_only_post_cleanup_sequence_completion(self):
        self.execute();self.assertEqual(self.commands,[1,5,10,25])
        self.assertEqual(self.resource.authority.call_count,1)
        self.assertFalse(self.record['sequence_complete'])
        self.assertEqual(self.record['native']['verified_users'],41)
        self.record['cleanup']=dict(errors=[],groups_absent=True)
        runner.seal_load_sequence(self.resource,self.record)
        self.assertTrue(self.record['load_sequence_verified'])
        self.assertTrue(all(not x['sequence_complete']for x in self.record['load_phases']))
        path=Path(self.record['load_phases'][0]['native']['retained'][0]['files'][0]['path']);path.write_text('changed')
        with self.assertRaises(SmokeError):runner.seal_load_sequence(self.resource,self.record)

    def test_failed_member_keeps_the_sequence_incomplete_and_stops_queued_members(self):
        self.fail_user=5
        with self.assertRaises(RuntimeError):self.execute()
        self.assertEqual(self.commands,[1,5]);self.assertFalse(self.record['sequence_complete'])
        self.assertEqual([x['status']for x in self.record['load_phases']],['PASS','FAIL'])

    def test_missing_prerequisites_or_inherited_lease_cannot_start_preparation_or_fixture(self):
        for private in ({},{'load_prerequisites':{},'values':{'lease.load.url':'http://foreign'}}):
            with patch.object(runner,'load_sequence_builds')as builds,patch.object(runner.smoke,'identity',return_value='candidate'):
                with self.assertRaises(SmokeError):
                    runner.execute_load_sequence(self.options,self.resource,self.record,self.profile,'profile','frozen',private)
                self.assertFalse(builds.called);self.assertFalse(self.resource.authority.called)


class HTTPObserverTests(unittest.TestCase):
    """Real owned loopback HTTP control, with a stub database sampler only."""
    def setUp(self):
        import http.server,threading,gzip
        from unittest.mock import Mock
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.output=Path(self.temp.name).resolve();self.received=[]
        self.entered=threading.Event();self.release=threading.Event();self.upstream_cancelled=threading.Event()
        owner=self
        self.response=gzip.compress(b'{"ok":true}',mtime=0)
        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_POST(self):
                raw=self.rfile.read(int(self.headers['Content-Length']))
                owner.received.append((raw,self.headers.get('Authorization')))
                if self.path=='/sync/block':owner.entered.set();owner.release.wait(3)
                if self.path=='/sync/delayed':owner.entered.set();owner.release.wait(11)
                if self.path=='/sync/client-cancel':
                    owner.entered.set();self.connection.settimeout(2)
                    try:
                        if self.rfile.read(1)==b'':owner.upstream_cancelled.set()
                    except OSError:pass
                    return
                try:
                    self.send_response(204 if self.path.endswith('/ack')else 200)
                    body=b''if self.path.endswith('/ack')else owner.response
                    self.send_header('Content-Length',str(len(body)))
                    if body:self.send_header('Content-Encoding','gzip');self.send_header('Vary','Accept-Encoding')
                    self.end_headers();self.wfile.write(body)
                except OSError:pass
        self.server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler)
        self.server.daemon_threads=True;self.server.block_on_close=False
        self.thread=threading.Thread(target=lambda:self.server.serve_forever(poll_interval=.01),daemon=True);self.thread.start()
        self.resource=Mock(env={'JWT_SECRET':'credential-sentinel'},command_deadline=time.monotonic()+10)
        self.resource.sql_json.return_value=dict(database_bytes=1024,outcome_envelope_bytes=128,
            outcome_row_storage_bytes=256,duplicate_source_owners=0)
        self.observer=HTTPObserver(self.resource,'http://127.0.0.1:'+str(self.server.server_port),'owned',self.output,
            nonce='b'*32,capture_bodies=True)
        self.observer.start();self.addCleanup(self.finish)

    def finish(self):
        import threading
        self.release.set()
        self.observer.stop(time.monotonic()+3)
        shutdown=threading.Thread(target=self.server.shutdown,daemon=True);shutdown.start();shutdown.join(3)
        self.server.server_close();self.thread.join(3)
        self.assertFalse(shutdown.is_alive());self.assertFalse(self.thread.is_alive())

    def request(self,route,body,encoding='',timeout=3):
        import http.client
        connection=http.client.HTTPConnection('127.0.0.1',self.observer.server.server_port,timeout=timeout)
        headers={'Oversync-Protocol-Version':'v1','Oversync-Source-ID':'source-exact','Authorization':'Bearer credential-sentinel'}
        if encoding:headers['Content-Encoding']=encoding
        connection.request('POST',route,body,headers);response=connection.getresponse();raw=response.read();status=response.status
        connection.close();self.observer.wait_idle(time.monotonic()+3)
        return status,raw

    def test_actual_entity_bytes_headers_gzip_and_capture_are_preserved_without_credentials(self):
        import gzip
        body=gzip.compress(b'{"source_seq":1}',mtime=0)
        self.assertEqual(self.request('/sync/push',body,'gzip'),(200,self.response))
        self.assertEqual(self.received,[(body,'Bearer credential-sentinel')])
        report=self.observer.report();self.assertEqual(report['errors'],[])
        row=report['requests'][0]
        self.assertEqual(row['encoded_request_sha256'],runner.smoke.sha(body))
        self.assertEqual(row['decoded_response_sha256'],runner.smoke.sha(b'{"ok":true}'))
        self.assertEqual(Path(row['raw_entities']['request']).read_bytes(),body)
        self.assertEqual(Path(row['raw_entities']['response']).read_bytes(),self.response)
        self.assertNotIn('credential-sentinel',json.dumps(report))

    def test_ack_cost_and_positive_storage_observations_are_real_proxy_events(self):
        self.observer.set_phase('load-1')
        self.assertEqual(self.request('/sync/push-outcomes/ack',b'{"source_seq":1}'),(204,b''))
        report=self.observer.report();self.assertEqual(self.resource.sql_json.call_count,2)
        self.assertEqual(report['acks'][0]['operation'],{'source_seq':1})
        self.assertEqual(results.audit_load_observation(report,phase='load-1',nonce='b'*32)['ack_request_count'],1)
        report['acks'][0]['status']=500
        with self.assertRaises(SmokeError):results.audit_load_observation(report,phase='load-1',nonce='b'*32)

    def test_retained_mode_samples_every_sync_request_with_joined_full_storage_census(self):
        import copy
        phase=self.output/'audit-phase.json';phase.write_text('{"phase":"steady"}');phase.chmod(0o600)
        self.observer.phase_file=phase
        self.resource.sql_json.return_value.update(max_outcome_envelope_bytes=64,snapshot_wire_bytes=32,
            outcomes=1,snapshot_rows=1,snapshot_sessions=1,relations=[
                dict(schema='sync',name=name,heap_bytes=8192,total_bytes=16384)
                for name in('push_outcomes','snapshot_session_rows')])
        sample=copy.deepcopy(self.resource.sql_json.return_value)
        self.resource.sql_json.side_effect=lambda *args,**kwargs:copy.deepcopy(sample)
        self.assertEqual(self.request('/sync/pull',b'{}')[0],200)
        report=self.observer.report()
        self.assertEqual(results.audit_retained_observation(report)['request_samples'],2)
        self.assertEqual(self.resource.sql_json.call_count,2)
        self.assertIn('pg_total_relation_size',self.resource.sql_json.call_args.args[0])
        for change in('missing','duplicate','field','relation','zero-snapshot','zero-outcome'):
            bad=copy.deepcopy(report)
            if change=='missing':bad['physical_samples'].pop()
            elif change=='duplicate':bad['physical_samples'].append(copy.deepcopy(bad['physical_samples'][0]))
            elif change=='field':del bad['physical_samples'][0]['snapshot_wire_bytes']
            elif change=='relation':bad['physical_samples'][0]['relations'].pop()
            else:
                field='snapshot_wire_bytes'if change=='zero-snapshot'else'max_outcome_envelope_bytes'
                for row in bad['physical_samples']:row[field]=0
            with self.subTest(change=change),self.assertRaises(SmokeError):results.audit_retained_observation(bad)

    def test_mixed_unsuccessful_acks_are_not_hidden_by_one_success(self):
        self.observer.set_phase('load-1');self.request('/sync/push-outcomes/ack',b'{"source_seq":1}')
        original=self.observer.report()
        for status in (401,500):
            report=json.loads(json.dumps(original));failed=dict(report['acks'][0],status=status)
            report['acks'].append(failed)
            with self.subTest(status=status),self.assertRaises(SmokeError):
                results.audit_load_observation(report,phase='load-1',nonce='b'*32)
        report=json.loads(json.dumps(original));report['acks'].append(dict(report['acks'][0],status=None,client_cancelled=True))
        result=results.audit_load_observation(report,phase='load-1',nonce='b'*32)
        self.assertEqual((result['ack_request_count'],result['successful_ack_responses'],result['cancelled_ack_requests']),(2,1,1))

    def test_auth_response_bodies_are_never_written_as_raw_entities(self):
        self.request('/auth/token',b'private-auth-body')
        self.assertNotIn('raw_entities',self.observer.report()['requests'][0])
        self.assertEqual(list(self.output.iterdir()),[])

    def test_wire_opaque_bytes_have_owned_ordinals_and_raw_entity_proof(self):
        import http.client
        connection=http.client.HTTPConnection('127.0.0.1',self.observer.server.server_port,timeout=3)
        connection.request('POST','/qualification/wire/incompressible',b'',{
            'X-Qualification-Workload':'deterministic-incompressible-above-threshold','X-Qualification-Stage':'measure'})
        response=connection.getresponse();self.assertEqual(response.status,200)
        self.assertEqual(response.getheader('X-Qualification-Nonce'),'b'*32)
        self.assertEqual(response.getheader('X-Qualification-Ordinal'),'1')
        self.assertEqual(response.read(),self.response);connection.close();self.observer.wait_idle(time.monotonic()+3)
        row=self.observer.report()['requests'][0]
        self.assertEqual(row['wire_stage'],'measure')
        self.assertEqual(row['wire_workload'],'deterministic-incompressible-above-threshold')
        self.assertEqual(Path(row['raw_entities']['response']).read_bytes(),self.response)

    def test_browser_preflight_is_forwarded_without_an_sdk_v1_marker_or_body_capture(self):
        import http.client
        connection=http.client.HTTPConnection('127.0.0.1',self.observer.server.server_port,timeout=3)
        connection.request('OPTIONS','/sync/push',headers={'Origin':'http://127.0.0.1:1','Access-Control-Request-Method':'POST'})
        response=connection.getresponse();self.assertEqual(response.status,501) # Exact fixture response, not proxy502.
        response.read();connection.close();self.observer.wait_idle(time.monotonic()+3)
        report=self.observer.report();self.assertEqual(report['errors'],[])
        self.assertNotIn('raw_entities',report['requests'][0])

    def test_ack_preflight_is_not_parsed_sampled_or_counted_as_a_protocol_ack(self):
        import http.client
        connection=http.client.HTTPConnection('127.0.0.1',self.observer.server.server_port,timeout=3)
        connection.request('OPTIONS','/sync/push-outcomes/ack',headers={'Origin':'http://127.0.0.1:1','Access-Control-Request-Method':'POST'})
        response=connection.getresponse();self.assertEqual(response.status,501)
        response.read();connection.close();self.observer.wait_idle(time.monotonic()+3)
        report=self.observer.report();self.assertEqual(report['errors'],[]);self.assertEqual(report['acks'],[])
        self.assertEqual(report['ack_repeat_counts'],[]);self.resource.sql_json.assert_not_called()

    def test_stop_closes_both_sides_of_an_inflight_upstream_request(self):
        import threading
        def client():
            try:self.request('/sync/block',b'waiting')
            except OSError:pass
        thread=threading.Thread(target=client,daemon=True);thread.start()
        self.assertTrue(self.entered.wait(2))
        self.observer.stop(time.monotonic()+2);thread.join(2)
        self.assertFalse(thread.is_alive());self.assertTrue(self.observer.stopped)
        self.assertEqual(self.observer.handlers,set())

    def test_client_only_cancellation_closes_its_upstream_without_stopping_observer(self):
        import http.client
        connection=http.client.HTTPConnection('127.0.0.1',self.observer.server.server_port,timeout=3)
        connection.request('POST','/sync/client-cancel',b'waiting',{'Oversync-Protocol-Version':'v1'})
        self.assertTrue(self.entered.wait(2));connection.close()
        self.assertTrue(self.upstream_cancelled.wait(2))
        self.observer.wait_idle(time.monotonic()+2)
        self.assertFalse(self.observer.stopped)
        report=self.observer.report()
        self.assertEqual(report['errors'],[]);self.assertTrue(report['requests'][0]['client_cancelled'])
        self.assertEqual(self.request('/sync/next',b'healthy'),(200,self.response))

    def test_delayed_response_preserves_the_sdk_allowance_above_ten_seconds(self):
        self.resource.command_deadline=time.monotonic()+25
        before=time.monotonic()
        self.assertEqual(self.request('/sync/delayed',b'delayed',timeout=20),(200,self.response))
        self.assertGreater(time.monotonic()-before,10)
        self.assertEqual(self.observer.report()['errors'],[])

    def test_sampling_failure_survives_coincident_client_disconnect(self):
        import http.client,threading
        entered=threading.Event();fail=threading.Event()
        def sampling(*args,**kwargs):
            entered.set()
            if not fail.wait(2):raise AssertionError('controlled sampling latch was not released')
            raise SmokeError('sampling-failed-positive-control')
        self.resource.sql_json.side_effect=sampling
        connection=http.client.HTTPConnection('127.0.0.1',self.observer.server.server_port,timeout=3)
        connection.request('POST','/sync/push-outcomes/ack',b'{"source_seq":1}',{'Oversync-Protocol-Version':'v1'})
        self.assertTrue(entered.wait(2));connection.close()
        self.assertTrue(self.observer.disconnect_observed.wait(2));fail.set()
        self.observer.wait_idle(time.monotonic()+2)
        self.assertTrue(any('sampling-failed-positive-control'in x['error']for x in self.observer.report()['errors']))

    def test_cancellation_intent_is_visible_before_shutdown_wakes_the_reader(self):
        import http.client,socket,threading
        entered=threading.Event();release=threading.Event();terminal=threading.Event()
        class RecordedRequests(list):
            def append(self,row):super().append(row);terminal.set()
        self.observer.requests=RecordedRequests()
        original=socket.socket.shutdown
        def shutdown(connection,how):
            try:is_upstream=connection.getpeername()[1]==self.server.server_port
            except OSError:is_upstream=False
            result=original(connection,how)
            if is_upstream:
                entered.set()
                if not release.wait(2):raise AssertionError('shutdown handoff was not released')
            return result
        connection=http.client.HTTPConnection('127.0.0.1',self.observer.server.server_port,timeout=3)
        with patch.object(socket.socket,'shutdown',shutdown):
            connection.request('POST','/sync/client-cancel',b'waiting',{'Oversync-Protocol-Version':'v1'})
            self.assertTrue(self.entered.wait(2));connection.close()
            try:
                self.assertTrue(entered.wait(2))
                self.assertTrue(terminal.wait(.5),'reader classified cancellation before close intent was visible')
                self.assertEqual(self.observer.report()['errors'],[])
            finally:release.set()
            self.observer.wait_idle(time.monotonic()+2)

    def test_chunked_entities_and_decode_limits_reject_partial_ambiguous_input(self):
        import email.message,io,gzip
        from unittest.mock import Mock
        headers=email.message.Message();headers['Transfer-Encoding']='chunked'
        handler=Mock(headers=headers,rfile=io.BytesIO(b'3\r\nabc\r\n0\r\n\r\n'))
        self.assertEqual(self.observer.read_body(handler),b'abc')
        for raw in(b'3\r\nab',b'3\r\nabc\n',b'0\r\npartial'):
            handler.rfile=io.BytesIO(raw)
            with self.assertRaises((SmokeError,ValueError)):self.observer.read_body(handler)
        headers['Content-Length']='3'
        with self.assertRaises(SmokeError):self.observer.read_body(handler)
        with patch('tools.qualification.platform_resources.HTTP_BODY_LIMIT',32):
            with self.assertRaises(SmokeError):self.observer.decoded(gzip.compress(b'x'*100),'gzip')


class LoadPhaseAuditTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.base=Path(self.temp.name).resolve();self.phones=self.base/'phones';self.phones.mkdir()
        self.output=self.base/'phase';self.output.mkdir();(self.output/'test_reports').mkdir()
        self.nonce='0123456789abcdef0123456789abcdef';self.report=self.output/'test_reports/report.json'
        self.configure(1)

    def configure(self,users):
        self.users=users;reports=[];durability=[]
        for ordinal in range(1,users+1):
            user=f'parallel-500-10-user-{ordinal:03d}'
            path=self.phones/f'mobile_flow_phone-{user}_{user}_12.db';path.write_bytes(b'closed evidence')
            counts=dict(users=23,posts=447,files=50,file_reviews=50)
            reports.append(dict(user_id=user,sqlite_database_path=str(path),verification_passed=True,errors=[],
                sqlite_results=dict(pending_changes=0,table_counts=counts),
                postgresql_results=dict(business_table_counts={'business.'+k:v for k,v in counts.items()})))
            durability.append(f'QUALIFICATION_LOAD_SQLITE_FULL path={path} synchronous=2 foreign_keys=1 journal=wal')
        self.value=dict(total_users=users,successful_users=users,failed_users=0,user_reports=reports,scenario_name='complex-multi-batch')
        self.native=(f'=== RUN   TestQualificationLoadCompleteScenario\nQUALIFICATION_LOAD_START nonce={self.nonce} pid=500 pgid=500\n'
            +'\n'.join(durability)+f'\nQUALIFICATION_LOAD_COMPLETE nonce={self.nonce} users={users} concurrency={users} verify=true scenario=complex-multi-batch mode=process elapsed_ns=100\n--- PASS: TestQualificationLoadCompleteScenario (0.1s)\n')
        self.write()

    def write(self):
        self.report.write_text(json.dumps(self.value));(self.output/'native.log').write_text(self.native)

    def audit(self):
        return results.audit_load_phase(self.output,users=self.users,nonce=self.nonce,
            started_ns=0,ended_ns=time.time_ns(),phone_root=self.phones)

    def test_complete_native_users_durability_and_preserved_files_are_bound(self):
        self.configure(5)
        row=self.audit()
        self.assertEqual((row['bodies'],row['verified_users'],row['sequence_complete']),(1,5,False))
        self.assertEqual(len(row['retained']),5)
        self.assertTrue(all(len(x['files'])==1 for x in row['retained']))

    def test_missing_duplicate_wrong_nonce_pid_mode_or_terminal_fail(self):
        before=self.native
        for raw in (before.replace('QUALIFICATION_LOAD_START','MISSING'),before+before,
                    before.replace(self.nonce,'f'*32),before.replace('pgid=500','pgid=501'),
                    before.replace('mode=process','mode=goroutine'),before.replace('--- PASS:','--- SKIP:'),
                    before.replace('synchronous=2','synchronous=1')):
            self.native=raw;self.write()
            with self.subTest(raw=raw),self.assertRaises(SmokeError):self.audit()
        self.native=before

    def test_incomplete_final_verification_cannot_be_replaced_by_success_count(self):
        original=json.dumps(self.value)
        for mutation in ('pending','counts','verification','errors','pid','missing-file'):
            self.value=json.loads(original);user=self.value['user_reports'][0]
            if mutation=='pending':user['sqlite_results']['pending_changes']=1
            elif mutation=='counts':user['postgresql_results']['business_table_counts']['business.posts']=446
            elif mutation=='verification':user['verification_passed']=False
            elif mutation=='errors':user['errors']=['worker failure']
            elif mutation=='pid':user['user_id']=user['user_id'].replace('500','501')
            else:user['sqlite_database_path']+='missing'
            self.write()
            with self.subTest(mutation=mutation),self.assertRaises(SmokeError):self.audit()

    def test_missing_duplicate_stale_or_symlink_reports_fail(self):
        duplicate=self.report.with_name('extra.json');duplicate.write_bytes(self.report.read_bytes())
        with self.assertRaises(SmokeError):self.audit()
        duplicate.unlink();os.utime(self.report,ns=(1,1))
        with self.assertRaises(SmokeError):
            results.audit_load_phase(self.output,users=1,nonce=self.nonce,started_ns=2,ended_ns=time.time_ns(),phone_root=self.phones)
        self.write();body=self.report.read_bytes();self.report.unlink()
        with self.assertRaises(SmokeError):self.audit()
        external=self.base/'external.json';external.write_bytes(body);self.report.symlink_to(external)
        with self.assertRaises(SmokeError):self.audit()

    def test_duplicate_user_or_run_identities_cannot_cover_missing_users(self):
        self.configure(5)
        original=json.dumps(self.value)
        self.value['user_reports'][1]=self.value['user_reports'][0];self.write()
        with self.assertRaises(SmokeError):self.audit()
        self.value=json.loads(original);self.value['user_reports'][0]['user_id']='parallel-500-11-user-001';self.write()
        with self.assertRaises(SmokeError):self.audit()


class WebDeadlineTests(unittest.TestCase):
    def make(self,root,task,owners):
        from types import SimpleNamespace
        source=Path(__file__).resolve().parents[3]
        output=root/'attempt';output.mkdir()
        resource=SimpleNamespace(root=source,output=output,command_deadline=time.monotonic()+5)
        entry=dict(task=task,expected_source_owners=owners,
            argv=['/wrapper','270','--','./gradlew',task,*[arg for owner in owners for arg in('--tests',owner)]])
        inputs=list((source/'tools/qualification/profiles').glob('*.init.gradle'))
        inputs+=list((source/'library-oversqlite/karma.config.d').glob('*.js'))
        inputs+=[source/'tools/qualification/profiles/backlog-karma/timeout.js']
        pins={str(path):runner.smoke.sha(path.read_bytes())for path in inputs}
        argv,support=runner.web_deadline_support(resource,entry,entry['argv'],pins)
        return entry,argv,support

    def test_named_node_hook_preserves_other_deadlines_and_exact_completion_census(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory);prefix='dev.goquick.sqlitenow.oversqlite.'
            owners=[prefix+'AutomaticRecoveryTest.uploadAndSnapshotShareDispatchBudget',prefix+'AutomaticRecoveryTest.other']
            entry,argv,support=self.make(root,':library-oversqlite:jsNodeTest',owners)
            self.assertIn('web-dispatch-deadline.init.gradle',' '.join(argv))
            folder=Path(support['folder']);hook=(folder/'node-hook.cjs').read_text()
            self.assertIn('? 120000 : 30000',hook)
            command=dict(task=entry['task'],policy='named-dispatch',milliseconds=120000,nodeTimeout='30s',selectors=sorted(owners))
            (folder/'web-command.json').write_text(json.dumps(command));rows=[]
            for owner in owners:
                name=owner.removeprefix(prefix);limit=120000 if name.endswith('uploadAndSnapshotShareDispatchBudget')else 30000
                rows.extend([dict(phase='deadline',name=name,milliseconds=limit),dict(phase='completion',name=name,milliseconds=limit,state='passed',elapsedMilliseconds=1)])
            path=folder/'deadlines.jsonl';path.write_text('\n'.join(json.dumps(row)for row in rows))
            self.assertEqual(results.audit_web_deadlines(support,entry,0)['cases'],2)
            rows[-1]['milliseconds']=120000;path.write_text('\n'.join(json.dumps(row)for row in rows))
            with self.assertRaises(SmokeError):results.audit_web_deadlines(support,entry,0)

    def test_backlog_karma_preserves_all_normal_configs_and_applies_override_last(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory);owner='dev.goquick.sqlitenow.oversqlite.SettlementTest.backlogBound'
            entry,argv,support=self.make(root,':library-oversqlite:wasmJsBrowserTest',[owner])
            karma=Path(support['folder'])/'karma';names=sorted(path.name for path in karma.glob('*.js'))
            original=Path(__file__).resolve().parents[3]/'library-oversqlite/karma.config.d'
            self.assertEqual(names[:-1],sorted(path.name for path in original.glob('*.js')))
            self.assertEqual(names[-1],'zzzz-qualification-timeout.js')
            for path in original.glob('*.js'):self.assertEqual(path.read_bytes(),(karma/path.name).read_bytes())
            self.assertIn('480000',(karma/names[-1]).read_text())
            self.assertIsNone(runner.web_deadline_policy(dict(entry,expected_source_owners=[owner,'another'])))
            (karma/'zzzzz-unreviewed-override.js').write_text('config.client.mocha.timeout = 1;')
            self.assertNotEqual(runner.preparation.file_map(support['roots'],time.monotonic()+2),support['files'])


class WorkerBrowserTests(unittest.TestCase):
    def test_actual_client_owns_readiness_rejection_at_each_cancellation_boundary(self):
        import shutil
        source=Path(__file__).resolve().parents[3]/'library-core/src/webMain/resources/sqlitenow-worker-v1/client.mjs'
        with tempfile.TemporaryDirectory()as directory:
            script=Path(directory)/'readiness.cjs'
            script.write_text(r'''const fs=require('node:fs'),vm=require('node:vm'),assert=require('node:assert/strict');
const original=fs.readFileSync(process.argv[2],'utf8'),observer='      void ready.catch(() => {});';
assert.equal(original.split(observer).length,2);
const unhandled=[];process.on('unhandledRejection',reason=>unhandled.push(reason));
const turn=()=>new Promise(resolve=>setImmediate(resolve));
async function scenario(phase,faulty=false) {
 const failuresBefore=unhandled.length,workers=[];let armedResolve;
 const armed=new Promise(resolve=>armedResolve=resolve);
 class Worker {
  constructor(){this.listeners=new Map();this.terminations=0;workers.push(this);}
  addEventListener(type,listener){if(!this.listeners.has(type))this.listeners.set(type,new Set());this.listeners.get(type).add(listener);if(type==='messageerror')armedResolve();}
  removeEventListener(type,listener){this.listeners.get(type)?.delete(listener);}
  postMessage(){}
  terminate(){this.terminations++;}
  emit(type,value){for(const fn of this.listeners.get(type)||[])fn(type==='message'?{data:value}:value);}
 }
 const context=vm.createContext({Worker,URL,WebAssembly,SharedArrayBuffer,Atomics,isSecureContext:true,crossOriginIsolated:true,setTimeout,clearTimeout});
 const source=faulty?original.replace(observer,''):original;
 const mod=new vm.SourceTextModule(source,{context,initializeImportMeta(meta){meta.url='http://localhost/sqlitenow-worker-v1/client.mjs';}});
 await mod.link(()=>{throw new Error('Unexpected static client dependency');});await mod.evaluate();
 const client=mod.namespace,opening=client.createSqliteWorkerClient('{}',null,'normal',50);
 const outcome=opening.then(value=>({value}),error=>({error}));
 if(phase==='early')await opening.cancel();
 else {
  await armed;
  if(phase==='before-ready')await opening.cancel();
  else if(phase==='startup-error')workers[0].emit('error',new Error('owned startup failure'));
  else {
   workers[0].emit('message',{kind:'ready',protocol:'sqlitenow-sqlite-worker-v1',runtimeKind:'browser-worker',sqliteVersion:'3.53.0'});
   assert.ok((await outcome).value);await opening.cancel();
  }
 }
 const result=await outcome;
 if(phase==='after-ready')assert.ok(result.value);
 else {assert.ok(result.error);assert.match(result.error.message,phase==='startup-error'?/owned startup failure/:/startup cancelled/);}
 await turn();await turn();
 assert.equal(workers.length,1);assert.equal(workers[0].terminations,1);
 assert.equal([...workers[0].listeners.values()].reduce((sum,set)=>sum+set.size,0),0);
 const diagnostics=JSON.parse(client.sqliteWorkerGlobalDiagnosticsForTest());
 assert.equal(diagnostics.activeWorkers,0);assert.equal(diagnostics.pendingStartups,0);
 assert.equal(diagnostics.workersCreated,1);assert.equal(diagnostics.workersTerminated,1);
 const errors=unhandled.slice(failuresBefore);
 if(faulty){assert.equal(errors.length,1);assert.match(errors[0].message,/startup cancelled/);}
 else assert.deepEqual(errors,[]);
}
(async()=>{for(const phase of ['early','before-ready','after-ready','startup-error'])await scenario(phase);await scenario('early',true);process.stdout.write('four readiness boundaries and original-defect control passed\n');})().catch(error=>{console.error(error);process.exitCode=1;});
''')
            completed=subprocess.run([shutil.which('node')or'node','--experimental-vm-modules',str(script),str(source)],check=False,capture_output=True,text=True,timeout=10)
            self.assertEqual(completed.returncode,0,completed.stdout+completed.stderr)
            self.assertIn('original-defect control passed',completed.stdout)

    def make(self, root, owners=None):
        source=Path(__file__).resolve().parents[3]
        output=root/'attempt';output.mkdir()
        resource=SimpleNamespace(root=source,output=output,command_deadline=time.monotonic()+10)
        owner='dev.goquick.sqlitenow.core.worker.SqliteWorkerDriverTest.oneWayFailuresRemainOrderedAcrossStickyTerminalTransition'
        entry=dict(kind='native',runtime='browser',task=':library-core:jsBrowserTest',expected_source_owners=owners or[owner])
        entry['argv']=['/wrapper','270','--','./gradlew',entry['task'],*[arg for selected in entry['expected_source_owners'] for arg in ('--tests',selected)]]
        inputs=[source/'library-core/src/webTest/kotlin/dev/goquick/sqlitenow/core/worker/SqliteWorkerDriverTest.kt',
                source/'library-core/src/webMain/resources/sqlitenow-worker-v1/client.mjs',
                source/'library-core/src/webMain/resources/sqlitenow-worker-v1/sqlite-3.53.0-build1/worker.mjs',
                *sorted((source/'library-core/karma.config.d').glob('*.js'))]
        pins={str(path):runner.smoke.sha(path.read_bytes())for path in inputs}
        argv,support=runner.worker_browser_support(resource,entry,entry['argv'],pins)
        return entry,argv,support,pins

    def rows(self,support,entry):
        owner=entry['expected_source_owners'][0]
        return [dict(phase='test',owner=owner),dict(phase='armed',id=1,owner=owner,message=support['expected_crashes'][owner],workerPath=support['worker_path']),
                dict(phase='result',owner=owner,state='passed',pending=False),
                dict(phase='expected-error',id=1,owner=owner,message=support['expected_crashes'][owner]),
                dict(phase='terminal',failures=0,abort=False,total=1)]

    def write(self,support,rows):
        path=Path(support['folder'])/'events.jsonl'
        path.write_text('\n'.join(json.dumps(dict(browser='one',log="'E42_WORKER:"+support['nonce']+':'+json.dumps(row)+"'"))for row in rows)+'\n')

    def test_late_failure_after_complete_passing_xml_rejects_admission(self):
        with tempfile.TemporaryDirectory()as directory:
            entry,_,support,_=self.make(Path(directory));owner=entry['expected_source_owners'][0]
            cls,method=owner.rsplit('.',1);xml=Path(directory)/'complete.xml'
            xml.write_text('<testsuite><testcase classname="'+cls+'" name="'+method+'[js, browser]" time="1"/></testsuite>')
            policy={owner:dict(kind='unconditional',reason='controlled assertion body')}
            self.assertEqual(results.audit_full_entry([xml],0,entry,policy)['bodies'],1)
            rows=self.rows(support,entry);self.write(support,rows)
            self.assertEqual(results.audit_worker_browser(support,entry,0)['cases'],1)
            for change in('late-fail','late-global','abort','missing-result','duplicate-result','wrong-error','missing-error','result-before-test','error-before-arm','foreign-module','invalid-id'):
                changed=json.loads(json.dumps(rows))
                if change=='late-fail':changed.insert(-1,dict(phase='fail',owner=owner,message='unexpected final error'))
                elif change=='late-global':changed.append(dict(phase='global-error',message='unexpected after final case'))
                elif change=='abort':changed[-1]['abort']=True
                elif change=='missing-result':changed.pop(2)
                elif change=='duplicate-result':changed.insert(3,dict(changed[2]))
                elif change=='wrong-error':changed[3]['message']='unrelated'
                elif change=='missing-error':changed.pop(3)
                elif change=='result-before-test':changed[0],changed[2]=changed[2],changed[0]
                elif change=='error-before-arm':changed[1],changed[3]=changed[3],changed[1]
                elif change=='foreign-module':changed[1]['workerPath']='/foreign.mjs'
                elif change=='invalid-id':changed[1]['id']=changed[3]['id']=True
                self.write(support,changed)
                with self.subTest(change=change),self.assertRaises(SmokeError):results.audit_worker_browser(support,entry,0)

    def test_hook_contains_only_owned_matching_injected_error_and_preserves_transport(self):
        import shutil
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory);entry,_,support,_=self.make(root)
            script=root/'control.cjs'
            script.write_text(r'''const fs=require('node:fs'),vm=require('node:vm'),assert=require('node:assert/strict');
const info=[],listeners={},handlers={};
const runner={failures:0,_abort:false,total:1,on(k,f){(handlers[k]??=[]).push(f);return this;},emit(k,...v){for(const f of handlers[k]||[])f(...v);}};
class Worker {constructor(){this.listeners=[];this.sent=[];}postMessage(value){this.sent.push(value);}addEventListener(type,fn){if(type==='error')this.listeners.push(fn);}}
const window={Worker,location:{href:'http://localhost:9876/context.html'},addEventListener(k,f){listeners[k]=f;},mocha:{run(){return runner;}},__karma__:{info(v){info.push(v.log);},complete(){}}};
vm.runInNewContext(fs.readFileSync(process.argv[2],'utf8'),{window,URL});
window.mocha.run();
const test={type:'test',title:'oneWayFailuresRemainOrderedAcrossStickyTerminalTransition',state:'passed',pending:false,parent:{title:'SqliteWorkerDriverTest',parent:{title:'dev.goquick.sqlitenow.core.worker'}}};
runner.emit('test',test);
let transport=0,prevented=0;
const foreignDuring=new window.Worker('/foreign.mjs',{type:'module'});foreignDuring.postMessage({kind:'test-control',crash:'terminal after queued one-way failures'});assert.equal(foreignDuring.listeners.length,0);
const worker=new window.Worker('/sqlitenow-worker-v1/sqlite-3.53.0-build1/worker.mjs',{type:'module'});worker.addEventListener('error',()=>transport++);
worker.postMessage({kind:'test-control',crash:'terminal after queued one-way failures'});
runner.emit('test end',test);
for(const fn of worker.listeners)fn.call(worker,{currentTarget:worker,filename:'http://localhost:9876/sqlitenow-worker-v1/sqlite-3.53.0-build1/worker.mjs',cancelable:true,message:'unrelated',preventDefault(){prevented++;}});
assert.equal(prevented,0);assert.equal(transport,1);
for(const fn of worker.listeners)fn.call(worker,{currentTarget:worker,filename:'http://localhost:9876/sqlitenow-worker-v1/sqlite-3.53.0-build1/worker.mjs',cancelable:true,message:'terminal after queued one-way failures',preventDefault(){prevented++;}});
assert.equal(prevented,1);assert.equal(transport,2);assert.equal(worker.sent.length,1);
const foreign=new window.Worker('/sqlitenow-worker-v1/sqlite-3.53.0-build1/worker.mjs',{type:'module'});foreign.postMessage({kind:'test-control',crash:'terminal after queued one-way failures'});assert.equal(foreign.listeners.length,0);
window.__karma__.complete({});
fs.writeFileSync(process.argv[3],info.map(log=>JSON.stringify({browser:'one',log})).join('\n')+'\n');
listeners.error({message:'unexpected page error',preventDefault(){throw Error('must not suppress unrelated error');}});
assert.ok(info.at(-1).includes('global-error'));
''')
            subprocess.run([shutil.which('node')or'node',str(script),str(Path(support['folder'])/'client.js'),str(Path(support['folder'])/'events.jsonl')],check=True,capture_output=True,timeout=10)
            self.assertEqual(results.audit_worker_browser(support,entry,0)['expected_errors'],1)

    def test_normal_config_membership_source_binding_and_route_are_strict(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory);entry,argv,support,pins=self.make(root)
            original=Path(__file__).resolve().parents[3]/'library-core/karma.config.d'
            karma=Path(support['folder'])/'karma'
            for source in original.glob('*.js'):self.assertEqual(source.read_bytes(),(karma/source.name).read_bytes())
            self.assertEqual(argv[6:],entry['argv'][4:])
            self.assertNotIn('timeout',(karma/'zzzz-qualification-worker.js').read_text())
            (karma/'unexpected.js').write_text('config.client.mocha.timeout=1;')
            self.assertNotEqual(runner.preparation.file_map(support['roots'],time.monotonic()+5),support['files'])
            resource=SimpleNamespace(root=original.parents[1],output=root/'other',command_deadline=time.monotonic()+5)
            with self.assertRaises(SmokeError):runner.worker_browser_support(resource,entry,entry['argv'],{})
            untouched=dict(entry,task=':library-core:jsNodeTest')
            self.assertEqual(runner.worker_browser_support(resource,untouched,entry['argv'],{}),(entry['argv'],{}))

    def test_acknowledgement_and_release_faults_require_exact_owned_controls(self):
        import shutil
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory);owner='dev.goquick.sqlitenow.core.worker.SqliteWorkerDriverTest.responseHandoffFailureModesBecomeStickyAndTerminateWithoutRetainedResponses'
            entry,_,support,_=self.make(root,[owner]);script=root/'handoff.cjs'
            script.write_text(r'''const fs=require('node:fs'),vm=require('node:vm'),assert=require('node:assert/strict');
const info=[],handlers={};const runner={failures:0,_abort:false,total:1,on(k,f){(handlers[k]??=[]).push(f);return this;},emit(k,...v){for(const f of handlers[k]||[])f(...v);}};
class Worker {constructor(){this.listeners=[];}postMessage(){}addEventListener(type,fn){if(type==='error')this.listeners.push(fn);}}
const url='http://localhost:9876/sqlitenow-worker-v1/sqlite-3.53.0-build1/worker.mjs';
const window={Worker,location:{href:'http://localhost:9876/context.html'},addEventListener(){},mocha:{run(){return runner;}},__karma__:{info(v){info.push(v.log);},complete(){}}};
vm.runInNewContext(fs.readFileSync(process.argv[2],'utf8'),{window,URL});window.mocha.run();
const test={type:'test',title:'responseHandoffFailureModesBecomeStickyAndTerminateWithoutRetainedResponses',state:'passed',pending:false,parent:{title:'SqliteWorkerDriverTest',parent:{title:'dev.goquick.sqlitenow.core.worker'}}};runner.emit('test',test);
for(const [field,message]of[['acknowledgementMode','controlled acknowledgement failure'],['releaseMode','controlled response release failure']]) {
 const worker=new window.Worker(url,{type:'module'});let prevented=0,transport=0;worker.addEventListener('error',()=>transport++);
 worker.postMessage({kind:'test-control',[field]:'drop-confirmation'});assert.equal(worker.listeners.length,1);
 if(field==='acknowledgementMode'){worker.postMessage({kind:'test-control',acknowledgementMode:'throw',acknowledgementCommand:'page'});assert.equal(worker.listeners.length,1);}
 worker.postMessage({kind:'test-control',[field]:'throw'});
 for(const fn of worker.listeners)fn.call(worker,{currentTarget:worker,filename:url,cancelable:true,message:'Uncaught Error: '+message,preventDefault(){prevented++;}});
 assert.equal(prevented,1);assert.equal(transport,1);
}
runner.emit('test end',test);window.__karma__.complete({});fs.writeFileSync(process.argv[3],info.map(log=>JSON.stringify({browser:'one',log})).join('\n')+'\n');
''')
            subprocess.run([shutil.which('node')or'node',str(script),str(Path(support['folder'])/'client.js'),str(Path(support['folder'])/'events.jsonl')],check=True,capture_output=True,timeout=10)
            self.assertEqual(results.audit_worker_browser(support,entry,0)['expected_errors'],2)


class ResourceDiagnosticTests(unittest.TestCase):
    def test_exact_five_native_bodies_and_files_cannot_be_replaced_by_missing_stale_or_observed_results(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory);(root/'diagnostics').mkdir()
            cases=['1/16','1000/16','1000/1024','10000/16','10000/1024']
            log='--- PASS: TestReplayDiagnostics_Public (1s)\n'+''.join('    --- PASS: TestReplayDiagnostics_Public/'+case+' (1s)\n'for case in cases)+'    1000 maximum resident set size\n'
            (root/'native-command.log').write_text(log)
            paths=[]
            for index,case in enumerate(cases):
                rows,width=map(int,case.split('/'));path=root/'diagnostics'/str(index)
                path=path.with_suffix('.json');paths.append(path)
                path.write_text(json.dumps(dict(rows=rows,width=width,measurement_version=2,state_fingerprint={'complete':'value'},
                    frozen_hash='f'*64,memory_measurement=None,memory=None,observation=None,public_ns=1,replay_tail_ns=1,case_ns=1,setup_ns=1,work=1)))
            self.assertEqual(results.audit_resource_diagnostics(root,0,False)['bodies'],5)
            with self.assertRaises(SmokeError):results.audit_resource_diagnostics(root,time.time_ns()+1_000_000,False)
            with self.assertRaises(SmokeError):results.audit_resource_diagnostics(root,0,True)
            value=json.loads(paths[0].read_text());value['memory_measurement']={'mode':'whole_process'};paths[0].write_text(json.dumps(value))
            with self.assertRaises(SmokeError):results.audit_resource_diagnostics(root,0,False)
            value['memory_measurement']=None;paths[0].write_text(json.dumps(value));paths[-1].unlink()
            with self.assertRaises(SmokeError):results.audit_resource_diagnostics(root,0,False)


class WireNativeResultTests(unittest.TestCase):
    def test_snapshot_seed_preserves_fixed_rows_hash_isolation_and_server_write_commits(self):
        import copy
        manifest = dict(workloads=[dict(id='large-snapshot-chunk', rows_sha256='a'*64)])
        seed = dict(nonce='b'*32, user='wire-'+('b'*32)+'-5', rows=1000, batches=5, route='ScopeManager.ExecWrite', native_credit=0,
            rows_sha256='a'*64, elapsed_seconds=1, bundles=[dict(RowCount=200, BundleSeq=index, BundleHash='c'*64) for index in range(1, 6)])
        self.assertEqual(results.audit_wire_snapshot_seed(seed, manifest, 'b'*32), seed)
        for change in ('nonce', 'user', 'rows', 'hash', 'route', 'credit', 'count', 'sequence', 'bundle-hash', 'deadline'):
            bad = copy.deepcopy(seed)
            if change=='nonce': bad['nonce']='c'*32
            if change=='user': bad['user']='foreign'
            if change=='rows': bad['rows']=999
            if change=='hash': bad['rows_sha256']='d'*64
            if change=='route': bad['route']='direct-metadata'
            if change=='credit': bad['native_credit']=1
            if change=='count': bad['bundles'][0]['RowCount']=199
            if change=='sequence': bad['bundles'][0]['BundleSeq']=0
            if change=='bundle-hash': bad['bundles'][0]['BundleHash']=''
            if change=='deadline': bad['elapsed_seconds']=30
            with self.subTest(change=change), self.assertRaises(SmokeError): results.audit_wire_snapshot_seed(bad, manifest, 'b'*32)

    def test_snapshot_uses_actual_session_route_one_based_rows_and_last_consumed_cursor(self):
        import copy
        session=dict(snapshot_id='exact-owned-id',row_count=1000,logical_byte_count=10)
        chunks=[('/sync/snapshot-sessions/exact-owned-id',dict(rows=[[0,1,[[]]*400]],next_row_ordinal=400,logical_byte_count=4,has_more=True)),
                ('/sync/snapshot-sessions/exact-owned-id',dict(rows=[[0,401,[[]]*600]],next_row_ordinal=1000,logical_byte_count=6,has_more=False))]
        self.assertEqual(results.audit_wire_snapshot([session],chunks),dict(rows=1000,logical_bytes=10,chunks=2))
        for change in('zero-base','cursor','terminal','route','bytes','rows'):
            current=copy.deepcopy(chunks);bound=dict(session)
            if change=='zero-base':current[0][1]['rows'][0][1]=0
            if change=='cursor':current[0][1]['next_row_ordinal']=399
            if change=='terminal':current[0][1]['has_more']=False
            if change=='route':current[0]=('/sync/snapshot-sessions/other',current[0][1])
            if change=='bytes':bound['logical_byte_count']=9
            if change=='rows':bound['row_count']=999
            with self.subTest(change=change),self.assertRaises(SmokeError):results.audit_wire_snapshot([bound],current)

    def text(self):
        return '\n'.join('fixture.go:1: QUALIFICATION_WIRE_WORKLOAD_COMPLETE runtime=raw nonce='+('b'*32)+' workload='+name+' rows=0'for name in results.WIRE_IDS)+'\n--- PASS: TestQualificationWireByteArtifacts (1.00s)\nPASS\n'

    def test_all_scenarios_and_exact_native_owner_pass_are_required(self):
        for change in('none','missing','duplicate','skip'):
            with self.subTest(change=change),tempfile.TemporaryDirectory()as directory:
                root=Path(directory);text=self.text()
                if change=='missing':text=text.replace('workload='+results.WIRE_IDS[0],'workload=other')
                if change=='duplicate':text+='--- PASS: TestQualificationWireByteArtifacts (1.00s)\n'
                if change=='skip':text+='    --- SKIP: TestQualificationWireByteArtifacts/example (0.00s)\n'
                (root/'native-command.log').write_text(text)
                if change=='none':
                    result=runner.wire_native_audit(SimpleNamespace(output=root),{},'raw',{},0,'b'*32)
                    self.assertEqual((result['bodies'],result['workloads']),(1,7))
                else:
                    with self.assertRaises(SmokeError):runner.wire_native_audit(SimpleNamespace(output=root),{},'raw',{},0,'b'*32)


class WireCaptureTests(unittest.TestCase):
    def capture(self, directory):
        import gzip
        root=Path(directory).resolve();folder=root/'actual';folder.mkdir(mode=0o700)
        decoded=b'{"value":"'+b'A'*4096+b'"}';encoded=gzip.compress(decoded,mtime=0)
        request=folder/'request.entity';response=folder/'response.entity'
        request.write_bytes(b'');response.write_bytes(encoded);request.chmod(0o600);response.chmod(0o600)
        row=dict(ordinal=1,raw_entities=dict(request=str(request),response=str(response)),
            encoded_request_body_bytes=0,decoded_request_body_bytes=0,encoded_request_sha256=runner.smoke.sha(b''),
            decoded_request_sha256=runner.smoke.sha(b''),request_content_encoding='',
            encoded_response_body_bytes=len(encoded),decoded_response_body_bytes=len(decoded),
            encoded_response_sha256=runner.smoke.sha(encoded),decoded_response_sha256=runner.smoke.sha(decoded),response_content_encoding='gzip')
        return root,row,decoded

    def test_raw_entity_is_immutable_and_decoded_copy_has_exact_byte_identity(self):
        with tempfile.TemporaryDirectory()as directory:
            root,row,decoded=self.capture(directory);before=Path(row['raw_entities']['response']).read_bytes()
            inputs=results.wire_variant_inputs(root,dict(requests=[row]),time.monotonic()+3)
            self.assertEqual(len(inputs),2)
            self.assertEqual(Path(inputs[1]['path']).read_bytes(),decoded)
            self.assertEqual(Path(row['raw_entities']['response']).read_bytes(),before)

    def test_encoded_or_decoded_drift_or_nonprivate_path_fails_closed(self):
        for change in('encoded','decoded','mode','deadline'):
            with self.subTest(change=change),tempfile.TemporaryDirectory()as directory:
                root,row,_=self.capture(directory)
                if change=='encoded':row['encoded_response_sha256']='0'*64
                if change=='decoded':row['decoded_response_sha256']='0'*64
                if change=='mode':Path(row['raw_entities']['response']).chmod(0o644)
                with self.assertRaises(SmokeError):results.wire_variant_inputs(root,dict(requests=[row]),time.monotonic()+(-1 if change=='deadline'else 3))

    def test_observation_and_comparison_files_cannot_substitute_for_actual_native_receipts(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();(root/'manifest.json').write_text('[]')
            manifest=dict(workloads=[dict(id=name)for name in results.WIRE_IDS])
            observation=dict(nonce='b'*32,errors=[],requests=[])
            with self.assertRaisesRegex(SmokeError,'exactly seven private native'):
                results.audit_wire_capture(root,observation,manifest,root,root,'raw',time.monotonic()+3)


class RetainedJvmAdapterTests(unittest.TestCase):
    def test_missing_preparation_stops_before_any_fixture(self):
        from unittest.mock import Mock
        options=SimpleNamespace(prepared=[])
        resource=Mock()
        with self.assertRaisesRegex(SmokeError,'before fixture startup'):
            runner.retained_jvm_preparation(options,resource,{}, {},'profile','freeze',{})
        resource.authority.assert_not_called()

    def native(self, directory, *, method='defaultTarget', failed=False, executed=True):
        from unittest.mock import Mock
        root=Path(directory);out=root/'attempt';out.mkdir()
        resource=Mock(root=root,output=out,command_deadline=time.monotonic()+10)
        (out/'native-command.log').write_text('Gradle native log')
        report=root/'library-oversqlite/build/test-results/jvmTest/TEST-retained.xml';report.parent.mkdir(parents=True)
        report.write_text('<testsuite><testcase classname="dev.goquick.sqlitenow.oversqlite.DatabaseCleanupAuditJvmTest" name="'+method+'[jvm]" time="1">'+('<skipped/>'if failed else'')+'</testcase><system-out>actual-body-witness</system-out></testsuite>')
        trace=dict(selected=':library-oversqlite:jvmTest',tasks=[dict(path=':library-oversqlite:jvmTest',kind='native-test',
            outcome=dict(skipped=not executed,up_to_date=False,seconds=1))])
        prepared=dict(paths=['/completed/preparation.json'],context={'bound':'producer'},required=[':compile-owner'],snapshots={'source':'bound'})
        record={}
        with patch.object(runner.preparation,'read_full_trace',return_value=trace),patch.object(runner.preparation,'verify_full_consumer')as verify:
            stdout=runner.retained_native_result(None,resource,record,{},0,1000000,prepared)
        return stdout,record,verify,prepared

    def test_actual_jvm_owner_requires_fresh_task_and_completed_classpath_reconciliation(self):
        with tempfile.TemporaryDirectory()as directory:
            stdout,record,verify,prepared=self.native(directory)
            self.assertEqual(stdout,'actual-body-witness')
            self.assertEqual(record['native']['bodies'],1)
            self.assertEqual(record['native']['preparation']['executed_compilation_tasks'],[])
            verify.assert_called_once();self.assertIn('trace',prepared)

    def test_wrong_owner_skip_or_cached_native_task_cannot_receive_credit(self):
        for options in(dict(method='targetOne'),dict(failed=True),dict(executed=False)):
            with self.subTest(options=options),tempfile.TemporaryDirectory()as directory:
                with self.assertRaises(SmokeError):self.native(directory,**options)


class RetainedVectorTests(unittest.TestCase):
    def test_complete_typed_vector_preserves_null_integer_real_text_blob_empty_and_duplicate_rows(self):
        import sqlite3
        db=sqlite3.connect(':memory:');self.addCleanup(db.close)
        db.executescript('CREATE TABLE _sync_example(value); CREATE TABLE categories(id TEXT PRIMARY KEY);')
        for value in(None,0,0.5,'0',b'0','',b'',9223372036854775807,' source ',' source '):
            db.execute('INSERT INTO _sync_example VALUES(?)',(value,))
        vector=results.retained_sqlite_vector(db,time.monotonic()+2)
        self.assertEqual(set(vector['tables']),{'_sync_example','categories'})
        rows=vector['tables']['_sync_example']['rows']
        self.assertEqual(len(rows),10)
        self.assertIn([dict(type='integer',value='9223372036854775807')],rows)
        self.assertIn([dict(type='real',ieee754='3fe0000000000000')],rows)
        self.assertIn([dict(type='blob',hex='')],rows)
        self.assertIn([dict(type='text',value='')],rows)
        self.assertEqual(rows.count([dict(type='text',value=' source ')]),2)
        self.assertEqual(vector['tables']['categories']['rows'],[])
        with self.assertRaises(SmokeError):results.retained_sqlite_vector(db,time.monotonic()-1)

    def test_phase_file_records_current_native_phase_and_rejects_unknown_or_public_state(self):
        import http.server
        from unittest.mock import Mock
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();file=root/'audit-phase.json'
            resource=Mock(command_deadline=time.monotonic()+5,env={})
            observer=HTTPObserver(resource,'http://127.0.0.1:1','owned',root,nonce='c'*32,phase_file=file)
            self.addCleanup(observer.server.server_close)
            self.assertEqual(observer.request_phase('/syncx/status'),'initial')
            file.write_text('{"phase":"steady"}');file.chmod(0o600)
            self.assertEqual(observer.request_phase('/sync/pull'),'steady')
            file.write_text('{"phase":"unknown"}')
            with self.assertRaises(SmokeError):observer.request_phase('/sync/pull')
            file.write_text('{"phase":"steady"}');file.chmod(0o644)
            with self.assertRaises(SmokeError):observer.request_phase('/sync/pull')


if __name__ == "__main__":
    unittest.main()

"""Preparation reuse must never turn stale artifacts/results into fresh credit."""
import argparse
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from tools.qualification import platform_preparation as preparation
from tools.qualification import oversqlite_platform as runner
from tools.qualification.oversqlite_smoke import SmokeError


TASK = ":platform-oversqlite-test:composeApp:jsBrowserTest"


def trace():
    state = dict(did_work=False, skipped=True, up_to_date=True, failure=False, seconds=.1)
    return dict(version=1, selected=TASK, gradle="9.6.0", java_home="/java",
                tasks=[dict(path=":library-core:compileKotlinJs", inputs=[], outputs=[], outcome=state),
                       dict(path=TASK, inputs=[], outputs=[], outcome=dict(state, did_work=True, skipped=False, up_to_date=False))])


class PreparationTests(unittest.TestCase):
    def test_graph_trace_is_private_and_does_not_replace_native_command(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            command = ["/repo/gradlew", TASK, "--tests", "dev.Owner.method", "--rerun"]
            instrumented = preparation.instrument(command, folder)
            self.assertEqual(instrumented[0], command[0])
            self.assertEqual(instrumented[3:], command[1:])
            script = (folder / "preparation.init.gradle").read_text()
            self.assertIn("task.inputs.files", script)
            self.assertIn("task.outputs.files", script)
            self.assertIn("task.path == selected.path ? []", script)
            self.assertNotIn("upToDateWhen", script)

    def test_partial_missing_wrong_duplicate_and_cached_trace_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            path = folder / "preparation-trace.json"
            with self.assertRaises(SmokeError):
                preparation.read_trace(folder, TASK)
            path.write_text('{"version":')
            with self.assertRaises(SmokeError):
                preparation.read_trace(folder, TASK)
            cases = []
            for field, value in [("selected", ":wrong"), ("tasks", []), ("version", 7)]:
                data = trace(); data[field] = value; cases.append(data)
            data = trace(); data["tasks"].append(data["tasks"][-1]); cases.append(data)
            for key in ["skipped", "up_to_date", "failure"]:
                data = trace(); data["tasks"][-1]["outcome"][key] = True; cases.append(data)
            data = trace(); data["tasks"][-1]["outputs"] = ["/cached/report.xml"]; cases.append(data)
            data = trace(); data["tasks"][0]["outcome"] = None; cases.append(data)
            for data in cases:
                path.write_text(json.dumps(data))
                with self.subTest(data=data), self.assertRaises(SmokeError):
                    preparation.read_trace(folder, TASK)
            path.write_text(json.dumps(trace()))
            self.assertEqual(preparation.read_trace(folder, TASK)["selected"], TASK)

    def test_files_bind_content_inventory_mode_and_symlink_targets(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            artifact = folder / "artifact"; artifact.mkdir()
            file = artifact / "compiled.js"; file.write_text("healthy")
            external = folder / "dependency"; external.write_text("one")
            (artifact / "dependency-link").symlink_to(external)
            original = preparation.file_map([str(artifact)])
            external.write_text("two")
            self.assertNotEqual(original, preparation.file_map([str(artifact)]))
            external.write_text("one"); self.assertEqual(original, preparation.file_map([str(artifact)]))
            file.chmod(0o700); self.assertNotEqual(original, preparation.file_map([str(artifact)]))
            file.chmod(original[str(file)]["mode"])
            extra = artifact / "injected.js"; extra.write_text("extra")
            self.assertNotEqual(original, preparation.file_map([str(artifact)]))
            extra.unlink(); self.assertEqual(original, preparation.file_map([str(artifact)]))
            with self.assertRaises(SmokeError):
                preparation.file_map(["relative"])

    def test_changed_sources_fixture_environment_generated_outputs_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory); artifact = folder / "program"; artifact.write_text("compiled")
            binding = dict(source="candidate", environment="flags", runtime="browser", fixture="none")
            saved = dict(version=1, context=binding, roots=[str(artifact)], files=preparation.file_map([str(artifact)]),
                         gradle_runtime={"gradle": "9.6.0", "java_home": "/java"},
                         producer_receipt=str(folder / "receipt.json"),
                         native={"bodies": 9}, cleanup={"errors": [], "groups_absent": True})
            manifest = folder / "binding.json"; manifest.write_text(json.dumps(saved))
            producer = dict(status="PASS", preparation_record={"sha256": preparation.smoke.sha(manifest.read_bytes())}, native=saved["native"], cleanup=saved["cleanup"])
            (folder / "receipt.json").write_text(json.dumps(producer))
            self.assertEqual(preparation.verify(manifest, binding), saved)
            for key in binding:
                with self.subTest(key=key), self.assertRaises(SmokeError):
                    preparation.verify(manifest, dict(binding, **{key: "changed"}))
            artifact.write_text("changed generated executable")
            with self.assertRaises(SmokeError):
                preparation.verify(manifest, binding)
            artifact.write_text("compiled")
            for key, value in [("native", {"bodies": 0}), ("cleanup", {"errors": ["leak"], "groups_absent": False}), ("files", {})]:
                invalid = dict(saved, **{key: value}); manifest.write_text(json.dumps(invalid))
                with self.subTest(key=key), self.assertRaises(SmokeError):
                    preparation.verify(manifest, binding)
            manifest.write_text(json.dumps(saved))
            producer["status"] = "FAIL"; (folder / "receipt.json").write_text(json.dumps(producer))
            with self.assertRaises(SmokeError):
                preparation.verify(manifest, binding)

    def test_unexpected_recompilation_cannot_claim_reuse(self):
        data = trace()
        self.assertEqual(preparation.observed(data, True)["executed_compilation_tasks"], [])
        data["tasks"][0]["outcome"].update(did_work=True, skipped=False, up_to_date=False)
        with self.assertRaises(SmokeError):
            preparation.observed(data, True)
        self.assertEqual(preparation.observed(data, False)["executed_compilation_tasks"], [":library-core:compileKotlinJs"])

    def test_cli_default_and_pilot_restrictions(self):
        parsed = runner.parser().parse_args(["verifier", "--runtime", "browser", "--output", "/new", "--freeze", "/freeze"])
        self.assertFalse(parsed.record_preparation)
        self.assertIsNone(parsed.reuse_preparation)
        for extra in [["--runtime", "jvm", "--record-preparation"], ["--runtime", "browser", "--rebuild-dependencies"]]:
            with self.assertRaises(SystemExit):
                runner.main(["verifier", "--output", "/never-created", "--freeze", "/freeze", *extra])

    def test_manifest_is_written_only_after_unchanged_success(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory); artifact = folder / "program"; artifact.write_text("compiled")
            current = {"source": "unchanged"}
            resource = argparse.Namespace(output=folder)
            frozen = folder / "freeze.json"; frozen.write_text('{"files": {}}')
            options = argparse.Namespace(freeze=frozen, reuse_preparation=None)
            for cleanup in [{"errors": [], "groups_absent": True}]:
                pending = dict(context=current, roots=[str(artifact)], files=preparation.file_map([str(artifact)]))
                record = dict(preparation_pending=pending, metadata_seconds_remaining=120,
                              freeze_sha256=preparation.smoke.sha(frozen.read_bytes()), native={"bodies": 9}, cleanup=cleanup)
                with patch.object(preparation, "context", return_value=current):
                    preparation.seal(resource, options, record)
                self.assertTrue((folder / "preparation.json").is_file())
                self.assertIn("preparation_record", record)

    def test_cleanup_failure_or_interruption_cannot_publish_preparation(self):
        for interrupted in (False, True):
            with tempfile.TemporaryDirectory() as directory:
                folder = Path(directory) / "attempt"
                def execute(options, resources, record):
                    record["preparation_pending"] = {"incomplete": True}
                    if interrupted:
                        raise InterruptedError("controlled cancellation")
                cleanup = {"errors": [] if interrupted else ["owned group remains"], "groups_absent": interrupted}
                with patch.object(runner, "execute", side_effect=execute), patch.object(runner, "clean_environment", return_value={}), patch.object(runner.Resources, "cleanup", return_value=cleanup), patch.object(preparation, "seal") as seal:
                    status = runner.main(["verifier", "--runtime", "browser", "--freeze", "/freeze", "--record-preparation", "--output", str(folder)])
                self.assertEqual(status, 1)
                seal.assert_not_called()
                self.assertFalse((folder / "preparation.json").exists())
                self.assertEqual(json.loads((folder / "receipt.json").read_text())["status"], "FAIL")

    def test_transport_id_does_not_hide_runtime_environment_drift(self):
        original = {"CODEX_TOOL_CALL_ID": "one", "OVERSQLITE_PROCESS_OWNER": "owner-a", "OVERSQLITE_PLATFORM_TESTS": "true", "JAVA_HOME": "/jdk21"}
        binding = preparation.environment_binding(original)
        self.assertEqual(binding, preparation.environment_binding(dict(original, CODEX_TOOL_CALL_ID="two", OVERSQLITE_PROCESS_OWNER="owner-b")))
        for name in ("OVERSQLITE_PLATFORM_TESTS", "JAVA_HOME"):
            self.assertNotEqual(binding, preparation.environment_binding(dict(original, **{name: "changed"})))

    def test_actual_gradle_and_java_header_drift_rejected(self):
        data = trace(); saved = dict(gradle_runtime={key: data[key] for key in ("gradle", "java_home")}, roots=["/artifact"], files={"/artifact": "hash"})
        preparation.verify_graph(saved, data, saved["roots"], saved["files"])
        for key in ("gradle", "java_home"):
            changed = dict(data, **{key: "/other"})
            with self.subTest(key=key), self.assertRaises(SmokeError):
                preparation.verify_graph(saved, changed, saved["roots"], saved["files"])

    def test_shared_metadata_alarm_interrupts_blocking_work_and_restores_handler(self):
        import signal
        previous = signal.getsignal(signal.SIGALRM)
        budget = preparation.Budget(.08)
        with self.assertRaisesRegex(SmokeError, "metadata deadline"):
            with budget.phase():
                time.sleep(.03)
            with budget.phase():
                time.sleep(1)
        self.assertLessEqual(budget.remaining, .005)
        self.assertEqual(signal.getitimer(signal.ITIMER_REAL), (0.0, 0.0))
        self.assertEqual(signal.getsignal(signal.SIGALRM), previous)

    def test_manifest_replacement_during_cleanup_is_detected(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory); frozen = folder / "freeze.json"; frozen.write_text('{"files": {}}')
            manifest = folder / "preparation.json"; manifest.write_text("replacement")
            resource = argparse.Namespace(output=folder); options = argparse.Namespace(freeze=frozen, reuse_preparation=manifest)
            current = {"source": "unchanged"}
            record = dict(preparation_pending=dict(context=current, roots=[], files={}),
                          metadata_seconds_remaining=120, preparation_reuse={"sha256": "original"},
                          freeze_sha256=preparation.smoke.sha(frozen.read_bytes()))
            with patch.object(preparation, "context", return_value=current), self.assertRaisesRegex(SmokeError, "manifest changed"):
                preparation.seal(resource, options, record)

    def test_interruption_during_final_binding_is_recorded_as_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory) / "attempt"
            with patch.object(runner, "execute"), patch.object(runner, "clean_environment", return_value={}), patch.object(runner.Resources, "cleanup", return_value={"errors": [], "groups_absent": True}), patch.object(preparation, "seal", side_effect=InterruptedError("metadata cancelled")):
                status = runner.main(["verifier", "--runtime", "browser", "--freeze", "/freeze", "--record-preparation", "--output", str(folder)])
            self.assertEqual(status, 1)
            self.assertIn("metadata cancelled", json.loads((folder / "receipt.json").read_text())["errors"][0])


class FullPreparationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.output=self.root/'attempt';self.output.mkdir()
        self.artifact=self.root/'classes.bin';self.artifact.write_bytes(b'current compiled bytes')
        self.context=preparation.full_context('profile','freeze',self.root,self.root,{'JAVA_HOME':'/java','OVERSQLITE_REALSERVER_TESTS':'false'})
        self.manifest=self.output/'preparation.json';self.producer=self.output/'receipt.json'
        state=dict(did_work=True,skipped=False,up_to_date=False,failure=False,seconds=.1)
        self.trace=dict(version=2,gradle='9.6.0',java_home='/java',tasks=[dict(path=':core:compile',kind='preparation',inputs=[],outputs=[str(self.artifact)],outcome=state)])
        self.saved=preparation.full_preparation_pending(self.trace,[':core:compile'],self.context,self.root,
            {'JAVA_HOME':'/java','OVERSQLITE_REALSERVER_TESTS':'false'},time.monotonic()+10,entry_id='prepare-controlled')
        self.saved['producer_receipt']=str(self.producer.resolve())
        self.manifest.write_text(json.dumps(self.saved));self.write_producer()

    def write_producer(self,**overrides):
        value=dict(status='PASS',cleanup=dict(errors=[],groups_absent=True),native_credit=0,
                   entry_id='prepare-controlled',profile_sha256='profile',freeze_sha256='freeze',
                   before={str(self.root):'controlled-source'},after={str(self.root):'controlled-source'},
                   preparation_record=dict(path=str(self.manifest.resolve()),sha256=preparation.smoke.sha(self.manifest.read_bytes())))
        value.update(overrides);self.producer.write_text(json.dumps(value))

    def test_only_completed_nonbody_producer_covers_required_tasks(self):
        with self.assertRaises(SmokeError):preparation.verify_full_preparation([],self.context,{':core:compile'},time.monotonic()+10)
        self.assertEqual(preparation.verify_full_preparation([self.manifest],self.context,{':core:compile'},time.monotonic()+10),[':core:compile'])
        for override in (dict(status='FAIL'),dict(native_credit=1),dict(cleanup=dict(errors=['leak'],groups_absent=False))):
            self.write_producer(**override)
            with self.assertRaises(SmokeError):preparation.verify_full_preparation([self.manifest],self.context,{':core:compile'},time.monotonic()+10)
        self.write_producer()
        with self.assertRaises(SmokeError):preparation.verify_full_preparation([self.manifest],self.context,{':missing'},time.monotonic()+10)

    def test_current_artifact_environment_and_manifest_must_match(self):
        self.artifact.write_bytes(b'changed')
        with self.assertRaises(SmokeError):preparation.verify_full_preparation([self.manifest],self.context,{':core:compile'},time.monotonic()+10)
        self.artifact.write_bytes(b'current compiled bytes')
        with self.assertRaises(SmokeError):preparation.verify_full_preparation([self.manifest],dict(self.context,profile_sha256='other'),{':core:compile'},time.monotonic()+10)
        self.saved['task_ids'].append(':other');self.manifest.write_text(json.dumps(self.saved))
        with self.assertRaises(SmokeError):preparation.verify_full_preparation([self.manifest],self.context,{':core:compile'},time.monotonic()+10)

    def test_empty_or_partial_graph_bindings_cannot_authorize_exclusions(self):
        import copy
        valid=copy.deepcopy(self.saved)
        for mutation in ('files','roots','requested','trace','task_ids'):
            self.saved=copy.deepcopy(valid)
            self.saved[mutation]={}if mutation in('files','trace')else[]
            self.manifest.write_text(json.dumps(self.saved));self.write_producer()
            with self.subTest(field=mutation),self.assertRaises(SmokeError):
                preparation.verify_full_preparation([self.manifest],self.context,{':core:compile'},time.monotonic()+10)

    def test_consumption_rechecks_producer_artifact_runtime_and_actual_graph(self):
        snapshots={}
        preparation.verify_full_preparation([self.manifest],self.context,{':core:compile'},time.monotonic()+10,snapshots=snapshots)
        state=dict(did_work=True,skipped=False,up_to_date=False,failure=False,seconds=.1)
        native=dict(path=':core:test',kind='native-test',inputs=[str(self.artifact)],outputs=[],outcome=state)
        trace=dict(gradle='9.6.0',java_home='/java',selected=':core:test',tasks=[native])
        def verify(current=trace):
            preparation.verify_full_consumer([self.manifest],self.context,{':core:compile'},current,snapshots,time.monotonic()+10)
        verify()
        aggregate=dict(path=':core:testClasses',kind='preparation',actionable=False,
            inputs=[str(self.artifact)],outputs=[],outcome=dict(state,did_work=False))
        verify(dict(trace,tasks=[aggregate,native]))
        for changed in(dict(aggregate,actionable=True),dict(aggregate,outcome=state),dict(aggregate,outputs=[str(self.artifact)]),dict(aggregate,inputs=[str(self.root/'unbound')])):
            with self.assertRaises(SmokeError):verify(dict(trace,tasks=[changed,native]))
        self.artifact.write_bytes(b'changed by native consumer')
        with self.assertRaises(SmokeError):verify()
        self.artifact.write_bytes(b'current compiled bytes')
        self.write_producer(extra='changed after admission')
        with self.assertRaises(SmokeError):verify()
        self.write_producer()
        with self.assertRaises(SmokeError):verify(dict(trace,java_home='/wrong-java'))
        with self.assertRaises(SmokeError):verify(dict(trace,tasks=[native,*self.trace['tasks']]))
        other=dict(path=':unexpected:check',kind='preparation',inputs=[],outputs=[],outcome=dict(state,did_work=False))
        with self.assertRaises(SmokeError):verify(dict(trace,tasks=[native,other]))

    def test_effective_private_gradle_configuration_is_part_of_closure(self):
        with patch.dict('os.environ',{'GRADLE_USER_HOME':'/ambient'}):
            roots=preparation.roots_for(dict(tasks=[]),self.root,{'GRADLE_USER_HOME':'/private'})
        self.assertIn('/private/init.d',roots)
        self.assertNotIn('/ambient/init.d',roots)

    def test_provider_predecessor_stays_enabled_with_bound_outputs_and_no_compilation(self):
        import copy
        provider=':core:generateSyntheticLinkageSwiftPMImportProjectForCinteropsAndLdDump'
        prepared=copy.deepcopy(self.trace)
        metadata=dict(path=provider,kind='preparation',inputs=[str(self.artifact)],outputs=[],
            outcome=dict(did_work=False,skipped=True,up_to_date=True,failure=False,seconds=.1))
        prepared['tasks'].append(metadata)
        self.saved=preparation.full_preparation_pending(prepared,[':core:compile',provider],self.context,
            self.root,{'JAVA_HOME':'/java','OVERSQLITE_REALSERVER_TESTS':'false'},time.monotonic()+10,
            entry_id='prepare-controlled')
        self.saved['producer_receipt']=str(self.producer.resolve())
        self.manifest.write_text(json.dumps(self.saved));self.write_producer()
        required={':core:compile',provider};snapshots={}
        verified=preparation.verify_full_preparation([self.manifest],self.context,required,
            time.monotonic()+10,snapshots=snapshots)
        self.assertEqual(set(verified),required)
        native=dict(path=':core:test',kind='native-test',inputs=[str(self.artifact)],outputs=[],
            outcome=dict(did_work=True,skipped=False,up_to_date=False,failure=False,seconds=.1))
        compiled=copy.deepcopy(prepared['tasks'][0])
        compiled['outcome'].update(did_work=False,skipped=True,up_to_date=True)
        current=dict(gradle='9.6.0',java_home='/java',selected=':core:test',tasks=[compiled,metadata,native])
        def verify():
            preparation.verify_full_consumer([self.manifest],self.context,required,current,snapshots,time.monotonic()+10)
        verify()
        current['tasks']=[*prepared['tasks'],native]
        with self.assertRaisesRegex(SmokeError,'executed build tasks'):verify()
        current['tasks']=[metadata,native]
        self.artifact.write_bytes(b'provider changed a sealed dependency')
        with self.assertRaises(SmokeError):verify()

    def test_runtime_only_jars_are_sealed_before_any_native_execution(self):
        import copy
        runtime=self.root/'runtime-only.jar';runtime.write_bytes(b'actual runtime dependency')
        trace=copy.deepcopy(self.trace)
        trace.update(runtime_classpaths=[dict(task=':core:test',files=[str(runtime)])],runtime_inputs=[str(runtime)])
        self.saved=preparation.full_preparation_pending(trace,[':core:compile'],self.context,self.root,
            {'JAVA_HOME':'/java','OVERSQLITE_REALSERVER_TESTS':'false'},time.monotonic()+10,entry_id='prepare-controlled')
        self.assertIn(str(runtime),self.saved['files'])
        self.saved['producer_receipt']=str(self.producer.resolve())
        self.manifest.write_text(json.dumps(self.saved));self.write_producer()
        preparation.verify_full_preparation([self.manifest],self.context,{':core:compile'},time.monotonic()+10)
        runtime.write_bytes(b'changed runtime dependency')
        with self.assertRaises(SmokeError):preparation.verify_full_preparation([self.manifest],self.context,{':core:compile'},time.monotonic()+10)
        malformed=copy.deepcopy(trace);malformed['runtime_inputs']=[]
        with self.assertRaises(SmokeError):preparation._full_preparation_graph(malformed)

    def test_native_sources_and_framework_aliases_are_bound_before_execution(self):
        import copy
        source=self.root/'Case.swift';source.write_text('source one')
        framework=self.root/'Runtime.framework';headers=framework/'Versions/A/Headers';headers.mkdir(parents=True)
        (headers/'Runtime.h').write_text('header one')
        (framework/'Versions/Current').symlink_to('A')
        (framework/'Headers').symlink_to('Versions/Current/Headers')
        paths=sorted(map(str,[source,framework,framework/'Headers',framework/'Versions/Current/Headers']))
        trace=copy.deepcopy(self.trace)
        trace.update(native_inputs=[dict(task=':swift:liveTest',files=paths)],runtime_inputs=paths)
        self.saved=preparation.full_preparation_pending(trace,[':core:compile'],self.context,self.root,
            {'JAVA_HOME':'/java','OVERSQLITE_REALSERVER_TESTS':'false'},time.monotonic()+10,entry_id='prepare-controlled')
        self.assertEqual(self.saved['files'][str(framework/'Headers')]['kind'],'symlink')
        self.assertEqual(self.saved['files'][str(framework/'Versions/Current')]['target'],'A')
        self.assertIn(str(headers/'Runtime.h'),self.saved['files'])
        self.saved['producer_receipt']=str(self.producer.resolve())
        self.manifest.write_text(json.dumps(self.saved));self.write_producer()
        def verify():preparation.verify_full_preparation([self.manifest],self.context,{':core:compile'},time.monotonic()+10)
        verify();source.write_text('source changed')
        with self.assertRaises(SmokeError):verify()
        source.write_text('source one');(framework/'Versions/Current').unlink();(framework/'Versions/Current').symlink_to('B')
        with self.assertRaises((SmokeError,FileNotFoundError)):verify()
        with self.assertRaises(SmokeError):preparation._full_preparation_graph(dict(trace,native_inputs=None))

    def test_device_selection_is_separate_but_activation_and_urls_are_bound(self):
        base={'JAVA_HOME':'/java','OVERSQLITE_REALSERVER_TESTS':'true','OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL':'http://127.0.0.1:1'}
        def context(env):return preparation.full_context('profile','freeze',self.root,self.root,env)
        self.assertEqual(context(base),context(base|{'ANDROID_SERIAL':'emulator-5570','SIMCTL_CHILD_OVERSQLITE_REALSERVER_TESTS':'true'}))
        self.assertNotEqual(context(base),context(base|{'OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL':'http://127.0.0.1:2'}))
        self.assertNotEqual(context(base),context(base|{'SIMCTL_CHILD_OVERSQLITE_REALSERVER_TESTS':'false'}))
        self.assertNotEqual(context(base),context(base|{'ANDROID_USER_HOME':'/different-signing-home'}))

    def test_owned_android_tools_do_not_change_the_prepared_build_signing_home(self):
        from types import SimpleNamespace
        from tools.qualification.platform_resources import Resources
        for build_home in (None, '/prepared/android-user'):
            with self.subTest(build_home=build_home), tempfile.TemporaryDirectory() as directory:
                root=Path(directory); env={'ANDROID_HOME':'/sdk'}
                if build_home is not None:env['ANDROID_USER_HOME']=build_home
                resource=Resources(root,root,root,env)
                calls=[]
                def inspect(argv,*args,**kwargs):
                    calls.append((argv,kwargs.get('env',dict(resource.env))))
                    return '1' if 'sys.boot_completed' in argv else ''
                def launch(argv,log,**kwargs):
                    calls.append((argv,kwargs['env']))
                    return SimpleNamespace(proc=SimpleNamespace(poll=lambda:None))
                boot=SimpleNamespace(proc=SimpleNamespace(returncode=0),output=[b'1'])
                with patch.object(resource,'inspect',side_effect=inspect), patch.object(resource,'launch',side_effect=launch), patch.object(resource,'verify_android'), patch.object(resource,'probe',return_value=boot):
                    resource.android_device()
                self.assertEqual(resource.env.get('ANDROID_USER_HOME'),build_home)
                self.assertTrue(resource.env['ANDROID_SERIAL'].startswith('emulator-'))
                device_calls=[(argv,value)for argv,value in calls if 'create' in argv or Path(argv[0]).name=='emulator']
                self.assertEqual(len(device_calls),2)
                for argv,value in device_calls:
                    self.assertEqual(value['ANDROID_USER_HOME'],str(root/'android/user'))
                    self.assertEqual(value['ANDROID_AVD_HOME'],str(root/'android/avd'))

    def test_android_preparation_seals_instrumentation_inputs_without_executing_tests(self):
        task=':harness:connectedAndroidDeviceTest'
        inputs=preparation.android_preparation_input_tasks(dict(task_ids=[':harness:packageAndroidDeviceTest',':harness:compileAndroidDeviceTest']))
        self.assertEqual(inputs,(task,))
        args=preparation.instrument_full(['./gradlew',':harness:packageAndroidDeviceTest'],self.output,prepare_only=True,native_input_tasks=inputs)
        self.assertIn(':harness:help',args)
        self.assertNotIn(task,args)
        runtime=self.root/'instrumentation-engine.jar';runtime.write_bytes(b'actual runtime dependency')
        trace=dict(self.trace,native_inputs=[dict(task=task,files=[str(runtime)])],runtime_inputs=[str(runtime)])
        saved=preparation.full_preparation_pending(trace,[':core:compile'],self.context,self.root,{'JAVA_HOME':'/java','OVERSQLITE_REALSERVER_TESTS':'false'},time.monotonic()+10,entry_id='prepare-controlled')
        self.assertIn(str(runtime),saved['files'])

    def test_explicit_launch_environment_always_binds_the_actual_process_owner(self):
        from types import SimpleNamespace
        from tools.qualification import platform_resources
        resource=platform_resources.Resources(self.root,self.root,self.output,{'OVERSQLITE_PROCESS_OWNER':'foreign'})
        for supplied in ({'MARKER':'private'},{'OVERSQLITE_PROCESS_OWNER':'foreign'}):
            original=dict(supplied)
            child=SimpleNamespace(proc=SimpleNamespace(pid=987654))
            with patch.object(platform_resources.smoke,'Child',return_value=child)as constructor:
                resource.launch(['owned-child'],self.output/'child.log',env=supplied)
            actual=constructor.call_args.args[2]
            self.assertEqual(actual,dict(supplied,OVERSQLITE_PROCESS_OWNER=resource.run_id))
            self.assertEqual(supplied,original)

    def test_exited_group_permission_race_requires_verified_absence(self):
        from tools.qualification import platform_resources
        resource=platform_resources.Resources(self.root,self.root,self.output,{})
        resource.groups={987654};errors=[]
        with patch.object(platform_resources,'group_alive',side_effect=[True,False,False]), patch.object(platform_resources.os,'killpg',side_effect=PermissionError(1,'Operation not permitted'))as send:
            resource.stop_groups(time.monotonic()+10,errors)
        self.assertEqual(errors,[])
        send.assert_called_once_with(987654,platform_resources.signal.SIGTERM)
        self.assertEqual(resource.record['resolved_signal_races'],[dict(group=987654,signal='SIGTERM',error='[Errno 1] Operation not permitted',group_absent=True)])

    def test_persistent_group_permission_denial_and_escalation_still_fail(self):
        from tools.qualification import platform_resources
        resource=platform_resources.Resources(self.root,self.root,self.output,{})
        resource.groups={987654};errors=[]
        with patch.object(platform_resources,'group_alive',return_value=True), patch.object(platform_resources.os,'killpg',side_effect=PermissionError(1,'Operation not permitted'))as send, patch.object(platform_resources.time,'monotonic',side_effect=[0,11]):
            resource.stop_groups(10,errors)
        self.assertEqual(send.call_count,2)
        self.assertIn('owned group SIGTERM permission denied: 987654:',errors[0])
        self.assertIn('owned group required SIGKILL: 987654',errors)
        self.assertNotIn('resolved_signal_races',resource.record)

    def test_preparation_trace_refuses_native_driver_stale_and_partial_graphs(self):
        folder=self.output/'task-traces';folder.mkdir()
        state=dict(did_work=True,skipped=False,up_to_date=False,failure=False,seconds=.1)
        data=dict(version=2,root=str(self.root),preparation=True,gradle='9.6.0',java_home='/java',failure=False,
                  tasks=[dict(path=':core:compile',kind='preparation',actionable=True,inputs=[],outputs=[str(self.artifact)],outcome=state)])
        file=folder/'graph-root.json';file.write_text(json.dumps(data))
        got=preparation.read_full_trace(self.output,self.root,0,prepare_only=True)
        self.assertEqual(got['tasks'][0]['path'],':core:compile')
        for kind in('native-test','driver'):
            data['tasks'][0]['kind']=kind;file.write_text(json.dumps(data))
            with self.assertRaises(SmokeError):preparation.read_full_trace(self.output,self.root,0,prepare_only=True)
        with self.assertRaises(SmokeError):preparation.read_full_trace(self.output,self.root,time.time_ns()+1000000,prepare_only=True)

    def test_instrumentation_preserves_the_exact_requested_task_and_options(self):
        args=preparation.instrument_full(['./gradlew',':core:compile','--no-parallel'],self.output,prepare_only=True)
        self.assertEqual(args[0],'./gradlew')
        self.assertEqual(args[3:5],[':core:compile','--no-parallel'])
        self.assertEqual(args[-1],'-Pqualification.preparationOnly=true')
        self.assertTrue(Path(args[2]).is_file())
        self.assertIn('def prepareOnly = true',Path(args[2]).read_text())
        fresh=preparation.instrument_full(['./gradlew',':core:test'],self.output,prepare_only=False)
        self.assertIn('def prepareOnly = false',Path(fresh[2]).read_text())
        with self.assertRaises(SmokeError):preparation.instrument_full(['python3','test'],self.output,prepare_only=True)



class WireDependencyTests(unittest.TestCase):
    def owner(self):
        return dict(task=':native',dependencies=[dict(path=':compile',kind='preparation',actionable=True,dependencies=[]),
            dict(path=':classes',kind='preparation',actionable=False,dependencies=[':compile',':resources'])])

    def test_only_declared_owners_execute_and_action_free_aggregate_edges_are_preserved(self):
        selection=preparation.wire_dependency_roots(self.owner(),':native',{':compile',':resources'},{})
        self.assertEqual(selection['requested'],[':compile',':resources'])
        self.assertEqual(selection['elided_action_free_aggregates'],{':classes':[':compile',':resources']})

    def test_unknown_actionable_native_missing_edge_duplicate_and_cycles_fail_closed(self):
        import copy
        for change in('actionable','native','missing','duplicate','cycle','wrong-owner'):
            with self.subTest(change=change):
                owner=copy.deepcopy(self.owner())
                if change=='actionable':owner['dependencies'][1]['actionable']=True
                if change=='native':owner['dependencies'][0]['kind']='native-test'
                if change=='missing':owner['dependencies'][1]['dependencies']=[':undeclared']
                if change=='duplicate':owner['dependencies'].append(owner['dependencies'][0])
                if change=='cycle':owner['dependencies'][1]['dependencies']=[':classes']
                if change=='wrong-owner':owner['task']=':other'
                with self.assertRaises(SmokeError):preparation.wire_dependency_roots(owner,':native',{':compile',':resources'},{})


class WireDerivationTests(unittest.TestCase):
    def setUp(self):
        from types import SimpleNamespace
        from unittest.mock import Mock
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve();self.go=self.root/'go';self.kotlin=self.root/'kotlin'
        self.go.mkdir();self.kotlin.mkdir();self.names=['app.go','go.mod']
        self.engines=[self.root/name for name in('launcher','toolchain','VERSION')]
        named=[self.kotlin/'tools/qualification/profiles/fixtures'/name for name in('wire_authority.go','wire_encoder.go','wire-workloads.json')]
        paths=[self.go/name for name in self.names]+self.engines+named+[self.go/'scripts/run-with-timeout.pl']
        for path in paths:path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(b'current source '+path.name.encode())
        self.sources={str(path):preparation.smoke.sha(path.read_bytes())for path in paths}
        binary=self.root/'generator';binary.write_bytes(b'compiled generator');binary.chmod(0o755)
        self.files=self.sources|{str(binary):preparation.smoke.sha(binary.read_bytes())}
        self.runtime=dict(version='go1.25.12',tool_directory='/current/tool',launcher='/current/launcher')
        self.proof=dict(status='PASS',native_credit=0,cleanup=dict(errors=[],groups_absent=True),manifest=dict(sha256='a'*64),
            go_runtime=self.runtime,before={str(self.go):'current-go'},after={str(self.go):'current-go'},sources=self.sources.copy(),
            binary=dict(path=str(binary),sha256=self.files[str(binary)],mode=0o755))
        self.resources=SimpleNamespace(root=self.kotlin,go=self.go,env={},inspect=Mock(side_effect=lambda *args,**kwargs:'\0'.join(self.names)+'\0'))
        self.identity=patch.object(preparation.smoke,'identity',return_value='current-go');self.identity.start();self.addCleanup(self.identity.stop)

    def verify(self):
        return preparation.verify_wire_derivation(self.resources,self.proof,self.files,'a'*64,self.engines,self.runtime)

    def test_complete_current_go_named_engine_and_generator_bindings_pass(self):
        result=self.verify();self.assertEqual(result['go_sources'],2);self.assertEqual(len(result['mandatory_named_sources']),3)

    def test_empty_subset_new_source_or_changed_engine_cannot_pass(self):
        import copy
        original=copy.deepcopy(self.proof)
        for change in('empty','named','engine','new-source','identity'):
            with self.subTest(change=change):
                self.proof=copy.deepcopy(original)
                if change=='empty':self.proof['sources']={}
                if change=='named':self.proof['sources'].pop(str(self.kotlin/'tools/qualification/profiles/fixtures/wire_encoder.go'))
                if change=='engine':self.proof['sources'].pop(str(self.engines[0]))
                if change=='new-source':
                    path=self.go/'late.go';path.write_bytes(b'new owning source');self.names.append('late.go')
                if change=='identity':self.proof['before'][str(self.go)]='stale-go'
                with self.assertRaises(SmokeError):self.verify()
                if change=='new-source':self.names.remove('late.go');path.unlink()


class LoadOverlayTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name).resolve()
        self.go = self.base/'go';self.root = self.base/'kotlin';self.output = self.base/'output'
        self.original = self.go/'oversqlite/upgrade.go'
        self.scenario = self.root/'tools/qualification/profiles/fixtures/load_scenario_test.go'
        self.original.parent.mkdir(parents=True);self.scenario.parent.mkdir(parents=True);self.output.mkdir()
        self.source = b'package oversqlite\nfunc opening() {\n\tquery.Set("_journal_mode", "WAL")\n\tdb.SetMaxOpenConns(4)\n\tif err := bindOpeningDBOwnership(identity.Key, db); err != nil {\n\t}\n}\n'
        self.original.write_bytes(self.source);self.scenario.write_text('package main\n')
        self.pins = {str(p):preparation.smoke.sha(p.read_bytes())for p in(self.original,self.scenario)}

    def generate(self):
        with patch.object(preparation,'LOAD_UPGRADE_PREIMAGE',preparation.smoke.sha(self.source)):
            return preparation.generate_load_overlay(self.root,self.go,self.output,self.pins)

    def test_overlay_preserves_the_source_and_pool_and_binds_exact_two_replacements(self):
        record = self.generate()
        self.assertEqual(self.original.read_bytes(),self.source)
        self.assertEqual(record['pool_connections'],4)
        self.assertEqual(record['native_credit'],0)
        overlay = json.loads(Path(record['path']).read_text())['Replace']
        self.assertEqual(set(overlay),{str(self.original),str(self.go/'integration/mobile_flow/qualification_load_scenario_test.go')})
        generated = Path(record['generated_source']).read_text()
        self.assertIn('query.Set("_synchronous", "FULL")',generated)
        self.assertIn('qualificationSync != 2 || qualificationFK != 1 || qualificationJournal != "wal"',generated)
        self.assertIn('QUALIFICATION_LOAD_SQLITE_FULL path=',generated)
        self.assertEqual(generated.count('db.SetMaxOpenConns(4)'),1)
        self.assertEqual(Path(record['path']).stat().st_mode & 0o777,0o600)
        self.assertEqual(Path(record['generated_source']).stat().st_mode & 0o777,0o600)

    def test_unreviewed_preimage_is_rejected_even_when_frozen(self):
        with self.assertRaises(SmokeError):
            preparation.generate_load_overlay(self.root,self.go,self.output,self.pins)
        self.assertFalse((self.output/'load-overlay').exists())

    def test_changed_source_or_scenario_cannot_be_hidden_by_overlay(self):
        for path in(self.original,self.scenario):
            before = path.read_bytes();path.write_bytes(before+b'// external drift\n')
            with self.subTest(path=path),self.assertRaises(SmokeError):self.generate()
            self.assertFalse((self.output/'load-overlay').exists());path.write_bytes(before)

    def test_existing_scenario_and_existing_output_are_never_overwritten(self):
        virtual = self.go/'integration/mobile_flow/qualification_load_scenario_test.go'
        virtual.parent.mkdir(parents=True);virtual.write_text('existing')
        with self.assertRaises(SmokeError):self.generate()
        self.assertEqual(virtual.read_text(),'existing');virtual.unlink()
        record = self.generate()
        before = Path(record['path']).read_bytes()
        with self.assertRaises(FileExistsError):self.generate()
        self.assertEqual(Path(record['path']).read_bytes(),before)

    def test_overlay_and_inputs_remain_bound_through_cleanup(self):
        record = self.generate()
        preparation.verify_load_overlay(record)
        for path in (Path(record['path']),Path(record['generated_source']),self.original,self.scenario):
            before=path.read_bytes();path.write_bytes(before+b' ')
            with self.subTest(path=path),self.assertRaises(SmokeError):
                preparation.verify_load_overlay(record)
            path.write_bytes(before)


class ResourceOverlayTests(unittest.TestCase):
    def test_durability_delta_keeps_pool_and_workload_and_rejects_changed_preimage(self):
        name='oversqlite/baseline_qualification_test.go'
        raw=b'path+"?_foreign_keys=on"\npath+"?_foreign_keys=on"\ndb.SetMaxOpenConns(1)\nwork=100000000\n'
        with patch.dict(preparation.RESOURCE_PREIMAGES,{name:runner.smoke.sha(raw)}):
            result=preparation.resource_durability_source(name,raw)
            self.assertEqual(result.count(b'_synchronous=FULL&_journal_mode=WAL'),2)
            self.assertIn(b'db.SetMaxOpenConns(1)\nwork=100000000',result)
            with self.assertRaises(SmokeError):preparation.resource_durability_source(name,raw+b'changed')
        with self.assertRaises(SmokeError):preparation.resource_driver_source(b'unreviewed driver')

    def test_generated_resource_overlay_and_inputs_are_immutable(self):
        import shutil
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();go=root/'go';(go/'oversqlite').mkdir(parents=True)
            output=root/'output';output.mkdir()
            raw=b'path+"?_foreign_keys=on"\npath+"?_foreign_keys=on"\n'
            name='oversqlite/baseline_qualification_test.go';source=go/name;source.write_bytes(raw)
            with patch.object(preparation,'RESOURCE_PREIMAGES',{name:runner.smoke.sha(raw)}):
                record=preparation.generate_resource_overlay(go,output,{str(source):runner.smoke.sha(raw)})
            self.assertIsNone(record['modfile']);self.assertFalse(record['observed'])
            preparation.verify_resource_overlay(record,time.monotonic()+2)
            source.write_bytes(raw+b'changed')
            with self.assertRaises(SmokeError):preparation.verify_resource_overlay(record,time.monotonic()+2)
            source.write_bytes(raw)
            (output/'resource-overlay'/source.name).write_text('tampered overlay')
            with self.assertRaises(SmokeError):preparation.verify_resource_overlay(record,time.monotonic()+2)


class LoadPrerequisiteTests(unittest.TestCase):
    def setUp(self):
        from unittest.mock import Mock
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.base=Path(self.temp.name).resolve();self.go=self.base/'go';self.go.mkdir()
        self.source=self.go/'source.go';self.source.write_text('package owning\n')
        self.sources={str(self.source):preparation.smoke.sha(self.source.read_bytes())}
        self.files=dict(self.sources);self.profile='profile';self.receipts={};self.hashes={}
        self.resource=Mock(go=self.go,env={});self.resource.inspect.return_value='source.go\0'
        for role in preparation.LOAD_PREREQUISITES:
            value=dict(status='PASS',errors=[],profile_sha256=self.profile,go_runtime=dict(version='go1.25.12'),
                before={str(self.go):'candidate'},after={str(self.go):'candidate'},native_credit=1,native=dict(bodies=1),
                cleanup=dict(errors=[],groups_absent=True))
            item=self.save(role+'.json',value);self.hashes[item['path']]=item['sha256']
            self.receipts[role]=dict(reason='complete controlled role/source union',attempts=[item])
        self.cost=dict(errors=[],acks=[dict(status=204,v1='v1',decoded_request_body_bytes=16,
            encoded_request_body_bytes=16,upstream_seconds=.01)],physical_samples=[dict(outcome_envelope_bytes=128)],peak_outcome_envelope_bytes=128)
        self.cost_binding=self.save('ack-storage.json',self.cost)
        self.proof=dict(schema_version=1,status='PASS',profile_sha256=self.profile,go_sources=self.sources,
            receipts=self.receipts,ack_storage=self.cost_binding)
        review=dict(status='approved-for-load-prerequisites',reviewer='independent controlled reviewer',profile_sha256=self.profile,
            receipt_hashes=self.hashes,go_sources_sha256=preparation.smoke.sha(preparation.planner.canonical_bytes(self.sources)),
            prerequisites_sha256=preparation.smoke.sha(preparation.planner.canonical_bytes(self.proof)),
            ack_storage_sha256=self.cost_binding['sha256'],required_roles=sorted(preparation.LOAD_PREREQUISITES))
        self.proof['completed_review']=self.save('review.json',review)
        self.binding=self.save('prerequisites.json',self.proof)

    def save(self,name,value):
        path=self.base/name;path.write_text(json.dumps(value))
        digest=preparation.smoke.sha(path.read_bytes());self.files[str(path)]=digest
        return dict(path=str(path),sha256=digest)

    def verify(self):
        with patch.object(preparation.smoke,'identity',return_value='candidate'):
            return preparation.verify_load_prerequisites(self.resource,self.binding,self.profile,self.files)

    def test_complete_current_reviewed_union_and_real_measurement_shape_are_required(self):
        result=self.verify()
        self.assertEqual(result['ack_request_count'],1)
        self.assertEqual(result['peak_outcome_envelope_bytes'],128)
        self.assertEqual(result['receipt_hashes'],self.hashes)

    def test_missing_role_unfrozen_artifact_changed_source_and_failed_native_cannot_pass(self):
        original=json.dumps(self.proof)
        del self.proof['receipts']['client_race'];self.binding=self.save('prerequisites.json',self.proof)
        with self.assertRaises(SmokeError):self.verify()
        self.proof=json.loads(original);self.binding=self.save('prerequisites.json',self.proof)
        self.source.write_text('external source drift')
        with self.assertRaises(SmokeError):self.verify()
        self.source.write_text('package owning\n')
        item=self.proof['receipts']['client_race']['attempts'][0];path=Path(item['path'])
        receipt=json.loads(path.read_text());receipt['native_credit']=0
        changed=self.save(path.name,receipt);item.update(changed);self.binding=self.save('prerequisites.json',self.proof)
        with self.assertRaises(SmokeError):self.verify()

    def test_review_binds_roles_reasons_and_measurements_without_auto_reuse(self):
        self.proof['receipts']['client_race']['reason']='different claim'
        self.binding=self.save('prerequisites.json',self.proof)
        with self.assertRaises(SmokeError):self.verify()

    def test_missing_ack_bytes_peak_or_success_status_blocks_admission(self):
        before=json.dumps(self.cost)
        for field,value in [('peak_outcome_envelope_bytes',0),('acks',[]),('errors',['capture failed'])]:
            self.cost=json.loads(before);self.cost[field]=value
            self.proof['ack_storage']=self.save('ack-storage.json',self.cost)
            self.binding=self.save('prerequisites.json',self.proof)
            with self.subTest(field=field),self.assertRaises(SmokeError):self.verify()


if __name__ == "__main__":
    unittest.main()

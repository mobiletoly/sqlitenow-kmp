#!/usr/bin/env python3
"""Explicit focused Oversqlite verifier/replay runs with owned platform resources.

This command records evidence; it grants no qualification reuse or acceptance.
"""
from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
import shutil
import signal
import secrets
import sys
import time
from urllib.parse import urlsplit

# Support both python -m and the documented file command from any directory.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from tools.qualification import oversqlite_smoke as smoke
from tools.qualification import platform_results as results
from tools.qualification import platform_preparation as preparation
from tools.qualification import planner
from tools.qualification.platform_resources import remaining
from tools.qualification.platform_resources import Resources
from tools.qualification.platform_resources import group_alive
from tools.qualification.platform_resources import RETAINED_POSTGRES_INSPECTION

ROOT = Path(__file__).resolve().parents[2]
MODULE = Path("platform-oversqlite-test/composeApp")


def write_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def verify_freeze(path: Path) -> str:
    data = json.loads(path.read_text())
    if not isinstance(data.get("files"), dict) or not data["files"]:
        raise smoke.SmokeError("freeze must pin nonempty absolute file map")
    for name, expected in data["files"].items():
        file = Path(name)
        if not file.is_absolute() or not file.is_file() or smoke.sha(file.read_bytes()) != expected:
            raise smoke.SmokeError("frozen dependency drift: " + name)
    return smoke.sha(path.read_bytes())


def clean_environment(source: dict[str, str]) -> dict[str, str]:
    for key, value in source.items():
        plain = key.removeprefix("SIMCTL_CHILD_")
        if plain=='OVERSQLITE_PROCESS_OWNER':continue  # Parent census tag, never SDK activation/authority.
        if value and (plain.startswith(("OVERSQLITE_", "TRANSITION_", "ROLLOUT_", "OVERSYNC_SMOKE_"))
                      or plain in ("TEST_DATABASE_URL", "DATABASE_URL", "OVERSYNC_EXAMPLE_TEST_DATABASE_URL")):
            raise smoke.SmokeError("unexpected inherited test/authority route: " + key)
    env = {key:value for key,value in source.items()if key.removeprefix('SIMCTL_CHILD_')!='OVERSQLITE_PROCESS_OWNER'}
    env.update(GOTOOLCHAIN="go1.25.12", PYTHONDONTWRITEBYTECODE="1")
    return env


def native_command(root: Path, mode: str, runtime: str, methods: list[str], env: dict,
                   device: str | None) -> list[str]:
    task = results.TASKS[runtime]
    cls = results.VERIFIER_CLASS if mode == "verifier" else results.REPLAY_CLASS
    argv = [str(root / "gradlew"), ":platform-oversqlite-test:composeApp:" + task]
    if runtime == "android":
        if not device or not device.startswith("emulator-") or env.get("ANDROID_SERIAL") != device:
            raise smoke.SmokeError("Android needs explicit owned emulator serial")
        if mode == "replay" and len(methods) != 1:
            raise smoke.SmokeError("Android replay executes one exact method per invocation")
        selector = cls if mode == "verifier" else cls + "#" + methods[0]
        argv.append("-Pandroid.testInstrumentationRunnerArguments.class=" + selector)
        for key, value in env.items():
            if key.startswith("OVERSQLITE_") and key not in ("OVERSQLITE_CONTRACTS_ROOT",):
                argv.append(f"-Pandroid.testInstrumentationRunnerArguments.{key}={value}")
        # Instrumentation task freshness is controlled by AGP; force only that
        # task, never all compile dependencies. Native XML/body audit is mandatory.
        argv.append("--rerun")
    else:
        for method in methods:
            argv.extend(["--tests", cls + "." + method])
        argv.append("--rerun")
        if runtime == "ios":
            if not device:
                raise smoke.SmokeError("iOS needs owned simulator UDID")
            argv.extend(["--device", device])
    return argv + ["--no-daemon", "--console=plain"]


def result_directories(root: Path, runtime: str) -> list[Path]:
    build = root / MODULE / "build"
    if runtime == "android":
        return [build / "outputs/androidTest-results/connected"]
    return [build / "test-results" / results.TASKS[runtime]]


def retain_reports(root: Path, runtime: str, output: Path, started_ns: int) -> tuple[list[dict], str]:
    copies = []
    for directory in result_directories(root, runtime):
        for path in sorted(directory.rglob("TEST-*.xml")):
            if path.stat().st_mtime_ns < started_ns:
                raise smoke.SmokeError("stale native result")
            target = output / "native" / path.relative_to(root / MODULE / "build")
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)
            copies.append(target)
    return results.native_results(copies, started_ns, results.TASKS[runtime])


def prepare_fixture(resources: Resources, output: Path) -> dict:
    start = time.monotonic()
    version = resources.inspect(["go", "env", "GOVERSION"])
    if version != "go1.25.12":
        raise smoke.SmokeError("fixture needs Go1.25.12")
    before = smoke.identity(resources.go, resources.env)
    fixture = output / "categories-authority"
    resources.command(["go", "build", "-o", str(fixture), "./integration/nethttp_server/cmd/a1_survivor_fixture"], 300, "build-fixture")
    # command uses the Kotlin cwd; use an explicit -C rather than a global cwd.
    if before != smoke.identity(resources.go, resources.env):
        raise smoke.SmokeError("Go source changed during fixture build")
    record = dict(binary=str(fixture), sha256=smoke.sha(fixture.read_bytes()), go_identity=before,
                  go_version=version, preparation_seconds=round(time.monotonic() - start, 6))
    write_json(output / "fixture-build.json", record)
    return record


def configure(resources: Resources, mode: str, runtime: str, fixture: Path | None) -> str | None:
    env = resources.env
    env.update(OVERSQLITE_PLATFORM_TESTS="true", OVERSQLITE_CONTRACTS_ROOT=str(resources.root / "oversqlite-contracts"),
               GO_OVERSYNC_REPO=str(resources.go))
    base = None
    if mode == "replay":
        if fixture is None:
            raise smoke.SmokeError("replay needs current prepared fixture")
        binding = json.loads((fixture.parent / "fixture-build.json").read_text())
        if binding["sha256"] != smoke.sha(fixture.read_bytes()) or binding["go_identity"] != smoke.identity(resources.go, env):
            raise smoke.SmokeError("fixture build/source identity mismatch")
        base = resources.authority(fixture)
        env.update(OVERSQLITE_REALSERVER_TESTS="true", OVERSQLITE_REALSERVER_HEAVY="true",
                   OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL=base, OVERSQLITE_CATEGORIES_REAL_SERVER_SMOKE_BASE_URL=base)
    device = None
    if runtime == "android":
        device = resources.android_device()
        if base:
            port = str(urlsplit(base).port)
            adb = str(Path(resources.android["sdk"]) / "platform-tools/adb")
            resources.inspect([adb, "-s", device, "reverse", "tcp:" + port, "tcp:" + port])
    elif runtime == "ios":
        device = resources.ios_device()
        for key, value in list(env.items()):
            if key.startswith("OVERSQLITE_"):
                env["SIMCTL_CHILD_" + key] = value
    return device


def profile_activation(options, profile: dict, digest: str, *, resources: Resources | None = None) -> tuple[str, dict]:
    if bool(getattr(options,'expected_freeze_sha256',None)) != bool(getattr(options,'expected_source_sha256',None)):
        raise smoke.SmokeError('campaign dispatch needs both exported freeze and source anchors')
    frozen = verify_freeze(options.freeze)
    if getattr(options,'expected_freeze_sha256',None) and frozen!=options.expected_freeze_sha256:
        raise smoke.SmokeError('campaign dispatch freeze differs from the exported anchor')
    if (getattr(options,'expected_source_sha256',None)
            and planner.profile_source_anchor(options.root,options.go_repo,
                inspector=resources.inspect if resources else None,
                deadline=resources.command_deadline if resources else None)!=options.expected_source_sha256):
        raise smoke.SmokeError('campaign dispatch source census differs from the exported anchor')
    data = json.loads(options.freeze.read_text())
    authorization = data.get('authorization', {})
    if (authorization.get('stage') != 'E42' or authorization.get('profile_sha256') != digest
            or data['files'].get(str(options.profile.resolve())) != digest):
        raise smoke.SmokeError('full-profile execution needs independently frozen E42 authorization/profile')
    owner = Path(authorization.get('record', ''))
    if not owner.is_absolute() or data['files'].get(str(owner)) != authorization.get('record_sha256'):
        raise smoke.SmokeError('missing frozen owner authorization record')
    record = json.loads(owner.read_text())
    if record.get('stage') != 'E42' or record.get('profile_sha256') != digest or not record.get('owner_request'):
        raise smoke.SmokeError('owner record does not activate this E42 profile')
    bindings = {}
    if options.bindings:
        path = options.bindings.resolve()
        if path.stat().st_mode & 0o077:
            raise smoke.SmokeError('typed binding data must be a private file')
        if data['files'].get(str(path)) != smoke.sha(path.read_bytes()):
            raise smoke.SmokeError('typed binding file is not independently frozen')
        bindings = json.loads(path.read_text())
    sources=['oversqlite_platform.py','oversqlite_smoke.py','planner.py','platform_resources.py','platform_preparation.py','platform_results.py']
    if getattr(options,'worker_contract',None):sources.append('campaign.py')
    for source in sources:
        for path in {options.root.resolve()/'tools/qualification'/source,Path(__file__).resolve().with_name(source)}:
            if data['files'].get(str(path)) != smoke.sha(path.read_bytes()):
                raise smoke.SmokeError('coordinator source missing from freeze: '+str(path))
    if getattr(options,'worker_contract',None):
        if resources is None:raise smoke.SmokeError('worker dispatch constraints need a bounded resource supervisor')
        resources.record['dispatch_worker_constraints']=verify_worker_contract(resources,options,profile)
    return frozen, bindings


def verify_worker_contract(resources: Resources, options, profile: dict) -> dict:
    """Recheck ignored mutable state at actual dispatch, after preparation, and cleanup."""
    from tools.qualification import campaign
    deadline=resources.command_deadline
    remaining(deadline,1)
    path=options.worker_contract.resolve()
    reference=dict(path=str(path),sha256=options.expected_worker_contract_sha256)
    contract,_=preparation.frozen_json(reference,{str(path):options.expected_worker_contract_sha256})
    worker,layout=contract['worker'],contract['layout']
    if (contract['entry']!=options.entry or contract.get('context')!=options.context
            or Path(worker['root']).resolve()!=options.root.resolve() or Path(worker['go']).resolve()!=options.go_repo.resolve()
            or Path(worker['freeze']).resolve()!=options.freeze.resolve()
            or Path(worker['bindings']).resolve()!=options.bindings.resolve()):
        raise smoke.SmokeError('dispatch worker constraint paths/entry/context changed')
    if not layout['private']:return dict(private=False,sha256=options.expected_worker_contract_sha256)
    folder=Path(layout['directory'])
    if folder.resolve()!=folder or folder.stat().st_mode&0o077:
        raise smoke.SmokeError('dispatch private worker directory changed')
    for output in layout['output_roots']:
        remaining(deadline,1)
        if not Path(output).resolve().is_relative_to(folder):raise smoke.SmokeError('dispatch private build/output became shared')
    for key,repository in (('root',options.root),('go',options.go_repo)):
        remaining(deadline,1)
        metadata=Path(resources.inspect(['git','rev-parse','--absolute-git-dir'],remaining(deadline,5),cwd=repository)).resolve()
        expected=layout['metadata'][key]
        if (metadata!=Path(expected['directory']) or not metadata.is_relative_to(folder)
                or (metadata/'index').samefile(expected['canonical_index'])):
            raise smoke.SmokeError('dispatch private Git metadata became shared')
    entry=planner.effective_profile_entry(profile,profile['schedule']['entries'][options.entry])
    campaign._profile_job_paths(entry,dict(worker,context=options.context),profile,resources.output)
    remaining(deadline,1)
    return dict(private=True,sha256=options.expected_worker_contract_sha256,output_roots=len(layout['output_roots']),
        metadata='private',effective_caches='private',deadline='inside remaining entry budget')


def rebase_profile_entry(entry: dict, profile: dict, options, freeze: dict) -> dict:
    if entry.get('kind')=='preparation':
        choices=dict(entry.get('environment_contexts',{}))
        if options.context in ('isolation-macos','isolation-browser','swift-generated-live-only'):
            if options.context=='swift-generated-live-only':
                basis='swift-bridge-macos';recipe=profile['schedule']['entries']['swift-generated-live-only']
            else:
                runtime=options.context.removeprefix('isolation-')
                basis='coordinated-heavy-macos' if runtime=='macos' else 'coordinated-live-browser'
                recipe=profile['schedule']['entries']['retained-'+options.context]
            if basis not in choices:raise smoke.SmokeError('missing adopted preparation basis for named support context')
            choices[options.context]=dict(choices[basis])|recipe['environment']
            if options.context.startswith('isolation-'):
                runtime=options.context.removeprefix('isolation-')
                entry=dict(entry,argv=[*entry['argv'],'-Pkotlin.compiler.execution.strategy=in-process',
                    '-Pkonan.data.dir=binding.worker_'+runtime+'_konan'])
        if options.context not in choices:
            raise smoke.SmokeError('preparation requires exact consuming environment context')
        entry=dict(entry,environment=entry['environment']|choices[options.context])
        if entry.get('task_ids') and profile.get('schedule'):
            if options.context.startswith('isolation-'):
                consumers=[planner.effective_profile_entry(profile,profile['schedule']['entries']['retained-'+options.context])]
            elif options.context=='swift-generated-live-only':
                consumers=[profile['schedule']['entries']['swift-generated-live-only']]
            else:
                consumers=[item for item in profile['schedule']['entries'].values() if item.get('context')==options.context]
            if consumers:
                required={task for consumer in consumers for task in consumer.get('preparation_tasks',[])}
                requested=[task for task in entry['task_ids'] if task in required]
                if not requested:raise smoke.SmokeError('preparation group has no owning task in the selected context')
                argv=[];excluded=False
                for value in entry['argv']:
                    if excluded:
                        argv.append(value);excluded=False
                    elif value=='-x':
                        argv.append(value);excluded=True
                    elif value not in entry['task_ids'] or value in requested:
                        argv.append(value)
                entry=dict(entry,argv=argv,task_ids=requested,catalog_group_task_ids=entry['task_ids'])
    actual = dict(kotlin=str(options.root.resolve()), go=str(options.go_repo.resolve()))
    expected = profile['roots']
    for name, root in actual.items():
        if root != expected[name] and freeze.get('root_rebindings', {}).get(expected[name]) != root:
            raise smoke.SmokeError('private worker needs explicitly frozen source-root rebinding')
    replacements={original:actual[name] for name,original in expected.items()}
    pattern=re.compile('|'.join(re.escape(original)+r'(?=/|$)' for original in sorted(replacements,key=len,reverse=True)))
    def rebase(value):
        # A worker can live inside the other canonical repository's ignored
        # directory. Replace original matches once; never rescan replacement text.
        return pattern.sub(lambda match:replacements[match.group(0)],value)
    result = dict(entry)
    result['argv'] = list(map(rebase,entry['argv']))
    result['cwd'] = rebase(entry['cwd'])
    result['environment'] = {key:rebase(value)for key,value in entry['environment'].items()}
    return result


def lane_environment(source: dict[str,str]) -> dict[str,str]:
    names = ('PATH','HOME','USER','LOGNAME','SHELL','TMPDIR','LANG','LC_ALL','LC_CTYPE','TERM',
             'JAVA_HOME','DEVELOPER_DIR','SDKROOT','ANDROID_HOME','ANDROID_SDK_ROOT',
             'GOPATH','GOROOT','GOCACHE','GOMODCACHE','GRADLE_USER_HOME','GRADLE_OPTS','JAVA_OPTS',
             'KONAN_DATA_DIR','KOTLIN_USER_HOME','CHROME_BIN','YARN_CACHE_FOLDER','NPM_CONFIG_CACHE')
    result={key:source[key]for key in names if key in source} | dict(GOTOOLCHAIN='go1.25.12',PYTHONDONTWRITEBYTECODE='1')
    sdk=Path(result.get('ANDROID_HOME',str(Path.home()/'Library/Android/sdk')))
    if sdk.is_dir():
        result.update(ANDROID_HOME=str(sdk),ANDROID_SDK_ROOT=str(sdk))
    return result


def profile_reports(root: Path, entry: dict) -> list[Path]:
    task = entry.get('task','')
    parts = task.strip(':').split(':')
    if len(parts)<2:
        return []
    build = root.joinpath(*parts[:-1])/'build'
    return [build/'outputs/androidTest-results/connected'] if entry.get('runtime')=='android' else [build/'test-results'/parts[-1]]


def web_deadline_policy(entry: dict) -> str | None:
    owners=entry.get('expected_source_owners',[]);task=entry.get('task')
    backlog='dev.goquick.sqlitenow.oversqlite.SettlementTest.backlogBound'
    dispatch='dev.goquick.sqlitenow.oversqlite.AutomaticRecoveryTest.uploadAndSnapshotShareDispatchBudget'
    if owners==[backlog]and task in(':library-oversqlite:jsNodeTest',':library-oversqlite:jsBrowserTest',':library-oversqlite:wasmJsBrowserTest'):
        return'single-backlog'
    if dispatch in owners and task==':library-oversqlite:jsNodeTest':return'named-dispatch'
    return None


def web_deadline_support(resources: Resources, entry: dict, argv: list[str], frozen: dict) -> tuple[list[str],dict]:
    policy=web_deadline_policy(entry)
    if policy is None:return argv,{}
    folder=resources.output/'web-deadline';folder.mkdir(mode=0o700)
    profile=resources.root/'tools/qualification/profiles'
    script=profile/('library-backlog.init.gradle'if policy=='single-backlog'else'web-dispatch-deadline.init.gradle')
    inputs=[script]
    if policy=='single-backlog'and entry['task']!=':library-oversqlite:jsNodeTest':
        karma=folder/'karma';karma.mkdir(mode=0o700)
        last='zzzz-qualification-timeout.js'
        for source in sorted((resources.root/'library-oversqlite/karma.config.d').glob('*.js')):
            if source.name>=last:raise smoke.SmokeError('Karma deadline override must follow every normal config')
            inputs.append(source);shutil.copyfile(source,karma/source.name)
        timeout=profile/'backlog-karma/timeout.js';inputs.append(timeout)
        shutil.copyfile(timeout,karma/last)
    for path in inputs:
        if path.is_symlink()or frozen.get(str(path))!=smoke.sha(path.read_bytes()):
            raise smoke.SmokeError('web deadline source/config is not frozen: '+str(path))
    if entry['task']==':library-oversqlite:jsNodeTest':
        exception='SettlementTest.backlogBound'if policy=='single-backlog'else'AutomaticRecoveryTest.uploadAndSnapshotShareDispatchBudget'
        milliseconds=480000 if policy=='single-backlog'else 120000
        hook=''''use strict';
const fs = require('node:fs');
const began = new WeakMap();
function identity(test) {
  if (!test.parent || !test.parent.parent || test.parent.parent.title !== 'dev.goquick.sqlitenow.oversqlite') throw new Error('Unexpected qualification Node package');
  return test.parent.title + '.' + test.title;
}
function write(value) {
  if (!process.env.OVERSQLITE_WEB_DEADLINE_AUDIT) throw new Error('Missing owned deadline audit');
  fs.appendFileSync(process.env.OVERSQLITE_WEB_DEADLINE_AUDIT, JSON.stringify(value) + '\\n');
}
exports.mochaHooks = {
  beforeEach() {
    const test = this.currentTest, name = identity(test);
    const milliseconds = name === __EXCEPTION__ ? __MILLISECONDS__ : 30000;
    test.timeout(milliseconds); began.set(test, process.hrtime.bigint());
    write({phase: 'deadline', name, milliseconds});
  },
  afterEach() {
    const test = this.currentTest;
    write({phase: 'completion', name: identity(test), state: test.state, milliseconds: test.timeout(),
      elapsedMilliseconds: Number(process.hrtime.bigint() - began.get(test)) / 1e6});
  }
};
'''.replace('__EXCEPTION__',json.dumps(exception)).replace('__MILLISECONDS__',str(milliseconds))
        (folder/'node-hook.cjs').write_text(hook)
    # Karma executes every fragment in this directory. Bind its complete
    # membership so an added late-sorting override cannot evade the seal.
    immutable=[str(folder/'karma')]if(folder/'karma').exists()else[str(path)for path in folder.rglob('*')if path.is_file()]
    result=list(argv)
    if str(script)not in result:result=result[:4]+['-I',str(script)]+result[4:]
    if any(arg.startswith('-Pqualification.web')for arg in result):raise smoke.SmokeError('web deadline properties belong to this new run')
    result+=['-Pqualification.webTask='+entry['task'],'-Pqualification.webOutput='+str(folder)]
    return result,dict(policy=policy,task=entry['task'],folder=str(folder),sources={str(path):frozen[str(path)]for path in inputs},
        roots=immutable,files=preparation.file_map(immutable,resources.command_deadline))


def worker_browser_support(resources: Resources, entry: dict, argv: list[str], frozen: dict) -> tuple[list[str], dict]:
    """Contain reviewed injected crashes without hiding transport or Mocha failure."""
    if entry.get('kind') == 'preparation' or entry.get('task') not in (
            ':library-core:jsBrowserTest', ':library-core:wasmJsBrowserTest'):
        return argv, {}
    prefix = 'dev.goquick.sqlitenow.core.worker.SqliteWorkerDriverTest.'
    crashes = {prefix + 'oneWayFailuresRemainOrderedAcrossStickyTerminalTransition': 'terminal after queued one-way failures',
               prefix + 'postReadyWorkerFailureTerminatesAndFailsCurrentAndFutureRequests': 'controlled post-ready failure'}
    source = resources.root / 'library-core/src/webTest/kotlin/dev/goquick/sqlitenow/core/worker/SqliteWorkerDriverTest.kt'
    worker_path = '/sqlitenow-worker-v1/sqlite-3.53.0-build1/worker.mjs'
    assets = resources.root / 'library-core/src/webMain/resources/sqlitenow-worker-v1'
    configs = sorted((resources.root / 'library-core/karma.config.d').glob('*.js'))
    inputs = [source, assets / 'client.mjs', assets / 'sqlite-3.53.0-build1/worker.mjs', *configs]
    if not configs:
        raise smoke.SmokeError('worker Browser support requires the complete normal Karma configuration')
    for path in inputs:
        if path.is_symlink() or frozen.get(str(path)) != smoke.sha(path.read_bytes()):
            raise smoke.SmokeError('worker Browser source/config is not frozen: ' + str(path))
    for message in crashes.values():
        if source.read_text().count('driver.failWorkerForTest(' + json.dumps(message) + ')') != 1:
            raise smoke.SmokeError('reviewed worker crash seam changed')
    faults = {owner: [dict(field='crash', value=message, message=message,
                          count=2 if owner.endswith('postReadyWorkerFailureTerminatesAndFailsCurrentAndFutureRequests') else 1)]
              for owner, message in crashes.items()}
    handoff = prefix + 'responseHandoffFailureModesBecomeStickyAndTerminateWithoutRetainedResponses'
    handoff_body = source.read_text().split('fun responseHandoffFailureModesBecomeStickyAndTerminateWithoutRetainedResponses()', 1)[1].split('\n    @Test', 1)[0]
    if handoff_body.count('mode = "throw",') != 1 or handoff_body.count('mode = "throw-release",') != 1:
        raise smoke.SmokeError('reviewed acknowledgement/release fault seams changed')
    faults[handoff] = [dict(field='acknowledgementMode', value='throw', message='controlled acknowledgement failure', count=1),
                       dict(field='releaseMode', value='throw', message='controlled response release failure', count=1)]
    faults = {owner: specs for owner, specs in faults.items() if owner in entry['expected_source_owners']}
    crashes = {owner: specs[0]['message'] for owner, specs in faults.items()}
    crash_counts = {owner: sum(spec['count'] for spec in specs) for owner, specs in faults.items()}
    error_counts = {owner: {spec['message']: spec['count'] for spec in specs} for owner, specs in faults.items()}
    folder = resources.output / 'worker-browser'; folder.mkdir(mode=0o700)
    karma = folder / 'karma'; karma.mkdir(mode=0o700)
    last = 'zzzz-qualification-worker.js'
    for path in configs:
        if path.name >= last:
            raise smoke.SmokeError('worker observation fragment must follow the normal Karma configuration')
        shutil.copyfile(path, karma / path.name)
    nonce = os.urandom(16).hex()
    hook = folder / 'client.js'
    hook.write_text(r'''(function () {
 'use strict';
 const karma=window.__karma__, expected=__FAULTS__, nonce=__NONCE__;
 const info=karma.info.bind(karma), complete=karma.complete, run=window.mocha.run;
 const NativeWorker=window.Worker, post=NativeWorker.prototype.postMessage, armed=new WeakMap(), owned=new WeakMap();
 const workerPath=__WORKER_PATH__, workerUrl=new URL(workerPath,window.location.href).href;
 let current=null, runner=null, sequence=0;
 function emit(value) { info({log:'E42_WORKER:'+nonce+':'+JSON.stringify(value),type:'log'}); }
 function owner(test) {
   if (!test || !test.parent || !test.parent.parent) throw new Error('Missing worker test identity');
   return test.parent.parent.title+'.'+test.parent.title+'.'+test.title;
 }
 window.Worker=new Proxy(NativeWorker,{construct(target,args,newTarget) {
   const worker=Reflect.construct(target,args,newTarget);
   if (current && Object.prototype.hasOwnProperty.call(expected,current) && args[1] && args[1].type==='module' &&
       new URL(String(args[0]),window.location.href).href===workerUrl) owned.set(worker,{owner:current,url:workerUrl});
   return worker;
 }});
 NativeWorker.prototype.postMessage=function(message) {
   const created=owned.get(this);
   const spec=current && message && message.kind==='test-control' && (expected[current]||[]).find(item=>
     message[item.field]===item.value && (item.field!=='acknowledgementMode' || message.acknowledgementCommand===undefined));
   if (current && created && created.owner===current && Object.prototype.hasOwnProperty.call(expected,current) && message &&
       spec) {
     if (armed.has(this)) throw new Error('Duplicate injected crash for owned worker');
     const arm={id:++sequence,owner:current,message:spec.message,consumed:false};
     armed.set(this,arm);emit({phase:'armed',id:arm.id,owner:arm.owner,message:arm.message,workerPath:workerPath});
     this.addEventListener('error',function(event) {
       if (event.currentTarget===this && event.cancelable===true && event.filename===workerUrl &&
           [arm.message,'Uncaught Error: '+arm.message].includes(event.message) && !arm.consumed) {
         event.preventDefault();arm.consumed=true;
         emit({phase:'expected-error',id:arm.id,owner:arm.owner,message:arm.message});
       }
     });
   }
   return post.apply(this,arguments);
 };
 window.addEventListener('error',event=>emit({phase:'global-error',message:String(event.message)}));
 window.addEventListener('unhandledrejection',event=>emit({phase:'global-error',message:String(event.reason)}));
 window.mocha.run=function() {
   runner=run.apply(this,arguments);
   runner.on('test',test=>{current=owner(test);emit({phase:'test',owner:current});});
   runner.on('test end',test=>{emit({phase:'result',owner:owner(test),state:test.state,pending:test.pending===true});current=null;});
   runner.on('fail',(test,error)=>emit({phase:'fail',owner:test && test.type==='test'?owner(test):null,message:String(error)}));
   return runner;
 };
 karma.complete=function() {
   emit({phase:'terminal',failures:runner?runner.failures:null,abort:runner?runner._abort===true:null,total:runner?runner.total:null});
   return complete.apply(this,arguments);
 };
})();
'''.replace('__FAULTS__', json.dumps(faults)).replace('__NONCE__', json.dumps(nonce)).replace('__WORKER_PATH__', json.dumps(worker_path)))
    fragment = karma / last
    fragment.write_text(r'''const e42fs=require('node:fs'), e42out=__OUTPUT__, e42prefix=__PREFIX__;
function E42WorkerReporter() {
 this.onBrowserLog=function(browser,log) {
   if (String(log).includes(e42prefix)) e42fs.appendFileSync(e42out,JSON.stringify({browser:browser.id,log:String(log)})+'\n');
 };
 this.onBrowserError=function(browser,error) {
   e42fs.appendFileSync(e42out,JSON.stringify({browser:browser.id,error:String(error)})+'\n');
 };
}
if (!Array.isArray(config.files) || !String(typeof config.files[0]==='string'?config.files[0]:config.files[0].pattern).endsWith('kotlin-test-karma-runner.js')) throw new Error('Unexpected Kotlin Karma runner order');
config.files.splice(1,0,{pattern:__CLIENT__,included:true,served:true,watched:false});
config.plugins=(config.plugins||['karma-*']).concat([{'reporter:e42-worker':['type',E42WorkerReporter]}]);
config.reporters=['e42-worker'].concat(config.reporters||[]);
'''.replace('__OUTPUT__', json.dumps(str(folder / 'events.jsonl')))
        .replace('__PREFIX__', json.dumps('E42_WORKER:' + nonce + ':')).replace('__CLIENT__', json.dumps(str(hook))))
    init = folder / 'capture.init.gradle'
    project, task = entry['task'].rsplit(':', 1)
    init.write_text('''gradle.projectsEvaluated {
 if (!rootProject.file('library-core/build.gradle.kts').isFile()) return
 rootProject.project(__PROJECT__).tasks.named(__TASK__).configure {
  doFirst { testFramework.useConfigDirectory(new File(__KARMA__)) }
 }
}
'''.replace('__PROJECT__', json.dumps(project)).replace('__TASK__', json.dumps(task)).replace('__KARMA__', json.dumps(str(karma))))
    roots = [str(karma), str(hook), str(init)]
    return argv[:4] + ['-I', str(init)] + argv[4:], dict(folder=str(folder), nonce=nonce, expected_crashes=crashes,
        expected_crash_counts=crash_counts,
        expected_error_counts=error_counts,
        worker_path=worker_path,
        roots=roots, files=preparation.file_map(roots, resources.command_deadline),
        sources={str(path): frozen[str(path)] for path in inputs})


def swift_capture_argv(argv: list[str], task: str, output: Path, script: Path) -> list[str]:
    report=output/'native-swift.xml'
    if report.exists() or report.is_symlink() or (output/'swift-command.json').exists():
        raise smoke.SmokeError('Swift capture requires new attempt outputs')
    selected=[x.split('=',1)[1]for x in argv if x.startswith('-Pqualification.swiftTask=')]
    if selected and selected!=[task]:raise smoke.SmokeError('Swift capture task differs from selected entry')
    if any(x.startswith('-Pqualification.swiftXunit=')for x in argv):
        raise smoke.SmokeError('Swift xUnit destination is owned by this new attempt')
    result=list(argv)
    if str(script) not in result:result+=['-I',str(script)]
    if not selected:result+=['-Pqualification.swiftTask='+task]
    result+=['-Pqualification.swiftXunit='+str(report)]
    return result


def full_go_runtime(resources: Resources) -> tuple[list[Path], dict]:
    actual = resources.inspect(['go','env','GOVERSION','GOTOOLDIR'],5).splitlines()
    if (len(actual)!=2 or actual[0]!='go1.25.12' or not Path(actual[1]).is_absolute()
            or Path(actual[1]).parent.name!='tool' or Path(actual[1]).parent.parent.name!='pkg'):
        raise smoke.SmokeError('Go lane requires the actual Go1.25.12 toolchain')
    launcher = shutil.which('go',path=resources.env.get('PATH',''))
    if not launcher:raise smoke.SmokeError('Go launcher is unavailable')
    toolchain = Path(actual[1]).parents[2]
    return ([Path(launcher).resolve(),toolchain/'bin/go',toolchain/'VERSION'],
            dict(version=actual[0],tool_directory=actual[1],launcher=launcher))


def verify_build_artifact(artifact: dict, output: Path) -> None:
    path = Path(artifact['path'])
    if (not path.is_absolute() or not path.resolve().is_relative_to(output.resolve())
            or path.is_symlink() or not path.is_file()
            or path.stat().st_mode & 0o777 != artifact['mode']
            or not artifact['mode'] & 0o111 or smoke.sha(path.read_bytes())!=artifact['sha256']):
        raise smoke.SmokeError('owned build artifact changed before terminal binding')


def profile_source_identities(resources: Resources) -> dict:
    return {str(root):smoke.identity(root,resources.env,inspector=resources.inspect)
            for root in(resources.root,resources.go)}


def load_sequence_builds(options, resources: Resources, profile: dict, record: dict) -> dict:
    artifacts={}
    deadline=record['coordinator_started']+420
    for entry,name in [('load-prepare-1','load-authority'),('load-prepare-2','load-client.test')]:
        if remaining(deadline,420)<profile['schedule']['entries'][entry]['wall_seconds']:
            raise smoke.SmokeError('insufficient measured preparation budget for next load build')
        output=resources.output/entry
        args=['lane','--root',str(resources.root),'--go-repo',str(resources.go),'--profile',str(options.profile),
              '--entry',entry,'--freeze',str(options.freeze),'--output',str(output)]
        if options.bindings:args+=['--bindings',str(options.bindings)]
        if main(args):raise smoke.SmokeError('load sequence preparation failed: '+entry)
        receipt=json.loads((output/'receipt.json').read_text())
        if receipt.get('status')!='PASS' or receipt.get('native_credit')!=0 or receipt.get('entry_id')!=entry:
            raise smoke.SmokeError('load build lacks its successful exact preparation receipt')
        verify_build_artifact(receipt['build_artifact'],output)
        artifacts[name]=receipt['build_artifact']
        record.setdefault('load_preparation',{})[entry]=dict(path=str(output/'receipt.json'),
            sha256=smoke.sha((output/'receipt.json').read_bytes()))
    return artifacts


def execute_load_sequence(options, resources: Resources, record: dict, profile: dict,
                          digest: str, frozen: str, private: dict) -> None:
    started=record['coordinator_started'];deadline=started+1620
    resources.command_deadline=deadline-30;resources.cleanup_deadline=deadline
    record.update(entry_id='load-sequence',profile_sha256=digest,freeze_sha256=frozen,job_deadline=deadline,
        wall_seconds=1620,native_credit=0,sequence_complete=False,
        before=profile_source_identities(resources))
    files=json.loads(options.freeze.read_text())['files']
    if not options.bindings or not private.get('load_prerequisites'):
        raise smoke.SmokeError('load sequence requires frozen reviewed structural/ack/storage prerequisites')
    if any(key.startswith('lease.')for key in private.get('values',{})):
        raise smoke.SmokeError('load sequence creates its own fresh authority; inherited leases are refused')
    record['load_prerequisites']=preparation.verify_load_prerequisites(resources,private['load_prerequisites'],digest,files)
    artifacts=load_sequence_builds(options,resources,profile,record)
    if remaining(deadline,1620)<1200:raise smoke.SmokeError('insufficient complete1200s load fixture allowance after preparation')
    resources.acquire_exclusive('measurement-exclusive')
    fixture_started=time.monotonic();fixture_wall=time.time_ns();fixture_deadline=fixture_started+1200
    if fixture_deadline>deadline:raise smoke.SmokeError('load fixture cannot fit the remaining logical composition')
    resources.command_deadline=fixture_deadline-30;resources.cleanup_deadline=fixture_deadline
    record.update(job_deadline=fixture_deadline,fixture_started_unix_ns=fixture_wall,
                  fixture_started_monotonic=fixture_started,
                  fixture_seconds=1200,readiness_seconds=60,cleanup_seconds=30)
    resources.command_deadline=fixture_started+60
    resources.env['JWT_SECRET']=secrets.token_urlsafe(48)
    base=resources.authority(Path(artifacts['load-authority']['path']),profile='load',private_ready=True,
        expected_app='oversqlite-qualification-load',readiness_deadline=fixture_started+60,
        expected_marker='QUALIFICATION_LOAD_AUTHORITY_PROFILE history_projections=32 product_default=4 max_allowed=32')
    ready=resources.record['private_readiness']
    if (ready.get('details')!={'history_projection_concurrency':32,'product_default':4}
            or resources.sql('SELECT (SELECT count(*) FROM sync.user_state),(SELECT count(*) FROM sync.source_state)',ready['database'])!='0|0'):
        raise smoke.SmokeError('load rich profile32/empty state differs before the first member')
    nonce=ready['nonce'];dsn=urlsplit(resources.env['OVERSYNC_EXAMPLE_TEST_DATABASE_URL'])
    observer=resources.http_observer(base,ready['database'],nonce=nonce)
    record['readiness_seconds_actual']=time.monotonic()-fixture_started
    if record['readiness_seconds_actual']>60:raise smoke.SmokeError('complete load fixture readiness exceeded60s')
    values=dict(private.get('values',{}))|{'binding.load_client_binary':artifacts['load-client.test']['path'],
        'lease.load.url':observer.url,'lease.load.database_dsn':dsn._replace(path='/'+ready['database']).geturl(),
        'lease.load.jwt_secret':resources.env['JWT_SECRET']}
    values.setdefault('binding.java_home',resources.env.get('JAVA_HOME',''))
    record['load_lease']=dict(authority_pid=ready['pid'],authority_group=ready['pgid'],database=ready['database'],
        application='oversqlite-qualification-load',reset_domain=ready['reset_domain'],nonce=nonce,authority_url=base)
    record['load_lease']['client_url']=observer.url
    record['load_phases']=[]
    for users in (1,5,10,25):
        entry=profile['schedule']['entries']['phase-load-'+str(users)]
        if remaining(fixture_deadline,1200)<entry['wall_seconds']+30:
            raise smoke.SmokeError('insufficient unchanged next-phase budget plus shared cleanup')
        output=resources.output/('users-'+str(users));output.mkdir(mode=0o700)
        tmp=output/'tmp';tmp.mkdir(mode=0o700)
        phase_values=values|{'binding.phase_output':str(output),'binding.entry_tmp':str(tmp)}
        # This is the only internal phase dispatch. Public selection remains closed.
        resolved=planner.resolve_profile_entry(dict(entry,internal_only=False),phase_values)
        resources.env.update(resolved['environment']);resources.env['OVERSQLITE_LOAD_NONCE']=nonce
        observer.set_phase('load-'+str(users))
        phase_started=time.monotonic();phase_wall=time.time_ns();phase_deadline=phase_started+entry['wall_seconds']
        resources.command_deadline=phase_deadline
        row=dict(entry_id=entry['id'],users=users,concurrency=users,sequence_complete=False,lease=record['load_lease'],
                 argv=resolved['argv'],wall_seconds=entry['wall_seconds'],status='RUNNING')
        record['load_phases'].append(row)
        try:
            resources.command(resolved['argv'],entry['native_seconds'],'phase-'+str(users),cwd=output)
            # Keep the native contract's exact report path under its phase output.
            shutil.copy2(resources.output/('phase-'+str(users)+'.log'),output/'native.log')
            row['native']=results.audit_load_phase(output,users=users,nonce=nonce,started_ns=phase_wall,ended_ns=time.time_ns())
            if group_alive(row['native']['group']):raise smoke.SmokeError('completed load native group remains alive')
            observer.wait_idle(phase_deadline)
            observed=observer.report();row['ack_storage']=results.audit_load_observation(observed,phase='load-'+str(users),nonce=nonce)
            observation=output/'http-observation.json';write_json(observation,observed)
            row['observation']=dict(path=str(observation),sha256=smoke.sha(observation.read_bytes()))
            row['physical']=resources.sql_json("BEGIN READ ONLY; SELECT json_build_object('database_bytes',pg_database_size(current_database()),'outcomes',(SELECT count(*) FROM sync.push_outcomes),'sources',(SELECT count(*) FROM sync.source_state)); COMMIT;",ready['database'])
            row['elapsed_seconds']=time.monotonic()-phase_started
            row['wall_seconds_actual']=(time.time_ns()-phase_wall)/1e9
            if time.monotonic()>phase_deadline or abs(row['wall_seconds_actual']-row['elapsed_seconds'])>2:
                raise smoke.SmokeError('complete load phase exceeded its cap or was suspended')
            if verify_freeze(options.freeze)!=frozen:raise smoke.SmokeError('load source changed between members')
            row['status']='PASS'
        except BaseException:
            row['status']='FAIL';write_json(output/'receipt.json',row);raise
        write_json(output/'receipt.json',row)
        row['receipt']=dict(path=str(output/'receipt.json'),sha256=smoke.sha((output/'receipt.json').read_bytes()))
        resources.command_deadline=fixture_deadline-30
    record['native_credit']=4
    record['native']=dict(bodies=4,verified_users=41)
    record['load_sequence_pending']=dict(artifacts=artifacts,freeze_sha256=frozen,
        fixture_started_monotonic=fixture_started,fixture_started_unix_ns=fixture_wall,phase_users=[1,5,10,25])
    record['after']=profile_source_identities(resources)


def seal_load_sequence(resources: Resources, record: dict) -> None:
    pending=record['load_sequence_pending']
    if (record.get('cleanup',{}).get('groups_absent')is not True or record['cleanup'].get('errors')
            or [x['users']for x in record['load_phases']]!=[1,5,10,25]
            or any(x['status']!='PASS' or x['sequence_complete']is not False or x['lease']!=record['load_lease']
                   or x['concurrency']!=x['users'] or x['native']['verified_users']!=x['users']
                   or x['native']['nonce']!=record['load_lease']['nonce']
                   for x in record['load_phases'])):
        raise smoke.SmokeError('load sequence does not contain four fresh ordered members of one lease')
    retained=[user for row in record['load_phases']for user in row['native']['retained']]
    if len(retained)!=41 or len({x['path']for x in retained})!=41 or len({x['user_id']for x in retained})!=41:
        raise smoke.SmokeError('load sequence reused a prior member user/SQLite identity')
    for receipt in record['load_preparation'].values():
        if smoke.sha(Path(receipt['path']).read_bytes())!=receipt['sha256']:
            raise smoke.SmokeError('load preparation receipt changed through fixture cleanup')
    for artifact in pending['artifacts'].values():verify_build_artifact(artifact,resources.output)
    for row in record['load_phases']:
        required=[row['receipt'],row['native']['report'],row['observation'],
            dict(path=str(resources.output/('users-'+str(row['users']))/'native.log'),sha256=row['native']['native_sha256'])]
        required += [file for user in row['native']['retained']for file in user['files']]
        for item in required:
            path=Path(item['path'])
            if path.is_symlink() or not path.is_file() or smoke.sha(path.read_bytes())!=item['sha256']:
                raise smoke.SmokeError('load member/native/retained evidence changed through cleanup')
    active=time.monotonic()-pending['fixture_started_monotonic']
    wall=(time.time_ns()-pending['fixture_started_unix_ns'])/1e9
    if abs(wall-active)>2 or active>1200:raise smoke.SmokeError('load fixture clock exceeded or was suspended')
    record['fixture_elapsed_seconds']=active;record['fixture_wall_seconds_actual']=wall
    record['load_sequence_verified']=True


def execute_resource(options, resources: Resources, record: dict, profile: dict,
                     digest: str, frozen: str, private: dict) -> None:
    name=options.entry;observed=name=='resource-go-observed'
    logical_deadline=record['coordinator_started']+420
    resources.command_deadline=logical_deadline-30;resources.cleanup_deadline=logical_deadline
    record.update(entry_id=name,profile_sha256=digest,freeze_sha256=frozen,job_deadline=logical_deadline,
        composition_seconds=420,orchestration_only=True,native_credit=0,before=profile_source_identities(resources))
    build_id='contract-'+name+'-1';build_output=resources.output/'prepare-binary'
    args=['lane','--root',str(resources.root),'--go-repo',str(resources.go),'--profile',str(options.profile),
          '--entry',build_id,'--freeze',str(options.freeze),'--output',str(build_output)]
    if options.bindings:args+=['--bindings',str(options.bindings)]
    if main(args):raise smoke.SmokeError('resource diagnostic binary preparation failed')
    producer=build_output/'receipt.json';built=json.loads(producer.read_text())
    if built.get('status')!='PASS'or built.get('entry_id')!=build_id or built.get('native_credit')!=0:
        raise smoke.SmokeError('resource binary lacks its exact successful nonbody preparation')
    verify_build_artifact(built['build_artifact'],build_output)
    preparation.verify_resource_overlay(built['resource_overlay'],resources.command_deadline)
    record['resource_preparation']=dict(path=str(producer),sha256=smoke.sha(producer.read_bytes()))
    if remaining(logical_deadline,420)<180:raise smoke.SmokeError('resource root cannot fit full native150 and cleanup30')
    resources.acquire_exclusive('measurement-exclusive')
    spec=profile['schedule']['entries']['contract-'+name+'-2']
    native_started=time.monotonic();deadline=min(logical_deadline,native_started+spec['wall_seconds'])
    record['job_deadline']=deadline;resources.command_deadline=deadline-30;resources.cleanup_deadline=deadline
    temporary=resources.output/'tmp';temporary.mkdir(mode=0o700)
    diagnostics=resources.output/'diagnostics';diagnostics.mkdir(mode=0o700)
    values=dict(private.get('values',{}))|{'binding.replay_binary':built['build_artifact']['path'],
        'binding.entry_tmp':str(temporary),'binding.entry_diagnostics':str(diagnostics)}
    entry=rebase_profile_entry(spec,profile,options,json.loads(options.freeze.read_text()))
    resolved=planner.resolve_profile_entry(entry,values);resources.env.update(resolved['environment'])
    engines,record['go_runtime']=full_go_runtime(resources);files=json.loads(options.freeze.read_text())['files']
    for path in [*engines,Path(resolved['argv'][0]),Path('/usr/bin/time')]:
        if files.get(str(path))!=smoke.sha(path.read_bytes()):raise smoke.SmokeError('resource execution engine/wrapper is not frozen')
    native=dict(entry_id=entry['id'],argv=resolved['argv'],wall_seconds=180,native_seconds=150,
        cleanup_seconds=30,started_unix_ns=time.time_ns(),status='RUNNING')
    record['resource_native']=native
    record['file_sampling']=resources.sampled_files_command(resolved['argv'],temporary,
        remaining(resources.command_deadline,150),cwd=Path(resolved['cwd']))
    record['native']=results.audit_resource_diagnostics(resources.output,native['started_unix_ns'],observed)
    if record['file_sampling']['samples']<2 or record['file_sampling']['sampled_peak_file_bytes'].get('sqlite',0)<=0:
        raise smoke.SmokeError('resource run has no positive owned SQLite file samples')
    native.update(status='PASS',elapsed_before_cleanup_seconds=time.monotonic()-native_started)
    write_json(resources.output/'file-sampling.json',record['file_sampling'])
    roots=[str(diagnostics),str(resources.output/'file-sampling.json'),str(resources.output/'native-command.log')]
    record.update(native_credit=5,after=profile_source_identities(resources),
        resource_pending=dict(roots=roots,files=preparation.file_map(roots,resources.command_deadline),
            artifact=built['build_artifact'],producer=record['resource_preparation'],overlay=built['resource_overlay']))


def retained_postgres(resources: Resources, database: str) -> dict:
    inspection=resources.sql_json(RETAINED_POSTGRES_INSPECTION,database)
    zero=('business_parent_orphans','row_state_deleted','duplicate_replay_identities','effect_parent_orphans',
          'effect_count_mismatches','effect_ordinal_gaps','binding_orphans','snapshot_sessions','snapshot_rows',
          'capture_stage','server_reservations','write_receipts','outcomes','duplicate_outcome_sources')
    if (any(inspection[name]!=0 for name in zero)or inspection['business_rows']!=125
            or inspection['recovered_business_rows']!=124 or inspection['row_state_rows']!=125
            or inspection['constraints_validated']is not True):
        raise smoke.SmokeError('retained PostgreSQL payload/metadata/constraint reconciliation failed')
    write_json(resources.output/'postgres-readonly-inspection.json',inspection)
    tables=resources.sql_json("BEGIN READ ONLY; SELECT json_build_object('tables',(SELECT coalesce(json_agg(json_build_array(schemaname,tablename) ORDER BY schemaname,tablename),'[]') FROM pg_tables WHERE schemaname IN ('business','sync'))); COMMIT;",database)['tables']
    branches=[]
    for schema,table in tables:
        name=(schema+'.'+table).replace("'","''")
        quoted='"'+schema.replace('"','""')+'"."'+table.replace('"','""')+'"'
        branches.append("SELECT '"+name+"'AS name,count(*)AS count FROM "+quoted)
    census=resources.sql_json('BEGIN READ ONLY; SELECT json_object_agg(name,count) FROM ('+' UNION ALL '.join(branches)+') census; COMMIT;',database)
    write_json(resources.output/'postgres-census.json',census)
    schema=resources.sql_json("BEGIN READ ONLY; SELECT json_build_object('columns',(SELECT json_agg(json_build_object('schema',table_schema,'table',table_name,'column',column_name,'type',data_type) ORDER BY table_schema,table_name,ordinal_position) FROM information_schema.columns WHERE table_schema IN ('business','sync'))); COMMIT;",database)
    write_json(resources.output/'postgres-schema.json',schema)
    return inspection


def wire_authority_build(resources: Resources, record: dict, frozen_files: dict) -> dict:
    source=resources.root/'tools/qualification/profiles/fixtures/wire_authority.go'
    if frozen_files.get(str(source))!=smoke.sha(source.read_bytes()):raise smoke.SmokeError('wire authority source is not frozen')
    overlay=preparation.generate_wire_overlay(resources.root,resources.go,resources.output,frozen_files,raw_client=False)
    output=resources.output/'wire-authority';started=time.monotonic()
    resources.command(['go','build','-overlay='+overlay['path'],'-o',str(output),str(source)],180,'wire-authority-build',cwd=resources.go)
    artifact=dict(path=str(output),sha256=smoke.sha(output.read_bytes()),mode=output.stat().st_mode&0o777)
    verify_build_artifact(artifact,resources.output)
    record['wire_authority_preparation']=dict(artifact=artifact,overlay=overlay,source=str(source),
        source_sha256=frozen_files[str(source)],elapsed_seconds=time.monotonic()-started,native_credit=0,build_seconds=180)
    return artifact


def wire_native_audit(resources: Resources, record: dict, runtime: str, entry: dict,
                      started_ns: int, nonce: str) -> dict:
    text=(resources.output/'native-command.log').read_text()
    for identity in results.WIRE_IDS:
        marker=f'QUALIFICATION_WIRE_WORKLOAD_COMPLETE runtime={runtime} nonce={nonce} workload={identity} rows='
        if text.count(marker)!=1 and runtime=='raw':raise smoke.SmokeError('raw wire workload body witness missing or duplicated')
    if runtime=='raw':
        if (len(re.findall(r'^--- PASS: TestQualificationWireByteArtifacts \([0-9.]+s\)$',text,re.M))!=1
                or re.search(r'^\s*--- (?:FAIL|SKIP):',text,re.M)):
            raise smoke.SmokeError('raw wire exact positive native owner did not complete')
        return dict(bodies=1,workloads=7,framework_cases=8,owner='TestQualificationWireByteArtifacts')
    trace=preparation.read_full_trace(resources.output,resources.root,started_ns,prepare_only=False)
    trace['selected']=entry['task']
    selected=[task for task in trace['tasks']if task['path']==entry['task']]
    if len(selected)!=1 or selected[0]['outcome']['skipped']or selected[0]['outcome']['up_to_date']:
        raise smoke.SmokeError('wire Fetch task did not execute freshly')
    paths=[]
    for directory in profile_reports(resources.root,entry):
        for path in sorted(directory.rglob('TEST-*.xml')):
            if path.stat().st_mtime_ns<started_ns:raise smoke.SmokeError('stale wire Fetch report')
            destination=resources.output/'native'/path.relative_to(resources.root)
            destination.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(path,destination);paths.append(destination)
    cases,stdout=results.native_results(paths,started_ns,entry['task'].rsplit(':',1)[-1])
    expected=('dev.goquick.sqlitenow.oversqlite.WireByteArtifactTest','collectSevenTransportWorkloads')
    if len(cases)!=1 or(cases[0]['cls'],cases[0]['method'])!=expected or cases[0]['failed']:
        raise smoke.SmokeError('wire Fetch exact positive native owner did not complete')
    for identity in results.WIRE_IDS:
        if stdout.count(f'QUALIFICATION_WIRE_WORKLOAD_COMPLETE runtime={runtime} nonce={nonce} workload={identity} rows=')!=1:
            raise smoke.SmokeError('wire Fetch workload body witness missing or duplicated')
    record['wire_gradle_trace']=trace
    return dict(bodies=1,workloads=7,framework_cases=1,owner='.'.join(expected),
        reports=[dict(path=str(path),sha256=smoke.sha(path.read_bytes()))for path in paths])


def wire_preparation_job(parent: Resources, output: Path, profile: str, frozen: str,
                         requested: list[str], *, dependency_target: str | None = None) -> Path:
    """One bounded nonbody job with its own process registration and cleanup."""
    output.mkdir(mode=0o700);started=time.monotonic();deadline=started+(150 if dependency_target else 300)
    resources=Resources(parent.root,parent.go,output,dict(parent.env))
    resources.command_deadline=deadline-30;resources.cleanup_deadline=deadline
    entry_id='wire-dependency-discovery'if dependency_target else'wire-dependency-preparation'
    context=preparation.full_context(profile,frozen,parent.root,parent.go,resources.env)
    record=dict(entry_id=entry_id,profile_sha256=profile,freeze_sha256=frozen,native_credit=0,errors=[])
    pending=None
    try:
        record['before']=profile_source_identities(resources)
        argv=['./gradlew',*requested,'--no-daemon','--no-parallel','--console=plain']
        argv=preparation.instrument_full(argv,output,prepare_only=True,dependency_target=dependency_target)
        record.update(argv=argv,environment_binding=preparation.environment_binding(resources.env))
        started_ns=time.time_ns();resources.command(argv,remaining(resources.command_deadline,270),'native-command',capture=False)
        trace=preparation.read_full_trace(output,parent.root,started_ns,prepare_only=True)
        pending=preparation.full_preparation_pending(trace,requested,context,parent.root,resources.env,
            resources.command_deadline,entry_id=entry_id)
    except BaseException as error:record['errors'].append(type(error).__name__+': '+str(error))
    finally:
        previous={sig:signal.signal(sig,signal.SIG_IGN)for sig in(signal.SIGINT,signal.SIGTERM,signal.SIGHUP)}
        try:
            record['cleanup']=resources.cleanup();record['errors']+=record['cleanup']['errors'];resources.command_deadline=deadline
            if not record['errors']and pending is not None:
                if preparation.file_map(pending['roots'],deadline)!=pending['files']:raise smoke.SmokeError('wire preparation output changed during cleanup')
                record['after']=profile_source_identities(resources)
                if record['after']!=record['before']:raise smoke.SmokeError('wire preparation source changed')
                pending['producer_receipt']=str(output/'receipt.json');write_json(output/'preparation.json',pending)
                record['preparation_record']=dict(path=str(output/'preparation.json'),sha256=smoke.sha((output/'preparation.json').read_bytes()))
        except BaseException as error:record['errors'].append(type(error).__name__+': '+str(error))
        finally:
            record.update(status='FAIL'if record['errors']else'PASS',elapsed_seconds=time.monotonic()-started)
            write_json(output/'receipt.json',record)
            for sig,handler in previous.items():signal.signal(sig,handler)
    if record['errors']:raise smoke.SmokeError('wire nonbody preparation failed; see '+str(output/'receipt.json'))
    return output/'preparation.json'


def prepare_wire_fetch(resources: Resources, record: dict, profile: dict, digest: str,
                        frozen: str, target: str, deadline: float) -> dict:
    context=preparation.full_context(digest,frozen,resources.root,resources.go,resources.env)
    metadata=resources.output/'dependency-discovery'
    if remaining(deadline,1980)<150:raise smoke.SmokeError('insufficient bounded wire dependency discovery allowance')
    first=wire_preparation_job(resources,metadata,digest,frozen,[target.rsplit(':',1)[0]+':help'],dependency_target=target)
    owners=[]
    for path in(metadata/'task-traces').glob('graph-*.json'):
        value=json.loads(path.read_bytes())
        if value.get('dependency_owner')is not None:owners.append(value['dependency_owner'])
    if len(owners)!=1 or owners[0]['task']!=target or not owners[0]['dependencies']:
        raise smoke.SmokeError('wire dependency discovery lacks its exact selected Test owner')
    approved={task for entry in profile['schedule']['entries'].values()if entry['kind']=='preparation'for task in entry['task_ids']}
    aliases=profile['schedule']['preparation']['included_build_edge_aliases']
    selection=preparation.wire_dependency_roots(owners[0],target,approved,aliases);requested=selection['requested']
    if remaining(deadline,1980)<300:raise smoke.SmokeError('insufficient bounded wire dependency preparation allowance')
    second=wire_preparation_job(resources,resources.output/'dependency-preparation',digest,frozen,sorted(requested))
    snapshots={};paths=[first,second];required=set(requested)
    preparation.verify_full_preparation(paths,context,required,deadline,snapshots=snapshots)
    result=dict(context=context,paths=[str(path)for path in paths],required=sorted(required),snapshots=snapshots,
        selected=target,discovery=owners[0],selection=selection,
        policy='Same owned live context; existing approved dependency roots only; zero native bodies; complete producer closure and cleanup required.')
    record['wire_fetch_preparation']=result
    return result


def execute_wire(options, resources: Resources, record: dict, profile: dict,
                 digest: str, frozen: str, private: dict, runtime: str) -> None:
    started=record['coordinator_started'];logical_seconds=750 if runtime=='raw'else 1980
    logical_deadline=started+logical_seconds
    resources.command_deadline=logical_deadline-30;resources.cleanup_deadline=logical_deadline
    record.update(entry_id=options.entry,profile_sha256=digest,freeze_sha256=frozen,job_deadline=logical_deadline,
        composition_seconds=logical_seconds,orchestration_only=True,native_credit=0,before=profile_source_identities(resources))
    frozen_files=json.loads(options.freeze.read_text())['files']
    engines,record['go_runtime']=full_go_runtime(resources)
    manifest,manifest_sha=preparation.frozen_json(private.get('wire_manifest',{}),frozen_files)
    derivation,derivation_sha=preparation.frozen_json(private.get('wire_derivation',{}),frozen_files)
    record['wire_derivation_admission']=preparation.verify_wire_derivation(resources,derivation,frozen_files,
        manifest_sha,engines,record['go_runtime'])
    record['wire_input_derivation']=dict(path=private['wire_derivation']['path'],sha256=derivation_sha,manifest_sha256=manifest_sha)
    definitions=resources.root/'tools/qualification/profiles/fixtures/wire-workloads.json'
    if (tuple(item['id']for item in manifest['workloads'])!=results.WIRE_IDS
            or manifest['definitions_sha256']!=frozen_files.get(str(definitions))):
        raise smoke.SmokeError('wire manifest differs from frozen fixed definitions')
    wrapper=resources.go/'scripts/run-with-timeout.pl'
    for path in [*engines,wrapper]:
        if frozen_files.get(str(path))!=smoke.sha(path.read_bytes()):raise smoke.SmokeError('wire toolchain/wrapper is not frozen')
    raw_artifact=None
    if runtime=='raw':
        build=resources.output/'prepare-raw'
        argv=['lane','--root',str(resources.root),'--go-repo',str(resources.go),'--profile',str(options.profile),
            '--entry','contract-wire-bytes-raw-1','--freeze',str(options.freeze),'--bindings',str(options.bindings),'--output',str(build)]
        if main(argv):raise smoke.SmokeError('wire raw client preparation failed')
        producer=build/'receipt.json';receipt=json.loads(producer.read_text())
        if receipt['status']!='PASS'or receipt.get('native_credit')!=0:raise smoke.SmokeError('wire raw build lacks completed preparation')
        raw_artifact=receipt['build_artifact'];verify_build_artifact(raw_artifact,build)
        record['wire_raw_preparation']=dict(path=str(producer),sha256=smoke.sha(producer.read_bytes()),artifact=raw_artifact,
            overlay=receipt['wire_overlay'])
    authority=wire_authority_build(resources,record,frozen_files)
    files=resources.output/'files';files.mkdir(mode=0o700)
    receipts=resources.output/'wire-receipts';receipts.mkdir(mode=0o700)
    resources.env.update(JWT_SECRET=secrets.token_urlsafe(48),OVERSQLITE_WIRE_RUNTIME=runtime,
        OVERSQLITE_WIRE_WORKLOAD_MANIFEST=private['wire_manifest']['path'],OVERSQLITE_WIRE_ARTIFACT_OUTPUT=str(receipts))
    readiness=time.monotonic();resources.command_deadline=min(logical_deadline-30,readiness+60)
    base=resources.authority(Path(authority['path']),profile='wire',private_ready=True,expected_app='oversqlite-qualification-wire',
        readiness_deadline=readiness+60,expected_marker='QUALIFICATION_WIRE_AUTHORITY_PROFILE workloads=7 product_defaults=true')
    ready=resources.record['private_readiness']
    details=dict(ready['details'])
    snapshot_seed=details.pop('snapshot_seed',None)
    if details!={'manifest_sha256':manifest_sha,'workloads':7,'product_defaults':True}:
        raise smoke.SmokeError('wire actual authority manifest/defaults differ from frozen inputs')
    if runtime!='raw':
        record['wire_snapshot_preparation']=results.audit_wire_snapshot_seed(snapshot_seed,manifest,ready['nonce'])
    elif snapshot_seed is not None:
        raise smoke.SmokeError('raw wire fixture must retain its SDK writer')
    observer=resources.http_observer(base,ready['database'],nonce=ready['nonce'],capture_bodies=True)
    entry_id='contract-wire-bytes-raw-2'if runtime=='raw'else'contract-wire-bytes-fetch-'+('1'if runtime=='browser'else'2')
    entry=rebase_profile_entry(profile['schedule']['entries'][entry_id],profile,options,json.loads(options.freeze.read_text()))
    if runtime!='raw':entry=dict(entry,task=entry['argv'][4],runtime=runtime)
    temporary=resources.output/'tmp';temporary.mkdir(mode=0o700)
    values=dict(private.get('values',{}))|{'binding.entry_wire_artifacts':str(files),'binding.entry_tmp':str(temporary),
        'binding.wire_workload_manifest':private['wire_manifest']['path'],'lease.wire.capture_url':observer.url}
    if raw_artifact:values['binding.wire_raw_binary']=raw_artifact['path']
    values.setdefault('binding.java_home',resources.env.get('JAVA_HOME',''))
    resolved=planner.resolve_profile_entry(entry,values);resources.env.update(resolved['environment'])
    resources.command_deadline=logical_deadline-30;resources.cleanup_deadline=logical_deadline
    prepared=None
    if runtime!='raw':
        for path in(resources.root/'gradlew',resources.root/'gradle/wrapper/gradle-wrapper.jar',
                    resources.root/'gradle/wrapper/gradle-wrapper.properties',Path(resources.env['JAVA_HOME'])/'bin/java',Path(resources.env['JAVA_HOME'])/'release'):
            if frozen_files.get(str(path))!=smoke.sha(path.read_bytes()):raise smoke.SmokeError('wire Fetch wrapper/Java is not frozen')
        for directory in profile_reports(resources.root,entry):
            if directory.exists():
                destination=resources.output/'prior-native'/directory.relative_to(resources.root)
                destination.parent.mkdir(parents=True,exist_ok=True);shutil.move(str(directory),destination)
        prepared=prepare_wire_fetch(resources,record,profile,digest,frozen,entry['task'],logical_deadline-30)
    native_started=time.monotonic();deadline=min(logical_deadline,native_started+entry['wall_seconds'])
    resources.command_deadline=deadline-30;resources.cleanup_deadline=deadline;record['job_deadline']=deadline
    argv=resolved['argv']
    if runtime!='raw':argv=argv[:3]+preparation.instrument_full(argv[3:],resources.output,prepare_only=False)
    record.update(runtime_environment_binding=preparation.environment_binding(resources.env),
        wire_native=dict(entry_id=entry_id,runtime=runtime,argv=argv,wall_seconds=entry['wall_seconds'],native_seconds=entry['native_seconds'],
            cleanup_seconds=30,started_unix_ns=time.time_ns(),status='RUNNING'))
    resources.command(argv,remaining(resources.command_deadline,entry['native_seconds']),'native-command',cwd=Path(resolved['cwd']),capture=runtime=='raw')
    record['native']=wire_native_audit(resources,record,runtime,entry,record['wire_native']['started_unix_ns'],ready['nonce'])
    if prepared is not None:
        trace=record['wire_gradle_trace'];prepared['trace']=trace
        preparation.verify_full_consumer([Path(path)for path in prepared['paths']],prepared['context'],
            set(prepared['required']),trace,prepared['snapshots'],resources.command_deadline)
        record['native']['preparation']=preparation.observed(trace,True)
    audit_deadline=min(resources.command_deadline,time.monotonic()+60);resources.command_deadline=audit_deadline
    observer.wait_idle(audit_deadline);observation=observer.report();write_json(resources.output/'http-wire-observation.json',observation)
    inputs=results.wire_variant_inputs(resources.output,observation,audit_deadline)
    index=resources.output/'wire-variant-inputs.json';write_json(index,inputs)
    variants=resources.output/'wire-variants'
    resources.command([authority['path'],'--capture-variants',str(index),str(variants)],remaining(audit_deadline,60),'wire-capture-variants',cwd=resources.go)
    record['wire_capture']=results.audit_wire_capture(resources.output,observation,manifest,receipts,variants,runtime,audit_deadline)
    record.update(native_credit=1,after=profile_source_identities(resources))
    record['wire_native'].update(status='PASS',elapsed_before_cleanup_seconds=time.monotonic()-native_started)
    roots=[str(files),str(receipts),str(variants),str(resources.output/'decoded-wire'),str(index),
        str(resources.output/'http-wire-observation.json'),str(resources.output/'native-command.log')]
    roots.extend(str(Path(row['raw_entities']['request']).parent)for row in observation['requests']if row.get('raw_entities'))
    if runtime!='raw':roots.extend([str(resources.output/'native'),str(resources.output/'task-traces')])
    record['wire_pending']=dict(roots=roots,files=preparation.file_map(roots,audit_deadline),
        requests=observation['requests'],observer_index=resources.observers.index(observer))


def execute_wire_fetch(options, resources: Resources, record: dict, profile: dict, digest: str, frozen: str) -> None:
    deadline=record['coordinator_started']+3960
    resources.command_deadline=deadline-30;resources.cleanup_deadline=deadline
    record.update(entry_id=options.entry,profile_sha256=digest,freeze_sha256=frozen,job_deadline=deadline,
        composition_seconds=3960,orchestration_only=True,native_credit=0,before=profile_source_identities(resources),wire_runtimes=[])
    for index,runtime in((1,'browser'),(2,'wasm')):
        if remaining(deadline,3960)<1980:raise smoke.SmokeError('insufficient remaining whole Fetch composition for next runtime')
        output=resources.output/runtime
        argv=['lane','--root',str(resources.root),'--go-repo',str(resources.go),'--profile',str(options.profile),
            '--entry','contract-wire-bytes-fetch-'+str(index),'--freeze',str(options.freeze),'--bindings',str(options.bindings),'--output',str(output)]
        if main(argv):raise smoke.SmokeError('wire Fetch runtime failed: '+runtime)
        receipt=output/'receipt.json';child=json.loads(receipt.read_text())
        if (child['status']!='PASS'or child['cleanup']['errors']or not child['cleanup']['groups_absent']
                or child['native_credit']!=1 or child['wire_native']['runtime']!=runtime or len(child['wire_capture']['workloads'])!=7):
            raise smoke.SmokeError('wire Fetch child lacks exact completed runtime/cleanup evidence')
        record['wire_runtimes'].append(dict(runtime=runtime,path=str(receipt),sha256=smoke.sha(receipt.read_bytes())))
    record.update(native_credit=2,native=dict(bodies=2,workloads_per_runtime=7),after=profile_source_identities(resources))


def retained_jvm_preparation(options, resources: Resources, record: dict, profile: dict,
                             digest: str, frozen: str, private: dict) -> dict:
    """Validate completed class owners before the authority/fixture exists."""
    if not options.prepared:
        raise smoke.SmokeError('JVM retained needs completed classpath preparation before fixture startup')
    prepared_options=argparse.Namespace(**vars(options))
    prepared_options.context='library-ordinary-jvm'
    entry=rebase_profile_entry(profile['schedule']['entries']['prepare-013'],profile,prepared_options,
        json.loads(options.freeze.read_text()))
    values=dict(private.get('values',{}))
    values.setdefault('binding.java_home',resources.env.get('JAVA_HOME',''))
    resources.env.update(planner.resolve_profile_entry(entry,values)['environment'])
    context=preparation.full_context(digest,frozen,resources.root,resources.go,resources.env)
    required={task for name in ('prepare-012','prepare-013')
        for task in profile['schedule']['entries'][name]['task_ids']}
    snapshots={}
    preparation.verify_full_preparation(options.prepared,context,required,
        min(resources.command_deadline,time.monotonic()+120),snapshots=snapshots)
    runtime=[item for path in options.prepared for item in json.loads(path.read_text())['trace'].get('runtime_classpaths',[])
        if item['task']==':library-oversqlite:jvmTest']
    if len(runtime)!=1 or not runtime[0]['files']:
        raise smoke.SmokeError('retained JVM classpath preparation lacks its complete actual Test runtime dependencies')
    result=dict(context=context,required=sorted(required),snapshots=snapshots,
        paths=[str(path.resolve())for path in options.prepared],
        policy='Preparation completes before fixture launch. Native Gradle retains every dependency; no -x reuse. Actual unchanged graph/files and zero compilation work are required despite fresh live activation/owned URLs.')
    record['retained_classpath_preparation']=result
    return result


def retained_native_result(options, resources: Resources, record: dict, entry: dict,
                           started_ns: int, target: int, classpath: dict | None) -> str:
    log=resources.output/'native-command.log'
    if classpath is None:
        inventory=[dict(package='github.com/mobiletoly/go-oversync/oversqlite',test='TestDatabaseCleanupAudit',
            reason='current exact native owner; explicit realserver flag/target and fresh files root')]
        record['native']=results.audit_full_go(log,inventory)
        events=[]
        for line in log.read_text().splitlines():
            try:events.append(json.loads(line))
            except ValueError:pass
        return ''.join(x.get('Output','')for x in events if isinstance(x,dict))
    trace=preparation.read_full_trace(resources.output,resources.root,started_ns,prepare_only=False)
    trace['selected']=':library-oversqlite:jvmTest'
    paths=[]
    for directory in profile_reports(resources.root,dict(task=trace['selected'])):
        for path in sorted(directory.rglob('TEST-*.xml')):
            if path.stat().st_mtime_ns<started_ns:raise smoke.SmokeError('stale retained JVM report')
            destination=resources.output/'native'/path.relative_to(resources.root)
            destination.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(path,destination);paths.append(destination)
    cases,stdout=results.native_results(paths,started_ns,'jvmTest')
    expected=('dev.goquick.sqlitenow.oversqlite.DatabaseCleanupAuditJvmTest',
        'targetOne'if target==1 else'defaultTarget')
    if len(cases)!=1 or(cases[0]['cls'],cases[0]['method'])!=expected or cases[0]['failed']:
        raise smoke.SmokeError('retained JVM requires its one exact fresh positive native owner')
    selected=[task for task in trace['tasks']if task['path']==trace['selected']]
    if (len(selected)!=1 or selected[0]['outcome']['skipped']or selected[0]['outcome']['up_to_date']
            or selected[0]['kind']!='native-test'):
        raise smoke.SmokeError('retained JVM native task did not execute freshly')
    preparation.verify_full_consumer([Path(path)for path in classpath['paths']],classpath['context'],
        set(classpath['required']),trace,classpath['snapshots'],resources.command_deadline)
    classpath['trace']=trace
    record['native']=dict(bodies=1,framework_cases=1,owners=['.'.join(expected)],
        reports=[dict(path=str(path),sha256=smoke.sha(path.read_bytes()))for path in paths],
        preparation=preparation.observed(trace,True))
    return stdout


def execute_retained_go(options, resources: Resources, record: dict, profile: dict,
                         digest: str, frozen: str, private: dict) -> None:
    name=options.entry;target=1 if name.endswith('-one')else 1000000
    started=record['coordinator_started'];logical_deadline=started+540
    resources.command_deadline=logical_deadline-30;resources.cleanup_deadline=logical_deadline
    record.update(entry_id=name,profile_sha256=digest,freeze_sha256=frozen,job_deadline=logical_deadline,
        composition_seconds=540,orchestration_only=True,native_credit=0,before=profile_source_identities(resources))
    family='kotlin'if name.startswith('retained-jvm-')else'go'
    classpath=retained_jvm_preparation(options,resources,record,profile,digest,frozen,private)if family=='kotlin'else None
    build_id='contract-'+name+'-1';build_output=resources.output/'prepare-authority'
    args=['lane','--root',str(resources.root),'--go-repo',str(resources.go),'--profile',str(options.profile),
          '--entry',build_id,'--freeze',str(options.freeze),'--output',str(build_output)]
    if options.bindings:args+=['--bindings',str(options.bindings)]
    if main(args):raise smoke.SmokeError('retained authority preparation failed')
    producer=build_output/'receipt.json';built=json.loads(producer.read_text())
    if built.get('status')!='PASS' or built.get('entry_id')!=build_id or built.get('native_credit')!=0:
        raise smoke.SmokeError('retained authority lacks exact successful preparation')
    verify_build_artifact(built['build_artifact'],build_output)
    record['retained_preparation']=dict(path=str(producer),sha256=smoke.sha(producer.read_bytes()))
    files=resources.output/'files';files.mkdir(mode=0o700)
    # Java Files.writeString preserves an existing file's permissions. Publish
    # the private control inode before either native owner can update its phase.
    phase_file=files/'audit-phase.json'
    phase_file.write_text('{"phase":"initial"}\n');phase_file.chmod(0o600)
    setup=time.monotonic()
    resources.env['JWT_SECRET']=secrets.token_urlsafe(48)
    base=resources.authority(Path(built['build_artifact']['path']),private_ready=True,readiness_deadline=setup+60)
    observer=resources.http_observer(base,resources.record['database'],nonce=resources.record['private_readiness']['nonce'],
        phase_file=phase_file)
    record['fixture_setup_seconds']=time.monotonic()-setup
    spec=profile['schedule']['entries']['contract-'+name+'-2']
    if remaining(logical_deadline,540)<270:
        raise smoke.SmokeError('retained fixture needs270s for native, inspection and cleanup admission')
    native_started=time.monotonic();deadline=native_started+spec['wall_seconds']
    resources.command_deadline=deadline-30;resources.cleanup_deadline=deadline;record['job_deadline']=deadline
    native_tmp=resources.output/'tmp';native_tmp.mkdir(mode=0o700)
    values=dict(private.get('values',{}))|{'binding.entry_files':str(files),'binding.entry_tmp':str(native_tmp),
        'lease.categories.observer_url':observer.url}
    values.setdefault('binding.java_home',resources.env.get('JAVA_HOME',''))
    entry=rebase_profile_entry(spec,profile,options,json.loads(options.freeze.read_text()))
    resolved=planner.resolve_profile_entry(entry,values);resources.env.update(resolved['environment'])
    record['runtime_environment_binding']=preparation.environment_binding(resources.env)
    if classpath is not None:
        prior=classpath['context']['environment'];current=record['runtime_environment_binding']
        record['retained_classpath_environment_delta']={key:dict(preparation=prior.get(key),native=current.get(key))
            for key in sorted(set(prior)|set(current))if prior.get(key)!=current.get(key)}
    engines,record['go_runtime']=full_go_runtime(resources);frozen_files=json.loads(options.freeze.read_text())['files']
    for path in [*engines,Path(resolved['argv'][0])]:
        if frozen_files.get(str(path))!=smoke.sha(path.read_bytes()):raise smoke.SmokeError('retained Go engine/wrapper is not frozen')
    if classpath is not None:
        java=Path(resources.env.get('JAVA_HOME',''))
        for path in (resources.root/'gradlew',resources.root/'gradle/wrapper/gradle-wrapper.jar',
                     resources.root/'gradle/wrapper/gradle-wrapper.properties',java/'bin/java',java/'release'):
            if not path.is_absolute()or frozen_files.get(str(path))!=smoke.sha(path.read_bytes()):
                raise smoke.SmokeError('retained JVM wrapper/Java is not frozen')
    source=(resources.go/'oversqlite/database_cleanup_audit_test.go'if family=='go'else
        resources.root/'library-oversqlite/src/jvmTest/kotlin/dev/goquick/sqlitenow/oversqlite/DatabaseCleanupAuditJvmTest.kt')
    if frozen_files.get(str(source))!=smoke.sha(source.read_bytes()):raise smoke.SmokeError('retained native owner source is not frozen')
    native=dict(entry_id=entry['id'],argv=resolved['argv'],wall_seconds=entry['wall_seconds'],
        native_seconds=entry['native_seconds'],cleanup_seconds=30,started_unix_ns=time.time_ns(),status='RUNNING')
    record['retained_native']=native
    argv=resolved['argv']
    if classpath is not None:
        for directory in profile_reports(resources.root,dict(task=':library-oversqlite:jvmTest')):
            if directory.exists():
                destination=resources.output/'prior-native'/directory.relative_to(resources.root)
                destination.parent.mkdir(parents=True,exist_ok=True);shutil.move(str(directory),destination)
        argv=argv[:3]+preparation.instrument_full(argv[3:],resources.output,prepare_only=False)
    native['argv']=argv
    resources.command(argv,remaining(resources.command_deadline,entry['native_seconds']),'native-command',cwd=Path(resolved['cwd']),capture=classpath is None)
    log=resources.output/'native-command.log'
    stdout=retained_native_result(options,resources,record,entry,native['started_unix_ns'],target,classpath)
    witness=f'RECOVERY_RETAINED_AUDIT_COMPLETE {family} target={target} cycles=10 keys=125 cancelled_reopen=true ack_reopen=true'
    if stdout.count(witness)!=1:raise smoke.SmokeError('retained exact complete native witness missing or duplicated')
    inspection_started=time.monotonic()
    inspection_deadline=min(resources.command_deadline,inspection_started+30)
    resources.command_deadline=inspection_deadline
    manifest=json.loads((files/'manifest.json').read_text())
    if (manifest['target']!=target or manifest['cycles']!=10 or manifest['keys_count']!=125
            or manifest['ack_losses']!=1 or manifest['ack_reopened']is not True):
        raise smoke.SmokeError('retained complete125-key/10-cycle/cancel/ack fixture changed')
    observer.wait_idle(resources.command_deadline);observer.physical('final','after')
    observation=observer.report();write_json(resources.output/'http-storage-observation.json',observation)
    record['retained_physical_coverage']=results.audit_retained_observation(observation)
    if (observation['errors'] or observation['download_phase_counts'].get('steady',0)!=0
            or observation['download_phase_counts'].get('explicit-rebuild',0)==0
            or not observation['acks'] or observation['peak_outcome_envelope_bytes']<=0
            or observation['ack_repeat_counts'].count(2)!=1 or any(x not in(1,2)for x in observation['ack_repeat_counts'])):
        raise smoke.SmokeError('retained real snapshot/ack retry/positive storage observations incomplete')
    final=observation['physical_samples'][-1]
    if any(final[key]!=0 for key in('outcomes','snapshot_sessions','snapshot_rows')):
        raise smoke.SmokeError('retained completed outcome/snapshot storage not released')
    postgres=retained_postgres(resources,resources.record['database'])
    inspection=results.inspect_retained_sqlite(resources.output,family,postgres,observation,inspection_deadline)
    write_json(resources.output/'readonly-sqlite-inspection.json',inspection)
    if inspection['errors']:raise smoke.SmokeError('retained closed-copy/source/certificate invariants failed: '+repr(inspection['errors']))
    remaining(inspection_deadline,30)
    record['readonly_inspection_seconds']=time.monotonic()-inspection_started
    native.update(status='PASS',elapsed_before_cleanup_seconds=time.monotonic()-native_started)
    record.update(native_credit=1,after=profile_source_identities(resources),retained_manifest=manifest)
    roots=[str(files),str(log),str(resources.output/'http-storage-observation.json'),
        str(resources.output/'postgres-readonly-inspection.json'),str(resources.output/'postgres-census.json'),
        str(resources.output/'postgres-schema.json'),str(resources.output/'readonly-sqlite-inspection.json')]
    if classpath is not None:roots.extend([str(resources.output/'native'),str(resources.output/'task-traces')])
    record['retained_pending']=dict(roots=roots,files=preparation.file_map(roots,resources.command_deadline),
        artifact=built['build_artifact'],producer=record['retained_preparation'])


def execute_full_lane(options, resources: Resources, record: dict) -> None:
    profile,digest = planner.load_full_profile(options.profile,resources.root,resources.go)
    special={'wire-bytes-fetch':3960,'wire-bytes-raw':750,'contract-wire-bytes-fetch-1':1980,
        'contract-wire-bytes-fetch-2':1980,'load-sequence':1620,'resource-go-normal':420,'resource-go-observed':420,
        'retained-go-default':540,'retained-go-one':540,'retained-jvm-default':540,'retained-jvm-one':540}
    if options.entry in special:
        seconds=special[options.entry]
    else:
        ids=planner.full_profile_selection(profile,[options.entry])
        if len(ids)!=1:raise smoke.SmokeError('multi-member root needs the campaign/sequence coordinator')
        seconds=planner.effective_profile_entry(profile,profile['schedule']['entries'][ids[0]]).get('wall_seconds')
        if type(seconds)is not int or not 0<seconds<=300:raise smoke.SmokeError('entry lacks an approved normal cap')
    record['job_deadline']=record['coordinator_started']+seconds
    resources.command_deadline=record['job_deadline']-30;resources.cleanup_deadline=record['job_deadline']
    frozen,private = profile_activation(options,profile,digest,resources=resources)
    if options.entry=='wire-bytes-fetch':
        execute_wire_fetch(options,resources,record,profile,digest,frozen)
        return
    if options.entry in('wire-bytes-raw','contract-wire-bytes-fetch-1','contract-wire-bytes-fetch-2'):
        runtime='raw'if options.entry=='wire-bytes-raw'else('browser'if options.entry.endswith('-1')else'wasm')
        execute_wire(options,resources,record,profile,digest,frozen,private,runtime)
        return
    if options.entry=='load-sequence':
        execute_load_sequence(options,resources,record,profile,digest,frozen,private)
        return
    if options.entry in('resource-go-normal','resource-go-observed'):
        execute_resource(options,resources,record,profile,digest,frozen,private)
        return
    if options.entry in('retained-go-default','retained-go-one','retained-jvm-default','retained-jvm-one'):
        execute_retained_go(options,resources,record,profile,digest,frozen,private)
        return
    ids = planner.full_profile_selection(profile,[options.entry])
    if len(ids)!=1:
        raise smoke.SmokeError('multi-member root needs the campaign/sequence coordinator')
    schedule = profile['schedule'];entry = planner.effective_profile_entry(profile,schedule['entries'][ids[0]])
    if entry.get('orchestration_only') or entry.get('internal_only'):
        raise smoke.SmokeError('phase controller cannot be a normal entry')
    started = record.get('coordinator_started',time.monotonic());deadline=started+entry['wall_seconds']
    record['job_deadline']=deadline
    resources.command_deadline=deadline-entry['cleanup_seconds']
    resources.cleanup_deadline=deadline
    before=profile_source_identities(resources)
    values=dict(private.get('values',{}))
    values.update({'binding.current_freeze':str(options.freeze.resolve()),'binding.new_output':str(resources.output),
                   'binding.entry_tmp':str(resources.output/'tmp')})
    if 'JAVA_HOME'in resources.env:values.setdefault('binding.java_home',resources.env['JAVA_HOME'])
    category_build=entry.get('parent_contract')in('retained-go-default','retained-go-one','retained-jvm-default','retained-jvm-one')and entry['kind']=='build'
    wire_build=entry['id']=='contract-wire-bytes-raw-1'
    if wire_build:
        artifact=resources.output/'wire-raw.test';key='binding.wire_raw_binary'
        if key in values and values[key]!=str(artifact):raise smoke.SmokeError('wire raw output belongs to its new owning preparation')
        values[key]=str(artifact);private=dict(private,owned_outputs=[*private.get('owned_outputs',[]),str(artifact)])
        if 'binding.wire_support_overlay'in values:raise smoke.SmokeError('wire overlay must be generated by its owning preparation')
        record['wire_overlay']=preparation.generate_wire_overlay(resources.root,resources.go,resources.output,
            json.loads(options.freeze.read_text())['files'],raw_client=True)
        values['binding.wire_support_overlay']=record['wire_overlay']['path']
    resource_build=entry.get('parent_contract')in('resource-go-normal','resource-go-observed')and entry['kind']=='build'
    if resource_build:
        key='binding.replay_binary';artifact=resources.output/'replay.test'
        if key in values and values[key]!=str(artifact):raise smoke.SmokeError('resource binary must be owned by its new build')
        values[key]=str(artifact);private=dict(private,owned_outputs=[*private.get('owned_outputs',[]),str(artifact)])
        if any(key in values for key in('binding.replay_full_durability_overlay','binding.observer_modfile')):
            raise smoke.SmokeError('resource overlays/modfiles are generated by the owning build')
        driver=None
        if entry['parent_contract']=='resource-go-observed':
            module=json.loads(resources.inspect(['go','list','-m','-json','github.com/mattn/go-sqlite3'],cwd=resources.go))
            if module.get('Version')!='v1.14.37'or module.get('Replace'):raise smoke.SmokeError('resource driver dependency differs from reviewed source')
            driver=Path(module['Dir'])
        record['resource_overlay']=preparation.generate_resource_overlay(resources.go,resources.output,
            json.loads(options.freeze.read_text())['files'],driver=driver)
        values['binding.replay_full_durability_overlay']=record['resource_overlay']['path']
        if driver is not None:values['binding.observer_modfile']=record['resource_overlay']['modfile']
    if entry['id'] in ('load-prepare-1','load-prepare-2')or category_build:
        # The declared build owns these new outputs. In particular the overlay
        # is generated from independently frozen preimages, never inherited.
        key = 'binding.category_authority'if category_build else('binding.load_authority'if entry['id']=='load-prepare-1'else'binding.load_client_binary')
        artifact = resources.output / ('category-authority'if category_build else('load-authority'if entry['id']=='load-prepare-1'else'load-client.test'))
        if key in values and values[key] != str(artifact):
            raise smoke.SmokeError('load build output belongs to its new attempt')
        values[key] = str(artifact)
        private = dict(private,owned_outputs=[*private.get('owned_outputs',[]),str(artifact)])
        if entry['id']=='load-prepare-2':
            if 'binding.load_full_overlay' in values:
                raise smoke.SmokeError('load durability overlay is generated by its owning build')
            record['load_overlay'] = preparation.generate_load_overlay(resources.root,resources.go,
                resources.output,json.loads(options.freeze.read_text())['files'])
            values['binding.load_full_overlay'] = record['load_overlay']['path']
    swift=entry.get('runtime')=='Swift/macOS'
    swift_script=resources.root/'tools/qualification/profiles/short-swift-filter.init.gradle'
    if swift:values.setdefault('binding.reviewed_swift_filter_init',str(swift_script))
    if web_deadline_policy(entry)=='single-backlog':
        script=str(resources.root/'tools/qualification/profiles/library-backlog.init.gradle')
        if values.get('binding.reviewed_backlog_init_script',script)!=script:
            raise smoke.SmokeError('backlog binding must name the adopted support script')
        values['binding.reviewed_backlog_init_script']=script
    if entry.get('runtime')=='ios':
        if 'lease.ios.udid'in values:raise smoke.SmokeError('normal iOS entry allocates its own disposable simulator')
        values['lease.ios.udid']='__OWNED_DEVICE_PENDING__'
    entry = rebase_profile_entry(entry,profile,options,json.loads(options.freeze.read_text()))
    resolved=planner.resolve_profile_entry(entry,values)
    resources.env.update(resolved['environment'])
    (resources.output/'tmp').mkdir(mode=0o700)
    resources.record['delegated_leases'] = private.get('lease_receipts',{})
    # No inherited route or guessed fixture is eligible. The supervisor must bind
    # every lease value and its ownership proof before the native process exists.
    required_leases={m.group(0)for value in [*entry['argv'],entry['cwd'],*entry['environment'].values()]for m in planner.PROFILE_BINDING.finditer(value)if m.group(0).startswith('lease.')}
    for key in required_leases:
        if key=='lease.ios.udid'and entry.get('runtime')=='ios':continue
        proof=private.get('lease_receipts',{}).get(key)
        if not isinstance(proof,dict):
            raise smoke.SmokeError('missing exact owned lease proof: '+key)
        resources.verify_delegated_lease(key,values[key],proof,json.loads(options.freeze.read_text())['files'])
    if entry.get('expected_source_owners'):
        results.validate_body_policies(entry,private.get('body_policies',{}))
        frozen_files=json.loads(options.freeze.read_text())['files']
        for owner in entry['expected_source_owners']:
            policy=private.get('body_policies',{}).get(owner,{})
            source=policy.get('source_file','')
            if (not Path(source).is_absolute()or frozen_files.get(source)!=policy.get('source_sha256')
                    or not policy.get('reason')):
                raise smoke.SmokeError('missing frozen body-policy source: '+owner)
    context=preparation.full_context(digest,frozen,resources.root,resources.go,resources.env)
    argv=resolved['argv'];is_gradle=Path(argv[3]).name=='gradlew'
    frozen_files=json.loads(options.freeze.read_text())['files']
    critical=[Path(argv[0])]
    if is_gradle:
        critical.extend([Path(resolved['cwd'])/argv[3],resources.root/'gradle/wrapper/gradle-wrapper.jar',
                         resources.root/'gradle/wrapper/gradle-wrapper.properties'])
        home=Path(resources.env.get('JAVA_HOME',''))
        if not home.is_absolute():raise smoke.SmokeError('Gradle lane needs an explicit absolute Java home')
        critical.extend([home/'bin/java',home/'release'])
    if swift:critical.append(swift_script)
    critical.extend(Path(argv[i+1]) for i,arg in enumerate(argv[:-1])if arg=='-I')
    if Path(argv[3]).name=='go' or any(Path(arg).name=='go-test-with-cleanup.sh'for arg in argv):
        engines,record['go_runtime'] = full_go_runtime(resources)
        critical.extend(engines)
    for path in critical:
        if not path.is_absolute():path=Path(resolved['cwd'])/path
        if not path.is_file()or frozen_files.get(str(path))!=smoke.sha(path.read_bytes()):
            raise smoke.SmokeError('execution script/engine missing from freeze: '+str(path))
    if entry['kind']=='preparation'and not is_gradle:
        raise smoke.SmokeError('preparation needs an observable nonbody Gradle graph')
    go_inventory=private.get('go_test_inventory',{}).get(entry['id'],[])
    go_test=any(Path(value).name=='go-test-with-cleanup.sh'for value in argv)
    build=entry['kind']=='build'
    static=entry.get('parent_command')in('new-go-vet','metadata','worker-assets')
    if go_test:
        frozen_files=json.loads(options.freeze.read_text())['files']
        if not go_inventory:raise smoke.SmokeError('missing complete frozen Go owning-test inventory')
        for owner in go_inventory:
            if frozen_files.get(owner.get('source_file'))!=owner.get('source_sha256')or not owner.get('reason'):
                raise smoke.SmokeError('Go owner source/admission classification is not frozen')
        resources.prepare_go_fixtures()
    if not entry.get('expected_source_owners')and entry['kind']!='preparation'and not(go_test or build or static):
        raise smoke.SmokeError('entry requires its sequence/native-contract adapter before child startup')
    build_output=None
    if build:
        if '-o'not in argv:raise smoke.SmokeError('build needs its declared output identity')
        build_output=Path(argv[argv.index('-o')+1])
        if (not build_output.is_absolute()or build_output.exists()or str(build_output)not in private.get('owned_outputs',[])
                or not build_output.resolve().is_relative_to(resources.output.resolve())):
            raise smoke.SmokeError('build output must be a new explicitly owned path')
    record.update(entry_id=entry['id'],runtime=entry.get('runtime'),profile_sha256=digest,freeze_sha256=frozen,wall_seconds=entry['wall_seconds'],
                  native_seconds=entry['native_seconds'],before=before,native_credit=0,
                  environment_binding=preparation.environment_binding(resources.env))
    if entry.get('catalog_group_task_ids'):
        record['preparation_scope']=dict(context=options.context,requested=entry['task_ids'],
            catalog_group_tasks=entry['catalog_group_task_ids'],claim='selected-context owning dependency closure only')
    if entry.get('task') and is_gradle and entry['kind']!='preparation':
        possible={task for candidate in schedule['entries'].values()if candidate['kind']=='preparation'for task in candidate['task_ids']}
        required=set(entry.get('preparation_tasks',[]))&possible
        consumed={}
        verified=preparation.verify_full_preparation(options.prepared,context,required,min(resources.command_deadline,time.monotonic()+120),snapshots=consumed)
        # Kotlin maps both native environments and executable paths from task
        # providers. Even a verified -x producer is unreadable in this invocation.
        # Keep predecessors enabled; the terminal closure audit rejects actual
        # compile/link work and any changed or unbound dependency/output files.
        record['verified_preparation_tasks']=verified
        record['verified_preparation_exclusions']=[]
        if consumed:
            record['full_preparation_consumed']=dict(context=context,required=sorted(required),snapshots=consumed)
    if entry.get('runtime')=='ios'and entry['kind']!='preparation':
        values['lease.ios.udid']=resources.ios_device()
        replacement=planner.resolve_profile_entry(entry,values)
        argv=[values['lease.ios.udid']if arg=='__OWNED_DEVICE_PENDING__'else arg for arg in argv]
        if replacement['cwd']!=resolved['cwd']:raise smoke.SmokeError('runtime device allocation changed working directory')
    elif entry.get('runtime')=='android'and entry['kind']!='preparation':
        serial=resources.android_device()
        for key in ('OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL','OVERSQLITE_CATEGORIES_REAL_SERVER_SMOKE_BASE_URL'):
            if key in resources.env:
                port=str(urlsplit(resources.env[key]).port)
                resources.inspect([str(Path(resources.android['sdk'])/'platform-tools/adb'),'-s',serial,'reverse','tcp:'+port,'tcp:'+port])
    record['runtime_environment_binding']=preparation.environment_binding(resources.env)
    if is_gradle:
        if swift:argv=swift_capture_argv(argv,entry['task'],resources.output,swift_script)
        argv,web=web_deadline_support(resources,entry,argv,frozen_files)
        if web:record['web_deadline_support']=web
        argv,worker_web=worker_browser_support(resources,entry,argv,frozen_files)
        if worker_web:record['worker_browser_support']=worker_web
        runtime_tasks=preparation.jvm_preparation_runtime_tasks(entry)
        native_inputs=preparation.android_preparation_input_tasks(entry)
        if entry['kind']=='preparation'and options.context=='swift-generated-live-only':
            native_inputs+=(schedule['entries']['swift-generated-live-only']['task'],)
        argv=argv[:3]+preparation.instrument_full(argv[3:],resources.output,prepare_only=entry['kind']=='preparation',runtime_tasks=runtime_tasks,native_input_tasks=native_inputs)
    for directory in ([]if swift else profile_reports(resources.root,entry)):
        if directory.exists():
            target=resources.output/'prior-native'/directory.relative_to(resources.root)
            target.parent.mkdir(parents=True,exist_ok=True);shutil.move(str(directory),target)
    record['argv']=argv;write_json(resources.output/'start.json',record)
    started_ns=time.time_ns()
    seconds=min(entry['native_seconds'],remaining(deadline,entry['wall_seconds'])-entry['cleanup_seconds'])
    if seconds<=0:raise smoke.SmokeError('no normal-entry budget remains before child startup')
    resources.command(argv,seconds,'native-command',cwd=Path(resolved['cwd']),capture=not is_gradle)
    if is_gradle:
        trace=preparation.read_full_trace(resources.output,resources.root,started_ns,prepare_only=entry['kind']=='preparation')
        if entry['kind']=='preparation':
            record['full_preparation_pending']=preparation.full_preparation_pending(trace,entry['task_ids'],context,resources.root,resources.env,min(resources.command_deadline,time.monotonic()+120),entry_id=entry['id'])
        elif entry.get('task'):
            selected=[task for task in trace['tasks']if task['path']==entry['task']]
            if len(selected)!=1 or selected[0]['outcome']['skipped']or selected[0]['outcome']['up_to_date']:
                raise smoke.SmokeError('selected native task did not execute freshly')
        if record.get('full_preparation_consumed'):
            record['full_preparation_consumed']['trace']=dict(trace,selected=entry['task'])
    if entry.get('expected_source_owners'):
        paths=[]
        for directory in ([]if swift else profile_reports(resources.root,entry)):
            for path in sorted(directory.rglob('TEST-*.xml')):
                if path.stat().st_mtime_ns<started_ns:raise smoke.SmokeError('stale profile native report')
                target=resources.output/'native'/path.relative_to(resources.root)
                target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(path,target);paths.append(target)
        if swift:
            command=json.loads((resources.output/'swift-command.json').read_text())
            report=resources.output/'native-swift.xml'
            if command.get('task')!=entry['task']or command.get('xunit')!=str(report):
                raise smoke.SmokeError('Swift command/capture ownership mismatch')
            record['swift_command']=command
            captured=resources.output/'native-swift-stdout.log'
            if captured.exists():raise smoke.SmokeError('Swift stdout capture already exists')
            captured.write_bytes((resources.output/'native-command.log').read_bytes())
            record['swift_stdout_capture']=dict(path=str(captured),sha256=smoke.sha(captured.read_bytes()))
            if report.is_file():
                paths=[report];record['swift_native_format']='xunit-file'
            elif entry['id']=='swift-generated-live-only'and'--parallel'not in command.get('argv',[]):
                paths=[];record['swift_native_format']='serial-xctest-stdout'
            else:
                raise smoke.SmokeError('missing Swift native xUnit output')
        record['native']=results.audit_full_entry(paths,started_ns,entry,private.get('body_policies',{}),
            native_stdout=captured.read_text()if swift else'')
        record['native_credit']=record['native']['bodies']
        if record.get('web_deadline_support'):
            record['web_deadline_audit']=results.audit_web_deadlines(record['web_deadline_support'],entry,started_ns)
        if record.get('worker_browser_support'):
            record['worker_browser_audit']=results.audit_worker_browser(record['worker_browser_support'],entry,started_ns)
    elif go_test:
        record['native']=results.audit_full_go(resources.output/'native-command.log',go_inventory)
        record['native_credit']=record['native']['bodies']
    elif build_output:
        if not build_output.is_file():raise smoke.SmokeError('successful build did not produce its owned artifact')
        record['build_artifact']=dict(path=str(build_output),sha256=smoke.sha(build_output.read_bytes()),
                                      mode=build_output.stat().st_mode & 0o777)
    elif static:
        record['support_check']='completed; zero native body credit'
    record['after']=profile_source_identities(resources)
    if verify_freeze(options.freeze)!=frozen or before!=record['after']:
        raise smoke.SmokeError('profile source or frozen dependency drift')
    record['job_deadline']=deadline
    record['profile_entry_elapsed_seconds']=time.monotonic()-started


def execute(options: argparse.Namespace, resources: Resources, record: dict) -> None:
    if options.mode == "lane":
        execute_full_lane(options, resources, record)
        return
    if options.mode == "prepare":
        # Build in Go; all runtime invocations continue to use the Kotlin root.
        original = resources.root
        resources.root = resources.go
        try:
            record["fixture"] = prepare_fixture(resources, resources.output)
        finally:
            resources.root = original
        return
    frozen = verify_freeze(options.freeze)
    before = {str(root): smoke.identity(root, resources.env) for root in (resources.root, resources.go)}
    methods = list(results.VERIFIER_METHODS) if options.mode == "verifier" else options.method
    if not methods or any(name not in results.REPLAY_METHODS for name in methods) and options.mode == "replay":
        raise smoke.SmokeError("select exact replay method(s)")
    start = time.monotonic()
    device = configure(resources, options.mode, options.runtime, options.fixture)
    record["preparation_seconds"] = round(time.monotonic() - start, 6)
    for directory in result_directories(resources.root, options.runtime):
        if directory.exists():
            target = resources.output / "prior-native" / directory.relative_to(resources.root / MODULE / "build")
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(directory), target)
    argv = native_command(resources.root, options.mode, options.runtime, methods, resources.env, device)
    pilot = options.record_preparation or options.reuse_preparation
    binding = None
    budget = preparation.Budget() if pilot else None
    if pilot:
        with budget.phase() as deadline:
            binding = preparation.context(resources, options, frozen, deadline)
            if options.reuse_preparation:
                manifest_sha = smoke.sha(options.reuse_preparation.read_bytes())
                saved = preparation.verify(options.reuse_preparation, binding, deadline)
                producer_sha = smoke.sha((options.reuse_preparation.parent / "receipt.json").read_bytes())
                if smoke.sha(options.reuse_preparation.read_bytes()) != manifest_sha:
                    raise smoke.SmokeError("preparation manifest changed before native execution")
        if options.rebuild_dependencies:
            argv.append("--rerun-tasks")
        argv = preparation.instrument(argv, resources.output)
    record.update(argv=argv, selectors=methods, freeze_sha256=frozen, before=before, body_seconds=180 if options.mode == "verifier" else 900,
                  wrapper_seconds=1800, phases={"setup": "included in testcase; not separately instrumented", "public_operation": "retained body output where available", "verification": "not separately instrumented"})
    write_json(resources.output / "start.json", record)
    if options.runtime == "android":
        resources.verify_android()
    began = time.monotonic(); started_ns = time.time_ns()
    wrapped = [str(resources.go / "scripts/run-with-timeout.pl"), "1800", "--", *argv]
    record["wrapper_argv"] = wrapped
    resources.command(wrapped, 1810, "native-command", capture=not pilot)
    record["native_wrapper_seconds"] = round(time.monotonic() - began, 6)
    cases, stdout = retain_reports(resources.root, options.runtime, resources.output, started_ns)
    if options.runtime == "android":
        resources.verify_android()
        adb = str(Path(resources.android["sdk"]) / "platform-tools/adb")
        logcat = resources.inspect([adb, "-s", device, "logcat", "-d", "-v", "brief"], 10)
        (resources.output / "owned-logcat.log").write_text(smoke.redact(logcat, resources.env))
        if "F1_" not in stdout and "A3_" not in stdout:
            stdout = logcat
    elif "F1_" not in stdout and "A3_" not in stdout:
        stdout = (resources.output / "native-command.log").read_text()
    (resources.output / "body-witnesses.log").write_text(stdout)
    record["native"] = results.audit(cases, stdout, options.mode, options.runtime, methods)
    if pilot:
        with budget.phase() as deadline:
            task = ":platform-oversqlite-test:composeApp:" + results.TASKS[options.runtime]
            trace = preparation.read_trace(resources.output, task)
            record["build_observation"] = preparation.observed(trace, bool(options.reuse_preparation))
            if preparation.context(resources, options, frozen, deadline) != binding:
                raise smoke.SmokeError("preparation source/runtime/environment changed during execution")
            roots = preparation.roots_for(trace, resources.root, resources.env)
            current_files = preparation.file_map(roots, deadline)
            runtime = {key: trace[key] for key in ("gradle", "java_home")}
            if options.reuse_preparation:
                if smoke.sha(options.reuse_preparation.read_bytes()) != manifest_sha:
                    raise smoke.SmokeError("preparation manifest changed during native execution")
                if smoke.sha((options.reuse_preparation.parent / "receipt.json").read_bytes()) != producer_sha:
                    raise smoke.SmokeError("preparation producer receipt changed during native execution")
                preparation.verify(options.reuse_preparation, binding, deadline)
                preparation.verify_graph(saved, trace, roots, current_files)
                record["preparation_reuse"] = dict(path=str(options.reuse_preparation),
                    sha256=manifest_sha, producer_receipt_sha256=producer_sha,
                    compilation="reused; native bodies freshly executed")
            record["preparation_pending"] = dict(context=binding, roots=roots, files=current_files, gradle_runtime=runtime)
        record["metadata_seconds_remaining"] = budget.remaining
    if options.mode == "replay":
        counts = resources.sql("SELECT (SELECT count(*) FROM sync.user_state),(SELECT count(*) FROM sync.source_state)", resources.record["database"])
        if not all(int(value) > 0 for value in counts.split("|")) or len(counts.split("|")) != 2:
            raise smoke.SmokeError("categories authority never reached")
        record["authority_scope_source_counts"] = counts
    record["after"] = {str(root): smoke.identity(root, resources.env) for root in (resources.root, resources.go)}
    if record["after"] != before or verify_freeze(options.freeze) != frozen:
        raise smoke.SmokeError("source or qualification input drift")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("mode", choices=("prepare", "verifier", "replay", "lane"))
    result.add_argument("--runtime", choices=tuple(results.TASKS))
    result.add_argument("--root", type=Path, default=ROOT)
    result.add_argument("--go-repo", type=Path, default=ROOT.parent / "go-oversync")
    result.add_argument("--output", type=Path, required=True)
    result.add_argument("--freeze", type=Path)
    result.add_argument('--expected-freeze-sha256')
    result.add_argument('--expected-source-sha256')
    result.add_argument('--worker-contract',type=Path)
    result.add_argument('--expected-worker-contract-sha256')
    result.add_argument("--profile", type=Path)
    result.add_argument("--entry")
    result.add_argument("--bindings", type=Path)
    result.add_argument("--prepared", type=Path, action="append", default=[])
    result.add_argument("--context")
    result.add_argument("--fixture", type=Path)
    result.add_argument("--method", action="append", default=[])
    pilot = result.add_mutually_exclusive_group()
    pilot.add_argument("--record-preparation", action="store_true", help="record an opt-in browser/macOS verifier preparation binding after success")
    pilot.add_argument("--reuse-preparation", type=Path, help="verify prepared dependencies while executing fresh verifier bodies")
    result.add_argument("--rebuild-dependencies", action="store_true", help="pilot baseline only: reproduce dependency reruns without altering default commands")
    return result


def main(arguments: list[str] | None = None) -> int:
    options = parser().parse_args(arguments)
    if options.mode!='lane' and (options.expected_freeze_sha256 or options.expected_source_sha256):
        parser().error('campaign dispatch anchors apply only to lane mode')
    if bool(options.worker_contract)!=bool(options.expected_worker_contract_sha256):
        parser().error('worker contract needs its exported digest')
    if options.worker_contract and (options.mode!='lane' or not options.expected_source_sha256):
        parser().error('worker contract needs both lane dispatch anchors')
    if options.mode == "lane" and (not options.profile or not options.entry or not options.freeze):
        parser().error("lane requires profile, entry and frozen authorization")
    if options.mode not in ("prepare", "lane") and (not options.runtime or not options.freeze):
        parser().error("runtime and frozen dependency map required")
    if options.mode == "verifier" and options.method:
        parser().error("verifier selects the complete nine-method class")
    pilot = options.record_preparation or options.reuse_preparation
    if pilot and (options.mode != "verifier" or options.runtime not in ("browser", "macos")):
        parser().error("preparation pilot is limited to browser/macOS verifier runs")
    if options.rebuild_dependencies and not options.record_preparation:
        parser().error("dependency rebuild is permitted only with --record-preparation")
    output = options.output.resolve(); output.mkdir(mode=0o700, parents=True, exist_ok=False)
    record = dict(mode=options.mode, runtime=options.runtime, started_unix_ns=time.time_ns(), errors=[], qualification="evidence pending independent review")
    started = time.monotonic()
    record['coordinator_started'] = started
    previous = {}
    resources = None
    def interrupted(sig, frame):
        raise InterruptedError(f"coordinator interrupted by signal {sig}")
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        previous[sig] = signal.signal(sig, interrupted)
    try:
        resources = Resources(options.root.resolve(), options.go_repo.resolve(), output, lane_environment(dict(os.environ)) if options.mode == "lane" else clean_environment(dict(os.environ)))
        execute(options, resources, record)
    except BaseException as error:
        record["errors"].append(f"{type(error).__name__}: {error}")
    finally:
        for sig in previous:
            signal.signal(sig, signal.SIG_IGN)
        try:
            record["cleanup"] = resources.cleanup() if resources else {"errors": [], "resources": "none started"}
            if resources:
                record["errors"].extend(record["cleanup"]["errors"])
        except BaseException as error:
            record["errors"].append(f"cleanup: {error}")
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, interrupted)
    if not record["errors"] and pilot:
        try:
            preparation.seal(resources, options, record)
        except BaseException as error:
            record["errors"].append(f"preparation: {type(error).__name__}: {error}")
    if resources is not None and options.mode=='lane' and record.get('job_deadline') is not None:
        resources.command_deadline=record['job_deadline']
    if not record["errors"] and record.get("full_preparation_pending"):
        try:
            pending = record.pop("full_preparation_pending")
            deadline = record["job_deadline"]
            if preparation.file_map(pending["roots"], deadline) != pending["files"]:
                raise smoke.SmokeError("preparation outputs changed during cleanup")
            if verify_freeze(options.freeze) != record["freeze_sha256"]:
                raise smoke.SmokeError("full preparation source changed during cleanup")
            pending["producer_receipt"] = str(output / "receipt.json")
            manifest = output / "preparation.json"
            write_json(manifest, pending)
            record["preparation_record"] = dict(path=str(manifest), sha256=smoke.sha(manifest.read_bytes()))
        except BaseException as error:
            record["errors"].append(f"full preparation: {type(error).__name__}: {error}")
    if not record['errors'] and record.get('full_preparation_consumed'):
        try:
            pending=record['full_preparation_consumed']
            preparation.verify_full_consumer(options.prepared,pending['context'],set(pending['required']),
                pending['trace'],pending['snapshots'],record['job_deadline'])
            record['preparation_consumed']=pending['snapshots']
        except BaseException as error:
            record['errors'].append(f'full preparation consumption: {type(error).__name__}: {error}')
    record.pop('full_preparation_consumed',None)
    record.pop("full_preparation_pending", None)
    if options.mode=='lane' and not record['errors'] and record.get('freeze_sha256'):
        try:
            remaining(record['job_deadline'],1)
            if (options.expected_source_sha256
                    and planner.profile_source_anchor(options.root,options.go_repo,inspector=resources.inspect,
                        deadline=record['job_deadline'])!=options.expected_source_sha256):
                raise smoke.SmokeError('campaign source anchor changed through cleanup')
            if options.worker_contract:
                profile=json.loads(options.profile.read_text())
                record['terminal_worker_constraints']=verify_worker_contract(resources,options,profile)
            if (verify_freeze(options.freeze)!=record['freeze_sha256']
                    or record['before']!=profile_source_identities(resources)):
                raise smoke.SmokeError('full-profile source/freeze changed through cleanup')
            if record.get('swift_stdout_capture'):
                captured=record['swift_stdout_capture']
                if (smoke.sha(Path(captured['path']).read_bytes())!=captured['sha256']
                        or smoke.sha((resources.output/'native-command.log').read_bytes())!=captured['sha256']):
                    raise smoke.SmokeError('Swift native stdout changed through cleanup')
            if record.get('load_overlay'):
                preparation.verify_load_overlay(record['load_overlay'])
            if record.get('wire_overlay'):
                preparation.verify_wire_overlay(record['wire_overlay'],record['job_deadline'])
            if record.get('resource_overlay'):
                preparation.verify_resource_overlay(record['resource_overlay'],record['job_deadline'])
            if record.get('web_deadline_support'):
                web=record['web_deadline_support']
                if preparation.file_map(web['roots'],record['job_deadline'])!=web['files']:
                    raise smoke.SmokeError('generated web deadline hook/config changed during execution')
                if any(smoke.sha(Path(name).read_bytes())!=digest for name,digest in web['sources'].items()):
                    raise smoke.SmokeError('web deadline source/config changed during execution')
                if any(smoke.sha(Path(name).read_bytes())!=digest for name,digest in record['web_deadline_audit']['artifacts'].items()):
                    raise smoke.SmokeError('actual web deadline evidence changed during cleanup')
            if record.get('build_artifact'):
                verify_build_artifact(record['build_artifact'],output)
            if record.get('worker_browser_support'):
                web=record['worker_browser_support']
                if preparation.file_map(web['roots'],record['job_deadline'])!=web['files']:
                    raise smoke.SmokeError('worker Browser hook/config changed through cleanup')
                if any(smoke.sha(Path(name).read_bytes())!=digest for name,digest in web['sources'].items()):
                    raise smoke.SmokeError('worker Browser source/config changed through cleanup')
                if any(smoke.sha(Path(name).read_bytes())!=digest for name,digest in record['worker_browser_audit']['artifacts'].items()):
                    raise smoke.SmokeError('raw worker Browser evidence changed through cleanup')
            if record.get('load_sequence_pending'):
                seal_load_sequence(resources,record)
            if record.get('retained_pending'):
                pending=record['retained_pending']
                if preparation.file_map(pending['roots'],record['job_deadline'])!=pending['files']:
                    raise smoke.SmokeError('retained evidence changed during cleanup')
                verify_build_artifact(pending['artifact'],output)
                if smoke.sha(Path(pending['producer']['path']).read_bytes())!=pending['producer']['sha256']:
                    raise smoke.SmokeError('retained authority producer changed through cleanup')
                if record.get('retained_classpath_preparation'):
                    prepared=record['retained_classpath_preparation']
                    preparation.verify_full_consumer([Path(path)for path in prepared['paths']],prepared['context'],
                        set(prepared['required']),prepared['trace'],prepared['snapshots'],record['job_deadline'])
                record['retained_native']['cleanup']=record['cleanup']
            if record.get('wire_pending'):
                pending=record['wire_pending']
                if preparation.file_map(pending['roots'],record['job_deadline'])!=pending['files']:
                    raise smoke.SmokeError('wire actual/native/comparison evidence changed during cleanup')
                observer=resources.observers[pending['observer_index']]
                if observer.report()['requests']!=pending['requests']or observer.report()['errors']:
                    raise smoke.SmokeError('wire capture gained requests/errors after native reconciliation')
                build=record['wire_authority_preparation']
                verify_build_artifact(build['artifact'],output)
                preparation.verify_wire_overlay(build['overlay'],record['job_deadline'])
                if record.get('wire_raw_preparation'):
                    build=record['wire_raw_preparation'];verify_build_artifact(build['artifact'],Path(build['path']).parent)
                    preparation.verify_wire_overlay(build['overlay'],record['job_deadline'])
                    if smoke.sha(Path(build['path']).read_bytes())!=build['sha256']:raise smoke.SmokeError('wire raw producer changed during cleanup')
                if record.get('wire_fetch_preparation'):
                    prepared=record['wire_fetch_preparation']
                    preparation.verify_full_consumer([Path(path)for path in prepared['paths']],prepared['context'],
                        set(prepared['required']),prepared['trace'],prepared['snapshots'],record['job_deadline'])
                record['wire_native']['cleanup']=record['cleanup']
            for child in record.get('wire_runtimes',[]):
                if smoke.sha(Path(child['path']).read_bytes())!=child['sha256']:raise smoke.SmokeError('wire Fetch child receipt changed during cleanup')
            if record.get('resource_pending'):
                pending=record['resource_pending']
                if preparation.file_map(pending['roots'],record['job_deadline'])!=pending['files']:
                    raise smoke.SmokeError('resource diagnostics changed during cleanup')
                verify_build_artifact(pending['artifact'],output)
                preparation.verify_resource_overlay(pending['overlay'],record['job_deadline'])
                if smoke.sha(Path(pending['producer']['path']).read_bytes())!=pending['producer']['sha256']:
                    raise smoke.SmokeError('resource binary producer changed through cleanup')
                record['resource_native']['cleanup']=record['cleanup']
        except BaseException as error:
            record['errors'].append(f'full-profile terminal binding: {type(error).__name__}: {error}')
    if options.mode == "lane" and record.get("job_deadline",float("inf")) < time.monotonic():
        record["errors"].append("entry wall budget exceeded including metadata/cleanup")
    if not record['errors'] and record.pop('load_sequence_verified',False):record['sequence_complete']=True
    if record.get('fixture_started_monotonic') is not None:
        record['fixture_elapsed_seconds']=time.monotonic()-record['fixture_started_monotonic']
        record['fixture_wall_seconds_actual']=(time.time_ns()-record['fixture_started_unix_ns'])/1e9
        if abs(record['fixture_elapsed_seconds']-record['fixture_wall_seconds_actual'])>2:
            record['errors'].append('load fixture monotonic/wall clock differs: suspended or clock changed')
            record['sequence_complete']=False
    record.pop('load_sequence_pending',None)
    record.pop('retained_pending',None)
    record.pop('wire_pending',None)
    record.pop("job_deadline", None)
    record.pop("coordinator_started", None)
    record.pop("preparation_pending", None)
    record.update(status="FAIL" if record["errors"] else "PASS", elapsed_seconds=round(time.monotonic() - started, 6))
    for sig in previous:
        signal.signal(sig, signal.SIG_IGN)
    try:
        safe = smoke.redact(json.dumps(record, indent=2) + "\n", resources.env if resources else os.environ)
        (output / "receipt.json").write_text(safe)
        print(json.dumps({k: record[k] for k in ("status", "errors", "elapsed_seconds")}))
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    return int(bool(record["errors"]))


if __name__ == "__main__":
    raise SystemExit(main())

"""Opt-in Gradle preparation bindings; native results are never reusable artifacts."""
from __future__ import annotations

import json
import math
import hashlib
import os
from contextlib import contextmanager
from pathlib import Path
import re
import signal
import stat
import time

from tools.qualification import oversqlite_smoke as smoke
from tools.qualification import planner
from tools.qualification.platform_resources import remaining

METADATA_SECONDS = 120
LOAD_UPGRADE_PREIMAGE = '2f16315a2aeff82c255fa5cb030a39f728b453eeaafc241dc43d330817bdd13c'


def load_durability_source(original: bytes) -> bytes:
    """Apply only the reviewed load writer durability and per-opening witness."""
    if smoke.sha(original) != LOAD_UPGRADE_PREIMAGE:
        raise smoke.SmokeError('load durability needs the reviewed current upgrade.go preimage')
    text = original.decode('utf-8')
    journal = '\tquery.Set("_journal_mode", "WAL")\n'
    ownership = '\tif err := bindOpeningDBOwnership(identity.Key, db); err != nil {\n'
    if (text.count(journal) != 1 or text.count(ownership) != 1
            or text.count('\tdb.SetMaxOpenConns(4)\n') != 1):
        raise smoke.SmokeError('load durability source anchors changed')
    witness = '''\tvar qualificationSync, qualificationFK int
\tvar qualificationJournal string
\tif err := db.QueryRow(`SELECT s.synchronous,f.foreign_keys,j.journal_mode FROM pragma_synchronous s CROSS JOIN pragma_foreign_keys f CROSS JOIN pragma_journal_mode j`).Scan(&qualificationSync, &qualificationFK, &qualificationJournal); err != nil {
\t\treturn nil, fmt.Errorf("qualification SQLite durability witness: %w", err)
\t}
\tif qualificationSync != 2 || qualificationFK != 1 || qualificationJournal != "wal" {
\t\treturn nil, fmt.Errorf("qualification SQLite durability is not FULL/WAL/FK")
\t}
\tfmt.Printf("QUALIFICATION_LOAD_SQLITE_FULL path=%s synchronous=%d foreign_keys=%d journal=%s\\n", canonicalPath, qualificationSync, qualificationFK, qualificationJournal)
'''
    return text.replace(journal, journal+'\tquery.Set("_synchronous", "FULL")\n').replace(ownership, witness+ownership).encode('utf-8')


def generate_load_overlay(root: Path, go: Path, output: Path, frozen_files: dict) -> dict:
    """New owned overlay; neither the SDK source nor a historical packet is edited."""
    original = go / 'oversqlite/upgrade.go'
    scenario = root / 'tools/qualification/profiles/fixtures/load_scenario_test.go'
    sources = {}
    for path in (original, scenario):
        raw = path.read_bytes()
        digest = smoke.sha(raw)
        if frozen_files.get(str(path)) != digest:
            raise smoke.SmokeError('load overlay source is not frozen: '+str(path))
        sources[str(path)] = digest
    generated = load_durability_source(original.read_bytes())
    virtual = go / 'integration/mobile_flow/qualification_load_scenario_test.go'
    if virtual.exists() or virtual.is_symlink():
        raise smoke.SmokeError('load scenario overlay would shadow an existing source')
    folder = output / 'load-overlay'
    folder.mkdir(mode=0o700)
    upgrade = folder / 'upgrade.go'
    with upgrade.open('xb') as stream:
        stream.write(generated)
    upgrade.chmod(0o600)
    overlay = folder / 'overlay.json'
    value = {'Replace': {str(original): str(upgrade), str(virtual): str(scenario)}}
    with overlay.open('x') as stream:
        json.dump(value, stream, indent=2);stream.write('\n')
    overlay.chmod(0o600)
    record = dict(path=str(overlay), sha256=smoke.sha(overlay.read_bytes()),
        generated_source=str(upgrade), generated_sha256=smoke.sha(generated), sources=sources,
        allowed_delta=['NewOpening DSN _synchronous=FULL', 'per-opening actual FULL/WAL/FK witness'],
        pool_connections=4, native_credit=0)
    (folder / 'binding.json').write_text(json.dumps(record, indent=2)+'\n')
    return record


def verify_load_overlay(record: dict) -> None:
    for name, digest in ((record['path'],record['sha256']),
                         (record['generated_source'],record['generated_sha256']),
                         *record['sources'].items()):
        path = Path(name)
        if not path.is_absolute() or path.is_symlink() or smoke.sha(path.read_bytes()) != digest:
            raise smoke.SmokeError('load overlay/input changed before terminal binding: '+name)


def generate_wire_overlay(root: Path, go: Path, output: Path, frozen: dict, *, raw_client: bool) -> dict:
    """Only named new facades/tests and the already pinned FULL opening witness."""
    folder=output/'wire-overlay';folder.mkdir(mode=0o700);sources={};replace={}
    names=[('oversync/qualification_wire_encoder.go','wire_encoder.go')]
    if raw_client:names.append(('integration/nethttp_server/server/qualification_wire_artifacts_test.go','wire_artifacts_test.go'))
    for virtual,name in names:
        source=root/'tools/qualification/profiles/fixtures'/name;target=go/virtual
        if target.exists()or target.is_symlink():raise smoke.SmokeError('wire overlay would shadow an existing source')
        digest=smoke.sha(source.read_bytes())
        if source.is_symlink()or frozen.get(str(source))!=digest:raise smoke.SmokeError('wire named support source is not frozen')
        sources[str(source)]=digest;replace[str(target)]=str(source)
    if raw_client:
        source=go/'oversqlite/upgrade.go';digest=smoke.sha(source.read_bytes())
        if frozen.get(str(source))!=digest:raise smoke.SmokeError('wire FULL opening preimage is not frozen')
        sources[str(source)]=digest;generated=folder/'upgrade.go'
        generated.write_bytes(load_durability_source(source.read_bytes()).replace(b'QUALIFICATION_LOAD_SQLITE_FULL',b'QUALIFICATION_WIRE_SQLITE_FULL'))
        generated.chmod(0o600);replace[str(source)]=str(generated)
    path=folder/'overlay.json';path.write_text(json.dumps({'Replace':replace},indent=2)+'\n');path.chmod(0o600)
    return dict(path=str(path),sources=sources,roots=[str(folder)],files=file_map([str(folder)],time.monotonic()+30),
        raw_client=raw_client,native_credit=0,
        allowed_delta='Named qualification facades/test; raw client only inherits the reviewed exact FULL/WAL/FK opening DSN and per-opening witness; pool4 unchanged.')


def verify_wire_overlay(record: dict, deadline: float) -> None:
    if file_map(record['roots'],deadline)!=record['files']:raise smoke.SmokeError('wire generated overlay changed')
    for name,digest in record['sources'].items():
        remaining(deadline,1)
        if Path(name).is_symlink()or smoke.sha(Path(name).read_bytes())!=digest:raise smoke.SmokeError('wire named source changed')


RESOURCE_PREIMAGES = {
    'oversqlite/baseline_qualification_test.go':'8377b869543dee63a4825924fcdcf3ba587221be6fc3817e50562d4ad6631bb7',
    'oversqlite/replay_diagnostics_test.go':'2d92f22471ab08b833a9b92afd8b0e85e9be4d46a63eb8e58692c44a4670d1ab',
}


def resource_durability_source(name: str, raw: bytes) -> bytes:
    if smoke.sha(raw)!=RESOURCE_PREIMAGES.get(name):
        raise smoke.SmokeError('resource durability needs its reviewed current test preimage')
    text=raw.decode();old='path+"?_foreign_keys=on"'
    if text.count(old)!=(2 if name.endswith('baseline_qualification_test.go')else 1):
        raise smoke.SmokeError('resource durability DSN anchors changed')
    text=text.replace(old,'path+"?_foreign_keys=on&_synchronous=FULL&_journal_mode=WAL"')
    if name.endswith('replay_diagnostics_test.go'):
        anchor='\t\t\t\tc, db, path := qualificationDurableClient(t)\n'
        witness='''\t\t\t\tvar journal string
\t\t\t\tvar synchronous, foreignKeys int
\t\t\t\trequire.NoError(t, db.QueryRow("PRAGMA journal_mode").Scan(&journal))
\t\t\t\trequire.NoError(t, db.QueryRow("PRAGMA synchronous").Scan(&synchronous))
\t\t\t\trequire.NoError(t, db.QueryRow("PRAGMA foreign_keys").Scan(&foreignKeys))
\t\t\t\trequire.Equal(t, "wal", journal)
\t\t\t\trequire.Equal(t, 2, synchronous)
\t\t\t\trequire.Equal(t, 1, foreignKeys)
'''
        if text.count(anchor)!=1:raise smoke.SmokeError('resource durability witness anchor changed')
        text=text.replace(anchor,anchor+witness)
    return text.encode()


def resource_driver_source(raw: bytes) -> bytes:
    if smoke.sha(raw)!='6e442717046fe2f3cb03eca6305ab0ea37bb705eb03e9bedd2cc7086435b4b48':
        raise smoke.SmokeError('resource observer requires the reviewed sqlite3 v1.14.37 source')
    text=raw.decode()
    patches=[
        ('type SQLiteStmt struct {','type SQLiteStmt struct {\n\tdiagnosticSQL string'),
        ('ss := &SQLiteStmt{c: c, s: s, t: t}','ss := &SQLiteStmt{c: c, s: s, t: t, diagnosticSQL: query}\n\treplayBindingStatements.Store(ss, query)'),
        ('s.closed = true\n\truntime.SetFinalizer(s, nil)','s.closed = true\n\treplayBindingStatements.Delete(s)\n\truntime.SetFinalizer(s, nil)'),
        ('rv := C._sqlite3_prepare_v2_internal(c.db, pquery, C.int(-1), &s, &tail)','diagnosticStart := time.Now()\n\trv := C._sqlite3_prepare_v2_internal(c.db, pquery, C.int(-1), &s, &tail)\n\treplayDiagnosticEvent(query, "prepare", time.Since(diagnosticStart).Nanoseconds(), rv == C.SQLITE_OK, 0)'),
        ('rv := C._sqlite3_step_row_internal(s.s, &rowid, &changes)','diagnosticStart := time.Now()\n\trv := C._sqlite3_step_row_internal(s.s, &rowid, &changes)\n\treplayDiagnosticEvent(s.diagnosticSQL, "step", time.Since(diagnosticStart).Nanoseconds(), rv == C.SQLITE_ROW || rv == C.SQLITE_DONE || rv == C.SQLITE_OK, 0)'),
        ('rv := C._sqlite3_step_internal(rc.s.s)','diagnosticStart := time.Now()\n\trv := C._sqlite3_step_internal(rc.s.s)\n\treplayDiagnosticEvent(rc.s.diagnosticSQL, "step", time.Since(diagnosticStart).Nanoseconds(), rv == C.SQLITE_ROW || rv == C.SQLITE_DONE || rv == C.SQLITE_OK, 0)'),
        ('func (s *SQLiteStmt) bind(args []driver.NamedValue) error {','func (s *SQLiteStmt) bind(args []driver.NamedValue) error {\n\treplayDiagnosticEvent(s.diagnosticSQL, "bind", 0, true, int64(len(args)))'),
        ('rv := C.sqlite3_finalize(stmt)','if stmt != nil { replayDiagnosticNative(s.diagnosticSQL, int64(C.sqlite3_stmt_status(stmt, C.SQLITE_STMTSTATUS_VM_STEP, 0)), int64(C.sqlite3_stmt_status(stmt, C.SQLITE_STMTSTATUS_FULLSCAN_STEP, 0)), int64(C.sqlite3_stmt_status(stmt, C.SQLITE_STMTSTATUS_SORT, 0)), int64(C.sqlite3_stmt_status(stmt, C.SQLITE_STMTSTATUS_AUTOINDEX, 0)), int64(C.sqlite3_stmt_status(stmt, C.SQLITE_STMTSTATUS_REPREPARE, 0))) }\n\trv := C.sqlite3_finalize(stmt)\n\treplayDiagnosticEvent(s.diagnosticSQL, "finalize", 0, rv == C.SQLITE_OK, 0)'),
    ]
    for old,new in patches:
        if text.count(old)!=1:raise smoke.SmokeError('resource observer driver anchor changed')
        text=text.replace(old,new)
    return (text+'''
// Qualification-only read observers. Explicit Close removes strong references.
// Counters use reset=0 and never alter binding, execution or finalization.
var replayBindingStatements sync.Map
func ReplayBindingMemory() map[string]int64 {
 result := map[string]int64{"native_memory_used": int64(C.sqlite3_memory_used())}
 replayBindingStatements.Range(func(key, value any) bool {
  stmt := key.(*SQLiteStmt); stmt.mu.Lock()
  if !stmt.closed && stmt.s != nil { result[value.(string)] += int64(C.sqlite3_stmt_status(stmt.s, C.SQLITE_STMTSTATUS_MEMUSED, 0)) }
  stmt.mu.Unlock(); return true
 }); return result
}
''').encode()


RESOURCE_DRIVER_COUNTERS = '''package sqlite3
import "sync"
type ReplayDiagnosticCounts struct {
 Prepares, PrepareNS, Steps, StepNS, Binds, Arguments, Finalizes, Errors int64
 VMInstructions, FullscanSteps, Sorts, AutomaticIndexRows, Reprepares int64
}
var replayDiagnosticMu sync.Mutex
var replayDiagnosticData map[string]*ReplayDiagnosticCounts
func ReplayDiagnosticStart() { replayDiagnosticMu.Lock(); defer replayDiagnosticMu.Unlock(); replayDiagnosticData = map[string]*ReplayDiagnosticCounts{} }
func ReplayDiagnosticStop() map[string]*ReplayDiagnosticCounts { replayDiagnosticMu.Lock(); defer replayDiagnosticMu.Unlock(); d := replayDiagnosticData; replayDiagnosticData = nil; return d }
func replayDiagnosticEvent(q, kind string, ns int64, ok bool, args int64) {
 replayDiagnosticMu.Lock(); defer replayDiagnosticMu.Unlock()
 if replayDiagnosticData == nil { return }
 c := replayDiagnosticData[q]; if c == nil { c = &ReplayDiagnosticCounts{}; replayDiagnosticData[q] = c }
 if !ok { c.Errors++ }
 switch kind {
 case "prepare": c.Prepares++; c.PrepareNS += ns
 case "step": c.Steps++; c.StepNS += ns
 case "bind": c.Binds++; c.Arguments += args
 case "finalize": c.Finalizes++
 }
}
func replayDiagnosticNative(q string, vm, fullscan, sorts, autoindex, reprepares int64) {
 replayDiagnosticMu.Lock(); defer replayDiagnosticMu.Unlock(); if replayDiagnosticData == nil { return }
 c:=replayDiagnosticData[q]; if c==nil { c=&ReplayDiagnosticCounts{}; replayDiagnosticData[q]=c }
 c.VMInstructions+=vm; c.FullscanSteps+=fullscan; c.Sorts+=sorts; c.AutomaticIndexRows+=autoindex; c.Reprepares+=reprepares
}
'''


def generate_resource_overlay(go: Path, output: Path, frozen: dict, *, driver: Path | None = None) -> dict:
    """Generate fresh durability overlays and an optional private observer dependency."""
    import shutil
    folder=output/'resource-overlay';folder.mkdir(mode=0o700)
    sources={};replacements={}
    def source(path):
        raw=path.read_bytes();digest=smoke.sha(raw)
        if path.is_symlink()or frozen.get(str(path))!=digest:raise smoke.SmokeError('resource input is not independently frozen: '+str(path))
        sources[str(path)]=digest
        return raw
    for name in RESOURCE_PREIMAGES:
        path=go/name;generated=folder/path.name
        generated.write_bytes(resource_durability_source(name,source(path)));generated.chmod(0o600)
        replacements[str(path)]=str(generated)
    overlay=folder/'overlay.json';overlay.write_text(json.dumps({'Replace':replacements},indent=2)+'\n');overlay.chmod(0o600)
    modfile=None
    if driver is not None:
        if not driver.is_absolute()or driver.is_symlink()or driver.name!='go-sqlite3@v1.14.37':
            raise smoke.SmokeError('resource observer needs the exact resolved sqlite3 dependency')
        for path in sorted(driver.rglob('*')):
            if path.is_symlink():raise smoke.SmokeError('resource dependency contains a symlink')
            if path.is_file():source(path)
        destination=folder/'instrumented-driver';shutil.copytree(driver,destination)
        for path in [destination,*destination.rglob('*')]:path.chmod(0o700 if path.is_dir()else 0o600)
        (destination/'sqlite3.go').write_bytes(resource_driver_source(source(driver/'sqlite3.go')))
        (destination/'replay_diagnostic.go').write_text(RESOURCE_DRIVER_COUNTERS)
        modfile=folder/'diagnostic.mod'
        modfile.write_bytes(source(go/'go.mod')+('\nreplace github.com/mattn/go-sqlite3 => '+str(destination)+'\n').encode())
        (folder/'diagnostic.sum').write_bytes(source(go/'go.sum'))
    record=dict(path=str(overlay),modfile=str(modfile)if modfile else None,observed=driver is not None,
        sources=sources,roots=[str(folder)],files=file_map([str(folder)],time.monotonic()+30),
        claim='External observer overhead included; 10ms file samples are lower bounds; no SDK or deployment-wide ceiling.')
    (output/'resource-overlay-binding.json').write_text(json.dumps(record,indent=2)+'\n')
    return record


def verify_resource_overlay(record: dict, deadline: float) -> None:
    if file_map(record['roots'],deadline)!=record['files']:
        raise smoke.SmokeError('resource generated overlay/driver/modfile changed')
    for name,digest in record['sources'].items():
        remaining(deadline,1)
        if Path(name).is_symlink()or smoke.sha(Path(name).read_bytes())!=digest:
            raise smoke.SmokeError('resource input changed through cleanup')


LOAD_PREREQUISITES = {'server_owning','client_batch_a','client_batch_b','client_race',
                      'go_retained_default','go_retained_one','public_resource'}


def frozen_json(binding: dict, files: dict) -> tuple[dict,str]:
    path=Path(binding.get('path',''))
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise smoke.SmokeError('missing independently frozen prerequisite artifact')
    raw=path.read_bytes();digest=smoke.sha(raw)
    if digest!=binding.get('sha256') or files.get(str(path))!=digest:
        raise smoke.SmokeError('prerequisite artifact changed or is not frozen')
    try:
        value=planner._strict_record_json(raw,require_canonical=False)
    except ValueError as error:
        raise smoke.SmokeError('malformed/duplicate/nonfinite prerequisite JSON')from error
    if not isinstance(value,dict):raise smoke.SmokeError('prerequisite artifact must be an object')
    return value,digest


def verify_wire_derivation(resources, proof: dict, files: dict, manifest_sha: str,
                           engines: list[Path], runtime: dict) -> dict:
    """Require the complete current producer census, never a supplied subset."""
    paths=resources.inspect(['git','ls-files','-z','--cached','--others','--exclude-standard'],cwd=resources.go).split('\0')
    go_sources={str(resources.go/name):smoke.sha((resources.go/name).read_bytes())for name in paths
        if name and(name.endswith('.go')or Path(name).name in('go.mod','go.sum','go.work','go.work.sum'))and(resources.go/name).is_file()}
    named={resources.root/'tools/qualification/profiles/fixtures'/name for name in('wire_authority.go','wire_encoder.go','wire-workloads.json')}
    critical={*named,*engines,resources.go/'scripts/run-with-timeout.pl'}
    expected=go_sources|{str(path):smoke.sha(path.read_bytes())for path in critical}
    supplied=proof.get('sources',{})
    required_supplied={name:value for name,value in supplied.items()if Path(name).is_relative_to(resources.go)or Path(name)in critical}
    current=smoke.identity(resources.go,resources.env,inspector=resources.inspect)
    binary=proof.get('binary',{});path=Path(binary.get('path',''))
    if (proof.get('status')!='PASS'or proof.get('native_credit')!=0 or proof.get('cleanup',{}).get('errors')
            or proof.get('cleanup',{}).get('groups_absent')is not True or proof.get('manifest',{}).get('sha256')!=manifest_sha
            or proof.get('go_runtime')!=runtime or runtime.get('version')!='go1.25.12'
            or proof.get('before',{}).get(str(resources.go))!=current or proof.get('after',{}).get(str(resources.go))!=current
            or not go_sources or required_supplied!=expected or any(files.get(name)!=value for name,value in expected.items())
            or not path.is_absolute()or path.is_symlink()or not path.is_file()or files.get(str(path))!=binary.get('sha256')
            or smoke.sha(path.read_bytes())!=binary.get('sha256')or path.stat().st_mode&0o777!=binary.get('mode')
            or not binary.get('mode',0)&0o111):
        raise smoke.SmokeError('wire derivation lacks complete unchanged Go/named/engine/binary bindings')
    return dict(go_sources=len(go_sources),go_sources_sha256=smoke.sha(planner.canonical_bytes(go_sources)),
        mandatory_named_sources=sorted(str(path)for path in named),generator_sha256=binary['sha256'])


def verify_load_prerequisites(resources, binding: dict, profile: str, files: dict) -> dict:
    """Explicit reviewed structural union and measured ack/storage, before a fixture."""
    proof,digest=frozen_json(binding,files)
    paths=resources.inspect(['git','ls-files','-z','--cached','--others','--exclude-standard'],cwd=resources.go).split('\0')
    sources={str(resources.go/name):smoke.sha((resources.go/name).read_bytes())for name in paths
             if name and name.endswith(('.go','go.mod','go.sum')) and (resources.go/name).is_file()}
    if (proof.get('schema_version')!=1 or proof.get('status')!='PASS' or proof.get('profile_sha256')!=profile
            or not sources or proof.get('go_sources')!=sources or any(files.get(k)!=v for k,v in sources.items())
            or set(proof.get('receipts',{}))!=LOAD_PREREQUISITES):
        raise smoke.SmokeError('load prerequisites omit or change the current complete Go/source union')
    current=smoke.identity(resources.go,resources.env,inspector=resources.inspect);hashes={}
    for role,group in proof['receipts'].items():
        if not group.get('reason') or not group.get('attempts'):
            raise smoke.SmokeError('missing reviewed load prerequisite role: '+role)
        for item in group['attempts']:
            receipt,receipt_hash=frozen_json(item,files)
            if (receipt.get('status')!='PASS' or receipt.get('errors') or receipt.get('profile_sha256')!=profile
                    or receipt.get('go_runtime',{}).get('version')!='go1.25.12'
                    or receipt.get('before',{}).get(str(resources.go))!=current
                    or receipt.get('after',{}).get(str(resources.go))!=current
                    or receipt.get('native_credit',0)<=0 or receipt.get('native',{}).get('bodies')!=receipt['native_credit']
                    or receipt.get('cleanup',{}).get('groups_absent')is not True or receipt['cleanup'].get('errors')):
                raise smoke.SmokeError('load prerequisite is failed/stale/unexecuted: '+role)
            hashes[item['path']]=receipt_hash
    cost,cost_hash=frozen_json(proof['ack_storage'],files)
    acks=cost.get('acks',[]);samples=cost.get('physical_samples',[])
    if (cost.get('errors') or not acks or not samples or cost.get('peak_outcome_envelope_bytes',0)<=0
            or max(x['outcome_envelope_bytes']for x in samples)!=cost['peak_outcome_envelope_bytes']):
        raise smoke.SmokeError('load prerequisite lacks actual positive ack/storage observations')
    for ack in acks:
        if (ack.get('status')!=204 or ack.get('v1')!='v1' or ack.get('decoded_request_body_bytes',0)<=0
                or ack.get('encoded_request_body_bytes',0)<=0 or not math.isfinite(ack.get('upstream_seconds',-1))
                or ack['upstream_seconds']<0):
            raise smoke.SmokeError('load prerequisite ack bytes/status/latency are incomplete')
    review,review_hash=frozen_json(proof['completed_review'],files)
    if (review.get('status')!='approved-for-load-prerequisites' or not review.get('reviewer')
            or review.get('profile_sha256')!=profile or review.get('receipt_hashes')!=hashes
            or review.get('go_sources_sha256')!=smoke.sha(planner.canonical_bytes(sources))
            or review.get('prerequisites_sha256')!=smoke.sha(planner.canonical_bytes({k:v for k,v in proof.items()if k!='completed_review'}))
            or review.get('ack_storage_sha256')!=cost_hash or set(review.get('required_roles',[]))!=LOAD_PREREQUISITES):
        raise smoke.SmokeError('load prerequisites need an exact completed source/union/measurement review')
    return dict(proof_sha256=digest,receipt_hashes=hashes,review_sha256=review_hash,
        go_sources_sha256=smoke.sha(planner.canonical_bytes(sources)),ack_storage_sha256=cost_hash,
        ack_request_count=len(acks),peak_outcome_envelope_bytes=cost['peak_outcome_envelope_bytes'])


class Budget:
    """One cumulative allowance shared by new metadata phases, excluding native execution."""
    def __init__(self, seconds: float = METADATA_SECONDS):
        self.remaining = seconds

    @contextmanager
    def phase(self):
        started = time.monotonic()
        deadline = started + self.remaining
        remaining(deadline, self.remaining)
        if signal.getitimer(signal.ITIMER_REAL) != (0.0, 0.0):
            raise smoke.SmokeError("metadata cannot replace an existing alarm deadline")
        def expired(number, frame):
            raise smoke.SmokeError("shared metadata deadline exhausted")
        previous = signal.signal(signal.SIGALRM, expired)
        signal.setitimer(signal.ITIMER_REAL, self.remaining)
        try:
            yield deadline
            remaining(deadline, self.remaining)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous)
            self.remaining -= time.monotonic() - started


def environment_binding(env: dict[str, str]) -> dict:
    excluded = {"OVERSQLITE_PROCESS_OWNER", "CODEX_TOOL_CALL_ID"}
    return {key: smoke.sha(value.encode()) for key, value in sorted(env.items()) if key not in excluded}


TRACE = r'''
import groovy.json.JsonOutput
gradle.taskGraph.whenReady { graph ->
    def selected = graph.allTasks.find { it.path == __TASK__ }
    if (selected != null) {
        def events = [:]
        def starts = [:]
        graph.beforeTask { task -> starts[task.path] = System.nanoTime() }
        graph.afterTask { task, state ->
            events[task.path] = [did_work: state.didWork, skipped: state.skipped,
                up_to_date: state.upToDate, failure: state.failure != null,
                seconds: (System.nanoTime() - starts[task.path]) / 1_000_000_000.0]
        }
        gradle.buildFinished { result ->
            if (result.failure == null) {
                def tasks = graph.allTasks.collect { task ->
                    [path: task.path, outcome: events[task.path],
                     inputs: task.inputs.files.files.collect { it.absolutePath }.sort(),
                     outputs: task.path == selected.path ? [] :
                         task.outputs.files.files.collect { it.absolutePath }.sort()]
                }
                new File(__REPORT__).text = JsonOutput.toJson([version: 1,
                    selected: selected.path, gradle: gradle.gradleVersion,
                    java_home: System.getProperty('java.home'), tasks: tasks])
            }
        }
    }
}
'''


def _literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def instrument(argv: list[str], output: Path) -> list[str]:
    """Observe the existing task graph without changing task inputs or outputs."""
    target = next(arg for arg in argv if arg.startswith(":platform-oversqlite-test:composeApp:"))
    script = output / "preparation.init.gradle"
    script.write_text(TRACE.replace("__TASK__", _literal(target)).replace(
        "__REPORT__", _literal(str(output / "preparation-trace.json"))))
    return [argv[0], "-I", str(script), *argv[1:]]


def read_trace(output: Path, task: str) -> dict:
    try:
        trace = json.loads((output / "preparation-trace.json").read_text())
        if not isinstance(trace["gradle"], str) or not trace["gradle"] or not Path(trace["java_home"]).is_absolute():
            raise ValueError("missing actual Gradle/Java runtime")
        tasks = trace["tasks"]
        if trace["version"] != 1 or trace["selected"] != task or not tasks:
            raise ValueError("wrong graph")
        names = [entry["path"] for entry in tasks]
        if len(names) != len(set(names)) or names.count(task) != 1:
            raise ValueError("duplicate/missing task")
        for entry in tasks:
            if not isinstance(entry["inputs"], list) or not isinstance(entry["outputs"], list):
                raise ValueError("invalid file inventory")
            state = entry["outcome"]
            if state["failure"] or not isinstance(state["seconds"], (int, float)) or not math.isfinite(state["seconds"]) or state["seconds"] < 0:
                raise ValueError("incomplete task outcome")
        selected = tasks[names.index(task)]
        if selected["outputs"] or selected["outcome"]["skipped"] or selected["outcome"]["up_to_date"]:
            raise ValueError("native task did not execute freshly")
        return trace
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise smoke.SmokeError("missing, partial or invalid preparation trace") from error


def file_map(roots: list[str], deadline: float | None = None) -> dict:
    """Include directory membership, modes and symlink targets as well as bytes."""
    pending = [Path(name) for name in roots]
    deadline = time.monotonic() + METADATA_SECONDS if deadline is None else deadline
    visited, entries = set(), {}
    while pending:
        remaining(deadline, METADATA_SECONDS)
        path = pending.pop()
        if not path.is_absolute():
            raise smoke.SmokeError("preparation path must be absolute")
        name = str(path)
        if name in visited:
            continue
        visited.add(name)
        try:
            info = path.lstat()
        except FileNotFoundError:
            entries[name] = {"kind": "missing"}
            continue
        mode = stat.S_IMODE(info.st_mode)
        if path.is_symlink():
            target = path.resolve(strict=True)
            entries[name] = dict(kind="symlink", mode=mode, target=os.readlink(path))
            pending.append(target)
        elif path.is_dir():
            children = sorted(path.iterdir())
            entries[name] = dict(kind="directory", mode=mode, children=[p.name for p in children])
            pending.extend(children)
        elif path.is_file():
            digest = hashlib.sha256()
            with path.open("rb") as stream:
                while chunk := stream.read(1024 * 1024):
                    remaining(deadline, METADATA_SECONDS)
                    digest.update(chunk)
            after = path.stat()
            if (info.st_ino, info.st_size, info.st_mtime_ns, info.st_mode) != (after.st_ino, after.st_size, after.st_mtime_ns, after.st_mode):
                raise smoke.SmokeError("preparation input changed while hashing: " + name)
            entries[name] = dict(kind="file", mode=mode, sha256=digest.hexdigest())
        else:
            raise smoke.SmokeError("unsupported preparation filesystem entry: " + name)
    return dict(sorted(entries.items()))


def context(resources, options, frozen: str, deadline: float | None = None) -> dict:
    # Hash values rather than retaining credentials. Only the runner's per-run
    # ownership token and transport-only tool-call ID vary; all other caller
    # values remain dependencies, with per-key hashes for drift diagnosis.
    deadline = time.monotonic() + METADATA_SECONDS if deadline is None else deadline
    env = resources.env
    java = Path(env.get("JAVA_HOME", ""))
    if not java.is_absolute():
        raise smoke.SmokeError("preparation pilot requires explicit JAVA_HOME")
    probes = [[str(java / "bin/java"), "-version"], ["sw_vers"], ["xcodebuild", "-version"]]
    chrome = Path(env.get("CHROME_BIN", "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"))
    if options.runtime == "browser":
        probes.append([str(chrome), "--version"])
    tools = []
    for argv in probes:
        # java writes its version to stderr; use the existing redacted Child path
        # only for execution. Runtime identities use command output explicitly.
        if argv[-1] == "-version" and argv[0].endswith("/java"):
            argv = [str(java / "bin/java"), "--version"]
        tools.append(dict(argv=argv, output_sha256=smoke.sha(resources.inspect(argv, remaining(deadline, 15)).encode())))
    return dict(version=1, runtime=options.runtime, mode=options.mode, freeze_sha256=frozen,
                source={str(p): smoke.identity(p, resources.env) for p in (resources.root, resources.go)},
                environment=environment_binding(env), tools=tools,
                engine_files=file_map([str(java / "release"), str(java / "bin/java")]+
                                      ([str(chrome), str(chrome.parent.parent / "Frameworks")] if options.runtime == "browser" else []), deadline),
                server="none: verifier uses local owned mock database/worker fixtures")


def roots_for(trace: dict, root: Path, env: dict[str, str] | None = None) -> list[str]:
    paths = {name for entry in trace["tasks"] for field in ("inputs", "outputs") for name in entry[field]}
    paths.update(trace.get('runtime_inputs',[]))
    # Included-build plugin/compiler artifacts and local Gradle configuration
    # influence configuration before the selected graph becomes visible.
    effective = os.environ if env is None else env
    home = Path(effective.get("GRADLE_USER_HOME", str(Path(effective.get("HOME", str(Path.home()))) / ".gradle")))
    paths.update(str(p) for p in (root / "local.properties", home / "gradle.properties", home / "init.d",
                                 root / "sqlitenow-compiler/build/libs", root / "sqlitenow-gradle-plugin/build/libs"))
    return sorted(paths)


def verify(manifest: Path, current: dict, deadline: float | None = None) -> dict:
    try:
        saved = json.loads(manifest.read_text())
        if saved["version"] != 1 or saved["context"] != current or not saved["roots"] or not saved["files"]:
            raise ValueError("input/context drift")
        if not saved["native"]["bodies"] or saved["cleanup"]["errors"] or not saved["cleanup"]["groups_absent"]:
            raise ValueError("unsuccessful predecessor")
        if not saved["gradle_runtime"]["gradle"] or not Path(saved["gradle_runtime"]["java_home"]).is_absolute():
            raise ValueError("missing Gradle runtime binding")
        receipt = manifest.parent / "receipt.json"
        producer = json.loads(receipt.read_text())
        if (saved["producer_receipt"] != str(receipt) or producer["status"] != "PASS"
                or producer["preparation_record"]["sha256"] != smoke.sha(manifest.read_bytes())
                or producer["native"] != saved["native"] or producer["cleanup"] != saved["cleanup"]):
            raise ValueError("preparation producer did not finish successfully")
        if file_map(saved["roots"], deadline) != saved["files"]:
            raise ValueError("artifact/generated/dependency drift")
        return saved
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise smoke.SmokeError("preparation binding mismatch; perform a fresh recorded run") from error


def observed(trace: dict, reused: bool) -> dict:
    builds = [entry for entry in trace["tasks"] if re.search(r":(?:compile|link)[^:]*$", entry["path"], re.I)]
    executed = [entry["path"] for entry in builds if entry["outcome"]["did_work"] and not entry["outcome"]["skipped"]]
    if reused and executed:
        raise smoke.SmokeError("claimed compilation reuse executed build tasks: " + repr(executed))
    return dict(compilation_tasks=len(builds), executed_compilation_tasks=executed,
                compilation_task_seconds=sum(entry["outcome"]["seconds"] for entry in builds),
                native_task_seconds=next(entry["outcome"]["seconds"] for entry in trace["tasks"] if entry["path"] == trace["selected"]))


def verify_graph(saved: dict, trace: dict, roots: list[str], files: dict) -> None:
    runtime = {key: trace[key] for key in ("gradle", "java_home")}
    if runtime != saved["gradle_runtime"] or roots != saved["roots"] or files != saved["files"]:
        raise smoke.SmokeError("Gradle runtime/dependency closure changed during reuse")


def seal(resources, options, record: dict) -> None:
    pending = record.pop("preparation_pending")
    budget = Budget(record["metadata_seconds_remaining"])
    with budget.phase() as deadline:
        if smoke.sha(options.freeze.read_bytes()) != record["freeze_sha256"]:
            raise smoke.SmokeError("freeze manifest changed during cleanup")
        for name, expected in json.loads(options.freeze.read_text())["files"].items():
            remaining(deadline, METADATA_SECONDS)
            if smoke.sha(Path(name).read_bytes()) != expected:
                raise smoke.SmokeError("frozen input changed during cleanup: " + name)
        if context(resources, options, record["freeze_sha256"], deadline) != pending["context"]:
            raise smoke.SmokeError("preparation context changed before successful cleanup")
        if file_map(pending["roots"], deadline) != pending["files"]:
            raise smoke.SmokeError("prepared artifacts changed during execution/cleanup")
        if options.reuse_preparation:
            if smoke.sha(options.reuse_preparation.read_bytes()) != record["preparation_reuse"]["sha256"]:
                raise smoke.SmokeError("preparation manifest changed during cleanup")
            if smoke.sha((options.reuse_preparation.parent / "receipt.json").read_bytes()) != record["preparation_reuse"]["producer_receipt_sha256"]:
                raise smoke.SmokeError("preparation producer receipt changed during cleanup")
        else:
            saved = dict(version=1, **pending, native=record["native"], cleanup=record["cleanup"],
                         producer_receipt=str(resources.output / "receipt.json"))
            path = resources.output / "preparation.json"
            with path.open("x") as stream:
                json.dump(saved, stream, indent=2, sort_keys=True)
                stream.write("\n")
            record["preparation_record"] = dict(path=str(path), sha256=smoke.sha(path.read_bytes()),
                                              files=len(saved["files"]))
    record["metadata_seconds_remaining"] = budget.remaining
    record["metadata_seconds"] = METADATA_SECONDS - budget.remaining


FULL_TRACE = r'''
import groovy.json.JsonOutput
import org.gradle.api.tasks.AbstractExecTask
import org.gradle.api.tasks.testing.AbstractTestTask
def kindOf = { task ->
    if (task instanceof AbstractTestTask || task.class.name.contains('DeviceProviderInstrumentTestTask')) return 'native-test'
    if (task.path == ':oversqliteCrossRepoConformance') return 'driver'
    if (task instanceof AbstractExecTask) {
        def command = task.commandLine.collect { it.toString() }
        if (!command.empty && new File(command[0]).name in ['gradlew', 'gradlew.bat']) return 'driver'
        if (command.contains('test')) return 'native-test'
    }
    return 'preparation'
}
def discoveredDependencyOwner = null
def runtimeClasspaths = [:]
def nativeInputs = [:]
gradle.projectsEvaluated {
    __NATIVE_INPUT_TASKS__.each { target ->
        def split = target.lastIndexOf(':')
        def owner = rootProject.findProject(target.substring(0, split))
        if (owner != null) owner.tasks.named('help').configure {
            doLast {
                def task = owner.tasks.named(target.substring(split + 1)).get()
                if (kindOf(task) != 'native-test') throw new GradleException('Expected native input owner: ' + target)
                def files = task.inputs.files.files.collect { it.absolutePath } as Set
                // Bind framework aliases, Current links and their exact targets.
                files.toList().each { path ->
                    def parent = new File(path)
                    while (parent != null && !parent.name.endsWith('.framework')) parent = parent.parentFile
                    if (parent != null) files.add(parent.absolutePath)
                }
                nativeInputs[target] = [task: target, files: files.toList().sort()]
            }
        }
    }
    __JVM_RUNTIME_TASKS__.each { target ->
        def split = target.lastIndexOf(':')
        def owner = rootProject.findProject(target.substring(0, split))
        if (owner != null) owner.tasks.named('help').configure {
            doLast {
                def task = owner.tasks.named(target.substring(split + 1)).get()
                if (!(task instanceof org.gradle.api.tasks.testing.Test)) throw new GradleException('Expected JVM Test classpath: ' + target)
                runtimeClasspaths[target] = [task: target, files: task.classpath.files.collect { it.absolutePath }.sort()]
            }
        }
    }
    def target = __DEPENDENCY_TARGET__
    if (target != null) {
        def split = target.lastIndexOf(':')
        def owner = rootProject.findProject(target.substring(0,split))
        if (owner != null) owner.tasks.named('help').configure {
          doLast {
            def selected = owner.tasks.named(target.substring(split + 1)).get()
            if (kindOf(selected) != 'native-test') throw new GradleException('Expected native dependency owner: ' + target)
            discoveredDependencyOwner = [task: target, dependencies: selected.taskDependencies.getDependencies(selected).collect {
                [path: it.path, actionable: !it.actions.empty, kind: kindOf(it),
                 dependencies: it.actions.empty ? it.taskDependencies.getDependencies(it).collect { it.path }.sort() : []]
            }.sort { it.path }]
          }
        }
    }
}
gradle.taskGraph.whenReady { graph ->
    def events = [:]
    def starts = [:]
    // Included builds do not inherit every root -P property. The coordinator
    // freezes this phase literal into the same init script for every build.
    def prepareOnly = __PREPARATION_ONLY__
    graph.beforeTask { task ->
        if (prepareOnly && kindOf(task) != 'preparation') throw new GradleException('Native/driver task in preparation: ' + task.path)
        starts[task.path] = System.nanoTime()
    }
    graph.afterTask { task, state ->
        events[task.path] = [did_work: state.didWork, skipped: state.skipped,
            up_to_date: state.upToDate, failure: state.failure != null,
            seconds: starts[task.path] == null ? 0 : (System.nanoTime()-starts[task.path])/1_000_000_000.0]
    }
    gradle.buildFinished { result ->
        def directory = new File(__OUTPUT__)
        directory.mkdirs()
        def tasks = graph.allTasks.collect { task ->
            [path: task.path, kind: kindOf(task), actionable: !task.actions.empty,
             inputs: task.inputs.files.files.collect { it.absolutePath }.sort(),
             outputs: kindOf(task) == 'native-test' ? [] : task.outputs.files.files.collect { it.absolutePath }.sort(),
             outcome: events[task.path]]
        }
        new File(directory, 'graph-' + Integer.toHexString(rootProject.projectDir.absolutePath.hashCode()) + '.json').text = JsonOutput.toJson(
            [version: 2, root: rootProject.projectDir.absolutePath, preparation: prepareOnly,
             gradle: gradle.gradleVersion, java_home: System.getProperty('java.home'),
             failure: result.failure != null, tasks: tasks, runtime_classpaths: runtimeClasspaths.values().toList().sort { it.task }, native_inputs: nativeInputs.values().toList().sort { it.task }, dependency_owner: discoveredDependencyOwner])
    }
}
'''


def instrument_full(argv: list[str], output: Path, *, prepare_only: bool, runtime_tasks: tuple[str,...] = (),
                    dependency_target: str | None = None, native_input_tasks: tuple[str,...] = ()) -> list[str]:
    """Observe all included graphs and refuse native/driver actions in preparation."""
    if Path(argv[0]).name != 'gradlew':
        raise smoke.SmokeError('full preparation trace requires the existing Gradle wrapper')
    script = output / 'full-preparation.init.gradle'
    script.write_text(FULL_TRACE.replace('__OUTPUT__', _literal(str(output / 'task-traces')))
                      .replace('__PREPARATION_ONLY__','true' if prepare_only else 'false')
                      .replace('__JVM_RUNTIME_TASKS__','['+','.join(_literal(task)for task in runtime_tasks)+']')
                      .replace('__NATIVE_INPUT_TASKS__','['+','.join(_literal(task)for task in native_input_tasks)+']')
                      .replace('__DEPENDENCY_TARGET__',_literal(dependency_target)if dependency_target is not None else'null'))
    args = [argv[0], '-I', str(script), *argv[1:]]
    for task in sorted({target.rsplit(':',1)[0]+':help' for target in (*runtime_tasks,*native_input_tasks)}):
        if task not in args:args.append(task)
    if prepare_only:
        args.append('-Pqualification.preparationOnly=true')
    return args


def wire_dependency_roots(owner: dict, target: str, approved: set[str], aliases: dict) -> dict:
    """Elide only proven action-free aggregates; all selected owners are approved."""
    if owner.get('task')!=target or not isinstance(owner.get('dependencies'),list)or not owner['dependencies']:
        raise smoke.SmokeError('wire dependency discovery lacks exact selected Test owner')
    indexed={}
    for item in owner['dependencies']:
        path=item.get('path')
        if (not isinstance(path,str)or not path.startswith(':')or path in indexed
                or item.get('kind')!='preparation'or type(item.get('actionable'))is not bool
                or not isinstance(item.get('dependencies'),list)):
            raise smoke.SmokeError('malformed/duplicate/native wire dependency owner')
        indexed[path]=item
    selected=set();aggregates={};visiting=set()
    def visit(path):
        canonical=aliases.get(path,path)
        if canonical in approved:selected.add(canonical);return
        if path in visiting:raise smoke.SmokeError('wire dependency aggregate cycle')
        item=indexed.get(path)
        if item is None or item['actionable']or not item['dependencies']:
            raise smoke.SmokeError('wire dependency owner outside approved census: '+path)
        visiting.add(path)
        for child in item['dependencies']:
            if not isinstance(child,str)or not child.startswith(':'):raise smoke.SmokeError('invalid wire aggregate edge')
            visit(child)
        visiting.remove(path);aggregates[path]=item['dependencies']
    for path in indexed:visit(path)
    if not selected:raise smoke.SmokeError('wire dependency discovery has no approved owners')
    return dict(requested=sorted(selected),elided_action_free_aggregates=aggregates)


def read_full_trace(output: Path, root: Path, started_ns: int, *, prepare_only: bool) -> dict:
    paths = sorted((output / 'task-traces').glob('graph-*.json'))
    if not paths:
        raise smoke.SmokeError('missing full task graph trace')
    tasks, versions, homes, runtime_classpaths, native_inputs = {}, set(), set(), [], []
    for path in paths:
        if path.stat().st_mtime_ns < started_ns:
            raise smoke.SmokeError('stale full task graph trace')
        data = json.loads(path.read_text())
        if data['version'] != 2 or data['failure'] or data['preparation'] is not prepare_only:
            raise smoke.SmokeError('failed or wrong-phase task graph trace')
        build = Path(data['root']).resolve()
        relative = build.relative_to(root.resolve())
        prefix = '' if relative == Path('.') else ':' + ':'.join(relative.parts)
        for item in data.get('runtime_classpaths',[]):
            if (not isinstance(item,dict)or not isinstance(item.get('task'),str)
                    or not item['task'].startswith(':')or not isinstance(item.get('files'),list)
                    or not item['files']or any(not isinstance(name,str)or not Path(name).is_absolute()for name in item['files'])):
                raise smoke.SmokeError('invalid actual JVM runtime classpath inventory')
            runtime_classpaths.append(dict(task=prefix+item['task'],files=sorted(set(item['files']))))
        for item in data.get('native_inputs',[]):
            if (not isinstance(item,dict)or not isinstance(item.get('task'),str)
                    or not item['task'].startswith(':')or not isinstance(item.get('files'),list)
                    or not item['files']or any(not isinstance(name,str)or not Path(name).is_absolute()for name in item['files'])):
                raise smoke.SmokeError('invalid actual native input inventory')
            native_inputs.append(dict(task=prefix+item['task'],files=sorted(set(item['files']))))
        versions.add(data['gradle']); homes.add(data['java_home'])
        for task in data['tasks']:
            task = dict(task, path=prefix + task['path'])
            if task['path'] in tasks or task['outcome'] is None or task['outcome']['failure']:
                raise smoke.SmokeError('duplicate or incomplete full task outcome')
            if prepare_only and task['kind'] != 'preparation':
                raise smoke.SmokeError('native/driver task reached during preparation')
            tasks[task['path']] = task
    if len(versions) != 1 or len(homes) != 1 or not all(Path(x).is_absolute() for x in homes):
        raise smoke.SmokeError('ambiguous actual Gradle/Java runtime')
    result=dict(version=2, tasks=list(tasks.values()), gradle=versions.pop(), java_home=homes.pop())
    if runtime_classpaths or native_inputs:
        if (len({item['task']for item in runtime_classpaths})!=len(runtime_classpaths)
                or len({item['task']for item in native_inputs})!=len(native_inputs)):
            raise smoke.SmokeError('duplicate JVM runtime classpath inventory')
        result.update(runtime_classpaths=runtime_classpaths,native_inputs=native_inputs,
            runtime_inputs=sorted({name for item in [*runtime_classpaths,*native_inputs] for name in item['files']}))
    return result


def full_context(profile: str, frozen: str, root: Path, go: Path, env: dict[str, str]) -> dict:
    # Device selection belongs to fresh execution, not compilation. Retain every
    # build/activation/URL value; only exact simulator mirrors are redundant.
    runtime_only = {'ANDROID_SERIAL', 'ANDROID_AVD_HOME'}
    runtime_only.update(key for key, value in env.items()
                        if key.startswith('SIMCTL_CHILD_') and env.get(key.removeprefix('SIMCTL_CHILD_')) == value)
    return dict(version=2, profile_sha256=profile, freeze_sha256=frozen,
                roots=dict(kotlin=str(root.resolve()), go=str(go.resolve())),
                configuration_roots=roots_for(dict(tasks=[]), root, env),
                environment=environment_binding({key:value for key,value in env.items() if key not in runtime_only}),
                runtime_device_policy='owned device selectors excluded; activation/URL values remain bound')


def jvm_preparation_runtime_tasks(entry: dict) -> tuple[str, ...]:
    """Seal runtime-only dependencies when the owning JVM test classes are prepared."""
    return tuple(sorted({task.rsplit(':',1)[0]+':jvmTest' for task in entry.get('task_ids',[])
        if task.endswith((':compileTestKotlinJvm',':compileJvmTestJava'))}))


def android_preparation_input_tasks(entry: dict) -> tuple[str, ...]:
    """Seal instrumentation-only inputs without running the device test owner."""
    return tuple(sorted({task.rsplit(':',1)[0]+':connectedAndroidDeviceTest'
        for task in entry.get('task_ids',[]) if task.endswith(':packageAndroidDeviceTest')}))


def _full_preparation_graph(trace: dict) -> dict:
    tasks = trace.get('tasks', [])
    if not tasks or not trace.get('gradle') or not Path(trace.get('java_home', '')).is_absolute():
        raise smoke.SmokeError('missing actual preparation graph/runtime')
    runtime=trace.get('runtime_classpaths',[]);native=trace.get('native_inputs',[])
    if not isinstance(runtime,list)or not isinstance(native,list):raise smoke.SmokeError('invalid preparation runtime inputs')
    runtime=[*runtime,*native]
    names=set();files=set()
    for item in runtime:
        if (not isinstance(item,dict)or not isinstance(item.get('task'),str)or not item['task'].startswith(':')
                or item['task']in names or not isinstance(item.get('files'),list)or not item['files']
                or any(not isinstance(name,str)or not Path(name).is_absolute()for name in item['files'])):
            raise smoke.SmokeError('incomplete or duplicate preparation runtime classpath')
        names.add(item['task']);files.update(item['files'])
    if trace.get('runtime_inputs',[])!=sorted(files):
        raise smoke.SmokeError('preparation runtime classpath closure mismatch')
    indexed = {}
    for task in tasks:
        name = task.get('path')
        state = task.get('outcome', {})
        if (not isinstance(name, str) or not name.startswith(':') or name in indexed
                or task.get('kind') != 'preparation' or state.get('failure') is not False
                or any(type(state.get(k)) is not bool for k in ('did_work', 'skipped', 'up_to_date'))
                or not isinstance(state.get('seconds'), (int, float))
                or not math.isfinite(state['seconds']) or state['seconds'] < 0):
            raise smoke.SmokeError('incomplete, duplicate or native preparation graph')
        for key in ('inputs', 'outputs'):
            if (not isinstance(task.get(key), list)
                    or any(not isinstance(x, str) or not Path(x).is_absolute() for x in task[key])):
                raise smoke.SmokeError('incomplete preparation graph file inventory')
        indexed[name] = task
    return indexed


def _full_preparation_roots(trace: dict, context: dict) -> list[str]:
    configuration = context.get('configuration_roots', [])
    if not configuration or any(not Path(x).is_absolute() for x in configuration):
        raise smoke.SmokeError('missing effective preparation configuration roots')
    return sorted(set(configuration) | set(trace.get('runtime_inputs',[])) | {
        name for task in trace['tasks'] for field in ('inputs', 'outputs') for name in task[field]})


def full_preparation_pending(trace: dict, requested: list[str], context: dict,
                             root: Path, env: dict[str, str], deadline: float, *, entry_id: str) -> dict:
    indexed = _full_preparation_graph(trace)
    if not entry_id or not requested or len(requested) != len(set(requested)) or not set(requested) <= indexed.keys():
        raise smoke.SmokeError('requested preparation tasks absent from actual nonbody graph')
    roots = _full_preparation_roots(trace, context)
    if roots != roots_for(trace, root, env):
        raise smoke.SmokeError('preparation configuration/context closure mismatch')
    files = file_map(roots, deadline)
    if not any(value.get('kind') == 'file' for value in files.values()):
        raise smoke.SmokeError('preparation has no bound dependency/output files')
    return dict(version=2, context=context, entry_id=entry_id, task_ids=sorted(indexed),
                requested=requested, trace=trace, roots=roots, files=files,
                gradle_runtime={key:trace[key] for key in ('gradle','java_home')})


def verify_full_preparation(paths: list[Path], context: dict, required: set[str],
                            deadline: float, *, snapshots: dict | None = None) -> list[str]:
    """Bind completed nonbody graphs and artifacts, never native test results."""
    covered = set()
    try:
        for path in paths:
            path = path.resolve()
            raw = path.read_bytes()
            saved = json.loads(raw)
            if saved.get('version') != 2 or saved.get('context') != context:
                raise smoke.SmokeError('full preparation context/source/environment mismatch')
            indexed = _full_preparation_graph(saved['trace'])
            if (saved.get('task_ids') != sorted(indexed) or not saved.get('requested')
                    or len(saved['requested']) != len(set(saved['requested']))
                    or not set(saved['requested']) <= indexed.keys()
                    or saved.get('roots') != _full_preparation_roots(saved['trace'], context)
                    or not saved.get('files')
                    or not any(x.get('kind') == 'file' for x in saved['files'].values())
                    or saved.get('gradle_runtime') != {k:saved['trace'][k] for k in ('gradle','java_home')}):
                raise smoke.SmokeError('incomplete preparation request/graph/artifact binding')
            producer = path.parent / 'receipt.json'
            if saved.get('producer_receipt') != str(producer):
                raise smoke.SmokeError('preparation receipt is not its adjacent producer')
            receipt_raw = producer.read_bytes()
            receipt = json.loads(receipt_raw)
            if (receipt.get('status') != 'PASS' or receipt['cleanup']['errors']
                    or receipt['cleanup'].get('groups_absent') is not True
                    or receipt.get('native_credit') != 0 or receipt.get('entry_id') != saved.get('entry_id')
                    or receipt.get('profile_sha256') != context['profile_sha256']
                    or receipt.get('freeze_sha256') != context['freeze_sha256']
                    or not receipt.get('before') or receipt.get('after') != receipt['before']
                    or receipt['preparation_record'].get('path') != str(path)
                    or receipt['preparation_record']['sha256'] != smoke.sha(raw)):
                raise smoke.SmokeError('full preparation producer identity/cleanup did not finish successfully')
            if file_map(saved['roots'], deadline) != saved['files']:
                raise smoke.SmokeError('full preparation dependency/output drift')
            if path.read_bytes() != raw or producer.read_bytes() != receipt_raw:
                raise smoke.SmokeError('preparation producer changed during validation')
            if snapshots is not None:
                snapshots[str(path)] = dict(manifest_sha256=smoke.sha(raw),
                    producer_receipt=str(producer), producer_sha256=smoke.sha(receipt_raw))
            covered.update(indexed)
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
        raise smoke.SmokeError('invalid full preparation binding') from error
    if not required <= covered:
        raise smoke.SmokeError('missing completed preparation tasks: ' + repr(sorted(required-covered)))
    return sorted(required)


def verify_full_consumer(paths: list[Path], context: dict, required: set[str], trace: dict,
                         snapshots: dict, deadline: float) -> None:
    """Reconcile the actual consuming graph and all proofs after cleanup."""
    current = {}
    verify_full_preparation(paths, context, required, deadline, snapshots=current)
    if current != snapshots:
        raise smoke.SmokeError('consumed preparation manifest/producer changed')
    observed(trace, True)
    known_tasks, known_files, runtimes = {}, {}, set()
    for path in paths:
        saved = json.loads(path.read_text())
        runtimes.add((saved['gradle_runtime']['gradle'], saved['gradle_runtime']['java_home']))
        for task in saved['trace']['tasks']:
            previous = known_tasks.get(task['path'])
            if previous and any(previous[k] != task[k] for k in ('kind','inputs','outputs')):
                raise smoke.SmokeError('inconsistent preparation graph closures')
            known_tasks[task['path']] = task
        for name, value in saved['files'].items():
            if name in known_files and known_files[name] != value:
                raise smoke.SmokeError('inconsistent preparation artifact closures')
            known_files[name] = value
    if runtimes != {(trace['gradle'],trace['java_home'])}:
        raise smoke.SmokeError('actual consuming Gradle/Java changed')
    for task in trace['tasks']:
        if task['kind'] != 'preparation':
            continue
        previous = known_tasks.get(task['path'])
        if (previous is None and task.get('actionable') is False and not task['outputs']
                and task['outcome'].get('did_work') is False):
            # Lifecycle aggregates (for example jvmTestClasses) can appear only
            # in the consumer after its compile/resource owners were excluded.
            # They perform no action; their complete inputs still pass the
            # bound-file closure comparison below.
            continue
        if not previous or any(previous[k] != task[k] for k in ('kind','inputs','outputs')):
            raise smoke.SmokeError('consuming preparation graph differs from producer: ' + task['path'])
    roots = _full_preparation_roots(trace, context)
    actual = file_map(roots, deadline)
    if any(known_files.get(name) != value for name,value in actual.items()):
        raise smoke.SmokeError('consuming dependency closure is not bound by its producers')

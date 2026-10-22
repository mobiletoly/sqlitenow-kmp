"""Exact native identities and assertion-body witnesses for focused platform runs."""
from __future__ import annotations

from collections import Counter
import gzip
import io
import json
import math
from pathlib import Path
import re
import xml.etree.ElementTree as ET

from tools.qualification.oversqlite_smoke import SmokeError
from tools.qualification.oversqlite_smoke import sha

WIRE_IDS=('small-single-row-insert','small-single-row-delete','two-hundred-one-table-push','mixed-table-push',
          'ordinary-multi-bundle-pull','large-snapshot-chunk','deterministic-incompressible-above-threshold')


def audit_worker_browser(support: dict, entry: dict, started_ns: int) -> dict:
    """A passing XML census cannot conceal a late Mocha failure or abort."""
    path = Path(support['folder']) / 'events.jsonl'
    if not path.is_file() or path.is_symlink() or path.stat().st_mtime_ns < started_ns:
        raise SmokeError('missing or stale raw worker Browser observations')
    prefix = 'E42_WORKER:' + support['nonce'] + ':'
    events, browsers = [], set()
    for line in path.read_text().splitlines():
        row = json.loads(line); browsers.add(row.get('browser'))
        log = row.get('log', '')
        if row.get('error') or not isinstance(log, str) or prefix not in log:
            raise SmokeError('unexpected raw worker Browser error/record')
        before, value = log.split(prefix, 1)
        event, end = json.JSONDecoder().raw_decode(value)
        if before not in ('', "'") or value[end:] not in ('', "'") or not isinstance(event, dict):
            raise SmokeError('malformed raw worker Browser event frame')
        events.append(event)
    if len(browsers) != 1 or None in browsers or not events:
        raise SmokeError('raw worker Browser identity is missing or mixed')
    allowed = {'test', 'result', 'armed', 'expected-error', 'terminal'}
    if any(event.get('phase') not in allowed for event in events):
        raise SmokeError('raw worker Browser failure, global error or unknown event')
    terminal = [event for event in events if event['phase'] == 'terminal']
    expected = Counter(entry['expected_source_owners'])
    if (len(terminal) != 1 or events[-1] != terminal[0]
            or type(terminal[0].get('failures')) is not int or terminal[0]['failures'] != 0
            or terminal[0].get('abort') is not False or type(terminal[0].get('total')) is not int
            or terminal[0]['total'] != sum(expected.values())):
        raise SmokeError('raw worker Mocha terminal failure/abort/census differs')
    active = None
    for event in events:
        if event['phase'] == 'test':
            if active is not None or event.get('owner') not in expected:
                raise SmokeError('raw worker Mocha overlapping or unknown test')
            active = event['owner']
        elif event['phase'] == 'result':
            if active is None or active != event.get('owner'):
                raise SmokeError('raw worker Mocha result has no active owner')
            active = None
    if active is not None:
        raise SmokeError('raw worker Mocha unfinished test')
    for phase in ('test', 'result'):
        rows = [event for event in events if event['phase'] == phase]
        if Counter(event.get('owner') for event in rows) != expected:
            raise SmokeError('raw worker Mocha owner census differs: ' + phase)
        if phase == 'result' and any(event.get('state') != 'passed' or event.get('pending') is not False for event in rows):
            raise SmokeError('raw worker Mocha case failed or pending')
    arms = [event for event in events if event['phase'] == 'armed']
    errors = [event for event in events if event['phase'] == 'expected-error']
    if (Counter(event.get('owner') for event in arms) != Counter(support['expected_crash_counts'])
            or any(type(event.get('id')) is not int or event['id'] != index for index, event in enumerate(arms, 1))
            or len({event.get('id') for event in arms}) != len(arms)
            or [{key: event.get(key) for key in ('id', 'owner', 'message')} for event in errors]
               != [{key: event.get(key) for key in ('id', 'owner', 'message')} for event in arms]
            or Counter((event.get('owner'), event.get('message')) for event in arms)
               != Counter({(owner, message): count for owner, messages in support['expected_error_counts'].items()
                           for message, count in messages.items()})):
        raise SmokeError('owned deliberate worker crash observations differ')
    if any(event.get('workerPath') != support['worker_path'] for event in arms):
        raise SmokeError('deliberate worker crash used a foreign module')
    if any(events.index(arm) >= events.index(error) for arm, error in zip(arms, errors)):
        raise SmokeError('worker expected error preceded its owned arm')
    return dict(cases=sum(expected.values()), expected_errors=len(errors), failures=0, abort=False,
                artifacts={str(path): sha(path.read_bytes())})


def audit_wire_snapshot_seed(seed: dict, manifest: dict, nonce: str) -> dict:
    expected = next(item for item in manifest['workloads'] if item['id'] == 'large-snapshot-chunk')
    if (not isinstance(seed, dict) or seed.get('nonce') != nonce
            or seed.get('user') != 'wire-' + nonce + '-5'
            or seed.get('rows') != 1000 or seed.get('batches') != 5
            or seed.get('route') != 'ScopeManager.ExecWrite' or seed.get('native_credit') != 0
            or not expected.get('rows_sha256') or seed.get('rows_sha256') != expected['rows_sha256']
            or len(seed.get('bundles', [])) != 5
            or [item.get('RowCount') for item in seed['bundles']] != [200] * 5
            or [item.get('BundleSeq') for item in seed['bundles']] != list(range(1, 6))
            or any(not item.get('BundleHash') for item in seed['bundles'])
            or not 0 <= seed.get('elapsed_seconds', float('inf')) < 30):
        raise SmokeError('wire snapshot preparation identity/count/hash/route differs')
    return seed


def audit_wire_snapshot(sessions: list[dict], chunk_routes: list[tuple[str,dict]]) -> dict:
    """Reconcile one session, its one-based durable rows and exact terminal totals."""
    if len(sessions)!=1:raise SmokeError('snapshot wire workload requires exactly one actual session')
    session=sessions[0]
    if any(route!='/sync/snapshot-sessions/'+session['snapshot_id']for route,_ in chunk_routes):
        raise SmokeError('wire snapshot chunk belongs to a different actual session')
    chunks=[body for _,body in chunk_routes];ordinal=0
    for index,chunk in enumerate(chunks):
        for group in chunk['rows']:
            # Session rows start at one; the cursor is the last row consumed.
            # Push compact ordinals use a separate zero base.
            if group[1]!=ordinal+1:raise SmokeError('wire snapshot compact row ordinal gap/overlap')
            ordinal+=len(group[2])
        if chunk['next_row_ordinal']!=ordinal or index<len(chunks)-1 and chunk['has_more']is not True:
            raise SmokeError('wire snapshot continuation/terminal mismatch')
    logical=sum(chunk['logical_byte_count']for chunk in chunks)
    if (session['row_count']!=1000 or not chunks or ordinal!=1000
            or logical!=session['logical_byte_count']or chunks[-1]['has_more']is not False):
        raise SmokeError('complete snapshot wire chunk totals differ')
    return dict(rows=ordinal,logical_bytes=logical,chunks=len(chunks))


def wire_variant_inputs(root: Path, observation: dict, deadline: float) -> list[dict]:
    """Retain every decoded entity without changing the actual raw captures."""
    folder=root/'decoded-wire';folder.mkdir(mode=0o700)
    inputs=[]
    from tools.qualification.platform_resources import remaining
    for row in observation['requests']:
        remaining(deadline,60)
        if not row.get('raw_entities'):continue
        for direction in('request','response'):
            path=Path(row['raw_entities'][direction])
            if (not path.is_absolute()or not path.is_file()or path.is_symlink()or path.resolve()!=path
                    or not path.is_relative_to(root.resolve())or path.stat().st_mode&0o077):
                raise SmokeError('wire raw entity is not a private owned regular path')
            raw=path.read_bytes()
            if len(raw)!=row['encoded_'+direction+'_body_bytes']or sha(raw)!=row['encoded_'+direction+'_sha256']:
                raise SmokeError('actual raw wire entity changed')
            encoding=row[direction+'_content_encoding']
            if encoding not in('','identity','gzip'):raise SmokeError('unsupported actual wire encoding')
            if encoding=='gzip':
                with gzip.GzipFile(fileobj=io.BytesIO(raw))as decoder:decoded=decoder.read((64<<20)+1)
            else:decoded=raw
            if len(decoded)>64<<20 or len(decoded)!=row['decoded_'+direction+'_body_bytes']or sha(decoded)!=row['decoded_'+direction+'_sha256']:
                raise SmokeError('actual decoded wire entity differs from capture')
            identity=f'{row["ordinal"]:06d}-{direction}';destination=folder/(identity+'.bin')
            destination.write_bytes(decoded);destination.chmod(0o600)
            inputs.append(dict(id=identity,path=str(destination),sha256=sha(decoded)))
    if not inputs:raise SmokeError('wire capture contains no actual protocol entities')
    return inputs


def audit_wire_capture(root: Path, observation: dict, manifest: dict, receipts: Path,
                      variants: Path, runtime: str, deadline: float) -> dict:
    """Join actual entities to native receiver receipts and all seven fixed workloads."""
    if runtime not in('raw','browser','wasm')or observation['errors']:
        raise SmokeError('wire runtime/capture errors prevent qualification')
    nonce=observation['nonce'];workloads=manifest['workloads']
    from tools.qualification.platform_resources import remaining
    if tuple(item['id']for item in workloads)!=WIRE_IDS:
        raise SmokeError('wire fixed workload inventory/order changed')
    requests={row['ordinal']:row for row in observation['requests']if row.get('raw_entities')}
    if len(requests)!=sum(bool(row.get('raw_entities'))for row in observation['requests']):
        raise SmokeError('duplicate actual wire capture ordinal')
    comparison=json.loads((variants/'manifest.json').read_bytes())
    indexed={item['id']:item for item in comparison}
    if len(indexed)!=len(comparison):raise SmokeError('duplicate wire comparison identity')
    comparison=indexed
    expected_variants={f'{ordinal:06d}-{direction}'for ordinal in requests for direction in('request','response')}
    if set(comparison)!=expected_variants:raise SmokeError('wire comparison inventory differs from actual captures')
    for ordinal,row in requests.items():
        remaining(deadline,60)
        for direction in('request','response'):
            candidate=comparison[f'{ordinal:06d}-{direction}']
            if candidate['source_sha256']!=row['decoded_'+direction+'_sha256']:
                raise SmokeError('wire comparison source differs from actual decoded bytes')
            if set(candidate['files'])!={'identity','gzip-comparison','production-selected'}:
                raise SmokeError('incomplete identity/gzip/production-selector wire artifacts')
            for value in candidate['files'].values():
                path=Path(value['path'])
                if (path.is_symlink()or path.resolve().parent!=variants.resolve()or path.stat().st_mode&0o077
                        or path.stat().st_size!=value['bytes']or sha(path.read_bytes())!=value['sha256']):
                    raise SmokeError('wire comparison artifact changed or escaped ownership')
            if direction=='response'and candidate['files']['production-selected']['sha256']!=row['encoded_response_sha256']:
                raise SmokeError('actual response differs from the production gzip selector')
            if row[direction+'_content_encoding']=='gzip':
                if row['decoded_'+direction+'_body_bytes']<2048 or row['encoded_'+direction+'_body_bytes']>=row['decoded_'+direction+'_body_bytes']:
                    raise SmokeError('actual gzip violates threshold/strictly-smaller selection')
    paths=sorted(receipts.glob('receipt-*.json'))
    if len(paths)!=7:raise SmokeError('wire requires exactly seven private native workload receipts')
    received={};native_ordinals=set()
    for path in paths:
        remaining(deadline,60)
        if path.is_symlink()or path.stat().st_mode&0o077:raise SmokeError('unsafe wire native receipt')
        receipt=json.loads(path.read_bytes());identity=receipt['workload'];evidence=receipt['evidence']
        if (identity not in WIRE_IDS or identity in received or receipt['nonce']!=nonce or receipt['runtime']!=runtime
                or evidence['nonce']!=nonce or evidence['runtime']!=runtime or evidence['workload']!=identity
                or evidence['full_state_verified']is not True):raise SmokeError('native wire receipt identity/semantic proof mismatch')
        expected=next(item for item in workloads if item['id']==identity)
        if evidence['expected_rows']!=expected['expected_rows']or not evidence['exchanges']:
            raise SmokeError('native wire workload state/count differs from frozen literals')
        ordinals=set()
        for exchange in evidence['exchanges']:
            ordinal=exchange['ordinal'];actual=requests.get(ordinal)
            if (ordinal in native_ordinals or ordinal in ordinals or actual is None or exchange['nonce']!=nonce
                    or exchange['workload']!=identity or actual['wire_workload']!=identity
                    or exchange['stage']!=actual['wire_stage']or exchange['route']!=actual['route']
                    or exchange['status']!=actual['status']
                    or exchange['response_decoded_sha256']!=actual['decoded_response_sha256']):
                raise SmokeError('actual SDK receiver bytes do not join the encoded capture')
            if runtime=='raw'and exchange['request_decoded_sha256']!=actual['decoded_request_sha256']:
                raise SmokeError('raw native request bytes differ from actual capture')
            ordinals.add(ordinal)
        native_ordinals.update(ordinals);received[identity]=dict(path=str(path),sha256=sha(path.read_bytes()),ordinals=sorted(ordinals))
    rows=[]
    for expected in workloads:
        remaining(deadline,60)
        identity=expected['id']
        measured=[row for row in requests.values()if row.get('wire_workload')==identity and row.get('wire_stage')=='measure']
        if not measured:raise SmokeError('wire workload has no actual measured exchanges: '+identity)
        if any(row['status']not in(200,204)or row.get('client_cancelled')for row in measured):
            raise SmokeError('wire workload contains a failed actual exchange')
        if any(row['decoded_response_body_bytes']>0 and row['ordinal']not in native_ordinals for row in measured):
            raise SmokeError('positive actual response lacks an application-decoded byte receipt')
        pushes=[row for row in measured if row['route']=='/sync/push'and row['method']=='POST']
        if identity in WIRE_IDS[:4]:
            if len(pushes)!=1:raise SmokeError('wire push workload did not use one complete bundle')
            row=pushes[0];body=json.loads((root/'decoded-wire'/f'{row["ordinal"]:06d}-request.bin').read_bytes())
            literal=expected['compact_requests'][0]
            if body['canonical_request_hash']!=literal['canonical_request_hash']or body['rows']!=literal['rows']:
                raise SmokeError('actual compact push/hash differs from frozen production-encoded literals')
            if identity in WIRE_IDS[:2]and(row['decoded_request_body_bytes']>=2048 or row['request_content_encoding']not in('','identity')):
                raise SmokeError('small complete wire push must remain below threshold/identity')
            if identity in WIRE_IDS[2:4]and(row['decoded_request_body_bytes']<2048 or row['request_content_encoding']!='gzip'):
                raise SmokeError('large complete wire push did not exercise actual gzip')
        if identity=='ordinary-multi-bundle-pull':
            if any(row['route'].startswith('/sync/snapshot-sessions')for row in measured):
                raise SmokeError('ordinary wire pull was replaced with snapshot traffic')
            bundles=[]
            for row in measured:
                if row['route']=='/sync/pull':bundles.extend(json.loads((root/'decoded-wire'/f'{row["ordinal"]:06d}-response.bin').read_bytes())['bundles'])
            if len(bundles)!=3 or sum(len(group[2])for bundle in bundles for group in bundle['rows'])!=300:
                raise SmokeError('ordinary wire pull lost one of three complete100-row bundles')
        if identity=='large-snapshot-chunk':
            sessions=[];chunk_routes=[]
            for row in measured:
                body=json.loads((root/'decoded-wire'/f'{row["ordinal"]:06d}-response.bin').read_bytes())if row['decoded_response_body_bytes']else{}
                if row['route']=='/sync/snapshot-sessions'and row['method']=='POST':sessions.append(body)
                if row['route'].startswith('/sync/snapshot-sessions/')and row['method']=='GET':chunk_routes.append((row['route'],body))
            audit_wire_snapshot(sessions,chunk_routes)
        if identity==WIRE_IDS[-1]:
            opaque=[row for row in measured if row['route']=='/qualification/wire/incompressible']
            if (len(opaque)!=1 or opaque[0]['encoded_response_body_bytes']!=4096
                    or opaque[0]['response_content_encoding']not in('','identity')
                    or opaque[0]['encoded_response_sha256']!=manifest['opaque_sha256']
                    or opaque[0]['decoded_response_sha256']!=manifest['opaque_sha256']):raise SmokeError('actual opaque selector/byte identity differs')
        rows.append(dict(workload=identity,measured_exchanges=len(measured),
            encoded_bytes=sum(row['encoded_request_body_bytes']+row['encoded_response_body_bytes']for row in measured),
            decoded_bytes=sum(row['decoded_request_body_bytes']+row['decoded_response_body_bytes']for row in measured)))
    return dict(scenarios=7,runtime=runtime,nonce=nonce,workloads=rows,receipts=received,
        actual_entities=len(expected_variants),native_decoded_exchanges=len(native_ordinals),
        policy='Actual encoded entities and application-decoded receipts; gzip-comparison is external evidence, production-selected bytes must equal every actual server response. Framing excluded; opaque bytes have no protocol-hash claim.')


def _load_user_evidence(user: dict, *, pid: int, users: int, started_ns: int,
                        ended_ns: int, phone_root: Path) -> dict:
    identity = re.fullmatch(r'parallel-([0-9]+)-([0-9]+)-user-([0-9]{3})',user['user_id'])
    if (identity is None or int(identity[1])!=pid or not started_ns<=int(identity[2])<=ended_ns
            or not 1<=int(identity[3])<=users):
        raise SmokeError('load user does not belong to this native process/run')
    path = Path(user['sqlite_database_path'])
    filename = re.fullmatch('mobile_flow_phone-'+re.escape(user['user_id'])+'_'+re.escape(user['user_id'])+r'_([0-9]+)\.db',path.name)
    if (not path.is_absolute() or path.is_symlink() or path.resolve().parent!=phone_root.resolve()
            or not path.is_file() or filename is None or not int(identity[2])<=int(filename[1])<=ended_ns
            or user['verification_passed'] is not True or user.get('errors')
            or user['sqlite_results']['pending_changes']!=0):
        raise SmokeError('load preserved SQLite identity/final verification failed')
    for table,count in {'users':23,'posts':447,'files':50,'file_reviews':50}.items():
        if (user['sqlite_results']['table_counts'].get(table)!=count
                or user['postgresql_results']['business_table_counts'].get('business.'+table)!=count):
            raise SmokeError('load complete scenario final table counts differ')
    files=[]
    for file in [path,Path(str(path)+'-wal'),Path(str(path)+'-shm')]:
        if file.is_symlink():raise SmokeError('load retained sidecar is a symlink')
        if file.exists():files.append(dict(path=str(file),sha256=sha(file.read_bytes()),bytes=file.stat().st_size))
    return dict(user_id=user['user_id'],ordinal=int(identity[3]),run_ns=int(identity[2]),path=str(path.resolve()),files=files)


def audit_load_phase(output: Path, *, users: int, nonce: str, started_ns: int,
                     ended_ns: int, phone_root: Path = Path('/tmp')) -> dict:
    """Audit a fresh complete phase; it cannot qualify a sequence independently."""
    if users not in (1,5,10,25) or re.fullmatch('[a-f0-9]{32}',nonce) is None:
        raise SmokeError('load audit needs the exact approved dimension and sequence nonce')
    log=output/'native.log'
    if log.is_symlink() or log.stat().st_mtime_ns<started_ns:raise SmokeError('stale load native log')
    native=log.read_text()
    starts=re.findall(r'QUALIFICATION_LOAD_START nonce=([a-f0-9]{32}) pid=([0-9]+) pgid=([0-9]+)\b',native)
    completions=re.findall(r'QUALIFICATION_LOAD_COMPLETE nonce=([a-f0-9]{32}) users=([0-9]+) concurrency=([0-9]+) verify=true scenario=complex-multi-batch mode=process elapsed_ns=([0-9]+)\b',native)
    if (len(starts)!=1 or starts[0][0]!=nonce or int(starts[0][1])<=1 or starts[0][1]!=starts[0][2]
            or len(completions)!=1 or completions[0][:3]!=(nonce,str(users),str(users)) or int(completions[0][3])<=0
            or len(re.findall(r'^=== RUN   TestQualificationLoadCompleteScenario\s*$',native,re.M))!=1
            or len(re.findall(r'^--- PASS: TestQualificationLoadCompleteScenario \(',native,re.M))!=1
            or re.search(r'^--- (?:FAIL|SKIP):',native,re.M)):
        raise SmokeError('load exact fresh native start/completion witness failed')
    reports=list((output/'test_reports').glob('*.json'))
    if len(reports)!=1 or reports[0].is_symlink() or reports[0].stat().st_mtime_ns<started_ns:
        raise SmokeError('missing/duplicate/stale load scenario report')
    report=json.loads(reports[0].read_text())
    if (report['total_users']!=users or report['successful_users']!=users or report['failed_users']!=0
            or len(report['user_reports'])!=users or report['scenario_name']!='complex-multi-batch'):
        raise SmokeError('load complete scenario report does not verify every user')
    evidence=[_load_user_evidence(user,pid=int(starts[0][1]),users=users,started_ns=started_ns,
        ended_ns=ended_ns,phone_root=phone_root)for user in report['user_reports']]
    if (len({x['user_id']for x in evidence})!=users or len({x['path']for x in evidence})!=users
            or {x['ordinal']for x in evidence}!=set(range(1,users+1)) or len({x['run_ns']for x in evidence})!=1):
        raise SmokeError('load users/SQLite paths/run identities are duplicated or incomplete')
    durability=re.findall(r'QUALIFICATION_LOAD_SQLITE_FULL path=([^\s]+) synchronous=([0-9]+) foreign_keys=([0-9]+) journal=([^\s]+)',native)
    if (not durability or any((sync,foreign,journal)!=('2','1','wal')for _,sync,foreign,journal in durability)
            or not {x['path']for x in evidence}<={str(Path(path).resolve())for path,_,_,_ in durability}):
        raise SmokeError('load actual managed-writer FULL/WAL/FK witness incomplete')
    return dict(sequence_complete=False,bodies=1,verified_users=users,pid=int(starts[0][1]),
        group=int(starts[0][2]),nonce=nonce,report=dict(path=str(reports[0]),sha256=sha(reports[0].read_bytes())),
        native_sha256=sha(log.read_bytes()),durability_openings=len(durability),retained=evidence)


def audit_load_observation(report: dict, *, phase: str, nonce: str) -> dict:
    acks=[x for x in report['acks']if x['phase']==phase]
    completed=[x for x in acks if x.get('status')==204]
    samples=[x for x in report['physical_samples']if x['phase']==phase]
    if (report['nonce']!=nonce or report['errors'] or not completed or not samples
            or any(x.get('status')!=204 and not(x.get('status')is None and x.get('client_cancelled')is True)for x in acks)
            or any(x['nonce']!=nonce or x.get('v1')!='v1' or not x.get('source')
                   or not isinstance(x.get('encoded_request_body_bytes'),int) or x['encoded_request_body_bytes']<=0
                   or not isinstance(x.get('decoded_request_body_bytes'),int) or x['decoded_request_body_bytes']<=0 for x in acks)
            or any(not math.isfinite(x['upstream_seconds'])or x['upstream_seconds']<0 for x in completed)
            or max(x['outcome_envelope_bytes']for x in samples)<=0):
        raise SmokeError('load phase actual ack byte/latency/peak outcome observations incomplete')
    return dict(ack_request_count=len(acks),successful_ack_responses=len(completed),
        cancelled_ack_requests=sum(x.get('client_cancelled')is True for x in acks),
        ack_encoded_body_bytes=sum(x['encoded_request_body_bytes']for x in acks),
        ack_decoded_body_bytes=sum(x['decoded_request_body_bytes']for x in acks),
        ack_upstream_seconds_min=min(x['upstream_seconds']for x in completed),
        ack_upstream_seconds_max=max(x['upstream_seconds']for x in completed),
        peak_outcome_envelope_bytes=max(x['outcome_envelope_bytes']for x in samples))

PACKAGE = "dev.goquick.sqlitenow.oversqlite."
VERIFIER_CLASS = PACKAGE + "platform.DatabaseStateVectorTest"
REPLAY_CLASS = PACKAGE + "realserver.OrdinarySafetyLiveTest"
VERIFIER_METHODS = (
    "typedBytesAndDigests", "completeVectorsOptionsAndFailureCleanup",
    "retainedStateAndOptionSensitivity", "workerEncodedRowBoundaries",
    "selectedVectorsMatchOracleAndRejectInvalidSelections", "selectedVectorsDetectEveryTypedMutation",
    "selectedVectorFailuresCloseAndRemainFresh", "selectedVectorsObservePhysicalReopen",
    "selectedReplayObservationsAvoidDiscardedScans",
)
REPLAY_METHODS = tuple(f"committedReplayWith{size}RowsWidth{width}"
                       for size in (1000, 10000) for width in (16, 1024))
TASKS = dict(jvm="jvmTest", android="connectedAndroidDeviceTest", macos="macosArm64Test",
             ios="iosSimulatorArm64Test", node="jsNodeTest", browser="jsBrowserTest",
             wasm="wasmJsBrowserTest")
KINDS = dict(jvm="jvm", android="android", macos="native", ios="native",
             node="js-node", browser="js-browser", wasm="wasm-browser")


def expected_nonbodies(runtime: str) -> dict[str, str]:
    if runtime in ("jvm", "android", "macos", "ios"):
        return {"workerEncodedRowBoundaries": "non_worker"}
    if runtime == "node":
        return {"selectedVectorsObservePhysicalReopen": "transient_storage"}
    return {}


def native_results(paths: list[Path], started_ns: int, family: str) -> tuple[list[dict], str]:
    cases, stdout = [], []
    if not paths:
        raise SmokeError("missing native reports")
    for path in paths:
        if path.stat().st_mtime_ns < started_ns:
            raise SmokeError(f"stale native report: {path}")
        tree = ET.parse(path).getroot()
        for case in tree.iter("testcase"):
            cls = case.get("classname", "").removeprefix(family + ".")
            name = case.get("name", "").split("[")[0]
            cases.append(dict(cls=cls, method=name, seconds=float(case.get("time", "0")),
                              failed=any(case.find(tag) is not None for tag in ("failure", "error", "skipped"))))
        stdout.extend(node.text or "" for node in tree.iter("system-out"))
    return cases, "\n".join(stdout)


def audit(cases: list[dict], stdout: str, mode: str, runtime: str, methods: list[str]) -> dict:
    cls = VERIFIER_CLASS if mode == "verifier" else REPLAY_CLASS
    expected = Counter((cls, name) for name in methods)
    actual = Counter((c["cls"], c["method"]) for c in cases)
    if actual != expected or any(c["failed"] for c in cases):
        raise SmokeError(f"native identity/failure mismatch: expected={dict(expected)}, actual={dict(actual)}")
    kind = KINDS[runtime]
    nonbodies = expected_nonbodies(runtime) if mode == "verifier" else {}
    rows = []
    for method in methods:
        identity = method if mode == "verifier" else cls + "." + method
        tags = ("F1_ENTER", "F1_PASS") if mode == "verifier" else ("A3_ENTER", "A3_COMPLETE", "A3_SCENARIO")
        counts = {tag: len(re.findall(re.escape(f"{tag} {kind} {identity}") + r"(?=\s|$|F1_|A3_|LIVE_REPLAY_)", stdout)) for tag in tags}
        nonbody = nonbodies.get(method)
        if nonbody:
            witness = f"F1_NOT_APPLICABLE {kind} {method} {nonbody}"
            if stdout.count(witness) != 1 or any(counts.values()):
                raise SmokeError(f"invalid explicit nonbody: {method}")
        elif any(value != 1 for value in counts.values()):
            raise SmokeError(f"missing/duplicate body witnesses: {method}: {counts}")
        if mode == "replay":
            size, width = re.fullmatch(r"committedReplayWith(\d+)RowsWidth(\d+)", method).groups()
            if len(re.findall(re.escape(f"A3_SCENARIO {kind} {identity} backlog={size}/width={width}") + r"(?=\s|$|F1_|A3_|LIVE_REPLAY_)", stdout)) != 1:
                raise SmokeError("missing exact replay workload witness")
            if "A3_NOT_EXECUTED" in stdout:
                raise SmokeError("guarded replay body")
        rows.append(dict(method=method, disposition="not-applicable" if nonbody else "fresh-body",
                         reason=nonbody, witnesses=counts))
    return dict(cases=cases, bodies=sum(not row["reason"] for row in rows), nonbodies=len(nonbodies), entries=rows,
                physical_reopen="unsupported-transient" if runtime == "node" else "required-body-assertions")


def validate_body_policies(entry: dict, policies: dict[str, dict]) -> None:
    required = []
    for owner in entry.get('expected_source_owners', []):
        policy = policies.get(owner, {})
        if policy.get('runtime') is not None and policy['runtime'] != entry.get('runtime'):
            raise SmokeError('body policy belongs to a different runtime: ' + owner)
        if (policy.get('kind') not in ('unconditional', 'witness', 'guarded-nonbody', 'capability-nonbody')
                or not isinstance(policy.get('reason'), str) or not policy['reason']):
            raise SmokeError('missing source-reviewed body/guard policy: ' + owner)
        markers = policy.get('required_markers', [])
        if (not isinstance(markers, list) or any(not isinstance(x, str) or not x or '\x00' in x for x in markers)
                or len(markers) != len(set(markers))
                or (policy['kind'] == 'witness' and not markers)):
            raise SmokeError('witness policy requires exact nonempty markers: ' + owner)
        if 'marker' in policy and (not isinstance(policy['marker'], str) or not policy['marker']):
            raise SmokeError('invalid explicit nonbody marker: ' + owner)
        required.extend(markers)
    if len(required) != len(set(required)):
        raise SmokeError('a body witness cannot grant credit to multiple owners')


def _serial_swift_tree(stdout: str, task: str) -> ET.Element:
    """Normalize observed XCTest events in memory; never invent a native file."""
    marker='> Task '+task+'\n'
    if stdout.count(marker)!=1:raise SmokeError('missing or duplicate selected Swift task frame')
    frame=stdout.split(marker,1)[1].split('\n> Task ',1)[0]
    pattern=re.compile(r"^Test Case '-\[([^\s\[\]]+) ([^\s\[\]]+)\]' (started|passed|failed|skipped)(?: \(([0-9]+(?:\.[0-9]+)?) seconds\))?\.$")
    events={};active=None;terminal=False
    for line in frame.splitlines():
        if re.search(r'Executed \d+ tests?, with \d+ failures?',line):
            if active is not None or not events:raise SmokeError('serial XCTest summary before completed cases')
            terminal=True
        if not line.startswith('Test Case '):continue
        if terminal:raise SmokeError('serial XCTest case after terminal summary')
        match=pattern.fullmatch(line)
        if not match:raise SmokeError('unrecognized serial XCTest event')
        cls,method,state,seconds=match.groups()
        if state=='started':
            if active is not None:raise SmokeError('overlapping serial XCTest cases')
            active=(cls,method)
        else:
            if active!=(cls,method):raise SmokeError('serial XCTest terminal without its active case')
            active=None
        events.setdefault((cls,method),[]).append((state,seconds))
    if not events or active is not None:raise SmokeError('missing or unfinished serial XCTest cases')
    if len(re.findall(r"^Test Suite 'Selected tests' passed at .+\.$",frame,re.M))!=1:
        raise SmokeError('missing enclosing serial XCTest completion')
    summaries=re.findall(r'Executed (\d+) tests?, with (\d+) failures? \((\d+) unexpected\)',frame)
    if not summaries or any(tuple(map(int,row))!=(len(events),0,0)for row in summaries):
        raise SmokeError('serial XCTest summary/case mismatch')
    tree=ET.Element('testsuite')
    for (cls,method),states in events.items():
        if [state for state,_ in states]!=['started','passed']or states[-1][1]is None:
            raise SmokeError('serial XCTest case missing, duplicate, failed or skipped: '+cls+'.'+method)
        ET.SubElement(tree,'testcase',classname=cls,name=method+'()',time=states[-1][1])
    return tree


def audit_full_entry(paths: list[Path], started_ns: int, entry: dict, policies: dict[str, dict], *, native_stdout: str = '') -> dict:
    """Keep framework outcomes separate from source-reviewed body/guard evidence."""
    serial=not paths and entry.get('id')=='swift-generated-live-only'and entry.get('runtime')=='Swift/macOS'
    if not paths and not serial:
        raise SmokeError('missing full-profile native reports')
    validate_body_policies(entry, policies)
    expected=Counter(entry['expected_source_owners']);actual=Counter();cases=[];stdout=[native_stdout]
    family=entry['task'].rsplit(':',1)[-1]
    trees=[]
    for path in paths:
        if path.stat().st_mtime_ns<started_ns:
            raise SmokeError('stale full-profile native report')
        trees.append(ET.parse(path).getroot())
    if serial:trees.append(_serial_swift_tree(native_stdout,entry['task']))
    for tree in trees:
        stdout.extend(x.text or ''for x in tree.iter('system-out'))
        for case in tree.iter('testcase'):
            cls=case.get('classname','').removeprefix(family+'.')
            raw=case.get('name','')
            # Match exactly one approved source method plus a known native target
            # decoration, rather than trim arbitrary method/token suffixes.
            allowed={'jvm':('[jvm]',),'android':('',),'macos':('[macosArm64]',),'ios':('[iosSimulatorArm64]',),'node':('[js, node]', '[jsNode]', '[js]'),'browser':('[js, browser]', '[jsBrowser]', '[js]'),'wasm':('[wasmJs, browser]', '[wasmJsBrowser]', '[wasmJs]'),'Swift/macOS':('()',)}.get(entry['runtime'],())
            display=raw
            browser_target={'browser':'js','wasm':'wasmJs'}.get(entry['runtime'])
            if browser_target:
                decorated=re.fullmatch(r'([^\[\]]+)\['+browser_target+r', browser, ([^,\[\]\s]+), ([^,\[\]\s]+)\]',raw)
                if decorated:
                    display=decorated.group(1)
            matches=[owner for owner in expected if policies[owner].get('native_class',owner.rsplit('.',1)[0])==cls and display in {owner.rsplit('.',1)[1]+suffix for suffix in ('',*allowed)}]
            if len(matches)!=1 or any(case.find(tag)is not None for tag in('failure','error')):
                raise SmokeError('unexpected or failing full-profile native identity: '+cls+'.'+raw)
            owner=matches[0];actual[owner]+=1
            policy=policies.get(owner,{})
            if policy.get('kind')not in('unconditional','witness','guarded-nonbody','capability-nonbody') or not policy.get('reason'):
                raise SmokeError('missing source-reviewed body/guard policy: '+owner)
            skipped=case.find('skipped')is not None
            nonbody=policy['kind']in('guarded-nonbody','capability-nonbody')
            if skipped and not nonbody:
                raise SmokeError('required native body skipped: '+owner)
            seconds=float(case.get('time','0'))
            if not math.isfinite(seconds)or seconds<0:raise SmokeError('invalid native duration')
            cases.append(dict(owner=owner,native_class=case.get('classname'),native_name=raw,
                              skipped=skipped,disposition='not-applicable'if nonbody else'fresh-body',
                              reason=policy['reason'],seconds=seconds))
    if actual!=expected:
        raise SmokeError('missing or duplicate full-profile native cases')
    text='\n'.join(stdout)
    for owner in expected:
        policy=policies[owner]
        for marker in policy.get('required_markers',[]):
            if text.count(marker)!=1:raise SmokeError('missing/duplicate body marker: '+owner)
        if policy.get('marker')and text.count(policy['marker'])!=1:
            raise SmokeError('missing/duplicate explicit nonbody marker: '+owner)
    # Existing typed-vector controls retain their strict ENTER/PASS/capability
    # witness oracle, even when selected alongside unrelated methods.
    selected=[name for name in VERIFIER_METHODS if VERIFIER_CLASS+'.'+name in expected]
    if selected:
        nonbodies=expected_nonbodies(entry['runtime'])
        for name in selected:
            if (name in nonbodies)!=(policies[VERIFIER_CLASS+'.'+name]['kind']=='capability-nonbody'):
                raise SmokeError('typed-vector capability disposition mismatch')
        audit([dict(cls=VERIFIER_CLASS,method=name,seconds=0,failed=False)for name in selected],text,'verifier',entry['runtime'],selected)
    return dict(framework_cases=len(cases),bodies=sum(x['disposition']=='fresh-body'for x in cases),
                nonbodies=sum(x['disposition']=='not-applicable'for x in cases),cases=cases)


def audit_full_go(log: Path, expected: list[dict]) -> dict:
    """Reconcile complete predeclared Go owners, retaining subcases and skips."""
    import json
    events=[]
    for line in log.read_text().splitlines():
        try:event=json.loads(line)
        except json.JSONDecodeError:continue  # Toolchain/cleanup diagnostics are retained separately.
        if isinstance(event,dict)and'Action'in event:events.append(event)
    owners={(x['package'],x['test']):x for x in expected}
    if not owners or len(owners)!=len(expected):raise SmokeError('empty/duplicate Go source owner inventory')
    runs=Counter((x['Package'],x['Test'])for x in events if x['Action']=='run'and'Test'in x and'/'not in x['Test'])
    terminals=[x for x in events if x['Action']in('pass','fail','skip')and'Test'in x]
    all_runs=Counter((x['Package'],x['Test'])for x in events if x['Action']=='run'and'Test'in x)
    all_terminals=Counter((x['Package'],x['Test'])for x in terminals)
    if all_runs!=all_terminals or any(count!=1 for count in all_runs.values()):
        raise SmokeError('Go native subtest start/terminal mismatch')
    top=[x for x in terminals if'/'not in x['Test']]
    if runs!=Counter(owners.keys())or Counter((x['Package'],x['Test'])for x in top)!=Counter(owners.keys()):
        raise SmokeError('Go native/source owner inventory mismatch')
    if any(x['Action']=='fail'for x in events):raise SmokeError('Go native test/package failure')
    subtests=[]
    for event in terminals:
        if '/'not in event['Test']:continue
        parent=owners.get((event['Package'],event['Test'].split('/',1)[0]))
        if parent is None:raise SmokeError('Go subtest has no selected owning test')
        reason=parent.get('subtest_nonbodies',{}).get(event['Test'])
        if event['Action']=='skip' and parent.get('disposition')!='not-applicable' and not reason:
            raise SmokeError('unclassified skipped Go subtest')
        subtests.append(dict(package=event['Package'],test=event['Test'],framework=event['Action'],
            reason=reason or parent.get('reason'),body_credit=0))
    rows=[]
    for x in top:
        owner=owners[(x['Package'],x['Test'])];nonbody=owner.get('disposition')=='not-applicable'
        if not owner.get('reason')or x['Action']=='skip'and not nonbody:
            raise SmokeError('unclassified/skipped required Go body')
        if x['Test'].startswith('Fuzz')and not nonbody and not any(
                child['Package']==x['Package']and child['Test'].startswith(x['Test']+'/')and child['Action']=='pass'
                for child in terminals):
            raise SmokeError('Go fuzz owner has no executed passing seed body')
        rows.append(dict(package=x['Package'],test=x['Test'],framework=x['Action'],disposition='not-applicable'if nonbody else'fresh-body',reason=owner['reason']))
    packages={x['Package']for x in events if x['Action']=='pass'and'Test'not in x}
    if not {x['package']for x in expected}<=packages:raise SmokeError('Go owning package completion missing')
    return dict(framework_cases=len(terminals),top_level=len(top),bodies=sum(x['disposition']=='fresh-body'for x in rows),nonbodies=sum(x['disposition']=='not-applicable'for x in rows),cases=rows,subtests=subtests,packages=sorted(packages))


def audit_web_deadlines(support: dict, entry: dict, started_ns: int) -> dict:
    folder=Path(support['folder']);command=folder/'web-command.json'
    if command.is_symlink()or command.stat().st_mtime_ns<started_ns:raise SmokeError('missing/stale actual web deadline configuration')
    value=json.loads(command.read_bytes());selectors=[entry['argv'][index+1]for index,arg in enumerate(entry['argv'][:-1])if arg=='--tests']
    milliseconds=480000 if support['policy']=='single-backlog'else 120000
    if (value['task']!=entry['task']or value['policy']!=support['policy']or value['selectors']!=sorted(selectors)
            or value.get('milliseconds')!=milliseconds):
        raise SmokeError('actual web deadline task/policy/selection differs')
    artifacts={str(command):sha(command.read_bytes())}
    if entry['task']!=':library-oversqlite:jsNodeTest':return dict(artifacts=artifacts,framework_ms=480000)
    if value.get('nodeTimeout')!=('480s'if support['policy']=='single-backlog'else'30s'):
        raise SmokeError('actual Node framework default differs from approved policy')
    path=folder/'deadlines.jsonl'
    if path.is_symlink()or path.stat().st_mtime_ns<started_ns:raise SmokeError('missing/stale actual Node method deadlines')
    rows=[json.loads(line)for line in path.read_text().splitlines()]
    prefix='dev.goquick.sqlitenow.oversqlite.'
    expected=Counter(owner.removeprefix(prefix)for owner in entry['expected_source_owners'])
    exception='SettlementTest.backlogBound'if support['policy']=='single-backlog'else'AutomaticRecoveryTest.uploadAndSnapshotShareDispatchBudget'
    milliseconds=480000 if support['policy']=='single-backlog'else 120000
    if len(rows)!=2*sum(expected.values()):raise SmokeError('Node deadline/completion count differs from exact native selection')
    for phase in('deadline','completion'):
        selected=[row for row in rows if row.get('phase')==phase]
        if Counter(row['name']for row in selected)!=expected:raise SmokeError('Node deadline identities incomplete or duplicated')
        for row in selected:
            limit=milliseconds if row['name']==exception else 30000
            if row['milliseconds']!=limit:raise SmokeError('Node method received an unauthorized framework deadline')
            if phase=='completion'and(row['state']!='passed'or not isinstance(row['elapsedMilliseconds'],(int,float))
                    or not math.isfinite(row['elapsedMilliseconds'])or not 0<=row['elapsedMilliseconds']<=limit):
                raise SmokeError('Node method failed or exceeded its exact framework deadline')
    artifacts[str(path)]=sha(path.read_bytes())
    return dict(artifacts=artifacts,cases=sum(expected.values()),exception=exception,milliseconds=milliseconds,other_ms=30000)


def audit_resource_diagnostics(output: Path, started_ns: int, observed: bool) -> dict:
    """Qualify exact unchanged public bodies; keep ordinary and observer metrics distinct."""
    native=(output/'native-command.log').read_text()
    expected={'1/16','1000/16','1000/1024','10000/16','10000/1024'}
    cases=re.findall(r'^    --- PASS: TestReplayDiagnostics_Public/([^ ]+) ',native,re.M)
    if (set(cases)!=expected or len(cases)!=5 or re.search(r'^\s*--- (?:SKIP|FAIL):',native,re.M)
            or len(re.findall(r'^--- PASS: TestReplayDiagnostics_Public ',native,re.M))!=1):
        raise SmokeError('resource diagnostics require exactly five fresh successful public replay bodies')
    rss=re.findall(r'^\s*([0-9]+)\s+maximum resident set size\s*$',native,re.M)
    if len(rss)!=1 or int(rss[0])<=0:raise SmokeError('resource process RSS observation is absent or ambiguous')
    files=list((output/'diagnostics').glob('*.json'));rows=[]
    for path in files:
        if path.is_symlink()or path.stat().st_mtime_ns<started_ns:raise SmokeError('stale resource diagnostic output')
        value=json.loads(path.read_bytes());measurement=value['memory_measurement'];observation=value['observation']
        if (value['measurement_version']!=2 or not value['state_fingerprint']or not value['frozen_hash']
                or any(type(value.get(key))is not int or value[key]<=0 for key in('public_ns','replay_tail_ns','case_ns','setup_ns','work'))):
            raise SmokeError('resource diagnostic timings/state/replay identity incomplete')
        if observed:
            if (not isinstance(measurement,dict)or measurement.get('mode')!='whole_process'
                    or measurement.get('native_observer_available')is not True
                    or not observation or sum(item['VMInstructions']for item in observation.values())<=0
                    or measurement.get('allocated_bytes',0)<=0 or value['memory'].get('native_memory_used',0)<=0):
                raise SmokeError('instrumented resource run lacks positive native/VM/allocation controls')
        elif measurement is not None or observation is not None or value['memory']is not None:
            raise SmokeError('ordinary resource run contains instrumentation')
        rows.append(dict(path=str(path),sha256=sha(path.read_bytes()),rows=value['rows'],width=value['width'],
            public_ns=value['public_ns'],replay_tail_ns=value['replay_tail_ns'],observed=observed))
    if len(rows)!=5 or {str(row['rows'])+'/'+str(row['width'])for row in rows}!=expected:
        raise SmokeError('resource diagnostic files do not cover the exact five workload dimensions')
    return dict(bodies=5,cases=cases,diagnostics=rows,observed=observed,process_peak_rss_bytes=int(rss[0]),
        timing_scope='Instrumented process time includes observer overhead'if observed else'Ordinary process and replay spans')


def audit_retained_observation(report: dict) -> dict:
    """Every actual sync request must retain its complete before/after samples."""
    requests=[row for row in report['requests']if row['route'].startswith('/sync/')]
    samples=[row for row in report['physical_samples']if row.get('ordinal')is not None]
    expected=Counter((row['ordinal'],row['route'],row['phase'],when)for row in requests for when in('before','after'))
    actual=Counter((row.get('ordinal'),row['route'],row['phase'],row['when'])for row in samples)
    if not requests or report['errors']or actual!=expected:
        raise SmokeError('retained per-request physical sample coverage is incomplete or duplicated')
    fields=('database_bytes','outcome_envelope_bytes','max_outcome_envelope_bytes','outcome_row_storage_bytes',
        'outcomes','snapshot_sessions','snapshot_rows','snapshot_wire_bytes','duplicate_source_owners')
    for row in report['physical_samples']:
        if (row.get('nonce')!=report['nonce']or any(type(row.get(key))is not int or row[key]<0 for key in fields)
                or row['duplicate_source_owners']!=0 or not row.get('relations')):
            raise SmokeError('retained physical sample fields/ownership are incomplete')
        names=[(item['schema'],item['name'])for item in row['relations']]
        if (len(names)!=len(set(names))or not {('sync','push_outcomes'),('sync','snapshot_session_rows')}<=set(names)
                or any(type(item.get(key))is not int or item[key]<0 for item in row['relations']for key in('heap_bytes','total_bytes'))
                or any(item['total_bytes']<item['heap_bytes']for item in row['relations'])):
            raise SmokeError('retained per-relation physical census is incomplete')
    if max(row['snapshot_wire_bytes']for row in samples)<=0 or max(row['max_outcome_envelope_bytes']for row in samples)<=0:
        raise SmokeError('retained transient snapshot/outcome storage was never observed')
    return dict(sync_requests=len(requests),request_samples=len(samples),
        peak_snapshot_wire_bytes=max(row['snapshot_wire_bytes']for row in samples),
        peak_individual_outcome_bytes=max(row['max_outcome_envelope_bytes']for row in samples))


def retained_sqlite_vector(db, deadline: float) -> dict:
    """Complete typed cells and schema, including every present sync table."""
    import struct,time
    def cell(value):
        if value is None:return dict(type='null')
        if type(value)is int:return dict(type='integer',value=str(value))
        if type(value)is float:return dict(type='real',ieee754=struct.pack('>d',value).hex())
        if isinstance(value,bytes):return dict(type='blob',hex=value.hex())
        if isinstance(value,str):return dict(type='text',value=value)
        raise SmokeError('unsupported SQLite storage value in complete vector')
    schema=[list(row)for row in db.execute("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY type,name")]
    tables={}
    for (name,)in db.execute("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*' ORDER BY name"):
        if time.monotonic()>deadline:raise SmokeError('complete SQLite vector deadline')
        quoted='"'+name.replace('"','""')+'"'
        columns=[list(row)for row in db.execute('PRAGMA table_xinfo('+quoted+')')]
        selection=','.join('"'+column[1].replace('"','""')+'"'for column in columns)
        rows=[[cell(value)for value in row]for row in db.execute('SELECT '+selection+' FROM '+quoted)]
        rows.sort(key=lambda row:json.dumps(row,sort_keys=True,separators=(',',':')))
        tables[name]=dict(columns=columns,rows=rows)
    return dict(scope='all application and sync tables; complete typed cells, schema and duplicate rows',schema=schema,tables=tables)


def inspect_retained_sqlite(root: Path, family: str, postgres: dict, observation: dict, deadline: float) -> dict:
    """Port the reviewed closed-copy invariants; never execute a historical controller."""
    import hashlib,shutil,sqlite3,tempfile,time
    if family not in ('go','kotlin'):raise SmokeError('unknown retained fixture family')
    metadata=set('row_state relationships admitted_contracts apply_state attachment_state conflict_state contract_identity managed_tables opening_state operation_state outbox_bundle source_state action_required push_rejection_state upgrade_identity upgrade_journal'.split())
    pending=set('dirty_rows outbox_rows upload_journal'.split());temporary=set('apply_edges apply_plan apply_rows push_edges push_nodes push_plan push_stage snapshot_stage'.split())
    classification={**{'_sync_'+x:'persistent metadata' for x in metadata},**{'_sync_'+x:'pending/recovery state' for x in pending},**{'_sync_'+x:'temporary work state' for x in temporary}}
    reports=[];errors=[]
    def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()
    def scalar(db,sql):return db.execute(sql).fetchone()[0]
    def rows(db,sql):return [{k:(v.hex() if isinstance(v,bytes) else v) for k,v in dict(x).items()} for x in db.execute(sql)]
    expected_labels={'initial','cancelled','ack-pending','recovered'}|{'cycle-%02d'%i for i in range(1,11)}
    paths=[p for p in sorted((root/'files').glob('*.sqlite')) if p.stem not in ('0','1')]
    if {p.stem for p in paths}!={label+'-'+str(owner) for label in expected_labels for owner in range(2)}:errors.append('exact28closed-copy set mismatch')
    for original in paths:
     if time.monotonic()>deadline:raise SmokeError("retained closed-copy inspection deadline")
     label,owner_text=original.stem.rsplit('-',1);owner=int(owner_text);retained=label in ('cancelled','ack-pending');local=[]
     def check(condition,message):
      if not condition:local.append(message)
     sources=[Path(str(original)+suffix) for suffix in ('','-wal','-shm') if Path(str(original)+suffix).exists()];before={str(p):digest(p) for p in sources}
     with tempfile.TemporaryDirectory(prefix='readonly-clone-',dir=root) as directory:
      clone=Path(directory)/original.name
      for p in sources:shutil.copyfile(p,Path(str(clone)+str(p)[len(str(original)):]))
      db=sqlite3.connect(clone.resolve().as_uri()+'?mode=ro',uri=True);db.row_factory=sqlite3.Row
      db.set_progress_handler(lambda: int(time.monotonic()>deadline),1000)
      try:
       names=[x[0] for x in db.execute("SELECT name FROM sqlite_schema WHERE type='table' AND name GLOB '_sync_*' ORDER BY name")]
       counts={n:scalar(db,'SELECT count(*) FROM "'+n+'"') for n in names}
       check(set(names)<=classification.keys(),'unclassified sync table: '+str(set(names)-classification.keys()))
       one={'_sync_managed_tables','_sync_admitted_contracts','_sync_source_state','_sync_contract_identity','_sync_attachment_state','_sync_apply_state','_sync_operation_state','_sync_outbox_bundle'}
       check(one<=set(names),'required persistent metadata owner missing')
       for name in names:
        if classification.get(name)=='persistent metadata' and name not in ('_sync_row_state','_sync_relationships'):
         check(counts[name]==1 if name in one else counts[name]<=1,'persistent metadata owner count '+name)
       managed=rows(db,'SELECT schema_name,table_name FROM _sync_managed_tables')
       check(managed==[{'schema_name':'business','table_name':'categories'}],'fixed managed-table owner set')
       identity=rows(db,'SELECT * FROM _sync_contract_identity')[0];admitted=rows(db,'SELECT * FROM _sync_admitted_contracts')
       check(identity['database_id']==('cleanup-audit' if family=='go' else 'fixture') and identity['service_id']=='business' and identity['generation_id']==1 and identity['local_version']==(1 if family=='go' else 0),'fixed local contract identity')
       check(len(admitted)==1 and admitted[0]['generation_id']==1,'fixed admitted generation owner set')
       check([x[0] for x in db.execute('PRAGMA integrity_check')]==['ok'],'integrity')
       check(not list(db.execute('PRAGMA foreign_key_check')),'foreign key violation')
       op=rows(db,'SELECT * FROM _sync_operation_state')[0];attachment=rows(db,'SELECT * FROM _sync_attachment_state')[0];outbox=rows(db,'SELECT * FROM _sync_outbox_bundle')[0]
       check(op['reserved_entries']==op['reserved_bytes']==0 and op['workspace_context']=='','reservation/context leak')
       check(scalar(db,'SELECT apply_mode FROM _sync_apply_state')==0,'apply mode leak')
       check(counts['_sync_row_state']==125,'row metadata accumulated or missing');check(scalar(db,'SELECT count(*) FROM categories')==125,'business row count')
       orphan=scalar(db,"SELECT count(*) FROM _sync_relationships r LEFT JOIN _sync_row_state s ON s.schema_name=r.child_schema AND s.table_name=r.child_table AND s.key_json=r.child_key WHERE s.key_json IS NULL")
       parent_orphan=scalar(db,"SELECT count(*) FROM _sync_relationships r LEFT JOIN categories p ON p.id=json_extract(r.parent_key,'$.id') WHERE p.id IS NULL")
       check(orphan==parent_orphan==0,'relationship orphan');check(counts['_sync_relationships']<=248,'relationship accumulation')
       if 'push_stage_entries' in op:check(op['push_stage_entries']==counts.get('_sync_push_stage',0),'push stage counter mismatch')
       check(op['snapshot_stage_entries']==counts['_sync_snapshot_stage'],'snapshot stage counter mismatch')
       singletons={n:rows(db,'SELECT * FROM '+n) for n in ('_sync_action_required','_sync_push_rejection_state','_sync_upgrade_journal') if n in names}
       if '_sync_action_required' in names:check(singletons['_sync_action_required'][0]['action']=='','action required')
       if '_sync_push_rejection_state' in names:check(singletons['_sync_push_rejection_state'][0]['pending']=='','pending rejection')
       if '_sync_upgrade_journal' in names:check(singletons['_sync_upgrade_journal'][0]['phase']=='ready','opening not ready')
       journal=rows(db,'SELECT * FROM _sync_upload_journal');sources_state=rows(db,'SELECT * FROM _sync_source_state')
       check(len(sources_state)==1 and sources_state[0]['source_id']==attachment['current_source_id'] and sources_state[0]['replaced_by_source_id']=='','one original active local source')
       if not retained or owner==1:
        for table,count in counts.items():
         if classification.get(table) in ('temporary work state','pending/recovery state'):check(count==0,table+' retained at completed owner')
        check(outbox['state']=='none','outbox not idle');check(op['kind']=='none','lifecycle not idle')
        check(op['snapshot_stage_bytes']==0,'snapshot stage bytes leak')
        if 'push_stage_bytes' in op:check(op['push_stage_bytes']==0,'push stage bytes leak')
        check(counts['_sync_relationships']==124,'established relationship count')
       else:
        check(len(journal)==1,'one exact upload certificate required')
        if len(journal)==1:
         j=journal[0];raw=bytes.fromhex(j['request_body']);operation=json.loads(j['binding'])['operation'];request=json.loads(raw)
         check(hashlib.sha256(raw).hexdigest()==j['request_digest'],'original F raw-byte digest mismatch')
         for key in ('source_bundle_id','canonical_request_hash','initialization_id'):check(request.get(key)==operation.get(key),'F binding mismatch '+key)
         current=next(x for x in sources_state if x['source_id']==operation['source_id'])
         check(current['next_source_bundle_id']==operation['source_bundle_id']+(j['phase']=='ack_pending'),'consumed source sequence mismatch')
         if j['outcome_body']:
          outcome=json.loads(bytes.fromhex(j['outcome_body']))
          check(outcome['outcome_hash']==j['outcome_hash'],'C hash owner mismatch')
          for key in ('source_id','schema_generation_id','schema_generation_fingerprint','source_bundle_id','canonical_request_hash','initialization_id'):check(outcome.get(key)==operation.get(key),'C binding mismatch '+key)
         if label=='cancelled':check(counts['_sync_outbox_rows']==40 and outbox['canonical_request_hash']!='' and j['phase']=='frozen','cancelled F identity lost')
         if label=='ack-pending':check(j['phase']=='ack_pending' and bool(j['outcome_body']) and counts['_sync_outbox_rows']==0,'ack_pending completion/certificate mismatch')
       business=rows(db,'SELECT id,name,parent_id FROM categories ORDER BY id')
       check(all(x['parent_id']==('00000000-0000-4000-8000-000000000000' if int(x['id'][-12:],16)>0 else None) for x in business),'wrong category dependency')
       if label.startswith('cycle-'):
        cycle=int(label[6:])
        for row in business:
         index=int(row['id'][-12:],16);value=f'cycle-{cycle:02d}-update-{index:03d}' if 1<=index<=10 else f'cycle-{cycle:02d}-reinsert-{index:03d}' if index>=115 else f'initial-{index:03d}'
         check(row['name']==value,'business payload mismatch '+row['id'])
       if label=='recovered':check(sum(x['name']=='recovered' for x in business)==124,'final recovery payload')
       vector=retained_sqlite_vector(db,deadline)
       reports.append({'typed_vector':vector,'typed_vector_sha256':sha(json.dumps(vector,sort_keys=True,separators=(',',':')).encode()),'file':str(original),'label':label,'owner':owner,'counts':counts,'classification':{n:classification.get(n,'UNCLASSIFIED') for n in names},'operation':op,'attachment':attachment,'outbox':outbox,'journal':journal,'source':sources_state,'singletons':singletons,'relationship_orphans':orphan,'parent_orphans':parent_orphan,'business':business,'business_digest':hashlib.sha256(json.dumps(business,sort_keys=True).encode()).hexdigest(),'pages':{p:scalar(db,'PRAGMA '+p) for p in ('page_count','freelist_count','page_size')},'original_files':before,'file_bytes':{str(p):p.stat().st_size for p in sources},'errors':local})
      finally:db.close()
     after_sources=[Path(str(original)+suffix)for suffix in('','-wal','-shm')if Path(str(original)+suffix).exists()]
     check(before=={str(p):digest(p) for p in after_sources},'retained original/sidecar mutated during inspection')
     errors.extend(original.name+': '+message for message in local)
    for owner in range(2):
     initial=next((r for r in reports if r['label']=='initial' and r['owner']==owner),None)
     if initial:
      for report in [r for r in reports if r['owner']==owner]:
       stable=[name for name,kind in classification.items() if kind=='persistent metadata' and name not in ('_sync_row_state','_sync_relationships')]
       if {n:initial['counts'].get(n) for n in stable}!={n:report['counts'].get(n) for n in stable}:errors.append(report['file']+': persistent metadata accumulated or owner set changed')
       if report['attachment']['current_source_id']!=initial['attachment']['current_source_id']:errors.append(report['file']+': original source changed')
    final=[r for r in reports if r['label']=='recovered']
    server_sources=postgres['sources'] or [];progress=postgres['progress'] or [];users=postgres['user_state'] or []
    local_ids={r['attachment']['current_source_id'] for r in final}
    if len(final)!=2 or len(local_ids)!=2 or len(server_sources)!=2 or {r['source_id'] for r in server_sources}!=local_ids:errors.append('final server/local source owner set mismatch')
    if len(users)!=1:errors.append('final server user owner count')
    if len(progress)>2 or len({r['source_id'] for r in progress})!=len(progress) or not {r['source_id'] for r in progress}<=local_ids:errors.append('server progress metadata accumulated or mismatched')
    reconciliation=[]
    for report in final:
     source_id=report['attachment']['current_source_id'];local=report['source'][0];server=next((r for r in server_sources if r['source_id']==source_id),None);ack=next((r for r in progress if r['source_id']==source_id),None);sequence=local['next_source_bundle_id']-1
     actual_acks=[r for r in observation['acks'] if r['source']==source_id];last=max(actual_acks,key=lambda x:x['operation']['source_bundle_id']) if actual_acks else None
     valid=server is not None and server['state']=='active' and server['max_committed_source_bundle_id']==sequence and (ack['acknowledged_sequence'] if ack else 0)==sequence
     if sequence:
      valid=valid and last is not None and ack is not None and last['operation']=={'source_bundle_id':sequence,'canonical_request_hash':ack['acknowledged_request_hash'],'outcome_hash':ack['acknowledged_outcome_hash']} and ack['acknowledged_generation_id']==1
     if len(users)==1:valid=valid and report['attachment']['last_bundle_seq_seen']==users[0]['next_bundle_seq']-1 and server is not None and server['user_pk']==users[0]['user_pk']
     if not valid:errors.append(report['file']+': server terminal/local next/ack hash/checkpoint reconciliation failed')
     reconciliation.append({'source':source_id,'local_next':local['next_source_bundle_id'],'server':server,'ack':ack,'last_observed_ack':last,'valid':bool(valid)})
    for label in expected_labels-{'cancelled','ack-pending'}:
     pair=[r for r in reports if r['label']==label]
     if len(pair)!=2 or pair[0]['business_digest']!=pair[1]['business_digest']:errors.append('writer/follower mismatch '+label)
    summary={'reports':reports,'errors':errors,'classification':classification,'source_ack_reconciliation':reconciliation,'originals_unchanged':not any('mutated during' in x for x in errors),'query_mode':'Only private cloned closed databases opened URI mode=ro; original file/sidecar hashes verified unchanged; no manual cleanup or VACUUM.'}
    return summary

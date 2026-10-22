"""Run-owned processes, durable PostgreSQL and disposable platform devices."""
from __future__ import annotations

import json
import fcntl
import gzip
import http.client
import http.server
import io
import errno
from collections import Counter
from contextlib import contextmanager
import os
import re
from pathlib import Path
import secrets
import select
import shutil
import signal
import socket
import stat
import subprocess
import threading
import time
from urllib.request import urlopen
from urllib.parse import urlsplit
import uuid

from tools.qualification import oversqlite_smoke as smoke

LABEL = "com.mobiletoly.go-oversync.test-run"
UNMASK = ["python3", "-B", "-c", "import os,signal,sys;signal.pthread_sigmask(signal.SIG_SETMASK,[]);os.execvp(sys.argv[1],sys.argv[1:])"]
HTTP_BODY_LIMIT=(64<<20)+8192


class ClientDisconnected(smoke.SmokeError):
    """A caller closed an incomplete request; no successful exchange is claimed."""


class HTTPObserver:
    """Run-owned byte-preserving HTTP observation, outside SDK memory accounting."""
    def __init__(self, resources, upstream: str, database: str, output: Path, *, nonce: str,
                 capture_bodies: bool = False, observe_acks: bool = True, phase_file: Path | None = None):
        parsed=urlsplit(upstream)
        if (parsed.scheme!='http' or parsed.hostname!='127.0.0.1' or not parsed.port
                or parsed.username or parsed.password or parsed.path not in ('','/') or parsed.query or parsed.fragment):
            raise smoke.SmokeError('observer upstream must be the owned loopback authority')
        self.resources=resources;self.upstream=parsed;self.database=database;self.output=output;self.nonce=nonce
        self.capture_bodies=capture_bodies;self.observe_acks=observe_acks;self.phase='initial'
        self.phase_file=phase_file
        self.lock=threading.Lock();self.handlers=set();self.sockets=set();self.requests=[];self.samples=[];self.errors=[]
        self.ordinal=0;self.thread=None;self.stopped=False
        self.stopping=threading.Event();self.disconnect_observed=threading.Event()
        observer=self
        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version='HTTP/1.1'
            def log_message(self,*args):pass
            def setup(self):
                super().setup();self.connection.settimeout(remaining(resources.command_deadline or time.monotonic()+120,120))
            def do_GET(self):observer.forward(self)
            def do_POST(self):observer.forward(self)
            def do_DELETE(self):observer.forward(self)
            def do_PUT(self):observer.forward(self)
            def do_OPTIONS(self):observer.forward(self)
        class Server(http.server.ThreadingHTTPServer):
            daemon_threads=True
            block_on_close=False
            def process_request(self,request,address):
                def handle():
                    try:self.process_request_thread(request,address)
                    finally:
                        with observer.lock:
                            observer.handlers.discard(threading.current_thread());observer.sockets.discard(request)
                thread=threading.Thread(target=handle,daemon=True)
                with observer.lock:
                    observer.handlers.add(thread);observer.sockets.add(request)
                    try:thread.start()
                    except BaseException:
                        observer.handlers.discard(thread);observer.sockets.discard(request);request.close();raise
        self.server=Server(('127.0.0.1',0),Handler)
        self.url='http://127.0.0.1:'+str(self.server.server_port)

    def start(self):
        self.thread=threading.Thread(target=lambda:self.server.serve_forever(poll_interval=.05),daemon=True)
        self.thread.start()

    def set_phase(self,name: str):
        with self.lock:
            if self.handlers:raise smoke.SmokeError('observer phase changed while requests remain active')
            self.phase=name

    def request_phase(self,route: str) -> str:
        if self.phase_file is None:return self.phase
        if not route.startswith('/sync/')and not self.phase_file.exists():return'initial'
        if self.phase_file.is_symlink():raise smoke.SmokeError('audit phase witness is a symlink')
        info=self.phase_file.stat()
        if not stat.S_ISREG(info.st_mode)or info.st_mode & 0o077 or info.st_size>1024:
            raise smoke.SmokeError('audit phase witness has unsafe type/mode/size')
        value=json.loads(self.phase_file.read_bytes())
        if set(value)!={'phase'}or value['phase']not in('initial','steady','explicit-rebuild'):
            raise smoke.SmokeError('audit phase witness is incomplete or unknown')
        return value['phase']

    def read_body(self,handler) -> bytes:
        transfer=handler.headers.get_all('Transfer-Encoding',[]);lengths=handler.headers.get_all('Content-Length',[])
        if len(transfer)>1 or len(lengths)>1 or transfer and lengths:
            raise smoke.SmokeError('ambiguous observer HTTP entity framing')
        if transfer:
            if transfer!=['chunked']:raise smoke.SmokeError('unsupported observer transfer encoding')
            chunks=[];total=0
            while True:
                size_line=handler.rfile.readline(128)
                if not size_line or len(size_line)<128 and not size_line.endswith(b'\r\n'):
                    raise ClientDisconnected('client closed a partial chunk size')
                if not size_line.endswith(b'\r\n'):raise smoke.SmokeError('oversized observer chunk size')
                count=int(size_line.split(b';',1)[0],16)
                if count<0:raise smoke.SmokeError('negative observer chunk')
                total+=count
                if total>HTTP_BODY_LIMIT:raise smoke.SmokeError('observer request entity exceeds protocol ceiling')
                if count==0:
                    for _ in range(100):
                        line=handler.rfile.readline(8192)
                        if line==b'\r\n':return b''.join(chunks)
                        if not line:raise ClientDisconnected('client closed partial chunk trailers')
                        if not line.endswith(b'\r\n'):break
                    raise smoke.SmokeError('partial/oversized observer chunk trailers')
                raw=handler.rfile.read(count)
                trailer=handler.rfile.read(2)
                if len(raw)!=count or len(trailer)<2:raise ClientDisconnected('client closed a partial observer chunk')
                if trailer!=b'\r\n':raise smoke.SmokeError('malformed observer chunk trailer')
                chunks.append(raw)
        size=int(lengths[0])if lengths else 0
        if not 0<=size<=HTTP_BODY_LIMIT:raise smoke.SmokeError('observer request entity exceeds protocol ceiling')
        raw=handler.rfile.read(size)
        if len(raw)!=size:raise ClientDisconnected('client closed a partial request entity')
        return raw

    def decoded(self,raw: bytes,encoding: str) -> bytes:
        if encoding in ('','identity'):return raw
        if encoding!='gzip':raise smoke.SmokeError('unsupported observer content encoding')
        with gzip.GzipFile(fileobj=io.BytesIO(raw))as stream:decoded=stream.read(HTTP_BODY_LIMIT+1)
        if len(decoded)>HTTP_BODY_LIMIT:raise smoke.SmokeError('observer decoded entity exceeds protocol ceiling')
        return decoded

    def physical(self,route: str,when: str,phase: str | None = None,ordinal: int | None = None) -> None:
        value=self.resources.sql_json("""BEGIN READ ONLY; SELECT json_build_object(
          'database_bytes',pg_database_size(current_database()),
          'outcome_envelope_bytes',(SELECT coalesce(sum(octet_length(envelope)),0) FROM sync.push_outcomes),
          'max_outcome_envelope_bytes',(SELECT coalesce(max(octet_length(envelope)),0) FROM sync.push_outcomes),
          'outcome_row_storage_bytes',(SELECT coalesce(sum(pg_column_size(p)),0) FROM sync.push_outcomes p),
          'duplicate_source_owners',(SELECT count(*) FROM (SELECT user_pk,source_id FROM sync.push_outcomes GROUP BY user_pk,source_id HAVING count(*)>1) d),
          'outcomes',(SELECT count(*) FROM sync.push_outcomes),
          'snapshot_sessions',(SELECT count(*) FROM sync.snapshot_sessions),
          'snapshot_rows',(SELECT count(*) FROM sync.snapshot_session_rows),
          'snapshot_wire_bytes',(SELECT coalesce(sum(wire_byte_count),0) FROM sync.snapshot_session_rows),
          'relations',(SELECT json_agg(json_build_object('schema',n.nspname,'name',c.relname,
             'heap_bytes',pg_relation_size(c.oid),'total_bytes',pg_total_relation_size(c.oid)) ORDER BY n.nspname,c.relname)
             FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
             WHERE c.relkind='r' AND n.nspname IN ('business','sync'))); COMMIT;""",self.database)
        if value['duplicate_source_owners']!=0:raise smoke.SmokeError('observer found duplicate outcome owners')
        value.update(route=route,when=when,phase=phase or self.request_phase('/sync/'),nonce=self.nonce,ordinal=ordinal)
        with self.lock:self.samples.append(value)

    def forward(self,handler):
        connection=None;upstream_socket=None;watcher=None;item=None;route=urlsplit(handler.path).path
        cancelled=threading.Event();upstream_cancelled=threading.Event();watch_done=threading.Event();transport_stage=None
        sampled=False;sample_ended=False
        upstream_state=threading.Lock();upstream_finished=False
        def close_upstream():
            with upstream_state:
                if upstream_socket is None or upstream_finished or upstream_socket.fileno()<0:return
                # Publish intent before shutdown can wake getresponse/read on
                # the forwarding thread. A failed syscall stays observable.
                upstream_cancelled.set()
                try:upstream_socket.shutdown(socket.SHUT_RDWR)
                except OSError as error:
                    with self.lock:self.errors.append(dict(route=route,error='upstream cancellation shutdown: '+str(error)))
        def watch_client():
            try:
                while not watch_done.is_set():
                    try:
                        ready,_,_=select.select([handler.connection],[],[],.05)
                        if not ready:continue
                        if handler.connection.recv(1,socket.MSG_PEEK|socket.MSG_DONTWAIT):
                            watch_done.wait(.02);continue
                    except BlockingIOError:continue
                    except (OSError,ValueError):
                        if watch_done.is_set():return
                    cancelled.set();self.disconnect_observed.set();close_upstream();return
            finally:
                with self.lock:self.handlers.discard(threading.current_thread())
        with self.lock:
            self.ordinal+=1;ordinal=self.ordinal;phase=self.phase
        try:
            phase=self.request_phase(route)
            if (self.phase_file is not None and route.startswith('/sync/snapshot-sessions')
                    and handler.command in('GET','POST')and phase not in('initial','explicit-rebuild')):
                raise smoke.SmokeError('unchanged-state audit attempted snapshot creation/download')
            raw=self.read_body(handler);decoded=self.decoded(raw,handler.headers.get('Content-Encoding',''))
            item=dict(ordinal=ordinal,nonce=self.nonce,phase=phase,method=handler.command,route=route,
                encoded_request_body_bytes=len(raw),decoded_request_body_bytes=len(decoded),
                encoded_request_sha256=smoke.sha(raw),decoded_request_sha256=smoke.sha(decoded),
                source=handler.headers.get('Oversync-Source-ID'),v1=handler.headers.get('Oversync-Protocol-Version'))
            if self.capture_bodies:
                item.update(wire_workload=handler.headers.get('X-Qualification-Workload'),
                    wire_stage=handler.headers.get('X-Qualification-Stage'))
            watcher=threading.Thread(target=watch_client,daemon=True)
            with self.lock:
                self.handlers.add(watcher)
                try:watcher.start()
                except BaseException:self.handlers.discard(watcher);raise
            if route.startswith('/sync/')and handler.command!='OPTIONS'and item['v1']!='v1':raise smoke.SmokeError('observer SDK request lacks exact v1 marker')
            if route=='/sync/push-outcomes/ack'and handler.command=='POST':
                if len(decoded)>4096:raise smoke.SmokeError('observer ack exceeds the existing control bound')
                item['operation']=json.loads(decoded)
            sample_request=self.observe_acks and handler.command!='OPTIONS'and(route=='/sync/push-outcomes/ack'or self.phase_file is not None and route.startswith('/sync/'))
            if sample_request:
                self.physical(route,'before',phase,ordinal);sampled=True
            headers={k:v for k,v in handler.headers.items()if k.lower()not in('host','connection','transfer-encoding','content-length','proxy-connection')}
            headers.update({'Content-Length':str(len(raw)),'Connection':'close'})
            timeout=remaining(self.resources.command_deadline or time.monotonic()+120,120)
            connection=http.client.HTTPConnection(self.upstream.hostname,self.upstream.port,timeout=timeout)
            transport_stage='upstream'
            connection.connect()
            with upstream_state:upstream_socket=connection.sock
            with self.lock:self.sockets.add(upstream_socket)
            if cancelled.is_set():raise ClientDisconnected('client cancelled before forwarding')
            began=time.monotonic();connection.request(handler.command,handler.path,raw,headers)
            response=connection.getresponse();body=response.read(HTTP_BODY_LIMIT+1)
            with upstream_state:upstream_finished=True
            transport_stage=None
            if len(body)>HTTP_BODY_LIMIT:raise smoke.SmokeError('observer response entity exceeds protocol ceiling')
            decoded_response=self.decoded(body,response.getheader('Content-Encoding',''))
            item.update(status=response.status,upstream_seconds=time.monotonic()-began,
                encoded_response_body_bytes=len(body),decoded_response_body_bytes=len(decoded_response),
                encoded_response_sha256=smoke.sha(body),decoded_response_sha256=smoke.sha(decoded_response),
                request_content_encoding=handler.headers.get('Content-Encoding',''),
                response_content_encoding=response.getheader('Content-Encoding',''),vary=response.getheader('Vary',''))
            if self.capture_bodies and (route.startswith('/sync/')or route=='/qualification/wire/incompressible')and handler.command!='OPTIONS':
                folder=self.output/f'{ordinal:06d}';folder.mkdir(mode=0o700)
                for name,value in [('request.entity',raw),('response.entity',body)]:
                    path=folder/name;path.write_bytes(value);path.chmod(0o600)
                item['raw_entities']=dict(request=str(folder/'request.entity'),response=str(folder/'response.entity'))
            if sampled:
                self.physical(route,'after',phase,ordinal);sample_ended=True
            item['client_cancelled']=False
            transport_stage='downstream'
            handler.send_response(response.status)
            for key,value in response.getheaders():
                if key.lower()not in('connection','transfer-encoding','content-length','server','date','x-qualification-ordinal','x-qualification-nonce'):handler.send_header(key,value)
            if self.capture_bodies:
                handler.send_header('X-Qualification-Ordinal',str(ordinal));handler.send_header('X-Qualification-Nonce',self.nonce)
            handler.send_header('Content-Length',str(len(body)));handler.send_header('Connection','close');handler.end_headers()
            handler.wfile.write(body);handler.close_connection=True
            transport_stage=None
            with self.lock:self.requests.append(item)
        except BaseException as error:
            if self.expected_cancellation(error,transport_stage,cancelled.is_set(),upstream_cancelled.is_set()):
                row=item or dict(ordinal=ordinal,nonce=self.nonce,phase=phase,method=handler.command,route=route)
                row.update(client_cancelled=True)
                row.setdefault('status',None)
                with self.lock:self.requests.append(row)
            else:
                with self.lock:self.errors.append(dict(route=route,error=smoke.redact(str(error),self.resources.env)))
                try:handler.send_error(502,'owned qualification observer failed')
                except (OSError,ValueError):pass
        finally:
            watch_done.set()
            with upstream_state:upstream_finished=True
            if connection:connection.close()
            if sampled and not sample_ended:
                try:self.physical(route,'after',phase,ordinal)
                except BaseException as error:
                    with self.lock:self.errors.append(dict(route=route,error='terminal physical sample: '+smoke.redact(str(error),self.resources.env)))
            if upstream_socket is not None:
                with self.lock:self.sockets.discard(upstream_socket)
            if watcher is not None and watcher.ident is not None:
                watcher.join(timeout=max(0,min(1,(self.resources.command_deadline or time.monotonic()+1)-time.monotonic())))
                if watcher.is_alive():
                    with self.lock:self.errors.append(dict(route=route,error='client-disconnect watcher did not join'))

    def expected_cancellation(self,error: BaseException, stage: str | None, cancelled: bool, upstream_closed: bool) -> bool:
        if isinstance(error,ClientDisconnected):return True
        if stage=='downstream' and isinstance(error,(BrokenPipeError,ConnectionResetError,ConnectionAbortedError)):
            return True
        if stage not in ('upstream','downstream')or not(cancelled or self.stopping.is_set()):return False
        if isinstance(error,TimeoutError):return False
        closed_transport=isinstance(error,(http.client.RemoteDisconnected,http.client.IncompleteRead))or(
            isinstance(error,OSError)and error.errno in(errno.EBADF,errno.ENOTCONN,errno.EPIPE,errno.ECONNRESET,errno.ECONNABORTED))
        return closed_transport and(upstream_closed or self.stopping.is_set())

    def wait_idle(self,deadline: float):
        while True:
            with self.lock:threads=list(self.handlers)
            if not threads:return
            for thread in threads:thread.join(timeout=remaining(deadline,.1))

    def stop(self,deadline: float):
        if self.stopped:return
        self.stopping.set()
        if self.thread is not None and self.thread.is_alive():
            shutdown=threading.Thread(target=self.server.shutdown,daemon=True);shutdown.start()
            shutdown.join(timeout=remaining(deadline,30))
            if shutdown.is_alive():raise smoke.SmokeError('observer listener shutdown did not join')
        self.server.server_close()
        with self.lock:sockets=list(self.sockets)
        for connection in sockets:
            try:connection.shutdown(socket.SHUT_RDWR)
            except OSError:pass
            connection.close()
        self.wait_idle(deadline)
        if self.thread is not None:
            self.thread.join(timeout=remaining(deadline,30))
            if self.thread.is_alive():raise smoke.SmokeError('observer listener remains alive')
        self.stopped=True

    def report(self) -> dict:
        with self.lock:
            acks=[x for x in self.requests if x['route']=='/sync/push-outcomes/ack'and x['method']=='POST']
            repeats=Counter((x.get('source'),json.dumps(x.get('operation'),sort_keys=True))for x in acks)
            return dict(nonce=self.nonce,requests=list(self.requests),acks=acks,ack_repeat_counts=sorted(repeats.values()),
                download_phase_counts=dict(Counter(x['phase']for x in self.requests if x['route'].startswith('/sync/snapshot-sessions')and x['method']in('GET','POST'))),
                physical_samples=list(self.samples),errors=list(self.errors),
                peak_outcome_envelope_bytes=max((x['outcome_envelope_bytes']for x in self.samples),default=0),
                measurement_scope='External observer entity copies/PG reads; upstream latency excludes observer SQL; no SDK memory or deployment storage ceiling inferred.')


def remaining(deadline: float, maximum: float) -> float:
    left = deadline - time.monotonic()
    if left <= 0:
        raise smoke.SmokeError("shared deadline exhausted")
    return min(left, maximum)


def group_alive(group: int) -> bool:
    try:
        os.killpg(group, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # EPERM is presence, not proof of absence.


def _read_private_readiness(path: Path, *, owner: str, nonce: str, pid: int, group: int,
                           profile: str, database: str, fixture_sha256: str, started_ns: int) -> dict:
    """A complete private IPC frame must identify the exact registered producer."""
    before=path.lstat()
    if (not stat.S_ISREG(before.st_mode) or stat.S_IMODE(before.st_mode)!=0o600
            or before.st_uid!=os.getuid() or before.st_size>1024*1024 or before.st_mtime_ns<started_ns):
        raise smoke.SmokeError('unsafe or stale private readiness file')
    def unique(pairs):
        result={}
        for key,value in pairs:
            if key in result:raise smoke.SmokeError('duplicate private readiness field')
            result[key]=value
        return result
    def invalid_constant(value):
        raise smoke.SmokeError('nonfinite private readiness value')
    value=json.loads(path.read_bytes(),object_pairs_hook=unique,parse_constant=invalid_constant)
    after=path.lstat()
    if (before.st_ino,before.st_size,before.st_mtime_ns,before.st_mode)!=(after.st_ino,after.st_size,after.st_mtime_ns,after.st_mode):
        raise smoke.SmokeError('private readiness changed while reading')
    expected=dict(schema_version=1,owner=owner,nonce=nonce,pid=pid,pgid=group,
                  profile=profile,database=database,fixture_sha256=fixture_sha256,reset_domain=nonce)
    if (not isinstance(value,dict) or any(value.get(k)!=v for k,v in expected.items())
            or any(type(value.get(k))is not int for k in('schema_version','pid','pgid','created_unix_ns'))
            or not started_ns<=value['created_unix_ns']<=time.time_ns()):
        raise smoke.SmokeError('private readiness producer/nonce/profile binding mismatch')
    os.kill(pid,0)
    if os.getpgid(pid)!=group:raise smoke.SmokeError('private readiness producer group changed')
    parsed=urlsplit(value.get('authority_url',''))
    if (parsed.scheme!='http' or parsed.hostname!='127.0.0.1' or not parsed.port
            or parsed.path or parsed.query or parsed.fragment or parsed.username or parsed.password):
        raise smoke.SmokeError('private readiness listener must be exact loopback HTTP')
    generation=value.get('generation',{});schema=generation.get('schema_generation',{})
    if (generation.get('protocol_version')!='v1' or schema.get('id')!='1'
            or not re.fullmatch(r'[a-f0-9]{64}',schema.get('fingerprint',''))
            or not isinstance(schema.get('tables'),list) or not schema['tables']
            or not isinstance(generation.get('limits'),dict) or not generation['limits']):
        raise smoke.SmokeError('private readiness lacks actual generation/capability metadata')
    return value


def read_private_readiness(path: Path, **expected) -> dict:
    try:
        return _read_private_readiness(path, **expected)
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
        raise smoke.SmokeError('invalid or incomplete private readiness frame') from error


class Inspection:
    """Registered probe with independent pipe drains and no unbounded wait."""
    def __init__(self, argv, cwd, env, input_text):
        self.proc=subprocess.Popen(UNMASK+argv,cwd=cwd,env=env,start_new_session=True,
            stdin=subprocess.PIPE if input_text is not None else subprocess.DEVNULL,
            stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        self.output=[bytearray(),bytearray()]
        self.errors=[]
        self.readers=[]
        self.input_text=input_text

    def start(self):
        def drain(index,pipe):
            try:
                with pipe:
                    while chunk:=pipe.read(65536):self.output[index].extend(chunk)
            except Exception as error:self.errors.append(error)
        self.readers=[threading.Thread(target=drain,args=(i,pipe),daemon=True)
                      for i,pipe in enumerate((self.proc.stdout,self.proc.stderr))]
        if self.input_text is not None:
            def feed():
                try:
                    with self.proc.stdin:self.proc.stdin.write(self.input_text.encode())
                except Exception as error:self.errors.append(error)
            self.readers.append(threading.Thread(target=feed,daemon=True))
        for reader in self.readers:reader.start()

    def close_idle_pipes(self):
        if not any(reader.ident is not None for reader in self.readers):
            for pipe in(self.proc.stdin,self.proc.stdout,self.proc.stderr):
                if pipe is not None:pipe.close()


class Resources:
    def __init__(self, root: Path, go: Path, output: Path, env: dict[str, str]):
        self.root, self.go, self.output, self.env = root, go, output, env
        self.children: list[smoke.Child] = []
        self.inspections: list[Inspection] = []
        self.process_registration = threading.RLock()
        self.observers: list[HTTPObserver] = []
        self.exclusive_leases: list[tuple[int,dict]] = []
        self.groups: set[int] = set()
        self.run_id = "oversqlite-" + uuid.uuid4().hex
        self.volume, self.container = None, None
        self.android: dict | None = None
        self.ios: str | None = None
        self.ios_name: str | None = None
        self.protected_groups={os.getpgrp(),os.getpgid(os.getppid())}
        self.record: dict = {"run_id": self.run_id,"coordinator_pid":os.getpid(),
            "coordinator_group":os.getpgrp(),"protected_groups":sorted(self.protected_groups)}
        self.command_deadline: float | None = None
        self.cleanup_deadline: float | None = None
        self.env["OVERSQLITE_PROCESS_OWNER"] = self.run_id

    def verify_delegated_lease(self, key: str, value: str, proof: dict, frozen: dict) -> dict:
        """Bind a supervisor-owned lease without adopting or deleting its resources."""
        path = Path(proof.get("path", ""))
        if (not path.is_absolute() or not path.is_file()
                or path.stat().st_mode & 0o077
                or frozen.get(str(path)) != proof.get("sha256")
                or smoke.sha(path.read_bytes()) != proof.get("sha256")):
            raise smoke.SmokeError("lease proof is not a frozen private ownership record: " + key)
        record = json.loads(path.read_text())
        owner = record.get("owner_pid", 0)
        if (type(owner) is not int or owner <= 1 or not record.get("nonce")
                or record.get("values", {}).get(key) != smoke.sha(value.encode())
                or not re.fullmatch(r"oversqlite-[a-f0-9]{32}", record.get("run_id", ""))):
            raise smoke.SmokeError("lease value/owner/nonce binding mismatch: " + key)
        os.kill(owner, 0)
        if os.getpgid(owner) != record.get("owner_group"):
            raise smoke.SmokeError("lease supervisor process group changed")
        resource = record["resource"]
        if resource.get("run_id") != record["run_id"]:
            raise smoke.SmokeError("lease resource belongs to another supervisor")
        container = resource.get("container")
        if container:
            if container != record["run_id"]:
                raise smoke.SmokeError("lease container name is not its recorded owner")
            label = self.inspect(["docker", "inspect", "--format", '{{index .Config.Labels "' + LABEL + '"}}', container], 5)
            if label != record["run_id"]:
                raise smoke.SmokeError("lease container ownership label mismatch")
        if key.endswith((".url", ".android_url", ".observer_url")):
            parsed, canonical = urlsplit(value), urlsplit(resource.get("authority_url", ""))
            expected_app=resource.get("authority_status",{}).get("app_name")
            expected_profile=next((part for part in reversed(key.split('.')) if part in ('rich','categories','dependency','activity','load','wire')),None)
            if not expected_app or not expected_profile or resource.get("profile") != expected_profile:
                raise smoke.SmokeError("lease application/schema profile is not bound")
            if (parsed.scheme != "http" or parsed.hostname not in ("127.0.0.1", "10.0.2.2")
                    or parsed.username or parsed.password or parsed.query or parsed.fragment
                    or parsed.path not in ("", "/") or parsed.port != canonical.port
                    or canonical.hostname != "127.0.0.1"):
                raise smoke.SmokeError("lease URL does not identify its owned loopback listener")
            with urlopen(resource["authority_url"] + "/syncx/status", timeout=5) as response:
                current = json.load(response)
            if current.get("app_name") != expected_app:
                raise smoke.SmokeError("lease live application readiness changed")
        elif key.endswith("admin_dsn"):
            parsed = urlsplit(value)
            if not container or parsed.scheme not in ("postgres", "postgresql") or parsed.hostname != "127.0.0.1" or parsed.port != resource.get("postgres_port"):
                raise smoke.SmokeError("lease PostgreSQL route is not the owned container")
        elif key == "lease.ios.udid":
            if value != resource.get("ios_udid"):
                raise smoke.SmokeError("lease simulator identity mismatch")
        else:
            raise smoke.SmokeError("unsupported delegated lease type: " + key)
        return dict(path=str(path), sha256=proof["sha256"], run_id=record["run_id"], nonce=record["nonce"])

    @contextmanager
    def registering_processes(self, deadline: float):
        """Exclude a census from the fork-to-registration window, within its budget."""
        if not self.process_registration.acquire(timeout=remaining(deadline, 5)):
            raise smoke.SmokeError('process registration exceeded inspection deadline')
        try:
            remaining(deadline, 5)
            yield
        finally:
            self.process_registration.release()

    def probe(self, argv: list[str], seconds: float = 10, *, env: dict | None = None,
              input_text: str | None = None, cwd: Path | None = None) -> Inspection:
        """Return a completed registered probe, including expected nonzero status."""
        inspect_env = dict(env or self.env)
        inspect_env['OVERSQLITE_PROCESS_OWNER']=self.run_id
        if self.command_deadline is not None:
            seconds = remaining(self.command_deadline, seconds)
        deadline=time.monotonic()+seconds
        signals={signal.SIGINT,signal.SIGTERM,signal.SIGHUP}
        with self.registering_processes(deadline):
            prior=signal.pthread_sigmask(signal.SIG_BLOCK,signals)
            try:
                probe=Inspection(argv,cwd or self.root,inspect_env,input_text)
                self.inspections.append(probe);self.groups.add(probe.proc.pid)
            finally:
                signal.pthread_sigmask(signal.SIG_SETMASK,prior)
        try:
            probe.start()
            probe.proc.wait(timeout=remaining(deadline,seconds))
            for reader in probe.readers:reader.join(timeout=remaining(deadline,seconds))
            if any(reader.is_alive()for reader in probe.readers)or probe.errors:
                raise smoke.SmokeError('inspection pipe capture incomplete: '+argv[0])
            return probe
        except BaseException:
            # subprocess.run's timeout handler waits without a second bound.
            # Register and signal this exact launch, then let shared cleanup
            # perform bounded joins; a blocked probe can never stall the caller.
            try:os.killpg(probe.proc.pid,signal.SIGKILL)
            except ProcessLookupError:pass
            probe.close_idle_pipes()
            raise

    def inspect(self, argv: list[str], seconds: float = 10, *, env: dict | None = None,
                input_text: str | None = None, cwd: Path | None = None) -> str:
        probe=self.probe(argv,seconds,env=env,input_text=input_text,cwd=cwd)
        if probe.proc.returncode:
            stderr=probe.output[1].decode(errors='replace')
            raise smoke.SmokeError(smoke.redact(f'inspection failed: {argv[0]}: {stderr}',self.env))
        return probe.output[0].decode(errors='replace').strip()

    def launch(self, argv: list[str], log: Path, *, cwd: Path | None = None,
               env: dict | None = None) -> smoke.Child:
        child_env = dict(self.env if env is None else env)
        child_env['OVERSQLITE_PROCESS_OWNER'] = self.run_id
        if self.command_deadline is not None:
            remaining(self.command_deadline, 1)
        # Defer coordinator interruption until the child is registered. Unmask in
        # the child before exec rather than leaving inherited blocked signals.
        signals = {signal.SIGINT, signal.SIGTERM, signal.SIGHUP}
        deadline = min(time.monotonic()+5, self.command_deadline) if self.command_deadline is not None else time.monotonic()+5
        with self.registering_processes(deadline):
            prior = signal.pthread_sigmask(signal.SIG_BLOCK, signals)
            try:
                child = smoke.Child(UNMASK + argv, cwd or self.root, child_env, log)
                self.children.append(child)
                self.groups.add(child.proc.pid)
            finally:
                signal.pthread_sigmask(signal.SIG_SETMASK, prior)
        return child

    def capture_groups(self, deadline: float | None = None) -> None:
        deadline = min(deadline, time.monotonic()+5) if deadline is not None else time.monotonic()+5
        if self.command_deadline is not None:
            deadline = min(deadline, self.command_deadline)
        with self.registering_processes(deadline):
            raw = self.inspect(["ps", "eww", "-axo", "pid=,ppid=,pgid=,command="], remaining(deadline, 5))
            self.adopt_process_snapshot(raw, deadline)

    def adopt_process_snapshot(self, raw: str, deadline: float) -> None:
        rows, tagged = [], set()
        token = re.compile(r"(?:^|\s)OVERSQLITE_PROCESS_OWNER=" + re.escape(self.run_id) + r"(?:\s|$)")
        for line in raw.splitlines():
            parts = line.strip().split(None, 3)
            if len(parts) < 3:
                continue
            pid, parent, group = map(int, parts[:3])
            rows.append((pid, parent, group))
            if len(parts) == 4 and token.search(parts[3]):
                tagged.add(pid)
        # A reaped PID can name an unrelated process in a later snapshot. Only
        # still-live Popen identities may seed ancestry; tagged orphans remain
        # discoverable after their original launcher exits.
        registered = {item.proc.pid:item.proc for item in (*self.children,*self.inspections)}
        live = {pid:proc for pid,proc in registered.items() if proc.poll() is None}
        owned = set(live) | tagged
        while True:
            expanded = owned | {pid for pid, parent, _ in rows if parent in owned}
            if expanded == owned:
                break
            owned = expanded
        for pid, parent, group in rows:
            if pid in owned:
                remaining(deadline, 5)
                observed_group=group
                proc=live.get(pid)
                if proc is not None:
                    # macOS ps obtains process metadata and environment at
                    # different times. Recheck a registered live identity so a
                    # pre-setsid group cannot be combined with its later tag.
                    try:group=os.getpgid(pid)
                    except ProcessLookupError:continue
                    if proc.poll() is not None:continue
                    if group!=observed_group:
                        self.record.setdefault('process_census_rechecks',[]).append(dict(
                            pid=pid,ppid=parent,snapshot_group=observed_group,current_group=group,
                            registered_group=proc.pid,tagged=pid in tagged))
                if group <= 1 or group in self.protected_groups:
                    refusal=dict(pid=pid,ppid=parent,snapshot_group=observed_group,current_group=group,
                        tagged=pid in tagged,registered_live=proc is not None,
                        registered_group=proc.pid if proc is not None else None,
                        protected_groups=sorted(self.protected_groups))
                    self.record.setdefault('process_census_refusals',[]).append(refusal)
                    raise smoke.SmokeError("refuse caller/non-owned process group: "+json.dumps(refusal,sort_keys=True))
                self.groups.add(group)

    def wait(self, child: smoke.Child, seconds: float, *, capture: bool = True) -> int:
        if self.command_deadline is not None:
            seconds = remaining(self.command_deadline, seconds)
        deadline = time.monotonic() + seconds
        while child.proc.poll() is None:
            remaining(deadline, 1)
            if capture and self.ios_name is None:
                self.capture_groups(deadline)
            if self.record.get('go_fixture_observation'):
                self.capture_go_fixtures(deadline)
            time.sleep(min(1, remaining(deadline, 1)))
        return child.wait(remaining(deadline, 2))

    def prepare_go_fixtures(self) -> None:
        if self.env.get('OVERSYNC_TEST_RUN_ID')not in(None,self.run_id):
            raise smoke.SmokeError('inherited Go fixture ownership is not admitted')
        self.env['OVERSYNC_TEST_RUN_ID']=self.run_id
        if self.env.get('TEST_DATABASE_URL'):
            if self.env.get('OVERSYNC_TEST_ISOLATE_DATABASES')not in(None,'1'):
                raise smoke.SmokeError('caller PostgreSQL fixture requires per-pool database isolation')
            self.env['OVERSYNC_TEST_ISOLATE_DATABASES']='1'
            self.record['go_pool_isolation']='Existing test-only factory creates/drops one database per pool on the verified caller admin lease.'
        self.record['go_preexisting_volumes']=self.inspect(['docker','volume','ls','--format','{{.Name}}']).splitlines()
        self.record['go_fixtures']={};self.record['go_fixture_volumes']={}
        self.record['go_fixture_observation']=True

    def capture_go_fixtures(self, deadline: float, *, errors: list[str] | None = None) -> None:
        names=self.inspect(['docker','container','ls','--all','--filter','label='+LABEL+'='+self.run_id,'--format','{{.ID}}'],remaining(deadline,3)).splitlines()
        for name in names:
            try:self.capture_go_fixture(name,deadline)
            except Exception as error:
                if errors is None:raise
                errors.append(smoke.redact(str(error),self.env))

    def capture_go_fixture(self, name: str, deadline: float) -> None:
        value=self.inspect_go_fixture('container',name,deadline)
        if value is None:return
        identity=value['Id']
        if value.get('Config',{}).get('Labels',{}).get(LABEL)!=self.run_id:
            raise smoke.SmokeError('refuse foreign Go fixture container')
        image=value['Image']
        if re.fullmatch(r'sha256:[a-f0-9]{64}',image)is None:raise smoke.SmokeError('Go fixture image digest missing')
        binding=dict(name=value['Name'],created=value['Created'],image=image)
        if self.record['go_fixtures'].get(identity,binding)!=binding:
            raise smoke.SmokeError('Go fixture container identity changed')
        self.record['go_fixtures'][identity]=binding
        for mount in value.get('Mounts',[]):
            if mount['Type']!='volume':continue
            volume=mount['Name']
            if volume in self.record['go_preexisting_volumes']:
                raise smoke.SmokeError('Go fixture mounted a preexisting volume')
            data=self.inspect_go_fixture('volume',volume,deadline)
            if data is None:continue
            birth=data['CreatedAt']
            if data['Name']!=volume or self.record['go_fixture_volumes'].get(volume,birth)!=birth:
                raise smoke.SmokeError('Go fixture volume identity changed')
            self.record['go_fixture_volumes'][volume]=birth

    def inspect_go_fixture(self, resource: str, name: str, deadline: float) -> dict | None:
        probe=self.probe(['docker',resource,'inspect','--format','{{json .}}',name],remaining(deadline,3))
        if not probe.proc.returncode:return json.loads(probe.output[0])
        argv=['docker',resource,'ls','--format','{{.ID}}'if resource=='container'else'{{.Name}}']
        if resource=='container':argv+=['--all','--no-trunc']
        current=self.inspect(argv,remaining(deadline,3)).splitlines()
        if any(value==name or resource=='container'and value.startswith(name)for value in current):
            raise smoke.SmokeError('Go fixture inspection failed while the object remains')
        return None

    def cleanup_go_fixtures(self, deadline: float) -> None:
        errors=[]
        try:self.capture_go_fixtures(deadline,errors=errors)
        except Exception as error:errors.append(smoke.redact(str(error),self.env))
        for identity,saved in self.record['go_fixtures'].items():
            try:
                value=self.inspect_go_fixture('container',identity,deadline)
                if value is None:continue
                if (value['Id']!=identity or value['Name']!=saved['name']or value['Created']!=saved['created']
                        or value['Image']!=saved['image']
                        or value.get('Config',{}).get('Labels',{}).get(LABEL)!=self.run_id):
                    raise smoke.SmokeError('refuse changed/foreign Go fixture cleanup')
                self.inspect(['docker','rm','-fv',identity],remaining(deadline,5))
            except Exception as error:errors.append(smoke.redact(str(error),self.env))
        for name,birth in self.record['go_fixture_volumes'].items():
            try:
                value=self.inspect_go_fixture('volume',name,deadline)
                if value is None:continue
                if value['Name']!=name or value['CreatedAt']!=birth:
                    raise smoke.SmokeError('refuse replaced Go fixture volume')
                attached=self.inspect(['docker','container','ls','--all','--filter','volume='+name,'--format','{{.ID}}'],remaining(deadline,3))
                if attached:raise smoke.SmokeError('Go fixture volume is still attached; preserve it')
                self.inspect(['docker','volume','rm',name],remaining(deadline,4))
            except Exception as error:errors.append(smoke.redact(str(error),self.env))
        try:
            current=set(self.inspect(['docker','volume','ls','--format','{{.Name}}'],remaining(deadline,3)).splitlines())
            if current.intersection(self.record['go_fixture_volumes']):errors.append('Go fixture volumes remain')
        except Exception as error:errors.append(smoke.redact(str(error),self.env))
        self.record['go_fixture_cleanup_errors']=errors
        if errors:raise smoke.SmokeError('; '.join(errors))

    def command(self, argv: list[str], seconds: float, label: str, *, capture: bool = True, cwd: Path | None = None) -> None:
        if seconds <= 0:
            raise smoke.SmokeError("stage budget exhausted before child startup")
        child = self.launch(argv, self.output / (label + ".log"), cwd=cwd)
        if self.wait(child, seconds, capture=capture):
            raise smoke.SmokeError(f"{label} failed; see immutable log")

    def sampled_files_command(self, argv: list[str], temporary: Path, seconds: float, *, cwd: Path) -> dict:
        """Run one registered child and sample only its private temporary tree."""
        if temporary.is_symlink()or not temporary.resolve().is_relative_to(self.output.resolve()):
            raise smoke.SmokeError('file sampler requires the new run-owned temporary tree')
        deadline=min(self.command_deadline or float('inf'),time.monotonic()+seconds)
        report=dict(interval_seconds=.01,samples=0,per_file_sampled_peaks={},sampled_peak_file_bytes={},
            claim='Sampled logical/allocated file sizes are lower bounds; observer overhead included.')
        self.record['file_sampling']=report
        child=self.launch(argv,self.output/'native-command.log',cwd=cwd)
        start=time.monotonic();wall=time.time_ns();previous=None;longest=0
        while True:
            remaining(deadline,1);now=time.monotonic()
            if previous is not None:longest=max(longest,now-previous)
            previous=now;current={}
            for path in temporary.rglob('*'):
                remaining(deadline,1)
                try:info=path.lstat()
                except FileNotFoundError:continue
                if stat.S_ISLNK(info.st_mode):raise smoke.SmokeError('owned sampler refuses symlink traversal')
                if not stat.S_ISREG(info.st_mode):continue
                kind='wal'if path.name.endswith('-wal')else'shm'if path.name.endswith('-shm')else'sqlite'if '.sqlite'in path.name else'other'
                current[kind]=current.get(kind,0)+info.st_size
                name=str(path.relative_to(temporary))
                old=report['per_file_sampled_peaks'].get(name,dict(logical_bytes=0,allocated_bytes=0))
                report['per_file_sampled_peaks'][name]=dict(kind=kind,
                    logical_bytes=max(old['logical_bytes'],info.st_size),allocated_bytes=max(old['allocated_bytes'],info.st_blocks*512))
            current['all']=sum(current.values())
            for key,value in current.items():report['sampled_peak_file_bytes'][key]=max(report['sampled_peak_file_bytes'].get(key,0),value)
            report['samples']+=1
            if child.proc.poll()is not None:break
            time.sleep(min(.01,remaining(deadline,.01)))
        code=child.wait(remaining(deadline,2))
        report.update(exit_code=code,elapsed_seconds=time.monotonic()-start,
            wall_seconds=(time.time_ns()-wall)/1e9,longest_sample_gap_seconds=longest)
        if code:raise smoke.SmokeError('resource native command failed; see immutable log')
        if abs(report['wall_seconds']-report['elapsed_seconds'])>2:raise smoke.SmokeError('resource measurement crossed a suspend interval')
        return report

    def postgres(self) -> str:
        self.env["POSTGRES_PASSWORD"] = "fixture_" + secrets.token_urlsafe(32)
        name = self.run_id
        volume=name+'-data'
        if volume in self.inspect(['docker','volume','ls','--format','{{.Name}}']).splitlines():
            raise smoke.SmokeError('refuse existing PostgreSQL volume name')
        self.volume = volume  # Persist admitted intent before creating resources.
        self.record['volume_creation_admitted']=True
        self.record['volume']=self.volume
        self.inspect(["docker", "volume", "create", "--label", LABEL + "=" + name, self.volume])
        self.docker_owned('volume',self.volume)
        if name in self.inspect(['docker','container','ls','--all','--format','{{.Names}}']).splitlines():
            raise smoke.SmokeError('refuse existing PostgreSQL container name')
        self.container = name
        self.record['container_creation_admitted']=True
        self.record['container']=self.container
        self.inspect(["docker", "run", "--detach", "--name", name, "--label", LABEL + "=" + name,
                      "--publish", "127.0.0.1::5432", "--mount", f"type=volume,source={self.volume},target=/var/lib/postgresql/data",
                      "--env", "POSTGRES_USER=postgres", "--env", "POSTGRES_PASSWORD", "--env", "POSTGRES_DB=postgres", "postgres:16"])
        self.docker_owned('container',self.container)
        deadline = time.monotonic() + 60
        while True:
            ready = self.probe(["docker", "exec", name, "pg_isready", "-h", "127.0.0.1", "-U", "postgres"],
                               seconds=remaining(deadline, 5))
            if ready.proc.returncode == 0:
                break
            time.sleep(remaining(deadline, .2))
        port = self.inspect(["docker", "inspect", "--format", '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}', name])
        settings = self.sql("SELECT current_setting('fsync'),current_setting('synchronous_commit'),current_setting('full_page_writes')")
        if settings != "on|on|on":
            raise smoke.SmokeError("normal PostgreSQL durability required")
        self.record.update(postgres_settings=settings, image=self.inspect(["docker", "inspect", "--format", "{{.Image}}", name]), storage="durable-docker-volume",postgres_port=int(port))
        return f"postgres://postgres:{self.env['POSTGRES_PASSWORD']}@127.0.0.1:{port}/postgres?sslmode=disable"

    def docker_owned(self, resource: str, name: str, seconds: float = 5) -> str:
        expected=self.run_id+('-data'if resource=='volume'else'')
        if resource not in ('container','volume') or name!=expected or not self.record.get(resource+'_creation_admitted'):
            raise smoke.SmokeError('refuse unadmitted Docker resource cleanup')
        data=json.loads(self.inspect(['docker',resource,'inspect','--format','{{json .}}',name],seconds))
        labels=data.get('Labels',{})if resource=='volume'else data.get('Config',{}).get('Labels',{})
        identity=data.get('CreatedAt')if resource=='volume'else data.get('Id')
        if (not isinstance(labels,dict) or labels.get(LABEL)!=self.run_id or not identity
                or data.get('Name','').removeprefix('/')!=name
                or self.record.get(resource+'_identity',identity)!=identity):
            raise smoke.SmokeError('refuse changed/foreign Docker resource')
        self.record[resource+'_identity']=identity
        return name if resource=='volume'else identity

    def sql(self, query: str, database: str = "postgres", seconds: float = 5) -> str:
        return self.inspect(["docker", "exec", self.container, "psql", "-U", "postgres", "-d", database, "-Atc", query], seconds)

    def sql_json(self, query: str, database: str, seconds: float = 5) -> dict:
        lines=self.sql(query,database,seconds).splitlines()
        # PostgreSQL json_agg may put line breaks between composite rows.
        # Decode one entire document; json.loads also rejects a second result.
        payload='\n'.join(line for line in lines if line not in ('BEGIN','COMMIT'))
        try:
            value=json.loads(payload)
        except ValueError as error:
            raise smoke.SmokeError('read-only PostgreSQL observation is partial or ambiguous')from error
        if not isinstance(value,dict):
            raise smoke.SmokeError('read-only PostgreSQL observation is partial or ambiguous')
        return value

    def http_observer(self, upstream: str, database: str, *, nonce: str, capture_bodies: bool = False,
                      phase_file: Path | None = None) -> HTTPObserver:
        if upstream!=self.record.get('authority_url') or database!=self.record.get('database'):
            raise smoke.SmokeError('observer must use this invocation\'s actual owned authority/database')
        if phase_file is not None and not phase_file.resolve().is_relative_to(self.output.resolve()):
            raise smoke.SmokeError('audit phase file must belong to this new attempt')
        output=self.output/('http-observer-'+str(len(self.observers)));output.mkdir(mode=0o700)
        signals={signal.SIGINT,signal.SIGTERM,signal.SIGHUP};previous=signal.pthread_sigmask(signal.SIG_BLOCK,signals)
        try:
            observer=HTTPObserver(self,upstream,database,output,nonce=nonce,capture_bodies=capture_bodies,phase_file=phase_file)
            self.observers.append(observer)
            self.record.setdefault('http_observers',[]).append(dict(url=observer.url,upstream=upstream,database=database,
                nonce=nonce,output=str(output),capture_bodies=capture_bodies))
            observer.start()
            with urlopen(observer.url+'/syncx/status',timeout=remaining(self.command_deadline or time.monotonic()+5,5))as response:
                status=json.load(response)
            expected=self.record.get('authority_status',{}).get('app_name')
            if not expected or status.get('app_name')!=expected:
                raise smoke.SmokeError('observer does not reach the same owned application')
            observer.wait_idle(self.command_deadline or time.monotonic()+5)
            return observer
        finally:signal.pthread_sigmask(signal.SIG_SETMASK,previous)

    def authority(self, fixture: Path, *, profile: str = 'categories', arguments: tuple[str,...] = (),
                  private_ready: bool = False, expected_app: str = 'nethttp-server-example',
                  readiness_deadline: float | None = None, expected_marker: str | None = None) -> str:
        previous=self.command_deadline
        if readiness_deadline is not None:
            self.command_deadline=min(previous,readiness_deadline)if previous is not None else readiness_deadline
        try:
            return self._authority(fixture,profile=profile,arguments=arguments,private_ready=private_ready,
                expected_app=expected_app,readiness_deadline=readiness_deadline,expected_marker=expected_marker)
        finally:
            self.command_deadline=previous

    def _authority(self, fixture: Path, *, profile: str, arguments: tuple[str,...], private_ready: bool,
                   expected_app: str, readiness_deadline: float | None, expected_marker: str | None) -> str:
        url = self.postgres()
        self.env.update(TEST_DATABASE_URL=url, OVERSYNC_EXAMPLE_TEST_DATABASE_URL=url,
                        OVERSYNC_SMOKE_DATABASE_NAME="oversync_smoke_" + uuid.uuid4().hex)
        private=None
        if private_ready:
            directory=self.output/'private';directory.mkdir(mode=0o700)
            private=directory/'authority-ready.json'
            nonce=uuid.uuid4().hex
            self.env.update(OVERSYNC_QUALIFICATION_READY_FILE=str(private),OVERSYNC_QUALIFICATION_READY_NONCE=nonce)
        started_ns=time.time_ns();digest=smoke.sha(fixture.read_bytes())
        child = self.launch([str(fixture),*arguments], self.output / "authority.log", cwd=self.go)
        deadline = min(time.monotonic()+60,readiness_deadline)if readiness_deadline is not None else time.monotonic()+60
        base = None
        while base is None:
            if child.proc.poll() is not None:
                raise smoke.SmokeError("categories authority exited before readiness")
            remaining(deadline, 1)
            if private is not None:
                if not private.exists()and not private.is_symlink():
                    time.sleep(remaining(deadline,.05));continue
                ready=read_private_readiness(private,owner=self.run_id,nonce=nonce,pid=child.proc.pid,
                    group=child.proc.pid,profile=profile,database=self.env['OVERSYNC_SMOKE_DATABASE_NAME'],
                    fixture_sha256=digest,started_ns=started_ns)
                base=ready['authority_url']
                self.record.update(private_readiness=ready,profile=profile,private_readiness_path=str(private),
                    private_readiness_sha256=smoke.sha(private.read_bytes()))
                break
            text = child.log.read_text() if child.log.exists() else ""
            match = re.search(r"^OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL=(http://127\.0\.0\.1:[0-9]+)\r?\n", text, re.M)
            if match:
                base = match[1]
            else:
                time.sleep(remaining(deadline, .05))
        parsed = urlsplit(base)
        if parsed.scheme != "http" or parsed.hostname != "127.0.0.1" or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise smoke.SmokeError("authority is not owned loopback HTTP")
        with urlopen(base + "/syncx/status", timeout=remaining(deadline, 5)) as response:
            status = json.load(response)
        if status.get("app_name") != expected_app:
            raise smoke.SmokeError("wrong fixture application")
        if private_ready:
            if (status.get('status')!='healthy' or status.get('lifecycle')!='running'
                    or status.get('accepting_operations')is not True
                    or self.sql('SELECT current_database()',self.env['OVERSYNC_SMOKE_DATABASE_NAME'])!=self.env['OVERSYNC_SMOKE_DATABASE_NAME']
                    or smoke.sha(fixture.read_bytes())!=digest):
                raise smoke.SmokeError('private authority actual readiness/database/fixture changed')
            self.record.update(authority_pid=child.proc.pid,authority_group=child.proc.pid)
        if expected_marker:
            while True:
                count=child.log.read_text().splitlines().count(expected_marker)
                if count>1:raise smoke.SmokeError('duplicate authority configuration witness')
                if count==1:break
                if child.proc.poll()is not None:raise smoke.SmokeError('authority exited before configuration witness')
                time.sleep(remaining(deadline,.02))
            self.record['authority_profile_marker']=expected_marker
        self.record.update(authority_url=base, authority_status=status, database=self.env["OVERSYNC_SMOKE_DATABASE_NAME"],
                           fixture_sha256=smoke.sha(fixture.read_bytes()))
        return base

    def acquire_exclusive(self, name: str, *, lock_root: Path | None = None) -> dict:
        if name!='measurement-exclusive':raise smoke.SmokeError('unknown host-exclusive lease')
        folder=lock_root or Path('/tmp').resolve()/('oversqlite-qualification-locks-'+str(os.getuid()))
        folder.mkdir(mode=0o700,exist_ok=True)
        info=folder.stat()
        if folder.is_symlink() or folder.resolve()!=folder or info.st_uid!=os.getuid() or info.st_mode & 0o077:
            raise smoke.SmokeError('host lease directory is not canonical/private/current-user owned')
        path=folder/(name+'.lock');fd=None
        signals={signal.SIGINT,signal.SIGTERM,signal.SIGHUP}
        previous=signal.pthread_sigmask(signal.SIG_BLOCK,signals)
        try:
            fd=os.open(path,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
            info=os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.getuid() or info.st_mode & 0o077:
                raise smoke.SmokeError('host lease file has wrong ownership/type/mode')
            fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            record=dict(name=name,path=str(path),inode=info.st_ino,run_id=self.run_id,coordinator_pid=os.getpid(),released=False)
            self.exclusive_leases.append((fd,record));fd=None
            self.record.setdefault('exclusive_leases',[]).append(record)
            return record
        except BlockingIOError as error:
            raise smoke.SmokeError('host-exclusive measurement lease is already held')from error
        finally:
            if fd is not None:os.close(fd)
            signal.pthread_sigmask(signal.SIG_SETMASK,previous)

    def android_device(self) -> str:
        sdk = Path(self.env.get("ANDROID_HOME", str(Path.home() / "Library/Android/sdk")))
        name = "oversqlite-" + uuid.uuid4().hex[:12]
        private = self.output / "android"
        private.mkdir()
        avd = private / "avd"
        user = private / "user"
        avd.mkdir(); user.mkdir()
        adb = str(sdk / "platform-tools/adb")
        before = self.inspect([adb, "devices"])
        port = None
        for candidate in range(5570, 5680, 2):
            if f"emulator-{candidate}" in before:
                continue
            try:
                with socket.socket() as a, socket.socket() as b:
                    a.bind(("127.0.0.1", candidate)); b.bind(("127.0.0.1", candidate + 1))
                port = candidate
                break
            except OSError:
                continue
        if port is None:
            raise smoke.SmokeError("no unowned emulator port")
        serial = f"emulator-{port}"
        self.env.update(ANDROID_HOME=str(sdk), ANDROID_SDK_ROOT=str(sdk), ANDROID_AVD_HOME=str(avd),
                        ANDROID_SERIAL=serial)
        # The device tools need private preferences; Gradle must retain the
        # producer's user home and debug signing key so the sealed APK is reused.
        device_env = dict(self.env, ANDROID_USER_HOME=str(user))
        self.android = dict(name=name, serial=serial, sdk=str(sdk), private=str(private), before=before)
        self.record["android"] = self.android
        (self.output / "ownership.json").write_text(json.dumps(self.record, indent=2) + "\n")
        manager = str(sdk / "cmdline-tools/latest/bin/avdmanager")
        self.inspect([manager, "create", "avd", "--name", name, "--package", "system-images;android-33;aosp_atd;arm64-v8a", "--path", str(avd / name)], 60, env=device_env, input_text="no\n")
        child = self.launch([str(sdk / "emulator/emulator"), "-avd", name, "-port", str(port), "-no-window", "-no-audio",
                             "-no-boot-anim", "-no-snapshot", "-memory", "2048", "-gpu", "swiftshader_indirect"], private / "emulator.log", env=device_env)
        deadline = time.monotonic() + 120
        while True:
            if child.proc.poll() is not None:
                raise smoke.SmokeError("owned emulator stopped before boot")
            result = self.probe([adb, "-s", serial, "shell", "getprop", "sys.boot_completed"],
                                seconds=remaining(deadline, 5))
            if result.proc.returncode == 0 and result.output[0].decode().strip() == "1":
                break
            time.sleep(remaining(deadline, .2))
        self.verify_android()
        self.inspect([adb, "-s", serial, "logcat", "-c"])
        return serial

    def verify_android(self) -> None:
        device = self.android
        name = self.inspect([str(Path(device["sdk"]) / "platform-tools/adb"), "-s", device["serial"], "emu", "avd", "name"])
        if name.splitlines()[0] != device["name"]:
            raise smoke.SmokeError("owned emulator identity changed")

    def ios_device(self) -> str:
        self.ios_name = "oversqlite-" + uuid.uuid4().hex[:12]
        self.record["ios_creation_name"] = self.ios_name
        self.record["process_census_policy"] = "owned iOS: direct groups registered continuously; full tagged census after exact simulator deletion"
        (self.output / "ownership.json").write_text(json.dumps(self.record, indent=2) + "\n")
        self.ios = self.inspect(["xcrun", "simctl", "create", self.ios_name,
                                 "com.apple.CoreSimulator.SimDeviceType.iPhone-18-Pro", "com.apple.CoreSimulator.SimRuntime.iOS-27-0"], 60)
        if not re.fullmatch(r"[0-9A-Fa-f-]{36}", self.ios):
            self.ios = None
            raise smoke.SmokeError("invalid simulator create identity; reconcile by owned name")
        self.record["ios_udid"] = self.ios
        (self.output / "ownership.json").write_text(json.dumps(self.record, indent=2) + "\n")
        self.inspect(["xcrun", "simctl", "boot", self.ios], 30)
        # Global ps can block while the fresh simulator is alive. Retain all
        # launched group identities and tokens; cleanup deletes the exact owned
        # simulator before recovering and joining escaped tagged descendants.
        self.command(["xcrun", "simctl", "bootstatus", self.ios, "-b"], 120, "ios-boot", capture=False)
        return self.ios

    def owned_simulators(self, seconds: float) -> list[dict]:
        devices = json.loads(self.inspect(["xcrun", "simctl", "list", "devices", "--json"], seconds))
        return [device for family in devices["devices"].values() for device in family
                if device.get("name") == self.ios_name]

    def stop_groups(self, deadline: float, errors: list[str]) -> None:
        """Join every currently discovered group without extending cleanup."""
        denied = {}
        for group in sorted(self.groups):
            if not group_alive(group):
                continue
            try:
                # Signalling a registered group must remain possible after the
                # budget expires. Only waiting is budgeted; an exhausted clock
                # must not turn a timeout into a leaked owned process.
                os.killpg(group, signal.SIGTERM)
            except ProcessLookupError:
                pass
            except PermissionError as error:
                denied[group] = str(error)
            except Exception as error:
                errors.append(str(error))
        soft = min(deadline - 2, time.monotonic() + 12)
        while time.monotonic() < soft:
            for child in self.children:
                child.proc.poll()
            if not any(group_alive(group) for group in self.groups):
                break
            time.sleep(.05)
        for group in self.groups:
            if not group_alive(group):
                if group in denied:
                    self.record.setdefault('resolved_signal_races', []).append(dict(
                        group=group, signal='SIGTERM', error=denied[group], group_absent=True))
                continue
            if group in denied:
                errors.append(f"owned group SIGTERM permission denied: {group}: {denied[group]}")
            try:
                os.killpg(group, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except Exception as error:
                errors.append(str(error))
            errors.append(f"owned group required SIGKILL: {group}")

    def cleanup(self) -> dict:
        start = time.monotonic()
        self.command_deadline = None
        deadline = min(start + 30, self.cleanup_deadline) if self.cleanup_deadline is not None else start + 30
        errors: list[str] = []
        def attempt(action):
            try:
                return action()
            except Exception as error:
                errors.append(smoke.redact(str(error), self.env))
        for index,observer in enumerate(self.observers):
            attempt(lambda observer=observer:observer.stop(deadline))
            report=observer.report();path=observer.output/'observation.json'
            attempt(lambda path=path,report=report:path.write_text(json.dumps(report,indent=2)+'\n'))
            attempt(lambda index=index,path=path,observer=observer:self.record['http_observers'][index].update(
                observation=dict(path=str(path),sha256=smoke.sha(path.read_bytes()),stopped=observer.stopped)))
            if report['errors']:errors.append('owned HTTP observer recorded failed requests')
        if self.ios_name is None:
            attempt(lambda: self.capture_groups(deadline))
        if self.record.get('go_fixture_observation'):
            attempt(lambda:self.capture_go_fixtures(deadline))
        # Graceful authority termination drops its owned synthetic database.
        attempt(lambda: self.stop_groups(deadline, errors))
        if self.ios_name:
            matches = attempt(lambda: self.owned_simulators(remaining(deadline, 3)))
            if matches is not None:
                for device in matches:
                    if self.ios and device["udid"] != self.ios:
                        errors.append("simulator creation identity mismatch")
                        continue
                    udid = device["udid"]
                    if device.get("state") != "Shutdown":
                        attempt(lambda udid=udid: self.inspect(["xcrun", "simctl", "shutdown", udid], remaining(deadline, 8)))
                    attempt(lambda udid=udid: self.inspect(["xcrun", "simctl", "delete", udid], remaining(deadline, 8)))
            matches = attempt(lambda: self.owned_simulators(remaining(deadline, 3)))
            if matches is None or matches:
                errors.append("owned iOS simulator absence unproved")
        if self.ios_name:
            # Device deletion precedes global inspection; recover even reparented
            # groups whose direct parent exited between normal ownership polls.
            attempt(lambda: self.capture_groups(deadline))
            attempt(lambda: self.stop_groups(deadline, errors))
        for child in self.children:
            attempt(lambda child=child: child.proc.wait(timeout=remaining(deadline, 2)))
            attempt(lambda child=child: child.reader.join(timeout=remaining(deadline, 2)))
            if child.reader.is_alive() or child.error:
                errors.append("owned log capture incomplete")
        if self.record.get('go_fixture_observation'):
            attempt(lambda:self.cleanup_go_fixtures(deadline))
        if self.android:
            adb = str(Path(self.android["sdk"]) / "platform-tools/adb")
            serial_deadline = min(deadline - 3, time.monotonic() + 5)
            devices = attempt(lambda: self.inspect([adb, "devices"], remaining(deadline, 3)))
            while devices is not None and self.android["serial"] in devices and time.monotonic() < serial_deadline:
                time.sleep(.1)
                devices = attempt(lambda: self.inspect([adb, "devices"], remaining(deadline, 3)))
            if devices is None or self.android["serial"] in devices:
                errors.append("owned Android serial remains; preserve private AVD")
            else:
                # Only a directory made inside this invocation's new output.
                shutil.rmtree(self.android["private"] + "/avd", ignore_errors=False)
        if self.container:
            identity=attempt(lambda:self.docker_owned('container',self.container,remaining(deadline,3)))
            if identity:
                names = attempt(lambda: self.sql("SELECT datname FROM pg_database WHERE datname NOT IN ('postgres','template0','template1')", seconds=remaining(deadline, 3)))
                if names:
                    errors.append("fixture database cleanup incomplete")
                attempt(lambda: self.inspect(["docker", "rm", "-fv", identity], remaining(deadline, 5)))
        if self.volume:
            identity=attempt(lambda:self.docker_owned('volume',self.volume,remaining(deadline,3)))
            if identity:
                attempt(lambda: self.inspect(["docker", "volume", "rm", identity], remaining(deadline, 4)))
        for resource in ("container", "volume"):
            extra = ["--all"] if resource == "container" else []
            values = attempt(lambda resource=resource, extra=extra: self.inspect(["docker", resource, "ls", *extra, "--filter", "label=" + LABEL + "=" + self.run_id, "--format", "{{.ID}}" if resource == "container" else "{{.Name}}"], remaining(deadline, 3)))
            if values is None or values:
                errors.append(f"owned {resource} absence unproved")
        attempt(lambda: self.capture_groups(deadline))
        for probe in self.inspections:
            attempt(lambda probe=probe:probe.proc.wait(timeout=remaining(deadline,1)))
            for reader in probe.readers:
                if reader.ident is not None:
                    attempt(lambda reader=reader:reader.join(timeout=remaining(deadline,1)))
                else:
                    errors.append('owned inspection reader never started')
            if any(reader.is_alive()for reader in probe.readers)or probe.errors:
                errors.append('owned inspection capture incomplete')
        absent = not any(group_alive(group) for group in self.groups)
        if not absent:
            errors.append("owned process group remains")
        if time.monotonic() > deadline:
            errors.append("shared30s cleanup exceeded")
        for fd,lease in getattr(self,'exclusive_leases',[]):
            try:
                os.close(fd)  # Closing the owning descriptor releases its flock.
                lease['released']=True
            except OSError as error:
                errors.append('exclusive lease release: '+str(error))
        self.exclusive_leases=[]
        return dict(errors=errors, elapsed_seconds=round(time.monotonic() - start, 3), groups_absent=absent,
                    groups=sorted(self.groups), resources=self.record)


# Fixed read-only reconciliation of the current retained audit contract.
RETAINED_POSTGRES_INSPECTION = "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;\nSELECT json_build_object(\n 'business_rows',(SELECT count(*) FROM business.categories),\n 'recovered_business_rows',(SELECT count(*) FROM business.categories WHERE name='recovered'),\n 'business_parent_orphans',(SELECT count(*) FROM business.categories c LEFT JOIN business.categories p ON p.id=c.parent_id AND p._sync_scope_id=c._sync_scope_id WHERE c.parent_id IS NOT NULL AND p.id IS NULL),\n 'row_state_rows',(SELECT count(*) FROM sync.row_state),\n 'row_state_deleted',(SELECT count(*) FROM sync.row_state WHERE deleted),\n 'bundle_count',(SELECT count(*) FROM sync.bundle_log),\n 'effect_count',(SELECT count(*) FROM sync.bundle_rows),\n 'bundle_seq_min',(SELECT min(bundle_seq) FROM sync.bundle_log),\n 'bundle_seq_max',(SELECT max(bundle_seq) FROM sync.bundle_log),\n 'source_bundle_min',(SELECT min(source_bundle_id) FROM sync.bundle_log),\n 'source_bundle_max',(SELECT max(source_bundle_id) FROM sync.bundle_log),\n 'duplicate_replay_identities',(SELECT count(*) FROM (SELECT user_pk,source_id,source_bundle_id FROM sync.bundle_log GROUP BY 1,2,3 HAVING count(*)<>1) d),\n 'effect_parent_orphans',(SELECT count(*) FROM sync.bundle_rows r LEFT JOIN sync.bundle_log b USING(user_pk,bundle_seq) WHERE b.bundle_seq IS NULL),\n 'effect_count_mismatches',(SELECT count(*) FROM sync.bundle_log b WHERE b.row_count<>(SELECT count(*) FROM sync.bundle_rows r WHERE r.user_pk=b.user_pk AND r.bundle_seq=b.bundle_seq)),\n 'effect_ordinal_gaps',(SELECT count(*) FROM (SELECT user_pk,bundle_seq FROM sync.bundle_rows GROUP BY 1,2 HAVING min(row_ordinal)<>1 OR max(row_ordinal)<>count(*)) d),\n 'binding_orphans',(SELECT count(*) FROM sync.source_generation_bindings b LEFT JOIN sync.source_state s USING(user_pk,source_id) WHERE s.source_id IS NULL),\n 'outcomes',(SELECT count(*) FROM sync.push_outcomes),\n 'duplicate_outcome_sources',(SELECT count(*) FROM (SELECT user_pk,source_id FROM sync.push_outcomes GROUP BY 1,2 HAVING count(*)>1) d),\n 'progress',(SELECT json_agg(p) FROM sync.push_source_progress p),\n 'snapshot_sessions',(SELECT count(*) FROM sync.snapshot_sessions),\n 'snapshot_rows',(SELECT count(*) FROM sync.snapshot_session_rows),\n 'capture_stage',(SELECT count(*) FROM sync.bundle_capture_stage),\n 'server_reservations',(SELECT count(*) FROM sync.server_source_reservations),\n 'write_receipts',(SELECT count(*) FROM sync.scope_write_receipts),\n 'sources',(SELECT json_agg(row_to_json(s)) FROM sync.source_state s),\n 'user_state',(SELECT json_agg(row_to_json(s)) FROM sync.user_state s),\n 'constraints_validated',(SELECT bool_and(convalidated) FROM pg_constraint WHERE connamespace IN ('sync'::regnamespace,'business'::regnamespace)),\n 'database_bytes',pg_database_size(current_database())\n);\nCOMMIT;\n"

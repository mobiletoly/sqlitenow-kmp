#!/usr/bin/env python3
"""Collect read-only ADB diagnostics or analyze saved SQLiteNow logger output (stdlib only)."""
from __future__ import annotations

import argparse
import base64
from collections import defaultdict
import json
import re
import html
import os
from pathlib import Path
import selectors
import subprocess
import sys
import time

MARKER = 'SQLITENOW_DIAG '
CAPTURE_MARKER = 'SQLITENOW_CAPTURE '


def normalize(lines):
    events, fragments, issues, seen = [], {}, [], {}
    previous_pid = None
    for number, line in enumerate(lines, 1):
        if CAPTURE_MARKER in line:
            try:
                metadata = json.loads(line.split(CAPTURE_MARKER, 1)[1])
                if metadata.get('kind') == 'process_changed':
                    issues.append({'kind': 'process_restart', 'line': number})
            except (ValueError, TypeError):
                issues.append({'kind': 'malformed_capture_metadata', 'line': number})
            continue
        if MARKER in line:
            pid_match = re.match(r'^\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+\s+(\d+)\s+\d+\s+', line)
            if pid_match:
                current_pid = pid_match.group(1)
                if previous_pid is not None and previous_pid != current_pid:
                    issues.append({'kind': 'process_restart_or_multiple_processes', 'line': number})
                previous_pid = current_pid
            raw_message = line.split(MARKER, 1)[1]
        elif line.lstrip().startswith('{'):
            raw_message = line
        else:
            continue
        try:
            event = json.loads(raw_message)
            if not isinstance(event, dict):
                raise ValueError('object required')
            if event.get('v') != 1:
                issues.append({'kind': 'unsupported_version', 'line': number, 'version': event.get('v')})
                continue
            key = (event['capture'], event['seq'])
            if not isinstance(key[0], str) or type(key[1]) is not int or key[1] < 1:
                raise ValueError('invalid identity')
            if 'part' in event:
                part, parts = event['part'], event['parts']
                if type(part) is not int or type(parts) is not int or not 0 <= part < parts <= 1024 or event.get('encoding') != 'base64':
                    raise ValueError('invalid fragment')
                group = fragments.setdefault(key, {'parts': parts, 'data': {}})
                if group['parts'] != parts or part in group['data']:
                    issues.append({'kind': 'duplicate_or_inconsistent_fragment', 'capture': key[0], 'seq': key[1]})
                    continue
                group['data'][part] = event['data']
                if len(group['data']) != parts:
                    continue
                raw = base64.b64decode(''.join(group['data'][i] for i in range(parts)), validate=True)
                if len(raw) > 2_000_000:
                    raise ValueError('logical record too large')
                event = json.loads(raw.decode('utf-8'))
                del fragments[key]
                if event.get('v') != 1 or (event.get('capture'), event.get('seq')) != key or 'part' in event:
                    raise ValueError('fragment identity mismatch')
            if event.get('kind') not in {'span_start', 'span_end', 'event', 'sql_family', 'sql_metric', 'work_metric', 'resource_metric'}:
                raise ValueError('unknown record kind')
            if type(event.get('span')) is not int or type(event.get('t_ns')) is not int or event['t_ns'] < 0:
                raise ValueError('invalid span/timestamp')
            if event.get('kind') == 'span_end' and (type(event.get('elapsed_ns')) is not int or event['elapsed_ns'] < 0):
                raise ValueError('invalid elapsed time')
            if event.get('parent') is not None and type(event['parent']) is not int:
                raise ValueError('invalid parent')
            if not isinstance(event.get('outcome'), str):
                raise ValueError('invalid outcome')
            if not isinstance(event.get('name'), str) or not isinstance(event.get('counts'), dict):
                raise ValueError('invalid name/counts')
            if any(not isinstance(k, str) or type(v) is not int for k, v in event['counts'].items()):
                raise ValueError('invalid counter')
            if key in seen:
                issues.append({'kind': 'duplicate_event' if seen[key] == event else 'inconsistent_event', 'capture': key[0], 'seq': key[1]})
                continue
            seen[key] = event
            events.append(event)
        except (ValueError, TypeError, KeyError, UnicodeError):
            issues.append({'kind': 'malformed_record', 'line': number})
    for capture, seq in fragments:
        issues.append({'kind': 'missing_fragments', 'capture': capture, 'seq': seq})
    # Sequences restore interleaved/multi-fragment logger ordering. Captures have independent clocks.
    order = {capture: i for i, capture in enumerate(dict.fromkeys(e['capture'] for e in events))}
    events.sort(key=lambda e: (order[e['capture']], e['seq']))
    return events, issues


def union_duration(intervals):
    total, right = 0, None
    for start, end in sorted(intervals):
        if end < start:
            continue
        if right is None or start > right:
            total += end - start
        elif end > right:
            total += end - right
        right = max(end, right if right is not None else end)
    return total


def analyze(events, issues=()):
    issues = list(issues)
    captures = defaultdict(list)
    for event in events:
        captures[event['capture']].append(event)
    operations, sql, work, checks = [], [], [], []
    if not events:
        issues.append({'kind': 'no_diagnostic_records'})
    for capture, records in captures.items():
        sequences = sorted(e['seq'] for e in records)
        missing_count = sequences[-1] - len(set(sequences))
        if sequences[0] != 1 or missing_count:
            issues.append({'kind': 'sequence_gap', 'capture': capture, 'missing_count': missing_count})
        if any(e.get('multiple_connections') for e in records):
            issues.append({'kind': 'connection_attribution_partial', 'capture': capture})
        if any(e.get('lost_spans', 0) or (e.get('family_overflow_calls', 0) or e.get('lost_families', 0)) for e in records):
            issues.append({'kind': 'detail_overflow', 'capture': capture})
        starts, ends, families = {}, {}, {}
        for e in records:
            kind = e['kind']
            if kind == 'span_start':
                if e['span'] in starts:
                    issues.append({'kind': 'duplicate_span_start', 'capture': capture, 'span': e['span']})
                starts[e['span']] = e
            elif kind == 'span_end':
                if e['span'] in ends:
                    issues.append({'kind': 'duplicate_span_end', 'capture': capture, 'span': e['span']})
                ends[e['span']] = e
            elif kind == 'sql_family':
                families[e.get('family')] = e['name']
            elif kind == 'event':
                checks.append({k: e.get(k) for k in ('capture', 'span', 'name', 'outcome', 'counts', 'count_basis', 'schema', 'table', 'attempt', 'slice')})
        if not any(e.get('parent') is None for e in starts.values()):
            issues.append({'kind': 'missing_operation_root', 'capture': capture})
        for e in records:
            if e['kind'] not in {'span_start', 'span_end'} and e['span'] not in starts:
                issues.append({'kind': 'missing_span_context', 'capture': capture, 'span': e['span']})
        spans = []
        for span_id in starts.keys() | ends.keys():
            start, end = starts.get(span_id), ends.get(span_id)
            if start is None or end is None:
                issues.append({'kind': 'incomplete_span', 'capture': capture, 'span': span_id})
                continue
            # Prefer explicit elapsed time so timestamp/formatter bookkeeping is not counted twice.
            last = end['t_ns']
            elapsed = end.get('elapsed_ns', last - start['t_ns'])
            first = last - elapsed
            if elapsed < 0 or first < 0:
                issues.append({'kind': 'invalid_interval', 'capture': capture, 'span': span_id})
                continue
            parent = start.get('parent')
            if parent is not None and parent not in starts:
                issues.append({'kind': 'missing_parent', 'capture': capture, 'span': span_id})
            spans.append({'id': span_id, 'parent': parent, 'name': start['name'], 'start_ns': first,
                          'end_ns': last, 'inclusive_ns': elapsed, 'outcome': end['outcome'], 'count_basis': end.get('count_basis'), 'counts': end['counts']})
        by_id = {s['id']: s for s in spans}
        for span in spans:
            visited, cursor = set(), span['id']
            while cursor in by_id:
                if cursor in visited:
                    issues.append({'kind': 'parent_cycle', 'capture': capture, 'span': span['id']})
                    break
                visited.add(cursor); cursor = by_id[cursor]['parent']
            parent = by_id.get(span['parent'])
            if parent and (span['start_ns'] < parent['start_ns'] or span['end_ns'] > parent['end_ns']):
                issues.append({'kind': 'child_outside_parent', 'capture': capture, 'span': span['id']})
            children = [(max(span['start_ns'], c['start_ns']), min(span['end_ns'], c['end_ns']))
                        for c in spans if c['parent'] == span['id']]
            span['child_covered_ns'] = union_duration(children)
            span['exclusive_ns'] = max(0, span['inclusive_ns'] - span['child_covered_ns'])
            span['unattributed_ns'] = span['exclusive_ns']
        roots = {s['id'] for s in spans if s['parent'] is None}
        operations.append({'capture': capture, 'connection': next((e.get('connection') for e in records if e.get('connection')), None),
                           'spans': sorted(spans, key=lambda s: (s['start_ns'], s['id']))})
        for e in records:
            if e['kind'] not in {'sql_metric', 'work_metric'}:
                continue
            if e.get('family') not in families and e.get('family') != 'overflow':
                issues.append({'kind': 'missing_sql_family', 'capture': capture, 'span': e['span']})
            metric = {'capture': capture, 'span': e['span'], 'operation_total': e['span'] in roots,
                      'family': e.get('family'), 'label': families.get(e.get('family'), e.get('family')),
                      'action': e['name'], **e['counts']}
            (work if e['kind'] == 'work_metric' else sql).append(metric)
    root_sql = [m for m in sql if m['operation_total']]
    hits = sum(m.get('calls', 0) for m in root_sql if m['action'] == 'cache_hit')
    misses = sum(m.get('calls', 0) for m in root_sql if m['action'] == 'cache_miss')
    return {'schema_version': 1, 'complete': not issues, 'issues': issues, 'operations': operations,
            'sql_metrics': sorted(sql, key=lambda m: m.get('total_ns', 0), reverse=True),
            'work_metrics': sorted(work, key=lambda m: m.get('total_ns', 0), reverse=True),
            'cache': {'hits': hits, 'misses': misses, 'hit_ratio': hits / (hits + misses) if hits + misses else None},
            'checks': checks,
            'notes': ['Span/work timings are inclusive; sibling overlap is unioned for exclusive span time.',
                      'Unattributed time includes orchestration, uninstrumented work and diagnostic overhead.',
                      'Root SQL metrics include descendants; do not add root and child metrics together.',
                      'Step calls/returned rows are not VM instructions/rows scanned. HTTP time is not server CPU.',
                      'An observed successful check describes that check only; absent events are not correctness proof.']}


def markdown(report):
    lines = ['# SQLiteNow diagnostics', '', 'Capture: ' + ('complete' if report['complete'] else 'incomplete'), '']
    if report['issues']:
        lines += ['Issues: ' + ', '.join(sorted({i['kind'] for i in report['issues']})), '']
    lines += ['| Capture / phase | Outcome | Inclusive ms | Exclusive / unattributed ms |', '| --- | --- | ---: | ---: |']
    for operation in report['operations']:
        for span in operation['spans']:
            name = html.escape(str(span['name'])).replace('|', '\\|').replace('\n', ' ')
            lines.append(f"| {operation['capture']} / {name} | {span['outcome']} | {span['inclusive_ns']/1e6:.3f} | {span['exclusive_ns']/1e6:.3f} |")
    lines += ['', '| SQL family / action (operation totals) | Calls | Returned rows | Total ms | Max ms |', '| --- | ---: | ---: | ---: | ---: |']
    for m in report['sql_metrics']:
        if m['operation_total']:
            label = html.escape(str(m['label'])).replace('|', '\\|').replace('\n', ' ')
            lines.append(f"| {label} / {m['action']} | {m.get('calls',0)} | {m.get('returned_rows',0)} | {m.get('total_ns',0)/1e6:.3f} | {m.get('max_ns',0)/1e6:.3f} |")
    lines += ['', '| Aggregated work (operation totals; inclusive) | Calls | Total ms |', '| --- | ---: | ---: |']
    for m in report['work_metrics']:
        if m['operation_total']:
            lines.append(f"| {m['label']} | {m.get('calls',0)} | {m.get('total_ns',0)/1e6:.3f} |")
    lines += ['', '| Existing checks / transitions | Outcome | Counts and basis |', '| --- | --- | --- |']
    for check in report['checks']:
        if not any(term in check['name'] for term in ('check', 'guard', 'committed', 'transition', 'retry', 'work_slice', 'resources')):
            continue
        label = html.escape(check['name']).replace('|', '\\|')
        entity = '.'.join(str(check.get(key) or '') for key in ('schema','table')).strip('.')
        if entity:
            label += ' (' + html.escape(entity).replace('|', '\\|') + ')'
        counts = html.escape(json.dumps(check.get('counts') or {}, sort_keys=True)).replace('|', '\\|')
        lines.append(f"| {label} | {check['outcome']} | {counts}; {check.get('count_basis')} |")
    lines += ['', f"Cache hits: {report['cache']['hits']}; misses: {report['cache']['misses']}.", '']
    lines += [f'- {note}' for note in report['notes']]
    return '\n'.join(lines) + '\n'


def capture(args):
    if not re.fullmatch(r'[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)+', args.package):
        raise RuntimeError('Invalid Android package name.')
    output = Path(args.output)
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    target = [args.adb, '-s', args.serial]
    def pid():
        result = subprocess.run(target + ['shell', 'pidof', '-s', args.package], capture_output=True, text=True, check=False)
        return sorted(result.stdout.strip().split()) if result.returncode == 0 else []
    selected = pid()
    if not selected:
        raise RuntimeError('App is not running; collector will not start or restart it.')
    (output/'metadata.json').write_text(json.dumps({'serial': args.serial, 'package': args.package, 'pids': selected}, indent=2)+'\n')
    os.chmod(output/'metadata.json', 0o600)
    started, process = time.monotonic(), None
    try:
        with (output/'capture.log').open('x', encoding='utf-8') as sink:
            os.chmod(output/'capture.log', 0o600)
            while not args.duration or time.monotonic() - started < args.duration:
                if not selected:
                    time.sleep(0.25)
                    selected = pid()
                    continue
                process = subprocess.Popen(target + ['logcat', '-v', 'threadtime', '--pid=' + ','.join(selected)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                selector = selectors.DefaultSelector()
                selector.register(process.stdout, selectors.EVENT_READ)
                pending = b''
                try:
                    while not args.duration or time.monotonic() - started < args.duration:
                        for key, _ in selector.select(timeout=0.25):
                            chunk = os.read(key.fileobj.fileno(), 65536)
                            if not chunk:
                                raise RuntimeError('ADB logcat ended; capture is incomplete.')
                            pending += chunk
                            while b'\n' in pending:
                                raw, pending = pending.split(b'\n', 1)
                                line = raw.decode('utf-8', errors='replace')
                                if MARKER in line:
                                    sink.write(line+'\n'); sink.flush()
                            if len(pending) > 1_000_000:
                                raise RuntimeError('Oversized unterminated logger record.')
                        current = pid()
                        if current != selected:
                            sink.write(CAPTURE_MARKER + json.dumps({'kind': 'process_changed', 'old_pids': selected, 'pids': current})+'\n')
                            sink.flush(); selected = current; break
                finally:
                    if MARKER.encode() in pending:
                        sink.write(pending.decode('utf-8', errors='replace')+'\n'); sink.flush()
                    selector.close()
                    process.terminate()
                    try: process.wait(timeout=3)
                    except subprocess.TimeoutExpired: process.kill(); process.wait()
    except KeyboardInterrupt:
        pass
    return output/'capture.log'


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    collect = commands.add_parser('capture')
    collect.add_argument('--serial', required=True); collect.add_argument('--package', required=True)
    collect.add_argument('--adb', default='adb'); collect.add_argument('--output', required=True)
    collect.add_argument('--duration', type=float, default=0, help='seconds; zero follows until Ctrl-C')
    inspect = commands.add_parser('analyze')
    inspect.add_argument('input'); inspect.add_argument('--output', required=True)
    args = parser.parse_args(argv)
    if args.command == 'capture':
        print(capture(args)); return 0
    output = Path(args.output); output.mkdir(parents=True, exist_ok=True)
    with Path(args.input).open(encoding='utf-8', errors='replace') as source:
        events, issues = normalize(source)
    report = analyze(events, issues)
    (output/'events.jsonl').write_text(''.join(json.dumps(e, ensure_ascii=False)+'\n' for e in events))
    (output/'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')
    (output/'report.md').write_text(markdown(report))
    print(json.dumps({'complete': report['complete'], 'events': len(events), 'issues': len(report['issues'])}))
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, RuntimeError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)

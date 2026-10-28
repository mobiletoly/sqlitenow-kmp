import base64
import importlib.util
import json
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('sqlitenow_diagnostics', Path(__file__).resolve().parents[1]/'sqlitenow_diagnostics.py')
d = importlib.util.module_from_spec(spec)
spec.loader.exec_module(d)


def event(seq, kind='event', span=1, name='phase', t=0, parent=None, **more):
    value = dict(v=1, capture='capture', seq=seq, operation=1, span=span, kind=kind, name=name,
                 t_ns=t, outcome='success', counts={}, count_basis='observed', lost_spans=0, lost_families=0)
    if parent is not None:
        value['parent'] = parent
    value.update(more)
    return value


def log(value):
    return '10-28 14:00:00.000 123 123 I sqlitenow: '+d.MARKER+json.dumps(value, ensure_ascii=False)+'\n'


class DiagnosticAnalysisTest(unittest.TestCase):
    def test_overlapping_children_use_union_not_sum(self):
        records = [event(1,'span_start',t=0,name='root'), event(2,'span_start',2,'a',0,1),
                   event(3,'span_start',3,'b',20,1), event(4,'span_end',2,'a',70,1,elapsed_ns=70),
                   event(5,'span_end',3,'b',90,1,elapsed_ns=70), event(6,'span_end',t=100,name='root',elapsed_ns=100)]
        report = d.analyze(records)
        self.assertTrue(report['complete'])
        root = next(s for s in report['operations'][0]['spans'] if s['id'] == 1)
        self.assertEqual(90, root['child_covered_ns'])
        self.assertEqual(10, root['exclusive_ns'])

    def test_reordered_interleaved_unicode_fragments_reassemble(self):
        original = event(1,name='таблица🚀')
        encoded = base64.b64encode(json.dumps(original,ensure_ascii=False).encode()).decode()
        chunks = [encoded[:80],encoded[80:]]
        lines = [log(dict(v=1,capture='capture',seq=1,part=i,parts=2,encoding='base64',data=chunks[i])) for i in (1,0)]
        records, issues = d.normalize(lines)
        self.assertEqual([original], records)
        self.assertEqual([], issues)

    def test_missing_fragments_and_partial_capture_are_not_success(self):
        records, issues = d.normalize([log(dict(v=1,capture='capture',seq=1,part=0,parts=2,encoding='base64',data='e30='))])
        self.assertIn('missing_fragments', {i['kind'] for i in issues})
        self.assertFalse(d.analyze(records,issues)['complete'])

    def test_duplicate_inconsistent_and_malformed_records_report_loss(self):
        first = event(1)
        records, issues = d.normalize([log(first),log(first),log(event(1,name='changed')),d.MARKER+'{"v":1',log(event(2,v=9))])
        self.assertEqual(1,len(records))
        self.assertEqual({'duplicate_event','inconsistent_event','malformed_record','unsupported_version'}, {i['kind'] for i in issues})

    def test_sequence_gaps_missing_parents_overflow_and_restarts(self):
        records = [event(2,'span_start',2,parent=99), event(4,'span_end',2,t=50,elapsed_ns=50,lost_families=1)]
        normalized, issues = d.normalize([d.CAPTURE_MARKER+'{"kind":"process_changed"}\n']+[log(e) for e in records])
        report = d.analyze(normalized, issues)
        self.assertFalse(report['complete'])
        self.assertTrue({'process_restart','sequence_gap','missing_parent','detail_overflow'} <= {i['kind'] for i in report['issues']})

    def test_unsupported_version_and_no_logs_remain_incomplete(self):
        normalized, issues = d.normalize(['ordinary app line',log(event(1,v=2))])
        report = d.analyze(normalized, issues)
        self.assertFalse(report['complete'])
        self.assertEqual([], report['operations'])

    def test_root_metrics_are_not_added_to_child_metrics(self):
        records=[event(1,'span_start'),event(2,'sql_family',name='SELECT ?',family='q1'),
                 event(3,'sql_metric',name='cache_hit',family='q1',counts={'calls':4}),
                 event(4,'span_start',2,t=1,parent=1),
                 event(5,'sql_metric',2,name='cache_hit',family='q1',counts={'calls':4}),
                 event(6,'span_end',2,t=10,parent=1,elapsed_ns=9),event(7,'span_end',t=20,elapsed_ns=20)]
        report=d.analyze(records)
        self.assertTrue(report['complete'])
        self.assertEqual(4,report['cache']['hits'])
        self.assertEqual(1.0,report['cache']['hit_ratio'])

    def test_events_without_span_context_cannot_form_a_complete_capture(self):
        report=d.analyze([event(1)])
        self.assertFalse(report['complete'])
        self.assertIn('missing_operation_root',{i['kind'] for i in report['issues']})

    def test_saved_adb_pid_changes_are_reported(self):
        lines=[log(event(1)), log(event(2)).replace(' 123 123 I',' 456 456 I')]
        _,issues=d.normalize(lines)
        self.assertIn('process_restart_or_multiple_processes',{i['kind'] for i in issues})

    def test_child_outside_parent_is_not_silently_clipped_into_success(self):
        records=[event(1,'span_start'),event(2,'span_start',2,parent=1),
                 event(3,'span_end',t=10,elapsed_ns=10),event(4,'span_end',2,t=20,parent=1,elapsed_ns=20)]
        report=d.analyze(records)
        self.assertFalse(report['complete'])
        self.assertIn('child_outside_parent',{i['kind'] for i in report['issues']})

    def test_invalid_fragment_identity_is_rejected(self):
        encoded=base64.b64encode(json.dumps(event(2)).encode()).decode()
        normalized,issues=d.normalize([log(dict(v=1,capture='capture',seq=1,part=0,parts=1,encoding='base64',data=encoded))])
        self.assertFalse(normalized)
        self.assertEqual('malformed_record',issues[0]['kind'])

    def test_invalid_numeric_fields_are_rejected(self):
        for invalid in [event(True),event(1,t=-1),event(1,counts={'bad':True}),event(1,'span_end',elapsed_ns='slow')]:
            normalized,issues=d.normalize([log(invalid)])
            self.assertFalse(normalized)
            self.assertEqual('malformed_record',issues[0]['kind'])

    def test_saved_jsonl_roundtrip_and_missing_terminal(self):
        records,issues=d.normalize([json.dumps(event(1,'span_start'))])
        report=d.analyze(records,issues)
        self.assertFalse(report['complete'])
        self.assertIn('incomplete_span',{i['kind'] for i in report['issues']})
        self.assertIn('Capture: incomplete',d.markdown(report))


if __name__=='__main__': unittest.main()

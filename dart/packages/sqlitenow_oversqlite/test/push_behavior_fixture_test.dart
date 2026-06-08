import 'dart:convert';
import 'dart:io';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

import 'support/behavior_fixture_support.dart';

void main() {
  final cases = _readPushBehaviorCases();

  test('Dart shared push behavior fixtures execute against runtime', () async {
    for (final fixture in cases) {
      await _runPushCase(fixture);
    }
  });
}

Future<void> _runPushCase(Map<String, Object?> fixture) async {
  final database = await openUsersDatabase();
  final server = _newPushServer(fixture);
  final client = newUsersClient(database, server);
  try {
    await client.open();
    await client.attach('user-1');
    for (final sql
        in (fixture['localSetupSql']! as List<Object?>).cast<String>()) {
      await database.connection.execute(sql);
    }
    for (final step
        in (fixture['steps']! as List<Object?>).cast<Map<String, Object?>>()) {
      Object? error;
      try {
        switch (step['action']) {
          case 'pushPending':
            await client.pushPending();
          default:
            fail('${fixture['name']}: unknown action ${step['action']}');
        }
      } catch (caught) {
        error = caught;
      }
      _expectException(
        fixture['name']! as String,
        step['expectedException']! as String,
        error,
      );
      final expectedState = step['expectedState'];
      if (expectedState != null) {
        await _expectPushState(
          database,
          fixture['name']! as String,
          (expectedState as Map).cast<String, Object?>(),
        );
      }
      final expectedServerState = step['expectedServerState'];
      if (expectedServerState != null) {
        final expected = (expectedServerState as Map).cast<String, Object?>();
        if (expected.containsKey('createRequestCount')) {
          expect(
            server.createRequests,
            hasLength(expected['createRequestCount']! as int),
            reason: fixture['name']! as String,
          );
        }
      }
    }
  } finally {
    await client.close();
    await database.close();
  }
}

PushFixtureServer _newPushServer(Map<String, Object?> fixture) {
  final script = (fixture['serverScript']! as Map).cast<String, Object?>();
  switch (script['kind']) {
    case 'already_committed_mismatch':
      final payload = (script['committedPayload']! as Map)
          .cast<String, Object?>();
      return PushFixtureServer(
        alreadyCommitted: true,
        createResponse: {
          'status': 'already_committed',
          'bundle_seq': 1,
          'source_id': 'source-fixture',
          'source_bundle_id': 1,
          'row_count': 1,
          'bundle_hash': 'fixture-already-committed-mismatch',
        },
        committedRowsResponse: _committedRowsResponse(payload),
      );
    case 'committed_remote_mismatch':
      return PushFixtureServer(
        commitResponse: (script['commitResponse']! as Map)
            .cast<String, Object?>(),
        committedRowsResponse: (script['committedRowsResponse']! as Map)
            .cast<String, Object?>(),
      );
    case 'committed_replay_pruned':
      return PushFixtureServer(pruneFirstCommittedFetch: true);
    case 'precommit_retry':
      return PushFixtureServer(failFirstCommit: true);
    default:
      fail('${fixture['name']}: unknown server script ${script['kind']}');
  }
}

Map<String, Object?> _committedRowsResponse(Map<String, Object?> payload) {
  return {
    'bundle_seq': 1,
    'source_id': 'source-fixture',
    'source_bundle_id': 1,
    'row_count': 1,
    'bundle_hash': 'fixture-already-committed-mismatch',
    'rows': [
      {
        'schema': 'main',
        'table': 'users',
        'key': {'id': 'user-1'},
        'op': 'INSERT',
        'row_version': 1,
        'payload': payload,
      },
    ],
    'next_row_ordinal': 0,
    'has_more': false,
  };
}

Future<void> _expectPushState(
  SqliteNowDatabase database,
  String name,
  Map<String, Object?> expected,
) async {
  if (expected.containsKey('rebuildRequired')) {
    expect(
      await scalarInt(
        database,
        'SELECT rebuild_required FROM _sync_attachment_state',
      ),
      expected['rebuildRequired'] == true ? 1 : 0,
      reason: '$name rebuildRequired',
    );
  }
  if (expected.containsKey('outboxState')) {
    expect(
      await scalarText(
        database,
        'SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1',
      ),
      expected['outboxState'],
      reason: '$name outboxState',
    );
  }
  if (expected.containsKey('outboxRowCount')) {
    expect(
      await scalarInt(database, 'SELECT COUNT(*) FROM _sync_outbox_rows'),
      expected['outboxRowCount'],
      reason: '$name outboxRowCount',
    );
  }
  if (expected.containsKey('dirtyRowCount')) {
    expect(
      await scalarInt(database, 'SELECT COUNT(*) FROM _sync_dirty_rows'),
      expected['dirtyRowCount'],
      reason: '$name dirtyRowCount',
    );
  }
  if (expected.containsKey('nextSourceBundleId')) {
    expect(
      await scalarInt(
        database,
        'SELECT next_source_bundle_id FROM _sync_source_state',
      ),
      expected['nextSourceBundleId'],
      reason: '$name nextSourceBundleId',
    );
  }
}

void _expectException(String name, String expected, Object? error) {
  switch (expected) {
    case 'none':
      expect(error, isNull, reason: name);
    case 'source_sequence_mismatch':
      expect(error, isA<SourceSequenceMismatchException>(), reason: name);
    case 'rebuild_required':
      expect(error, isA<RebuildRequiredException>(), reason: name);
    case 'http_error':
      expect(error, isA<OversqliteHttpException>(), reason: name);
    default:
      fail('$name: unknown expected exception $expected');
  }
}

List<Map<String, Object?>> _readPushBehaviorCases() {
  final file = repoRoot().uri
      .resolve('oversqlite-contracts/push/behavior/recovery.json')
      .toFilePath();
  final raw = jsonDecode(File(file).readAsStringSync()) as Map<String, Object?>;
  return (raw['cases']! as List<Object?>).cast<Map<String, Object?>>();
}

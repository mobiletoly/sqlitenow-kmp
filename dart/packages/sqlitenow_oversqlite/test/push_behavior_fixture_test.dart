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
  final database = await openBehaviorDatabase(
    fixture['schema'] as String? ?? 'users',
  );
  final server = _newPushServer(fixture)
    ..registeredTableSpecs = phase4RegisteredTableSpecsForConfig(
      behaviorConfig(fixture),
    );
  final client = newBehaviorClient(
    database,
    server,
    fixture,
    resolver: _resolverFor(fixture['resolver'] as String? ?? 'server_wins'),
  );
  try {
    await client.open();
    await client.attach('user-1');
    await executeSetupSql(
      database,
      (fixture['localSetupSql'] as List<Object?>? ?? const []).cast<String>(),
    );
    await executeApplyModeSql(
      database,
      (fixture['localApplyModeSql'] as List<Object?>? ?? const [])
          .cast<String>(),
    );
    for (final step
        in (fixture['steps']! as List<Object?>).cast<Map<String, Object?>>()) {
      Object? error;
      try {
        switch (step['action']) {
          case 'pushPending':
            await client.pushPending();
          case 'localSql':
            await executeSetupSql(
              database,
              (step['sql'] as List<Object?>? ?? const []).cast<String>(),
            );
          case 'applyModeSql':
            await executeApplyModeSql(
              database,
              (step['sql'] as List<Object?>? ?? const []).cast<String>(),
            );
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
      final expectedAppState = step['expectedAppState'];
      if (expectedAppState != null) {
        await expectAppState(
          database,
          fixture['name']! as String,
          (expectedAppState as Map).cast<String, Object?>(),
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
        if (expected.containsKey('uploadedRows')) {
          expect(
            server.uploadedRows.map(_uploadedRowSummary).toList(),
            expected['uploadedRows'],
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
    case 'echo_committed_rows':
      return PushFixtureServer();
    case 'committed_bundle_seq_gap':
      return PushFixtureServer(committedBundleSeq: script['bundleSeq']! as int);
    case 'committed_replay_first_fetch_http_error':
      return PushFixtureServer(failFirstCommittedFetch: true);
    case 'committed_replay_bad_bundle_hash':
      return PushFixtureServer(badCommittedBundleHash: true);
    case 'committed_bundle_not_found_then_success':
      return PushFixtureServer(
        committedBundleNotFoundFailures: (script['notFoundCount'] as int?) ?? 1,
      );
    case 'already_committed_request_hash_mismatch':
      final payload = (script['committedPayload']! as Map)
          .cast<String, Object?>();
      final committedRows = [_committedRow(payload)];
      final committedHash = committedBundleHashForFixtureRows(committedRows);
      return PushFixtureServer(
        alreadyCommitted: true,
        createResponse: {
          'status': 'already_committed',
          'bundle_seq': 1,
          'source_id': 'source-fixture',
          'source_bundle_id': 1,
          'row_count': 1,
          'bundle_hash': committedHash,
          'canonical_request_hash':
              'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
        },
        committedRowsResponse: _committedRowsResponse(committedRows),
      );
    case 'already_committed_rows':
      final committedRows = (script['committedRows']! as List<Object?>)
          .cast<Map<String, Object?>>()
          .map((row) => row.cast<String, Object?>())
          .toList();
      final committedHash = committedBundleHashForFixtureRows(committedRows);
      return PushFixtureServer(
        alreadyCommitted: true,
        createResponse: {
          'status': 'already_committed',
          'bundle_seq': 1,
          'source_id': 'source-fixture',
          'source_bundle_id': 1,
          'row_count': committedRows.length,
          'bundle_hash': committedHash,
        },
        committedRowsResponse: _committedRowsResponse(committedRows),
      );
    case 'committed_remote_request_hash_mismatch':
      final committedRows =
          ((script['committedRowsResponse']! as Map)['rows']! as List<Object?>)
              .cast<Map<String, Object?>>()
              .map((row) => row.cast<String, Object?>())
              .toList();
      final committedHash = committedBundleHashForFixtureRows(committedRows);
      final commitResponse = (script['commitResponse']! as Map)
          .cast<String, Object?>();
      final rowsResponse = (script['committedRowsResponse']! as Map)
          .cast<String, Object?>();
      return PushFixtureServer(
        commitResponse: {...commitResponse, 'bundle_hash': committedHash},
        committedRowsResponse: {
          ...rowsResponse,
          'bundle_hash': committedHash,
          'canonical_request_hash':
              'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
        },
      );
    case 'committed_replay_pruned':
      return PushFixtureServer(pruneFirstCommittedFetch: true);
    case 'precommit_retry':
      return PushFixtureServer(failFirstCommit: true);
    case 'conflict_once':
      return PushFixtureServer(
        conflictOnce: (script['conflict']! as Map).cast<String, Object?>(),
      );
    case 'conflict_once_on_row_count':
      return PushFixtureServer(
        conflictOnce: (script['conflict']! as Map).cast<String, Object?>(),
        conflictOnceOnRowCount: script['rowCount']! as int,
      );
    default:
      fail('${fixture['name']}: unknown server script ${script['kind']}');
  }
}

Resolver _resolverFor(String name) {
  switch (name) {
    case 'server_wins':
      return const ServerWinsResolver();
    case 'client_wins':
      return const ClientWinsResolver();
    case 'keep_local':
      return const _KeepLocalResolver();
    case 'keep_merged_user':
      return const _MergedUserResolver();
    default:
      throw StateError('unknown resolver $name');
  }
}

String _uploadedRowSummary(Map<String, Object?> row) {
  final key = (row['key']! as Map).cast<String, Object?>();
  final id = key['id'] ?? key.values.first;
  return '${row['table']}:${row['op']}:$id';
}

Map<String, Object?> _committedRow(Map<String, Object?> payload) {
  return {
    'schema': 'main',
    'table': 'users',
    'key': {'id': 'user-1'},
    'op': 'INSERT',
    'row_version': 1,
    'payload': payload,
  };
}

Map<String, Object?> _committedRowsResponse(List<Map<String, Object?>> rows) {
  final committedHash = committedBundleHashForFixtureRows(rows);
  return {
    'bundle_seq': 1,
    'source_id': 'source-fixture',
    'source_bundle_id': 1,
    'row_count': rows.length,
    'bundle_hash': committedHash,
    'rows': rows,
    'next_row_ordinal': rows.length - 1,
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
  if (expected.containsKey('lastBundleSeqSeen')) {
    expect(
      await scalarInt(
        database,
        'SELECT last_bundle_seq_seen FROM _sync_attachment_state',
      ),
      expected['lastBundleSeqSeen'],
      reason: '$name lastBundleSeqSeen',
    );
  }
  if (expected.containsKey('applyMode')) {
    expect(
      await scalarInt(database, 'SELECT apply_mode FROM _sync_apply_state'),
      expected['applyMode'],
      reason: '$name applyMode',
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
    case 'protocol_error':
      expect(
        error,
        anyOf(
          isA<OversqliteProtocolException>(),
          isA<ArgumentError>(),
          isA<InvalidConflictResolutionException>(),
        ),
        reason: name,
      );
    case 'http_error':
      expect(error, isA<OversqliteHttpException>(), reason: name);
    default:
      fail('$name: unknown expected exception $expected');
  }
}

final class _KeepLocalResolver implements Resolver {
  const _KeepLocalResolver();

  @override
  MergeResult resolve(ConflictContext conflict) {
    return const KeepLocal();
  }
}

final class _MergedUserResolver implements Resolver {
  const _MergedUserResolver();

  @override
  MergeResult resolve(ConflictContext conflict) {
    return const KeepMerged({'id': 'user-1', 'name': 'Merged'});
  }
}

List<Map<String, Object?>> _readPushBehaviorCases() {
  return [
    ..._readPushBehaviorFile('recovery.json'),
    ..._readPushBehaviorFile('apply.json'),
  ];
}

List<Map<String, Object?>> _readPushBehaviorFile(String name) {
  final file = repoRoot().uri
      .resolve('oversqlite-contracts/push/behavior/$name')
      .toFilePath();
  final raw = jsonDecode(File(file).readAsStringSync()) as Map<String, Object?>;
  return (raw['cases']! as List<Object?>).cast<Map<String, Object?>>();
}

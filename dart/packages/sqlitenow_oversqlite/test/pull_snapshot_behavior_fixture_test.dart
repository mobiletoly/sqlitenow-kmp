import 'dart:convert';
import 'dart:io';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

import 'support/behavior_fixture_support.dart';

void main() {
  final cases = _readPullBehaviorCases();

  test('Dart shared pull behavior fixtures execute against runtime', () async {
    for (final fixture in cases) {
      await _runPullCase(fixture);
    }
  });
}

Future<void> _runPullCase(Map<String, Object?> fixture) async {
  final database = await openBehaviorDatabase(
    fixture['schema'] as String? ?? 'users',
  );
  final server = _newPullServer(fixture);
  final client = newBehaviorClient(database, server, fixture);
  try {
    await client.open();
    await client.attach('user-1');
    for (final step
        in (fixture['steps']! as List<Object?>).cast<Map<String, Object?>>()) {
      await executeSetupSql(
        database,
        (step['setupSql'] as List<Object?>?)?.cast<String>() ?? const [],
      );
      final sourceBeforeStep = await scalarText(
        database,
        'SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1',
      );
      Object? error;
      try {
        switch (step['action']) {
          case 'pullToStable':
            await client.pullToStable();
          case 'rebuild':
            await client.rebuild();
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
        expectedMessage: step['expectedErrorContains'] as String?,
        expectedBlocked: (step['expectedCheckpointRecoveryBlocked'] as Map?)
            ?.cast<String, Object?>(),
      );
      final expectedState = step['expectedState'];
      if (expectedState != null) {
        await _expectPullState(
          database,
          fixture['name']! as String,
          (expectedState as Map).cast<String, Object?>(),
          sourceBeforeStep,
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
    }
  } finally {
    await client.close();
    await database.close();
  }
}

OversqliteHttpClient _newPullServer(Map<String, Object?> fixture) {
  final script = (fixture['serverScript']! as Map).cast<String, Object?>();
  switch (script['kind']) {
    case 'pull_incremental_bundles':
      return PullSnapshotBehaviorFixtureServer(
        pullResponses: [(script['response']! as Map).cast<String, Object?>()],
      );
    case 'pull_sequence':
      return PullSnapshotBehaviorFixtureServer(
        pullResponses: (script['responses']! as List<Object?>)
            .cast<Map<Object?, Object?>>()
            .map((response) => response.cast<String, Object?>())
            .toList(),
      );
    case 'snapshot_sequence':
      return PullSnapshotBehaviorFixtureServer(
        snapshotSessions: (script['sessions']! as List<Object?>)
            .cast<Map<Object?, Object?>>()
            .map((session) => session.cast<String, Object?>())
            .toList(),
        snapshotChunks: (script['chunks']! as Map).cast<String, Object?>(),
      );
    default:
      fail('${fixture['name']}: unknown server script ${script['kind']}');
  }
}

Future<void> _expectPullState(
  SqliteNowDatabase database,
  String name,
  Map<String, Object?> expected,
  String sourceBeforeStep,
) async {
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
  if (expected.containsKey('dirtyRowCount')) {
    expect(
      await scalarInt(database, 'SELECT COUNT(*) FROM _sync_dirty_rows'),
      expected['dirtyRowCount'],
      reason: '$name dirtyRowCount',
    );
  }
  if (expected.containsKey('snapshotStageCount')) {
    expect(
      await scalarInt(database, 'SELECT COUNT(*) FROM _sync_snapshot_stage'),
      expected['snapshotStageCount'],
      reason: '$name snapshotStageCount',
    );
  }
  if (expected.containsKey('rowStateCount')) {
    expect(
      await scalarInt(database, 'SELECT COUNT(*) FROM _sync_row_state'),
      expected['rowStateCount'],
      reason: '$name rowStateCount',
    );
  }
  if (expected.containsKey('outboxRowCount')) {
    expect(
      await scalarInt(database, 'SELECT COUNT(*) FROM _sync_outbox_rows'),
      expected['outboxRowCount'],
      reason: '$name outboxRowCount',
    );
  }
  if (expected.containsKey('rebuildRequired')) {
    expect(
      await scalarInt(
        database,
        'SELECT rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1',
      ),
      expected['rebuildRequired'],
      reason: '$name rebuildRequired',
    );
  }
  if (expected.containsKey('currentSourceChangedFromStepStart')) {
    final currentSource = await scalarText(
      database,
      'SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1',
    );
    expect(
      currentSource != sourceBeforeStep,
      expected['currentSourceChangedFromStepStart'],
      reason: '$name currentSourceChangedFromStepStart',
    );
  }
  if (expected.containsKey('oldSourceReplacedByCurrent')) {
    final currentSource = await scalarText(
      database,
      'SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1',
    );
    final replacedBy = await scalarText(
      database,
      "SELECT replaced_by_source_id FROM _sync_source_state WHERE source_id = '$sourceBeforeStep'",
    );
    expect(
      replacedBy == currentSource && currentSource != sourceBeforeStep,
      expected['oldSourceReplacedByCurrent'],
      reason: '$name oldSourceReplacedByCurrent',
    );
  }
}

void _expectException(
  String name,
  String expected,
  Object? error, {
  String? expectedMessage,
  Map<String, Object?>? expectedBlocked,
}) {
  switch (expected) {
    case 'none':
      expect(error, isNull, reason: name);
    case 'any_error':
      expect(error, isNotNull, reason: name);
      if (expectedMessage != null) {
        expect(error.toString(), contains(expectedMessage), reason: name);
      }
      if (expectedBlocked != null) {
        expect(error, isA<CheckpointRecoveryBlockedException>(), reason: name);
        final blocked = error! as CheckpointRecoveryBlockedException;
        expect(
          blocked.reason.name,
          _blockedReasonName(expectedBlocked['reason']! as String),
          reason: '$name blocker reason',
        );
        expect(
          blocked.dirtyCount,
          expectedBlocked['dirtyRowCount'],
          reason: '$name blocker dirty rows',
        );
        expect(
          blocked.outboundCount,
          expectedBlocked['outboxRowCount'],
          reason: '$name blocker outbox rows',
        );
        expect(
          blocked.replayState,
          expectedBlocked['replayState'],
          reason: '$name blocker replay state',
        );
      }
    default:
      fail('$name: unknown expected exception $expected');
  }
}

String _blockedReasonName(String wireName) => switch (wireName) {
  'upload_paused' => 'uploadPaused',
  'pending_work' => 'pendingWork',
  'push_failed' => 'pushFailed',
  _ => throw ArgumentError.value(wireName, 'wireName'),
};

final class PullSnapshotBehaviorFixtureServer implements OversqliteHttpClient {
  PullSnapshotBehaviorFixtureServer({
    this.pullResponses = const [],
    this.snapshotSessions = const [],
    this.snapshotChunks = const {},
  });

  final List<Map<String, Object?>> pullResponses;
  final List<Map<String, Object?>> snapshotSessions;
  final Map<String, Object?> snapshotChunks;
  var _pullIndex = 0;
  var _snapshotSessionIndex = 0;

  @override
  Future<OversqliteHttpResponse> get(
    String path, {
    required String sourceId,
  }) async {
    if (path == 'sync/capabilities') {
      return _json({
        'protocol_version': 'v0',
        'schema_version': 1,
        'features': {'connect_lifecycle': true},
      });
    }
    if (path.startsWith('sync/pull')) {
      final response = _pullIndex < pullResponses.length
          ? pullResponses[_pullIndex]
          : null;
      if (response == null) {
        return _json({
          'error': 'fixture_exhausted',
          'message': 'pull responses exhausted',
        }, statusCode: 500);
      }
      _pullIndex++;
      return _json(response);
    }
    if (path.startsWith('sync/snapshot-sessions/')) {
      final parts = path.split('?');
      final snapshotId = parts.first.substring(
        'sync/snapshot-sessions/'.length,
      );
      final query = Uri.splitQueryString(parts.length > 1 ? parts[1] : '');
      final afterRowOrdinal = query['after_row_ordinal'] ?? '0';
      final chunksForSnapshot = (snapshotChunks[snapshotId]! as Map)
          .cast<String, Object?>();
      final response = (chunksForSnapshot[afterRowOrdinal]! as Map)
          .cast<String, Object?>();
      return _fixtureJson(response);
    }
    return _json({'error': 'not_found', 'message': path}, statusCode: 404);
  }

  @override
  Future<OversqliteHttpResponse> postJson(
    String path, {
    required String sourceId,
    required Object? body,
  }) async {
    if (path == 'sync/connect') {
      return _json({'resolution': 'initialize_empty'});
    }
    if (path == 'sync/snapshot-sessions') {
      final response = _snapshotSessionIndex < snapshotSessions.length
          ? snapshotSessions[_snapshotSessionIndex]
          : null;
      if (response == null) {
        return _json({
          'error': 'fixture_exhausted',
          'message': 'snapshot sessions exhausted',
        }, statusCode: 500);
      }
      _snapshotSessionIndex++;
      return _json(response);
    }
    if (path == 'sync/push-sessions') {
      return _json({
        'error': 'pending_work',
        'message': 'fixture keeps checkpoint recovery outbox pending',
      }, statusCode: 400);
    }
    return _json({'error': 'not_found', 'message': path}, statusCode: 404);
  }

  @override
  Future<OversqliteHttpResponse> delete(
    String path, {
    required String sourceId,
  }) async {
    return const OversqliteHttpResponse(statusCode: 204, body: '');
  }

  OversqliteHttpResponse _fixtureJson(Map<String, Object?> response) {
    final body = response.containsKey('body') ? response['body'] : response;
    return OversqliteHttpResponse(
      statusCode: response['status'] as int? ?? 200,
      body: jsonEncode(body),
    );
  }

  OversqliteHttpResponse _json(
    Map<String, Object?> body, {
    int statusCode = 200,
  }) {
    return OversqliteHttpResponse(
      statusCode: statusCode,
      body: jsonEncode(body),
    );
  }
}

List<Map<String, Object?>> _readPullBehaviorCases() {
  final file = repoRoot().uri
      .resolve('oversqlite-contracts/pull-snapshot/behavior/apply.json')
      .toFilePath();
  final raw = jsonDecode(File(file).readAsStringSync()) as Map<String, Object?>;
  return (raw['cases']! as List<Object?>).cast<Map<String, Object?>>();
}

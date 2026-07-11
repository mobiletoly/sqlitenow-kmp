import 'dart:convert';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

import 'support/behavior_fixture_support.dart';
import 'support/runtime_state_fixture_support.dart';

void main() {
  final fixtures = [
    readRuntimeStateFixture(
          'oversqlite-contracts/lifecycle/behavior/basic.json',
        )!
        as Map<String, Object?>,
    readRuntimeStateFixture(
          'oversqlite-contracts/lifecycle/behavior/source-recovery-replacement.json',
        )!
        as Map<String, Object?>,
  ];

  test('Dart shared lifecycle behavior fixture matches runtime', () async {
    for (final fixture in fixtures) {
      expect(fixture['formatVersion'], 1);
      for (final rawCase
          in (fixture['cases']! as List<Object?>)
              .cast<Map<String, Object?>>()) {
        await _runCase(rawCase);
      }
    }
  });
}

Future<void> _runCase(Map<String, Object?> fixture) async {
  final database = await openUsersDatabase();
  final server = LifecycleFixtureServer(
    database: database,
    script:
        (fixture['serverScript'] as Map<Object?, Object?>?)
            ?.cast<String, Object?>() ??
        const {'kind': 'default'},
  );
  var client = newRuntimeStateClient(database, server);
  try {
    for (final step
        in (fixture['steps']! as List<Object?>).cast<Map<String, Object?>>()) {
      Object? error;
      Map<String, Object?>? actualResult;
      try {
        actualResult = await _runStep(database, server, client, step, (next) {
          client = next;
        });
      } catch (caught) {
        error = caught;
      }
      _expectException(
        fixture['name']! as String,
        step['expectedException'] as String? ?? 'none',
        error,
      );
      final expectedResult = step['expectedResult'];
      if (expectedResult != null) {
        expect(
          actualResult,
          (expectedResult as Map).cast<String, Object?>(),
          reason: '${fixture['name']}/${step['action']} result',
        );
      }
      final expectedServerState = step['expectedServerState'];
      if (expectedServerState != null) {
        _expectServerState(
          server,
          fixture['name']! as String,
          (expectedServerState as Map).cast<String, Object?>(),
        );
      }
      expect(
        await dumpRuntimeState(database),
        step['expectedState'],
        reason: '${fixture['name']}/${step['action']}',
      );
    }
  } finally {
    await client.close();
    await database.close();
  }
}

Future<Map<String, Object?>?> _runStep(
  SqliteNowDatabase database,
  LifecycleFixtureServer server,
  DefaultOversqliteClient client,
  Map<String, Object?> step,
  void Function(DefaultOversqliteClient) replaceClient,
) async {
  switch (step['action']) {
    case 'open':
      await client.open();
    case 'attach':
      return _attachResult(await client.attach('user-1'));
    case 'localSql':
      await executeRuntimeStateSql(
        database,
        (step['sql'] as List<Object?>? ?? const []).cast<String>(),
      );
    case 'applyModeSql':
      await executeApplyModeSql(
        database,
        (step['sql'] as List<Object?>? ?? const []).cast<String>(),
      );
    case 'sync':
      return _syncResult(await client.sync());
    case 'syncThenDetach':
      return _syncThenDetachResult(await client.syncThenDetach());
    case 'rebuild':
      return _remoteSyncResult(await client.rebuild());
    case 'detach':
      return _detachResult(await client.detach());
    case 'sourceInfo':
      return _sourceInfoResult(await client.sourceInfo());
    case 'reopenOpen':
      await client.close();
      final next = newRuntimeStateClient(database, server);
      replaceClient(next);
      await next.open();
    default:
      fail('unknown lifecycle action ${step['action']}');
  }
  return null;
}

Map<String, Object?> _attachResult(AttachResult result) {
  return switch (result) {
    AttachConnected(:final outcome) => {
      'kind': 'attach',
      'outcome': _snake(outcome.name),
    },
    AttachRetryLater(:final retryAfterSeconds) => {
      'kind': 'attach_retry_later',
      'retryAfterSeconds': retryAfterSeconds,
    },
  };
}

Map<String, Object?> _syncResult(SyncReport result) {
  return {
    'kind': 'sync',
    'pushOutcome': _snake(result.pushOutcome.name),
    'remoteOutcome': _snake(result.remoteOutcome.name),
  };
}

Map<String, Object?> _syncThenDetachResult(SyncThenDetachResult result) {
  return {
    'kind': 'sync_then_detach',
    'isSuccess': result.isSuccess,
    'detach': _snake(result.detach.name),
    'syncRounds': result.syncRounds,
    'remainingPendingRowCount': result.remainingPendingRowCount,
    'pushOutcome': _snake(result.lastSync.pushOutcome.name),
    'remoteOutcome': _snake(result.lastSync.remoteOutcome.name),
  };
}

Map<String, Object?> _remoteSyncResult(RemoteSyncReport result) {
  return {'kind': 'remote_sync', 'outcome': _snake(result.outcome.name)};
}

Map<String, Object?> _detachResult(DetachOutcome result) {
  return {'kind': 'detach', 'outcome': _snake(result.name)};
}

Map<String, Object?> _sourceInfoResult(SourceInfo result) {
  return {
    'kind': 'source_info',
    'currentSourceIdPresent': result.currentSourceId.isNotEmpty,
    'rebuildRequired': result.rebuildRequired,
    'sourceRecoveryRequired': result.sourceRecoveryRequired,
    if (result.sourceRecoveryReason != null)
      'sourceRecoveryReason': result.sourceRecoveryReason,
  };
}

void _expectException(String name, String expected, Object? error) {
  switch (expected) {
    case 'none':
      expect(error, isNull, reason: name);
    case 'any_error':
      expect(error, isNotNull, reason: name);
    case 'http_error':
      expect(error, isA<OversqliteHttpException>(), reason: name);
    case 'rebuild_required':
      expect(error, isA<RebuildRequiredException>(), reason: name);
    case 'source_recovery_required':
      expect(error, isA<SourceRecoveryRequiredException>(), reason: name);
    case 'source_sequence_mismatch':
      expect(error, isA<SourceSequenceMismatchException>(), reason: name);
    case 'source_replacement_diverged':
      expect(error, isA<SourceReplacementDivergedException>(), reason: name);
    case 'source_replacement_invalid':
      expect(error, isA<SourceReplacementInvalidException>(), reason: name);
    case 'invalid_source_recovery_reason':
      expect(error, isA<StateError>(), reason: name);
    default:
      fail('$name: unknown expected exception $expected');
  }
}

void _expectServerState(
  LifecycleFixtureServer server,
  String name,
  Map<String, Object?> expected,
) {
  if (expected.containsKey('createRequestCount')) {
    expect(
      server.createRequests,
      hasLength(expected['createRequestCount']! as int),
      reason: '$name createRequestCount',
    );
  }
  if (expected.containsKey('createSourceBundleIds')) {
    expect(
      server.createRequests
          .map((request) => request['source_bundle_id'])
          .toList(),
      expected['createSourceBundleIds'],
      reason: '$name createSourceBundleIds',
    );
  }
  if (expected.containsKey('snapshotSessionCreateCount')) {
    expect(
      server.snapshotSessionCreateCount,
      expected['snapshotSessionCreateCount'],
      reason: '$name snapshotSessionCreateCount',
    );
  }
  if (expected.containsKey('snapshotSourceReplacementRequests')) {
    expect(
      server.snapshotSourceReplacementRequests,
      expected['snapshotSourceReplacementRequests'],
      reason: '$name snapshotSourceReplacementRequests',
    );
  }
}

String _snake(String value) {
  return value.replaceAllMapped(
    RegExp('[A-Z]'),
    (match) => '_${match.group(0)!.toLowerCase()}',
  );
}

final class LifecycleFixtureServer implements OversqliteHttpClient {
  LifecycleFixtureServer({
    required this.database,
    required Map<String, Object?> script,
  }) : connectResolution = script['connectResolution'] as String? ?? 'default',
       lateWritesByPull =
           (script['lateWritesByPull'] as List<Object?>? ?? const [])
               .map((entry) => (entry as List<Object?>).cast<String>())
               .toList(),
       snapshots = (script['snapshots'] as List<Object?>? ?? const [])
           .map(
             (entry) => LifecycleSnapshot.fromJson(
               (entry as Map).cast<String, Object?>(),
             ),
           )
           .toList(),
       snapshotSessionErrors =
           (script['snapshotSessionErrors'] as List<Object?>? ?? const [])
               .map(
                 (entry) => LifecycleSnapshotSessionError.fromJson(
                   (entry as Map).cast<String, Object?>(),
                 ),
               )
               .toList();

  final SqliteNowDatabase database;
  final String connectResolution;
  final List<List<String>> lateWritesByPull;
  final List<LifecycleSnapshot> snapshots;
  final List<LifecycleSnapshotSessionError> snapshotSessionErrors;
  final bundles = <Map<String, Object?>>[];
  final uploadedRows = <Map<String, Object?>>[];
  final createRequests = <Map<String, Object?>>[];
  final snapshotSourceReplacementRequests = <String>[];
  final _uploadedRowsByBundleSeq = <int, List<Map<String, Object?>>>{};
  final _sourceBundleIdByBundleSeq = <int, int>{};
  var snapshotSessionCreateCount = 0;
  var stableBundleSeq = 0;
  var _sourceId = '';
  var _activeSourceBundleId = 1;
  var _activeRequestHash = '';
  var _nextBundleSeq = 1;
  var _pullRequestCount = 0;
  var _nextSnapshotIndex = 0;

  @override
  Future<OversqliteHttpResponse> get(
    String path, {
    required String sourceId,
  }) async {
    _sourceId = sourceId;
    if (path == 'sync/capabilities') {
      return _json({
        'protocol_version': 'v1',
        'schema_version': 1,
        'features': {'connect_lifecycle': true},
      });
    }
    if (path.startsWith('sync/pull')) {
      final lateWrites = _pullRequestCount < lateWritesByPull.length
          ? lateWritesByPull[_pullRequestCount]
          : const <String>[];
      _pullRequestCount++;
      if (lateWrites.isNotEmpty) {
        await executeSetupSql(database, lateWrites);
      }
      final uri = Uri.parse('http://local/$path');
      final after = int.parse(uri.queryParameters['after_bundle_seq'] ?? '0');
      final visible = bundles
          .where((bundle) => (bundle['bundle_seq']! as int) > after)
          .toList();
      return _json({
        'stable_bundle_seq': stableBundleSeq,
        'bundles': visible,
        'has_more': false,
      });
    }
    if (path.startsWith('sync/committed-bundles/')) {
      final bundleSeq = int.parse(
        path.substring('sync/committed-bundles/'.length).split('/').first,
      );
      final uploaded = _uploadedRowsByBundleSeq[bundleSeq] ?? uploadedRows;
      final rows = [
        for (final row in uploaded)
          {
            'schema': row['schema'],
            'table': row['table'],
            'key': row['key'],
            'op': row['op'],
            'row_version': bundleSeq,
            'payload': row['payload'],
          },
      ];
      return _json({
        'bundle_seq': bundleSeq,
        'source_id': _sourceId,
        'source_bundle_id':
            _sourceBundleIdByBundleSeq[bundleSeq] ?? _activeSourceBundleId,
        'row_count': rows.length,
        'bundle_hash': committedBundleHashForFixtureRows(rows),
        'canonical_request_hash': _activeRequestHash,
        'rows': rows,
        'next_row_ordinal': rows.isEmpty ? -1 : rows.length - 1,
        'has_more': false,
      });
    }
    if (path.startsWith('sync/snapshot-sessions/')) {
      final snapshotId = path
          .substring('sync/snapshot-sessions/'.length)
          .split('?')
          .first;
      final snapshot = snapshots.firstWhere((entry) => entry.id == snapshotId);
      final uri = Uri.parse('http://local/$path');
      final after = int.parse(uri.queryParameters['after_row_ordinal'] ?? '0');
      if (snapshot.failAfterRowOrdinal == after) {
        return _json({
          'error': 'snapshot_failed',
          'message': 'forced snapshot failure',
        }, statusCode: 500);
      }
      final maxRows = int.parse(uri.queryParameters['max_rows'] ?? '1000');
      final rowLimit = snapshot.rowsPerChunk == null
          ? maxRows
          : (snapshot.rowsPerChunk! < maxRows
                ? snapshot.rowsPerChunk!
                : maxRows);
      final rows = snapshot.rows.skip(after).take(rowLimit).toList();
      return _json({
        'snapshot_id': snapshot.id,
        'snapshot_bundle_seq': snapshot.bundleSeq,
        'rows': [
          for (final row in rows)
            {
              'schema': 'main',
              'table': 'users',
              'key': row.key,
              'row_version': row.rowVersion,
              'payload': row.payload,
            },
        ],
        'next_row_ordinal': after + rows.length,
        'has_more': after + rows.length < snapshot.rows.length,
      });
    }
    return _json({'error': 'not_found', 'message': path}, statusCode: 404);
  }

  @override
  Future<OversqliteHttpResponse> postJson(
    String path, {
    required String sourceId,
    required Object? body,
  }) async {
    _sourceId = sourceId;
    if (path == 'sync/connect') {
      return _json({
        'resolution': connectResolution == 'default'
            ? 'initialize_empty'
            : connectResolution,
      });
    }
    if (path == 'sync/snapshot-sessions') {
      snapshotSessionCreateCount += 1;
      final request = body is Map
          ? body.cast<String, Object?>()
          : const <String, Object?>{};
      final replacement = request['source_replacement'];
      snapshotSourceReplacementRequests.add(
        replacement is Map
            ? canonicalizeFixtureJson(replacement.cast<String, Object?>())
            : '',
      );
      if (_nextSnapshotIndex < snapshotSessionErrors.length) {
        final error = snapshotSessionErrors[_nextSnapshotIndex];
        _nextSnapshotIndex++;
        return _json({
          'error': error.error,
          'message': error.message,
          'source_id': error.sourceId,
          if (error.replacedBySourceId != null)
            'replaced_by_source_id': error.replacedBySourceId,
        }, statusCode: error.status);
      }
      final snapshot =
          snapshots[_nextSnapshotIndex < snapshots.length
              ? _nextSnapshotIndex
              : snapshots.length - 1];
      _nextSnapshotIndex++;
      return _json({
        'snapshot_id': snapshot.id,
        'snapshot_bundle_seq': snapshot.bundleSeq,
        'row_count': snapshot.rows.length,
        'byte_count': 0,
        'expires_at': '2099-01-01T00:00:00Z',
      });
    }
    if (path == 'sync/push-sessions') {
      final request = (body! as Map).cast<String, Object?>();
      createRequests.add(request);
      _activeSourceBundleId = request['source_bundle_id']! as int;
      _activeRequestHash = request['canonical_request_hash']! as String;
      uploadedRows.clear();
      return _json({
        'status': 'staging',
        'push_id': 'push-1',
        'planned_row_count': request['planned_row_count'],
        'next_expected_row_ordinal': 0,
        'canonical_request_hash': _activeRequestHash,
      });
    }
    if (path == 'sync/push-sessions/push-1/chunks') {
      final request = (body! as Map).cast<String, Object?>();
      uploadedRows.addAll(
        (request['rows']! as List<Object?>).cast<Map<String, Object?>>(),
      );
      return _json({
        'push_id': 'push-1',
        'next_expected_row_ordinal': uploadedRows.length,
      });
    }
    if (path == 'sync/push-sessions/push-1/commit') {
      final bundleSeq = _nextBundleSeq++;
      final storedRows = [
        for (final row in uploadedRows) {...row},
      ];
      _uploadedRowsByBundleSeq[bundleSeq] = storedRows;
      _sourceBundleIdByBundleSeq[bundleSeq] = _activeSourceBundleId;
      stableBundleSeq = bundleSeq;
      final committedRows = [
        for (final row in storedRows) {...row, 'row_version': bundleSeq},
      ];
      bundles.add({
        'bundle_seq': bundleSeq,
        'source_id': _sourceId,
        'source_bundle_id': _activeSourceBundleId,
        'rows': committedRows,
      });
      return _json({
        'bundle_seq': bundleSeq,
        'source_id': _sourceId,
        'source_bundle_id': _activeSourceBundleId,
        'row_count': uploadedRows.length,
        'bundle_hash': committedBundleHashForFixtureRows(committedRows),
        'canonical_request_hash': _activeRequestHash,
      });
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

final class LifecycleSnapshotSessionError {
  const LifecycleSnapshotSessionError({
    required this.status,
    required this.error,
    required this.message,
    required this.sourceId,
    this.replacedBySourceId,
  });

  factory LifecycleSnapshotSessionError.fromJson(Map<String, Object?> json) {
    return LifecycleSnapshotSessionError(
      status: json['status'] as int? ?? 409,
      error: json['error']! as String,
      message: json['message']! as String,
      sourceId: json['sourceId'] as String? ?? 'current-source',
      replacedBySourceId: json['replacedBySourceId'] as String?,
    );
  }

  final int status;
  final String error;
  final String message;
  final String sourceId;
  final String? replacedBySourceId;
}

final class LifecycleSnapshot {
  const LifecycleSnapshot({
    required this.id,
    required this.bundleSeq,
    required this.rows,
    this.failAfterRowOrdinal,
    this.rowsPerChunk,
  });

  factory LifecycleSnapshot.fromJson(Map<String, Object?> json) {
    return LifecycleSnapshot(
      id: json['id']! as String,
      bundleSeq: json['bundleSeq']! as int,
      failAfterRowOrdinal: json['failAfterRowOrdinal'] as int?,
      rowsPerChunk: json['rowsPerChunk'] as int?,
      rows: [
        for (final row in (json['rows']! as List<Object?>))
          LifecycleSnapshotRow.fromJson((row as Map).cast<String, Object?>()),
      ],
    );
  }

  final String id;
  final int bundleSeq;
  final List<LifecycleSnapshotRow> rows;
  final int? failAfterRowOrdinal;
  final int? rowsPerChunk;
}

final class LifecycleSnapshotRow {
  const LifecycleSnapshotRow({
    required this.key,
    required this.rowVersion,
    required this.payload,
  });

  factory LifecycleSnapshotRow.fromJson(Map<String, Object?> json) {
    return LifecycleSnapshotRow(
      key: (json['key']! as Map).cast<String, Object?>(),
      rowVersion: json['rowVersion']! as int,
      payload: (json['payload']! as Map).cast<String, Object?>(),
    );
  }

  final Map<String, Object?> key;
  final int rowVersion;
  final Map<String, Object?> payload;
}

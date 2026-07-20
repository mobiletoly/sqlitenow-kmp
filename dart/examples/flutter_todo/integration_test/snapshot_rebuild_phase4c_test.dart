import 'dart:convert';

import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';
import 'package:sqlite3/sqlite3.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';

import 'realserver_test_support.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  testWidgets(
    'Phase 4C snapshot rebuild is bounded, atomic, and lifecycle-safe',
    (tester) async {
      final tempDirectory = await createRealserverTempDir();
      final database = await openBusinessDatabase(
        '${tempDirectory.path}/phase4c-snapshot.sqlite',
      );
      final server = _SnapshotServer()
        ..replaceSnapshot(
          snapshotId: 'android-snapshot-1',
          bundleSeq: 1,
          rows: [_userRow('user-1'), _postRow('post-1', 'user-1')],
        );
      final client = DefaultOversqliteClient(
        database: database,
        config: OversqliteConfig(
          schema: 'business',
          syncTables: businessSyncTables,
          snapshotChunkRows: 1,
          snapshotChunkBytes: 512,
          snapshotApplyBatchRows: 1,
          snapshotApplyBatchBytes: 512,
        ),
        httpClient: server,
      );
      addTearDown(client.close);
      addTearDown(database.close);

      await client.open();
      final initialSource = (await client.sourceInfo()).currentSourceId;
      final attached = await client.attach('phase4c-android-user');

      expect(attached, isA<AttachConnected>());
      expect(
        (attached as AttachConnected).outcome,
        AttachOutcome.usedRemoteState,
      );
      expect(await scalarInt(database, 'SELECT COUNT(*) FROM users'), 1);
      expect(await scalarInt(database, 'SELECT COUNT(*) FROM posts'), 1);
      expect(
        await scalarText(
          database,
          "SELECT name FROM users WHERE id = 'user-1'",
        ),
        'Android User user-1',
      );
      expect(
        await scalarInt(database, 'SELECT COUNT(*) FROM _sync_snapshot_stage'),
        0,
        reason: 'a committed rebuild must delete its completed stage',
      );
      expect(server.maxRequestedChunkRows, 1);
      expect(server.maxRequestedChunkBytes, 512);
      expect(server.maxReturnedChunkRows, 1);
      expect(server.snapshotChunkRequests, 2);
      expect((await client.syncStatus()).lastBundleSeqSeen, 1);
      expect((await client.sourceInfo()).currentSourceId, initialSource);
      final committedLifecycle = await _lifecycle(database);
      expect(committedLifecycle.bindingState, 'attached');
      expect(committedLifecycle.userId, 'phase4c-android-user');
      expect(committedLifecycle.bundleSeq, 1);

      server.replaceSnapshot(
        snapshotId: 'android-snapshot-2',
        bundleSeq: 2,
        rows: [_postRow('post-2', 'missing-user')],
      );
      await expectLater(
        client.rebuild(),
        throwsA(
          isA<SqliteException>()
              .having(
                (error) => error.extendedResultCode,
                'extended result code',
                SqlExtendedError.SQLITE_CONSTRAINT_FOREIGNKEY,
              )
              .having(
                (error) => error.resultCode,
                'primary result code',
                SqlError.SQLITE_CONSTRAINT,
              )
              .having(
                (error) => error.causingStatement,
                'causing statement',
                'COMMIT',
              )
              .having((error) => error.operation, 'operation', 'executing'),
        ),
        reason:
            'the real SQLite driver must reach the deferred foreign-key failure at COMMIT',
      );
      expect(await scalarInt(database, 'SELECT COUNT(*) FROM users'), 1);
      expect(await scalarInt(database, 'SELECT COUNT(*) FROM posts'), 1);
      expect(
        await scalarText(
          database,
          "SELECT name FROM users WHERE id = 'user-1'",
        ),
        'Android User user-1',
        reason: 'the previous authoritative snapshot must survive rollback',
      );
      expect(
        await scalarInt(
          database,
          "SELECT COUNT(*) FROM posts WHERE id = 'post-2'",
        ),
        0,
      );
      expect((await client.syncStatus()).lastBundleSeqSeen, 1);
      expect((await client.sourceInfo()).currentSourceId, initialSource);
      expect(await _lifecycle(database), committedLifecycle);
      expect(
        await scalarInt(
          database,
          "SELECT COUNT(*) FROM _sync_snapshot_stage "
          "WHERE snapshot_id = 'android-snapshot-2'",
        ),
        1,
        reason: 'failed apply rolls back stage deletion with the old state',
      );
      expect(server.retirementCount, 2);
      expect(server.everyRetirementDiscardedBody, isTrue);
    },
  );
}

Future<({String bindingState, String userId, int bundleSeq})> _lifecycle(
  SqliteNowDatabase database,
) async {
  final rows = await database.connection.select(
    '''SELECT binding_state, attached_user_id, last_bundle_seq_seen
FROM _sync_attachment_state
WHERE singleton_key = 1''',
    (row) => (
      bindingState: row.readString(0),
      userId: row.readString(1),
      bundleSeq: row.readInt(2),
    ),
  );
  return rows.single;
}

Map<String, Object?> _userRow(String id) => {
  'schema': 'business',
  'table': 'users',
  'key': {'id': id},
  'row_version': 1,
  'payload': {
    'id': id,
    'name': 'Android User $id',
    'email': '$id@example.com',
    'created_at': '2026-07-20T00:00:00Z',
    'updated_at': '2026-07-20T00:00:00Z',
  },
};

Map<String, Object?> _postRow(String id, String authorId) => {
  'schema': 'business',
  'table': 'posts',
  'key': {'id': id},
  'row_version': 1,
  'payload': {
    'id': id,
    'title': 'Android Post $id',
    'content': 'Phase 4C Android snapshot proof',
    'author_id': authorId,
    'created_at': '2026-07-20T00:00:00Z',
    'updated_at': '2026-07-20T00:00:00Z',
  },
};

final class _SnapshotServer implements OversqliteHttpClient {
  var _snapshotId = '';
  var _bundleSeq = 0;
  var _rows = <Map<String, Object?>>[];
  var snapshotChunkRequests = 0;
  var maxRequestedChunkRows = 0;
  var maxRequestedChunkBytes = 0;
  var maxReturnedChunkRows = 0;
  var retirementCount = 0;
  var everyRetirementDiscardedBody = true;

  void replaceSnapshot({
    required String snapshotId,
    required int bundleSeq,
    required List<Map<String, Object?>> rows,
  }) {
    _snapshotId = snapshotId;
    _bundleSeq = bundleSeq;
    _rows = [...rows];
  }

  @override
  Future<OversqliteHttpResponse> get(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async {
    if (path == 'sync/capabilities') {
      return _json(_capabilities());
    }
    if (path.startsWith('sync/snapshot-sessions/')) {
      snapshotChunkRequests++;
      final uri = Uri.parse('http://fixture/$path');
      final after = int.parse(uri.queryParameters['after_row_ordinal']!);
      final maxRows = int.parse(uri.queryParameters['max_rows']!);
      final maxBytes = int.parse(uri.queryParameters['max_bytes']!);
      if (maxRows > maxRequestedChunkRows) maxRequestedChunkRows = maxRows;
      if (maxBytes > maxRequestedChunkBytes) maxRequestedChunkBytes = maxBytes;
      final selected = <Map<String, Object?>>[];
      var selectedBytes = 0;
      for (final row in _rows.skip(after)) {
        final rowBytes = utf8.encode(jsonEncode(row)).length;
        if (selected.length >= maxRows || selectedBytes + rowBytes > maxBytes) {
          break;
        }
        selected.add(row);
        selectedBytes += rowBytes;
      }
      if (selected.length > maxReturnedChunkRows) {
        maxReturnedChunkRows = selected.length;
      }
      final next = after + selected.length;
      return _json({
        'snapshot_id': _snapshotId,
        'snapshot_bundle_seq': _bundleSeq,
        'rows': selected,
        'next_row_ordinal': next,
        'byte_count': selectedBytes,
        'has_more': next < _rows.length,
      });
    }
    return _json({
      'error': 'not_found',
      'message': 'fixture route not found',
    }, statusCode: 404);
  }

  @override
  Future<OversqliteHttpResponse> postJson(
    String path, {
    required String sourceId,
    required Object? body,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async {
    if (path == 'sync/connect') {
      return _json({'resolution': 'remote_authoritative'});
    }
    if (path == 'sync/snapshot-sessions') {
      return _json({
        'snapshot_id': _snapshotId,
        'snapshot_bundle_seq': _bundleSeq,
        'row_count': _rows.length,
        'byte_count': _rows.fold<int>(
          0,
          (total, row) => total + utf8.encode(jsonEncode(row)).length,
        ),
        'expires_at': '2099-01-01T00:00:00Z',
      });
    }
    return _json({
      'error': 'not_found',
      'message': 'fixture route not found',
    }, statusCode: 404);
  }

  @override
  Future<OversqliteHttpResponse> delete(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async {
    retirementCount++;
    everyRetirementDiscardedBody &= bounds.discardBody;
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

Map<String, Object?> _capabilities() => {
  'protocol_version': 'v1',
  'schema_version': 1,
  'features': {'connect_lifecycle': true},
  'bundle_limits': {
    'max_rows_per_bundle': 1000,
    'max_bytes_per_bundle': 4194304,
    'max_bundles_per_pull': 100,
    'default_rows_per_push_chunk': 1000,
    'max_rows_per_push_chunk': 1000,
    'push_session_ttl_seconds': 300,
    'default_rows_per_committed_bundle_chunk': 1000,
    'max_rows_per_committed_bundle_chunk': 1000,
    'default_rows_per_snapshot_chunk': 1,
    'max_rows_per_snapshot_chunk': 1,
    'snapshot_session_ttl_seconds': 300,
    'max_rows_per_snapshot_session': 1000,
    'max_bytes_per_snapshot_session': 4194304,
    'default_bytes_per_snapshot_chunk': 512,
    'max_bytes_per_snapshot_chunk': 512,
    'max_bytes_per_snapshot_row': 512,
    'snapshot_materialization_batch_rows': 1,
    'snapshot_materialization_batch_bytes': 512,
    'max_concurrent_snapshot_builds': 1,
    'max_concurrent_snapshot_chunk_requests': 1,
    'initialization_lease_ttl_seconds': 300,
  },
};

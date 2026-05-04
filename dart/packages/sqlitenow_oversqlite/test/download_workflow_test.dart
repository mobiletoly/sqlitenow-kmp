import 'dart:convert';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

void main() {
  test('pullToStable applies incremental bundles and tombstones', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _SyncServer();
    server.addRemoteBundle([
      _bundleRow('INSERT', {'id': 'user-1'}, {'id': 'user-1', 'name': 'Ada'}),
    ]);
    server.addRemoteBundle([
      _bundleRow('DELETE', {'id': 'user-1'}, null, rowVersion: 2),
    ]);
    final client = _newClient(database, server);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');

    final report = await client.pullToStable();

    expect(report.outcome, RemoteSyncOutcome.appliedIncremental);
    expect(report.status.lastBundleSeqSeen, 2);
    expect(await _scalarInt(database, 'SELECT COUNT(*) FROM users'), 0);
    expect(
      await _scalarInt(database, 'SELECT deleted FROM _sync_row_state'),
      1,
    );
  });

  test('pull rejects dirty rows before applying remote bundles', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _SyncServer();
    final client = _newClient(database, server);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');
    await database.connection.execute(
      "INSERT INTO users(id, name) VALUES('local', 'Local')",
    );

    await expectLater(
      client.pullToStable(),
      throwsA(isA<DirtyStateRejectedException>()),
    );

    expect(await _scalarInt(database, 'SELECT COUNT(*) FROM users'), 1);
    expect(
      await _scalarInt(
        database,
        'SELECT last_bundle_seq_seen FROM _sync_attachment_state',
      ),
      0,
    );
  });

  test('failed bundle apply rolls back checkpoint and apply mode', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _SyncServer();
    server.addRemoteBundle([
      _bundleRow('INSERT', {'id': 'broken'}, {'id': 'broken'}),
    ]);
    final client = _newClient(database, server);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');

    await expectLater(client.pullToStable(), throwsA(isA<Exception>()));

    expect(await _scalarInt(database, 'SELECT COUNT(*) FROM users'), 0);
    expect(
      await _scalarInt(
        database,
        'SELECT last_bundle_seq_seen FROM _sync_attachment_state',
      ),
      0,
    );
    expect(
      await _scalarInt(database, 'SELECT apply_mode FROM _sync_apply_state'),
      0,
    );
  });

  test('history-pruned pull rebuilds from snapshot', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _SyncServer(historyPrunedOnPull: true)
      ..snapshotRows = [
        _snapshotRow({'id': 'user-1'}, {'id': 'user-1', 'name': 'Snapshot'}),
      ]
      ..stableBundleSeq = 7;
    final client = _newClient(database, server);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');

    final report = await client.pullToStable();

    expect(report.outcome, RemoteSyncOutcome.appliedSnapshot);
    expect(report.restore?.bundleSeq, 7);
    expect(
      await _scalarText(database, "SELECT name FROM users WHERE id = 'user-1'"),
      'Snapshot',
    );
  });

  test('remote-authoritative attach hydrates snapshot', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _SyncServer(connectResolution: 'remote_authoritative')
      ..snapshotRows = [
        _snapshotRow({'id': 'user-1'}, {'id': 'user-1', 'name': 'Remote'}),
      ]
      ..stableBundleSeq = 3;
    final client = _newClient(database, server);
    addTearDown(client.close);
    await client.open();

    final result = await client.attach('user-1');

    expect(result, isA<AttachConnected>());
    expect((result as AttachConnected).outcome, AttachOutcome.usedRemoteState);
    expect(
      await _scalarText(database, "SELECT name FROM users WHERE id = 'user-1'"),
      'Remote',
    );
    expect(
      await _scalarInt(
        database,
        'SELECT last_bundle_seq_seen FROM _sync_attachment_state',
      ),
      3,
    );
  });

  test('sync pushes local rows then pulls remote rows', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _SyncServer();
    server.addRemoteBundle([
      _bundleRow(
        'INSERT',
        {'id': 'remote'},
        {'id': 'remote', 'name': 'Remote'},
      ),
    ]);
    final client = _newClient(database, server);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');
    await database.connection.execute(
      "INSERT INTO users(id, name) VALUES('local', 'Local')",
    );

    final report = await client.sync();

    expect(report.pushOutcome, PushOutcome.committed);
    expect(report.remoteOutcome, RemoteSyncOutcome.alreadyAtTarget);
    expect(
      await _scalarText(database, "SELECT name FROM users WHERE id = 'local'"),
      'Local',
    );
  });

  test('syncThenDetach drains and clears local managed state', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final server = _SyncServer();
    final client = _newClient(database, server);
    addTearDown(client.close);
    await client.open();
    await client.attach('user-1');
    await database.connection.execute(
      "INSERT INTO users(id, name) VALUES('local', 'Local')",
    );

    final result = await client.syncThenDetach();

    expect(result.isSuccess, isTrue);
    expect(result.syncRounds, 1);
    expect(await _scalarInt(database, 'SELECT COUNT(*) FROM users'), 0);
    expect(
      await _scalarText(
        database,
        'SELECT binding_state FROM _sync_attachment_state',
      ),
      'anonymous',
    );
  });

  test(
    'source-recovery rebuild rotates source and preserves outbox replay',
    () async {
      final database = await _openUsersDatabase();
      addTearDown(database.close);
      final server = _SyncServer(sourceRetiredOnPushCreate: true);
      final client = _newClient(database, server);
      addTearDown(client.close);
      await client.open();
      await client.attach('user-1');
      await database.connection.execute(
        "INSERT INTO users(id, name) VALUES('local', 'Local')",
      );

      await expectLater(
        client.pushPending(),
        throwsA(isA<SourceRecoveryRequiredException>()),
      );
      server.sourceRetiredOnPushCreate = false;
      await client.rebuild();

      expect(
        await _scalarText(
          database,
          'SELECT current_source_id FROM _sync_attachment_state',
        ),
        'server-replacement-source',
      );
      expect(
        await _scalarText(
          database,
          'SELECT source_id FROM _sync_outbox_bundle WHERE singleton_key = 1',
        ),
        'server-replacement-source',
      );

      await client.pushPending();

      expect(
        await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_outbox_rows'),
        0,
      );
      expect(server.uploadedRows.single['payload'], {
        'id': 'local',
        'name': 'Local',
      });
    },
  );

  test(
    'snapshot rebuild applies self-referential rows under deferred FKs',
    () async {
      final database = await _openNodeDatabase();
      addTearDown(database.close);
      final server = _SyncServer(
        config: _nodeConfig,
        snapshotRows: [
          _snapshotRow(
            {'id': 'child'},
            {'id': 'child', 'parent_id': 'parent', 'name': 'Child'},
            table: 'nodes',
          ),
          _snapshotRow(
            {'id': 'parent'},
            {'id': 'parent', 'parent_id': null, 'name': 'Parent'},
            table: 'nodes',
          ),
        ],
      )..stableBundleSeq = 4;
      final client = _newClient(database, server, config: _nodeConfig);
      addTearDown(client.close);
      await client.open();
      await client.attach('user-1');

      final report = await client.rebuild();

      expect(report.outcome, RemoteSyncOutcome.appliedSnapshot);
      expect(await _scalarInt(database, 'SELECT COUNT(*) FROM nodes'), 2);
      expect(
        await _scalarText(
          database,
          "SELECT parent_id FROM nodes WHERE id = 'child'",
        ),
        'parent',
      );
    },
  );
}

DefaultOversqliteClient _newClient(
  SqliteNowDatabase database,
  OversqliteHttpClient http, {
  OversqliteConfig config = _usersConfig,
}) {
  return DefaultOversqliteClient(
    database: database,
    config: config,
    httpClient: http,
  );
}

const _usersConfig = OversqliteConfig(
  schema: 'main',
  syncTables: [SyncTable(tableName: 'users', syncKeyColumnName: 'id')],
);

const _nodeConfig = OversqliteConfig(
  schema: 'main',
  syncTables: [SyncTable(tableName: 'nodes', syncKeyColumnName: 'id')],
);

Future<SqliteNowDatabase> _openUsersDatabase() async {
  final database = SqliteNowDatabase.inMemory();
  await database.open(
    preInit: (connection) {
      return connection.execute(
        'CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)',
      );
    },
  );
  return database;
}

Future<SqliteNowDatabase> _openNodeDatabase() async {
  final database = SqliteNowDatabase.inMemory();
  await database.open(
    preInit: (connection) {
      return connection.execute('''CREATE TABLE nodes (
  id TEXT PRIMARY KEY NOT NULL,
  parent_id TEXT REFERENCES nodes(id) DEFERRABLE INITIALLY DEFERRED,
  name TEXT NOT NULL
)''');
    },
  );
  return database;
}

Future<int> _scalarInt(SqliteNowDatabase database, String sql) async {
  final rows = await database.connection.select(sql, (row) => row.readInt(0));
  return rows.single;
}

Future<String> _scalarText(SqliteNowDatabase database, String sql) async {
  final rows = await database.connection.select(
    sql,
    (row) => row.readString(0),
  );
  return rows.single;
}

Map<String, Object?> _bundleRow(
  String op,
  Map<String, String> key,
  Map<String, Object?>? payload, {
  String table = 'users',
  int? rowVersion,
}) {
  return {
    'schema': 'main',
    'table': table,
    'key': key,
    'op': op,
    'row_version': rowVersion ?? 1,
    'payload': payload,
  };
}

Map<String, Object?> _snapshotRow(
  Map<String, String> key,
  Map<String, Object?> payload, {
  String table = 'users',
}) {
  return {
    'schema': 'main',
    'table': table,
    'key': key,
    'row_version': 1,
    'payload': payload,
  };
}

final class _SyncServer implements OversqliteHttpClient {
  _SyncServer({
    this.connectResolution = 'initialize_empty',
    this.historyPrunedOnPull = false,
    this.sourceRetiredOnPushCreate = false,
    this.config = _usersConfig,
    List<Map<String, Object?>> snapshotRows = const [],
  }) : snapshotRows = [...snapshotRows];

  final String connectResolution;
  final bool historyPrunedOnPull;
  bool sourceRetiredOnPushCreate;
  final OversqliteConfig config;
  final bundles = <Map<String, Object?>>[];
  final uploadedRows = <Map<String, Object?>>[];
  final createRequests = <Map<String, Object?>>[];
  List<Map<String, Object?>> snapshotRows;
  var stableBundleSeq = 0;
  var _sourceId = '';
  var _nextBundleSeq = 1;

  void addRemoteBundle(List<Map<String, Object?>> rows) {
    final bundleSeq = _nextBundleSeq++;
    stableBundleSeq = bundleSeq;
    bundles.add({
      'bundle_seq': bundleSeq,
      'source_id': 'remote-source',
      'source_bundle_id': bundleSeq,
      'rows': [
        for (var i = 0; i < rows.length; i++)
          {...rows[i], 'row_version': rows[i]['row_version'] ?? i + 1},
      ],
    });
  }

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
      if (historyPrunedOnPull) {
        return _json({
          'error': 'history_pruned',
          'message': 'history pruned',
        }, statusCode: 409);
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
    if (path.startsWith('sync/snapshot-sessions/snapshot-1')) {
      final uri = Uri.parse('http://local/$path');
      final after = int.parse(uri.queryParameters['after_row_ordinal'] ?? '0');
      final maxRows = int.parse(uri.queryParameters['max_rows'] ?? '1000');
      final rows = snapshotRows.skip(after).take(maxRows).toList();
      return _json({
        'snapshot_id': 'snapshot-1',
        'snapshot_bundle_seq': stableBundleSeq,
        'rows': rows,
        'next_row_ordinal': after + rows.length,
        'has_more': after + rows.length < snapshotRows.length,
      });
    }
    if (path.startsWith('sync/committed-bundles/')) {
      return _json({
        'bundle_seq': stableBundleSeq,
        'source_id': _sourceId,
        'source_bundle_id': 1,
        'row_count': uploadedRows.length,
        'bundle_hash': 'hash',
        'rows': [
          for (var i = 0; i < uploadedRows.length; i++)
            {
              'schema': uploadedRows[i]['schema'],
              'table': uploadedRows[i]['table'],
              'key': uploadedRows[i]['key'],
              'op': uploadedRows[i]['op'],
              'row_version': i + 1,
              'payload': uploadedRows[i]['payload'],
            },
        ],
        'next_row_ordinal': uploadedRows.isEmpty ? -1 : uploadedRows.length - 1,
        'has_more': false,
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
      return _json({'resolution': connectResolution});
    }
    if (path == 'sync/snapshot-sessions') {
      return _json({
        'snapshot_id': 'snapshot-1',
        'snapshot_bundle_seq': stableBundleSeq,
        'row_count': snapshotRows.length,
        'byte_count': 0,
        'expires_at': '2099-01-01T00:00:00Z',
      });
    }
    if (path == 'sync/push-sessions') {
      final request = (body! as Map).cast<String, Object?>();
      createRequests.add(request);
      if (sourceRetiredOnPushCreate) {
        return _json({
          'error': 'source_retired',
          'message': 'source retired',
          'source_id': sourceId,
          'replaced_by_source_id': 'server-replacement-source',
        }, statusCode: 409);
      }
      uploadedRows.clear();
      return _json({
        'status': 'staging',
        'push_id': 'push-1',
        'planned_row_count': request['planned_row_count'],
        'next_expected_row_ordinal': 0,
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
      stableBundleSeq = stableBundleSeq == 0 ? 1 : stableBundleSeq + 1;
      snapshotRows = [
        for (final row in uploadedRows)
          if (row['op'] != 'DELETE')
            {
              'schema': row['schema'],
              'table': row['table'],
              'key': row['key'],
              'row_version': stableBundleSeq,
              'payload': row['payload'],
            },
      ];
      return _json({
        'bundle_seq': stableBundleSeq,
        'source_id': _sourceId,
        'source_bundle_id': 1,
        'row_count': uploadedRows.length,
        'bundle_hash': 'hash',
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

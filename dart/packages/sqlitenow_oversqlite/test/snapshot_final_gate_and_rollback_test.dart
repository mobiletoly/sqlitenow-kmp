import 'dart:async';
import 'dart:convert';

import 'package:sqlitenow_oversqlite/src/snapshot_gate.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:test/test.dart';

import 'support/behavior_fixture_support.dart';
import 'support/recording_sqlite_driver.dart';

void main() {
  test('final gate rejects every durable outbox tuple mutation', () async {
    const mutations = [
      "UPDATE _sync_outbox_bundle SET state = 'prepared' WHERE singleton_key = 1",
      "UPDATE _sync_outbox_bundle SET source_id = 'changed' WHERE singleton_key = 1",
      'UPDATE _sync_outbox_bundle SET source_bundle_id = 9 WHERE singleton_key = 1',
      "UPDATE _sync_outbox_bundle SET canonical_request_hash = 'changed' WHERE singleton_key = 1",
      'UPDATE _sync_outbox_bundle SET row_count = 1 WHERE singleton_key = 1',
      'UPDATE _sync_outbox_bundle SET remote_bundle_seq = 9 WHERE singleton_key = 1',
      "UPDATE _sync_outbox_bundle SET remote_bundle_hash = 'changed' WHERE singleton_key = 1",
      '''INSERT INTO _sync_outbox_rows(
  source_bundle_id, row_ordinal, schema_name, table_name, key_json,
  wire_key_json, op, base_row_version, local_payload, wire_payload
) VALUES (1, 1, 'main', 'users', '{"id":"secret-key"}',
          '{"id":"secret-key"}', 'INSERT', 0,
          '{"id":"secret-key","name":"secret-payload"}',
          '{"id":"secret-key","name":"secret-payload"}')''',
    ];

    for (final mutation in mutations) {
      final database = await openUsersDatabase();
      await OversqliteLocalRuntime(
        database: database,
        config: usersConfig,
      ).initialize();
      await _markAttachedRebuild(database);
      final gate = OversqliteSnapshotGate(database.connection);
      final pinned = await gate.pin(SnapshotRebuildOutboxMode.clearAll);
      await database.connection.execute(mutation);

      Object? caught;
      try {
        await database.connection.transaction(() async {
          await gate.validateFinal(
            pinned,
            const SnapshotSession(
              snapshotId: 'snapshot',
              snapshotBundleSeq: 7,
              rowCount: 0,
              byteCount: 0,
              expiresAt: '',
            ),
          );
        });
      } catch (error) {
        caught = error;
      }

      expect(caught, isA<SnapshotFinalApplyGateException>());
      expect(caught.toString(), isNot(contains('secret-key')));
      expect(caught.toString(), isNot(contains('secret-payload')));
      await database.close();
    }
  });

  test('dirty work appearing after pin is rejected before clear', () async {
    final database = await openUsersDatabase();
    addTearDown(database.close);
    await OversqliteLocalRuntime(
      database: database,
      config: usersConfig,
    ).initialize();
    await _markAttachedRebuild(database);
    final gate = OversqliteSnapshotGate(database.connection);
    final pinned = await gate.pin(SnapshotRebuildOutboxMode.clearAll);
    await database.connection.execute(
      "INSERT INTO users(id, name) VALUES('late-local', 'Must survive')",
    );

    await expectLater(
      database.connection.transaction(() async {
        await gate.validateFinal(
          pinned,
          const SnapshotSession(
            snapshotId: 'snapshot',
            snapshotBundleSeq: 7,
            rowCount: 0,
            byteCount: 0,
            expiresAt: '',
          ),
        );
        await database.connection.execute('DELETE FROM users');
      }),
      throwsA(isA<SnapshotFinalApplyGateException>()),
    );

    expect(
      await scalarText(
        database,
        "SELECT name FROM users WHERE id = 'late-local'",
      ),
      'Must survive',
    );
  });

  test('final gate fingerprint stays fixed-cardinality with 64 rows', () async {
    final recordingDriver = RecordingSqliteDriver();
    final database = await _openRecordedUsersDatabase(recordingDriver);
    addTearDown(database.close);
    await OversqliteLocalRuntime(
      database: database,
      config: usersConfig,
    ).initialize();
    await _markAttachedRebuild(database);
    final sourceId = await _attachmentSource(database);
    await _prepareSourceRecoveryOutbox(database, sourceId, rowCount: 64);
    final gate = OversqliteSnapshotGate(database.connection);
    recordingDriver.reset();
    final pinned = await gate.pin(
      SnapshotRebuildOutboxMode.preserveSourceRecovery,
    );
    await database.connection.transaction(
      () => gate.validateFinal(
        pinned,
        const SnapshotSession(
          snapshotId: 'snapshot',
          snapshotBundleSeq: 7,
          rowCount: 0,
          byteCount: 0,
          expiresAt: '',
        ),
      ),
      mode: TransactionMode.immediate,
    );

    bool isOutboxRowsSql(String sql) => sql.contains('_sync_outbox_rows');
    final outboxQueries = recordingDriver
        .selectCallsWhere(isOutboxRowsSql)
        .toList();
    expect(outboxQueries, hasLength(2));
    expect(outboxQueries.every((query) => query.returnedRowCount <= 1), isTrue);
    expect(
      outboxQueries.every(
        (query) => query.sql.trim() == 'SELECT COUNT(*) FROM _sync_outbox_rows',
      ),
      isTrue,
    );
    expect(recordingDriver.preparedSql.where(isOutboxRowsSql), isEmpty);
    for (final sql in recordingDriver.directSql.where(isOutboxRowsSql)) {
      final normalized = sql.toLowerCase();
      expect(normalized, isNot(contains('key_json')));
      expect(normalized, isNot(contains('payload')));
    }
  });

  test(
    'a later-page failure rolls back the replace and records held pages',
    () async {
      final config = _usersConfig(applyRows: 1);
      final database = await openUsersDatabase();
      addTearDown(database.close);
      final validated = await OversqliteLocalRuntime(
        database: database,
        config: config,
      ).initialize();
      await _markAttachedRebuild(database);
      await executeApplyModeSql(database, [
        "INSERT INTO users(id, name) VALUES('old', 'Old')",
      ]);
      final server = _SnapshotServer([
        _snapshotRow('remote-1', 'Remote one'),
        _snapshotRow('remote-2', 'Remote two'),
      ]);
      final api = OversqliteRemoteApi(server);
      var heldPages = 0;
      final runtime = OversqliteDownloadRuntime(
        database: database,
        config: config,
        remoteApi: api,
        capabilities: CapabilitiesResponse.fromJson(
          phase4CapabilitiesResponse(),
        ),
        faultInjector: SnapshotApplyFaultInjector(
          afterApplyPageLoaded: () async {
            heldPages++;
            if (heldPages == 2) throw StateError('later page failure');
          },
        ),
      );

      await expectLater(
        runtime.rebuildFromSnapshot(
          validated: validated,
          sourceId: (await _attachmentSource(database)),
          userId: 'user-1',
        ),
        throwsStateError,
      );

      expect(
        await scalarText(database, "SELECT name FROM users WHERE id = 'old'"),
        'Old',
      );
      expect(
        await scalarInt(
          database,
          "SELECT COUNT(*) FROM users WHERE id LIKE 'remote-%'",
        ),
        0,
      );
      expect(
        await scalarInt(
          database,
          'SELECT last_bundle_seq_seen FROM _sync_attachment_state',
        ),
        0,
      );
      final diagnostics = api.snapshotDiagnostics.snapshot();
      expect(diagnostics.applyPageCount, 2);
      expect(diagnostics.maxApplyPageRows, 1);
      expect(diagnostics.maxApplyPageBytes, 63);
      expect(diagnostics.maxApplyMetadataRows, 1);
      expect(diagnostics.maxApplyDriverRows, 1);
      expect(diagnostics.maxApplyDecodedRows, 1);
      expect(diagnostics.stagedRowCount, 2);
      expect(diagnostics.appliedRowCount, 0);
      expect(diagnostics.restoreDuration, greaterThan(Duration.zero));
    },
  );

  test(
    'a failure immediately before commit rolls back lifecycle and rows',
    () async {
      final config = _usersConfig();
      final database = await openUsersDatabase();
      addTearDown(database.close);
      final validated = await OversqliteLocalRuntime(
        database: database,
        config: config,
      ).initialize();
      await _markAttachedRebuild(database);
      await executeApplyModeSql(database, [
        "INSERT INTO users(id, name) VALUES('old', 'Old')",
      ]);
      final api = OversqliteRemoteApi(
        _SnapshotServer([_snapshotRow('remote', 'Remote')]),
      );

      await expectLater(
        OversqliteDownloadRuntime(
          database: database,
          config: config,
          remoteApi: api,
          capabilities: CapabilitiesResponse.fromJson(
            phase4CapabilitiesResponse(),
          ),
          faultInjector: SnapshotApplyFaultInjector(
            beforeCommit: () async => throw StateError('before commit failure'),
          ),
        ).rebuildFromSnapshot(
          validated: validated,
          sourceId: await _attachmentSource(database),
          userId: 'user-1',
        ),
        throwsStateError,
      );

      expect(
        await scalarText(database, "SELECT name FROM users WHERE id = 'old'"),
        'Old',
      );
      expect(
        await scalarInt(
          database,
          "SELECT COUNT(*) FROM users WHERE id = 'remote'",
        ),
        0,
      );
      expect(
        await scalarInt(
          database,
          'SELECT rebuild_required FROM _sync_attachment_state',
        ),
        1,
      );
      expect(
        await scalarText(database, 'SELECT kind FROM _sync_operation_state'),
        'none',
      );
    },
  );

  test('cancellation after a held page rolls back the replace', () async {
    final config = _usersConfig();
    final database = await openUsersDatabase();
    addTearDown(database.close);
    final validated = await OversqliteLocalRuntime(
      database: database,
      config: config,
    ).initialize();
    await _markAttachedRebuild(database);
    await executeApplyModeSql(database, [
      "INSERT INTO users(id, name) VALUES('old', 'Old')",
    ]);
    final cancellation = _CancellationSignal();
    final api = OversqliteRemoteApi(
      _SnapshotServer([_snapshotRow('remote', 'Remote')]),
    );

    await expectLater(
      OversqliteDownloadRuntime(
        database: database,
        config: config,
        remoteApi: api,
        capabilities: CapabilitiesResponse.fromJson(
          phase4CapabilitiesResponse(),
        ),
        faultInjector: SnapshotApplyFaultInjector(
          afterApplyPageLoaded: () async => throw cancellation,
        ),
      ).rebuildFromSnapshot(
        validated: validated,
        sourceId: await _attachmentSource(database),
        userId: 'user-1',
      ),
      throwsA(same(cancellation)),
    );

    expect(
      await scalarText(database, "SELECT name FROM users WHERE id = 'old'"),
      'Old',
    );
    expect(
      await scalarInt(
        database,
        "SELECT COUNT(*) FROM users WHERE id = 'remote'",
      ),
      0,
    );
    expect(api.snapshotDiagnostics.snapshot().applyPageCount, 1);
  });

  test('SQLite-full during apply rolls back the complete replace', () async {
    final config = _usersConfig();
    final database = await openUsersDatabase();
    addTearDown(database.close);
    final validated = await OversqliteLocalRuntime(
      database: database,
      config: config,
    ).initialize();
    await _markAttachedRebuild(database);
    await executeApplyModeSql(database, [
      "INSERT INTO users(id, name) VALUES('old', 'Old')",
    ]);
    final api = OversqliteRemoteApi(
      _SnapshotServer([_snapshotRow('remote-full', 'x' * (1024 * 1024))]),
    );

    await expectLater(
      OversqliteDownloadRuntime(
        database: database,
        config: config,
        remoteApi: api,
        capabilities: CapabilitiesResponse.fromJson(
          phase4CapabilitiesResponse(),
        ),
        faultInjector: SnapshotApplyFaultInjector(
          afterApplyPageLoaded: () async {
            final pageCount = await scalarInt(database, 'PRAGMA page_count');
            await database.connection.execute(
              'PRAGMA max_page_count = $pageCount',
            );
          },
        ),
      ).rebuildFromSnapshot(
        validated: validated,
        sourceId: await _attachmentSource(database),
        userId: 'user-1',
      ),
      throwsA(isA<SnapshotRowApplyException>()),
    );

    expect(
      await scalarText(database, "SELECT name FROM users WHERE id = 'old'"),
      'Old',
    );
    expect(
      await scalarInt(
        database,
        "SELECT COUNT(*) FROM users WHERE id = 'remote-full'",
      ),
      0,
    );
    expect(database.connection.isClosed, isFalse);
  });

  test(
    'a writer arriving after immediate ownership runs after snapshot commit',
    () async {
      final config = _usersConfig();
      final database = await openUsersDatabase();
      addTearDown(database.close);
      final validated = await OversqliteLocalRuntime(
        database: database,
        config: config,
      ).initialize();
      await _markAttachedRebuild(database);
      final entered = Completer<void>();
      final release = Completer<void>();
      final runtime = OversqliteDownloadRuntime(
        database: database,
        config: config,
        remoteApi: OversqliteRemoteApi(
          _SnapshotServer([_snapshotRow('remote', 'Remote')]),
        ),
        capabilities: CapabilitiesResponse.fromJson(
          phase4CapabilitiesResponse(),
        ),
        faultInjector: SnapshotApplyFaultInjector(
          afterApplyPageLoaded: () async {
            entered.complete();
            await release.future;
          },
        ),
      );
      final rebuild = runtime.rebuildFromSnapshot(
        validated: validated,
        sourceId: await _attachmentSource(database),
        userId: 'user-1',
      );
      await entered.future;
      final lateWrite = database.connection.execute(
        "INSERT INTO users(id, name) VALUES('late', 'Late')",
      );
      release.complete();

      await rebuild;
      await lateWrite;

      expect(
        await scalarInt(
          database,
          "SELECT COUNT(*) FROM users WHERE id = 'remote'",
        ),
        1,
      );
      expect(
        await scalarText(database, "SELECT name FROM users WHERE id = 'late'"),
        'Late',
      );
      expect(
        await scalarInt(database, 'SELECT COUNT(*) FROM _sync_dirty_rows'),
        1,
      );
    },
  );

  test(
    'a real deferred foreign-key commit failure rolls back and stays reusable',
    () async {
      final database = SqliteNowDatabase.inMemory();
      await database.open(
        preInit: (connection) async {
          await connection.execute(
            'CREATE TABLE parents (id TEXT PRIMARY KEY NOT NULL)',
          );
          await connection.execute('''CREATE TABLE children (
  id TEXT PRIMARY KEY NOT NULL,
  parent_id TEXT NOT NULL REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED
)''');
        },
      );
      addTearDown(database.close);
      final config = OversqliteConfig(
        schema: 'main',
        syncTables: [
          SyncTable(tableName: 'parents', syncKeyColumnName: 'id'),
          SyncTable(tableName: 'children', syncKeyColumnName: 'id'),
        ],
      );
      final validated = await OversqliteLocalRuntime(
        database: database,
        config: config,
      ).initialize();
      await _markAttachedRebuild(database);
      await executeApplyModeSql(database, [
        "INSERT INTO parents(id) VALUES('parent-old')",
        "INSERT INTO children(id, parent_id) VALUES('child-old', 'parent-old')",
      ]);
      final server = _SnapshotServer([
        {
          'schema': 'main',
          'table': 'children',
          'key': {'id': 'child-new'},
          'row_version': 1,
          'payload': {'id': 'child-new', 'parent_id': 'missing-parent'},
        },
      ]);

      await expectLater(
        OversqliteDownloadRuntime(
          database: database,
          config: config,
          remoteApi: OversqliteRemoteApi(server),
          capabilities: CapabilitiesResponse.fromJson(
            phase4CapabilitiesResponse(),
          ),
        ).rebuildFromSnapshot(
          validated: validated,
          sourceId: await _attachmentSource(database),
          userId: 'user-1',
        ),
        throwsA(anything),
      );

      expect(
        await scalarInt(
          database,
          "SELECT COUNT(*) FROM children WHERE id = 'child-old'",
        ),
        1,
      );
      expect(
        await scalarInt(
          database,
          "SELECT COUNT(*) FROM children WHERE id = 'child-new'",
        ),
        0,
      );
      expect(database.connection.isClosed, isFalse);
      expect(await scalarInt(database, 'SELECT 1'), 1);
    },
  );
}

OversqliteConfig _usersConfig({int applyRows = 1000}) => OversqliteConfig(
  schema: 'main',
  syncTables: [SyncTable(tableName: 'users', syncKeyColumnName: 'id')],
  snapshotApplyBatchRows: applyRows,
  snapshotApplyBatchBytes: 4 * 1024 * 1024,
);

Map<String, Object?> _snapshotRow(String id, String name) => {
  'schema': 'main',
  'table': 'users',
  'key': {'id': id},
  'row_version': 1,
  'payload': {'id': id, 'name': name},
};

Future<String> _attachmentSource(SqliteNowDatabase database) {
  return scalarText(
    database,
    'SELECT current_source_id FROM _sync_attachment_state',
  );
}

final class _SnapshotServer implements OversqliteHttpClient {
  _SnapshotServer(this.rows);

  final List<Map<String, Object?>> rows;
  static const snapshotId = 'snapshot-final-gate';
  static const bundleSeq = 7;

  @override
  Future<OversqliteHttpResponse> get(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async {
    final uri = Uri.parse('http://local/$path');
    final after = int.parse(uri.queryParameters['after_row_ordinal'] ?? '0');
    final maxRows = int.parse(uri.queryParameters['max_rows'] ?? '1000');
    final page = rows.skip(after).take(maxRows).toList();
    return _json({
      'snapshot_id': snapshotId,
      'snapshot_bundle_seq': bundleSeq,
      'rows': page,
      'byte_count': snapshotCompactWireByteCount(page),
      'next_row_ordinal': after + page.length,
      'has_more': after + page.length < rows.length,
    });
  }

  @override
  Future<OversqliteHttpResponse> postJson(
    String path, {
    required String sourceId,
    required Object? body,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async {
    return _json({
      'snapshot_id': snapshotId,
      'snapshot_bundle_seq': bundleSeq,
      'row_count': rows.length,
      'byte_count': snapshotCompactWireByteCount(rows),
      'expires_at': '2099-01-01T00:00:00Z',
    });
  }

  @override
  Future<OversqliteHttpResponse> delete(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async => const OversqliteHttpResponse(statusCode: 204, body: '');

  OversqliteHttpResponse _json(Map<String, Object?> body) =>
      OversqliteHttpResponse(statusCode: 200, body: jsonEncode(body));
}

final class _CancellationSignal implements Exception {}

Future<void> _markAttachedRebuild(SqliteNowDatabase database) async {
  await database.connection.execute('''UPDATE _sync_attachment_state
SET binding_state = 'attached',
    attached_user_id = 'user-1',
    schema_name = 'main',
    rebuild_required = 1
WHERE singleton_key = 1''');
}

Future<SqliteNowDatabase> _openRecordedUsersDatabase(
  RecordingSqliteDriver driver,
) async {
  final database = SqliteNowDatabase.inMemory(driver: driver);
  await database.open(
    preInit: (connection) => connection.execute(
      'CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)',
    ),
  );
  return database;
}

Future<void> _prepareSourceRecoveryOutbox(
  SqliteNowDatabase database,
  String sourceId, {
  required int rowCount,
}) async {
  await database.connection.execute('''UPDATE _sync_operation_state
SET kind = 'source_recovery', reason = 'history_pruned', replacement_source_id = 'replacement-source'
WHERE singleton_key = 1''');
  await database.connection.execute(
    '''UPDATE _sync_outbox_bundle
SET state = 'prepared', source_id = ?, source_bundle_id = 1,
    canonical_request_hash = 'pinned-hash', row_count = ?
WHERE singleton_key = 1''',
    parameters: [sourceId, rowCount],
  );
  for (var ordinal = 0; ordinal < rowCount; ordinal++) {
    final keyJson = jsonEncode({'id': 'local-$ordinal'});
    final payload = jsonEncode({
      'id': 'local-$ordinal',
      'name': 'Local $ordinal',
    });
    await database.connection.execute(
      '''INSERT INTO _sync_outbox_rows(
  source_bundle_id, row_ordinal, schema_name, table_name, key_json,
  wire_key_json, op, base_row_version, local_payload, wire_payload
) VALUES (1, ?, 'main', 'users', ?, ?, 'INSERT', 0, ?, ?)''',
      parameters: [ordinal, keyJson, keyJson, payload, payload],
    );
  }
}

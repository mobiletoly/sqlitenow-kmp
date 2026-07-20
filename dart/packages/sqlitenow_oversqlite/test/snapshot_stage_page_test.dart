import 'dart:convert';

import 'package:sqlitenow_oversqlite/src/download_stage_store.dart';
import 'package:sqlitenow_oversqlite/src/local_row_store.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:test/test.dart';

import 'support/behavior_fixture_support.dart';

void main() {
  test('empty stage returns an empty bounded page', () async {
    final database = await openUsersDatabase();
    addTearDown(database.close);
    final validated = await OversqliteLocalRuntime(
      database: database,
      config: usersConfig,
    ).initialize();
    final page =
        await OversqliteDownloadStageStore(
          connection: database.connection,
          localStore: OversqliteLocalRowStore(database.connection),
        ).loadStagedSnapshotPage(
          validated: validated,
          snapshotId: 'missing',
          afterRowOrdinal: 0,
          maxRows: 2,
          maxBytes: 1024,
        );

    expect(page.rows, isEmpty);
    expect(page.retainedTextBytes, 0);
    expect(page.metadataRowCount, 0);
    expect(page.driverRowCount, 0);
  });

  test('snapshot schema mismatch is fixed and redacted', () async {
    const schemaSecret = 'Bearer snapshot-schema-secret';
    final database = await openUsersDatabase();
    addTearDown(database.close);
    final validated = await OversqliteLocalRuntime(
      database: database,
      config: usersConfig,
    ).initialize();
    final store = OversqliteDownloadStageStore(
      connection: database.connection,
      localStore: OversqliteLocalRowStore(database.connection),
    );

    Object? caught;
    try {
      await store.stageSnapshotChunk(
        validated: validated,
        session: const SnapshotSession(
          snapshotId: 'snapshot-schema-redaction',
          snapshotBundleSeq: 1,
          rowCount: 1,
          byteCount: 1,
          expiresAt: '',
        ),
        chunk: const SnapshotChunkResponse(
          snapshotId: 'snapshot-schema-redaction',
          snapshotBundleSeq: 1,
          rows: [
            SnapshotRow(
              schema: schemaSecret,
              table: 'users',
              key: {'id': 'row-1'},
              rowVersion: 1,
              payload: {'id': 'row-1', 'name': 'Ada'},
            ),
          ],
          nextRowOrdinal: 1,
          hasMore: false,
          byteCount: 1,
        ),
        afterRowOrdinal: 0,
      );
    } catch (error) {
      caught = error;
    }

    expect(caught, isA<OversqliteProtocolException>());
    expect(
      (caught as OversqliteProtocolException).message,
      'snapshot row schema does not match the configured client schema',
    );
    expect(caught.toString(), isNot(contains(schemaSecret)));
    expect(await store.countSnapshotStageRows('snapshot-schema-redaction'), 0);
  });

  test('stage pages obey row and retained UTF-8 byte limits', () async {
    final database = await openUsersDatabase();
    addTearDown(database.close);
    final validated = await OversqliteLocalRuntime(
      database: database,
      config: usersConfig,
    ).initialize();
    final store = OversqliteDownloadStageStore(
      connection: database.connection,
      localStore: OversqliteLocalRowStore(database.connection),
    );
    final rows = [
      _row(1, 'one', 'Ada'),
      _row(2, 'two', 'Grace'),
      _row(3, 'three', 'Lin'),
    ];
    await _insertStageRows(database, rows);
    final firstTwoBytes = _retainedBytes(rows[0]) + _retainedBytes(rows[1]);

    final first = await store.loadStagedSnapshotPage(
      validated: validated,
      snapshotId: 'snapshot-page',
      afterRowOrdinal: 0,
      maxRows: 2,
      maxBytes: firstTwoBytes,
    );
    final second = await store.loadStagedSnapshotPage(
      validated: validated,
      snapshotId: 'snapshot-page',
      afterRowOrdinal: first.lastRowOrdinal!,
      maxRows: 2,
      maxBytes: firstTwoBytes,
    );

    expect(first.rows.map((row) => row.rowOrdinal), [1, 2]);
    expect(first.retainedTextBytes, firstTwoBytes);
    expect(first.metadataRowCount, 2);
    expect(first.driverRowCount, 2);
    expect(second.rows.map((row) => row.rowOrdinal), [3]);
    expect(second.lastRowOrdinal, 3);
  });

  test('one byte over the apply budget fails on the first row', () async {
    final database = await openUsersDatabase();
    addTearDown(database.close);
    final validated = await OversqliteLocalRuntime(
      database: database,
      config: usersConfig,
    ).initialize();
    final store = OversqliteDownloadStageStore(
      connection: database.connection,
      localStore: OversqliteLocalRowStore(database.connection),
    );
    final row = _row(1, 'oversized', 'secret-payload');
    await _insertStageRows(database, [row]);
    final retainedBytes = _retainedBytes(row);

    await expectLater(
      store.loadStagedSnapshotPage(
        validated: validated,
        snapshotId: 'snapshot-page',
        afterRowOrdinal: 0,
        maxRows: 1,
        maxBytes: retainedBytes - 1,
      ),
      throwsA(
        isA<SnapshotApplyRowTooLargeException>()
            .having((error) => error.rowOrdinal, 'rowOrdinal', 1)
            .having(
              (error) => error.retainedTextBytes,
              'retainedTextBytes',
              retainedBytes,
            ),
      ),
    );
  });

  test('the exact row and byte maxima are accepted', () async {
    final database = await openUsersDatabase();
    addTearDown(database.close);
    final validated = await OversqliteLocalRuntime(
      database: database,
      config: usersConfig,
    ).initialize();
    final store = OversqliteDownloadStageStore(
      connection: database.connection,
      localStore: OversqliteLocalRowStore(database.connection),
    );
    final row = _row(1, 'exact', 'Exact');
    await _insertStageRows(database, [row, _row(2, 'later', 'Later')]);

    final page = await store.loadStagedSnapshotPage(
      validated: validated,
      snapshotId: 'snapshot-page',
      afterRowOrdinal: 0,
      maxRows: 1,
      maxBytes: _retainedBytes(row),
    );

    expect(page.rows.map((item) => item.rowOrdinal), [1]);
    expect(page.retainedTextBytes, _retainedBytes(row));
    expect(page.metadataRowCount, 1);
    expect(page.driverRowCount, 1);
  });

  test('metadata/full-row drift is rejected deterministically', () async {
    final database = await openUsersDatabase();
    addTearDown(database.close);
    final validated = await OversqliteLocalRuntime(
      database: database,
      config: usersConfig,
    ).initialize();
    final store = OversqliteDownloadStageStore(
      connection: database.connection,
      localStore: OversqliteLocalRowStore(database.connection),
    );
    final rows = [_row(1, 'one', 'Ada'), _row(2, 'two', 'Grace')];
    await _insertStageRows(database, rows);
    store.afterMetadataPageLoadedForTest = () {
      return database.connection.execute(
        "DELETE FROM _sync_snapshot_stage WHERE snapshot_id = 'snapshot-page' AND row_ordinal = 2",
      );
    };

    await expectLater(
      store.loadStagedSnapshotPage(
        validated: validated,
        snapshotId: 'snapshot-page',
        afterRowOrdinal: 0,
        maxRows: 2,
        maxBytes: _retainedBytes(rows[0]) + _retainedBytes(rows[1]),
      ),
      throwsA(
        isA<OversqliteProtocolException>().having(
          (error) => error.message,
          'message',
          contains('changed between metadata and row load'),
        ),
      ),
    );
  });

  test('text and BLOB key failures do not expose key or payload values', () async {
    final database = SqliteNowDatabase.inMemory();
    await database.open(
      preInit: (connection) async {
        await connection.execute(
          'CREATE TABLE text_keys (id TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)',
        );
        await connection.execute(
          'CREATE TABLE blob_keys (id BLOB PRIMARY KEY NOT NULL, value TEXT NOT NULL)',
        );
      },
    );
    addTearDown(database.close);
    final config = OversqliteConfig(
      schema: 'main',
      syncTables: [
        SyncTable(tableName: 'text_keys', syncKeyColumnName: 'id'),
        SyncTable(tableName: 'blob_keys', syncKeyColumnName: 'id'),
      ],
    );
    final validated = await OversqliteLocalRuntime(
      database: database,
      config: config,
    ).initialize();
    final store = OversqliteDownloadStageStore(
      connection: database.connection,
      localStore: OversqliteLocalRowStore(database.connection),
    );
    const nestedSecret = 'nested-payload-secret';
    final cases = [
      (
        table: 'text_keys',
        keyJson: '{"id":"text-key-secret"',
        keySecret: 'text-key-secret',
      ),
      (
        table: 'blob_keys',
        keyJson: '{"id":"blob-key-secret"}',
        keySecret: 'blob-key-secret',
      ),
    ];
    for (var index = 0; index < cases.length; index++) {
      final testCase = cases[index];
      await database.connection.execute(
        '''INSERT INTO _sync_snapshot_stage(
  snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload
) VALUES (?, 1, 'main', ?, ?, 1, ?)''',
        parameters: [
          'redaction-$index',
          testCase.table,
          testCase.keyJson,
          jsonEncode({
            'id': testCase.keySecret,
            'value': {'nested': nestedSecret},
          }),
        ],
      );

      Object? caught;
      try {
        await store.loadStagedSnapshotPage(
          validated: validated,
          snapshotId: 'redaction-$index',
          afterRowOrdinal: 0,
          maxRows: 1,
          maxBytes: 4096,
        );
      } catch (error) {
        caught = error;
      }
      expect(caught, isA<SnapshotRowApplyException>());
      expect(caught.toString(), isNot(contains(testCase.keySecret)));
      expect(caught.toString(), isNot(contains(nestedSecret)));
    }
  });
}

Map<String, Object?> _row(int ordinal, String id, String name) {
  return {
    'rowOrdinal': ordinal,
    'schema': 'main',
    'table': 'users',
    'keyJson': jsonEncode({'id': id}),
    'rowVersion': ordinal,
    'payload': jsonEncode({'id': id, 'name': name}),
  };
}

int _retainedBytes(Map<String, Object?> row) {
  return utf8.encode(row['schema']! as String).length +
      utf8.encode(row['table']! as String).length +
      utf8.encode(row['keyJson']! as String).length +
      utf8.encode(row['payload']! as String).length;
}

Future<void> _insertStageRows(
  SqliteNowDatabase database,
  List<Map<String, Object?>> rows,
) async {
  for (final row in rows) {
    await database.connection.execute(
      '''INSERT INTO _sync_snapshot_stage(
  snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload
) VALUES (?, ?, ?, ?, ?, ?, ?)''',
      parameters: [
        'snapshot-page',
        row['rowOrdinal'],
        row['schema'],
        row['table'],
        row['keyJson'],
        row['rowVersion'],
        row['payload'],
      ],
    );
  }
}

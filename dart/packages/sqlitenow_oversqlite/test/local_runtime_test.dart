import 'dart:async';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

void main() {
  test(
    'initializes control tables, managed tables, triggers, and source state',
    () async {
      final database = await _openUsersDatabase();
      addTearDown(database.close);
      final runtime = OversqliteLocalRuntime(
        database: database,
        config: const OversqliteConfig(
          schema: 'main',
          syncTables: [SyncTable(tableName: 'users', syncKeyColumnName: 'id')],
        ),
      );

      final validated = await runtime.initialize();
      final source = await runtime.sourceInfo();

      expect(validated.tables.single.tableName, 'users');
      expect(source.currentSourceId, isNotEmpty);
      expect(
        await _scalarInt(database, "SELECT COUNT(*) FROM _sync_apply_state"),
        1,
      );
      expect(
        await _scalarInt(database, "SELECT COUNT(*) FROM _sync_managed_tables"),
        1,
      );
      expect(
        await _scalarInt(
          database,
          "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'trigger' AND tbl_name = 'users'",
        ),
        6,
      );
    },
  );

  test(
    'captures insert, update, delete, key-changing update, and apply-mode suppression',
    () async {
      final database = await _openUsersDatabase();
      addTearDown(database.close);
      final runtime = await _initializeUsersRuntime(database);

      await database.connection.execute(
        "INSERT INTO users(id, name) VALUES('insert-1', 'Ada')",
      );
      expect((await _dirtyRows(database)).single['op'], 'INSERT');

      await _clearDirtyRows(database);
      await _seedRemoteUser(
        database,
        id: 'update-1',
        name: 'Before',
        rowVersion: 7,
      );
      await database.connection.execute(
        "UPDATE users SET name = 'After' WHERE id = 'update-1'",
      );
      var dirty = await _dirtyRows(database);
      expect(dirty.single['op'], 'UPDATE');
      expect(dirty.single['baseRowVersion'], 7);

      await _clearDirtyRows(database);
      await database.connection.execute(
        "DELETE FROM users WHERE id = 'update-1'",
      );
      dirty = await _dirtyRows(database);
      expect(dirty.single['op'], 'DELETE');
      expect(dirty.single['payload'], isNull);

      await _clearDirtyRows(database);
      await _seedRemoteUser(
        database,
        id: 'key-old',
        name: 'Move',
        rowVersion: 3,
      );
      await database.connection.execute(
        "UPDATE users SET id = 'key-new' WHERE id = 'key-old'",
      );
      dirty = await _dirtyRows(database);
      expect(dirty.map((row) => '${row['keyJson']}:${row['op']}'), [
        '{"id":"key-old"}:DELETE',
        '{"id":"key-new"}:INSERT',
      ]);

      await _clearDirtyRows(database);
      await database.connection.execute(
        'UPDATE _sync_apply_state SET apply_mode = 1 WHERE singleton_key = 1',
      );
      await database.connection.execute(
        "INSERT INTO users(id, name) VALUES('apply-1', 'Remote')",
      );
      await database.connection.execute(
        'UPDATE _sync_apply_state SET apply_mode = 0 WHERE singleton_key = 1',
      );
      expect(await _dirtyRows(database), isEmpty);

      runtime.reportManagedTableChanges({'users'});
    },
  );

  test('captures BLOB primary keys and FK cascade deletes', () async {
    final database = await _openDatabase('''
CREATE TABLE users (
  id TEXT PRIMARY KEY NOT NULL,
  name TEXT NOT NULL
);
CREATE TABLE posts (
  id BLOB PRIMARY KEY NOT NULL,
  user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  title TEXT NOT NULL
);
''');
    addTearDown(database.close);
    final runtime = OversqliteLocalRuntime(
      database: database,
      config: const OversqliteConfig(
        schema: 'main',
        syncTables: [
          SyncTable(tableName: 'users', syncKeyColumnName: 'id'),
          SyncTable(tableName: 'posts', syncKeyColumnName: 'id'),
        ],
      ),
    );
    await runtime.initialize();
    await database.connection.execute(
      'UPDATE _sync_apply_state SET apply_mode = 1 WHERE singleton_key = 1',
    );
    await database.connection.execute(
      "INSERT INTO users(id, name) VALUES('user-1', 'Parent')",
    );
    await database.connection.execute(
      "INSERT INTO posts(id, user_id, title) VALUES(x'00112233445566778899aabbccddeeff', 'user-1', 'Child')",
    );
    await database.connection.execute(
      'UPDATE _sync_apply_state SET apply_mode = 0 WHERE singleton_key = 1',
    );
    await database.connection.execute(
      "INSERT INTO _sync_row_state(schema_name, table_name, key_json, row_version, deleted) VALUES('main', 'users', '{\"id\":\"user-1\"}', 10, 0)",
    );
    await database.connection.execute(
      "INSERT INTO _sync_row_state(schema_name, table_name, key_json, row_version, deleted) VALUES('main', 'posts', '{\"id\":\"00112233445566778899aabbccddeeff\"}', 11, 0)",
    );

    await database.connection.execute("DELETE FROM users WHERE id = 'user-1'");

    final dirty = await _dirtyRows(database);
    expect(
      dirty.map((row) => '${row['tableName']}:${row['keyJson']}:${row['op']}'),
      [
        'posts:{"id":"00112233445566778899aabbccddeeff"}:DELETE',
        'users:{"id":"user-1"}:DELETE',
      ],
    );
  });

  test('captures preexisting managed rows during first local bind', () async {
    final database = await _openUsersDatabase(
      seedSql: ["INSERT INTO users(id, name) VALUES('preexisting-1', 'Ada')"],
    );
    addTearDown(database.close);

    await _initializeUsersRuntime(database);

    final dirty = await _dirtyRows(database);
    expect(dirty.single['op'], 'INSERT');
    expect(dirty.single['keyJson'], '{"id":"preexisting-1"}');
    expect(dirty.single['payload'], '{"id":"preexisting-1","name":"Ada"}');
  });

  test('repeated initialize preserves source identity and triggers', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final runtime = await _initializeUsersRuntime(database);
    final sourceId = (await runtime.sourceInfo()).currentSourceId;

    await runtime.initialize();

    expect((await runtime.sourceInfo()).currentSourceId, sourceId);
    expect(
      await _scalarInt(
        database,
        "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'trigger' AND tbl_name = 'users'",
      ),
      6,
    );
  });

  test(
    'preserves unknown _sync application tables and repairs operation state table',
    () async {
      final database = await _openDatabase('''
CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL);
CREATE TABLE _sync_audit_log (id TEXT PRIMARY KEY NOT NULL);
INSERT INTO _sync_audit_log(id) VALUES('keep-me');
CREATE TABLE _sync_operation_state (
  singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
  kind TEXT NOT NULL DEFAULT 'none',
  target_user_id TEXT NOT NULL DEFAULT '',
  staged_snapshot_id TEXT NOT NULL DEFAULT '',
  snapshot_bundle_seq INTEGER NOT NULL DEFAULT 0,
  snapshot_row_count INTEGER NOT NULL DEFAULT 0,
  source_recovery_reason TEXT NOT NULL DEFAULT ''
);
INSERT INTO _sync_operation_state(singleton_key, kind) VALUES(1, 'none');
''');
      addTearDown(database.close);

      await _initializeUsersRuntime(database);

      expect(
        await _scalarInt(database, 'SELECT COUNT(*) FROM _sync_audit_log'),
        1,
      );
      final columns = await database.connection.select(
        'PRAGMA table_info(_sync_operation_state)',
        (row) => row.readString(1),
      );
      expect(columns, contains('reason'));
      expect(columns, contains('replacement_source_id'));
      expect(columns, isNot(contains('source_recovery_reason')));
    },
  );

  test(
    'write guards block local writes while a sync transition is pending',
    () async {
      final database = await _openUsersDatabase();
      addTearDown(database.close);
      await _initializeUsersRuntime(database);
      await database.connection.execute(
        "UPDATE _sync_operation_state SET kind = 'remote_replace' WHERE singleton_key = 1",
      );

      await expectLater(
        () => database.connection.execute(
          "INSERT INTO users(id, name) VALUES('blocked', 'Blocked')",
        ),
        throwsA(
          predicate(
            (error) => error.toString().contains('SYNC_TRANSITION_PENDING'),
          ),
        ),
      );
    },
  );

  test('reports invalidation for runtime-managed local writes', () async {
    final database = await _openUsersDatabase();
    addTearDown(database.close);
    final runtime = await _initializeUsersRuntime(database);
    final invalidated = expectLater(
      database.invalidationTracker.watchTables({'users'}),
      emits(anything),
    );

    await runtime.executeManagedWrite(
      "INSERT INTO users(id, name) VALUES('invalidate-1', 'Ada')",
      affectedTables: {'users'},
    );

    await invalidated;
  });

  test('rejects unsupported sync table configurations', () async {
    final integerDatabase = await _openDatabase(
      'CREATE TABLE users (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL);',
    );
    addTearDown(integerDatabase.close);
    await expectLater(
      () => _initializeUsersRuntime(integerDatabase),
      throwsA(
        predicate(
          (error) => error.toString().contains(
            'must be TEXT PRIMARY KEY or BLOB PRIMARY KEY',
          ),
        ),
      ),
    );

    final reservedDatabase = await _openDatabase(
      'CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, _sync_scope_id TEXT NOT NULL, name TEXT NOT NULL);',
    );
    addTearDown(reservedDatabase.close);
    await expectLater(
      () => _initializeUsersRuntime(reservedDatabase),
      throwsA(
        predicate(
          (error) => error.toString().contains(
            'reserved server column _sync_scope_id',
          ),
        ),
      ),
    );
  });
}

Future<SqliteNowDatabase> _openUsersDatabase({
  List<String> seedSql = const [],
}) {
  return _openDatabase('''
CREATE TABLE users (
  id TEXT PRIMARY KEY NOT NULL,
  name TEXT NOT NULL
);
${seedSql.join(';\n')}
''');
}

Future<SqliteNowDatabase> _openDatabase(String schemaSql) async {
  final database = SqliteNowDatabase.inMemory();
  await database.open(
    preInit: (connection) async {
      for (final statement in _splitSqlStatements(schemaSql)) {
        await connection.execute(statement);
      }
    },
  );
  return database;
}

Future<OversqliteLocalRuntime> _initializeUsersRuntime(
  SqliteNowDatabase database,
) async {
  final runtime = OversqliteLocalRuntime(
    database: database,
    config: const OversqliteConfig(
      schema: 'main',
      syncTables: [SyncTable(tableName: 'users', syncKeyColumnName: 'id')],
    ),
  );
  await runtime.initialize();
  return runtime;
}

Future<void> _seedRemoteUser(
  SqliteNowDatabase database, {
  required String id,
  required String name,
  required int rowVersion,
}) async {
  await database.connection.execute(
    'UPDATE _sync_apply_state SET apply_mode = 1 WHERE singleton_key = 1',
  );
  await database.connection.execute(
    "INSERT INTO users(id, name) VALUES('$id', '$name')",
  );
  await database.connection.execute(
    'UPDATE _sync_apply_state SET apply_mode = 0 WHERE singleton_key = 1',
  );
  await database.connection.execute(
    "INSERT INTO _sync_row_state(schema_name, table_name, key_json, row_version, deleted) VALUES('main', 'users', '{\"id\":\"$id\"}', $rowVersion, 0)",
  );
}

Future<List<Map<String, Object?>>> _dirtyRows(SqliteNowDatabase database) {
  return database.connection.select(
    '''SELECT schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal
FROM _sync_dirty_rows
ORDER BY dirty_ordinal, table_name, key_json''',
    (row) => {
      'schemaName': row.readString(0),
      'tableName': row.readString(1),
      'keyJson': row.readString(2),
      'op': row.readString(3),
      'baseRowVersion': row.readInt(4),
      'payload': row.readNullableString(5),
      'dirtyOrdinal': row.readInt(6),
    },
  );
}

Future<void> _clearDirtyRows(SqliteNowDatabase database) {
  return database.connection.execute('DELETE FROM _sync_dirty_rows');
}

Future<int> _scalarInt(SqliteNowDatabase database, String sql) async {
  final rows = await database.connection.select(sql, (row) => row.readInt(0));
  return rows.single;
}

List<String> _splitSqlStatements(String sql) {
  return sql
      .split(';')
      .map((statement) => statement.trim())
      .where((statement) => statement.isNotEmpty)
      .toList();
}

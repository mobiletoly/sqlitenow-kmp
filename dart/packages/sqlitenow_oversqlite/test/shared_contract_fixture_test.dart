import 'dart:convert';
import 'dart:io';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

void main() {
  final fixtureRoot = _repoRoot().uri
      .resolve('oversqlite-contracts/local-schema/')
      .toFilePath();

  test('Dart local runtime matches shared schema snapshots', () async {
    for (final fixture in _fixtures(fixtureRoot)) {
      final database = await _openFixtureDatabase(fixture);
      addTearDown(database.close);
      final config = _readConfig(fixture);
      final runtime = OversqliteLocalRuntime(
        database: database,
        config: config,
      );

      await runtime.initialize();

      final actual = await _dumpCatalogSnapshot(
        database,
        _fixtureName(fixture),
      );
      final expected = jsonDecode(
        File.fromUri(
          fixture.uri.resolve('schema.expected.json'),
        ).readAsStringSync(),
      );
      expect(actual, expected, reason: fixture.path);
      await database.close();
    }
  });

  test('Dart local runtime matches shared write-transition fixtures', () async {
    for (final fixture in _fixtures(fixtureRoot)) {
      final actual = await _runWriteTransitions(fixture);
      final expected = jsonDecode(
        File.fromUri(
          fixture.uri.resolve('write-transitions.expected.json'),
        ).readAsStringSync(),
      );
      expect(actual, expected, reason: fixture.path);
    }
  });
}

Future<Map<String, Object?>> _runWriteTransitions(Directory fixture) async {
  final spec =
      jsonDecode(
            File.fromUri(
              fixture.uri.resolve('write-transitions.json'),
            ).readAsStringSync(),
          )
          as Map<String, Object?>;
  final cases = spec['cases']! as List<Object?>;
  final applicationTables = (spec['applicationTables']! as List<Object?>)
      .cast<String>();
  final results = <Map<String, Object?>>[];
  for (final rawCase in cases.cast<Map<String, Object?>>()) {
    final database = await _openFixtureDatabase(fixture);
    final config = _readConfig(fixture);
    final runtime = OversqliteLocalRuntime(database: database, config: config);
    await runtime.initialize();
    for (final statement
        in (rawCase['setupSql']! as List<Object?>).cast<String>()) {
      await database.connection.execute(statement);
    }
    final expectedInvalidatedTables =
        ((rawCase['expectedInvalidatedTables'] as List<Object?>?) ?? const [])
            .cast<String>()
            .toSet();
    final invalidatedTables = <String>{};
    final subscription = expectedInvalidatedTables.isEmpty
        ? null
        : database.invalidationTracker
              .watchTables(expectedInvalidatedTables)
              .listen((_) {
                invalidatedTables.addAll(expectedInvalidatedTables);
              });
    for (final statement
        in (rawCase['actionSql']! as List<Object?>).cast<String>()) {
      if (expectedInvalidatedTables.isEmpty) {
        await database.connection.execute(statement);
      } else {
        await runtime.executeManagedWrite(
          statement,
          affectedTables: expectedInvalidatedTables,
        );
      }
    }
    await Future<void>.delayed(Duration.zero);
    await subscription?.cancel();
    expect(
      invalidatedTables.toList()..sort(),
      expectedInvalidatedTables.toList()..sort(),
      reason: '${fixture.path}/${rawCase['name']} invalidation',
    );
    results.add({
      'name': rawCase['name'],
      'dirtyRows': await _dumpDirtyRows(database),
      'applicationRowCounts': await _dumpApplicationRowCounts(
        database,
        applicationTables,
      ),
    });
    await database.close();
  }
  return {
    'formatVersion': 1,
    'fixture': _fixtureName(fixture),
    'cases': results,
  };
}

Future<SqliteNowDatabase> _openFixtureDatabase(Directory fixture) async {
  final schemaSql = File.fromUri(
    fixture.uri.resolve('schema.sql'),
  ).readAsStringSync();
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

OversqliteConfig _readConfig(Directory fixture) {
  final raw =
      jsonDecode(
            File.fromUri(fixture.uri.resolve('config.json')).readAsStringSync(),
          )
          as Map<String, Object?>;
  final syncTables = (raw['syncTables']! as List<Object?>)
      .cast<Map<String, Object?>>()
      .map(
        (table) => SyncTable(
          tableName: table['tableName']! as String,
          syncKeyColumnName: table['syncKeyColumnName']! as String,
        ),
      )
      .toList();
  return OversqliteConfig(
    schema: raw['schema']! as String,
    syncTables: syncTables,
  );
}

List<Directory> _fixtures(String fixtureRoot) {
  return Directory(fixtureRoot).listSync().whereType<Directory>().toList()
    ..sort((a, b) => a.path.compareTo(b.path));
}

String _fixtureName(Directory fixture) {
  return fixture.uri.pathSegments.where((segment) => segment.isNotEmpty).last;
}

Future<Map<String, Object?>> _dumpCatalogSnapshot(
  SqliteNowDatabase database,
  String fixtureName,
) async {
  final tableNames = await _namesByType(database, 'table');
  final viewNames = await _namesByType(database, 'view');
  final triggerNames = await _namesByType(database, 'trigger');
  return {
    'formatVersion': 1,
    'fixture': fixtureName,
    'catalog': await _dumpSqliteCatalog(database),
    'tables': [
      for (final tableName in tableNames) await _dumpTable(database, tableName),
    ],
    'views': [
      for (final viewName in viewNames) await _dumpView(database, viewName),
    ],
    'triggers': [
      for (final triggerName in triggerNames)
        await _dumpTrigger(database, triggerName),
    ],
  };
}

Future<List<Map<String, Object?>>> _dumpSqliteCatalog(
  SqliteNowDatabase database,
) {
  return database.connection.select(
    '''SELECT type, name, tbl_name, sql
FROM sqlite_schema
WHERE name NOT LIKE 'sqlite_%'
ORDER BY type, name''',
    (row) => {
      'type': row.readString(0),
      'name': row.readString(1),
      'tableName': row.readString(2),
      'sql': row.readString(0) == 'trigger'
          ? _normalizeTriggerSql(row.readString(3))
          : row.readNullableString(3),
    },
  );
}

Future<List<String>> _namesByType(SqliteNowDatabase database, String type) {
  return database.connection.select(
    '''SELECT name
FROM sqlite_schema
WHERE type = ? AND name NOT LIKE 'sqlite_%'
ORDER BY name''',
    (row) => row.readString(0),
    parameters: [type],
  );
}

Future<Map<String, Object?>> _dumpTable(
  SqliteNowDatabase database,
  String tableName,
) async {
  return {
    'name': tableName,
    'columns': await _dumpPragmaTableInfo(database, tableName),
    'foreignKeys': await _dumpPragmaForeignKeys(database, tableName),
    'indexes': await _dumpIndexes(database, tableName),
  };
}

Future<Map<String, Object?>> _dumpView(
  SqliteNowDatabase database,
  String viewName,
) async {
  final rows = await database.connection.select(
    "SELECT sql FROM sqlite_schema WHERE type = 'view' AND name = ?",
    (row) => {'name': viewName, 'sql': row.readNullableString(0)},
    parameters: [viewName],
  );
  return rows.single;
}

Future<Map<String, Object?>> _dumpTrigger(
  SqliteNowDatabase database,
  String triggerName,
) async {
  final rows = await database.connection.select(
    "SELECT tbl_name, sql FROM sqlite_schema WHERE type = 'trigger' AND name = ?",
    (row) => {
      'name': triggerName,
      'tableName': row.readString(0),
      'sql': _normalizeTriggerSql(row.readString(1)),
    },
    parameters: [triggerName],
  );
  return rows.single;
}

Future<List<Map<String, Object?>>> _dumpPragmaTableInfo(
  SqliteNowDatabase database,
  String tableName,
) {
  return database.connection.select(
    'PRAGMA table_info(${_quoteIdent(tableName)})',
    (row) => {
      'cid': row.readInt(0),
      'name': row.readString(1),
      'type': row.readString(2),
      'notNull': row.readInt(3) == 1,
      'defaultValue': row.readNullableString(4),
      'primaryKeyPosition': row.readInt(5),
    },
  );
}

Future<List<Map<String, Object?>>> _dumpPragmaForeignKeys(
  SqliteNowDatabase database,
  String tableName,
) {
  return database.connection.select(
    'PRAGMA foreign_key_list(${_quoteIdent(tableName)})',
    (row) => {
      'id': row.readInt(0),
      'seq': row.readInt(1),
      'table': row.readString(2),
      'from': row.readString(3),
      'to': row.readString(4),
      'onUpdate': row.readString(5),
      'onDelete': row.readString(6),
      'match': row.readString(7),
    },
  );
}

Future<List<Map<String, Object?>>> _dumpIndexes(
  SqliteNowDatabase database,
  String tableName,
) async {
  final indexes = await database.connection.select(
    'PRAGMA index_list(${_quoteIdent(tableName)})',
    (row) => (
      seq: row.readInt(0),
      name: row.readString(1),
      unique: row.readInt(2) == 1,
      origin: row.readString(3),
      partial: row.readInt(4) == 1,
    ),
  );
  return [
    for (final index in indexes)
      {
        'seq': index.seq,
        'name': index.name,
        'unique': index.unique,
        'origin': index.origin,
        'partial': index.partial,
        'columns': await _dumpIndexColumns(database, index.name),
      },
  ];
}

Future<List<Map<String, Object?>>> _dumpIndexColumns(
  SqliteNowDatabase database,
  String indexName,
) {
  return database.connection.select(
    'PRAGMA index_xinfo(${_quoteIdent(indexName)})',
    (row) => {
      'seqno': row.readInt(0),
      'cid': row.readInt(1),
      'name': row.readNullableString(2),
      'desc': row.readInt(3) == 1,
      'collation': row.readNullableString(4),
      'key': row.readInt(5) == 1,
    },
  );
}

Future<List<Map<String, Object?>>> _dumpDirtyRows(SqliteNowDatabase database) {
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

Future<Map<String, Object?>> _dumpApplicationRowCounts(
  SqliteNowDatabase database,
  List<String> tables,
) async {
  final result = <String, Object?>{};
  for (final table in [...tables]..sort()) {
    final rows = await database.connection.select(
      'SELECT COUNT(*) FROM ${_quoteIdent(table)}',
      (row) => row.readInt(0),
    );
    result[table] = rows.single;
  }
  return result;
}

List<String> _splitSqlStatements(String sql) {
  return sql
      .split(';')
      .map((statement) => statement.trim())
      .where((statement) => statement.isNotEmpty)
      .toList();
}

String _quoteIdent(String identifier) =>
    '"${identifier.replaceAll('"', '""')}"';

String _normalizeTriggerSql(String sql) {
  final collapsed = sql.trim().replaceAll(RegExp(r'\s+'), ' ');
  return collapsed
      .replaceFirst(
        RegExp(
          r'^CREATE\s+TRIGGER\s+IF\s+NOT\s+EXISTS\s+',
          caseSensitive: false,
        ),
        'CREATE TRIGGER ',
      )
      .replaceFirst(
        RegExp(r'^CREATE\s+TRIGGER\s+', caseSensitive: false),
        'CREATE TRIGGER ',
      );
}

Directory _repoRoot() {
  var current = Directory.current;
  while (true) {
    if (File.fromUri(current.uri.resolve('settings.gradle.kts')).existsSync()) {
      return current;
    }
    final parent = current.parent;
    if (parent.path == current.path) {
      throw StateError(
        'Could not locate repository root from ${Directory.current.path}',
      );
    }
    current = parent;
  }
}

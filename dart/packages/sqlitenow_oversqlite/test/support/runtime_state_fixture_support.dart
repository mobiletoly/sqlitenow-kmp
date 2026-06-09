import 'dart:convert';
import 'dart:io';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';

import 'behavior_fixture_support.dart';

Future<Map<String, Object?>> dumpRuntimeSchema(
  SqliteNowDatabase database,
) async {
  final normalizer = SourceIdNormalizer();
  final tableNames = await _runtimeTableNames(database);
  return {
    'formatVersion': 1,
    'fixture': 'users',
    'pragmas': {
      'foreignKeys': await _scalarInt(database, 'PRAGMA foreign_keys'),
    },
    'tables': [
      for (final tableName in tableNames)
        {
          'name': tableName,
          'columns': await _dumpTableColumns(database, tableName),
        },
    ],
    'indexes': await _dumpRuntimeIndexes(database, tableNames),
    'triggers': await _dumpRuntimeTriggers(database),
    'state': await _dumpRuntimeStateRows(database, normalizer),
  };
}

Future<Map<String, Object?>> dumpRuntimeState(
  SqliteNowDatabase database,
) async {
  return {
    'formatVersion': 1,
    'state': await _dumpRuntimeStateRows(database, SourceIdNormalizer()),
  };
}

Object? readRuntimeStateFixture(String relativePath) {
  final file = File.fromUri(repoRoot().uri.resolve(relativePath));
  return jsonDecode(file.readAsStringSync());
}

DefaultOversqliteClient newRuntimeStateClient(
  SqliteNowDatabase database,
  OversqliteHttpClient http,
) {
  return DefaultOversqliteClient(
    database: database,
    config: const OversqliteConfig(
      schema: 'main',
      syncTables: [SyncTable(tableName: 'users', syncKeyColumnName: 'id')],
    ),
    httpClient: http,
  );
}

Future<void> executeRuntimeStateSql(
  SqliteNowDatabase database,
  List<String> statements,
) {
  return executeSetupSql(database, statements);
}

Future<List<String>> _runtimeTableNames(SqliteNowDatabase database) {
  return database.connection.select('''SELECT name
FROM sqlite_schema
WHERE type = 'table' AND name LIKE '_sync_%'
ORDER BY name''', (row) => row.readString(0));
}

Future<List<Map<String, Object?>>> _dumpTableColumns(
  SqliteNowDatabase database,
  String tableName,
) {
  return database.connection.select(
    'PRAGMA table_info(${_quoteIdent(tableName)})',
    (row) => {
      'name': row.readString(1),
      'type': row.readString(2),
      'notNull': row.readInt(3) == 1,
      'defaultValue': row.readNullableString(4),
      'primaryKeyPosition': row.readInt(5),
    },
  );
}

Future<List<Map<String, Object?>>> _dumpRuntimeIndexes(
  SqliteNowDatabase database,
  List<String> tableNames,
) async {
  final indexes = <Map<String, Object?>>[];
  for (final tableName in tableNames) {
    final summaries = await database.connection.select(
      'PRAGMA index_list(${_quoteIdent(tableName)})',
      (row) => (name: row.readString(1), unique: row.readInt(2) == 1),
    );
    for (final summary in summaries) {
      if (summary.name.startsWith('sqlite_')) continue;
      indexes.add({
        'name': summary.name,
        'unique': summary.unique,
        'table': tableName,
        'columns': await _dumpIndexColumns(database, summary.name),
      });
    }
  }
  indexes.sort(
    (a, b) => (a['name']! as String).compareTo(b['name']! as String),
  );
  return indexes;
}

Future<List<String>> _dumpIndexColumns(
  SqliteNowDatabase database,
  String indexName,
) {
  return database.connection.select(
    'PRAGMA index_info(${_quoteIdent(indexName)})',
    (row) => row.readString(2),
  );
}

Future<List<Map<String, Object?>>> _dumpRuntimeTriggers(
  SqliteNowDatabase database,
) {
  return database.connection.select(
    '''SELECT name, tbl_name, sql
FROM sqlite_schema
WHERE type = 'trigger' AND sql LIKE '%_sync_%'
ORDER BY name''',
    (row) => {
      'name': row.readString(0),
      'table': row.readString(1),
      'sql': _normalizeTriggerSql(row.readString(2)),
    },
  );
}

Future<Map<String, Object?>> _dumpRuntimeStateRows(
  SqliteNowDatabase database,
  SourceIdNormalizer normalizer,
) async {
  final result = <String, Object?>{};
  for (final table in _runtimeStateTables) {
    result[table.name.substring('_sync_'.length)] = await _dumpRows(
      database,
      table,
      normalizer,
    );
  }
  return result;
}

Future<List<Map<String, Object?>>> _dumpRows(
  SqliteNowDatabase database,
  RuntimeStateTable table,
  SourceIdNormalizer normalizer,
) async {
  final columnTypes = await _columnTypes(database, table.name);
  return database.connection.select(
    'SELECT * FROM ${_quoteIdent(table.name)} ORDER BY ${table.orderBy}',
    (row) {
      final result = <String, Object?>{};
      for (var i = 0; i < columnTypes.length; i++) {
        final column = columnTypes[i];
        result[column.name] = _normalizedValue(row, i, column, normalizer);
      }
      return result;
    },
  );
}

Future<List<RuntimeStateColumn>> _columnTypes(
  SqliteNowDatabase database,
  String tableName,
) {
  return database.connection.select(
    'PRAGMA table_info(${_quoteIdent(tableName)})',
    (row) => RuntimeStateColumn(
      name: row.readString(1),
      declaredType: row.readString(2).toUpperCase(),
    ),
  );
}

Object? _normalizedValue(
  SqliteRowReader row,
  int index,
  RuntimeStateColumn column,
  SourceIdNormalizer normalizer,
) {
  final raw = row.readValue(index);
  if (raw == null) return null;
  if (column.name == 'created_at' || column.name == 'updated_at') {
    return '<timestamp>';
  }
  if (column.name == 'canonical_request_hash' ||
      column.name == 'remote_bundle_hash') {
    final text = row.readString(index);
    return text.isEmpty ? '' : '<hash>';
  }
  if (_isSourceIdColumn(column.name)) {
    final text = row.readString(index);
    return text.isEmpty ? '' : normalizer.normalize(text);
  }
  if (column.declaredType.contains('INT')) {
    return row.readInt(index);
  }
  return row.readString(index);
}

bool _isSourceIdColumn(String columnName) {
  return columnName == 'source_id' ||
      columnName == 'current_source_id' ||
      columnName == 'replaced_by_source_id' ||
      columnName == 'replacement_source_id';
}

Future<int> _scalarInt(SqliteNowDatabase database, String sql) async {
  final rows = await database.connection.select(sql, (row) => row.readInt(0));
  return rows.single;
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

final class SourceIdNormalizer {
  final _ids = <String, String>{};

  String normalize(String value) {
    return _ids.putIfAbsent(value, () => '<source-${_ids.length + 1}>');
  }
}

final class RuntimeStateTable {
  const RuntimeStateTable(this.name, this.orderBy);

  final String name;
  final String orderBy;
}

final class RuntimeStateColumn {
  const RuntimeStateColumn({required this.name, required this.declaredType});

  final String name;
  final String declaredType;
}

const _runtimeStateTables = [
  RuntimeStateTable('_sync_apply_state', 'singleton_key'),
  RuntimeStateTable('_sync_attachment_state', 'singleton_key'),
  RuntimeStateTable('_sync_source_state', 'source_id'),
  RuntimeStateTable('_sync_operation_state', 'singleton_key'),
  RuntimeStateTable('_sync_outbox_bundle', 'singleton_key'),
  RuntimeStateTable('_sync_outbox_rows', 'source_bundle_id, row_ordinal'),
  RuntimeStateTable('_sync_dirty_rows', 'dirty_ordinal, table_name, key_json'),
  RuntimeStateTable('_sync_row_state', 'schema_name, table_name, key_json'),
  RuntimeStateTable('_sync_snapshot_stage', 'snapshot_id, row_ordinal'),
  RuntimeStateTable('_sync_managed_tables', 'schema_name, table_name'),
];

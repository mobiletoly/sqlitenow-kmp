import 'dart:convert';
import 'dart:io';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

import 'support/behavior_fixture_support.dart';

void main() {
  final spec = _readFixture();
  final cases = (spec['cases']! as List<Object?>).cast<Map<String, Object?>>();

  test('Dart shared local config validation fixture matches runtime', () async {
    for (final fixture in cases) {
      await _runCase(fixture);
    }
  });
}

Future<void> _runCase(Map<String, Object?> fixture) async {
  final database = SqliteNowDatabase.inMemory();
  await database.open(
    preInit: (connection) async {
      for (final statement
          in (fixture['schemaSql']! as List<Object?>).cast<String>()) {
        await connection.execute(statement);
      }
    },
  );
  try {
    Object? error;
    try {
      final runtime = OversqliteLocalRuntime(
        database: database,
        config: _configFor(fixture),
      );
      await runtime.initialize();
    } catch (caught) {
      error = caught;
    }
    _expectError(fixture, error);
    if (error == null) {
      for (final statement
          in (fixture['afterOpenSql'] as List<Object?>? ?? const [])
              .cast<String>()) {
        await database.connection.execute(statement);
      }
      for (final rawQuery
          in (fixture['expectedQueries'] as List<Object?>? ?? const [])
              .cast<Map<String, Object?>>()) {
        final rows = await database.connection.select(
          rawQuery['sql']! as String,
          (row) => row.readValue(0),
        );
        expect(
          rows.single,
          rawQuery['value'],
          reason: fixture['name']! as String,
        );
      }
    }
  } finally {
    await database.close();
  }
}

OversqliteConfig _configFor(Map<String, Object?> fixture) {
  final rawConfig = (fixture['config']! as Map).cast<String, Object?>();
  final rawTables = (rawConfig['syncTables']! as List<Object?>)
      .cast<Map<String, Object?>>();
  final syncTables = <SyncTable>[];
  for (final table in rawTables) {
    final name = table['tableName']! as String;
    final columns = (table['syncKeyColumns'] as List<Object?>?)?.cast<String>();
    final column = table['syncKeyColumnName'] as String?;
    if (columns != null && columns.isNotEmpty) {
      throw StateError('table $name must declare exactly one sync key column');
    }
    if (column == null || column.trim().isEmpty) {
      throw StateError(
        'table $name must declare syncKeyColumnName or syncKeyColumns explicitly',
      );
    }
    syncTables.add(SyncTable(tableName: name, syncKeyColumnName: column));
  }
  return OversqliteConfig(
    schema: rawConfig['schema']! as String,
    syncTables: syncTables,
  );
}

void _expectError(Map<String, Object?> fixture, Object? error) {
  final expected = fixture['expectedError'];
  if (expected == null) {
    expect(error, isNull, reason: fixture['name']! as String);
    return;
  }
  expect(error, isNotNull, reason: fixture['name']! as String);
  final expectedError = (expected as Map).cast<String, Object?>();
  for (final fragment
      in (expectedError['messageContains']! as List<Object?>).cast<String>()) {
    expect(
      error.toString(),
      contains(fragment),
      reason: fixture['name']! as String,
    );
  }
}

Map<String, Object?> _readFixture() {
  final file = File.fromUri(
    repoRoot().uri.resolve(
      'oversqlite-contracts/local-schema/config-validation.json',
    ),
  );
  return jsonDecode(file.readAsStringSync()) as Map<String, Object?>;
}

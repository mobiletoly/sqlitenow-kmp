import 'dart:convert';
import 'dart:io';

import 'generated/rich_real_server_db.dart';
import 'generated/sync_dart_db.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

void main() {
  test('generated database exposes Oversqlite metadata only', () {
    expect(SyncDartDb.syncTables, hasLength(1));
    expect(SyncDartDb.syncTables.single.tableName, 'docs');
    expect(SyncDartDb.syncTables.single.syncKeyColumnName, 'doc_id');

    final database = SyncDartDb.inMemory();
    final config = database.buildOversqliteConfig(schema: 'main');

    expect(config.schema, 'main');
    expect(config.syncTables, SyncDartDb.syncTables);
    expect(config.uploadLimit, 200);
    expect(config.downloadLimit, 1000);
    expect(config.verboseLogs, isFalse);
    expect(config.automaticDownloadInterval, const Duration(seconds: 60));
    expect(config.bundleChangeWatchMode, BundleChangeWatchMode.off);
    expect(config.bundleChangeWatchReconnectMin, const Duration(seconds: 1));
    expect(config.bundleChangeWatchReconnectMax, const Duration(seconds: 60));

    final watchConfig = database.buildOversqliteConfig(
      schema: 'main',
      automaticDownloadInterval: const Duration(milliseconds: 25),
      bundleChangeWatchMode: BundleChangeWatchMode.auto,
      bundleChangeWatchReconnectMin: const Duration(milliseconds: 10),
      bundleChangeWatchReconnectMax: const Duration(milliseconds: 20),
    );
    expect(
      watchConfig.automaticDownloadInterval,
      const Duration(milliseconds: 25),
    );
    expect(watchConfig.bundleChangeWatchMode, BundleChangeWatchMode.auto);
    expect(
      watchConfig.bundleChangeWatchReconnectMin,
      const Duration(milliseconds: 10),
    );
    expect(
      watchConfig.bundleChangeWatchReconnectMax,
      const Duration(milliseconds: 20),
    );

    final client = database.newOversqliteClient(
      schema: 'main',
      httpClient: _NoopHttpClient(),
      automaticDownloadInterval: const Duration(milliseconds: 25),
      bundleChangeWatchMode: BundleChangeWatchMode.auto,
    );
    expect(client, isA<DefaultOversqliteClient>());
  });

  test('generated rich realserver database matches shared manifest', () async {
    final manifest = _readRichSchemaManifest();
    expect(manifest['formatVersion'], 1);
    expect(manifest['fixture'], 'business-rich-v0');
    expect(manifest['schema'], 'business');
    expect(
      (manifest['numericScenarios'] as List<Object?>)
          .map((scenario) => (scenario as Map<String, Object?>)['name'])
          .toList(),
      <String>[
        'signed-64-min',
        'signed-64-max',
        'above-javascript-safe-range',
        'binary64-negative-zero',
        'binary64-subnormal',
        'binary64-ordinary',
        'binary64-maximum-finite',
        'postgres-float4-authoritative-spelling',
        'boolean-false',
        'boolean-true',
      ],
    );

    final tables = ((manifest['tables']! as List<Object?>)
        .cast<Map<String, Object?>>());
    final expectedSyncTables =
        tables.map((table) => table['name']! as String).toList()..sort();
    final actualSyncTables =
        RichRealServerDb.syncTables.map((table) => table.tableName).toList()
          ..sort();
    expect(actualSyncTables, expectedSyncTables);
    expect(
      RichRealServerDb.syncTables.map((table) => table.syncKeyColumnName),
      everyElement('id'),
    );

    final database = RichRealServerDb.inMemory();
    final config = database.buildOversqliteConfig(
      schema: 'business',
      uploadLimit: 8,
      downloadLimit: 8,
    );

    expect(config.schema, 'business');
    expect(config.syncTables, RichRealServerDb.syncTables);
    expect(config.uploadLimit, 8);
    expect(config.downloadLimit, 8);

    await database.open();
    addTearDown(database.close);
    for (final table in tables) {
      await _expectTableMatchesManifest(database, table);
    }
  });
}

final class _NoopHttpClient implements OversqliteHttpClient {
  @override
  Future<OversqliteHttpResponse> get(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) {
    throw UnimplementedError();
  }

  @override
  Future<OversqliteHttpResponse> postJson(
    String path, {
    required String sourceId,
    required Object? body,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) {
    throw UnimplementedError();
  }

  @override
  Future<OversqliteHttpResponse> delete(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) {
    throw UnimplementedError();
  }
}

Future<void> _expectTableMatchesManifest(
  RichRealServerDb database,
  Map<String, Object?> table,
) async {
  final tableName = table['name']! as String;
  final columns = await _readColumns(database, tableName);
  final expectedColumns = ((table['columns']! as List<Object?>)
      .cast<Map<String, Object?>>()
      .map(
        (column) => {
          'name': column['name'],
          'logicalType': column['logicalType'],
          'nullable': column['nullable'],
        },
      )
      .toList());
  expect(
    columns.map((column) => column.manifestColumn).toList(),
    expectedColumns,
  );

  final expectedPrimaryKey = (table['primaryKey']! as List<Object?>)
      .cast<String>();
  final actualPrimaryKey =
      columns.where((column) => column.primaryKeyIndex > 0).toList()
        ..sort((a, b) => a.primaryKeyIndex.compareTo(b.primaryKeyIndex));
  expect(
    actualPrimaryKey.map((column) => column.name).toList(),
    expectedPrimaryKey,
  );

  final expectedForeignKeys =
      ((table['foreignKeys']! as List<Object?>)
            .cast<Map<String, Object?>>()
            .map(
              (foreignKey) => {
                'from': foreignKey['from'],
                'toTable': foreignKey['toTable'],
                'toColumn': foreignKey['toColumn'],
                'onDelete': foreignKey['onDelete'],
              },
            )
            .toList())
        ..sort(_compareForeignKeys);
  final actualForeignKeys = await _readForeignKeys(database, tableName)
    ..sort(_compareForeignKeys);
  expect(actualForeignKeys, expectedForeignKeys);
}

Future<List<_ActualColumn>> _readColumns(
  RichRealServerDb database,
  String tableName,
) {
  return database.connection.select('PRAGMA table_info($tableName)', (row) {
    final name = row.readString(1);
    return _ActualColumn(
      name: name,
      manifestColumn: {
        'name': name,
        'logicalType': _sqliteTypeToLogicalType(row.readString(2)),
        'nullable': row.readInt(3) == 0,
      },
      primaryKeyIndex: row.readInt(5),
    );
  });
}

Future<List<Map<String, Object?>>> _readForeignKeys(
  RichRealServerDb database,
  String tableName,
) {
  return database.connection.select('PRAGMA foreign_key_list($tableName)', (
    row,
  ) {
    return {
      'from': row.readString(3),
      'toTable': row.readString(2),
      'toColumn': row.readString(4),
      'onDelete': row.readString(6).toUpperCase(),
    };
  });
}

String _sqliteTypeToLogicalType(String type) {
  return switch (type.trim().toUpperCase()) {
    'TEXT' => 'text',
    'BLOB' => 'blob',
    'INTEGER' => 'integer',
    'REAL' => 'real',
    _ => type.trim().toLowerCase(),
  };
}

int _compareForeignKeys(Map<String, Object?> a, Map<String, Object?> b) {
  for (final key in ['from', 'toTable', 'toColumn']) {
    final comparison = (a[key]! as String).compareTo(b[key]! as String);
    if (comparison != 0) {
      return comparison;
    }
  }
  return 0;
}

Map<String, Object?> _readRichSchemaManifest() {
  final file = _repoRoot().uri
      .resolve('oversqlite-contracts/rich-schema/business-rich-v0.json')
      .toFilePath();
  return jsonDecode(File(file).readAsStringSync()) as Map<String, Object?>;
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

final class _ActualColumn {
  const _ActualColumn({
    required this.name,
    required this.manifestColumn,
    required this.primaryKeyIndex,
  });

  final String name;
  final Map<String, Object?> manifestColumn;
  final int primaryKeyIndex;
}

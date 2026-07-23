import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:sqlite3/sqlite3.dart';
import 'package:sqlitenow_flutter_samplesync/src/db/generated/now_sample_sync_database.dart';

void main() {
  test('Flutter and KMP SampleSync physical schemas stay equivalent', () {
    final root = _findRepositoryRoot();
    final flutterSchema = Directory(
      '${root.path}/dart/examples/flutter_samplesync/'
      'lib/src/db/sql/NowSampleSyncDatabase/schema',
    );
    final kmpSchema = Directory(
      '${root.path}/samplesync-kmp/composeApp/src/commonMain/sql/'
      'NowSampleSyncDatabase/schema',
    );

    final flutterDb = _buildSchema(flutterSchema);
    final kmpDb = _buildSchema(kmpSchema);
    addTearDown(flutterDb.close);
    addTearDown(kmpDb.close);

    for (final table in ['person', 'person_address', 'comment']) {
      expect(
        _tableContract(flutterDb, table),
        _tableContract(kmpDb, table),
        reason: '$table schema drifted',
      );
    }
  });

  test('generated sync metadata matches the SampleSync server contract', () {
    expect(
      [
        for (final table in NowSampleSyncDatabase.syncTables)
          '${table.tableName}/${table.syncKeyColumnName}',
      ],
      ['comment/id', 'person/id', 'person_address/id'],
    );
  });
}

Database _buildSchema(Directory directory) {
  final database = sqlite3.openInMemory();
  final files = directory.listSync().whereType<File>().toList()
    ..sort((left, right) => left.path.compareTo(right.path));
  for (final file in files) {
    database.execute(file.readAsStringSync());
  }
  return database;
}

Map<String, Object?> _tableContract(Database database, String table) {
  List<Map<String, Object?>> rows(String query) {
    return [
      for (final row in database.select(query))
        {for (final column in row.keys) column: row[column]},
    ];
  }

  final tableSql =
      database.select(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
            [table],
          ).single['sql']
          as String;
  final indexes = rows("PRAGMA index_list('$table')");
  return {
    'withoutRowId': tableSql.toUpperCase().contains('WITHOUT ROWID'),
    'columns': rows("PRAGMA table_info('$table')"),
    'foreignKeys': rows("PRAGMA foreign_key_list('$table')"),
    'indexes': [
      for (final index in indexes)
        {
          'name': index['name'],
          'unique': index['unique'],
          'origin': index['origin'],
          'partial': index['partial'],
          'columns': rows("PRAGMA index_info('${index['name']}')"),
        },
    ],
  };
}

Directory _findRepositoryRoot() {
  var directory = Directory.current.absolute;
  while (true) {
    if (File('${directory.path}/settings.gradle.kts').existsSync() &&
        Directory('${directory.path}/dart').existsSync()) {
      return directory;
    }
    final parent = directory.parent;
    if (parent.path == directory.path) {
      throw StateError('Could not locate sqlitenow-kmp repository root.');
    }
    directory = parent;
  }
}

import 'dart:io';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:test/test.dart';

import '../generated/dart_db.dart';
import 'test_support.dart';

void main() {
  group('migration conformance', () {
    test('generated database fresh bootstrap reopens idempotently', () async {
      final tempDir = await Directory.systemTemp.createTemp(
        'sqlitenow-conformance-fresh-',
      );
      addTearDown(() => tempDir.delete(recursive: true));
      final path = '${tempDir.path}/fresh.db';

      final first = DartDb(path: path, adapters: testAdapters);
      await first.open();
      await insertPerson(first, id: 1, name: 'Persisted');
      expect(await first.connection.readUserVersion(), 2);
      await first.close();

      final second = DartDb(path: path, adapters: testAdapters);
      addTearDown(second.close);
      await second.open();

      expect(await second.connection.readUserVersion(), 2);
      expect((await second.person.selectAll().asOne()).name, 'Persisted');
      expect(
        await _personIndexNames(second),
        isNot(contains('idx_person_name')),
      );
    });

    test('generated database upgrades an existing version-one schema', () async {
      final tempDir = await Directory.systemTemp.createTemp(
        'sqlitenow-conformance-upgrade-',
      );
      addTearDown(() => tempDir.delete(recursive: true));
      final path = '${tempDir.path}/upgrade.db';

      final v1 = SqliteNowDatabase(
        path: path,
        migrations: [
          SqliteNowMigrationStep(1, (connection) async {
            await connection.execute(
              'CREATE TABLE person('
              'id INTEGER PRIMARY KEY NOT NULL, '
              'name TEXT NOT NULL, '
              'status TEXT NOT NULL, '
              'score REAL, '
              'avatar BLOB'
              ')',
            );
            await connection.execute(
              "INSERT INTO person(id, name, status) VALUES (1, 'Existing', 'active')",
            );
          }),
        ],
      );
      await v1.open();
      await v1.close();

      final upgraded = DartDb(path: path, adapters: testAdapters);
      addTearDown(upgraded.close);
      await upgraded.open();

      expect(await upgraded.connection.readUserVersion(), 2);
      expect((await upgraded.person.selectAll().asOne()).name, 'Existing');
      expect(await _personIndexNames(upgraded), contains('idx_person_name'));
    });

    test('pending migration failure rolls back later steps', () async {
      final tempDir = await Directory.systemTemp.createTemp(
        'sqlitenow-conformance-rollback-',
      );
      addTearDown(() => tempDir.delete(recursive: true));
      final path = '${tempDir.path}/rollback.db';

      final v1 = SqliteNowDatabase(
        path: path,
        migrations: [
          SqliteNowMigrationStep(1, (connection) async {
            await connection.execute(
              'CREATE TABLE migration_log(version INTEGER PRIMARY KEY, label TEXT NOT NULL)',
            );
            await connection.execute(
              "INSERT INTO migration_log(version, label) VALUES (1, 'v1')",
            );
          }),
        ],
      );
      await v1.open();
      await v1.close();

      final failing = SqliteNowDatabase(
        path: path,
        migrations: [
          SqliteNowMigrationStep(2, (connection) {
            return connection.execute(
              "INSERT INTO migration_log(version, label) VALUES (2, 'v2')",
            );
          }),
          SqliteNowMigrationStep(3, (_) {
            throw StateError('boom');
          }),
        ],
      );

      await expectLater(failing.open(), throwsStateError);
      await failing.close();

      final verifier = SqliteNowDatabase(path: path);
      addTearDown(verifier.close);
      await verifier.open();
      expect(await verifier.connection.readUserVersion(), 1);
      expect(
        await verifier.connection.select(
          'SELECT label FROM migration_log ORDER BY version',
          (row) => row.readString(0),
        ),
        ['v1'],
      );
    });
  });
}

Future<List<String>> _personIndexNames(DartDb database) {
  return database.connection.select(
    'PRAGMA index_list(person)',
    (row) => row.readString(1),
  );
}

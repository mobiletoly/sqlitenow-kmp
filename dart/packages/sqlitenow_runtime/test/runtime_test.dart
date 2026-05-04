import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:test/test.dart';

void main() {
  group('SqliteNowDatabase lifecycle', () {
    test('opens, migrates, and closes one connection', () async {
      final database = SqliteNowDatabase.inMemory(
        migrations: [
          SqliteNowMigrationStep(1, (connection) {
            return connection.execute(
              'CREATE TABLE person(id INTEGER PRIMARY KEY, name TEXT NOT NULL)',
            );
          }),
        ],
      );

      expect(database.isOpen, isFalse);
      await database.open();
      expect(database.isOpen, isTrue);
      expect(await database.connection.readUserVersion(), 1);

      await database.close();
      expect(database.isOpen, isFalse);
      expect(() => database.connection, throwsStateError);
      expect(
        () => database.reportExternalTableChanges({'person'}),
        throwsStateError,
      );
    });

    test('rejects duplicate open', () async {
      final database = SqliteNowDatabase.inMemory();
      addTearDown(database.close);

      await database.open();

      await expectLater(database.open(), throwsStateError);
    });
  });

  group('serialized connection access', () {
    test('holds exclusive access across suspended transactions', () async {
      final database = SqliteNowDatabase.inMemory(
        migrations: [
          SqliteNowMigrationStep(1, (connection) {
            return connection.execute(
              'CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT NOT NULL)',
            );
          }),
        ],
      );
      addTearDown(database.close);
      await database.open();

      final transactionStarted = Completer<void>();
      final releaseTransaction = Completer<void>();
      final transaction = database.transaction(() async {
        await database.connection.execute(
          "INSERT INTO items(id, name) VALUES (1, 'outer')",
        );
        transactionStarted.complete();
        await releaseTransaction.future;
        await database.connection.execute(
          "INSERT INTO items(id, name) VALUES (2, 'inner')",
        );
      }, mode: TransactionMode.immediate);

      await transactionStarted.future;

      final concurrentRead = database.connection.select(
        'SELECT COUNT(*) FROM items',
        (row) => row.readInt(0),
      );

      final prematureResult = await _waitOrNull(
        concurrentRead,
        const Duration(milliseconds: 100),
      );
      expect(prematureResult, isNull);

      releaseTransaction.complete();
      await transaction;

      expect(await concurrentRead, [2]);
    });
  });

  group('transactions', () {
    test('commits nested transactions without savepoints', () async {
      final database = await _openItemsDatabase();
      addTearDown(database.close);

      await database.transaction(() async {
        await database.connection.execute(
          "INSERT INTO items(id, name) VALUES (1, 'outer')",
        );
        final result = await database.transaction(() async {
          await database.connection.execute(
            "INSERT INTO items(id, name) VALUES (2, 'inner')",
          );
          return 'ok';
        }, mode: TransactionMode.exclusive);
        expect(result, 'ok');
      }, mode: TransactionMode.immediate);

      expect(await _itemCount(database), 2);
    });

    test(
      'rolls back the outer transaction when a nested transaction fails',
      () async {
        final database = await _openItemsDatabase();
        addTearDown(database.close);

        await expectLater(
          database.transaction(() async {
            await database.connection.execute(
              "INSERT INTO items(id, name) VALUES (1, 'outer')",
            );
            await database.transaction(() async {
              await database.connection.execute(
                "INSERT INTO items(id, name) VALUES (2, 'inner')",
              );
              throw StateError('boom');
            }, mode: TransactionMode.exclusive);
          }, mode: TransactionMode.immediate),
          throwsStateError,
        );

        expect(await _itemCount(database), 0);
      },
    );
  });

  group('migrations', () {
    test(
      'fresh-only bootstrap skips incremental migrations on new databases',
      () async {
        final database = SqliteNowDatabase.inMemory(
          migrations: [
            SqliteNowMigrationStep.fresh(2, (connection) async {
              await connection.execute(
                'CREATE TABLE migration_log(version INTEGER PRIMARY KEY, label TEXT NOT NULL)',
              );
              await connection.execute(
                "INSERT INTO migration_log(version, label) VALUES (2, 'fresh')",
              );
            }),
            SqliteNowMigrationStep(1, (_) {
              throw StateError('fresh database should not run v1 migration');
            }),
            SqliteNowMigrationStep(2, (_) {
              throw StateError('fresh database should not run v2 migration');
            }),
          ],
        );
        addTearDown(database.close);

        await database.open();

        expect(await database.connection.readUserVersion(), 2);
        expect(
          await database.connection.select(
            'SELECT label FROM migration_log',
            (row) => row.readString(0),
          ),
          ['fresh'],
        );
      },
    );

    test('runs ordered migrations and stores user_version', () async {
      final database = SqliteNowDatabase.inMemory(
        migrations: [
          SqliteNowMigrationStep(2, (connection) {
            return connection.execute(
              "INSERT INTO migration_log(version, label) VALUES (2, 'second')",
            );
          }),
          SqliteNowMigrationStep(1, (connection) async {
            await connection.execute(
              'CREATE TABLE migration_log(version INTEGER PRIMARY KEY, label TEXT NOT NULL)',
            );
            await connection.execute(
              "INSERT INTO migration_log(version, label) VALUES (1, 'first')",
            );
          }),
        ],
      );
      addTearDown(database.close);

      await database.open();

      expect(await database.connection.readUserVersion(), 2);
      expect(
        await database.connection.select(
          'SELECT label FROM migration_log ORDER BY version',
          (row) => row.readString(0),
        ),
        ['first', 'second'],
      );
    });

    test('rejects duplicate migration versions', () {
      expect(
        () => SqliteNowDatabase.inMemory(
          migrations: [
            SqliteNowMigrationStep(1, (_) {}),
            SqliteNowMigrationStep(1, (_) {}),
          ],
        ),
        throwsArgumentError,
      );
    });

    test(
      'file-backed migration upgrades from an existing user_version',
      () async {
        final tempDir = await Directory.systemTemp.createTemp(
          'sqlitenow-migration-upgrade-',
        );
        addTearDown(() => tempDir.delete(recursive: true));
        final path = '${tempDir.path}/upgrade.db';

        final v1 = SqliteNowDatabase(
          path: path,
          migrations: [
            SqliteNowMigrationStep(1, (connection) async {
              await connection.execute(
                'CREATE TABLE migration_log('
                'version INTEGER PRIMARY KEY, '
                'label TEXT NOT NULL, '
                'extra TEXT'
                ')',
              );
              await connection.execute(
                "INSERT INTO migration_log(version, label) VALUES (1, 'v1')",
              );
            }),
          ],
        );
        await v1.open();
        await v1.close();

        final upgraded = SqliteNowDatabase(
          path: path,
          migrations: [
            SqliteNowMigrationStep.fresh(3, (connection) async {
              await connection.execute(
                'CREATE TABLE migration_log('
                'version INTEGER PRIMARY KEY, '
                'label TEXT NOT NULL, '
                'extra TEXT'
                ')',
              );
              await connection.execute(
                "INSERT INTO migration_log(version, label, extra) "
                "VALUES (3, 'fresh', 'fresh-only')",
              );
            }),
            SqliteNowMigrationStep(2, (connection) {
              return connection.execute(
                "INSERT INTO migration_log(version, label, extra) "
                "VALUES (2, 'v2', 'upgrade')",
              );
            }),
            SqliteNowMigrationStep(3, (connection) {
              return connection.execute(
                "INSERT INTO migration_log(version, label, extra) "
                "VALUES (3, 'v3', 'upgrade')",
              );
            }),
          ],
        );
        addTearDown(upgraded.close);

        await upgraded.open();

        expect(await upgraded.connection.readUserVersion(), 3);
        expect(
          await upgraded.connection.select(
            'SELECT label FROM migration_log ORDER BY version',
            (row) => row.readString(0),
          ),
          ['v1', 'v2', 'v3'],
        );
      },
    );

    test(
      'file-backed migration failure rolls back all pending steps',
      () async {
        final tempDir = await Directory.systemTemp.createTemp(
          'sqlitenow-migration-rollback-',
        );
        addTearDown(() => tempDir.delete(recursive: true));
        final path = '${tempDir.path}/rollback.db';

        final v1 = SqliteNowDatabase(
          path: path,
          migrations: [
            SqliteNowMigrationStep(1, (connection) async {
              await connection.execute(
                'CREATE TABLE migration_log('
                'version INTEGER PRIMARY KEY, '
                'label TEXT NOT NULL'
                ')',
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
      },
    );
  });

  group('prepared bind and read helpers', () {
    test('round-trips milestone scalar types and nulls', () async {
      final database = SqliteNowDatabase.inMemory(
        migrations: [
          SqliteNowMigrationStep(1, (connection) {
            return connection.execute(
              'CREATE TABLE scalars('
              'id INTEGER PRIMARY KEY, '
              'text_value TEXT, '
              'int_value INTEGER, '
              'real_value REAL, '
              'blob_value BLOB'
              ')',
            );
          }),
        ],
      );
      addTearDown(database.close);
      await database.open();

      final blob = Uint8List.fromList([1, 2, 3, 255]);
      await database.connection.usePrepared(
        'INSERT INTO scalars(id, text_value, int_value, real_value, blob_value) '
        'VALUES (?, ?, ?, ?, ?)',
        (statement) {
          statement.execute([1, 'Ada', 42, 3.5, blob]);
          statement.execute([2, null, null, null, null]);
        },
      );

      final rows = await database.connection.usePrepared(
        'SELECT text_value, int_value, real_value, blob_value FROM scalars ORDER BY id',
        (statement) {
          return statement.select((row) {
            return (
              text: row.readNullableString(0),
              integer: row.readNullableInt(1),
              real: row.readNullableDouble(2),
              blob: row.readNullableBlob(3),
            );
          });
        },
      );

      expect(rows[0].text, 'Ada');
      expect(rows[0].integer, 42);
      expect(rows[0].real, 3.5);
      expect(rows[0].blob, blob);
      expect(rows[1].text, isNull);
      expect(rows[1].integer, isNull);
      expect(rows[1].real, isNull);
      expect(rows[1].blob, isNull);
    });

    test('rejects unsupported bind values', () async {
      final database = await _openItemsDatabase();
      addTearDown(database.close);

      await expectLater(
        database.connection.execute(
          'INSERT INTO items(id, name) VALUES (?, ?)',
          parameters: [1, DateTime.utc(2026)],
        ),
        throwsArgumentError,
      );
    });

    test('encodes collection parameters for json_each clauses', () async {
      final database = await _openItemsDatabase();
      addTearDown(database.close);

      await database.connection.execute(
        "INSERT INTO items(id, name) VALUES (1, 'Ada'), (2, 'Bob'), (3, 'Cy')",
      );

      final rows = await database.connection.select(
        'SELECT name FROM items WHERE id IN (SELECT value FROM json_each(?)) ORDER BY id',
        (row) => row.readString(0),
        parameters: [
          [1, 3],
        ],
      );

      expect(rows, ['Ada', 'Cy']);
    });

    test('rejects unsupported collection parameter elements', () async {
      final database = await _openItemsDatabase();
      addTearDown(database.close);

      await expectLater(
        database.connection.select(
          'SELECT name FROM items WHERE id IN (SELECT value FROM json_each(?))',
          (row) => row.readString(0),
          parameters: [
            [DateTime.utc(2026)],
          ],
        ),
        throwsArgumentError,
      );
    });
  });

  group('SelectRunner and invalidation', () {
    test('supports asList, asOne, asOneOrNull, and watch', () async {
      final database = await _openItemsDatabase();
      addTearDown(database.close);

      final allNames = SelectRunner<String>(
        database: database,
        affectedTables: {'items'},
        query: () {
          return database.connection.select(
            'SELECT name FROM items ORDER BY id',
            (row) => row.readString(0),
          );
        },
      );

      expect(await allNames.asList(), isEmpty);
      expect(await allNames.asOneOrNull(), isNull);
      await expectLater(allNames.asOne(), throwsStateError);

      final iterator = StreamIterator(allNames.watch());
      addTearDown(iterator.cancel);
      expect(await _next(iterator), isEmpty);

      await database.connection.execute(
        "INSERT INTO items(id, name) VALUES (1, 'Ada')",
        affectedTables: {'items'},
      );
      expect(await _next(iterator), ['Ada']);
      expect(await allNames.asOne(), 'Ada');
      expect(await allNames.asOneOrNull(), 'Ada');

      await database.connection.execute(
        "INSERT INTO items(id, name) VALUES (2, 'Bob')",
      );
      database.reportExternalTableChanges({'ITEMS'});
      expect(await _next(iterator), ['Ada', 'Bob']);
      await expectLater(allNames.asOneOrNull(), throwsStateError);
    });

    test('ignores empty invalidations', () async {
      final database = await _openItemsDatabase();
      addTearDown(database.close);

      final runner = SelectRunner<int>(
        database: database,
        affectedTables: {'items'},
        query: () => _itemCountRows(database),
      );
      final iterator = StreamIterator(runner.watch());
      addTearDown(iterator.cancel);

      expect(await _next(iterator), [0]);
      database.reportExternalTableChanges({});

      expect(
        await _waitOrNull(
          iterator.moveNext(),
          const Duration(milliseconds: 100),
        ),
        isNull,
      );
    });

    test('refreshes watchers when reported tables overlap', () async {
      final database = await _openItemsDatabase();
      addTearDown(database.close);

      final runner = SelectRunner<int>(
        database: database,
        affectedTables: {'items', 'labels'},
        query: () => _itemCountRows(database),
      );
      final emissions = <List<int>>[];
      final subscription = runner.watch().listen(emissions.add);
      addTearDown(subscription.cancel);

      await _waitForEmissions(emissions, 1);
      expect(emissions.single, [0]);
      database.reportExternalTableChanges({'other'});
      await Future<void>.delayed(const Duration(milliseconds: 100));
      expect(emissions, hasLength(1));

      await database.connection.execute(
        "INSERT INTO items(id, name) VALUES (1, 'Ada')",
      );
      database.reportExternalTableChanges({'LABELS'});
      await _waitForEmissions(emissions, 2);
      expect(emissions.last, [1]);
    });

    test('queues watcher refresh behind a suspended transaction', () async {
      final database = await _openItemsDatabase();
      addTearDown(database.close);

      final runner = SelectRunner<String>(
        database: database,
        affectedTables: {'items'},
        query: () {
          return database.connection.select(
            'SELECT name FROM items ORDER BY id',
            (row) => row.readString(0),
          );
        },
      );
      final emissions = <List<String>>[];
      final subscription = runner.watch().listen(emissions.add);
      addTearDown(subscription.cancel);
      await _waitForEmissions(emissions, 1);
      expect(emissions.single, isEmpty);

      final transactionStarted = Completer<void>();
      final releaseTransaction = Completer<void>();
      final transaction = database.transaction(() async {
        await database.connection.execute(
          "INSERT INTO items(id, name) VALUES (1, 'Ada')",
          affectedTables: {'items'},
        );
        transactionStarted.complete();
        await releaseTransaction.future;
      }, mode: TransactionMode.immediate);

      await transactionStarted.future;
      await Future<void>.delayed(const Duration(milliseconds: 100));
      expect(emissions, hasLength(1));

      releaseTransaction.complete();
      await transaction;
      await _waitForEmissions(emissions, 2);
      expect(emissions.last, ['Ada']);
    });
  });
}

Future<SqliteNowDatabase> _openItemsDatabase() async {
  final database = SqliteNowDatabase.inMemory(
    migrations: [
      SqliteNowMigrationStep(1, (connection) {
        return connection.execute(
          'CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT NOT NULL)',
        );
      }),
    ],
  );
  await database.open();
  return database;
}

Future<int> _itemCount(SqliteNowDatabase database) async {
  return (await _itemCountRows(database)).single;
}

Future<List<int>> _itemCountRows(SqliteNowDatabase database) {
  return database.connection.select(
    'SELECT COUNT(*) FROM items',
    (row) => row.readInt(0),
  );
}

Future<T?> _waitOrNull<T>(Future<T> future, Duration timeout) async {
  try {
    return await future.timeout(timeout);
  } on TimeoutException {
    return null;
  }
}

Future<T> _next<T>(StreamIterator<T> iterator) async {
  final hasNext = await iterator.moveNext();
  expect(hasNext, isTrue);
  return iterator.current;
}

Future<void> _waitForEmissions<T>(List<T> emissions, int count) async {
  final deadline = DateTime.now().add(const Duration(seconds: 2));
  while (emissions.length < count && DateTime.now().isBefore(deadline)) {
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
  expect(emissions, hasLength(greaterThanOrEqualTo(count)));
}

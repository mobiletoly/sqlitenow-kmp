import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:test/test.dart';

void main() {
  group('SqliteNowDatabase lifecycle', () {
    test('opens, migrates, and closes one connection', () async {
      final migrationCallbacks = <String>[];
      final database = SqliteNowDatabase.inMemory(
        onMigrationStep: (scope) {
          migrationCallbacks.add('${scope.fromVersion}->${scope.toVersion}');
        },
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
      expect(
        migrationCallbacks,
        isEmpty,
        reason: 'fresh databases skip callbacks',
      );

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

    test('transforms full_name before the next version removes it', () async {
      final tempDir = await Directory.systemTemp.createTemp(
        'sqlitenow-migration-full-name-',
      );
      addTearDown(() => tempDir.delete(recursive: true));
      final path = '${tempDir.path}/person.db';
      final v1 = SqliteNowDatabase(
        path: path,
        migrations: [
          SqliteNowMigrationStep.fresh(1, (connection) async {
            await connection.execute(
              'CREATE TABLE person(id INTEGER PRIMARY KEY, full_name TEXT NOT NULL)',
            );
            await connection.execute(
              "INSERT INTO person(id, full_name) VALUES (1, 'Ada Lovelace')",
            );
          }),
        ],
      );
      await v1.open();
      await v1.close();

      final upgraded = SqliteNowDatabase(
        path: path,
        migrations: [
          SqliteNowMigrationStep.fresh(3, (connection) {
            return connection.execute(
              'CREATE TABLE person('
              'id INTEGER PRIMARY KEY, '
              'first_name TEXT NOT NULL, '
              'last_name TEXT NOT NULL)',
            );
          }),
          SqliteNowMigrationStep(2, (connection) async {
            await connection.execute(
              'ALTER TABLE person ADD COLUMN first_name TEXT',
            );
            await connection.execute(
              'ALTER TABLE person ADD COLUMN last_name TEXT',
            );
          }),
          SqliteNowMigrationStep(3, (connection) {
            return connection.execute(
              'ALTER TABLE person DROP COLUMN full_name',
            );
          }),
        ],
        onMigrationStep: (scope) async {
          if (scope.toVersion != 2) return;
          final rows = await scope.connection.select(
            'SELECT id, full_name FROM person',
            (row) => (id: row.readInt(0), fullName: row.readString(1)),
          );
          for (final row in rows) {
            final names = row.fullName.split(' ');
            await scope.connection.execute(
              'UPDATE person SET first_name = ?, last_name = ? WHERE id = ?',
              parameters: [names.first, names.skip(1).join(' '), row.id],
            );
          }
        },
      );
      await upgraded.open();
      expect(
        await upgraded.connection.select(
          'SELECT first_name, last_name FROM person',
          (row) => [row.readString(0), row.readString(1)],
        ),
        [
          ['Ada', 'Lovelace'],
        ],
      );
      await upgraded.close();

      final reopened = SqliteNowDatabase(
        path: path,
        migrations: upgradedMigrationsForPerson(),
        onMigrationStep: (_) => throw StateError('reopen must skip callback'),
      );
      await reopened.open();
      await reopened.close();

      final fresh = SqliteNowDatabase.inMemory(
        migrations: upgradedMigrationsForPerson(),
        onMigrationStep: (_) => throw StateError('fresh must skip callback'),
      );
      await fresh.open();
      await fresh.close();
    });

    test(
      'runs every sparse boundary after SQL with exact scope values',
      () async {
        final tempDir = await Directory.systemTemp.createTemp(
          'sqlitenow-migration-callback-',
        );
        addTearDown(() => tempDir.delete(recursive: true));
        final path = '${tempDir.path}/callback.db';

        final v2 = SqliteNowDatabase(
          path: path,
          migrations: [
            SqliteNowMigrationStep.fresh(2, (connection) async {
              await connection.execute(
                'CREATE TABLE migration_log(version INTEGER PRIMARY KEY, label TEXT NOT NULL)',
              );
            }),
          ],
        );
        await v2.open();
        await v2.close();

        final events = <String>[];
        SqliteNowMigrationConnection? retainedConnection;
        final upgraded = SqliteNowDatabase(
          path: path,
          migrations: [
            SqliteNowMigrationStep.fresh(5, (_) {}),
            SqliteNowMigrationStep(3, (connection) async {
              await connection.execute(
                "INSERT INTO migration_log(version, label) VALUES (3, 'sql')",
              );
              events.add('sql:3');
            }),
            SqliteNowMigrationStep(5, (_) {}),
          ],
          onMigrationStep: (scope) async {
            expect(scope.originalVersion, 2);
            expect(scope.targetVersion, 5);
            if (scope.toVersion == 3) {
              retainedConnection = scope.connection;
              final labels = await scope.connection.select(
                'SELECT label FROM migration_log WHERE version = 3',
                (row) => row.readString(0),
              );
              expect(labels, ['sql']);
            }
            events.add('${scope.fromVersion}->${scope.toVersion}');
            if (scope.toVersion == 4) await Future<void>.delayed(Duration.zero);
          },
        );
        addTearDown(upgraded.close);

        await upgraded.open();

        expect(events, ['sql:3', '2->3', '3->4', '4->5']);
        expect(await upgraded.connection.readUserVersion(), 5);
        expect(() => retainedConnection!.execute('SELECT 1'), throwsStateError);
      },
    );

    test(
      'drains an unawaited prepared operation before the next boundary',
      () async {
        final tempDir = await Directory.systemTemp.createTemp(
          'sqlitenow-migration-callback-drain-',
        );
        addTearDown(() => tempDir.delete(recursive: true));
        final path = '${tempDir.path}/drain.db';

        final v1 = SqliteNowDatabase(
          path: path,
          migrations: [
            SqliteNowMigrationStep.fresh(1, (connection) {
              return connection.execute(
                'CREATE TABLE migration_log(version INTEGER PRIMARY KEY)',
              );
            }),
          ],
        );
        await v1.open();
        await v1.close();

        final preparedBlockStarted = Completer<void>();
        final releasePreparedBlock = Completer<void>();
        final nextBoundaryReached = Completer<void>();
        final boundaries = <String>[];
        final upgraded = SqliteNowDatabase(
          path: path,
          migrations: [
            SqliteNowMigrationStep.fresh(3, (_) {}),
            SqliteNowMigrationStep(2, (_) {}),
            SqliteNowMigrationStep(3, (_) {}),
          ],
          onMigrationStep: (scope) {
            boundaries.add('${scope.fromVersion}->${scope.toVersion}');
            if (scope.toVersion == 2) {
              unawaited(
                scope.connection.usePrepared<void>('SELECT 1', (_) async {
                  preparedBlockStarted.complete();
                  await releasePreparedBlock.future;
                }),
              );
            } else {
              nextBoundaryReached.complete();
            }
          },
        );
        addTearDown(upgraded.close);

        final opening = upgraded.open();
        await preparedBlockStarted.future;
        expect(
          await _waitOrNull(
            nextBoundaryReached.future.then((_) => true),
            const Duration(milliseconds: 200),
          ),
          isNull,
        );
        releasePreparedBlock.complete();
        await opening;

        expect(boundaries, ['1->2', '2->3']);
        expect(await upgraded.connection.readUserVersion(), 3);
      },
    );

    for (final scenario
        in <
          ({
            String name,
            Future<void> Function(SqliteNowMigrationScope scope) fail,
            bool caughtByAwait,
          })
        >[
          (
            name: 'direct Future',
            caughtByAwait: true,
            fail: (scope) => scope.connection.execute(
              "INSERT INTO migration_log(id, label) VALUES (1, 'duplicate')",
            ),
          ),
          (
            name: 'asStream single Future',
            caughtByAwait: true,
            fail: (scope) => scope.connection
                .execute(
                  "INSERT INTO migration_log(id, label) VALUES (1, 'duplicate')",
                )
                .asStream()
                .single,
          ),
          (
            name: 'asStream listener',
            caughtByAwait: true,
            fail: (scope) {
              final handled = Completer<void>();
              scope.connection
                  .execute(
                    "INSERT INTO migration_log(id, label) VALUES (1, 'duplicate')",
                  )
                  .asStream()
                  .listen(
                    null,
                    onError: (Object error, StackTrace stackTrace) =>
                        handled.completeError(error, stackTrace),
                  );
              return handled.future;
            },
          ),
          (
            name: 'asStream handleError listener',
            caughtByAwait: false,
            fail: (scope) async {
              final handled = Completer<void>();
              scope.connection
                  .execute(
                    "INSERT INTO migration_log(id, label) VALUES (1, 'duplicate')",
                  )
                  .asStream()
                  .handleError((Object error, StackTrace stackTrace) {
                    handled.complete();
                  })
                  .listen(null);
              await handled.future;
            },
          ),
          (
            name: 'asStream asynchronous handleError listener',
            caughtByAwait: false,
            fail: (scope) async {
              final handled = Completer<void>();
              scope.connection
                  .execute(
                    "INSERT INTO migration_log(id, label) VALUES (1, 'duplicate')",
                  )
                  .asStream()
                  .handleError((Object error, StackTrace stackTrace) async {
                    await Future<void>.delayed(Duration.zero);
                    handled.complete();
                  })
                  .listen(null);
              await handled.future;
              await Future<void>.delayed(Duration.zero);
            },
          ),
        ]) {
      test('callback can catch an SQL failure through ${scenario.name}, '
          'compensate, and commit', () async {
        final tempDir = await Directory.systemTemp.createTemp(
          'sqlitenow-migration-callback-recovery-',
        );
        addTearDown(() => tempDir.delete(recursive: true));
        final path = '${tempDir.path}/recovery.db';

        final v1 = SqliteNowDatabase(
          path: path,
          migrations: [
            SqliteNowMigrationStep.fresh(1, (connection) async {
              await connection.execute(
                'CREATE TABLE migration_log('
                'id INTEGER PRIMARY KEY, label TEXT NOT NULL)',
              );
              await connection.execute(
                "INSERT INTO migration_log(id, label) VALUES (1, 'before')",
              );
            }),
          ],
        );
        await v1.open();
        await v1.close();

        var caughtFailure = false;
        final upgraded = SqliteNowDatabase(
          path: path,
          migrations: [
            SqliteNowMigrationStep.fresh(2, (_) {}),
            SqliteNowMigrationStep(2, (_) {}),
          ],
          onMigrationStep: (scope) async {
            try {
              await scenario.fail(scope);
            } catch (_) {
              caughtFailure = true;
            }
            await scope.connection.execute(
              "UPDATE migration_log SET label = 'recovered' WHERE id = 1",
            );
          },
        );
        addTearDown(upgraded.close);

        await upgraded.open();

        expect(caughtFailure, scenario.caughtByAwait);
        expect(await upgraded.connection.readUserVersion(), 2);
        expect(
          await upgraded.connection.select(
            'SELECT label FROM migration_log WHERE id = 1',
            (row) => row.readString(0),
          ),
          ['recovered'],
        );
      });
    }

    for (final scenario in <({String name, SqliteNowMigrationStepCallback callback})>[
      (
        name: 'synchronous callback',
        callback: (scope) {
          unawaited(
            scope.connection.execute(
              "INSERT INTO migration_log(id, label) VALUES (1, 'duplicate')",
            ),
          );
        },
      ),
      (
        name: 'asynchronous callback',
        callback: (scope) async {
          unawaited(
            scope.connection.execute(
              "INSERT INTO migration_log(id, label) VALUES (1, 'duplicate')",
            ),
          );
        },
      ),
      (
        name: 'transformed Stream with a forwarded failure',
        callback: (scope) async {
          final forwardedError = Completer<void>();
          runZonedGuarded(
            () {
              scope.connection
                  .execute(
                    "INSERT INTO migration_log(id, label) VALUES (1, 'duplicate')",
                  )
                  .asStream()
                  .map((_) => 1)
                  .listen(null);
            },
            (Object error, StackTrace stackTrace) {
              forwardedError.complete();
            },
          );
          await forwardedError.future;
        },
      ),
      (
        name: 'zero-element transformed Stream',
        callback: (scope) async {
          await scope.connection
              .execute(
                "INSERT INTO migration_log(id, label) VALUES (1, 'duplicate')",
              )
              .asStream()
              .take(0)
              .drain<void>();
          await scope.connection.select('SELECT 1', (row) => row.readInt(0));
        },
      ),
      (
        name: 'asynchronously rethrowing handleError transformation',
        callback: (scope) async {
          final handlerReturned = Completer<void>();
          runZonedGuarded(() {
            unawaited(
              scope.connection
                  .execute(
                    "INSERT INTO migration_log(id, label) VALUES (1, 'duplicate')",
                  )
                  .asStream()
                  .handleError((Object error, StackTrace stackTrace) async {
                    await Future<void>.delayed(Duration.zero);
                    handlerReturned.complete();
                    Error.throwWithStackTrace(error, stackTrace);
                  })
                  .drain<void>(),
            );
          }, (Object error, StackTrace stackTrace) {});
          await handlerReturned.future;
          await Future<void>.delayed(Duration.zero);
          await scope.connection.select('SELECT 1', (row) => row.readInt(0));
        },
      ),
      (
        name: 'asynchronously rethrowing Stream listener',
        callback: (scope) async {
          final handlerReturned = Completer<void>();
          runZonedGuarded(() {
            scope.connection
                .execute(
                  "INSERT INTO migration_log(id, label) VALUES (1, 'duplicate')",
                )
                .asStream()
                .listen(
                  null,
                  onError: (Object error, StackTrace stackTrace) async {
                    await Future<void>.delayed(Duration.zero);
                    handlerReturned.complete();
                    Error.throwWithStackTrace(error, stackTrace);
                  },
                );
          }, (Object error, StackTrace stackTrace) {});
          await handlerReturned.future;
          await Future<void>.delayed(Duration.zero);
        },
      ),
      (
        name: 'rethrowing catchError handler',
        callback: (scope) {
          unawaited(
            scope.connection
                .execute(
                  "INSERT INTO migration_log(id, label) VALUES (1, 'duplicate')",
                )
                .catchError((Object error) => throw error),
          );
        },
      ),
      (
        name: 'asynchronously rethrowing catchError handler',
        callback: (scope) {
          unawaited(
            scope.connection
                .execute(
                  "INSERT INTO migration_log(id, label) VALUES (1, 'duplicate')",
                )
                .catchError((Object error) async => throw error),
          );
        },
      ),
      (
        name: 'rejected catchError handler',
        callback: (scope) {
          unawaited(
            scope.connection
                .execute(
                  "INSERT INTO migration_log(id, label) VALUES (1, 'duplicate')",
                )
                .catchError((_) {}, test: (_) => false),
          );
        },
      ),
      (
        name: 'rethrowing then onError handler',
        callback: (scope) {
          unawaited(
            scope.connection
                .execute(
                  "INSERT INTO migration_log(id, label) VALUES (1, 'duplicate')",
                )
                .then<void>(
                  (_) {},
                  onError: (Object error, StackTrace stackTrace) =>
                      Error.throwWithStackTrace(error, stackTrace),
                ),
          );
        },
      ),
      (
        name: 'handled and rethrowing branches',
        callback: (scope) async {
          final operation = scope.connection.execute(
            "INSERT INTO migration_log(id, label) VALUES (1, 'duplicate')",
          );
          unawaited(operation.catchError((_) {}));
          unawaited(operation.catchError((Object error) => throw error));
          await Future<void>.delayed(Duration.zero);
        },
      ),
    ]) {
      test('${scenario.name} rolls back an unawaited SQL failure', () async {
        final tempDir = await Directory.systemTemp.createTemp(
          'sqlitenow-migration-callback-late-failure-',
        );
        addTearDown(() => tempDir.delete(recursive: true));
        final path = '${tempDir.path}/${scenario.name}.db';

        final v1 = SqliteNowDatabase(
          path: path,
          migrations: [
            SqliteNowMigrationStep.fresh(1, (connection) async {
              await connection.execute(
                'CREATE TABLE migration_log('
                'id INTEGER PRIMARY KEY, label TEXT NOT NULL)',
              );
              await connection.execute(
                "INSERT INTO migration_log(id, label) VALUES (1, 'before')",
              );
            }),
          ],
        );
        await v1.open();
        await v1.close();

        final failing = SqliteNowDatabase(
          path: path,
          migrations: [
            SqliteNowMigrationStep.fresh(2, (_) {}),
            SqliteNowMigrationStep(2, (connection) {
              return connection.execute(
                "UPDATE migration_log SET label = 'migration SQL' WHERE id = 1",
              );
            }),
          ],
          onMigrationStep: scenario.callback,
        );
        await expectLater(
          failing.open(),
          throwsA(
            predicate<Object>(
              (error) => error.toString().contains('UNIQUE constraint failed'),
            ),
          ),
        );
        await failing.close();

        final verifier = SqliteNowDatabase(path: path);
        addTearDown(verifier.close);
        await verifier.open();
        expect(await verifier.connection.readUserVersion(), 1);
        expect(
          await verifier.connection.select(
            'SELECT label FROM migration_log WHERE id = 1',
            (row) => row.readString(0),
          ),
          ['before'],
        );
      });
    }

    test(
      'a rejected second Stream listen does not replay a handled SQL failure',
      () async {
        final tempDir = await Directory.systemTemp.createTemp(
          'sqlitenow-migration-callback-double-listen-',
        );
        addTearDown(() => tempDir.delete(recursive: true));
        final path = '${tempDir.path}/double-listen.db';

        final v1 = SqliteNowDatabase(
          path: path,
          migrations: [
            SqliteNowMigrationStep.fresh(1, (connection) async {
              await connection.execute(
                'CREATE TABLE migration_log('
                'id INTEGER PRIMARY KEY, label TEXT NOT NULL)',
              );
              await connection.execute(
                "INSERT INTO migration_log(id, label) VALUES (1, 'before')",
              );
            }),
          ],
        );
        await v1.open();
        await v1.close();

        final upgraded = SqliteNowDatabase(
          path: path,
          migrations: [
            SqliteNowMigrationStep.fresh(2, (_) {}),
            SqliteNowMigrationStep(2, (_) {}),
          ],
          onMigrationStep: (scope) async {
            final handled = Completer<void>();
            final failure = scope.connection
                .execute(
                  "INSERT INTO migration_log(id, label) VALUES (1, 'duplicate')",
                )
                .asStream();
            failure.listen(
              null,
              onError: (Object error, StackTrace stackTrace) {
                handled.complete();
              },
            );
            expect(
              () => failure.listen(null, onError: (Object error) {}),
              throwsStateError,
            );
            await handled.future;
            await scope.connection.execute(
              "UPDATE migration_log SET label = 'recovered' WHERE id = 1",
            );
          },
        );
        addTearDown(upgraded.close);

        await upgraded.open();

        expect(await upgraded.connection.readUserVersion(), 2);
        expect(
          await upgraded.connection.select(
            'SELECT label FROM migration_log WHERE id = 1',
            (row) => row.readString(0),
          ),
          ['recovered'],
        );
      },
    );

    test('accepts synchronous callbacks and valid compound SQL', () async {
      final tempDir = await Directory.systemTemp.createTemp(
        'sqlitenow-migration-callback-synchronous-',
      );
      addTearDown(() => tempDir.delete(recursive: true));
      final path = '${tempDir.path}/synchronous.db';
      final v1 = SqliteNowDatabase(
        path: path,
        migrations: [
          SqliteNowMigrationStep.fresh(1, (connection) async {
            await connection.execute(
              'CREATE TABLE "orders; END"(id INTEGER PRIMARY KEY, value INTEGER NOT NULL)',
            );
            await connection.execute(
              'CREATE TABLE audit(value INTEGER NOT NULL DEFAULT 0)',
            );
            await connection.execute('INSERT INTO "orders; END" VALUES (1, 0)');
            await connection.execute('INSERT INTO audit VALUES (0)');
          }),
        ],
      );
      await v1.open();
      await v1.close();

      var synchronousCallCount = 0;
      final upgraded = SqliteNowDatabase(
        path: path,
        migrations: [
          SqliteNowMigrationStep.fresh(2, (_) {}),
          SqliteNowMigrationStep(2, (_) {}),
        ],
        onMigrationStep: (scope) {
          synchronousCallCount++;
          unawaited(
            scope.connection.execute(
              'CREATE TRIGGER orders_audit AFTER UPDATE ON "orders; END" BEGIN '
              'UPDATE audit SET value = value + 1; END;',
            ),
          );
          unawaited(
            scope.connection.execute('UPDATE "orders; END" SET value = 1'),
          );
        },
      );
      addTearDown(upgraded.close);

      await upgraded.open();
      expect(synchronousCallCount, 1);
      expect(
        await upgraded.connection.select(
          'SELECT value FROM audit',
          (row) => row.readInt(0),
        ),
        [1],
      );
    });

    test(
      'callback failure rolls back SQL, callback data, and user_version',
      () async {
        final tempDir = await Directory.systemTemp.createTemp(
          'sqlitenow-migration-callback-rollback-',
        );
        addTearDown(() => tempDir.delete(recursive: true));
        final path = '${tempDir.path}/rollback.db';

        final v1 = SqliteNowDatabase(
          path: path,
          migrations: [
            SqliteNowMigrationStep.fresh(1, (connection) {
              return connection.execute(
                'CREATE TABLE migration_log(version INTEGER PRIMARY KEY, label TEXT NOT NULL)',
              );
            }),
          ],
        );
        await v1.open();
        await v1.close();

        final failing = SqliteNowDatabase(
          path: path,
          migrations: [
            SqliteNowMigrationStep.fresh(3, (_) {}),
            SqliteNowMigrationStep(2, (connection) {
              return connection.execute(
                "INSERT INTO migration_log(version, label) VALUES (2, 'sql')",
              );
            }),
            SqliteNowMigrationStep(3, (_) {}),
          ],
          onMigrationStep: (scope) async {
            await scope.connection.execute(
              "INSERT OR REPLACE INTO migration_log(version, label) VALUES (${scope.toVersion + 10}, 'callback')",
            );
            if (scope.toVersion == 3) throw StateError('callback failed');
          },
        );
        await expectLater(failing.open(), throwsStateError);
        await failing.close();

        final verifier = SqliteNowDatabase(path: path);
        addTearDown(verifier.close);
        await verifier.open();
        expect(await verifier.connection.readUserVersion(), 1);
        expect(
          await verifier.connection.select(
            'SELECT COUNT(*) FROM migration_log',
            (row) => row.readInt(0),
          ),
          [0],
        );
      },
    );

    for (final forbiddenSql in [
      'BEGIN',
      'COMMIT',
      'END',
      'ROLLBACK',
      'SAVEPOINT nested',
      'RELEASE nested',
      'PRAGMA user_version',
      'PRAGMA main.user_version = 99',
      "PRAGMA 'user_version' = 99",
      "PRAGMA \"main\".'user_version'(99)",
    ]) {
      test('callback rejects $forbiddenSql', () async {
        final tempDir = await Directory.systemTemp.createTemp(
          'sqlitenow-migration-guard-',
        );
        addTearDown(() => tempDir.delete(recursive: true));
        final path = '${tempDir.path}/guard.db';
        final v1 = SqliteNowDatabase(
          path: path,
          migrations: [SqliteNowMigrationStep.fresh(1, (_) {})],
        );
        await v1.open();
        await v1.close();

        final upgraded = SqliteNowDatabase(
          path: path,
          migrations: [SqliteNowMigrationStep.fresh(2, (_) {})],
          onMigrationStep: (scope) => scope.connection.execute(forbiddenSql),
        );
        await expectLater(upgraded.open(), throwsArgumentError);
        await upgraded.close();
      });
    }
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

List<SqliteNowMigrationStep> upgradedMigrationsForPerson() => [
  SqliteNowMigrationStep.fresh(3, (connection) {
    return connection.execute(
      'CREATE TABLE person('
      'id INTEGER PRIMARY KEY, '
      'first_name TEXT NOT NULL, '
      'last_name TEXT NOT NULL)',
    );
  }),
  SqliteNowMigrationStep(2, (connection) async {
    await connection.execute('ALTER TABLE person ADD COLUMN first_name TEXT');
    await connection.execute('ALTER TABLE person ADD COLUMN last_name TEXT');
  }),
  SqliteNowMigrationStep(3, (connection) {
    return connection.execute('ALTER TABLE person DROP COLUMN full_name');
  }),
];

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

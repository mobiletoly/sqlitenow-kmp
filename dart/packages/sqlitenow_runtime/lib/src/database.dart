import 'dart:async';

import 'driver/sqlite3_driver.dart';
import 'driver/sqlite_driver.dart';
import 'invalidation.dart';
import 'migrations.dart';
import 'sqlite_connection.dart';
import 'transaction.dart';

final class SqliteNowDatabase {
  SqliteNowDatabase({
    required String path,
    Iterable<SqliteNowMigrationStep> migrations = const [],
    SqliteNowDriver driver = const Sqlite3Driver(),
  }) : _openOptions = SqliteNowOpenOptions.file(path),
       _driver = driver,
       _migrationPlan = SqliteNowMigrationPlan(migrations);

  SqliteNowDatabase.inMemory({
    Iterable<SqliteNowMigrationStep> migrations = const [],
    SqliteNowDriver driver = const Sqlite3Driver(),
  }) : _openOptions = const SqliteNowOpenOptions.inMemory(),
       _driver = driver,
       _migrationPlan = SqliteNowMigrationPlan(migrations);

  final SqliteNowOpenOptions _openOptions;
  final SqliteNowDriver _driver;
  final SqliteNowMigrationPlan _migrationPlan;
  final TableInvalidationTracker _invalidationTracker =
      TableInvalidationTracker();

  SqliteNowConnection? _connection;
  Future<void>? _openCloseOperation;
  var _closedPermanently = false;

  bool get isOpen => _connection != null;

  SqliteNowConnection get connection {
    final current = _connection;
    if (current == null) {
      throw StateError(
        'Database connection not initialized or already closed. Call open() first.',
      );
    }
    return current;
  }

  TableInvalidationTracker get invalidationTracker => _invalidationTracker;

  Future<void> open({
    FutureOr<void> Function(SqliteNowConnection connection)? preInit,
  }) {
    if (_closedPermanently) {
      throw StateError('Database has been closed permanently');
    }
    return _runOpenClose(() async {
      if (_connection != null) {
        throw StateError('Database connection already initialized');
      }

      final driverConnection = _driver.open(_openOptions);
      final opened = SqliteNowConnection(
        driverConnection,
        invalidationTracker: _invalidationTracker,
      );
      try {
        if (preInit != null) {
          await preInit(opened);
        }
        await _ensureBootstrapUserVersion(opened);
        await opened.transaction(() async {
          final currentVersion = await opened.readUserVersion();
          final newVersion = await _migrationPlan.apply(opened, currentVersion);
          if (newVersion != currentVersion) {
            await opened.setUserVersion(newVersion);
          }
        });
        _connection = opened;
      } catch (_) {
        await opened.close();
        rethrow;
      }
    });
  }

  Future<T> transaction<T>(
    FutureOr<T> Function() block, {
    TransactionMode mode = TransactionMode.deferred,
  }) {
    return connection.transaction(block, mode: mode);
  }

  void reportExternalTableChanges(Set<String> affectedTables) {
    if (!isOpen) {
      throw StateError(
        'Database connection not initialized or already closed. Call open() first.',
      );
    }
    _invalidationTracker.reportTablesChanged(affectedTables);
  }

  Future<void> close() {
    return _runOpenClose(() async {
      final current = _connection;
      if (current == null) return;
      _connection = null;
      await current.close();
      _closedPermanently = true;
      await _invalidationTracker.close();
    });
  }

  Future<void> _ensureBootstrapUserVersion(
    SqliteNowConnection connection,
  ) async {
    final currentVersion = await connection.readUserVersion();
    if (currentVersion != 0) return;
    if (await connection.hasUserTables()) return;
    await connection.setUserVersion(-1);
  }

  Future<void> _runOpenClose(Future<void> Function() block) {
    final previous = _openCloseOperation ?? Future.value();
    final operation = previous.then((_) => block());
    _openCloseOperation = operation.catchError((_) {});
    return operation;
  }
}

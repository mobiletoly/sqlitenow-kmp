import 'dart:async';

import 'driver/sqlite_driver.dart';
import 'invalidation.dart';
import 'sqlite_row_reader.dart';
import 'transaction.dart';

final _connectionOwnerKey = Object();

final class SqliteNowConnection {
  SqliteNowConnection(
    this._driverConnection, {
    required TableInvalidationTracker invalidationTracker,
  }) : _invalidationTracker = invalidationTracker;

  final SqliteNowDriverConnection _driverConnection;
  final TableInvalidationTracker _invalidationTracker;
  final Object _ownerToken = Object();

  Future<void> _tail = Future.value();
  var _closed = false;
  var _transactionDepth = 0;

  bool get isClosed => _closed;

  Future<T> withExclusiveAccess<T>(
    FutureOr<T> Function(SqliteNowDriverConnection connection) block,
  ) {
    _ensureOpen();
    if (identical(Zone.current[_connectionOwnerKey], _ownerToken)) {
      return Future<T>.sync(() => block(_driverConnection));
    }

    final previous = _tail;
    final completer = Completer<T>();
    _tail = completer.future.then<void>((_) {}, onError: (_, _) {});

    () async {
      try {
        await previous;
        _ensureOpen();
        final result = await runZoned(
          () => Future<T>.sync(() => block(_driverConnection)),
          zoneValues: {_connectionOwnerKey: _ownerToken},
        );
        completer.complete(result);
      } catch (error, stackTrace) {
        completer.completeError(error, stackTrace);
      }
    }();

    return completer.future;
  }

  Future<void> execute(
    String sql, {
    List<Object?> parameters = const [],
    Set<String> affectedTables = const {},
  }) async {
    await withExclusiveAccess((connection) {
      connection.execute(sql, parameters);
    });
    _invalidationTracker.reportTablesChanged(affectedTables);
  }

  Future<List<T>> select<T>(
    String sql,
    T Function(SqliteRowReader row) read, {
    List<Object?> parameters = const [],
  }) {
    return withExclusiveAccess((connection) {
      final rows = connection.select(sql, parameters);
      return [for (final row in rows) read(SqliteRowReader(row))];
    });
  }

  Future<T> usePrepared<T>(
    String sql,
    FutureOr<T> Function(SqliteNowPreparedStatement statement) block,
  ) {
    return withExclusiveAccess((connection) async {
      final statement = connection.prepare(sql);
      try {
        return await block(SqliteNowPreparedStatement(statement));
      } finally {
        statement.dispose();
      }
    });
  }

  Future<T> transaction<T>(
    FutureOr<T> Function() block, {
    TransactionMode mode = TransactionMode.deferred,
  }) {
    return withExclusiveAccess((connection) async {
      final outermost = _transactionDepth == 0;
      if (outermost) {
        connection.execute(mode.beginSql);
      }
      _transactionDepth++;
      try {
        final result = await block();
        if (outermost) {
          connection.execute('COMMIT');
        }
        return result;
      } catch (_) {
        if (outermost) {
          try {
            connection.execute('ROLLBACK');
          } catch (_) {
            // The original transaction failure is more useful to callers.
          }
        }
        rethrow;
      } finally {
        _transactionDepth--;
      }
    });
  }

  Future<int> readUserVersion() async {
    final rows = await select('PRAGMA user_version', (row) => row.readInt(0));
    if (rows.isEmpty) return 0;
    return rows.single;
  }

  Future<void> setUserVersion(int version) {
    if (version < -1) {
      throw ArgumentError.value(version, 'version', 'must be >= -1');
    }
    return execute('PRAGMA user_version = $version');
  }

  Future<bool> hasUserTables() async {
    final rows = await select(
      "SELECT name FROM sqlite_master "
      "WHERE type = 'table' "
      "AND name NOT LIKE 'sqlite_%' "
      "AND name != 'android_metadata' "
      "LIMIT 1",
      (row) => row.readString(0),
    );
    return rows.isNotEmpty;
  }

  Future<void> close() async {
    if (_closed) return;
    await withExclusiveAccess((connection) {
      _closed = true;
      connection.close();
    });
  }

  void _ensureOpen() {
    if (_closed) {
      throw StateError('Database connection is already closed');
    }
  }
}

final class SqliteNowPreparedStatement {
  const SqliteNowPreparedStatement(this._statement);

  final SqliteNowDriverStatement _statement;

  void execute([List<Object?> parameters = const []]) {
    _statement.execute(parameters);
  }

  List<T> select<T>(
    T Function(SqliteRowReader row) read, [
    List<Object?> parameters = const [],
  ]) {
    final rows = _statement.select(parameters);
    return [for (final row in rows) read(SqliteRowReader(row))];
  }
}

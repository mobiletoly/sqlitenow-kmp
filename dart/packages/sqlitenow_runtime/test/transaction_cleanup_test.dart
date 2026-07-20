import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:test/test.dart';

void main() {
  test(
    'rollback failure closes connection and preserves both failures',
    () async {
      final driver = _FaultDriver(failRollback: true);
      final database = SqliteNowDatabase.inMemory(driver: driver);
      addTearDown(database.close);
      await database.open();
      final primary = StateError('primary transaction failure');

      Object? caught;
      try {
        await database.transaction(() => throw primary);
      } catch (error) {
        caught = error;
      }

      expect(caught, isA<SqliteNowFatalTransactionException>());
      final fatal = caught as SqliteNowFatalTransactionException;
      expect(fatal.primaryFailure, same(primary));
      expect(fatal.cleanupFailure, same(driver.rollbackFailure));
      expect(driver.connection.closeCount, 1);
      expect(database.connection.isClosed, isTrue);
      await expectLater(
        () => database.connection.select('SELECT 1', (row) => row.readInt(0)),
        throwsStateError,
      );
    },
  );

  test('successful rollback keeps the connection reusable', () async {
    final driver = _FaultDriver();
    final database = SqliteNowDatabase.inMemory(driver: driver);
    addTearDown(database.close);
    await database.open();
    final primary = StateError('primary transaction failure');

    await expectLater(
      database.transaction(() => throw primary),
      throwsA(same(primary)),
    );

    expect(database.connection.isClosed, isFalse);
    expect(
      await database.connection.select('SELECT 1', (row) => row.readInt(0)),
      [1],
    );
  });

  test(
    'real SQLite commit failure rolls back and keeps connection reusable',
    () async {
      final database = SqliteNowDatabase.inMemory();
      addTearDown(database.close);
      await database.open(
        preInit: (connection) async {
          await connection.execute(
            'CREATE TABLE parents (id TEXT PRIMARY KEY NOT NULL)',
          );
          await connection.execute('''CREATE TABLE children (
  id TEXT PRIMARY KEY NOT NULL,
  parent_id TEXT NOT NULL REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED
)''');
          await connection.execute("INSERT INTO parents(id) VALUES('parent')");
          await connection.execute(
            "INSERT INTO children(id, parent_id) VALUES('child', 'parent')",
          );
        },
      );

      await expectLater(
        database.transaction(() async {
          await database.connection.execute('PRAGMA defer_foreign_keys = ON');
          await database.connection.execute(
            "DELETE FROM parents WHERE id = 'parent'",
          );
        }, mode: TransactionMode.immediate),
        throwsA(anything),
      );

      expect(database.connection.isClosed, isFalse);
      expect(
        await database.connection.select(
          "SELECT COUNT(*) FROM parents WHERE id = 'parent'",
          (row) => row.readInt(0),
        ),
        [1],
      );
    },
  );

  test(
    'statement cleanup failure closes connection and keeps primary typed',
    () async {
      final driver = _FaultDriver(failStatementDispose: true);
      final database = SqliteNowDatabase.inMemory(driver: driver);
      addTearDown(database.close);
      await database.open();
      final primary = ArgumentError('typed statement failure');

      Object? caught;
      try {
        await database.connection.usePrepared('SELECT 1', (_) => throw primary);
      } catch (error) {
        caught = error;
      }

      expect(caught, isA<SqliteNowFatalStatementCleanupException>());
      final fatal = caught as SqliteNowFatalStatementCleanupException;
      expect(fatal.primaryFailure, same(primary));
      expect(fatal.cleanupFailure, same(driver.statementDisposeFailure));
      expect(database.connection.isClosed, isTrue);
      expect(driver.connection.closeCount, 1);
    },
  );
}

final class _FaultDriver implements SqliteNowDriver {
  _FaultDriver({this.failRollback = false, this.failStatementDispose = false});

  final bool failRollback;
  final bool failStatementDispose;
  final rollbackFailure = StateError('rollback cleanup failure');
  final statementDisposeFailure = StateError('statement cleanup failure');
  late final _FaultConnection connection;

  @override
  SqliteNowDriverConnection open(SqliteNowOpenOptions options) {
    return connection = _FaultConnection(this);
  }
}

final class _FaultConnection implements SqliteNowDriverConnection {
  _FaultConnection(this.driver);

  final _FaultDriver driver;
  var closeCount = 0;

  @override
  void execute(String sql, [List<Object?> parameters = const []]) {
    if (sql == 'ROLLBACK' && driver.failRollback) {
      throw driver.rollbackFailure;
    }
  }

  @override
  List<SqliteNowDriverRow> select(
    String sql, [
    List<Object?> parameters = const [],
  ]) {
    if (sql == 'PRAGMA user_version') return const [_FaultRow(0)];
    if (sql.startsWith('SELECT name FROM sqlite_master')) return const [];
    if (sql == 'SELECT 1') return const [_FaultRow(1)];
    return const [];
  }

  @override
  SqliteNowDriverStatement prepare(String sql) => _FaultStatement(driver);

  @override
  void close() {
    closeCount++;
  }
}

final class _FaultStatement implements SqliteNowDriverStatement {
  _FaultStatement(this.driver);

  final _FaultDriver driver;

  @override
  void dispose() {
    if (driver.failStatementDispose) throw driver.statementDisposeFailure;
  }

  @override
  void execute([List<Object?> parameters = const []]) {}

  @override
  List<SqliteNowDriverRow> select([List<Object?> parameters = const []]) =>
      const [];
}

final class _FaultRow implements SqliteNowDriverRow {
  const _FaultRow(this.value);

  final Object? value;

  @override
  Object? column(int index) => value;
}

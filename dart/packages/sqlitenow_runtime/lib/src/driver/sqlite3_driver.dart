import 'package:sqlite3/sqlite3.dart' as sqlite3;

import 'sqlite_driver.dart';

final class Sqlite3Driver implements SqliteNowDriver {
  const Sqlite3Driver();

  @override
  SqliteNowDriverConnection open(SqliteNowOpenOptions options) {
    final database = options.inMemory
        ? sqlite3.sqlite3.openInMemory()
        : sqlite3.sqlite3.open(options.path!);
    database.execute('PRAGMA foreign_keys = ON');
    return Sqlite3DriverConnection(database);
  }
}

final class Sqlite3DriverConnection implements SqliteNowDriverConnection {
  Sqlite3DriverConnection(this._database);

  final sqlite3.Database _database;

  @override
  void execute(String sql, [List<Object?> parameters = const []]) {
    _database.execute(sql, normalizeSqliteNowParameters(parameters));
  }

  @override
  SqliteNowDriverStatement prepare(String sql) {
    return Sqlite3DriverStatement(_database.prepare(sql));
  }

  @override
  List<SqliteNowDriverRow> select(
    String sql, [
    List<Object?> parameters = const [],
  ]) {
    final rows = _database.select(
      sql,
      normalizeSqliteNowParameters(parameters),
    );
    return [for (final row in rows) Sqlite3DriverRow(row)];
  }

  @override
  void close() {
    _database.close();
  }
}

final class Sqlite3DriverStatement implements SqliteNowDriverStatement {
  Sqlite3DriverStatement(this._statement);

  final sqlite3.PreparedStatement _statement;

  @override
  void execute([List<Object?> parameters = const []]) {
    _statement.execute(normalizeSqliteNowParameters(parameters));
  }

  @override
  List<SqliteNowDriverRow> select([List<Object?> parameters = const []]) {
    final rows = _statement.select(normalizeSqliteNowParameters(parameters));
    return [for (final row in rows) Sqlite3DriverRow(row)];
  }

  @override
  void dispose() {
    _statement.close();
  }
}

final class Sqlite3DriverRow implements SqliteNowDriverRow {
  const Sqlite3DriverRow(this._row);

  final sqlite3.Row _row;

  @override
  Object? column(int index) => _row.columnAt(index);
}

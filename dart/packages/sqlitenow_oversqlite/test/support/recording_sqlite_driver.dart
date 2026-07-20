import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';

final class RecordingSqliteSelectCall {
  const RecordingSqliteSelectCall({
    required this.sql,
    required this.prepared,
    required this.parameters,
    required this.returnedRowCount,
  });

  final String sql;
  final bool prepared;
  final List<Object?> parameters;
  final int returnedRowCount;
}

final class RecordingSqliteDriver implements SqliteNowDriver {
  RecordingSqliteDriver({SqliteNowDriver delegate = const Sqlite3Driver()})
    : _delegate = delegate;

  final SqliteNowDriver _delegate;
  final List<String> directSql = [];
  final List<String> preparedSql = [];
  final List<RecordingSqliteSelectCall> selectCalls = [];

  void reset() {
    directSql.clear();
    preparedSql.clear();
    selectCalls.clear();
  }

  int prepareCountWhere(bool Function(String sql) matches) =>
      preparedSql.where(matches).length;

  Iterable<RecordingSqliteSelectCall> selectCallsWhere(
    bool Function(String sql) matches, {
    bool? prepared,
  }) {
    return selectCalls.where(
      (call) =>
          matches(call.sql) && (prepared == null || call.prepared == prepared),
    );
  }

  @override
  SqliteNowDriverConnection open(SqliteNowOpenOptions options) {
    return _RecordingSqliteConnection(this, _delegate.open(options));
  }
}

final class _RecordingSqliteConnection implements SqliteNowDriverConnection {
  const _RecordingSqliteConnection(this._owner, this._delegate);

  final RecordingSqliteDriver _owner;
  final SqliteNowDriverConnection _delegate;

  @override
  void close() => _delegate.close();

  @override
  void execute(String sql, [List<Object?> parameters = const []]) {
    _owner.directSql.add(sql);
    _delegate.execute(sql, parameters);
  }

  @override
  SqliteNowDriverStatement prepare(String sql) {
    _owner.preparedSql.add(sql);
    return _RecordingSqliteStatement(_owner, sql, _delegate.prepare(sql));
  }

  @override
  List<SqliteNowDriverRow> select(
    String sql, [
    List<Object?> parameters = const [],
  ]) {
    _owner.directSql.add(sql);
    final rows = _delegate.select(sql, parameters);
    _owner.selectCalls.add(
      RecordingSqliteSelectCall(
        sql: sql,
        prepared: false,
        parameters: List.unmodifiable(parameters),
        returnedRowCount: rows.length,
      ),
    );
    return rows;
  }
}

final class _RecordingSqliteStatement implements SqliteNowDriverStatement {
  const _RecordingSqliteStatement(this._owner, this._sql, this._delegate);

  final RecordingSqliteDriver _owner;
  final String _sql;
  final SqliteNowDriverStatement _delegate;

  @override
  void dispose() => _delegate.dispose();

  @override
  void execute([List<Object?> parameters = const []]) {
    _delegate.execute(parameters);
  }

  @override
  List<SqliteNowDriverRow> select([List<Object?> parameters = const []]) {
    final rows = _delegate.select(parameters);
    _owner.selectCalls.add(
      RecordingSqliteSelectCall(
        sql: _sql,
        prepared: true,
        parameters: List.unmodifiable(parameters),
        returnedRowCount: rows.length,
      ),
    );
    return rows;
  }
}

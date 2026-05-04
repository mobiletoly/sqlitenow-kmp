import 'dart:convert';
import 'dart:typed_data';

abstract interface class SqliteNowDriver {
  SqliteNowDriverConnection open(SqliteNowOpenOptions options);
}

final class SqliteNowOpenOptions {
  const SqliteNowOpenOptions.file(this.path) : inMemory = false;

  const SqliteNowOpenOptions.inMemory() : path = null, inMemory = true;

  final String? path;
  final bool inMemory;
}

abstract interface class SqliteNowDriverConnection {
  void execute(String sql, [List<Object?> parameters = const []]);

  List<SqliteNowDriverRow> select(
    String sql, [
    List<Object?> parameters = const [],
  ]);

  SqliteNowDriverStatement prepare(String sql);

  void close();
}

abstract interface class SqliteNowDriverStatement {
  void execute([List<Object?> parameters = const []]);

  List<SqliteNowDriverRow> select([List<Object?> parameters = const []]);

  void dispose();
}

abstract interface class SqliteNowDriverRow {
  Object? column(int index);
}

bool isSqliteNowScalar(Object? value) {
  return value == null ||
      value is String ||
      value is int ||
      value is double ||
      value is Uint8List;
}

List<Object?> normalizeSqliteNowParameters(Iterable<Object?> parameters) {
  return [
    for (final parameter in parameters)
      if (isSqliteNowScalar(parameter))
        parameter
      else if (parameter is Iterable<Object?>)
        jsonEncode([
          for (final item in parameter)
            if (isSqliteNowScalar(item))
              item
            else
              throw ArgumentError.value(
                item,
                'parameters',
                'Unsupported SQLiteNow collection parameter element type: ${item.runtimeType}',
              ),
        ])
      else
        throw ArgumentError.value(
          parameter,
          'parameters',
          'Unsupported SQLiteNow parameter type: ${parameter.runtimeType}',
        ),
  ];
}

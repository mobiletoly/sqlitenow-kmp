import 'dart:typed_data';

import 'driver/sqlite_driver.dart';

final class SqliteRowReader {
  const SqliteRowReader(this._row);

  final SqliteNowDriverRow _row;

  Object? readValue(int index) => _row.column(index);

  String readString(int index) {
    final value = readNullableString(index);
    if (value == null) throw StateError('Column $index was null');
    return value;
  }

  String? readNullableString(int index) {
    final value = readValue(index);
    if (value == null) return null;
    if (value is String) return value;
    throw StateError('Column $index was ${value.runtimeType}, expected String');
  }

  int readInt(int index) {
    final value = readNullableInt(index);
    if (value == null) throw StateError('Column $index was null');
    return value;
  }

  int? readNullableInt(int index) {
    final value = readValue(index);
    if (value == null) return null;
    if (value is int) return value;
    throw StateError('Column $index was ${value.runtimeType}, expected int');
  }

  double readDouble(int index) {
    final value = readNullableDouble(index);
    if (value == null) throw StateError('Column $index was null');
    return value;
  }

  double? readNullableDouble(int index) {
    final value = readValue(index);
    if (value == null) return null;
    if (value is double) return value;
    if (value is int) return value.toDouble();
    throw StateError('Column $index was ${value.runtimeType}, expected double');
  }

  Uint8List readBlob(int index) {
    final value = readNullableBlob(index);
    if (value == null) throw StateError('Column $index was null');
    return value;
  }

  Uint8List? readNullableBlob(int index) {
    final value = readValue(index);
    if (value == null) return null;
    if (value is Uint8List) return Uint8List.fromList(value);
    if (value is List<int>) return Uint8List.fromList(value);
    throw StateError('Column $index was ${value.runtimeType}, expected blob');
  }
}

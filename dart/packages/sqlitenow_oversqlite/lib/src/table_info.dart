import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';

import 'config.dart';

final class OversqliteTableInfo {
  const OversqliteTableInfo({
    required this.name,
    required this.columns,
    required this.primaryKey,
  });

  final String name;
  final List<OversqliteColumnInfo> columns;
  final OversqliteColumnInfo primaryKey;
}

final class OversqliteColumnInfo {
  const OversqliteColumnInfo({
    required this.name,
    required this.kind,
    required this.primaryKey,
  });

  final String name;
  final OversqliteColumnKind kind;
  final bool primaryKey;
}

final class _RawColumnInfo {
  const _RawColumnInfo({
    required this.name,
    required this.declaredType,
    required this.primaryKey,
  });

  final String name;
  final String declaredType;
  final bool primaryKey;
}

enum OversqliteColumnKind {
  text,
  integer,
  real,
  exactInt64,
  exactDecimal,
  blob,
  uuidBlob,
}

Future<OversqliteTableInfo> loadOversqliteTableInfo(
  SqliteNowConnection connection,
  String tableName, {
  Map<String, NumericColumnKind> numericColumns = const {},
}) async {
  final rawColumns = await connection.select(
    'PRAGMA table_info(${quoteSqlIdentifier(tableName)})',
    (row) => _RawColumnInfo(
      name: row.readString(1),
      declaredType: row.readString(2),
      primaryKey: row.readInt(5) > 0,
    ),
  );
  final foreignKeyColumnsLower = await connection.select(
    'PRAGMA foreign_key_list(${quoteSqlIdentifier(tableName)})',
    (row) => row.readString(3).toLowerCase(),
  );
  final foreignKeyColumnSet = foreignKeyColumnsLower.toSet();
  final rows = [
    for (final column in rawColumns)
      OversqliteColumnInfo(
        name: column.name,
        kind: _configuredOversqliteColumnKind(
          column.declaredType,
          column.name,
          numericColumns,
          primaryKey: column.primaryKey,
          blobReference: foreignKeyColumnSet.contains(
            column.name.toLowerCase(),
          ),
        ),
        primaryKey: column.primaryKey,
      ),
  ];
  final pk = rows.singleWhere((column) => column.primaryKey);
  return OversqliteTableInfo(name: tableName, columns: rows, primaryKey: pk);
}

OversqliteColumnKind _configuredOversqliteColumnKind(
  String declaredType,
  String columnName,
  Map<String, NumericColumnKind> numericColumns, {
  required bool primaryKey,
  required bool blobReference,
}) {
  final base = oversqliteColumnKind(
    declaredType,
    primaryKey: primaryKey,
    blobReference: blobReference,
  );
  NumericColumnKind? configured;
  for (final entry in numericColumns.entries) {
    if (entry.key.trim().toLowerCase() == columnName.toLowerCase()) {
      configured = entry.value;
      break;
    }
  }
  return switch (configured) {
    null => base,
    NumericColumnKind.exactInt64 =>
      base == OversqliteColumnKind.integer
          ? OversqliteColumnKind.exactInt64
          : throw ArgumentError(
              'exact-int64 column $columnName must have SQLite INTEGER affinity',
            ),
    NumericColumnKind.exactDecimal =>
      base == OversqliteColumnKind.text
          ? OversqliteColumnKind.exactDecimal
          : throw ArgumentError(
              'exact-decimal column $columnName must have SQLite TEXT affinity',
            ),
    NumericColumnKind.approximate =>
      base == OversqliteColumnKind.real
          ? OversqliteColumnKind.real
          : throw ArgumentError(
              'approximate column $columnName must have SQLite REAL affinity',
            ),
  };
}

OversqliteColumnKind oversqliteColumnKind(
  String type, {
  required bool primaryKey,
  required bool blobReference,
}) {
  final upper = type.trim().toUpperCase();
  if (upper.contains('BLOB')) {
    return primaryKey || blobReference
        ? OversqliteColumnKind.uuidBlob
        : OversqliteColumnKind.blob;
  }
  if (upper.contains('INT')) {
    return OversqliteColumnKind.integer;
  }
  if (upper.contains('REAL') ||
      upper.contains('FLOA') ||
      upper.contains('DOUB')) {
    return OversqliteColumnKind.real;
  }
  return OversqliteColumnKind.text;
}

String quoteSqlIdentifier(String identifier) =>
    '"${identifier.replaceAll('"', '""')}"';

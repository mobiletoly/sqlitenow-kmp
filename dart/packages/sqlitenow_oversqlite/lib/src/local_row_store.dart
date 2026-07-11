import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';

import 'config.dart';
import 'payload_codec.dart';
import 'payload_source.dart';
import 'protocol.dart';
import 'table_info.dart';

final class OversqliteLocalRowStore {
  const OversqliteLocalRowStore(
    this._connection, [
    this._syncTables = const [],
  ]);

  final SqliteNowConnection _connection;
  final List<SyncTable> _syncTables;

  Future<OversqliteTableInfo> tableInfo(String tableName) {
    final configured = _syncTables.where(
      (table) =>
          table.tableName.trim().toLowerCase() ==
          tableName.trim().toLowerCase(),
    );
    return loadOversqliteTableInfo(
      _connection,
      tableName,
      numericColumns: configured.isEmpty
          ? const {}
          : configured.single.numericColumns,
    );
  }

  Future<String?> serializeExistingRow(String tableName, String localPk) async {
    final table = await tableInfo(tableName);
    final rows = await _connection.select(
      'SELECT ${table.columns.map((column) => column.kind == OversqliteColumnKind.exactInt64 || column.kind == OversqliteColumnKind.exactDecimal ? 'CAST(${quoteSqlIdentifier(column.name)} AS TEXT)' : quoteSqlIdentifier(column.name)).join(', ')} FROM ${quoteSqlIdentifier(tableName)} WHERE ${quoteSqlIdentifier(table.primaryKey.name)} = ?',
      (row) => {
        for (var i = 0; i < table.columns.length; i++)
          table.columns[i].name.toLowerCase(): localOversqliteJsonValue(
            table.columns[i],
            row.readValue(i),
          ),
      },
      parameters: [bindOversqlitePrimaryKey(table, localPk)],
    );
    if (rows.isEmpty) {
      return null;
    }
    return canonicalizeOversqliteProtocolJson(rows.single);
  }

  Future<void> upsertPayload(
    String tableName,
    Map<String, Object?> payload,
    PayloadSource source, {
    bool requireCompletePayload = false,
  }) async {
    final table = await tableInfo(tableName);
    final normalized = {
      for (final entry in payload.entries) entry.key.toLowerCase(): entry.value,
    };
    if (requireCompletePayload) {
      _validatePayloadColumns(tableName, table, normalized);
    }
    final columns = table.columns.map((column) => column.name).toList();
    final updates = [
      for (final column in table.columns)
        if (!column.primaryKey)
          '${quoteSqlIdentifier(column.name)} = excluded.${quoteSqlIdentifier(column.name)}',
    ];
    await _connection.execute(
      '''INSERT INTO ${quoteSqlIdentifier(tableName)} (${columns.map(quoteSqlIdentifier).join(', ')})
VALUES (${columns.map((_) => '?').join(', ')})
ON CONFLICT(${quoteSqlIdentifier(table.primaryKey.name)}) DO UPDATE SET ${updates.join(', ')}''',
      parameters: [
        for (final column in table.columns)
          bindOversqlitePayloadValue(
            column,
            normalized[column.name.toLowerCase()],
            source,
          ),
      ],
    );
  }

  Future<void> deleteLocalRow(String tableName, String localPk) async {
    final table = await tableInfo(tableName);
    await _connection.execute(
      'DELETE FROM ${quoteSqlIdentifier(tableName)} WHERE ${quoteSqlIdentifier(table.primaryKey.name)} = ?',
      parameters: [bindOversqlitePrimaryKey(table, localPk)],
    );
  }

  Future<void> deleteAllRows(String tableName) {
    return _connection.execute('DELETE FROM ${quoteSqlIdentifier(tableName)}');
  }

  Future<({String keyJson, String localPk})> localKeyFromWire(
    String tableName,
    SyncKey key,
  ) async {
    final table = await tableInfo(tableName);
    return localKeyFromOversqliteWireKey(table, key);
  }
}

void _validatePayloadColumns(
  String tableName,
  OversqliteTableInfo table,
  Map<String, Object?> normalized,
) {
  if (normalized.length != table.columns.length) {
    throw OversqliteProtocolException(
      'payload for $tableName must contain every table column',
    );
  }
  for (final column in table.columns) {
    if (!normalized.containsKey(column.name.toLowerCase())) {
      throw OversqliteProtocolException(
        'payload for $tableName is missing column ${column.name}',
      );
    }
  }
}

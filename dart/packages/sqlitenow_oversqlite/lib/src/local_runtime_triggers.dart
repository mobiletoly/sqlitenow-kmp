part of 'local_runtime.dart';

final class _TriggerData {
  const _TriggerData({
    required this.schemaName,
    required this.tableName,
    required this.newRowJson,
    required this.oldKeyJson,
    required this.newKeyJson,
  });

  final String schemaName;
  final String tableName;
  final String newRowJson;
  final String oldKeyJson;
  final String newKeyJson;
}

String _insertTriggerSql(_TriggerData data) =>
    '''CREATE TRIGGER IF NOT EXISTS trg_${data.tableName}_ai
AFTER INSERT ON ${_quoteIdent(data.tableName)}
WHEN COALESCE((SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1), 0) = 0
BEGIN
${_dirtyRowCurrentPayloadUpsertSql(data, keyJson: data.newKeyJson, payloadJson: data.newRowJson)}
END''';

String _guardInsertTriggerSql(String tableName) =>
    _guardTriggerSql(tableName, suffix: 'bi', operation: 'INSERT');

String _guardUpdateTriggerSql(String tableName) =>
    _guardTriggerSql(tableName, suffix: 'bu', operation: 'UPDATE');

String _guardDeleteTriggerSql(String tableName) =>
    _guardTriggerSql(tableName, suffix: 'bd', operation: 'DELETE');

String _guardTriggerSql(
  String tableName, {
  required String suffix,
  required String operation,
}) =>
    '''CREATE TRIGGER IF NOT EXISTS trg_${tableName}_${suffix}_guard
BEFORE $operation ON ${_quoteIdent(tableName)}
WHEN EXISTS (
  SELECT 1
  FROM _sync_operation_state
  WHERE singleton_key = 1
    AND kind != 'none'
)
  AND NOT EXISTS (
    SELECT 1
    FROM _sync_apply_state
    WHERE singleton_key = 1
      AND apply_mode = 1
  )
BEGIN
  SELECT RAISE(ABORT, 'SYNC_TRANSITION_PENDING');
END''';

String _updateTriggerSql(_TriggerData data) =>
    '''CREATE TRIGGER IF NOT EXISTS trg_${data.tableName}_au
AFTER UPDATE ON ${_quoteIdent(data.tableName)}
WHEN COALESCE((SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1), 0) = 0
BEGIN
${_dirtyRowDeleteMarkerSelectSql(data, keyJson: data.oldKeyJson, whereClause: '${data.oldKeyJson} != ${data.newKeyJson}')}

${_dirtyRowCurrentPayloadUpsertSql(data, keyJson: data.newKeyJson, payloadJson: data.newRowJson)}
END''';

String _deleteTriggerSql(_TriggerData data) =>
    '''CREATE TRIGGER IF NOT EXISTS trg_${data.tableName}_ad
AFTER DELETE ON ${_quoteIdent(data.tableName)}
WHEN COALESCE((SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1), 0) = 0
BEGIN
${_dirtyRowDeleteMarkerValuesSql(data, keyJson: data.oldKeyJson)}
END''';

String _dirtyRowCurrentPayloadUpsertSql(
  _TriggerData data, {
  required String keyJson,
  required String payloadJson,
}) =>
    '''  INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal, updated_at)
  VALUES (
    '${data.schemaName}',
    '${data.tableName}',
    $keyJson,
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM _sync_row_state
        WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=$keyJson AND deleted=0
      ) THEN 'UPDATE'
      ELSE 'INSERT'
    END,
    COALESCE(
      (SELECT base_row_version FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=$keyJson),
      (SELECT row_version FROM _sync_row_state WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=$keyJson),
      0
    ),
    $payloadJson,
    COALESCE(
      (SELECT dirty_ordinal FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=$keyJson),
      (SELECT COALESCE(MAX(dirty_ordinal), 0) + 1 FROM _sync_dirty_rows)
    ),
    strftime('%Y-%m-%dT%H:%M:%fZ','now')
  )
  ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
    op=excluded.op,
    payload=excluded.payload,
    updated_at=excluded.updated_at;''';

String _dirtyRowDeleteMarkerSelectSql(
  _TriggerData data, {
  required String keyJson,
  required String whereClause,
}) =>
    '''  INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal, updated_at)
  SELECT
    '${data.schemaName}',
    '${data.tableName}',
    $keyJson,
    'DELETE',
    COALESCE(
      (SELECT base_row_version FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=$keyJson),
      (SELECT row_version FROM _sync_row_state WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=$keyJson),
      0
    ),
    NULL,
    COALESCE(
      (SELECT dirty_ordinal FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=$keyJson),
      (SELECT COALESCE(MAX(dirty_ordinal), 0) + 1 FROM _sync_dirty_rows)
    ),
    strftime('%Y-%m-%dT%H:%M:%fZ','now')
  WHERE $whereClause
  ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
    op='DELETE',
    payload=NULL,
    updated_at=excluded.updated_at;''';

String _dirtyRowDeleteMarkerValuesSql(
  _TriggerData data, {
  required String keyJson,
}) =>
    '''  INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal, updated_at)
  VALUES (
    '${data.schemaName}',
    '${data.tableName}',
    $keyJson,
    'DELETE',
    COALESCE(
      (SELECT base_row_version FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=$keyJson),
      (SELECT row_version FROM _sync_row_state WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=$keyJson),
      0
    ),
    NULL,
    COALESCE(
      (SELECT dirty_ordinal FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=$keyJson),
      (SELECT COALESCE(MAX(dirty_ordinal), 0) + 1 FROM _sync_dirty_rows)
    ),
    strftime('%Y-%m-%dT%H:%M:%fZ','now')
  )
  ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
    op='DELETE',
    payload=NULL,
    updated_at=excluded.updated_at;''';

String _buildJsonObjectExprHexAware(_TableInfo tableInfo, String prefix) {
  final pairs = tableInfo.columns.map((column) {
    final name = column.name.toLowerCase();
    final valueExpr = column.kind.isBlobKind
        ? 'CASE WHEN $prefix.${_quoteIdent(column.name)} IS NULL THEN NULL ELSE lower(hex($prefix.${_quoteIdent(column.name)})) END'
        : '$prefix.${_quoteIdent(column.name)}';
    return "'$name', $valueExpr";
  });
  return 'json_object(${pairs.join(", ")})';
}

String _buildKeyJsonObjectExprHexAware(
  _TableInfo tableInfo,
  String keyColumn,
  String prefix,
) {
  final column = tableInfo.columns.firstWhere(
    (column) => column.name.toLowerCase() == keyColumn.toLowerCase(),
    orElse: () => throw StateError(
      'table ${tableInfo.table} is missing sync key column $keyColumn',
    ),
  );
  final keyName = column.name.toLowerCase();
  final valueExpr = column.kind.isBlobKind
      ? 'lower(hex($prefix.${_quoteIdent(column.name)}))'
      : '$prefix.${_quoteIdent(column.name)}';
  return "json_object('$keyName', $valueExpr)";
}

String _quoteIdent(String identifier) =>
    '"${identifier.replaceAll('"', '""')}"';

final _collapseWhitespaceRegex = RegExp(r'\s+');
final _createTriggerPrefixRegex = RegExp(
  r'^CREATE\s+TRIGGER\s+(?:IF\s+NOT\s+EXISTS\s+)?',
  caseSensitive: false,
);

String _normalizeTriggerSql(String sql) {
  final collapsed = sql.trim().replaceAll(_collapseWhitespaceRegex, ' ');
  return collapsed.replaceFirst(_createTriggerPrefixRegex, 'CREATE TRIGGER ');
}

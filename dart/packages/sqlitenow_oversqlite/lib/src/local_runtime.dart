import 'dart:convert';
import 'dart:math';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';

import 'config.dart';

part 'local_runtime_control_tables.dart';
part 'local_runtime_models.dart';
part 'local_runtime_triggers.dart';

final class OversqliteLocalRuntime {
  OversqliteLocalRuntime({
    required SqliteNowDatabase database,
    required OversqliteConfig config,
  }) : _database = database,
       _config = config;

  final SqliteNowDatabase _database;
  final OversqliteConfig _config;
  OversqliteValidatedConfig? _validated;

  SqliteNowConnection get _connection => _database.connection;

  Future<OversqliteValidatedConfig> initialize() async {
    final schema = _config.schema.trim();
    if (schema.isEmpty) {
      throw ArgumentError('config.schema must be provided');
    }

    await _connection.execute('PRAGMA foreign_keys = ON');
    final tableInfoByName = <String, _TableInfo>{};
    final validated = await _validateConfig(schema, tableInfoByName);
    await _initializeOversqliteControlTables(_connection);
    await _registerManagedTables(validated);
    await _installTriggers(validated, tableInfoByName);
    await _bindLocalSource(validated, tableInfoByName);
    _validated = validated;
    return validated;
  }

  Future<OversqliteSourceInfo> sourceInfo() async {
    final rows = await _connection.select(
      'SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1',
      (row) => row.readString(0),
    );
    if (rows.isEmpty || rows.single.isEmpty) {
      throw StateError('Oversqlite local runtime has not been initialized');
    }
    return OversqliteSourceInfo(currentSourceId: rows.single);
  }

  void reportManagedTableChanges(Iterable<String> tables) {
    final managed = (_validated?.tables ?? const <OversqliteValidatedTable>[])
        .map((table) => table.tableName)
        .toSet();
    final affected = {
      for (final table in tables)
        if (managed.contains(table.trim().toLowerCase()))
          table.trim().toLowerCase(),
    };
    _database.reportExternalTableChanges(affected);
  }

  Future<void> executeManagedWrite(
    String sql, {
    List<Object?> parameters = const [],
    Set<String> affectedTables = const {},
  }) async {
    await _connection.execute(sql, parameters: parameters);
    reportManagedTableChanges(affectedTables);
  }

  Future<OversqliteValidatedConfig> _validateConfig(
    String schema,
    Map<String, _TableInfo> tableInfoByName,
  ) async {
    final managedTables = <String>{};
    final validatedTables = <OversqliteValidatedTable>[];

    for (final syncTable in _config.syncTables) {
      final tableName = syncTable.tableName.trim().toLowerCase();
      if (tableName.isEmpty) {
        throw ArgumentError('sync table name must be provided');
      }
      if (tableName.contains('.')) {
        throw ArgumentError(
          'table ${syncTable.tableName} must not include a schema qualifier; oversqlite supports exactly one config.schema per local database',
        );
      }
      if (!managedTables.add(tableName)) {
        throw ArgumentError(
          'duplicate sync table registration for ${syncTable.tableName}',
        );
      }
      final keyColumn = syncTable.syncKeyColumnName.trim();
      if (keyColumn.isEmpty) {
        throw ArgumentError(
          'table ${syncTable.tableName} must declare syncKeyColumnName explicitly',
        );
      }
      final tableInfo = await _loadTableInfo(tableName);
      if (tableInfo.columnNamesLower.contains(_hiddenSyncScopeColumnName)) {
        throw ArgumentError(
          'table ${syncTable.tableName} must not declare reserved server column $_hiddenSyncScopeColumnName in local oversqlite schema',
        );
      }
      tableInfoByName[tableName] = tableInfo;
      final primaryKeyColumn = _configuredPrimaryKeyColumn(
        tableInfo,
        syncTable,
        keyColumn,
      );
      validatedTables.add(
        OversqliteValidatedTable(
          tableName: tableName,
          syncKeyColumnName: primaryKeyColumn.name,
        ),
      );
    }

    _validateManagedForeignKeyClosure(tableInfoByName, managedTables);
    final tableOrder = _computeManagedTableOrder(
      tableInfoByName,
      validatedTables,
    );
    return OversqliteValidatedConfig(
      schema: schema,
      tables: List.unmodifiable(validatedTables),
      tableOrder: Map.unmodifiable(tableOrder),
    );
  }

  _ColumnInfo _configuredPrimaryKeyColumn(
    _TableInfo tableInfo,
    SyncTable syncTable,
    String configuredKeyColumn,
  ) {
    final primaryKeyColumns = tableInfo.columns
        .where((column) => column.isPrimaryKey)
        .toList();
    if (primaryKeyColumns.length != 1) {
      throw ArgumentError(
        'table ${syncTable.tableName} must declare exactly one local SQLite PRIMARY KEY column for oversqlite sync',
      );
    }
    final primaryKeyColumn = primaryKeyColumns.single;
    for (final column in tableInfo.columns) {
      if (column.name.toLowerCase() != configuredKeyColumn.toLowerCase()) {
        continue;
      }
      if (!column.isPrimaryKey) {
        throw ArgumentError(
          'configured primary key column ${column.name} for table ${syncTable.tableName} is not declared as PRIMARY KEY',
        );
      }
      if (primaryKeyColumn.name.toLowerCase() != column.name.toLowerCase()) {
        throw ArgumentError(
          'table ${syncTable.tableName} must use its only local SQLite PRIMARY KEY column as the visible sync key',
        );
      }
      if (column.kind != _ColumnKind.text && !column.kind.isBlobKind) {
        throw ArgumentError(
          'configured primary key column ${column.name} for table ${syncTable.tableName} must be TEXT PRIMARY KEY or BLOB PRIMARY KEY',
        );
      }
      if (!column.notNull) {
        throw ArgumentError(
          'configured visible sync-key column ${column.name} for table ${syncTable.tableName} must declare NOT NULL explicitly; repair or recreate the local application database before sync initialization',
        );
      }
      return column;
    }
    throw ArgumentError(
      'table ${syncTable.tableName} does not contain configured primary key column $configuredKeyColumn',
    );
  }

  void _validateManagedForeignKeyClosure(
    Map<String, _TableInfo> tableInfoByName,
    Set<String> managedTables,
  ) {
    final compositeRefs = <String>[];
    final missingRefs = <String>[];
    for (final tableName in managedTables) {
      final tableInfo = tableInfoByName[tableName]!;
      for (final foreignKey in tableInfo.foreignKeys) {
        final refTable = foreignKey.refTable;
        if (refTable.isEmpty) continue;
        if (foreignKey.seq > 0) {
          compositeRefs.add('$tableName -> $refTable');
          continue;
        }
        if (!managedTables.contains(refTable)) {
          missingRefs.add(
            '$tableName.${foreignKey.fromCol} -> $refTable.${foreignKey.toCol}',
          );
        }
      }
    }
    if (compositeRefs.isNotEmpty) {
      compositeRefs.sort();
      throw ArgumentError(
        'managed tables contain unsupported composite foreign keys: ${compositeRefs.join("; ")}',
      );
    }
    if (missingRefs.isNotEmpty) {
      missingRefs.sort();
      throw ArgumentError(
        'managed tables are not FK-closed: ${missingRefs.join("; ")}',
      );
    }
  }

  Map<String, int> _computeManagedTableOrder(
    Map<String, _TableInfo> tableInfoByName,
    List<OversqliteValidatedTable> tables,
  ) {
    final originalOrder = <String, int>{};
    for (var i = 0; i < tables.length; i++) {
      originalOrder[tables[i].tableName] = i;
    }
    final managed = tables.map((table) => table.tableName).toSet();
    final dependents = {for (final table in managed) table: <String>{}};
    final inDegree = {for (final table in managed) table: 0};

    for (final table in tables) {
      final tableInfo = tableInfoByName[table.tableName]!;
      for (final foreignKey in tableInfo.foreignKeys) {
        final refTable = foreignKey.refTable;
        if (refTable.isEmpty ||
            refTable == table.tableName ||
            !managed.contains(refTable)) {
          continue;
        }
        if (dependents[refTable]!.add(table.tableName)) {
          inDegree[table.tableName] = inDegree[table.tableName]! + 1;
        }
      }
    }

    final queue = managed.where((table) => inDegree[table] == 0).toList()
      ..sort((a, b) => originalOrder[a]!.compareTo(originalOrder[b]!));
    final ordered = <String>[];
    while (queue.isNotEmpty) {
      final current = queue.removeAt(0);
      ordered.add(current);
      final children = dependents[current]!.toList()
        ..sort((a, b) => originalOrder[a]!.compareTo(originalOrder[b]!));
      for (final child in children) {
        final next = inDegree[child]! - 1;
        inDegree[child] = next;
        if (next == 0) {
          queue.add(child);
          queue.sort((a, b) => originalOrder[a]!.compareTo(originalOrder[b]!));
        }
      }
    }
    for (final table in tables) {
      if (!ordered.contains(table.tableName)) {
        ordered.add(table.tableName);
      }
    }
    return {for (var i = 0; i < ordered.length; i++) ordered[i]: i};
  }

  Future<_TableInfo> _loadTableInfo(String tableName) async {
    final columnRows = await _connection.select(
      'PRAGMA table_info(${_quoteIdent(tableName)})',
      (row) => _RawColumnInfo(
        name: row.readString(1),
        declaredType: row.readString(2),
        notNull: row.readInt(3) == 1,
        defaultValue: row.readNullableString(4),
        isPrimaryKey: row.readInt(5) > 0,
      ),
    );
    if (columnRows.isEmpty) {
      throw ArgumentError('sync table $tableName does not exist');
    }
    final foreignKeys = await _connection.select(
      'PRAGMA foreign_key_list(${_quoteIdent(tableName)})',
      (row) => _ForeignKeyInfo(
        seq: row.readInt(1),
        refTable: row.readString(2).trim().toLowerCase(),
        fromCol: row.readString(3),
        toCol: row.readString(4),
      ),
    );
    final foreignKeyColumnsLower = {
      for (final foreignKey in foreignKeys) foreignKey.fromCol.toLowerCase(),
    };
    final columns = [
      for (final column in columnRows)
        _ColumnInfo(
          name: column.name,
          declaredType: column.declaredType,
          isPrimaryKey: column.isPrimaryKey,
          notNull: column.notNull,
          defaultValue: column.defaultValue,
          kind: _classifyColumnKind(
            column.declaredType,
            isPrimaryKey: column.isPrimaryKey,
            isBlobReference: foreignKeyColumnsLower.contains(
              column.name.toLowerCase(),
            ),
          ),
        ),
    ];
    return _TableInfo(
      table: tableName,
      columns: columns,
      foreignKeys: foreignKeys,
      foreignKeyColumnsLower: foreignKeyColumnsLower,
    );
  }

  Future<void> _registerManagedTables(
    OversqliteValidatedConfig validated,
  ) async {
    for (final table in validated.tables) {
      await _connection.execute(
        '''INSERT INTO _sync_managed_tables(schema_name, table_name)
VALUES(?, ?)
ON CONFLICT(schema_name, table_name) DO NOTHING''',
        parameters: [validated.schema, table.tableName],
      );
    }
  }

  Future<void> _installTriggers(
    OversqliteValidatedConfig validated,
    Map<String, _TableInfo> tableInfoByName,
  ) async {
    for (final table in validated.tables) {
      final tableInfo = tableInfoByName[table.tableName]!;
      final payloadExpr = _buildJsonObjectExprHexAware(tableInfo, 'NEW');
      final oldKeyExpr = _buildKeyJsonObjectExprHexAware(
        tableInfo,
        table.syncKeyColumnName,
        'OLD',
      );
      final newKeyExpr = _buildKeyJsonObjectExprHexAware(
        tableInfo,
        table.syncKeyColumnName,
        'NEW',
      );
      final data = _TriggerData(
        schemaName: validated.schema,
        tableName: table.tableName,
        newRowJson: payloadExpr,
        oldKeyJson: oldKeyExpr,
        newKeyJson: newKeyExpr,
      );
      final triggers = <String, String>{
        'trg_${table.tableName}_bi_guard': _guardInsertTriggerSql(
          table.tableName,
        ),
        'trg_${table.tableName}_bu_guard': _guardUpdateTriggerSql(
          table.tableName,
        ),
        'trg_${table.tableName}_bd_guard': _guardDeleteTriggerSql(
          table.tableName,
        ),
        'trg_${table.tableName}_ai': _insertTriggerSql(data),
        'trg_${table.tableName}_au': _updateTriggerSql(data),
        'trg_${table.tableName}_ad': _deleteTriggerSql(data),
      };
      final existingSqlByName = await _loadExistingTriggerSqlByName(
        table.tableName,
      );
      for (final entry in triggers.entries) {
        final existing = existingSqlByName[entry.key];
        if (existing != null &&
            _normalizeTriggerSql(existing) ==
                _normalizeTriggerSql(entry.value)) {
          continue;
        }
        if (existing != null) {
          await _connection.execute(
            'DROP TRIGGER IF EXISTS ${_quoteIdent(entry.key)}',
          );
        }
        await _connection.execute(entry.value);
      }
    }
  }

  Future<Map<String, String>> _loadExistingTriggerSqlByName(
    String tableName,
  ) async {
    final rows = await _connection.select(
      '''SELECT name, sql
FROM sqlite_master
WHERE type = 'trigger' AND tbl_name = ?
ORDER BY name''',
      (row) => MapEntry(row.readString(0), row.readNullableString(1) ?? ''),
      parameters: [tableName],
    );
    return Map.fromEntries(rows.where((entry) => entry.value.isNotEmpty));
  }

  Future<void> _bindLocalSource(
    OversqliteValidatedConfig validated,
    Map<String, _TableInfo> tableInfoByName,
  ) async {
    await _connection.transaction(() async {
      final operation = await _loadOperationState();
      final attachment = await _loadAttachmentState();
      final boundSourceId = attachment.currentSourceId.isEmpty
          ? _generateFreshSourceId()
          : attachment.currentSourceId;
      await _ensureSource(boundSourceId);
      if (attachment.currentSourceId.isEmpty) {
        await _capturePreexistingAnonymousRows(validated, tableInfoByName);
      }
      final isDurablyAttached =
          operation.kind != _operationKindRemoteReplace &&
          attachment.bindingState == _attachmentBindingAttached &&
          attachment.attachedUserId.isNotEmpty;
      await _persistAttachmentState(
        _AttachmentState(
          currentSourceId: boundSourceId,
          bindingState: isDurablyAttached
              ? _attachmentBindingAttached
              : _attachmentBindingAnonymous,
          attachedUserId: isDurablyAttached ? attachment.attachedUserId : '',
          schemaName: isDurablyAttached ? _config.schema.trim() : '',
          lastBundleSeqSeen: isDurablyAttached
              ? attachment.lastBundleSeqSeen
              : 0,
          rebuildRequired: isDurablyAttached && attachment.rebuildRequired,
          pendingInitializationId:
              operation.kind == _operationKindNone && isDurablyAttached
              ? attachment.pendingInitializationId
              : '',
        ),
      );
    }, mode: TransactionMode.immediate);
  }

  Future<_AttachmentState> _loadAttachmentState() async {
    final rows = await _connection.select(
      '''SELECT current_source_id, binding_state, attached_user_id, schema_name,
       last_bundle_seq_seen, rebuild_required, pending_initialization_id
FROM _sync_attachment_state
WHERE singleton_key = 1''',
      (row) => _AttachmentState(
        currentSourceId: row.readString(0),
        bindingState: row.readString(1),
        attachedUserId: row.readString(2),
        schemaName: row.readString(3),
        lastBundleSeqSeen: row.readInt(4),
        rebuildRequired: row.readInt(5) == 1,
        pendingInitializationId: row.readString(6),
      ),
    );
    if (rows.isEmpty) {
      throw StateError('_sync_attachment_state singleton row is missing');
    }
    return rows.single;
  }

  Future<_OperationState> _loadOperationState() async {
    final rows = await _connection.select(
      '''SELECT kind, target_user_id, staged_snapshot_id, snapshot_bundle_seq, snapshot_row_count,
       snapshot_byte_count, snapshot_stage_complete, reason, replacement_source_id
FROM _sync_operation_state
WHERE singleton_key = 1''',
      (row) => _OperationState(
        kind: row.readString(0),
        targetUserId: row.readString(1),
        stagedSnapshotId: row.readString(2),
        snapshotBundleSeq: row.readInt(3),
        snapshotRowCount: row.readInt(4),
        snapshotByteCount: row.readInt(5),
        snapshotStageComplete: row.readInt(6) == 1,
        reason: row.readString(7),
        replacementSourceId: row.readString(8),
      ),
    );
    if (rows.isEmpty) {
      throw StateError('_sync_operation_state singleton row is missing');
    }
    return rows.single;
  }

  Future<void> _persistAttachmentState(_AttachmentState state) {
    return _connection.execute(
      '''UPDATE _sync_attachment_state
SET current_source_id = ?,
    binding_state = ?,
    attached_user_id = ?,
    schema_name = ?,
    last_bundle_seq_seen = ?,
    rebuild_required = ?,
    pending_initialization_id = ?
WHERE singleton_key = 1''',
      parameters: [
        state.currentSourceId,
        state.bindingState,
        state.attachedUserId,
        state.schemaName,
        state.lastBundleSeqSeen,
        state.rebuildRequired ? 1 : 0,
        state.pendingInitializationId,
      ],
    );
  }

  Future<void> _ensureSource(String sourceId) {
    return _connection.execute(
      '''INSERT INTO _sync_source_state(source_id, next_source_bundle_id, replaced_by_source_id)
VALUES(?, ?, '')
ON CONFLICT(source_id) DO NOTHING''',
      parameters: [sourceId, 1],
    );
  }

  Future<void> _capturePreexistingAnonymousRows(
    OversqliteValidatedConfig validated,
    Map<String, _TableInfo> tableInfoByName,
  ) async {
    await _ensureInitialBindStateClear();
    var dirtyOrdinal = 0;
    final orderedTables = [...validated.tables]
      ..sort(
        (a, b) => (validated.tableOrder[a.tableName] ?? 1 << 30).compareTo(
          validated.tableOrder[b.tableName] ?? 1 << 30,
        ),
      );
    for (final table in orderedTables) {
      final tableInfo = tableInfoByName[table.tableName]!;
      final keyExpr = _buildKeyJsonObjectExprHexAware(
        tableInfo,
        table.syncKeyColumnName,
        'existing_row',
      );
      final payloadExpr = _buildJsonObjectExprHexAware(
        tableInfo,
        'existing_row',
      );
      final rows = await _connection.select(
        '''SELECT $keyExpr, $payloadExpr
FROM ${_quoteIdent(table.tableName)} existing_row
ORDER BY existing_row.${_quoteIdent(table.syncKeyColumnName)}''',
        (row) => (keyJson: row.readString(0), payload: row.readString(1)),
      );
      for (final row in rows) {
        dirtyOrdinal++;
        await _insertDirtyRow(
          schemaName: validated.schema,
          tableName: table.tableName,
          keyJson: row.keyJson,
          payload: row.payload,
          dirtyOrdinal: dirtyOrdinal,
        );
      }
    }
  }

  Future<void> _ensureInitialBindStateClear() async {
    final rowStateCount = await _scalarInt(
      'SELECT COUNT(*) FROM _sync_row_state',
    );
    final dirtyCount = await _scalarInt(
      'SELECT COUNT(*) FROM _sync_dirty_rows',
    );
    final outboxRowCount = await _scalarInt(
      'SELECT COUNT(*) FROM _sync_outbox_rows',
    );
    final incompatibilities = <String>[
      if (rowStateCount > 0) '_sync_row_state=$rowStateCount',
      if (dirtyCount > 0) '_sync_dirty_rows=$dirtyCount',
      if (outboxRowCount > 0) '_sync_outbox_rows=$outboxRowCount',
    ];
    if (incompatibilities.isNotEmpty) {
      throw StateError(
        'oversqlite cannot treat this database as first local capture because existing sync state is already present: ${incompatibilities.join(", ")}',
      );
    }
  }

  Future<void> _insertDirtyRow({
    required String schemaName,
    required String tableName,
    required String keyJson,
    required String payload,
    required int dirtyOrdinal,
  }) {
    return _connection.execute(
      '''INSERT INTO _sync_dirty_rows(
  schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal, updated_at
)
VALUES(?, ?, ?, 'INSERT', 0, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))''',
      parameters: [schemaName, tableName, keyJson, payload, dirtyOrdinal],
    );
  }

  Future<int> _scalarInt(String sql) async {
    final rows = await _connection.select(sql, (row) => row.readInt(0));
    return rows.single;
  }
}

String _generateFreshSourceId() {
  final random = Random.secure();
  final bytes = List<int>.generate(16, (_) => random.nextInt(256));
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  final hex = bytes
      .map((byte) => byte.toRadixString(16).padLeft(2, '0'))
      .join();
  return '${hex.substring(0, 8)}-${hex.substring(8, 12)}-${hex.substring(12, 16)}-${hex.substring(16, 20)}-${hex.substring(20)}';
}

Map<String, Object?> decodeDirtyPayload(String payload) {
  final decoded = jsonDecode(payload);
  if (decoded is Map<String, Object?>) return decoded;
  throw StateError('dirty payload was not a JSON object');
}

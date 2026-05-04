import 'dart:convert';
import 'dart:math';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';

import 'config.dart';

const _hiddenSyncScopeColumnName = '_sync_scope_id';
const _operationKindNone = 'none';
const _operationKindRemoteReplace = 'remote_replace';
const _attachmentBindingAnonymous = 'anonymous';
const _attachmentBindingAttached = 'attached';

final class OversqliteValidatedTable {
  const OversqliteValidatedTable({
    required this.tableName,
    required this.syncKeyColumnName,
  });

  final String tableName;
  final String syncKeyColumnName;
}

final class OversqliteValidatedConfig {
  const OversqliteValidatedConfig({
    required this.schema,
    required this.tables,
    required this.tableOrder,
  });

  final String schema;
  final List<OversqliteValidatedTable> tables;
  final Map<String, int> tableOrder;
}

final class OversqliteSourceInfo {
  const OversqliteSourceInfo({required this.currentSourceId});

  final String currentSourceId;
}

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
    await _initializeControlTables();
    final tableInfoByName = <String, _TableInfo>{};
    final validated = await _validateConfig(schema, tableInfoByName);
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

  Future<void> _initializeControlTables() async {
    await _connection.execute('''CREATE TABLE IF NOT EXISTS _sync_apply_state (
  singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
  apply_mode INTEGER NOT NULL DEFAULT 0
)''');
    await _connection.execute(
      '''INSERT INTO _sync_apply_state(singleton_key, apply_mode)
VALUES(1, 0)
ON CONFLICT(singleton_key) DO NOTHING''',
    );
    await _connection.execute('''CREATE TABLE IF NOT EXISTS _sync_row_state (
  schema_name TEXT NOT NULL,
  table_name TEXT NOT NULL,
  key_json TEXT NOT NULL,
  row_version INTEGER NOT NULL DEFAULT 0,
  deleted INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  PRIMARY KEY (schema_name, table_name, key_json)
)''');
    await _connection.execute('''CREATE TABLE IF NOT EXISTS _sync_dirty_rows (
  schema_name TEXT NOT NULL,
  table_name TEXT NOT NULL,
  key_json TEXT NOT NULL,
  op TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
  base_row_version INTEGER NOT NULL DEFAULT 0,
  payload TEXT,
  dirty_ordinal INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  PRIMARY KEY (schema_name, table_name, key_json)
)''');
    await _connection.execute(
      'CREATE INDEX IF NOT EXISTS idx_sync_dirty_rows_dirty_ordinal ON _sync_dirty_rows(dirty_ordinal)',
    );
    await _connection.execute(
      '''CREATE TABLE IF NOT EXISTS _sync_snapshot_stage (
  snapshot_id TEXT NOT NULL,
  row_ordinal INTEGER NOT NULL,
  schema_name TEXT NOT NULL,
  table_name TEXT NOT NULL,
  key_json TEXT NOT NULL,
  row_version INTEGER NOT NULL,
  payload TEXT NOT NULL,
  PRIMARY KEY (snapshot_id, row_ordinal)
)''',
    );
    await _connection.execute('''CREATE TABLE IF NOT EXISTS _sync_source_state (
  source_id TEXT NOT NULL PRIMARY KEY,
  next_source_bundle_id INTEGER NOT NULL DEFAULT 1,
  replaced_by_source_id TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
)''');
    await _connection.execute(
      '''CREATE TABLE IF NOT EXISTS _sync_attachment_state (
  singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
  current_source_id TEXT NOT NULL DEFAULT '',
  binding_state TEXT NOT NULL DEFAULT 'anonymous' CHECK (binding_state IN ('anonymous', 'attached')),
  attached_user_id TEXT NOT NULL DEFAULT '',
  schema_name TEXT NOT NULL DEFAULT '',
  last_bundle_seq_seen INTEGER NOT NULL DEFAULT 0,
  rebuild_required INTEGER NOT NULL DEFAULT 0,
  pending_initialization_id TEXT NOT NULL DEFAULT ''
)''',
    );
    await _connection.execute('''INSERT INTO _sync_attachment_state(
  singleton_key,
  current_source_id,
  binding_state,
  attached_user_id,
  schema_name,
  last_bundle_seq_seen,
  rebuild_required,
  pending_initialization_id
)
VALUES(1, '', 'anonymous', '', '', 0, 0, '')
ON CONFLICT(singleton_key) DO NOTHING''');
    await _connection.execute(
      '''CREATE TABLE IF NOT EXISTS _sync_operation_state (
  singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
  kind TEXT NOT NULL DEFAULT 'none' CHECK (kind IN ('none', 'remote_replace', 'source_recovery')),
  target_user_id TEXT NOT NULL DEFAULT '',
  staged_snapshot_id TEXT NOT NULL DEFAULT '',
  snapshot_bundle_seq INTEGER NOT NULL DEFAULT 0,
  snapshot_row_count INTEGER NOT NULL DEFAULT 0,
  reason TEXT NOT NULL DEFAULT '',
  replacement_source_id TEXT NOT NULL DEFAULT ''
)''',
    );
    await _ensureOperationStateSchema();
    await _connection.execute('''INSERT INTO _sync_operation_state(
  singleton_key,
  kind,
  target_user_id,
  staged_snapshot_id,
  snapshot_bundle_seq,
  snapshot_row_count,
  reason,
  replacement_source_id
)
VALUES(1, 'none', '', '', 0, 0, '', '')
ON CONFLICT(singleton_key) DO NOTHING''');
    await _connection.execute(
      '''CREATE TABLE IF NOT EXISTS _sync_outbox_bundle (
  singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
  state TEXT NOT NULL DEFAULT 'none' CHECK (state IN ('none', 'prepared', 'committed_remote')),
  source_id TEXT NOT NULL DEFAULT '',
  source_bundle_id INTEGER NOT NULL DEFAULT 0,
  initialization_id TEXT NOT NULL DEFAULT '',
  canonical_request_hash TEXT NOT NULL DEFAULT '',
  row_count INTEGER NOT NULL DEFAULT 0,
  remote_bundle_hash TEXT NOT NULL DEFAULT '',
  remote_bundle_seq INTEGER NOT NULL DEFAULT 0
)''',
    );
    await _connection.execute('''INSERT INTO _sync_outbox_bundle(
  singleton_key,
  state,
  source_id,
  source_bundle_id,
  initialization_id,
  canonical_request_hash,
  row_count,
  remote_bundle_hash,
  remote_bundle_seq
)
VALUES(1, 'none', '', 0, '', '', 0, '', 0)
ON CONFLICT(singleton_key) DO NOTHING''');
    await _connection.execute('''CREATE TABLE IF NOT EXISTS _sync_outbox_rows (
  source_bundle_id INTEGER NOT NULL,
  row_ordinal INTEGER NOT NULL,
  schema_name TEXT NOT NULL,
  table_name TEXT NOT NULL,
  key_json TEXT NOT NULL,
  wire_key_json TEXT NOT NULL,
  op TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
  base_row_version INTEGER NOT NULL DEFAULT 0,
  local_payload TEXT,
  wire_payload TEXT,
  PRIMARY KEY (source_bundle_id, row_ordinal)
)''');
    await _connection.execute(
      '''CREATE TABLE IF NOT EXISTS _sync_managed_tables (
  schema_name TEXT NOT NULL,
  table_name TEXT NOT NULL,
  PRIMARY KEY (schema_name, table_name)
)''',
    );
    await _connection.execute(
      'UPDATE _sync_apply_state SET apply_mode = 0 WHERE singleton_key = 1',
    );
  }

  Future<void> _ensureOperationStateSchema() async {
    final rows = await _connection.select(
      'PRAGMA table_info(_sync_operation_state)',
      (row) => row.readString(1).toLowerCase(),
    );
    final columns = rows.toSet();
    final hasCurrentSchema =
        columns.contains('reason') &&
        columns.contains('replacement_source_id') &&
        !columns.contains('source_recovery_reason') &&
        !columns.contains('source_recovery_source_id') &&
        !columns.contains('source_recovery_source_bundle_id') &&
        !columns.contains('source_recovery_intent_state');
    if (hasCurrentSchema) return;

    final existingRows = await _connection.select(
      '''SELECT kind, target_user_id, staged_snapshot_id, snapshot_bundle_seq, snapshot_row_count
FROM _sync_operation_state
WHERE singleton_key = 1''',
      (row) => _OperationState(
        kind: row.readString(0),
        targetUserId: row.readString(1),
        stagedSnapshotId: row.readString(2),
        snapshotBundleSeq: row.readInt(3),
        snapshotRowCount: row.readInt(4),
      ),
    );
    await _connection.execute('DROP TABLE _sync_operation_state');
    await _connection.execute('''CREATE TABLE _sync_operation_state (
  singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
  kind TEXT NOT NULL DEFAULT 'none' CHECK (kind IN ('none', 'remote_replace', 'source_recovery')),
  target_user_id TEXT NOT NULL DEFAULT '',
  staged_snapshot_id TEXT NOT NULL DEFAULT '',
  snapshot_bundle_seq INTEGER NOT NULL DEFAULT 0,
  snapshot_row_count INTEGER NOT NULL DEFAULT 0,
  reason TEXT NOT NULL DEFAULT '',
  replacement_source_id TEXT NOT NULL DEFAULT ''
)''');
    if (existingRows.isEmpty) return;
    final existing = existingRows.single;
    await _connection.execute(
      '''INSERT INTO _sync_operation_state(
  singleton_key,
  kind,
  target_user_id,
  staged_snapshot_id,
  snapshot_bundle_seq,
  snapshot_row_count,
  reason,
  replacement_source_id
)
VALUES(1, ?, ?, ?, ?, ?, '', '')''',
      parameters: [
        existing.kind,
        existing.targetUserId,
        existing.stagedSnapshotId,
        existing.snapshotBundleSeq,
        existing.snapshotRowCount,
      ],
    );
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
       reason, replacement_source_id
FROM _sync_operation_state
WHERE singleton_key = 1''',
      (row) => _OperationState(
        kind: row.readString(0),
        targetUserId: row.readString(1),
        stagedSnapshotId: row.readString(2),
        snapshotBundleSeq: row.readInt(3),
        snapshotRowCount: row.readInt(4),
        reason: row.readString(5),
        replacementSourceId: row.readString(6),
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

enum _ColumnKind { text, integer, real, blob, uuidBlob }

extension on _ColumnKind {
  bool get isBlobKind =>
      this == _ColumnKind.blob || this == _ColumnKind.uuidBlob;
}

_ColumnKind _classifyColumnKind(
  String declaredType, {
  required bool isPrimaryKey,
  required bool isBlobReference,
}) {
  final type = declaredType.toLowerCase();
  if (type.contains('blob') && (isPrimaryKey || isBlobReference)) {
    return _ColumnKind.uuidBlob;
  }
  if (type.contains('blob')) return _ColumnKind.blob;
  if (type.contains('real') ||
      type.contains('float') ||
      type.contains('double')) {
    return _ColumnKind.real;
  }
  if (type.contains('int')) return _ColumnKind.integer;
  return _ColumnKind.text;
}

final class _RawColumnInfo {
  const _RawColumnInfo({
    required this.name,
    required this.declaredType,
    required this.isPrimaryKey,
    required this.notNull,
    required this.defaultValue,
  });

  final String name;
  final String declaredType;
  final bool isPrimaryKey;
  final bool notNull;
  final String? defaultValue;
}

final class _ColumnInfo {
  const _ColumnInfo({
    required this.name,
    required this.declaredType,
    required this.isPrimaryKey,
    required this.notNull,
    required this.defaultValue,
    required this.kind,
  });

  final String name;
  final String declaredType;
  final bool isPrimaryKey;
  final bool notNull;
  final String? defaultValue;
  final _ColumnKind kind;
}

final class _ForeignKeyInfo {
  const _ForeignKeyInfo({
    required this.seq,
    required this.refTable,
    required this.fromCol,
    required this.toCol,
  });

  final int seq;
  final String refTable;
  final String fromCol;
  final String toCol;
}

final class _TableInfo {
  const _TableInfo({
    required this.table,
    required this.columns,
    required this.foreignKeys,
    required this.foreignKeyColumnsLower,
  });

  final String table;
  final List<_ColumnInfo> columns;
  final List<_ForeignKeyInfo> foreignKeys;
  final Set<String> foreignKeyColumnsLower;

  List<String> get columnNamesLower =>
      columns.map((column) => column.name.toLowerCase()).toList();
}

final class _AttachmentState {
  const _AttachmentState({
    required this.currentSourceId,
    required this.bindingState,
    required this.attachedUserId,
    required this.schemaName,
    required this.lastBundleSeqSeen,
    required this.rebuildRequired,
    required this.pendingInitializationId,
  });

  final String currentSourceId;
  final String bindingState;
  final String attachedUserId;
  final String schemaName;
  final int lastBundleSeqSeen;
  final bool rebuildRequired;
  final String pendingInitializationId;
}

final class _OperationState {
  const _OperationState({
    required this.kind,
    required this.targetUserId,
    required this.stagedSnapshotId,
    required this.snapshotBundleSeq,
    required this.snapshotRowCount,
    this.reason = '',
    this.replacementSourceId = '',
  });

  final String kind;
  final String targetUserId;
  final String stagedSnapshotId;
  final int snapshotBundleSeq;
  final int snapshotRowCount;
  final String reason;
  final String replacementSourceId;
}

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
  INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal, updated_at)
  VALUES (
    '${data.schemaName}',
    '${data.tableName}',
    ${data.newKeyJson},
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM _sync_row_state
        WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.newKeyJson} AND deleted=0
      ) THEN 'UPDATE'
      ELSE 'INSERT'
    END,
    COALESCE(
      (SELECT base_row_version FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.newKeyJson}),
      (SELECT row_version FROM _sync_row_state WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.newKeyJson}),
      0
    ),
    ${data.newRowJson},
    COALESCE(
      (SELECT dirty_ordinal FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.newKeyJson}),
      (SELECT COALESCE(MAX(dirty_ordinal), 0) + 1 FROM _sync_dirty_rows)
    ),
    strftime('%Y-%m-%dT%H:%M:%fZ','now')
  )
  ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
    op=excluded.op,
    payload=excluded.payload,
    updated_at=excluded.updated_at;
END''';

String _guardInsertTriggerSql(String tableName) =>
    '''CREATE TRIGGER IF NOT EXISTS trg_${tableName}_bi_guard
BEFORE INSERT ON ${_quoteIdent(tableName)}
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

String _guardUpdateTriggerSql(String tableName) =>
    '''CREATE TRIGGER IF NOT EXISTS trg_${tableName}_bu_guard
BEFORE UPDATE ON ${_quoteIdent(tableName)}
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

String _guardDeleteTriggerSql(String tableName) =>
    '''CREATE TRIGGER IF NOT EXISTS trg_${tableName}_bd_guard
BEFORE DELETE ON ${_quoteIdent(tableName)}
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
  INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal, updated_at)
  SELECT
    '${data.schemaName}',
    '${data.tableName}',
    ${data.oldKeyJson},
    'DELETE',
    COALESCE(
      (SELECT base_row_version FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.oldKeyJson}),
      (SELECT row_version FROM _sync_row_state WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.oldKeyJson}),
      0
    ),
    NULL,
    COALESCE(
      (SELECT dirty_ordinal FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.oldKeyJson}),
      (SELECT COALESCE(MAX(dirty_ordinal), 0) + 1 FROM _sync_dirty_rows)
    ),
    strftime('%Y-%m-%dT%H:%M:%fZ','now')
  WHERE ${data.oldKeyJson} != ${data.newKeyJson}
  ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
    op='DELETE',
    payload=NULL,
    updated_at=excluded.updated_at;

  INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal, updated_at)
  VALUES (
    '${data.schemaName}',
    '${data.tableName}',
    ${data.newKeyJson},
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM _sync_row_state
        WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.newKeyJson} AND deleted=0
      ) THEN 'UPDATE'
      ELSE 'INSERT'
    END,
    COALESCE(
      (SELECT base_row_version FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.newKeyJson}),
      (SELECT row_version FROM _sync_row_state WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.newKeyJson}),
      0
    ),
    ${data.newRowJson},
    COALESCE(
      (SELECT dirty_ordinal FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.newKeyJson}),
      (SELECT COALESCE(MAX(dirty_ordinal), 0) + 1 FROM _sync_dirty_rows)
    ),
    strftime('%Y-%m-%dT%H:%M:%fZ','now')
  )
  ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
    op=excluded.op,
    payload=excluded.payload,
    updated_at=excluded.updated_at;
END''';

String _deleteTriggerSql(_TriggerData data) =>
    '''CREATE TRIGGER IF NOT EXISTS trg_${data.tableName}_ad
AFTER DELETE ON ${_quoteIdent(data.tableName)}
WHEN COALESCE((SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1), 0) = 0
BEGIN
  INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal, updated_at)
  VALUES (
    '${data.schemaName}',
    '${data.tableName}',
    ${data.oldKeyJson},
    'DELETE',
    COALESCE(
      (SELECT base_row_version FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.oldKeyJson}),
      (SELECT row_version FROM _sync_row_state WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.oldKeyJson}),
      0
    ),
    NULL,
    COALESCE(
      (SELECT dirty_ordinal FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.oldKeyJson}),
      (SELECT COALESCE(MAX(dirty_ordinal), 0) + 1 FROM _sync_dirty_rows)
    ),
    strftime('%Y-%m-%dT%H:%M:%fZ','now')
  )
  ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
    op='DELETE',
    payload=NULL,
    updated_at=excluded.updated_at;
END''';

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

String _normalizeTriggerSql(String sql) {
  final collapsed = sql.trim().replaceAll(RegExp(r'\s+'), ' ');
  return collapsed
      .replaceFirst(
        RegExp(
          r'^CREATE\s+TRIGGER\s+IF\s+NOT\s+EXISTS\s+',
          caseSensitive: false,
        ),
        'CREATE TRIGGER ',
      )
      .replaceFirst(
        RegExp(r'^CREATE\s+TRIGGER\s+', caseSensitive: false),
        'CREATE TRIGGER ',
      );
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

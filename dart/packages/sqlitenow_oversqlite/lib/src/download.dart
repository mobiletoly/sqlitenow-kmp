import 'dart:convert';
import 'dart:typed_data';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';

import 'config.dart';
import 'local_runtime.dart';
import 'protocol.dart';

enum RemoteSyncOutcome { alreadyAtTarget, appliedIncremental, appliedSnapshot }

final class RestoreSummary {
  const RestoreSummary({required this.bundleSeq, required this.rowCount});

  final int bundleSeq;
  final int rowCount;
}

final class DownloadResult {
  const DownloadResult({
    required this.outcome,
    required this.updatedTables,
    this.restore,
    this.rotatedSourceId,
  });

  final RemoteSyncOutcome outcome;
  final Set<String> updatedTables;
  final RestoreSummary? restore;
  final String? rotatedSourceId;
}

enum SnapshotRebuildOutboxMode {
  clearAll,
  preserveCommittedRemote,
  preserveSourceRecovery,
}

final class DirtyStateRejectedException implements Exception {
  const DirtyStateRejectedException(this.pendingCount);

  final int pendingCount;

  @override
  String toString() =>
      'remote apply requires clean local dirty state; $pendingCount dirty rows remain';
}

final class PendingPushReplayException implements Exception {
  const PendingPushReplayException(this.pendingCount);

  final int pendingCount;

  @override
  String toString() =>
      'remote apply requires push replay first; $pendingCount outbox rows remain';
}

final class OversqliteDownloadRuntime {
  OversqliteDownloadRuntime({
    required SqliteNowDatabase database,
    required OversqliteConfig config,
    required OversqliteRemoteApi remoteApi,
  }) : _database = database,
       _config = config,
       _remoteApi = remoteApi;

  final SqliteNowDatabase _database;
  final OversqliteConfig _config;
  final OversqliteRemoteApi _remoteApi;

  SqliteNowConnection get _connection => _database.connection;

  Future<DownloadResult> pullToStable({
    required OversqliteValidatedConfig validated,
    required String sourceId,
    required String userId,
  }) async {
    await _requireOrdinarySyncAllowed();
    await _requireSnapshotRebuildState(SnapshotRebuildOutboxMode.clearAll);
    try {
      return await _pullIncremental(
        validated: validated,
        sourceId: sourceId,
        userId: userId,
      );
    } on HistoryPrunedException {
      return rebuildFromSnapshot(
        validated: validated,
        sourceId: sourceId,
        userId: userId,
      );
    }
  }

  Future<DownloadResult> rebuildFromSnapshot({
    required OversqliteValidatedConfig validated,
    required String sourceId,
    required String userId,
    String? rotatedSourceId,
    String? sourceReplacementReason,
    SnapshotRebuildOutboxMode outboxMode = SnapshotRebuildOutboxMode.clearAll,
  }) async {
    await _requireSnapshotRebuildState(outboxMode);
    await _connection.execute('DELETE FROM _sync_snapshot_stage');

    final session = await _remoteApi.createSnapshotSession(
      sourceId: sourceId,
      request: rotatedSourceId == null
          ? null
          : SnapshotSessionCreateRequest(
              sourceReplacement: SnapshotSourceReplacement(
                previousSourceId: sourceId,
                newSourceId: rotatedSourceId,
                reason: sourceReplacementReason ?? 'source_recovery',
              ),
            ),
    );
    try {
      var afterRowOrdinal = 0;
      while (true) {
        final chunk = await _remoteApi.fetchSnapshotChunk(
          snapshotId: session.snapshotId,
          sourceId: sourceId,
          snapshotBundleSeq: session.snapshotBundleSeq,
          afterRowOrdinal: afterRowOrdinal,
          maxRows: _config.downloadLimit <= 0 ? 1000 : _config.downloadLimit,
        );
        await _stageSnapshotChunk(
          validated: validated,
          session: session,
          chunk: chunk,
          afterRowOrdinal: afterRowOrdinal,
        );
        if (!chunk.hasMore) {
          break;
        }
        afterRowOrdinal = chunk.nextRowOrdinal;
      }
      return _applyStagedSnapshot(
        validated: validated,
        sourceId: sourceId,
        userId: userId,
        session: session,
        rotatedSourceId: rotatedSourceId,
        outboxMode: outboxMode,
      );
    } finally {
      await _remoteApi.deleteSnapshotSessionBestEffort(
        snapshotId: session.snapshotId,
        sourceId: sourceId,
      );
    }
  }

  Future<DownloadResult> _pullIncremental({
    required OversqliteValidatedConfig validated,
    required String sourceId,
    required String userId,
  }) async {
    final initialBundleSeq = await _lastBundleSeqSeen();
    var afterBundleSeq = initialBundleSeq;
    final maxBundles = _config.downloadLimit <= 0
        ? 1000
        : _config.downloadLimit;
    var targetBundleSeq = 0;
    final updatedTables = <String>{};

    while (true) {
      final response = await _remoteApi.sendPullRequest(
        sourceId: sourceId,
        afterBundleSeq: afterBundleSeq,
        maxBundles: maxBundles,
        targetBundleSeq: targetBundleSeq,
      );
      validatePullResponse(response, afterBundleSeq);
      if (targetBundleSeq == 0) {
        targetBundleSeq = response.stableBundleSeq;
      } else if (response.stableBundleSeq != targetBundleSeq) {
        throw OversqliteProtocolException(
          'pull response stable bundle seq changed from $targetBundleSeq to ${response.stableBundleSeq}',
        );
      }

      for (final bundle in response.bundles) {
        updatedTables.addAll(
          await _applyPulledBundle(validated: validated, bundle: bundle),
        );
        afterBundleSeq = bundle.bundleSeq;
      }

      if (afterBundleSeq >= response.stableBundleSeq) {
        return DownloadResult(
          outcome:
              afterBundleSeq == initialBundleSeq &&
                  response.stableBundleSeq == initialBundleSeq
              ? RemoteSyncOutcome.alreadyAtTarget
              : RemoteSyncOutcome.appliedIncremental,
          updatedTables: updatedTables,
        );
      }
      if (!response.hasMore) {
        throw OversqliteProtocolException(
          'pull ended early at bundle seq $afterBundleSeq before stable bundle seq ${response.stableBundleSeq}',
        );
      }
    }
  }

  Future<Set<String>> _applyPulledBundle({
    required OversqliteValidatedConfig validated,
    required Bundle bundle,
  }) async {
    final updatedTables = <String>{};
    await _connection.transaction(() async {
      await _beginApplyMode();
      try {
        for (final row in bundle.rows) {
          if (row.schema != validated.schema) {
            throw OversqliteProtocolException(
              'bundle row schema ${row.schema} does not match ${validated.schema}',
            );
          }
          final key = await _localKeyFromWire(row.table, row.key);
          final currentVersion = await _currentRowVersion(
            schema: row.schema,
            table: row.table,
            keyJson: key.keyJson,
          );
          if (currentVersion != null && currentVersion >= row.rowVersion) {
            continue;
          }
          await _applyAuthoritativeRow(
            row,
            keyJson: key.keyJson,
            localPk: key.localPk,
          );
          await _connection.execute(
            'DELETE FROM _sync_dirty_rows WHERE schema_name = ? AND table_name = ? AND key_json = ?',
            parameters: [row.schema, row.table, key.keyJson],
          );
          updatedTables.add(row.table.toLowerCase());
        }
        await _connection.execute(
          'UPDATE _sync_attachment_state SET last_bundle_seq_seen = ? WHERE singleton_key = 1',
          parameters: [bundle.bundleSeq],
        );
      } finally {
        await _endApplyMode();
      }
    }, mode: TransactionMode.immediate);
    return updatedTables;
  }

  Future<void> _stageSnapshotChunk({
    required OversqliteValidatedConfig validated,
    required SnapshotSession session,
    required SnapshotChunkResponse chunk,
    required int afterRowOrdinal,
  }) async {
    await _connection.transaction(() async {
      var rowOrdinal = afterRowOrdinal;
      for (final row in chunk.rows) {
        if (row.schema != validated.schema) {
          throw OversqliteProtocolException(
            'snapshot row schema ${row.schema} does not match client schema ${validated.schema}',
          );
        }
        final key = await _localKeyFromWire(row.table, row.key);
        rowOrdinal++;
        await _connection.execute(
          '''INSERT INTO _sync_snapshot_stage(
  snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload
) VALUES (?, ?, ?, ?, ?, ?, ?)''',
          parameters: [
            session.snapshotId,
            rowOrdinal,
            row.schema,
            row.table,
            key.keyJson,
            row.rowVersion,
            jsonEncode(row.payload),
          ],
        );
      }
    }, mode: TransactionMode.immediate);
  }

  Future<DownloadResult> _applyStagedSnapshot({
    required OversqliteValidatedConfig validated,
    required String sourceId,
    required String userId,
    required SnapshotSession session,
    required String? rotatedSourceId,
    required SnapshotRebuildOutboxMode outboxMode,
  }) async {
    final currentSourceId = rotatedSourceId ?? sourceId;
    final updatedTables = {
      for (final table in validated.tables) table.tableName.toLowerCase(),
    };
    await _connection.transaction(() async {
      await _connection.execute('PRAGMA defer_foreign_keys = ON');
      await _beginApplyMode();
      try {
        final preservedOutbox = await _loadOutboxBundle();
        await _clearManagedTables(
          validated,
          clearOutbox: outboxMode == SnapshotRebuildOutboxMode.clearAll,
        );
        var stagedRowCount = 0;
        final rows = await _connection.select(
          '''SELECT table_name, key_json, row_version, payload
FROM _sync_snapshot_stage
WHERE snapshot_id = ?
ORDER BY row_ordinal''',
          (row) => _StagedSnapshotRow(
            tableName: row.readString(0),
            keyJson: row.readString(1),
            rowVersion: row.readInt(2),
            payload: row.readString(3),
          ),
          parameters: [session.snapshotId],
        );
        for (final staged in rows) {
          final table = await _tableInfo(staged.tableName);
          final localPk = _localPkFromKeyJson(table, staged.keyJson);
          final wireKey = _wireKeyFromLocalKey(table, localPk);
          await _applyAuthoritativeRow(
            BundleRow(
              schema: validated.schema,
              table: staged.tableName,
              key: wireKey,
              op: 'INSERT',
              rowVersion: staged.rowVersion,
              payload: jsonDecode(staged.payload),
            ),
            keyJson: staged.keyJson,
            localPk: localPk,
          );
          stagedRowCount++;
        }
        if (stagedRowCount != session.rowCount) {
          throw OversqliteProtocolException(
            'staged snapshot row count $stagedRowCount does not match expected row_count ${session.rowCount}',
          );
        }
        if (rotatedSourceId != null && sourceId != rotatedSourceId) {
          await _ensureSource(rotatedSourceId);
          await _connection.execute(
            'UPDATE _sync_source_state SET replaced_by_source_id = ? WHERE source_id = ?',
            parameters: [rotatedSourceId, sourceId],
          );
        }
        switch (outboxMode) {
          case SnapshotRebuildOutboxMode.clearAll:
            break;
          case SnapshotRebuildOutboxMode.preserveCommittedRemote:
            if (preservedOutbox.state == _outboxStateCommittedRemote &&
                preservedOutbox.rowCount > 0) {
              await _advanceSourceAfterCommittedPush(
                sourceId: sourceId,
                sourceBundleId: preservedOutbox.sourceBundleId,
              );
              await _clearOutbox();
            }
          case SnapshotRebuildOutboxMode.preserveSourceRecovery:
            if (preservedOutbox.state != _outboxStateNone &&
                preservedOutbox.rowCount > 0) {
              await _ensureSource(currentSourceId);
              await _connection.execute(
                '''UPDATE _sync_source_state
SET next_source_bundle_id = CASE
  WHEN next_source_bundle_id < 2 THEN 2
  ELSE next_source_bundle_id
END
WHERE source_id = ?''',
                parameters: [currentSourceId],
              );
              await _connection.execute(
                '''UPDATE _sync_outbox_bundle
SET source_id = ?,
    source_bundle_id = 1
WHERE singleton_key = 1''',
                parameters: [currentSourceId],
              );
              await _connection.execute(
                'UPDATE _sync_outbox_rows SET source_bundle_id = 1',
              );
            }
        }
        await _connection.execute(
          '''UPDATE _sync_attachment_state
SET current_source_id = ?,
    binding_state = 'attached',
    attached_user_id = ?,
    schema_name = ?,
    last_bundle_seq_seen = ?,
    rebuild_required = 0,
    pending_initialization_id = ''
WHERE singleton_key = 1''',
          parameters: [
            currentSourceId,
            userId,
            validated.schema,
            session.snapshotBundleSeq,
          ],
        );
        await _connection.execute('''UPDATE _sync_operation_state
SET kind = 'none',
    target_user_id = '',
    staged_snapshot_id = '',
    snapshot_bundle_seq = 0,
    snapshot_row_count = 0,
    reason = '',
    replacement_source_id = ''
WHERE singleton_key = 1''');
        await _connection.execute(
          'DELETE FROM _sync_snapshot_stage WHERE snapshot_id = ?',
          parameters: [session.snapshotId],
        );
      } finally {
        await _endApplyMode();
      }
    }, mode: TransactionMode.immediate);

    return DownloadResult(
      outcome: RemoteSyncOutcome.appliedSnapshot,
      updatedTables: updatedTables,
      restore: RestoreSummary(
        bundleSeq: session.snapshotBundleSeq,
        rowCount: session.rowCount,
      ),
      rotatedSourceId: rotatedSourceId,
    );
  }

  Future<void> _applyAuthoritativeRow(
    BundleRow row, {
    required String keyJson,
    required String localPk,
  }) async {
    if (row.op == 'DELETE') {
      await _deleteLocalRow(row.table, localPk);
      await _updateRowState(
        schema: row.schema,
        table: row.table,
        keyJson: keyJson,
        rowVersion: row.rowVersion,
        deleted: true,
      );
      return;
    }
    final payload = row.payload;
    if (payload is! Map) {
      throw OversqliteProtocolException(
        'bundle row payload must be a JSON object for ${row.schema}.${row.table}',
      );
    }
    await _upsertPayload(row.table, payload.cast<String, Object?>());
    await _updateRowState(
      schema: row.schema,
      table: row.table,
      keyJson: keyJson,
      rowVersion: row.rowVersion,
      deleted: false,
    );
  }

  Future<void> _clearManagedTables(
    OversqliteValidatedConfig validated, {
    required bool clearOutbox,
  }) async {
    final tables = [...validated.tables]
      ..sort(
        (left, right) =>
            (validated.tableOrder[right.tableName] ?? 0) -
            (validated.tableOrder[left.tableName] ?? 0),
      );
    for (final table in tables) {
      await _connection.execute('DELETE FROM ${_quoteIdent(table.tableName)}');
      await _connection.execute(
        'DELETE FROM _sync_row_state WHERE schema_name = ? AND table_name = ?',
        parameters: [validated.schema, table.tableName],
      );
    }
    await _connection.execute('DELETE FROM _sync_dirty_rows');
    if (clearOutbox) {
      await _clearOutbox();
    }
  }

  Future<void> _requireOrdinarySyncAllowed() async {
    final operation = await _operationState();
    if (operation.kind == 'source_recovery') {
      throw SourceRecoveryRequiredException(
        _sourceRecoveryReasonFromPersisted(operation.reason),
      );
    }
    final rebuildRequired = await _scalarInt(
      'SELECT rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1',
    );
    if (rebuildRequired == 1) {
      throw const RebuildRequiredException();
    }
  }

  Future<void> _requireSnapshotRebuildState(
    SnapshotRebuildOutboxMode outboxMode,
  ) async {
    final outbox = await _loadOutboxBundle();
    final outboxRows = await _scalarInt(
      'SELECT COUNT(*) FROM _sync_outbox_rows',
    );
    switch (outboxMode) {
      case SnapshotRebuildOutboxMode.clearAll:
        if (outboxRows > 0) {
          throw PendingPushReplayException(outboxRows);
        }
      case SnapshotRebuildOutboxMode.preserveCommittedRemote:
        if (outboxRows > 0 && outbox.state != _outboxStateCommittedRemote) {
          throw PendingPushReplayException(outboxRows);
        }
      case SnapshotRebuildOutboxMode.preserveSourceRecovery:
        if (outboxRows > 0 && outbox.state == _outboxStateNone) {
          throw PendingPushReplayException(outboxRows);
        }
    }
    final dirtyRows = await _scalarInt('SELECT COUNT(*) FROM _sync_dirty_rows');
    if (dirtyRows > 0) {
      throw DirtyStateRejectedException(dirtyRows);
    }
  }

  Future<void> _beginApplyMode() async {
    await _connection.execute(
      'UPDATE _sync_apply_state SET apply_mode = 1 WHERE singleton_key = 1',
    );
  }

  Future<void> _endApplyMode() async {
    await _connection.execute(
      'UPDATE _sync_apply_state SET apply_mode = 0 WHERE singleton_key = 1',
    );
  }

  Future<int> _lastBundleSeqSeen() {
    return _scalarInt(
      'SELECT last_bundle_seq_seen FROM _sync_attachment_state WHERE singleton_key = 1',
    );
  }

  Future<int?> _currentRowVersion({
    required String schema,
    required String table,
    required String keyJson,
  }) async {
    final rows = await _connection.select(
      '''SELECT row_version
FROM _sync_row_state
WHERE schema_name = ? AND table_name = ? AND key_json = ?''',
      (row) => row.readInt(0),
      parameters: [schema, table, keyJson],
    );
    return rows.isEmpty ? null : rows.single;
  }

  Future<void> _upsertPayload(
    String tableName,
    Map<String, Object?> payload,
  ) async {
    final table = await _tableInfo(tableName);
    final normalized = {
      for (final entry in payload.entries) entry.key.toLowerCase(): entry.value,
    };
    final columns = table.columns.map((column) => column.name).toList();
    final updates = [
      for (final column in table.columns)
        if (!column.primaryKey)
          '${_quoteIdent(column.name)} = excluded.${_quoteIdent(column.name)}',
    ];
    await _connection.execute(
      '''INSERT INTO ${_quoteIdent(tableName)} (${columns.map(_quoteIdent).join(', ')})
VALUES (${columns.map((_) => '?').join(', ')})
ON CONFLICT(${_quoteIdent(table.primaryKey.name)}) DO UPDATE SET ${updates.join(', ')}''',
      parameters: [
        for (final column in table.columns)
          _bindPayloadValue(column, normalized[column.name.toLowerCase()]),
      ],
    );
  }

  Future<void> _deleteLocalRow(String tableName, String localPk) async {
    final table = await _tableInfo(tableName);
    await _connection.execute(
      'DELETE FROM ${_quoteIdent(tableName)} WHERE ${_quoteIdent(table.primaryKey.name)} = ?',
      parameters: [_bindPrimaryKey(table, localPk)],
    );
  }

  Future<void> _updateRowState({
    required String schema,
    required String table,
    required String keyJson,
    required int rowVersion,
    required bool deleted,
  }) async {
    await _connection.execute(
      '''INSERT INTO _sync_row_state(schema_name, table_name, key_json, row_version, deleted, updated_at)
VALUES(?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
  row_version = excluded.row_version,
  deleted = excluded.deleted,
  updated_at = excluded.updated_at''',
      parameters: [schema, table, keyJson, rowVersion, deleted ? 1 : 0],
    );
  }

  Future<_TableInfo> _tableInfo(String tableName) async {
    final rows = await _connection.select(
      'PRAGMA table_info(${_quoteIdent(tableName)})',
      (row) => _ColumnInfo(
        name: row.readString(1),
        kind: _columnKind(row.readString(2), row.readInt(5) > 0),
        primaryKey: row.readInt(5) > 0,
      ),
    );
    final pk = rows.singleWhere((column) => column.primaryKey);
    return _TableInfo(name: tableName, columns: rows, primaryKey: pk);
  }

  Future<({String keyJson, String localPk})> _localKeyFromWire(
    String tableName,
    SyncKey key,
  ) async {
    final table = await _tableInfo(tableName);
    final keyColumn = table.primaryKey.name.toLowerCase();
    final wireValue = key[keyColumn] ?? key[table.primaryKey.name];
    if (wireValue == null) {
      throw OversqliteProtocolException(
        'bundle row key for $tableName is missing ${table.primaryKey.name}',
      );
    }
    final localPk = table.primaryKey.kind == _ColumnKind.uuidBlob
        ? _hex(_decodeWireUuid(wireValue))
        : wireValue;
    return (keyJson: _canonicalizeJson({keyColumn: localPk}), localPk: localPk);
  }

  SyncKey _wireKeyFromLocalKey(_TableInfo table, String localPk) {
    return {
      table.primaryKey.name
          .toLowerCase(): table.primaryKey.kind == _ColumnKind.uuidBlob
          ? _canonicalUuid(localPk)
          : localPk,
    };
  }

  Future<_OutboxBundle> _loadOutboxBundle() async {
    final rows = await _connection.select(
      '''SELECT state, source_id, source_bundle_id, row_count, remote_bundle_seq
FROM _sync_outbox_bundle
WHERE singleton_key = 1''',
      (row) => _OutboxBundle(
        state: row.readString(0),
        sourceId: row.readString(1),
        sourceBundleId: row.readInt(2),
        rowCount: row.readInt(3),
        remoteBundleSeq: row.readInt(4),
      ),
    );
    return rows.single;
  }

  Future<void> _clearOutbox() async {
    await _connection.execute('DELETE FROM _sync_outbox_rows');
    await _connection.execute('''UPDATE _sync_outbox_bundle
SET state = 'none',
    source_id = '',
    source_bundle_id = 0,
    initialization_id = '',
    canonical_request_hash = '',
    row_count = 0,
    remote_bundle_hash = '',
    remote_bundle_seq = 0
WHERE singleton_key = 1''');
  }

  Future<void> _advanceSourceAfterCommittedPush({
    required String sourceId,
    required int sourceBundleId,
  }) async {
    await _connection.execute(
      '''UPDATE _sync_source_state
SET next_source_bundle_id = CASE
  WHEN next_source_bundle_id <= ? THEN ? + 1
  ELSE next_source_bundle_id
END
WHERE source_id = ?''',
      parameters: [sourceBundleId, sourceBundleId, sourceId],
    );
  }

  Future<void> _ensureSource(String sourceId) {
    return _connection.execute(
      '''INSERT INTO _sync_source_state(source_id, next_source_bundle_id, replaced_by_source_id)
VALUES(?, 1, '')
ON CONFLICT(source_id) DO NOTHING''',
      parameters: [sourceId],
    );
  }

  Future<_OperationState> _operationState() async {
    final rows = await _connection.select(
      'SELECT kind, reason FROM _sync_operation_state WHERE singleton_key = 1',
      (row) =>
          _OperationState(kind: row.readString(0), reason: row.readString(1)),
    );
    return rows.single;
  }

  Future<int> _scalarInt(String sql) async {
    final rows = await _connection.select(sql, (row) => row.readInt(0));
    return rows.single;
  }
}

final class _OperationState {
  const _OperationState({required this.kind, required this.reason});

  final String kind;
  final String reason;
}

final class _OutboxBundle {
  const _OutboxBundle({
    required this.state,
    required this.sourceId,
    required this.sourceBundleId,
    required this.rowCount,
    required this.remoteBundleSeq,
  });

  final String state;
  final String sourceId;
  final int sourceBundleId;
  final int rowCount;
  final int remoteBundleSeq;
}

final class _StagedSnapshotRow {
  const _StagedSnapshotRow({
    required this.tableName,
    required this.keyJson,
    required this.rowVersion,
    required this.payload,
  });

  final String tableName;
  final String keyJson;
  final int rowVersion;
  final String payload;
}

final class _TableInfo {
  const _TableInfo({
    required this.name,
    required this.columns,
    required this.primaryKey,
  });

  final String name;
  final List<_ColumnInfo> columns;
  final _ColumnInfo primaryKey;
}

final class _ColumnInfo {
  const _ColumnInfo({
    required this.name,
    required this.kind,
    required this.primaryKey,
  });

  final String name;
  final _ColumnKind kind;
  final bool primaryKey;
}

enum _ColumnKind { text, integer, real, blob, uuidBlob }

const _outboxStateNone = 'none';
const _outboxStateCommittedRemote = 'committed_remote';

_ColumnKind _columnKind(String type, bool primaryKey) {
  final upper = type.trim().toUpperCase();
  if (upper.contains('BLOB')) {
    return primaryKey ? _ColumnKind.uuidBlob : _ColumnKind.blob;
  }
  if (upper.contains('INT')) {
    return _ColumnKind.integer;
  }
  if (upper.contains('REAL') ||
      upper.contains('FLOA') ||
      upper.contains('DOUB')) {
    return _ColumnKind.real;
  }
  return _ColumnKind.text;
}

Object? _bindPayloadValue(_ColumnInfo column, Object? value) {
  if (value == null) {
    return null;
  }
  return switch (column.kind) {
    _ColumnKind.blob => base64Decode(value.toString()),
    _ColumnKind.uuidBlob => _decodeWireUuid(value.toString()),
    _ColumnKind.integer =>
      value is bool
          ? (value ? 1 : 0)
          : value is int
          ? value
          : int.parse(value.toString()),
    _ColumnKind.real => double.parse(value.toString()),
    _ColumnKind.text => value.toString(),
  };
}

Object _bindPrimaryKey(_TableInfo table, String localPk) {
  if (table.primaryKey.kind == _ColumnKind.uuidBlob) {
    return _decodeLocalUuid(localPk);
  }
  return localPk;
}

String _localPkFromKeyJson(_TableInfo table, String keyJson) {
  final key = (jsonDecode(keyJson) as Map).cast<String, Object?>();
  return key[table.primaryKey.name.toLowerCase()].toString();
}

Uint8List _decodeLocalUuid(String value) {
  final normalized = value.trim();
  if (RegExp(r'^[0-9a-fA-F]{32}$').hasMatch(normalized)) {
    return _decodeHex(normalized);
  }
  if (RegExp(
    r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$',
  ).hasMatch(normalized)) {
    return _decodeHex(normalized.replaceAll('-', ''));
  }
  final decoded = base64Decode(value);
  if (decoded.length != 16) {
    throw ArgumentError('UUID BLOB must decode to 16 bytes');
  }
  return Uint8List.fromList(decoded);
}

Uint8List _decodeWireUuid(String value) {
  final normalized = value.trim();
  if (normalized != value ||
      !RegExp(
        r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
      ).hasMatch(normalized)) {
    throw ArgumentError('invalid canonical wire UUID encoding');
  }
  return _decodeHex(normalized.replaceAll('-', ''));
}

String _canonicalUuid(String value) {
  final hex = _hex(_decodeLocalUuid(value));
  return '${hex.substring(0, 8)}-${hex.substring(8, 12)}-${hex.substring(12, 16)}-${hex.substring(16, 20)}-${hex.substring(20)}';
}

Uint8List _decodeHex(String value) {
  if (!value.length.isEven) {
    throw ArgumentError('hex value must have even length');
  }
  return Uint8List.fromList([
    for (var i = 0; i < value.length; i += 2)
      int.parse(value.substring(i, i + 2), radix: 16),
  ]);
}

String _hex(List<int> bytes) {
  return bytes
      .map((byte) => (byte & 0xff).toRadixString(16).padLeft(2, '0'))
      .join();
}

String _canonicalizeJson(Object? value) {
  if (value == null) {
    return 'null';
  }
  if (value is String || value is bool || value is num) {
    return jsonEncode(value);
  }
  if (value is List) {
    return '[${value.map(_canonicalizeJson).join(',')}]';
  }
  if (value is Map) {
    final entries = value.entries.toList()
      ..sort(
        (left, right) => left.key.toString().compareTo(right.key.toString()),
      );
    return '{${entries.map((entry) => '${jsonEncode(entry.key.toString())}:${_canonicalizeJson(entry.value)}').join(',')}}';
  }
  return jsonEncode(value);
}

SourceRecoveryReason _sourceRecoveryReasonFromPersisted(String value) {
  return switch (value) {
    'history_pruned' => SourceRecoveryReason.historyPruned,
    'source_sequence_out_of_order' =>
      SourceRecoveryReason.sourceSequenceOutOfOrder,
    'source_sequence_changed' => SourceRecoveryReason.sourceSequenceChanged,
    'source_retired' => SourceRecoveryReason.sourceRetired,
    _ => SourceRecoveryReason.historyPruned,
  };
}

String _quoteIdent(String identifier) =>
    '"${identifier.replaceAll('"', '""')}"';

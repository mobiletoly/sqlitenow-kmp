import 'dart:convert';
import 'dart:typed_data';

import 'package:crypto/crypto.dart';
import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';

import 'config.dart';
import 'local_runtime.dart';
import 'protocol.dart';
import 'resolver.dart';
import 'runtime_state_store.dart';

enum PushOutcome { noChange, committed }

final class PushReport {
  const PushReport({required this.outcome, required this.updatedTables});

  final PushOutcome outcome;
  final Set<String> updatedTables;
}

final class OversqlitePushRuntime {
  OversqlitePushRuntime({
    required SqliteNowDatabase database,
    required OversqliteConfig config,
    required OversqliteRemoteApi remoteApi,
    required Resolver resolver,
  }) : _database = database,
       _config = config,
       _remoteApi = remoteApi,
       _resolver = resolver;

  final SqliteNowDatabase _database;
  final OversqliteConfig _config;
  final OversqliteRemoteApi _remoteApi;
  final Resolver _resolver;

  SqliteNowConnection get _connection => _database.connection;
  OversqliteApplyRunner get _applyRunner => OversqliteApplyRunner(_connection);
  OversqliteAttachmentStateStore get _attachmentStore =>
      OversqliteAttachmentStateStore(_connection);
  OversqliteSourceStateStore get _sourceStore =>
      OversqliteSourceStateStore(_connection);

  Future<PushReport> pushPending({
    required OversqliteValidatedConfig validated,
    required String sourceId,
    required String pendingInitializationId,
    int conflictRetryCount = 0,
  }) async {
    final snapshot = await _ensureSnapshot(
      validated: validated,
      sourceId: sourceId,
      pendingInitializationId: pendingInitializationId,
    );
    if (snapshot.rows.isEmpty) {
      return const PushReport(outcome: PushOutcome.noChange, updatedTables: {});
    }

    late final _CommittedPush committed;
    try {
      committed = snapshot.state == _outboxStateCommittedRemote
          ? _CommittedPush(
              bundleSeq: snapshot.remoteBundleSeq,
              sourceId: sourceId,
              sourceBundleId: snapshot.sourceBundleId,
              rowCount: snapshot.rows.length,
              bundleHash: snapshot.remoteBundleHash,
              requiresStrictOutboxMatch: false,
            )
          : await _commitSnapshot(
              snapshot: snapshot,
              sourceId: sourceId,
              pendingInitializationId: pendingInitializationId,
            );
    } on PushConflictException catch (error) {
      await _resolveConflict(
        validated: validated,
        snapshot: snapshot,
        conflict: error.conflict,
      );
      final remainingDirtyCount = await _scalarInt(
        'SELECT COUNT(*) FROM _sync_dirty_rows',
      );
      if (remainingDirtyCount == 0) {
        return PushReport(
          outcome: PushOutcome.noChange,
          updatedTables: {error.conflict.table.toLowerCase()},
        );
      }
      if (conflictRetryCount >= 2) {
        throw PushConflictRetryExhaustedException(
          retryCount: 2,
          remainingDirtyCount: remainingDirtyCount,
        );
      }
      final retry = await pushPending(
        validated: validated,
        sourceId: sourceId,
        pendingInitializationId: pendingInitializationId,
        conflictRetryCount: conflictRetryCount + 1,
      );
      return PushReport(
        outcome: retry.outcome,
        updatedTables: {
          error.conflict.table.toLowerCase(),
          ...retry.updatedTables,
        },
      );
    }

    final committedRows = await _fetchCommittedRows(snapshot, committed);
    final updatedTables = await _applyCommittedRows(
      validated: validated,
      snapshot: snapshot,
      committed: committed,
      rows: committedRows,
      sourceId: sourceId,
    );
    return PushReport(
      outcome: PushOutcome.committed,
      updatedTables: updatedTables,
    );
  }

  Future<_PushSnapshot> _ensureSnapshot({
    required OversqliteValidatedConfig validated,
    required String sourceId,
    required String pendingInitializationId,
  }) async {
    final outbox = await _loadOutboxBundle();
    if (outbox.state != _outboxStateNone) {
      return _loadPersistedSnapshot(outbox);
    }

    final sourceBundleId = await _nextSourceBundleId(sourceId);
    final dirtyRows = await _loadDirtyRows(validated);
    final canonicalRows = <_OutboxRow>[];
    for (final row in dirtyRows) {
      final table = await _tableInfo(row.tableName);
      final wireKey = _wireKeyJson(table, row.localPk);
      final wirePayload = row.op == 'DELETE'
          ? null
          : _canonicalizeProtocolJson(
              _wirePayloadForUpload(table, jsonDecode(row.localPayload!)),
            );
      canonicalRows.add(
        _OutboxRow(
          sourceBundleId: sourceBundleId,
          rowOrdinal: canonicalRows.length,
          schemaName: row.schemaName,
          tableName: row.tableName,
          keyJson: row.keyJson,
          wireKeyJson: wireKey,
          op: row.op,
          baseRowVersion: row.baseRowVersion,
          localPayload: row.localPayload,
          wirePayload: wirePayload,
        ),
      );
    }

    final canonicalHash = canonicalRows.isEmpty
        ? ''
        : _sha256Hex(
            _canonicalizeProtocolJson([
              for (final row in canonicalRows) row.canonicalRequestJson(),
            ]),
          );
    await _connection.transaction(() async {
      await _clearOutbox();
      if (canonicalRows.isNotEmpty) {
        await _persistOutboxBundle(
          _OutboxBundle(
            state: _outboxStatePrepared,
            sourceId: sourceId,
            sourceBundleId: sourceBundleId,
            initializationId: pendingInitializationId,
            canonicalRequestHash: canonicalHash,
            rowCount: canonicalRows.length,
          ),
        );
        for (final row in canonicalRows) {
          await _insertOutboxRow(row);
        }
      }
      for (final row in dirtyRows) {
        await _connection.execute(
          'DELETE FROM _sync_dirty_rows WHERE schema_name = ? AND table_name = ? AND key_json = ?',
          parameters: [row.schemaName, row.tableName, row.keyJson],
        );
      }
    }, mode: TransactionMode.immediate);

    return _PushSnapshot(
      state: canonicalRows.isEmpty ? _outboxStateNone : _outboxStatePrepared,
      sourceId: sourceId,
      sourceBundleId: sourceBundleId,
      initializationId: pendingInitializationId,
      canonicalRequestHash: canonicalHash,
      remoteBundleHash: '',
      remoteBundleSeq: 0,
      rows: canonicalRows,
    );
  }

  Future<_CommittedPush> _commitSnapshot({
    required _PushSnapshot snapshot,
    required String sourceId,
    required String pendingInitializationId,
  }) async {
    final create = await _remoteApi.createPushSession(
      sourceId: sourceId,
      sourceBundleId: snapshot.sourceBundleId,
      plannedRowCount: snapshot.rows.length,
      initializationId: pendingInitializationId,
    );
    switch (create.status) {
      case 'already_committed':
        final committed = _CommittedPush(
          bundleSeq: create.bundleSeq,
          sourceId: sourceId,
          sourceBundleId: snapshot.sourceBundleId,
          rowCount: create.rowCount,
          bundleHash: create.bundleHash,
          requiresStrictOutboxMatch: true,
        );
        return committed;
      case 'staging':
        final pushId = create.pushId;
        final chunkSize = _config.uploadLimit <= 0 ? 1000 : _config.uploadLimit;
        try {
          var start = 0;
          while (start < snapshot.rows.length) {
            final end = (start + chunkSize).clamp(0, snapshot.rows.length);
            final response = await _remoteApi.uploadPushChunk(
              pushId: pushId,
              sourceId: sourceId,
              startRowOrdinal: start,
              rows: [
                for (final row in snapshot.rows.sublist(start, end))
                  row.toPushRequestRow(),
              ],
            );
            if (response.pushId != pushId ||
                response.nextExpectedRowOrdinal != end) {
              throw const OversqliteProtocolException(
                'push chunk response did not match requested chunk',
              );
            }
            start = end;
          }
          final committed = await _remoteApi.commitPushSession(
            pushId: pushId,
            sourceId: sourceId,
          );
          _validateCommittedPush(
            committed,
            sourceId: sourceId,
            sourceBundleId: snapshot.sourceBundleId,
          );
          final committedPush = _CommittedPush(
            bundleSeq: committed.bundleSeq,
            sourceId: sourceId,
            sourceBundleId: snapshot.sourceBundleId,
            rowCount: committed.rowCount,
            bundleHash: committed.bundleHash,
            requiresStrictOutboxMatch: false,
          );
          await _persistCommittedRemote(snapshot, committedPush);
          return committedPush;
        } catch (_) {
          await _remoteApi.deletePushSessionBestEffort(
            pushId: pushId,
            sourceId: sourceId,
          );
          rethrow;
        }
      default:
        throw OversqliteProtocolException(
          'unexpected push session status ${create.status}',
        );
    }
  }

  Future<List<BundleRow>> _fetchCommittedRows(
    _PushSnapshot snapshot,
    _CommittedPush committed,
  ) async {
    var afterRowOrdinal = null as int?;
    final rows = <BundleRow>[];
    while (true) {
      final chunk = await _fetchCommittedChunkWithRetry(
        committed: committed,
        afterRowOrdinal: afterRowOrdinal,
      );
      _validateCommittedChunk(
        committed: committed,
        chunk: chunk,
        afterRowOrdinal: afterRowOrdinal,
      );
      rows.addAll(chunk.rows);
      if (!chunk.hasMore) {
        break;
      }
      afterRowOrdinal = chunk.nextRowOrdinal;
    }
    if (rows.length != committed.rowCount) {
      throw OversqliteProtocolException(
        'committed bundle row count ${rows.length} does not match expected ${committed.rowCount}',
      );
    }
    final bundleHash = _computeCommittedBundleHash(rows);
    if (bundleHash != committed.bundleHash) {
      throw OversqliteProtocolException(
        'fetched committed bundle hash $bundleHash does not match expected ${committed.bundleHash}',
      );
    }
    await _assertCommittedRowsMatchOutbox(snapshot, committed, rows);
    return rows;
  }

  Future<CommittedBundleRowsResponse> _fetchCommittedChunkWithRetry({
    required _CommittedPush committed,
    required int? afterRowOrdinal,
  }) async {
    var attempt = 1;
    var backoff = _initialCommittedBundleFetchBackoff;
    while (true) {
      try {
        return await _remoteApi.fetchCommittedBundleChunk(
          bundleSeq: committed.bundleSeq,
          sourceId: committed.sourceId,
          afterRowOrdinal: afterRowOrdinal,
          maxRows: _config.downloadLimit <= 0 ? 1000 : _config.downloadLimit,
        );
      } on CommittedBundleNotFoundException {
        if (attempt >= _maxCommittedBundleFetchAttempts) {
          rethrow;
        }
        await Future<void>.delayed(backoff);
        final nextMillis = (backoff.inMilliseconds * 2).clamp(
          0,
          _maxCommittedBundleFetchBackoff.inMilliseconds,
        );
        backoff = Duration(milliseconds: nextMillis);
        attempt++;
      }
    }
  }

  Future<Set<String>> _applyCommittedRows({
    required OversqliteValidatedConfig validated,
    required _PushSnapshot snapshot,
    required _CommittedPush committed,
    required List<BundleRow> rows,
    required String sourceId,
  }) async {
    final updatedTables = <String>{};
    await _applyRunner.inApplyModeTransaction(() async {
      for (var i = 0; i < rows.length; i++) {
        final row = rows[i];
        if (row.schema != validated.schema) {
          throw OversqliteProtocolException(
            'committed row schema ${row.schema} does not match ${validated.schema}',
          );
        }
        await _applyCommittedReplayRow(uploaded: snapshot.rows[i], row: row);
        updatedTables.add(row.table.toLowerCase());
      }
      await _sourceStore.advanceAfterCommittedPush(
        sourceId: sourceId,
        sourceBundleId: committed.sourceBundleId,
      );
      await _attachmentStore.advanceAfterCommittedPush(committed.bundleSeq);
      await _clearOutbox();
    });
    return updatedTables;
  }

  Future<void> _assertCommittedRowsMatchOutbox(
    _PushSnapshot snapshot,
    _CommittedPush committed,
    List<BundleRow> committedRows,
  ) async {
    final requirePayloadMatch =
        snapshot.state == _outboxStateCommittedRemote ||
        committed.requiresStrictOutboxMatch;
    int? mismatchIndex;
    if (committedRows.length != snapshot.rows.length) {
      mismatchIndex = committedRows.length < snapshot.rows.length
          ? committedRows.length
          : snapshot.rows.length;
    } else {
      for (var i = 0; i < committedRows.length; i++) {
        if (!await _equivalentCommittedRow(
          expected: snapshot.rows[i],
          actual: committedRows[i],
          actualRowOrdinal: i,
          requirePayloadMatch: requirePayloadMatch,
        )) {
          mismatchIndex = i;
          break;
        }
      }
    }
    if (mismatchIndex != null) {
      final expected = mismatchIndex < snapshot.rows.length
          ? _ComparableCommittedRow.fromOutbox(snapshot.rows[mismatchIndex])
          : null;
      final actual = mismatchIndex < committedRows.length
          ? _ComparableCommittedRow.fromBundleRow(
              committedRows[mismatchIndex],
              mismatchIndex,
            )
          : null;
      throw SourceSequenceMismatchException(
        'committed bundle for source ${committed.sourceId} source_bundle_id ${committed.sourceBundleId} '
        'does not match the canonical prepared outbox rows at index $mismatchIndex; '
        'expected=$expected actual=$actual',
      );
    }
  }

  Future<bool> _equivalentCommittedRow({
    required _OutboxRow expected,
    required BundleRow actual,
    required int actualRowOrdinal,
    required bool requirePayloadMatch,
  }) async {
    if (expected.rowOrdinal != actualRowOrdinal ||
        expected.schemaName != actual.schema ||
        expected.tableName != actual.table ||
        !_syncKeysEqual(_syncKeyFromJson(expected.wireKeyJson), actual.key) ||
        expected.op != actual.op) {
      return false;
    }
    if (!requirePayloadMatch) {
      return true;
    }
    return _equivalentCommittedPayload(
      tableName: expected.tableName,
      expectedPayload: expected.wirePayload,
      actualPayload: actual.payload,
    );
  }

  Future<bool> _equivalentCommittedPayload({
    required String tableName,
    required String? expectedPayload,
    required Object? actualPayload,
  }) async {
    if (expectedPayload == null && actualPayload == null) {
      return true;
    }
    if (expectedPayload == null || actualPayload == null) {
      return false;
    }
    final expectedObject = jsonDecode(expectedPayload);
    if (expectedObject is! Map || actualPayload is! Map) {
      return false;
    }
    final table = await _tableInfo(tableName);
    return _equivalentPayloadObjects(
      table,
      expectedObject.cast<String, Object?>(),
      actualPayload.cast<String, Object?>(),
      PayloadSource.authoritativeWire,
      PayloadSource.authoritativeWire,
      allowRfc3339InstantEquivalence: true,
    );
  }

  Future<void> _resolveConflict({
    required OversqliteValidatedConfig validated,
    required _PushSnapshot snapshot,
    required PushConflictDetails conflict,
  }) async {
    final conflictingRow = snapshot.rows.firstWhere(
      (row) =>
          row.schemaName == conflict.schema &&
          row.tableName == conflict.table &&
          _syncKeysEqual(_syncKeyFromJson(row.wireKeyJson), conflict.key),
    );
    final conflictingTable = await _tableInfo(conflictingRow.tableName);
    final conflictingLocalPk = _localPkFromKeyJson(
      conflictingTable,
      conflictingRow.keyJson,
    );
    final context = ConflictContext(
      schema: conflict.schema,
      table: conflict.table,
      key: conflict.key,
      localOp: conflictingRow.op,
      localPayload: conflictingRow.localPayload == null
          ? null
          : (jsonDecode(conflictingRow.localPayload!) as Map)
                .cast<String, Object?>(),
      baseRowVersion: conflict.baseRowVersion,
      serverRowVersion: conflict.serverRowVersion,
      serverRowDeleted: conflict.serverRowDeleted,
      serverRow: conflict.serverRow is Map
          ? (conflict.serverRow as Map).cast<String, Object?>()
          : null,
    );
    final resolution = _resolver.resolve(context);
    final expectedColumns = {
      for (final column in conflictingTable.columns) column.name.toLowerCase(),
    };
    final invalidReason = _invalidConflictResolutionReason(
      context: context,
      resolution: resolution,
      expectedColumns: expectedColumns,
    );
    if (invalidReason != null) {
      await _restoreSnapshotToDirtyRows(snapshot);
      throw InvalidConflictResolutionException(invalidReason);
    }
    await _applyRunner.inApplyModeTransaction(() async {
      if (resolution is AcceptServer) {
        if (conflict.serverRowDeleted || context.serverRow == null) {
          await _deleteLocalRow(conflict.table, conflictingLocalPk);
          await _updateRowState(
            schema: validated.schema,
            table: conflict.table,
            keyJson: conflictingRow.keyJson,
            rowVersion: conflict.serverRowVersion,
            deleted: true,
          );
        } else {
          await _upsertPayload(
            conflict.table,
            context.serverRow!,
            PayloadSource.authoritativeWire,
          );
          await _updateRowState(
            schema: validated.schema,
            table: conflict.table,
            keyJson: conflictingRow.keyJson,
            rowVersion: conflict.serverRowVersion,
            deleted: false,
          );
        }
      } else if (resolution is KeepLocal) {
        await _requeueDirty(
          schema: conflictingRow.schemaName,
          table: conflictingRow.tableName,
          keyJson: conflictingRow.keyJson,
          op:
              conflictingRow.op == 'INSERT' &&
                  context.serverRow != null &&
                  !context.serverRowDeleted
              ? 'UPDATE'
              : conflictingRow.op,
          baseRowVersion: conflict.serverRowVersion,
          payload: conflictingRow.localPayload,
        );
      } else if (resolution is KeepMerged) {
        await _upsertPayload(
          conflict.table,
          resolution.mergedPayload,
          PayloadSource.localState,
        );
        final payload = await _serializeExistingRow(
          conflict.table,
          conflictingLocalPk,
        );
        await _requeueDirty(
          schema: conflictingRow.schemaName,
          table: conflictingRow.tableName,
          keyJson: conflictingRow.keyJson,
          op: 'UPDATE',
          baseRowVersion: conflict.serverRowVersion,
          payload: payload,
        );
      } else {
        throw const InvalidConflictResolutionException(
          'unsupported conflict resolution result',
        );
      }
      for (final row in snapshot.rows) {
        if (identical(row, conflictingRow)) {
          continue;
        }
        await _requeueDirty(
          schema: row.schemaName,
          table: row.tableName,
          keyJson: row.keyJson,
          op: row.op,
          baseRowVersion: row.baseRowVersion,
          payload: row.localPayload,
        );
      }
      await _clearOutbox();
    });
  }

  Future<void> _applyCommittedReplayRow({
    required _OutboxRow uploaded,
    required BundleRow row,
  }) async {
    final key = await _localKeyFromWire(row.table, row.key);
    final table = await _tableInfo(row.table);
    final pendingDirty = await _loadDirtyUploadState(
      schema: row.schema,
      table: row.table,
      keyJson: key.keyJson,
    );
    final livePayload = await _serializeExistingRow(row.table, key.localPk);
    final action = _planCommittedReplay(
      table: table,
      uploaded: uploaded,
      pendingDirty: pendingDirty,
      livePayload: livePayload,
    );
    switch (action) {
      case _ReplayAcceptAuthoritative():
        await _applyAuthoritativePayload(row, key.localPk, key.keyJson);
        await _deleteDirty(
          schema: row.schema,
          table: row.table,
          keyJson: key.keyJson,
        );
      case _ReplayPreserveLocal(:final op, :final payload):
        await _updateRowState(
          schema: row.schema,
          table: row.table,
          keyJson: key.keyJson,
          rowVersion: row.rowVersion,
          deleted: row.op == 'DELETE',
        );
        await _requeueDirty(
          schema: row.schema,
          table: row.table,
          keyJson: key.keyJson,
          op: op,
          baseRowVersion: row.rowVersion,
          payload: payload,
        );
    }
  }

  _ReplayAction _planCommittedReplay({
    required _TableInfo table,
    required _OutboxRow uploaded,
    required _DirtyUploadState? pendingDirty,
    required String? livePayload,
  }) {
    final pendingMatches =
        pendingDirty != null &&
        pendingDirty.op == uploaded.op &&
        _equivalentLocalPayload(
          table,
          pendingDirty.payload,
          uploaded.localPayload,
        );
    final liveMatches = uploaded.op == 'DELETE'
        ? livePayload == null
        : livePayload != null &&
              _equivalentLocalPayload(
                table,
                livePayload,
                uploaded.localPayload,
              );

    if (uploaded.op == 'DELETE') {
      return livePayload == null
          ? const _ReplayAcceptAuthoritative()
          : _ReplayPreserveLocal(op: 'INSERT', payload: livePayload);
    }

    if (livePayload == null) {
      return const _ReplayPreserveLocal(op: 'DELETE', payload: null);
    }
    if (pendingDirty != null && !pendingMatches) {
      return _ReplayPreserveLocal(op: 'UPDATE', payload: livePayload);
    }
    if (!liveMatches) {
      return _ReplayPreserveLocal(op: 'UPDATE', payload: livePayload);
    }
    return const _ReplayAcceptAuthoritative();
  }

  Future<void> _applyAuthoritativePayload(
    BundleRow row,
    String localPk,
    String keyJson,
  ) async {
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
        'committed ${row.op} row for ${row.table} must include object payload',
      );
    }
    await _upsertPayload(
      row.table,
      payload.cast<String, Object?>(),
      PayloadSource.authoritativeWire,
    );
    await _updateRowState(
      schema: row.schema,
      table: row.table,
      keyJson: keyJson,
      rowVersion: row.rowVersion,
      deleted: false,
    );
  }

  Future<List<_DirtyCapture>> _loadDirtyRows(
    OversqliteValidatedConfig validated,
  ) async {
    final tableOrder = validated.tableOrder;
    final rows = await _connection.select(
      '''SELECT d.schema_name, d.table_name, d.key_json, d.base_row_version, d.payload,
       d.dirty_ordinal,
       CASE WHEN rs.key_json IS NULL THEN 0 ELSE 1 END,
       COALESCE(rs.deleted, 0)
FROM _sync_dirty_rows d
LEFT JOIN _sync_row_state rs
  ON rs.schema_name = d.schema_name
 AND rs.table_name = d.table_name
 AND rs.key_json = d.key_json
ORDER BY d.dirty_ordinal, d.table_name, d.key_json''',
      (row) => _DirtySnapshot(
        schemaName: row.readString(0),
        tableName: row.readString(1),
        keyJson: row.readString(2),
        baseRowVersion: row.readInt(3),
        payload: row.readNullableString(4),
        dirtyOrdinal: row.readInt(5),
        stateExists: row.readInt(6) == 1,
        stateDeleted: row.readInt(7) == 1,
      ),
    );
    final captures = <_DirtyCapture>[];
    for (final row in rows) {
      final table = await _tableInfo(row.tableName);
      final localPk = _localPkFromKeyJson(table, row.keyJson);
      if (row.payload == null && (!row.stateExists || row.stateDeleted)) {
        continue;
      }
      final op = row.payload == null
          ? 'DELETE'
          : row.stateExists && !row.stateDeleted
          ? 'UPDATE'
          : 'INSERT';
      captures.add(
        _DirtyCapture(
          schemaName: row.schemaName,
          tableName: row.tableName,
          keyJson: row.keyJson,
          localPk: localPk,
          op: op,
          baseRowVersion: row.baseRowVersion,
          localPayload: row.payload,
          dirtyOrdinal: row.dirtyOrdinal,
        ),
      );
    }
    captures.sort((left, right) {
      final leftDelete = left.op == 'DELETE';
      final rightDelete = right.op == 'DELETE';
      if (leftDelete != rightDelete) {
        return leftDelete ? 1 : -1;
      }
      final leftOrder = tableOrder[left.tableName] ?? 1 << 30;
      final rightOrder = tableOrder[right.tableName] ?? 1 << 30;
      if (leftOrder != rightOrder) {
        return leftDelete ? rightOrder - leftOrder : leftOrder - rightOrder;
      }
      return left.dirtyOrdinal.compareTo(right.dirtyOrdinal);
    });
    return captures;
  }

  Future<_TableInfo> _tableInfo(String tableName) async {
    final rawColumns = await _connection.select(
      'PRAGMA table_info(${_quoteIdent(tableName)})',
      (row) => _RawColumnInfo(
        name: row.readString(1),
        declaredType: row.readString(2),
        primaryKey: row.readInt(5) > 0,
      ),
    );
    final foreignKeyColumnsLower = await _connection.select(
      'PRAGMA foreign_key_list(${_quoteIdent(tableName)})',
      (row) => row.readString(3).toLowerCase(),
    );
    final foreignKeyColumnSet = foreignKeyColumnsLower.toSet();
    final rows = [
      for (final column in rawColumns)
        _ColumnInfo(
          name: column.name,
          kind: _columnKind(
            column.declaredType,
            primaryKey: column.primaryKey,
            blobReference: foreignKeyColumnSet.contains(
              column.name.toLowerCase(),
            ),
          ),
          primaryKey: column.primaryKey,
        ),
    ];
    final pk = rows.singleWhere((column) => column.primaryKey);
    return _TableInfo(name: tableName, columns: rows, primaryKey: pk);
  }

  Future<String?> _serializeExistingRow(
    String tableName,
    String localPk,
  ) async {
    final table = await _tableInfo(tableName);
    final rows = await _connection.select(
      'SELECT ${table.columns.map((column) => _quoteIdent(column.name)).join(', ')} FROM ${_quoteIdent(tableName)} WHERE ${_quoteIdent(table.primaryKey.name)} = ?',
      (row) => {
        for (var i = 0; i < table.columns.length; i++)
          table.columns[i].name.toLowerCase(): _localJsonValue(
            table.columns[i],
            row.readValue(i),
          ),
      },
      parameters: [_bindPrimaryKey(table, localPk)],
    );
    if (rows.isEmpty) {
      return null;
    }
    return jsonEncode(rows.single);
  }

  Future<void> _upsertPayload(
    String tableName,
    Map<String, Object?> payload,
    PayloadSource source,
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
          _bindPayloadValue(
            column,
            normalized[column.name.toLowerCase()],
            source,
          ),
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

  Future<void> _requeueDirty({
    required String schema,
    required String table,
    required String keyJson,
    required String op,
    required int baseRowVersion,
    required String? payload,
  }) async {
    await _connection.execute(
      '''INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal, updated_at)
VALUES (
  ?, ?, ?, ?, ?, ?,
  COALESCE(
    (SELECT dirty_ordinal FROM _sync_dirty_rows WHERE schema_name = ? AND table_name = ? AND key_json = ?),
    (SELECT COALESCE(MAX(dirty_ordinal), 0) + 1 FROM _sync_dirty_rows)
  ),
  strftime('%Y-%m-%dT%H:%M:%fZ','now')
)
ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
  op = excluded.op,
  base_row_version = excluded.base_row_version,
  payload = excluded.payload,
  updated_at = excluded.updated_at''',
      parameters: [
        schema,
        table,
        keyJson,
        op,
        baseRowVersion,
        payload,
        schema,
        table,
        keyJson,
      ],
    );
  }

  Future<void> _deleteDirty({
    required String schema,
    required String table,
    required String keyJson,
  }) {
    return _connection.execute(
      'DELETE FROM _sync_dirty_rows WHERE schema_name = ? AND table_name = ? AND key_json = ?',
      parameters: [schema, table, keyJson],
    );
  }

  Future<int> _nextSourceBundleId(String sourceId) async {
    final rows = await _connection.select(
      'SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = ?',
      (row) => row.readInt(0),
      parameters: [sourceId],
    );
    return rows.single;
  }

  Future<_OutboxBundle> _loadOutboxBundle() async {
    final rows = await _connection.select(
      '''SELECT state, source_id, source_bundle_id, initialization_id,
       canonical_request_hash, row_count, remote_bundle_hash, remote_bundle_seq
FROM _sync_outbox_bundle
WHERE singleton_key = 1''',
      (row) => _OutboxBundle(
        state: row.readString(0),
        sourceId: row.readString(1),
        sourceBundleId: row.readInt(2),
        initializationId: row.readString(3),
        canonicalRequestHash: row.readString(4),
        rowCount: row.readInt(5),
        remoteBundleHash: row.readString(6),
        remoteBundleSeq: row.readInt(7),
      ),
    );
    return rows.single;
  }

  Future<_PushSnapshot> _loadPersistedSnapshot(_OutboxBundle outbox) async {
    if (outbox.state == _outboxStateCommittedRemote) {
      if (outbox.remoteBundleSeq <= 0) {
        throw StateError(
          'committed_remote outbox is missing remote_bundle_seq',
        );
      }
      if (outbox.remoteBundleHash.trim().isEmpty) {
        throw StateError(
          'committed_remote outbox is missing remote_bundle_hash',
        );
      }
    }
    final rows = await _connection.select(
      '''SELECT source_bundle_id, row_ordinal, schema_name, table_name, key_json,
       wire_key_json, op, base_row_version, local_payload, wire_payload
FROM _sync_outbox_rows
ORDER BY source_bundle_id, row_ordinal''',
      (row) => _OutboxRow(
        sourceBundleId: row.readInt(0),
        rowOrdinal: row.readInt(1),
        schemaName: row.readString(2),
        tableName: row.readString(3),
        keyJson: row.readString(4),
        wireKeyJson: row.readString(5),
        op: row.readString(6),
        baseRowVersion: row.readInt(7),
        localPayload: row.readNullableString(8),
        wirePayload: row.readNullableString(9),
      ),
    );
    if (rows.isEmpty) {
      throw StateError(
        '_sync_outbox_bundle state ${outbox.state} exists without rows',
      );
    }
    if (rows.any((row) => row.sourceBundleId != outbox.sourceBundleId)) {
      throw StateError(
        '_sync_outbox_rows source_bundle_id does not match _sync_outbox_bundle',
      );
    }
    if (rows.length != outbox.rowCount) {
      throw StateError(
        '_sync_outbox_rows row count ${rows.length} does not match _sync_outbox_bundle ${outbox.rowCount}',
      );
    }
    final canonicalRequestHash = _computeCanonicalRequestHash(rows);
    if (canonicalRequestHash != outbox.canonicalRequestHash) {
      throw StateError(
        'persisted canonical outbox hash $canonicalRequestHash does not match expected ${outbox.canonicalRequestHash}',
      );
    }
    return _PushSnapshot(
      state: outbox.state,
      sourceId: outbox.sourceId,
      sourceBundleId: outbox.sourceBundleId,
      initializationId: outbox.initializationId,
      canonicalRequestHash: canonicalRequestHash,
      remoteBundleHash: outbox.remoteBundleHash,
      remoteBundleSeq: outbox.remoteBundleSeq,
      rows: rows,
    );
  }

  Future<void> _persistOutboxBundle(_OutboxBundle bundle) {
    return _connection.execute(
      '''UPDATE _sync_outbox_bundle
SET state = ?,
    source_id = ?,
    source_bundle_id = ?,
    initialization_id = ?,
    canonical_request_hash = ?,
    row_count = ?,
    remote_bundle_hash = ?,
    remote_bundle_seq = ?
WHERE singleton_key = 1''',
      parameters: [
        bundle.state,
        bundle.sourceId,
        bundle.sourceBundleId,
        bundle.initializationId,
        bundle.canonicalRequestHash,
        bundle.rowCount,
        bundle.remoteBundleHash,
        bundle.remoteBundleSeq,
      ],
    );
  }

  Future<void> _persistCommittedRemote(
    _PushSnapshot snapshot,
    _CommittedPush committed,
  ) {
    return _persistOutboxBundle(
      _OutboxBundle(
        state: _outboxStateCommittedRemote,
        sourceId: committed.sourceId,
        sourceBundleId: snapshot.sourceBundleId,
        initializationId: snapshot.initializationId,
        canonicalRequestHash: snapshot.canonicalRequestHash,
        rowCount: snapshot.rows.length,
        remoteBundleHash: committed.bundleHash,
        remoteBundleSeq: committed.bundleSeq,
      ),
    );
  }

  Future<void> _insertOutboxRow(_OutboxRow row) {
    return _connection.execute(
      '''INSERT INTO _sync_outbox_rows(
  source_bundle_id, row_ordinal, schema_name, table_name, key_json,
  wire_key_json, op, base_row_version, local_payload, wire_payload
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
      parameters: [
        row.sourceBundleId,
        row.rowOrdinal,
        row.schemaName,
        row.tableName,
        row.keyJson,
        row.wireKeyJson,
        row.op,
        row.baseRowVersion,
        row.localPayload,
        row.wirePayload,
      ],
    );
  }

  Future<void> _clearOutbox() async {
    await _connection.execute('DELETE FROM _sync_outbox_rows');
    await _persistOutboxBundle(const _OutboxBundle());
  }

  Future<_DirtyUploadState?> _loadDirtyUploadState({
    required String schema,
    required String table,
    required String keyJson,
  }) async {
    final rows = await _connection.select(
      '''SELECT op, base_row_version, payload
FROM _sync_dirty_rows
WHERE schema_name = ? AND table_name = ? AND key_json = ?''',
      (row) => _DirtyUploadState(
        op: row.readString(0),
        baseRowVersion: row.readInt(1),
        payload: row.readNullableString(2),
      ),
      parameters: [schema, table, keyJson],
    );
    return rows.isEmpty ? null : rows.single;
  }

  Future<void> _restoreSnapshotToDirtyRows(_PushSnapshot snapshot) async {
    await _applyRunner.inApplyModeTransaction(() async {
      for (final row in snapshot.rows) {
        await _requeueDirty(
          schema: row.schemaName,
          table: row.tableName,
          keyJson: row.keyJson,
          op: row.op,
          baseRowVersion: row.baseRowVersion,
          payload: row.localPayload,
        );
      }
      await _clearOutbox();
    });
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
    return (
      keyJson: _canonicalizeProtocolJson({keyColumn: localPk}),
      localPk: localPk,
    );
  }

  Future<int> _scalarInt(String sql) async {
    final rows = await _connection.select(sql, (row) => row.readInt(0));
    return rows.single;
  }
}

enum PayloadSource { localState, authoritativeWire }

const _outboxStateNone = 'none';
const _outboxStatePrepared = 'prepared';
const _outboxStateCommittedRemote = 'committed_remote';
const _maxCommittedBundleFetchAttempts = 6;
const _initialCommittedBundleFetchBackoff = Duration(milliseconds: 75);
const _maxCommittedBundleFetchBackoff = Duration(milliseconds: 600);
final _jsonNumberPattern = RegExp(r'^-?(0|[1-9]\d*)(\.\d+)?([eE][+-]?\d+)?$');
final _rfc3339TimestampPattern = RegExp(
  r'^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$',
);

final class _PushSnapshot {
  const _PushSnapshot({
    required this.state,
    required this.sourceId,
    required this.sourceBundleId,
    required this.initializationId,
    required this.canonicalRequestHash,
    required this.remoteBundleHash,
    required this.remoteBundleSeq,
    required this.rows,
  });

  final String state;
  final String sourceId;
  final int sourceBundleId;
  final String initializationId;
  final String canonicalRequestHash;
  final String remoteBundleHash;
  final int remoteBundleSeq;
  final List<_OutboxRow> rows;
}

final class _CommittedPush {
  const _CommittedPush({
    required this.bundleSeq,
    required this.sourceId,
    required this.sourceBundleId,
    required this.rowCount,
    required this.bundleHash,
    required this.requiresStrictOutboxMatch,
  });

  final int bundleSeq;
  final String sourceId;
  final int sourceBundleId;
  final int rowCount;
  final String bundleHash;
  final bool requiresStrictOutboxMatch;
}

final class _OutboxBundle {
  const _OutboxBundle({
    this.state = _outboxStateNone,
    this.sourceId = '',
    this.sourceBundleId = 0,
    this.initializationId = '',
    this.canonicalRequestHash = '',
    this.rowCount = 0,
    this.remoteBundleHash = '',
    this.remoteBundleSeq = 0,
  });

  final String state;
  final String sourceId;
  final int sourceBundleId;
  final String initializationId;
  final String canonicalRequestHash;
  final int rowCount;
  final String remoteBundleHash;
  final int remoteBundleSeq;
}

final class _OutboxRow {
  const _OutboxRow({
    required this.sourceBundleId,
    required this.rowOrdinal,
    required this.schemaName,
    required this.tableName,
    required this.keyJson,
    required this.wireKeyJson,
    required this.op,
    required this.baseRowVersion,
    required this.localPayload,
    required this.wirePayload,
  });

  final int sourceBundleId;
  final int rowOrdinal;
  final String schemaName;
  final String tableName;
  final String keyJson;
  final String wireKeyJson;
  final String op;
  final int baseRowVersion;
  final String? localPayload;
  final String? wirePayload;

  PushRequestRow toPushRequestRow() {
    return PushRequestRow(
      schema: schemaName,
      table: tableName,
      key: _syncKeyFromJson(wireKeyJson),
      op: op,
      baseRowVersion: baseRowVersion,
      payload: wirePayload == null ? null : jsonDecode(wirePayload!),
    );
  }

  Map<String, Object?> canonicalRequestJson() {
    final json = <String, Object?>{
      'row_ordinal': rowOrdinal,
      'schema': schemaName,
      'table': tableName,
      'key': _syncKeyFromJson(wireKeyJson),
      'op': op,
      'base_row_version': baseRowVersion,
    };
    if (wirePayload != null) {
      json['payload'] = jsonDecode(wirePayload!);
    }
    return json;
  }
}

final class _DirtySnapshot {
  const _DirtySnapshot({
    required this.schemaName,
    required this.tableName,
    required this.keyJson,
    required this.baseRowVersion,
    required this.payload,
    required this.dirtyOrdinal,
    required this.stateExists,
    required this.stateDeleted,
  });

  final String schemaName;
  final String tableName;
  final String keyJson;
  final int baseRowVersion;
  final String? payload;
  final int dirtyOrdinal;
  final bool stateExists;
  final bool stateDeleted;
}

final class _ComparableCommittedRow {
  const _ComparableCommittedRow({
    required this.rowOrdinal,
    required this.schemaName,
    required this.tableName,
    required this.wireKey,
    required this.op,
    required this.wirePayload,
  });

  factory _ComparableCommittedRow.fromOutbox(_OutboxRow row) {
    return _ComparableCommittedRow(
      rowOrdinal: row.rowOrdinal,
      schemaName: row.schemaName,
      tableName: row.tableName,
      wireKey: _syncKeyFromJson(row.wireKeyJson),
      op: row.op,
      wirePayload: row.wirePayload == null
          ? null
          : _canonicalizeProtocolJson(jsonDecode(row.wirePayload!)),
    );
  }

  factory _ComparableCommittedRow.fromBundleRow(BundleRow row, int rowOrdinal) {
    return _ComparableCommittedRow(
      rowOrdinal: rowOrdinal,
      schemaName: row.schema,
      tableName: row.table,
      wireKey: row.key,
      op: row.op,
      wirePayload: row.payload == null
          ? null
          : _canonicalizeProtocolJson(row.payload),
    );
  }

  final int rowOrdinal;
  final String schemaName;
  final String tableName;
  final SyncKey wireKey;
  final String op;
  final String? wirePayload;

  @override
  String toString() {
    return 'CanonicalOutboxComparableRow(rowOrdinal=$rowOrdinal, '
        'schemaName=$schemaName, tableName=$tableName, wireKey=$wireKey, '
        'op=$op, wirePayload=$wirePayload)';
  }
}

final class _DirtyCapture {
  const _DirtyCapture({
    required this.schemaName,
    required this.tableName,
    required this.keyJson,
    required this.localPk,
    required this.op,
    required this.baseRowVersion,
    required this.localPayload,
    required this.dirtyOrdinal,
  });

  final String schemaName;
  final String tableName;
  final String keyJson;
  final String localPk;
  final String op;
  final int baseRowVersion;
  final String? localPayload;
  final int dirtyOrdinal;
}

final class _DirtyUploadState {
  const _DirtyUploadState({
    required this.op,
    required this.baseRowVersion,
    required this.payload,
  });

  final String op;
  final int baseRowVersion;
  final String? payload;
}

sealed class _ReplayAction {
  const _ReplayAction();
}

final class _ReplayAcceptAuthoritative extends _ReplayAction {
  const _ReplayAcceptAuthoritative();
}

final class _ReplayPreserveLocal extends _ReplayAction {
  const _ReplayPreserveLocal({required this.op, required this.payload});

  final String op;
  final String? payload;
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

enum _ColumnKind { text, integer, real, blob, uuidBlob }

_ColumnKind _columnKind(
  String type, {
  required bool primaryKey,
  required bool blobReference,
}) {
  final upper = type.trim().toUpperCase();
  if (upper.contains('BLOB')) {
    return primaryKey || blobReference
        ? _ColumnKind.uuidBlob
        : _ColumnKind.blob;
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

String _wireKeyJson(_TableInfo table, String localPk) {
  return _canonicalizeProtocolJson({
    table.primaryKey.name
        .toLowerCase(): table.primaryKey.kind == _ColumnKind.uuidBlob
        ? _canonicalUuid(localPk)
        : localPk,
  });
}

String _localPkFromKeyJson(_TableInfo table, String keyJson) {
  final key = (jsonDecode(keyJson) as Map).cast<String, Object?>();
  return key[table.primaryKey.name.toLowerCase()].toString();
}

SyncKey _syncKeyFromJson(String jsonText) {
  final parsed = (jsonDecode(jsonText) as Map).cast<String, Object?>();
  return {
    for (final entry in parsed.entries) entry.key: entry.value.toString(),
  };
}

bool _syncKeysEqual(SyncKey left, SyncKey right) {
  if (left.length != right.length) {
    return false;
  }
  for (final entry in left.entries) {
    if (right[entry.key] != entry.value) {
      return false;
    }
  }
  return true;
}

String _computeCanonicalRequestHash(List<_OutboxRow> rows) {
  if (rows.isEmpty) {
    return '';
  }
  return _sha256Hex(
    _canonicalizeProtocolJson([
      for (final row in rows) row.canonicalRequestJson(),
    ]),
  );
}

String _computeCommittedBundleHash(List<BundleRow> rows) {
  return _sha256Hex(
    _canonicalizeProtocolJson([
      for (var i = 0; i < rows.length; i++)
        {
          'row_ordinal': i,
          'schema': rows[i].schema,
          'table': rows[i].table,
          'key': rows[i].key,
          'op': rows[i].op,
          'row_version': rows[i].rowVersion,
          'payload': rows[i].payload,
        },
    ]),
  );
}

String? _invalidConflictResolutionReason({
  required ConflictContext context,
  required MergeResult resolution,
  required Set<String> expectedColumns,
}) {
  if (resolution is! KeepMerged && resolution is! KeepLocal) {
    return null;
  }

  if (resolution is KeepLocal &&
      context.localOp == 'UPDATE' &&
      (context.serverRowDeleted || context.serverRow == null)) {
    return 'KeepLocal is invalid for stale UPDATE on ${context.schema}.${context.table}; authoritative row is deleted or missing';
  }

  if (resolution is! KeepMerged) {
    return null;
  }
  if (context.localOp == 'DELETE') {
    return 'KeepMerged is invalid for DELETE conflict on ${context.schema}.${context.table}';
  }
  if (context.localOp == 'UPDATE' &&
      (context.serverRowDeleted || context.serverRow == null)) {
    return 'KeepMerged is invalid for stale UPDATE on ${context.schema}.${context.table}; authoritative row is deleted or missing';
  }
  final payloadColumns = {
    for (final key in resolution.mergedPayload.keys) key.toLowerCase(),
  };
  if (payloadColumns.length != expectedColumns.length ||
      !payloadColumns.containsAll(expectedColumns)) {
    return 'KeepMerged for ${context.schema}.${context.table} must include exactly every table column';
  }
  return null;
}

void _validateCommittedPush(
  PushSessionCommitResponse response, {
  required String sourceId,
  required int sourceBundleId,
}) {
  if (response.bundleSeq <= 0) {
    throw const OversqliteProtocolException(
      'push commit response bundle_seq must be positive',
    );
  }
  if (response.sourceId != sourceId) {
    throw OversqliteProtocolException(
      'push commit response source_id ${response.sourceId} does not match client $sourceId',
    );
  }
  if (response.sourceBundleId != sourceBundleId) {
    throw OversqliteProtocolException(
      'push commit response source_bundle_id ${response.sourceBundleId} does not match requested $sourceBundleId',
    );
  }
  if (response.rowCount < 0) {
    throw const OversqliteProtocolException(
      'push commit response row_count must be non-negative',
    );
  }
  if (response.bundleHash.trim().isEmpty) {
    throw const OversqliteProtocolException(
      'push commit response bundle_hash must be non-empty',
    );
  }
}

void _validateCommittedChunk({
  required _CommittedPush committed,
  required CommittedBundleRowsResponse chunk,
  required int? afterRowOrdinal,
}) {
  if (chunk.bundleSeq != committed.bundleSeq) {
    throw OversqliteProtocolException(
      'committed bundle chunk response bundle_seq ${chunk.bundleSeq} does not match expected ${committed.bundleSeq}',
    );
  }
  if (chunk.sourceId != committed.sourceId) {
    throw OversqliteProtocolException(
      'committed bundle chunk response source_id ${chunk.sourceId} does not match expected ${committed.sourceId}',
    );
  }
  if (chunk.sourceBundleId != committed.sourceBundleId) {
    throw OversqliteProtocolException(
      'committed bundle chunk response source_bundle_id ${chunk.sourceBundleId} does not match expected ${committed.sourceBundleId}',
    );
  }
  if (chunk.rowCount != committed.rowCount) {
    throw OversqliteProtocolException(
      'committed bundle chunk response row_count ${chunk.rowCount} does not match expected ${committed.rowCount}',
    );
  }
  if (chunk.bundleHash != committed.bundleHash) {
    throw OversqliteProtocolException(
      'committed bundle chunk response bundle_hash ${chunk.bundleHash} does not match expected ${committed.bundleHash}',
    );
  }
  final logicalAfter = afterRowOrdinal ?? -1;
  final expectedNext = chunk.rows.isEmpty
      ? logicalAfter
      : logicalAfter + chunk.rows.length;
  if (chunk.nextRowOrdinal != expectedNext) {
    throw OversqliteProtocolException(
      'committed bundle chunk response next_row_ordinal ${chunk.nextRowOrdinal} does not match expected $expectedNext',
    );
  }
  if (chunk.hasMore && chunk.rows.isEmpty) {
    throw const OversqliteProtocolException(
      'committed bundle chunk response with has_more=true must include at least one row',
    );
  }
}

Object? _localJsonValue(_ColumnInfo column, Object? value) {
  if (value == null) {
    return null;
  }
  if (value is Uint8List) {
    return _hex(value);
  }
  if (value is List<int>) {
    return _hex(Uint8List.fromList(value));
  }
  return value;
}

Map<String, Object?> _wirePayloadForUpload(
  _TableInfo table,
  Map<String, Object?> payload,
) {
  final normalized = {
    for (final entry in payload.entries) entry.key.toLowerCase(): entry.value,
  };
  return {
    for (final column in table.columns)
      column.name.toLowerCase(): _wirePayloadValue(
        column,
        normalized[column.name.toLowerCase()],
      ),
  };
}

Object? _wirePayloadValue(_ColumnInfo column, Object? value) {
  if (value == null) {
    return null;
  }
  return switch (column.kind) {
    _ColumnKind.blob => base64Encode(_decodeLocalBlob(value.toString())),
    _ColumnKind.uuidBlob => _canonicalUuid(value.toString()),
    _ColumnKind.integer =>
      value is bool
          ? (value ? 1 : 0)
          : value is int
          ? value
          : int.parse(value.toString()),
    _ColumnKind.real => num.parse(
      _canonicalizeFiniteJsonNumber(value.toString()),
    ),
    _ColumnKind.text => value.toString(),
  };
}

Object? _bindPayloadValue(
  _ColumnInfo column,
  Object? value,
  PayloadSource source,
) {
  if (value == null) {
    return null;
  }
  return switch (column.kind) {
    _ColumnKind.blob =>
      source == PayloadSource.authoritativeWire
          ? base64Decode(value.toString())
          : _decodeLocalBlob(value.toString()),
    _ColumnKind.uuidBlob =>
      source == PayloadSource.authoritativeWire
          ? _decodeWireUuid(value.toString())
          : _decodeLocalUuid(value.toString()),
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

bool _equivalentPayloadTexts(
  _TableInfo table,
  String? left,
  String? right,
  PayloadSource leftSource,
  PayloadSource rightSource,
) {
  if (left == null && right == null) {
    return true;
  }
  if (left == null || right == null) {
    return false;
  }
  final leftObject = jsonDecode(left);
  final rightObject = jsonDecode(right);
  if (leftObject is! Map || rightObject is! Map) {
    return false;
  }
  return _equivalentPayloadObjects(
    table,
    leftObject.cast<String, Object?>(),
    rightObject.cast<String, Object?>(),
    leftSource,
    rightSource,
  );
}

bool _equivalentLocalPayload(_TableInfo table, String? left, String? right) {
  return _equivalentPayloadTexts(
    table,
    left,
    right,
    PayloadSource.localState,
    PayloadSource.localState,
  );
}

bool _equivalentPayloadObjects(
  _TableInfo table,
  Map<String, Object?> left,
  Map<String, Object?> right,
  PayloadSource leftSource,
  PayloadSource rightSource, {
  bool allowRfc3339InstantEquivalence = false,
}) {
  if (left.length != table.columns.length ||
      right.length != table.columns.length) {
    return false;
  }
  for (final column in table.columns) {
    final leftLookup = _lookupPayloadValue(left, column);
    final rightLookup = _lookupPayloadValue(right, column);
    if (!leftLookup.found || !rightLookup.found) {
      return false;
    }
    final equivalent = _equivalentPayloadValue(
      column,
      leftLookup.value,
      rightLookup.value,
      leftSource,
      rightSource,
    );
    if (equivalent ||
        (allowRfc3339InstantEquivalence &&
            _rfc3339InstantEquivalent(leftLookup.value, rightLookup.value))) {
      continue;
    }
    return false;
  }
  return true;
}

({bool found, Object? value}) _lookupPayloadValue(
  Map<String, Object?> payload,
  _ColumnInfo column,
) {
  final lower = column.name.toLowerCase();
  if (payload.containsKey(lower)) {
    return (found: true, value: payload[lower]);
  }
  if (payload.containsKey(column.name)) {
    return (found: true, value: payload[column.name]);
  }
  return (found: false, value: null);
}

bool _equivalentPayloadValue(
  _ColumnInfo column,
  Object? left,
  Object? right,
  PayloadSource leftSource,
  PayloadSource rightSource,
) {
  if (left == null || right == null) {
    return left == right;
  }
  try {
    return switch (column.kind) {
      _ColumnKind.text => left.toString() == right.toString(),
      _ColumnKind.integer =>
        _decodeIntegerPayloadValue(left) == _decodeIntegerPayloadValue(right),
      _ColumnKind.real =>
        _canonicalizeFiniteJsonNumber(left.toString()) ==
            _canonicalizeFiniteJsonNumber(right.toString()),
      _ColumnKind.blob => _bytesEqual(
        _decodeBlobPayloadValue(left, leftSource),
        _decodeBlobPayloadValue(right, rightSource),
      ),
      _ColumnKind.uuidBlob => _bytesEqual(
        _decodeUuidPayloadValue(left, leftSource),
        _decodeUuidPayloadValue(right, rightSource),
      ),
    };
  } catch (_) {
    return false;
  }
}

int _decodeIntegerPayloadValue(Object value) {
  if (value is bool) {
    return value ? 1 : 0;
  }
  if (value is int) {
    return value;
  }
  return int.parse(value.toString());
}

Uint8List _decodeBlobPayloadValue(Object value, PayloadSource source) {
  return switch (source) {
    PayloadSource.localState => _decodeLocalBlob(value.toString()),
    PayloadSource.authoritativeWire => base64Decode(value.toString()),
  };
}

Uint8List _decodeUuidPayloadValue(Object value, PayloadSource source) {
  return switch (source) {
    PayloadSource.localState => _decodeLocalUuid(value.toString()),
    PayloadSource.authoritativeWire => _decodeWireUuid(value.toString()),
  };
}

bool _bytesEqual(Uint8List left, Uint8List right) {
  if (left.length != right.length) {
    return false;
  }
  for (var i = 0; i < left.length; i++) {
    if (left[i] != right[i]) {
      return false;
    }
  }
  return true;
}

bool _rfc3339InstantEquivalent(Object? left, Object? right) {
  if (left is! String || right is! String) {
    return false;
  }
  if (!_rfc3339TimestampPattern.hasMatch(left.trim()) ||
      !_rfc3339TimestampPattern.hasMatch(right.trim())) {
    return false;
  }
  try {
    return DateTime.parse(
      left,
    ).toUtc().isAtSameMomentAs(DateTime.parse(right).toUtc());
  } on FormatException {
    return false;
  }
}

Uint8List _decodeLocalBlob(String value) {
  final clean = value.trim().replaceFirst(
    RegExp(r'^\\x', caseSensitive: false),
    '',
  );
  if (clean.length.isEven && RegExp(r'^[0-9a-fA-F]*$').hasMatch(clean)) {
    return _decodeHex(clean);
  }
  return base64Decode(value);
}

Uint8List _decodeLocalUuid(String value) {
  final normalized = value.trim();
  if (RegExp(
    r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$',
  ).hasMatch(normalized)) {
    return _decodeHex(normalized.replaceAll('-', ''));
  }
  if (normalized.length == 32 &&
      RegExp(r'^[0-9a-fA-F]{32}$').hasMatch(normalized)) {
    return _decodeHex(normalized);
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

String _canonicalizeFiniteJsonNumber(String value) {
  if (!_jsonNumberPattern.hasMatch(value)) {
    throw ArgumentError('invalid JSON number: $value');
  }
  final negative = value.startsWith('-');
  final unsigned = negative ? value.substring(1) : value;
  final exponentIndex = unsigned.indexOf(RegExp('[eE]'));
  final mantissa = exponentIndex >= 0
      ? unsigned.substring(0, exponentIndex)
      : unsigned;
  final exponent = exponentIndex >= 0
      ? int.parse(unsigned.substring(exponentIndex + 1))
      : 0;
  final dotIndex = mantissa.indexOf('.');
  final integerPart = dotIndex >= 0
      ? mantissa.substring(0, dotIndex)
      : mantissa;
  final fractionPart = dotIndex >= 0 ? mantissa.substring(dotIndex + 1) : '';
  final rawDigits = integerPart + fractionPart;
  var leadingZeroCount = 0;
  while (leadingZeroCount < rawDigits.length &&
      rawDigits.codeUnitAt(leadingZeroCount) == 0x30) {
    leadingZeroCount++;
  }
  final digits = rawDigits.substring(leadingZeroCount);
  if (digits.isEmpty) {
    return '0';
  }
  final shiftedDecimalIndex = integerPart.length + exponent - leadingZeroCount;
  final plain = shiftedDecimalIndex <= 0
      ? '0.${_zeroes(-shiftedDecimalIndex)}$digits'
      : shiftedDecimalIndex >= digits.length
      ? '$digits${_zeroes(shiftedDecimalIndex - digits.length)}'
      : '${digits.substring(0, shiftedDecimalIndex)}.${digits.substring(shiftedDecimalIndex)}';
  final normalized = plain.contains('.')
      ? _trimDecimalNumber(plain)
      : plain.replaceFirst(RegExp(r'^0+'), '').isEmpty
      ? '0'
      : plain.replaceFirst(RegExp(r'^0+'), '');
  return negative && normalized != '0' ? '-$normalized' : normalized;
}

String _trimDecimalNumber(String plain) {
  final parts = plain.split('.');
  final integerPart = parts[0].replaceFirst(RegExp(r'^0+'), '');
  final fractionPart = parts[1].replaceFirst(RegExp(r'0+$'), '');
  final normalizedInteger = integerPart.isEmpty ? '0' : integerPart;
  if (fractionPart.isEmpty) {
    return normalizedInteger;
  }
  return '$normalizedInteger.$fractionPart';
}

String _zeroes(int count) => List.filled(count, '0').join();

String _canonicalizeProtocolJson(Object? value) {
  if (value == null) {
    return 'null';
  }
  if (value is String || value is bool) {
    return jsonEncode(value);
  }
  if (value is num) {
    return _canonicalizeProtocolJsonNumber(value.toString());
  }
  if (value is List) {
    return '[${value.map(_canonicalizeProtocolJson).join(',')}]';
  }
  if (value is Map) {
    final entries = value.entries.toList()
      ..sort(
        (left, right) => left.key.toString().compareTo(right.key.toString()),
      );
    return '{${entries.map((entry) => '${jsonEncode(entry.key.toString())}:${_canonicalizeProtocolJson(entry.value)}').join(',')}}';
  }
  return jsonEncode(value);
}

String _canonicalizeProtocolJsonNumber(String raw) {
  final number = double.parse(raw);
  if (!number.isFinite) {
    throw ArgumentError('JSON number must be finite: $raw');
  }
  if (number == 0) {
    return '0';
  }

  final absNumber = number.abs();
  final rendered = number.toString().toLowerCase();
  if (absNumber >= 1e-6 && absNumber < 1e21) {
    return _normalizeProtocolPlainNumber(_protocolScientificToPlain(rendered));
  }
  return _normalizeProtocolScientificNumber(rendered);
}

String _protocolScientificToPlain(String raw) {
  if (!raw.contains('e')) {
    return raw;
  }
  final sign = raw.startsWith('-') ? '-' : '';
  final unsigned = sign.isEmpty ? raw : raw.substring(1);
  final parts = unsigned.split('e');
  if (parts.length != 2) {
    throw ArgumentError('invalid scientific notation: $raw');
  }

  final mantissa = parts[0];
  final exponent = int.parse(parts[1]);
  final dotIndex = mantissa.indexOf('.');
  final digits = mantissa.replaceAll('.', '');
  final fractionalDigits = dotIndex >= 0 ? mantissa.length - dotIndex - 1 : 0;
  final decimalIndex = digits.length + exponent - fractionalDigits;

  if (decimalIndex <= 0) {
    return '${sign}0.${_zeroes(-decimalIndex)}$digits';
  }
  if (decimalIndex >= digits.length) {
    return '$sign$digits${_zeroes(decimalIndex - digits.length)}';
  }
  return '$sign${digits.substring(0, decimalIndex)}.'
      '${digits.substring(decimalIndex)}';
}

String _normalizeProtocolPlainNumber(String raw) {
  final sign = raw.startsWith('-') ? '-' : '';
  final unsigned = sign.isEmpty ? raw : raw.substring(1);
  final dotIndex = unsigned.indexOf('.');
  final integerRaw = dotIndex >= 0 ? unsigned.substring(0, dotIndex) : unsigned;
  final fractionRaw = dotIndex >= 0 ? unsigned.substring(dotIndex + 1) : '';
  final integerPart = integerRaw.replaceFirst(RegExp(r'^0+'), '');
  final normalizedInteger = integerPart.isEmpty ? '0' : integerPart;
  final fractionalPart = fractionRaw.replaceFirst(RegExp(r'0+$'), '');
  if (normalizedInteger == '0' && fractionalPart.isEmpty) {
    return '0';
  }
  if (fractionalPart.isEmpty) {
    return '$sign$normalizedInteger';
  }
  return '$sign$normalizedInteger.$fractionalPart';
}

String _normalizeProtocolScientificNumber(String raw) {
  if (!raw.contains('e')) {
    return _protocolPlainToScientific(_normalizeProtocolPlainNumber(raw));
  }
  final sign = raw.startsWith('-') ? '-' : '';
  final unsigned = sign.isEmpty ? raw : raw.substring(1);
  final parts = unsigned.split('e');
  if (parts.length != 2) {
    throw ArgumentError('invalid scientific notation: $raw');
  }

  final mantissa = _normalizeProtocolPlainNumber(parts[0]);
  final exponent = int.parse(parts[1]);
  return '$sign$mantissa'
      'e${exponent >= 0 ? '+' : ''}$exponent';
}

String _protocolPlainToScientific(String raw) {
  final sign = raw.startsWith('-') ? '-' : '';
  final unsigned = sign.isEmpty ? raw : raw.substring(1);
  final dotIndex = unsigned.indexOf('.');
  final integerPart = dotIndex >= 0
      ? unsigned.substring(0, dotIndex)
      : unsigned;
  final fractionalPart = dotIndex >= 0 ? unsigned.substring(dotIndex + 1) : '';

  final firstIntegerNonZero = _firstNonZeroIndex(integerPart);
  if (firstIntegerNonZero >= 0) {
    final digits = (integerPart + fractionalPart).replaceFirst(
      RegExp(r'^0+'),
      '',
    );
    final exponent = integerPart.length - 1;
    return '$sign${_protocolScientificMantissa(digits)}'
        'e+$exponent';
  }

  final firstFractionNonZero = _firstNonZeroIndex(fractionalPart);
  if (firstFractionNonZero < 0) {
    throw ArgumentError('plainToScientific requires a non-zero number: $raw');
  }
  final digits = fractionalPart.substring(firstFractionNonZero);
  final exponent = -(firstFractionNonZero + 1);
  return '$sign${_protocolScientificMantissa(digits)}'
      'e$exponent';
}

int _firstNonZeroIndex(String value) {
  for (var i = 0; i < value.length; i++) {
    if (value.codeUnitAt(i) != 0x30) {
      return i;
    }
  }
  return -1;
}

String _protocolScientificMantissa(String digits) {
  final head = digits.substring(0, 1);
  final tail = digits.substring(1);
  return tail.isEmpty ? head : '$head.$tail';
}

String _sha256Hex(String text) {
  return sha256.convert(utf8.encode(text)).bytes.map((byte) {
    return byte.toRadixString(16).padLeft(2, '0');
  }).join();
}

String _quoteIdent(String identifier) =>
    '"${identifier.replaceAll('"', '""')}"';

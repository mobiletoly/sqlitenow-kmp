import 'dart:convert';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';

import 'config.dart';
import 'local_runtime.dart';
import 'local_row_store.dart';
import 'payload_codec.dart';
import 'payload_source.dart';
import 'protocol.dart';
import 'push_conflict.dart';
import 'push_replay.dart';
import 'push_state_store.dart';
import 'resolver.dart';
import 'runtime_state_store.dart';

export 'payload_source.dart';

enum PushOutcome { noChange, committed }

const _maxCommittedBundleFetchAttempts = 6;
const _initialCommittedBundleFetchBackoff = Duration(milliseconds: 75);
const _maxCommittedBundleFetchBackoff = Duration(milliseconds: 600);

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
  OversqliteLocalRowStore get _localStore =>
      OversqliteLocalRowStore(_connection);
  OversqlitePushStateStore get _pushStateStore => OversqlitePushStateStore(
    connection: _connection,
    applyRunner: _applyRunner,
  );
  OversqlitePushReplayExecutor get _replayExecutor =>
      OversqlitePushReplayExecutor(
        applyRunner: _applyRunner,
        attachmentStore: _attachmentStore,
        sourceStore: _sourceStore,
        localStore: _localStore,
        pushStateStore: _pushStateStore,
      );
  OversqliteConflictExecutor get _conflictExecutor =>
      OversqliteConflictExecutor(
        resolver: _resolver,
        applyRunner: _applyRunner,
        localStore: _localStore,
        pushStateStore: _pushStateStore,
      );
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

    late final OversqliteCommittedPush committed;
    try {
      committed = snapshot.state == pushOutboxStateCommittedRemote
          ? OversqliteCommittedPush(
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
      await _conflictExecutor.resolveConflict(
        validated: validated,
        snapshot: snapshot,
        conflict: error.conflict,
      );
      final remainingDirtyCount = await _pushStateStore.scalarInt(
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
    final updatedTables = await _replayExecutor.applyCommittedRows(
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

  Future<OversqlitePushSnapshot> _ensureSnapshot({
    required OversqliteValidatedConfig validated,
    required String sourceId,
    required String pendingInitializationId,
  }) async {
    final outbox = await _pushStateStore.loadOutboxBundle();
    if (outbox.state != pushOutboxStateNone) {
      return _pushStateStore.loadPersistedSnapshot(outbox);
    }

    final sourceBundleId = await _pushStateStore.nextSourceBundleId(sourceId);
    final dirtyRows = await _pushStateStore.loadDirtyRows(validated);
    final canonicalRows = <OversqliteOutboxRow>[];
    for (final row in dirtyRows) {
      final table = await _localStore.tableInfo(row.tableName);
      final wireKey = wireKeyJsonFromOversqliteLocalKey(table, row.localPk);
      final wirePayload = row.op == 'DELETE'
          ? null
          : canonicalizeOversqliteProtocolJson(
              wireOversqlitePayloadForUpload(
                table,
                jsonDecode(row.localPayload!),
              ),
            );
      canonicalRows.add(
        OversqliteOutboxRow(
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
        : sha256Hex(
            canonicalizeOversqliteProtocolJson([
              for (final row in canonicalRows) row.canonicalRequestJson(),
            ]),
          );
    await _connection.transaction(() async {
      await _pushStateStore.clearOutbox();
      if (canonicalRows.isNotEmpty) {
        await _pushStateStore.persistOutboxBundle(
          OversqliteOutboxBundle(
            state: pushOutboxStatePrepared,
            sourceId: sourceId,
            sourceBundleId: sourceBundleId,
            initializationId: pendingInitializationId,
            canonicalRequestHash: canonicalHash,
            rowCount: canonicalRows.length,
          ),
        );
        for (final row in canonicalRows) {
          await _pushStateStore.insertOutboxRow(row);
        }
      }
      for (final row in dirtyRows) {
        await _connection.execute(
          'DELETE FROM _sync_dirty_rows WHERE schema_name = ? AND table_name = ? AND key_json = ?',
          parameters: [row.schemaName, row.tableName, row.keyJson],
        );
      }
    }, mode: TransactionMode.immediate);

    return OversqlitePushSnapshot(
      state: canonicalRows.isEmpty
          ? pushOutboxStateNone
          : pushOutboxStatePrepared,
      sourceId: sourceId,
      sourceBundleId: sourceBundleId,
      initializationId: pendingInitializationId,
      canonicalRequestHash: canonicalHash,
      remoteBundleHash: '',
      remoteBundleSeq: 0,
      rows: canonicalRows,
    );
  }

  Future<OversqliteCommittedPush> _commitSnapshot({
    required OversqlitePushSnapshot snapshot,
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
        final committed = OversqliteCommittedPush(
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
          final committedPush = OversqliteCommittedPush(
            bundleSeq: committed.bundleSeq,
            sourceId: sourceId,
            sourceBundleId: snapshot.sourceBundleId,
            rowCount: committed.rowCount,
            bundleHash: committed.bundleHash,
            requiresStrictOutboxMatch: false,
          );
          await _pushStateStore.persistCommittedRemote(snapshot, committedPush);
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
    OversqlitePushSnapshot snapshot,
    OversqliteCommittedPush committed,
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
    required OversqliteCommittedPush committed,
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

  Future<void> _assertCommittedRowsMatchOutbox(
    OversqlitePushSnapshot snapshot,
    OversqliteCommittedPush committed,
    List<BundleRow> committedRows,
  ) async {
    final requirePayloadMatch =
        snapshot.state == pushOutboxStateCommittedRemote ||
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
    required OversqliteOutboxRow expected,
    required BundleRow actual,
    required int actualRowOrdinal,
    required bool requirePayloadMatch,
  }) async {
    if (expected.rowOrdinal != actualRowOrdinal ||
        expected.schemaName != actual.schema ||
        expected.tableName != actual.table ||
        !oversqliteSyncKeysEqual(
          syncKeyFromOversqliteJson(expected.wireKeyJson),
          actual.key,
        ) ||
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
    final table = await _localStore.tableInfo(tableName);
    return equivalentOversqlitePayloadObjects(
      table,
      expectedObject.cast<String, Object?>(),
      actualPayload.cast<String, Object?>(),
      PayloadSource.authoritativeWire,
      PayloadSource.authoritativeWire,
      allowRfc3339InstantEquivalence: true,
    );
  }
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

  factory _ComparableCommittedRow.fromOutbox(OversqliteOutboxRow row) {
    return _ComparableCommittedRow(
      rowOrdinal: row.rowOrdinal,
      schemaName: row.schemaName,
      tableName: row.tableName,
      wireKey: syncKeyFromOversqliteJson(row.wireKeyJson),
      op: row.op,
      wirePayload: row.wirePayload == null
          ? null
          : canonicalizeOversqliteProtocolJson(jsonDecode(row.wirePayload!)),
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
          : canonicalizeOversqliteProtocolJson(row.payload),
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

String _computeCommittedBundleHash(List<BundleRow> rows) {
  return sha256Hex(
    canonicalizeOversqliteProtocolJson([
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
  required OversqliteCommittedPush committed,
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

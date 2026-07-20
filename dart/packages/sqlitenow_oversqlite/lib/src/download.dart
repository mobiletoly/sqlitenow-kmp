import 'dart:convert';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';

import 'config.dart';
import 'download_apply_store.dart';
import 'download_stage_store.dart';
import 'local_runtime.dart';
import 'local_row_store.dart';
import 'payload_codec.dart';
import 'payload_source.dart';
import 'protocol.dart';
import 'protocol_models.dart';
import 'push_state_store.dart';
import 'runtime_state_store.dart';
import 'snapshot_capacity_retry.dart';
import 'snapshot_gate.dart';
import 'watch_protocol.dart';

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

final class SnapshotApplyFaultInjector {
  const SnapshotApplyFaultInjector({
    this.afterApplyPageLoaded,
    this.afterAppliedRow,
    this.beforeCommit,
  });

  final Future<void> Function()? afterApplyPageLoaded;
  final Future<void> Function()? afterAppliedRow;
  final Future<void> Function()? beforeCommit;
}

final class OversqliteDownloadRuntime {
  OversqliteDownloadRuntime({
    required SqliteNowDatabase database,
    required OversqliteConfig config,
    required OversqliteRemoteApi remoteApi,
    required CapabilitiesResponse capabilities,
    SnapshotApplyFaultInjector? faultInjector,
  }) : _database = database,
       _config = config,
       _remoteApi = remoteApi,
       _faultInjector = faultInjector,
       _snapshotNegotiation = negotiateSnapshotLimits(capabilities, config);

  final SqliteNowDatabase _database;
  final OversqliteConfig _config;
  final OversqliteRemoteApi _remoteApi;
  final SnapshotApplyFaultInjector? _faultInjector;
  final SnapshotNegotiation _snapshotNegotiation;

  SqliteNowConnection get _connection => _database.connection;
  OversqliteApplyRunner get _applyRunner => OversqliteApplyRunner(_connection);
  OversqliteAttachmentStateStore get _attachmentStore =>
      OversqliteAttachmentStateStore(_connection);
  OversqliteSourceStateStore get _sourceStore =>
      OversqliteSourceStateStore(_connection);
  OversqliteClientStateStore get _clientStateStore =>
      OversqliteClientStateStore(_connection);
  OversqliteLocalRowStore get _localStore =>
      OversqliteLocalRowStore(_connection);
  OversqlitePushStateStore get _pushStateStore => OversqlitePushStateStore(
    connection: _connection,
    applyRunner: _applyRunner,
  );
  OversqliteDownloadStageStore get _stageStore => OversqliteDownloadStageStore(
    connection: _connection,
    localStore: _localStore,
  );
  OversqliteDownloadApplyStore get _downloadApplyStore =>
      OversqliteDownloadApplyStore(
        localStore: _localStore,
        stateStore: _pushStateStore,
      );
  OversqliteSnapshotGate get _snapshotGate =>
      OversqliteSnapshotGate(_connection);

  Future<DownloadResult> pullToStable({
    required OversqliteValidatedConfig validated,
    required String sourceId,
    required String userId,
  }) async {
    await _requireOrdinarySyncAllowed();
    await _requireSnapshotRebuildState(SnapshotRebuildOutboxMode.clearAll);
    try {
      return await _pullIncremental(validated: validated, sourceId: sourceId);
    } on HistoryPrunedException {
      await _clientStateStore.persistCheckpointRecoveryRequiredState(
        'history_pruned',
      );
      return rebuildFromSnapshot(
        validated: validated,
        sourceId: sourceId,
        userId: userId,
      );
    } on CheckpointAheadException {
      await _clientStateStore.persistCheckpointRecoveryRequiredState(
        'checkpoint_ahead',
      );
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
    bool persistRemoteReplaceOperation = false,
  }) async {
    return _recordSnapshotRestore(() async {
      final diagnostics = _remoteApi.snapshotDiagnostics;
      if (rotatedSourceId == null &&
          outboxMode == SnapshotRebuildOutboxMode.clearAll &&
          !persistRemoteReplaceOperation) {
        await _clientStateStore.persistCheckpointRecoveryRequiredState(
          'explicit_rebuild',
        );
      }
      await _requireSnapshotRebuildState(outboxMode);
      final remoteReplace = persistRemoteReplaceOperation;
      if (remoteReplace) {
        final operation = await _clientStateStore.loadOperationState();
        if (operation.kind != oversqliteOperationKindRemoteReplace ||
            operation.targetUserId != userId) {
          throw StateError(
            'cannot finalize the durable remote replacement operation',
          );
        }
        if (operation.stagedSnapshotId.isNotEmpty) {
          final totals = await _stageStore.snapshotStageTotals(
            operation.stagedSnapshotId,
          );
          if (operation.snapshotStageComplete &&
              operation.snapshotByteCount >= 0 &&
              totals.rowCount == operation.snapshotRowCount) {
            diagnostics.recordStagedRows(totals.rowCount);
            final session = SnapshotSession(
              snapshotId: operation.stagedSnapshotId,
              snapshotBundleSeq: operation.snapshotBundleSeq,
              rowCount: operation.snapshotRowCount,
              byteCount: operation.snapshotByteCount,
              expiresAt: '',
            );
            final guard = await _snapshotGate.pin(
              outboxMode,
              remoteReplace: true,
              remoteTargetUserId: userId,
            );
            return _applyStagedSnapshot(
              validated: validated,
              sourceId: sourceId,
              userId: userId,
              session: session,
              rotatedSourceId: rotatedSourceId,
              outboxMode: outboxMode,
              pinnedGuard: guard,
            );
          }
          await _clearRemoteReplaceStage(userId);
        }
      } else {
        await _stageStore.clearAllSnapshotStages();
      }

      final initialGuard = await _snapshotGate.pin(
        outboxMode,
        remoteReplace: remoteReplace,
        remoteTargetUserId: remoteReplace ? userId : '',
      );
      final session = await withSnapshotCapacityRetry(
        policy: _config.snapshotCapacityRetryPolicy,
        operation: 'snapshot session request',
        diagnostics: _remoteApi.snapshotDiagnostics,
        attempt: () => _remoteApi.createSnapshotSession(
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
        ),
      );
      try {
        SnapshotApplyGuard guard;
        if (remoteReplace) {
          guard = await _connection.transaction(() async {
            await _snapshotGate.validateFinal(initialGuard, session);
            await _stageStore.clearAllSnapshotStages();
            await _persistRemoteReplaceOperation(
              targetUserId: userId,
              stagedSnapshotId: session.snapshotId,
              snapshotBundleSeq: session.snapshotBundleSeq,
              snapshotRowCount: session.rowCount,
              snapshotByteCount: session.byteCount,
              snapshotStageComplete: false,
            );
            return _snapshotGate.pin(
              outboxMode,
              remoteReplace: true,
              remoteTargetUserId: userId,
            );
          }, mode: TransactionMode.immediate);
        } else {
          guard = initialGuard;
        }
        final totals = await _downloadSnapshotSession(
          validated: validated,
          sourceId: sourceId,
          session: session,
        );
        diagnostics.recordStagedRows(totals.rows);
        if (remoteReplace) {
          guard = await _connection.transaction(() async {
            await _snapshotGate.validateFinal(guard, session);
            await _persistRemoteReplaceOperation(
              targetUserId: userId,
              stagedSnapshotId: session.snapshotId,
              snapshotBundleSeq: session.snapshotBundleSeq,
              snapshotRowCount: totals.rows,
              snapshotByteCount: totals.bytes,
              snapshotStageComplete: true,
            );
            return _snapshotGate.pin(
              outboxMode,
              remoteReplace: true,
              remoteTargetUserId: userId,
            );
          }, mode: TransactionMode.immediate);
        }
        return _applyStagedSnapshot(
          validated: validated,
          sourceId: sourceId,
          userId: userId,
          session: session,
          rotatedSourceId: rotatedSourceId,
          outboxMode: outboxMode,
          pinnedGuard: guard,
        );
      } finally {
        await _remoteApi.deleteSnapshotSessionBestEffort(
          snapshotId: session.snapshotId,
          sourceId: sourceId,
        );
      }
    });
  }

  Future<T> _recordSnapshotRestore<T>(Future<T> Function() block) async {
    final diagnostics = _remoteApi.snapshotDiagnostics;
    final stopwatch = Stopwatch()..start();
    diagnostics.markRestoreAttempt();
    try {
      return await block();
    } finally {
      stopwatch.stop();
      diagnostics.recordRestoreDuration(stopwatch.elapsed);
    }
  }

  Future<({int rows, int bytes})> _downloadSnapshotSession({
    required OversqliteValidatedConfig validated,
    required String sourceId,
    required SnapshotSession session,
  }) async {
    var afterRowOrdinal = 0;
    var fetchedRows = 0;
    var fetchedBytes = 0;
    while (true) {
      final chunk = await withSnapshotCapacityRetry(
        policy: _config.snapshotCapacityRetryPolicy,
        operation: 'snapshot chunk request',
        diagnostics: _remoteApi.snapshotDiagnostics,
        attempt: () => _remoteApi.fetchSnapshotChunk(
          snapshotId: session.snapshotId,
          sourceId: sourceId,
          snapshotBundleSeq: session.snapshotBundleSeq,
          afterRowOrdinal: afterRowOrdinal,
          maxRows: _snapshotNegotiation.maxRows,
          maxBytes: _snapshotNegotiation.maxBytes,
        ),
      );
      fetchedRows = checkedAddOversqliteInt64(
        fetchedRows,
        chunk.rows.length,
        'snapshot row total',
      );
      fetchedBytes = checkedAddOversqliteInt64(
        fetchedBytes,
        chunk.byteCount,
        'snapshot byte total',
      );
      if (fetchedRows > session.rowCount) {
        throw OversqliteProtocolException(
          'snapshot row total $fetchedRows exceeds session row_count ${session.rowCount}',
        );
      }
      if (fetchedBytes > session.byteCount) {
        throw OversqliteProtocolException(
          'snapshot byte total $fetchedBytes exceeds session byte_count ${session.byteCount}',
        );
      }
      if (chunk.hasMore &&
          (fetchedRows == session.rowCount ||
              fetchedBytes == session.byteCount)) {
        throw const SnapshotSemanticException(
          SnapshotSemanticFailure.invalidChunk,
        );
      }
      await _stageStore.stageSnapshotChunk(
        validated: validated,
        session: session,
        chunk: chunk,
        afterRowOrdinal: afterRowOrdinal,
      );
      afterRowOrdinal = chunk.nextRowOrdinal;
      if (chunk.hasMore) continue;
      if (fetchedRows != session.rowCount) {
        throw OversqliteProtocolException(
          'staged snapshot row count $fetchedRows does not match expected row_count ${session.rowCount}',
        );
      }
      if (fetchedBytes != session.byteCount) {
        throw OversqliteProtocolException(
          'snapshot final byte total $fetchedBytes does not match session byte_count ${session.byteCount}',
        );
      }
      if (afterRowOrdinal != fetchedRows) {
        throw const SnapshotSemanticException(
          SnapshotSemanticFailure.invalidChunk,
        );
      }
      return (rows: fetchedRows, bytes: fetchedBytes);
    }
  }

  Future<DownloadResult> _pullIncremental({
    required OversqliteValidatedConfig validated,
    required String sourceId,
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
    await _applyRunner.inApplyModeTransaction(() async {
      for (final row in bundle.rows) {
        if (row.schema != validated.schema) {
          throw OversqliteProtocolException(
            'bundle row schema ${row.schema} does not match ${validated.schema}',
          );
        }
        final key = await _localStore.localKeyFromWire(row.table, row.key);
        final currentVersion = await _pushStateStore.loadRowVersion(
          schema: row.schema,
          table: row.table,
          keyJson: key.keyJson,
        );
        if (currentVersion != null && currentVersion >= row.rowVersion) {
          continue;
        }
        await _downloadApplyStore.applyAuthoritativeRow(
          validated: validated,
          row: row,
          keyJson: key.keyJson,
          localPk: key.localPk,
        );
        await _pushStateStore.deleteDirty(
          schema: row.schema,
          table: row.table,
          keyJson: key.keyJson,
        );
        updatedTables.add(row.table.toLowerCase());
      }
      await _attachmentStore.markBundleSeen(bundle.bundleSeq);
    });
    return updatedTables;
  }

  Future<DownloadResult> _applyStagedSnapshot({
    required OversqliteValidatedConfig validated,
    required String sourceId,
    required String userId,
    required SnapshotSession session,
    required String? rotatedSourceId,
    required SnapshotRebuildOutboxMode outboxMode,
    required SnapshotApplyGuard pinnedGuard,
  }) async {
    if (rotatedSourceId != null) {
      await _requireFreshRotatedSourceState(rotatedSourceId);
    }
    final currentSourceId = rotatedSourceId ?? sourceId;
    final updatedTables = {
      for (final table in validated.tables) table.tableName.toLowerCase(),
    };
    final result = await _applyRunner.inApplyModeTransaction(() async {
      await _snapshotGate.validateFinal(pinnedGuard, session);
      final preservedOutbox = await _pushStateStore.loadOutboxBundle();
      var stagedRowCount = 0;
      var afterRowOrdinal = 0;
      var firstPage = true;
      while (true) {
        final page = await _stageStore.loadStagedSnapshotPage(
          validated: validated,
          snapshotId: session.snapshotId,
          afterRowOrdinal: afterRowOrdinal,
          maxRows: _config.snapshotApplyBatchRows,
          maxBytes: _config.snapshotApplyBatchBytes,
        );
        if (page.rows.isNotEmpty) {
          _remoteApi.snapshotDiagnostics.recordApplyPage(
            rows: page.rows.length,
            retainedTextBytes: page.retainedTextBytes,
            metadataRows: page.metadataRowCount,
            driverRows: page.driverRowCount,
            decodedRows: page.rows.length,
          );
          await _faultInjector?.afterApplyPageLoaded?.call();
        }
        if (firstPage) {
          if (session.rowCount > 0 && page.rows.isEmpty) {
            throw const SnapshotSemanticException(
              SnapshotSemanticFailure.invalidChunk,
            );
          }
          await _clearManagedTables(
            validated,
            clearOutbox: outboxMode == SnapshotRebuildOutboxMode.clearAll,
          );
          firstPage = false;
        }
        if (page.rows.isEmpty) break;
        for (final staged in page.rows) {
          try {
            await _downloadApplyStore.applyAuthoritativeRow(
              validated: validated,
              row: BundleRow(
                schema: staged.schemaName,
                table: staged.tableName,
                key: staged.wireKey,
                op: 'INSERT',
                rowVersion: staged.rowVersion,
                payload: jsonDecode(staged.payload),
              ),
              keyJson: staged.keyJson,
              localPk: staged.localPk,
            );
            await _faultInjector?.afterAppliedRow?.call();
          } on OversqliteProtocolException {
            rethrow;
          } catch (_) {
            throw SnapshotRowApplyException(
              rowOrdinal: staged.rowOrdinal,
              schemaName: staged.schemaName,
              tableName: staged.tableName,
            );
          }
          stagedRowCount = checkedAddOversqliteInt64(
            stagedRowCount,
            1,
            'staged snapshot applied row count',
          );
        }
        afterRowOrdinal = page.lastRowOrdinal!;
      }
      if (stagedRowCount != session.rowCount) {
        throw const SnapshotSemanticException(
          SnapshotSemanticFailure.invalidChunk,
        );
      }
      if (rotatedSourceId != null && sourceId != rotatedSourceId) {
        await _sourceStore.ensureSource(rotatedSourceId);
        await _sourceStore.markSourceReplaced(
          sourceId: sourceId,
          replacedBySourceId: rotatedSourceId,
        );
      }
      switch (outboxMode) {
        case SnapshotRebuildOutboxMode.clearAll:
          break;
        case SnapshotRebuildOutboxMode.preserveCommittedRemote:
          if (preservedOutbox.state == pushOutboxStateCommittedRemote &&
              preservedOutbox.rowCount > 0) {
            await _sourceStore.advanceAfterCommittedPush(
              sourceId: sourceId,
              sourceBundleId: preservedOutbox.sourceBundleId,
            );
            await _pushStateStore.clearOutbox();
          }
        case SnapshotRebuildOutboxMode.preserveSourceRecovery:
          if (preservedOutbox.state == pushOutboxStatePrepared) {
            await _replayPreparedOutboxIntent(
              sourceBundleId: preservedOutbox.sourceBundleId,
              expectedRowCount: preservedOutbox.rowCount,
            );
            await _sourceStore.ensureSource(currentSourceId);
            await _sourceStore.reserveSourceRecoveryBundle(currentSourceId);
            await _pushStateStore.rebindOutboxSource(
              sourceId: currentSourceId,
              sourceBundleId: 1,
            );
          }
      }
      await _attachmentStore.persistAttachedState(
        sourceId: currentSourceId,
        userId: userId,
        schema: validated.schema,
        lastBundleSeqSeen: session.snapshotBundleSeq,
      );
      await _clientStateStore.persistOperationState(
        OversqliteClientOperationState(),
      );
      await _stageStore.deleteSnapshotStage(session.snapshotId);
      await _faultInjector?.beforeCommit?.call();
      return DownloadResult(
        outcome: RemoteSyncOutcome.appliedSnapshot,
        updatedTables: updatedTables,
        restore: RestoreSummary(
          bundleSeq: session.snapshotBundleSeq,
          rowCount: session.rowCount,
        ),
        rotatedSourceId: rotatedSourceId,
      );
    });
    _remoteApi.snapshotDiagnostics.recordAppliedRows(session.rowCount);
    return result;
  }

  Future<void> _replayPreparedOutboxIntent({
    required int sourceBundleId,
    required int expectedRowCount,
  }) async {
    final totalRowCount = await _pushStateStore.countOutboxRows();
    final sourceRowCount = await _pushStateStore.countOutboxRowsForSourceBundle(
      sourceBundleId,
    );
    if (totalRowCount != expectedRowCount ||
        sourceRowCount != expectedRowCount) {
      throw StateError('prepared outbox row count changed before replay');
    }
    var replayedRows = 0;
    await _pushStateStore.visitPreparedOutboxRows(
      sourceBundleId: sourceBundleId,
      visit: (row) async {
        switch (row.op) {
          case 'DELETE':
            final table = await _localStore.tableInfo(row.tableName);
            final localPk = localPkFromOversqliteKeyJson(table, row.keyJson);
            await _localStore.deleteLocalRow(row.tableName, localPk);
          case 'INSERT':
          case 'UPDATE':
            final decoded = row.localPayload == null
                ? null
                : jsonDecode(row.localPayload!);
            if (decoded is! Map) {
              throw StateError(
                'prepared outbox row has no valid local payload',
              );
            }
            await _localStore.upsertPayload(
              row.tableName,
              decoded.cast<String, Object?>(),
              PayloadSource.localState,
              requireCompletePayload: true,
            );
          default:
            throw StateError(
              'prepared outbox row has an unsupported operation',
            );
        }
        replayedRows = checkedAddOversqliteInt64(
          replayedRows,
          1,
          'prepared outbox replay row count',
        );
      },
    );
    if (replayedRows != expectedRowCount) {
      throw StateError('prepared outbox row count changed during replay');
    }
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
      await _localStore.deleteAllRows(table.tableName);
      await _pushStateStore.clearRowStateForTable(
        schema: validated.schema,
        table: table.tableName,
      );
    }
    await _pushStateStore.clearDirtyRows();
    if (clearOutbox) {
      await _pushStateStore.clearOutbox();
    }
  }

  Future<void> _requireOrdinarySyncAllowed() async {
    final operation = await _clientStateStore.loadOperationState();
    if (operation.kind == oversqliteOperationKindSourceRecovery) {
      throw SourceRecoveryRequiredException(
        operation.requireSourceRecoveryReason(),
      );
    }
    final attachment = await _attachmentStore.loadState();
    if (attachment.rebuildRequired) {
      throw const RebuildRequiredException();
    }
  }

  Future<void> _requireFreshRotatedSourceState(String rotatedSourceId) async {
    requireValidOversqliteSourceId(rotatedSourceId);
    final rows = await _connection.select(
      '''SELECT next_source_bundle_id, replaced_by_source_id
FROM _sync_source_state
WHERE source_id = ?''',
      (row) => (
        nextSourceBundleId: row.readInt(0),
        replacedBySourceId: row.readString(1),
      ),
      parameters: [rotatedSourceId],
    );
    if (rows.isEmpty) {
      throw StateError('_sync_source_state missing for rotated source');
    }
    final state = rows.single;
    if (state.nextSourceBundleId != 1 || state.replacedBySourceId.isNotEmpty) {
      throw StateError('rotated rebuild requires a fresh replacement source');
    }
  }

  Future<void> _requireSnapshotRebuildState(
    SnapshotRebuildOutboxMode outboxMode,
  ) async {
    final outbox = await _pushStateStore.loadOutboxBundle();
    final outboxRows = await _pushStateStore.countOutboxRows();
    switch (outboxMode) {
      case SnapshotRebuildOutboxMode.clearAll:
        if (outboxRows > 0) {
          throw PendingPushReplayException(outboxRows);
        }
      case SnapshotRebuildOutboxMode.preserveCommittedRemote:
        if (outboxRows > 0 && outbox.state != pushOutboxStateCommittedRemote) {
          throw PendingPushReplayException(outboxRows);
        }
      case SnapshotRebuildOutboxMode.preserveSourceRecovery:
        if (outboxRows > 0 && outbox.state == pushOutboxStateNone) {
          throw PendingPushReplayException(outboxRows);
        }
    }
    final dirtyRows = await _pushStateStore.countDirtyRows();
    if (dirtyRows > 0) {
      throw DirtyStateRejectedException(dirtyRows);
    }
  }

  Future<int> _lastBundleSeqSeen() async {
    return (await _attachmentStore.loadState()).lastBundleSeqSeen;
  }

  Future<void> _persistRemoteReplaceOperation({
    required String targetUserId,
    required String stagedSnapshotId,
    required int snapshotBundleSeq,
    required int snapshotRowCount,
    required int snapshotByteCount,
    required bool snapshotStageComplete,
  }) {
    return _clientStateStore.persistOperationState(
      OversqliteClientOperationState(
        kind: oversqliteOperationKindRemoteReplace,
        targetUserId: targetUserId,
        stagedSnapshotId: stagedSnapshotId,
        snapshotBundleSeq: snapshotBundleSeq,
        snapshotRowCount: snapshotRowCount,
        snapshotByteCount: snapshotByteCount,
        snapshotStageComplete: snapshotStageComplete,
      ),
    );
  }

  Future<void> _clearRemoteReplaceStage(String targetUserId) async {
    await _connection.transaction(() async {
      await _stageStore.clearAllSnapshotStages();
      await _persistRemoteReplaceOperation(
        targetUserId: targetUserId,
        stagedSnapshotId: '',
        snapshotBundleSeq: 0,
        snapshotRowCount: 0,
        snapshotByteCount: 0,
        snapshotStageComplete: false,
      );
    }, mode: TransactionMode.immediate);
  }
}

import 'dart:convert';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';

import 'config.dart';
import 'download_apply_store.dart';
import 'download_stage_store.dart';
import 'local_runtime.dart';
import 'local_row_store.dart';
import 'protocol.dart';
import 'push_state_store.dart';
import 'runtime_state_store.dart';

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
  OversqliteApplyRunner get _applyRunner => OversqliteApplyRunner(_connection);
  OversqliteAttachmentStateStore get _attachmentStore =>
      OversqliteAttachmentStateStore(_connection);
  OversqliteSourceStateStore get _sourceStore =>
      OversqliteSourceStateStore(_connection);
  OversqliteClientStateStore get _clientStateStore =>
      OversqliteClientStateStore(_connection);
  OversqliteLocalRowStore get _localStore =>
      OversqliteLocalRowStore(_connection, _config.syncTables);
  OversqlitePushStateStore get _pushStateStore => OversqlitePushStateStore(
    connection: _connection,
    applyRunner: _applyRunner,
    syncTables: _config.syncTables,
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
    if (rotatedSourceId == null &&
        outboxMode == SnapshotRebuildOutboxMode.clearAll &&
        !persistRemoteReplaceOperation) {
      await _clientStateStore.persistCheckpointRecoveryRequiredState(
        'explicit_rebuild',
      );
    }
    await _requireSnapshotRebuildState(outboxMode);
    if (persistRemoteReplaceOperation) {
      final operation = await _clientStateStore.loadOperationState();
      if (operation.kind != oversqliteOperationKindRemoteReplace) {
        throw StateError(
          'cannot finalize durable operation ${operation.kind} as remote_replace',
        );
      }
      if (operation.stagedSnapshotId.isNotEmpty) {
        final stagedCount = await _stageStore.countSnapshotStageRows(
          operation.stagedSnapshotId,
        );
        if (stagedCount == operation.snapshotRowCount) {
          return _applyStagedSnapshot(
            validated: validated,
            sourceId: sourceId,
            userId: userId,
            session: SnapshotSession(
              snapshotId: operation.stagedSnapshotId,
              snapshotBundleSeq: operation.snapshotBundleSeq,
              rowCount: operation.snapshotRowCount,
              byteCount: 0,
              expiresAt: '2099-01-01T00:00:00Z',
            ),
            rotatedSourceId: rotatedSourceId,
            outboxMode: outboxMode,
          );
        }
      }
      await _stageStore.clearAllSnapshotStages();
      await _persistRemoteReplaceOperation(
        targetUserId: userId,
        stagedSnapshotId: '',
        snapshotBundleSeq: 0,
        snapshotRowCount: 0,
      );
    } else {
      await _stageStore.clearAllSnapshotStages();
    }

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
      if (persistRemoteReplaceOperation) {
        await _persistRemoteReplaceOperation(
          targetUserId: userId,
          stagedSnapshotId: session.snapshotId,
          snapshotBundleSeq: session.snapshotBundleSeq,
          snapshotRowCount: session.rowCount,
        );
      }
      var afterRowOrdinal = 0;
      while (true) {
        final chunk = await _remoteApi.fetchSnapshotChunk(
          snapshotId: session.snapshotId,
          sourceId: sourceId,
          snapshotBundleSeq: session.snapshotBundleSeq,
          afterRowOrdinal: afterRowOrdinal,
          maxRows: _config.downloadLimit <= 0 ? 1000 : _config.downloadLimit,
        );
        await _stageStore.stageSnapshotChunk(
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
  }) async {
    if (rotatedSourceId != null) {
      await _requireFreshRotatedSourceState(rotatedSourceId);
    }
    final currentSourceId = rotatedSourceId ?? sourceId;
    final updatedTables = {
      for (final table in validated.tables) table.tableName.toLowerCase(),
    };
    await _applyRunner.inApplyModeTransaction(() async {
      final preservedOutbox = await _pushStateStore.loadOutboxBundle();
      await _clearManagedTables(
        validated,
        clearOutbox: outboxMode == SnapshotRebuildOutboxMode.clearAll,
      );
      var stagedRowCount = 0;
      final rows = await _stageStore.loadStagedSnapshotRows(session.snapshotId);
      for (final staged in rows) {
        await _downloadApplyStore.applyAuthoritativeRow(
          validated: validated,
          row: BundleRow(
            schema: validated.schema,
            table: staged.tableName,
            key: staged.wireKey,
            op: 'INSERT',
            rowVersion: staged.rowVersion,
            payload: jsonDecode(staged.payload),
          ),
          keyJson: staged.keyJson,
          localPk: staged.localPk,
        );
        stagedRowCount++;
      }
      if (stagedRowCount != session.rowCount) {
        throw OversqliteProtocolException(
          'staged snapshot row count $stagedRowCount does not match expected row_count ${session.rowCount}',
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
          if (preservedOutbox.state != pushOutboxStateNone &&
              preservedOutbox.rowCount > 0) {
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
    });

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
      throw StateError(
        '_sync_source_state missing for rotated source $rotatedSourceId',
      );
    }
    final state = rows.single;
    if (state.nextSourceBundleId != 1 ||
        state.replacedBySourceId.trim().isNotEmpty) {
      throw StateError(
        'rotated rebuild requires a fresh replacement source; $rotatedSourceId is already in use',
      );
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
  }) {
    return _clientStateStore.persistOperationState(
      OversqliteClientOperationState(
        kind: oversqliteOperationKindRemoteReplace,
        targetUserId: targetUserId,
        stagedSnapshotId: stagedSnapshotId,
        snapshotBundleSeq: snapshotBundleSeq,
        snapshotRowCount: snapshotRowCount,
      ),
    );
  }
}

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';

import 'protocol.dart';
import 'push_state_store.dart';
import 'runtime_state_store.dart';

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

typedef SnapshotOutboxFingerprint = ({
  String state,
  String sourceId,
  int sourceBundleId,
  String canonicalRequestHash,
  int rowCount,
  int remoteBundleSeq,
  String remoteBundleHash,
  int actualRowCount,
});

typedef SnapshotAttachmentFingerprint = ({
  String currentSourceId,
  String bindingState,
  String attachedUserId,
  String schemaName,
  int lastBundleSeqSeen,
  bool rebuildRequired,
  String pendingInitializationId,
});

typedef SnapshotOperationFingerprint = ({
  String kind,
  String targetUserId,
  String stagedSnapshotId,
  int snapshotBundleSeq,
  int snapshotRowCount,
  int snapshotByteCount,
  bool snapshotStageComplete,
  String reason,
  String replacementSourceId,
});

final class SnapshotApplyGuard {
  const SnapshotApplyGuard({
    required this.mode,
    required this.dirtyRowCount,
    required this.outbox,
    required this.attachment,
    required this.operation,
    required this.remoteReplace,
    required this.remoteTargetUserId,
  });

  final SnapshotRebuildOutboxMode mode;
  final int dirtyRowCount;
  final SnapshotOutboxFingerprint outbox;
  final SnapshotAttachmentFingerprint attachment;
  final SnapshotOperationFingerprint operation;
  final bool remoteReplace;
  final String remoteTargetUserId;
}

final class OversqliteSnapshotGate {
  const OversqliteSnapshotGate(this._connection);

  final SqliteNowConnection _connection;

  OversqlitePushStateStore get _pushStore => OversqlitePushStateStore(
    connection: _connection,
    applyRunner: OversqliteApplyRunner(_connection),
  );
  OversqliteClientStateStore get _clientStore =>
      OversqliteClientStateStore(_connection);

  Future<SnapshotApplyGuard> pin(
    SnapshotRebuildOutboxMode mode, {
    bool remoteReplace = false,
    String remoteTargetUserId = '',
  }) {
    return _connection.transaction(() async {
      final guard = await _load(
        mode,
        remoteReplace: remoteReplace,
        remoteTargetUserId: remoteTargetUserId,
      );
      _validateMode(guard);
      return guard;
    });
  }

  Future<void> validateFinal(
    SnapshotApplyGuard pinned,
    SnapshotSession session,
  ) async {
    final current = await _load(
      pinned.mode,
      remoteReplace: pinned.remoteReplace,
      remoteTargetUserId: pinned.remoteTargetUserId,
    );
    if (current.dirtyRowCount != pinned.dirtyRowCount) {
      throw SnapshotFinalApplyGateException(
        mode: pinned.mode.name,
        reason: 'dirty row count changed',
        cause: DirtyStateRejectedException(current.dirtyRowCount),
      );
    }
    if (current.outbox != pinned.outbox) {
      throw SnapshotFinalApplyGateException(
        mode: pinned.mode.name,
        reason: 'outbox fingerprint or actual row count changed',
        cause: PendingPushReplayException(current.outbox.actualRowCount),
      );
    }
    if (current.attachment != pinned.attachment ||
        current.operation != pinned.operation) {
      throw SnapshotFinalApplyGateException(
        mode: pinned.mode.name,
        reason: 'lifecycle guard changed',
        cause: pinned.mode == SnapshotRebuildOutboxMode.preserveSourceRecovery
            ? const SourceRecoveryRequiredException(
                SourceRecoveryReason.sourceSequenceChanged,
              )
            : const RebuildRequiredException(),
      );
    }
    try {
      _validateMode(current);
    } catch (cause) {
      throw SnapshotFinalApplyGateException(
        mode: pinned.mode.name,
        reason: 'pending-work mode is no longer valid',
        cause: cause,
      );
    }
    if (pinned.mode == SnapshotRebuildOutboxMode.preserveCommittedRemote &&
        session.snapshotBundleSeq < current.outbox.remoteBundleSeq) {
      throw SnapshotFinalApplyGateException(
        mode: pinned.mode.name,
        reason: 'snapshot is older than the committed remote bundle',
        cause: PendingPushReplayException(current.outbox.actualRowCount),
      );
    }
  }

  Future<SnapshotApplyGuard> _load(
    SnapshotRebuildOutboxMode mode, {
    required bool remoteReplace,
    required String remoteTargetUserId,
  }) async {
    final outbox = await _pushStore.loadOutboxBundle();
    final attachment = await _clientStore.loadAttachmentState();
    final operation = await _clientStore.loadOperationState();
    return SnapshotApplyGuard(
      mode: mode,
      dirtyRowCount: await _pushStore.countDirtyRows(),
      outbox: (
        state: outbox.state,
        sourceId: outbox.sourceId,
        sourceBundleId: outbox.sourceBundleId,
        canonicalRequestHash: outbox.canonicalRequestHash,
        rowCount: outbox.rowCount,
        remoteBundleSeq: outbox.remoteBundleSeq,
        remoteBundleHash: outbox.remoteBundleHash,
        actualRowCount: await _pushStore.countOutboxRows(),
      ),
      attachment: (
        currentSourceId: attachment.currentSourceId,
        bindingState: attachment.bindingState,
        attachedUserId: attachment.attachedUserId,
        schemaName: attachment.schemaName,
        lastBundleSeqSeen: attachment.lastBundleSeqSeen,
        rebuildRequired: attachment.rebuildRequired,
        pendingInitializationId: attachment.pendingInitializationId,
      ),
      operation: (
        kind: operation.kind,
        targetUserId: operation.targetUserId,
        stagedSnapshotId: operation.stagedSnapshotId,
        snapshotBundleSeq: operation.snapshotBundleSeq,
        snapshotRowCount: operation.snapshotRowCount,
        snapshotByteCount: operation.snapshotByteCount,
        snapshotStageComplete: operation.snapshotStageComplete,
        reason: operation.reason,
        replacementSourceId: operation.replacementSourceId,
      ),
      remoteReplace: remoteReplace,
      remoteTargetUserId: remoteTargetUserId,
    );
  }

  void _validateMode(SnapshotApplyGuard guard) {
    if (guard.dirtyRowCount != 0) {
      throw DirtyStateRejectedException(guard.dirtyRowCount);
    }
    switch (guard.mode) {
      case SnapshotRebuildOutboxMode.clearAll:
        _requireNoOutbox(guard);
        if (guard.remoteReplace) {
          if (guard.attachment.bindingState !=
                  oversqliteAttachmentBindingAnonymous ||
              guard.operation.kind != oversqliteOperationKindRemoteReplace ||
              guard.operation.targetUserId != guard.remoteTargetUserId) {
            throw const RebuildRequiredException();
          }
        } else if (guard.attachment.bindingState !=
                oversqliteAttachmentBindingAttached ||
            !guard.attachment.rebuildRequired ||
            guard.operation.kind != oversqliteOperationKindNone) {
          throw const RebuildRequiredException();
        }
      case SnapshotRebuildOutboxMode.preserveCommittedRemote:
        final outbox = guard.outbox;
        if (outbox.state != pushOutboxStateCommittedRemote ||
            outbox.sourceId.isEmpty ||
            outbox.sourceBundleId <= 0 ||
            outbox.canonicalRequestHash.isEmpty ||
            outbox.rowCount <= 0 ||
            outbox.remoteBundleSeq <= 0 ||
            outbox.remoteBundleHash.isEmpty ||
            outbox.actualRowCount != outbox.rowCount) {
          throw PendingPushReplayException(outbox.actualRowCount);
        }
        if (guard.attachment.bindingState !=
                oversqliteAttachmentBindingAttached ||
            !guard.attachment.rebuildRequired ||
            guard.operation.kind != oversqliteOperationKindNone) {
          throw const RebuildRequiredException();
        }
      case SnapshotRebuildOutboxMode.preserveSourceRecovery:
        if (guard.attachment.bindingState !=
                oversqliteAttachmentBindingAttached ||
            !guard.attachment.rebuildRequired ||
            guard.operation.kind != oversqliteOperationKindSourceRecovery) {
          throw const SourceRecoveryRequiredException(
            SourceRecoveryReason.sourceSequenceChanged,
          );
        }
        switch (guard.outbox.state) {
          case pushOutboxStateNone:
            _requireNoOutbox(guard);
          case pushOutboxStatePrepared:
            final outbox = guard.outbox;
            if (outbox.sourceId.isEmpty ||
                outbox.sourceBundleId <= 0 ||
                outbox.canonicalRequestHash.isEmpty ||
                outbox.rowCount <= 0 ||
                outbox.actualRowCount != outbox.rowCount ||
                outbox.remoteBundleSeq != 0 ||
                outbox.remoteBundleHash.isNotEmpty) {
              throw PendingPushReplayException(outbox.actualRowCount);
            }
          default:
            throw const SourceRecoveryRequiredException(
              SourceRecoveryReason.sourceSequenceChanged,
            );
        }
    }
  }

  void _requireNoOutbox(SnapshotApplyGuard guard) {
    if (guard.outbox.state != pushOutboxStateNone ||
        guard.outbox.actualRowCount != 0) {
      throw PendingPushReplayException(guard.outbox.actualRowCount);
    }
  }
}

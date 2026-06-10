import 'dart:math';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';

import 'local_runtime.dart';
import 'protocol.dart';

const oversqliteAttachmentBindingAttached = 'attached';
const oversqliteOperationKindNone = 'none';
const oversqliteOperationKindRemoteReplace = 'remote_replace';
const oversqliteOperationKindSourceRecovery = 'source_recovery';

final class OversqliteApplyRunner {
  const OversqliteApplyRunner(this._connection);

  final SqliteNowConnection _connection;

  Future<T> inApplyModeTransaction<T>(Future<T> Function() block) {
    return _connection.transaction(() async {
      await _connection.execute('PRAGMA defer_foreign_keys = ON');
      await _connection.execute(
        'UPDATE _sync_apply_state SET apply_mode = 1 WHERE singleton_key = 1',
      );
      try {
        return await block();
      } finally {
        await _connection.execute(
          'UPDATE _sync_apply_state SET apply_mode = 0 WHERE singleton_key = 1',
        );
      }
    }, mode: TransactionMode.immediate);
  }
}

final class OversqliteClientAttachmentState {
  const OversqliteClientAttachmentState({
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

final class OversqliteClientOperationState {
  OversqliteClientOperationState({
    this.kind = oversqliteOperationKindNone,
    this.targetUserId = '',
    this.stagedSnapshotId = '',
    this.snapshotBundleSeq = 0,
    this.snapshotRowCount = 0,
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

  SourceRecoveryReason requireSourceRecoveryReason() {
    return switch (reason) {
      'history_pruned' => SourceRecoveryReason.historyPruned,
      'source_sequence_out_of_order' =>
        SourceRecoveryReason.sourceSequenceOutOfOrder,
      'source_sequence_changed' => SourceRecoveryReason.sourceSequenceChanged,
      'source_retired' => SourceRecoveryReason.sourceRetired,
      _ => throw StateError(
        'source recovery operation state is missing a valid recovery reason',
      ),
    };
  }
}

final class OversqliteClientStateStore {
  const OversqliteClientStateStore(this._connection);

  final SqliteNowConnection _connection;

  OversqliteAttachmentStateStore get _attachmentStore =>
      OversqliteAttachmentStateStore(_connection);
  OversqliteSourceStateStore get _sourceStore =>
      OversqliteSourceStateStore(_connection);

  static String quoteIdentifier(String identifier) =>
      '"${identifier.replaceAll('"', '""')}"';

  String generateFreshSourceId() {
    final random = Random.secure();
    final bytes = List<int>.generate(16, (_) => random.nextInt(256));
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    final hex = bytes
        .map((byte) => byte.toRadixString(16).padLeft(2, '0'))
        .join();
    return '${hex.substring(0, 8)}-${hex.substring(8, 12)}-${hex.substring(12, 16)}-${hex.substring(16, 20)}-${hex.substring(20)}';
  }

  Future<OversqliteClientAttachmentState> loadAttachmentState() async {
    final rows = await _connection.select(
      '''SELECT current_source_id, binding_state, attached_user_id, schema_name,
       last_bundle_seq_seen, rebuild_required, pending_initialization_id
FROM _sync_attachment_state
WHERE singleton_key = 1''',
      (row) => OversqliteClientAttachmentState(
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

  Future<OversqliteClientOperationState> loadOperationState() async {
    final rows = await _connection.select(
      '''SELECT kind, target_user_id, staged_snapshot_id, snapshot_bundle_seq, snapshot_row_count,
       reason, replacement_source_id
FROM _sync_operation_state
WHERE singleton_key = 1''',
      (row) => OversqliteClientOperationState(
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

  Future<void> persistAnonymousState(String sourceId) {
    return _connection.execute(
      '''UPDATE _sync_attachment_state
SET current_source_id = ?,
    binding_state = 'anonymous',
    attached_user_id = '',
    schema_name = '',
    last_bundle_seq_seen = 0,
    rebuild_required = 0,
    pending_initialization_id = ''
WHERE singleton_key = 1''',
      parameters: [sourceId],
    );
  }

  Future<void> persistConnectedLifecycleState({
    required String userId,
    required String sourceId,
    required String schema,
    required String initializationId,
  }) {
    return _connection.execute(
      '''UPDATE _sync_attachment_state
SET current_source_id = ?,
    binding_state = 'attached',
    attached_user_id = ?,
    schema_name = ?,
    rebuild_required = 0,
    pending_initialization_id = ?
WHERE singleton_key = 1''',
      parameters: [sourceId, userId, schema, initializationId],
    );
  }

  Future<void> beginRemoteReplace({
    required String userId,
    required String sourceId,
  }) async {
    await persistAnonymousState(sourceId);
    await persistOperationState(
      OversqliteClientOperationState(
        kind: oversqliteOperationKindRemoteReplace,
        targetUserId: userId,
      ),
    );
  }

  Future<void> persistOperationState(OversqliteClientOperationState state) {
    return _connection.execute(
      '''UPDATE _sync_operation_state
SET kind = ?,
    target_user_id = ?,
    staged_snapshot_id = ?,
    snapshot_bundle_seq = ?,
    snapshot_row_count = ?,
    reason = ?,
    replacement_source_id = ?
WHERE singleton_key = 1''',
      parameters: [
        state.kind,
        state.targetUserId,
        state.stagedSnapshotId,
        state.snapshotBundleSeq,
        state.snapshotRowCount,
        state.reason,
        state.replacementSourceId,
      ],
    );
  }

  Future<String> loadOutboxState() async {
    final rows = await _connection.select(
      'SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1',
      (row) => row.readString(0),
    );
    return rows.single;
  }

  Future<void> persistSourceRecoveryRequiredState({
    required String currentSourceId,
    required SourceRecoveryReason reason,
    String? replacementSourceId,
  }) async {
    await _connection.transaction(() async {
      await _attachmentStore.markRebuildRequired();
      final replacement = await _reserveReplacementSourceId(
        currentSourceId: currentSourceId,
        preferredReplacementSourceId: replacementSourceId,
      );
      await persistOperationState(
        OversqliteClientOperationState(
          kind: oversqliteOperationKindSourceRecovery,
          reason: reason.wireName,
          replacementSourceId: replacement,
        ),
      );
    }, mode: TransactionMode.immediate);
  }

  Future<Set<String>> managedTableNames(
    OversqliteValidatedConfig validated,
  ) async {
    final rows = await _connection.select(
      'SELECT table_name FROM _sync_managed_tables WHERE schema_name = ? ORDER BY table_name',
      (row) => row.readString(0).toLowerCase(),
      parameters: [validated.schema],
    );
    return {
      ...rows,
      for (final table in validated.tables) table.tableName.toLowerCase(),
    };
  }

  Future<bool> sqliteTableExists(String tableName) async {
    final rows = await _connection.select(
      "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
      (row) => row.readInt(0),
      parameters: [tableName],
    );
    return rows.single > 0;
  }

  Future<int> scalarInt(String sql) async {
    final rows = await _connection.select(sql, (row) => row.readInt(0));
    return rows.single;
  }

  Future<String> _reserveReplacementSourceId({
    required String currentSourceId,
    String? preferredReplacementSourceId,
  }) async {
    final operation = await loadOperationState();
    final existing = operation.replacementSourceId.trim();
    final preferred = preferredReplacementSourceId?.trim() ?? '';
    final reserved = switch ((existing, preferred)) {
      (final local, final remote)
          when local.isNotEmpty && remote.isNotEmpty && local != remote =>
        throw SourceReplacementDivergedException(
          localReplacementSourceId: local,
          remoteReplacementSourceId: remote,
        ),
      (final local, _) when local.isNotEmpty => local,
      (_, final remote) when remote.isNotEmpty => remote,
      _ => _generateFreshReplacementSourceId(currentSourceId),
    };
    await _ensureFreshReplacementSourceState(
      currentSourceId: currentSourceId,
      replacementSourceId: reserved,
    );
    return reserved;
  }

  Future<void> _ensureFreshReplacementSourceState({
    required String currentSourceId,
    required String replacementSourceId,
  }) async {
    if (replacementSourceId.trim().isEmpty) {
      throw StateError('replacement source id must not be blank');
    }
    if (replacementSourceId == currentSourceId) {
      throw StateError(
        'replacement source id must differ from current source id',
      );
    }
    await _sourceStore.ensureSource(replacementSourceId);
    final rows = await _connection.select(
      '''SELECT next_source_bundle_id, replaced_by_source_id
FROM _sync_source_state
WHERE source_id = ?''',
      (row) => (
        nextSourceBundleId: row.readInt(0),
        replacedBySourceId: row.readString(1),
      ),
      parameters: [replacementSourceId],
    );
    if (rows.isEmpty) {
      throw StateError(
        '_sync_source_state missing for replacement source $replacementSourceId',
      );
    }
    final state = rows.single;
    if (state.nextSourceBundleId != 1 ||
        state.replacedBySourceId.trim().isNotEmpty) {
      throw StateError(
        'replacement source $replacementSourceId is not fresh for source recovery',
      );
    }
  }

  String _generateFreshReplacementSourceId(String currentSourceId) {
    var candidate = generateFreshSourceId();
    while (candidate == currentSourceId) {
      candidate = generateFreshSourceId();
    }
    return candidate;
  }
}

final class OversqliteAttachmentStateStore {
  const OversqliteAttachmentStateStore(this._connection);

  final SqliteNowConnection _connection;

  Future<OversqliteClientAttachmentState> loadState() async {
    final rows = await _connection.select(
      '''SELECT current_source_id, binding_state, attached_user_id, schema_name,
       last_bundle_seq_seen, rebuild_required, pending_initialization_id
FROM _sync_attachment_state
WHERE singleton_key = 1''',
      (row) => OversqliteClientAttachmentState(
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

  Future<void> markBundleSeen(int bundleSeq) {
    return _connection.execute(
      '''UPDATE _sync_attachment_state
SET last_bundle_seq_seen = CASE
      WHEN last_bundle_seq_seen < ? THEN ?
      ELSE last_bundle_seq_seen
    END
WHERE singleton_key = 1''',
      parameters: [bundleSeq, bundleSeq],
    );
  }

  Future<void> advanceAfterCommittedPush(int bundleSeq) {
    return _connection.execute(
      '''UPDATE _sync_attachment_state
SET last_bundle_seq_seen = CASE
      WHEN last_bundle_seq_seen + 1 = ? THEN ?
      ELSE last_bundle_seq_seen
    END,
    pending_initialization_id = ''
WHERE singleton_key = 1''',
      parameters: [bundleSeq, bundleSeq],
    );
  }

  Future<void> markRebuildRequired() {
    return _connection.execute(
      'UPDATE _sync_attachment_state SET rebuild_required = 1 WHERE singleton_key = 1',
    );
  }

  Future<void> persistAttachedState({
    required String sourceId,
    required String userId,
    required String schema,
    required int lastBundleSeqSeen,
  }) {
    return _connection.execute(
      '''UPDATE _sync_attachment_state
SET current_source_id = ?,
    binding_state = 'attached',
    attached_user_id = ?,
    schema_name = ?,
    last_bundle_seq_seen = ?,
    rebuild_required = 0,
    pending_initialization_id = ''
WHERE singleton_key = 1''',
      parameters: [sourceId, userId, schema, lastBundleSeqSeen],
    );
  }
}

final class OversqliteSourceStateStore {
  const OversqliteSourceStateStore(this._connection);

  final SqliteNowConnection _connection;

  Future<void> ensureSource(String sourceId) {
    return _connection.execute(
      '''INSERT INTO _sync_source_state(source_id, next_source_bundle_id, replaced_by_source_id)
VALUES(?, 1, '')
ON CONFLICT(source_id) DO NOTHING''',
      parameters: [sourceId],
    );
  }

  Future<void> markSourceReplaced({
    required String sourceId,
    required String replacedBySourceId,
  }) {
    return _connection.execute(
      'UPDATE _sync_source_state SET replaced_by_source_id = ? WHERE source_id = ?',
      parameters: [replacedBySourceId, sourceId],
    );
  }

  Future<void> advanceAfterCommittedPush({
    required String sourceId,
    required int sourceBundleId,
  }) {
    return _connection.execute(
      '''UPDATE _sync_source_state
SET next_source_bundle_id = CASE
  WHEN next_source_bundle_id <= ? THEN ? + 1
  ELSE next_source_bundle_id
END
WHERE source_id = ?''',
      parameters: [sourceBundleId, sourceBundleId, sourceId],
    );
  }

  Future<void> reserveSourceRecoveryBundle(String sourceId) {
    return _connection.execute(
      '''UPDATE _sync_source_state
SET next_source_bundle_id = CASE
  WHEN next_source_bundle_id < 2 THEN 2
  ELSE next_source_bundle_id
END
WHERE source_id = ?''',
      parameters: [sourceId],
    );
  }
}

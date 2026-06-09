import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';

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

final class OversqliteAttachmentStateStore {
  const OversqliteAttachmentStateStore(this._connection);

  final SqliteNowConnection _connection;

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

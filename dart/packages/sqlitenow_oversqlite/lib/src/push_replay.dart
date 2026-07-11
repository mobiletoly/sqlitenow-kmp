import 'local_runtime.dart';
import 'local_row_store.dart';
import 'payload_codec.dart';
import 'payload_source.dart';
import 'protocol.dart';
import 'push_state_store.dart';
import 'runtime_state_store.dart';
import 'table_info.dart';

final class OversqlitePushReplayExecutor {
  const OversqlitePushReplayExecutor({
    required OversqliteApplyRunner applyRunner,
    required OversqliteAttachmentStateStore attachmentStore,
    required OversqliteSourceStateStore sourceStore,
    required OversqliteLocalRowStore localStore,
    required OversqlitePushStateStore pushStateStore,
  }) : _applyRunner = applyRunner,
       _attachmentStore = attachmentStore,
       _sourceStore = sourceStore,
       _localStore = localStore,
       _pushStateStore = pushStateStore;

  final OversqliteApplyRunner _applyRunner;
  final OversqliteAttachmentStateStore _attachmentStore;
  final OversqliteSourceStateStore _sourceStore;
  final OversqliteLocalRowStore _localStore;
  final OversqlitePushStateStore _pushStateStore;

  Future<Set<String>> applyCommittedRows({
    required OversqliteValidatedConfig validated,
    required OversqlitePushSnapshot snapshot,
    required OversqliteCommittedPush committed,
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
      await _pushStateStore.clearOutbox();
    });
    return updatedTables;
  }

  Future<void> _applyCommittedReplayRow({
    required OversqliteOutboxRow uploaded,
    required BundleRow row,
  }) async {
    final key = await _localStore.localKeyFromWire(row.table, row.key);
    final table = await _localStore.tableInfo(row.table);
    final pendingDirty = await _pushStateStore.loadDirtyUploadState(
      schema: row.schema,
      table: row.table,
      keyJson: key.keyJson,
    );
    final livePayload = await _localStore.serializeExistingRow(
      row.table,
      key.localPk,
    );
    final action = _planCommittedReplay(
      table: table,
      uploaded: uploaded,
      pendingDirty: pendingDirty,
      livePayload: livePayload,
    );
    switch (action) {
      case _ReplayAcceptAuthoritative():
        await _applyAuthoritativePayload(row, key.localPk, key.keyJson);
        await _pushStateStore.deleteDirty(
          schema: row.schema,
          table: row.table,
          keyJson: key.keyJson,
        );
      case _ReplayPreserveLocal(:final op, :final payload):
        await _pushStateStore.updateRowState(
          schema: row.schema,
          table: row.table,
          keyJson: key.keyJson,
          rowVersion: row.rowVersion,
          deleted: row.op == 'DELETE',
        );
        await _pushStateStore.requeueDirty(
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
    required OversqliteTableInfo table,
    required OversqliteOutboxRow uploaded,
    required OversqliteDirtyUploadState? pendingDirty,
    required String? livePayload,
  }) {
    final pendingMatches =
        pendingDirty != null &&
        pendingDirty.op == uploaded.op &&
        equivalentOversqliteLocalPayload(
          table,
          pendingDirty.payload,
          uploaded.localPayload,
        );
    final liveMatches = uploaded.op == 'DELETE'
        ? livePayload == null
        : livePayload != null &&
              equivalentOversqliteLocalPayload(
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
      await _localStore.deleteLocalRow(row.table, localPk);
      await _pushStateStore.updateRowState(
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
    try {
      await _localStore.upsertPayload(
        row.table,
        payload.cast<String, Object?>(),
        PayloadSource.authoritativeWire,
      );
    } on OversqliteProtocolException {
      rethrow;
    } catch (error) {
      throw OversqliteProtocolException(
        'committed ${row.op} row for ${row.table} cannot be applied: $error',
      );
    }
    await _pushStateStore.updateRowState(
      schema: row.schema,
      table: row.table,
      keyJson: keyJson,
      rowVersion: row.rowVersion,
      deleted: false,
    );
  }
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

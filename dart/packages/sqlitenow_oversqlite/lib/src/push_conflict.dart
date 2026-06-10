import 'dart:convert';

import 'local_runtime.dart';
import 'local_row_store.dart';
import 'payload_codec.dart';
import 'payload_source.dart';
import 'protocol.dart';
import 'push_state_store.dart';
import 'resolver.dart';
import 'runtime_state_store.dart';

final class OversqliteConflictExecutor {
  const OversqliteConflictExecutor({
    required Resolver resolver,
    required OversqliteApplyRunner applyRunner,
    required OversqliteLocalRowStore localStore,
    required OversqlitePushStateStore pushStateStore,
  }) : _resolver = resolver,
       _applyRunner = applyRunner,
       _localStore = localStore,
       _pushStateStore = pushStateStore;

  final Resolver _resolver;
  final OversqliteApplyRunner _applyRunner;
  final OversqliteLocalRowStore _localStore;
  final OversqlitePushStateStore _pushStateStore;

  Future<void> resolveConflict({
    required OversqliteValidatedConfig validated,
    required OversqlitePushSnapshot snapshot,
    required PushConflictDetails conflict,
  }) async {
    final conflictingRow = snapshot.rows.firstWhere(
      (row) =>
          row.schemaName == conflict.schema &&
          row.tableName == conflict.table &&
          oversqliteSyncKeysEqual(
            syncKeyFromOversqliteJson(row.wireKeyJson),
            conflict.key,
          ),
    );
    final conflictingTable = await _localStore.tableInfo(
      conflictingRow.tableName,
    );
    final conflictingLocalPk = localPkFromOversqliteKeyJson(
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
      await _pushStateStore.restoreSnapshotToDirtyRows(snapshot);
      throw InvalidConflictResolutionException(invalidReason);
    }
    await _applyRunner.inApplyModeTransaction(() async {
      if (resolution is AcceptServer) {
        if (conflict.serverRowDeleted || context.serverRow == null) {
          await _localStore.deleteLocalRow(conflict.table, conflictingLocalPk);
          await _pushStateStore.updateRowState(
            schema: validated.schema,
            table: conflict.table,
            keyJson: conflictingRow.keyJson,
            rowVersion: conflict.serverRowVersion,
            deleted: true,
          );
        } else {
          await _localStore.upsertPayload(
            conflict.table,
            context.serverRow!,
            PayloadSource.authoritativeWire,
          );
          await _pushStateStore.updateRowState(
            schema: validated.schema,
            table: conflict.table,
            keyJson: conflictingRow.keyJson,
            rowVersion: conflict.serverRowVersion,
            deleted: false,
          );
        }
      } else if (resolution is KeepLocal) {
        await _pushStateStore.requeueDirty(
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
        await _localStore.upsertPayload(
          conflict.table,
          resolution.mergedPayload,
          PayloadSource.localState,
        );
        final payload = await _localStore.serializeExistingRow(
          conflict.table,
          conflictingLocalPk,
        );
        await _pushStateStore.requeueDirty(
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
        await _pushStateStore.requeueDirty(
          schema: row.schemaName,
          table: row.tableName,
          keyJson: row.keyJson,
          op: row.op,
          baseRowVersion: row.baseRowVersion,
          payload: row.localPayload,
        );
      }
      await _pushStateStore.clearOutbox();
    });
  }
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

import 'local_runtime.dart';
import 'local_row_store.dart';
import 'payload_source.dart';
import 'protocol.dart';
import 'push_state_store.dart';

final class OversqliteDownloadApplyStore {
  const OversqliteDownloadApplyStore({
    required OversqliteLocalRowStore localStore,
    required OversqlitePushStateStore stateStore,
  }) : _localStore = localStore,
       _stateStore = stateStore;

  final OversqliteLocalRowStore _localStore;
  final OversqlitePushStateStore _stateStore;

  Future<void> applyAuthoritativeRow({
    required OversqliteValidatedConfig validated,
    required BundleRow row,
    required String keyJson,
    required String localPk,
  }) async {
    if (row.op == 'DELETE') {
      await _localStore.deleteLocalRow(row.table, localPk);
      await _stateStore.updateRowState(
        schema: validated.schema,
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
    await _localStore.upsertPayload(
      row.table,
      payload.cast<String, Object?>(),
      PayloadSource.authoritativeWire,
      requireCompletePayload: true,
    );
    await _stateStore.updateRowState(
      schema: validated.schema,
      table: row.table,
      keyJson: keyJson,
      rowVersion: row.rowVersion,
      deleted: false,
    );
  }
}

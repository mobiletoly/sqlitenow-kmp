import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';

import 'local_runtime.dart';
import 'local_row_store.dart';
import 'payload_codec.dart';
import 'protocol.dart';

final class OversqliteDownloadStageStore {
  const OversqliteDownloadStageStore({
    required SqliteNowConnection connection,
    required OversqliteLocalRowStore localStore,
  }) : _connection = connection,
       _localStore = localStore;

  final SqliteNowConnection _connection;
  final OversqliteLocalRowStore _localStore;

  Future<void> clearAllSnapshotStages() {
    return _connection.execute('DELETE FROM _sync_snapshot_stage');
  }

  Future<int> countSnapshotStageRows(String snapshotId) async {
    final rows = await _connection.select(
      'SELECT COUNT(*) FROM _sync_snapshot_stage WHERE snapshot_id = ?',
      (row) => row.readInt(0),
      parameters: [snapshotId],
    );
    return rows.single;
  }

  Future<void> stageSnapshotChunk({
    required OversqliteValidatedConfig validated,
    required SnapshotSession session,
    required SnapshotChunkResponse chunk,
    required int afterRowOrdinal,
  }) async {
    await _connection.transaction(() async {
      var rowOrdinal = afterRowOrdinal;
      for (final row in chunk.rows) {
        if (row.schema != validated.schema) {
          throw OversqliteProtocolException(
            'snapshot row schema ${row.schema} does not match client schema ${validated.schema}',
          );
        }
        final key = await _localStore.localKeyFromWire(row.table, row.key);
        rowOrdinal++;
        await _connection.execute(
          '''INSERT INTO _sync_snapshot_stage(
  snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload
) VALUES (?, ?, ?, ?, ?, ?, ?)''',
          parameters: [
            session.snapshotId,
            rowOrdinal,
            row.schema,
            row.table,
            key.keyJson,
            row.rowVersion,
            canonicalizeOversqliteProtocolJson(row.payload),
          ],
        );
      }
    }, mode: TransactionMode.immediate);
  }

  Future<List<OversqliteStagedSnapshotRow>> loadStagedSnapshotRows(
    String snapshotId,
  ) async {
    final rows = await _connection.select(
      '''SELECT table_name, key_json, row_version, payload
FROM _sync_snapshot_stage
WHERE snapshot_id = ?
ORDER BY row_ordinal''',
      (row) => _StoredStagedSnapshotRow(
        tableName: row.readString(0),
        keyJson: row.readString(1),
        rowVersion: row.readInt(2),
        payload: row.readString(3),
      ),
      parameters: [snapshotId],
    );
    final stagedRows = <OversqliteStagedSnapshotRow>[];
    for (final row in rows) {
      final table = await _localStore.tableInfo(row.tableName);
      final localPk = localPkFromOversqliteKeyJson(table, row.keyJson);
      stagedRows.add(
        OversqliteStagedSnapshotRow(
          tableName: row.tableName,
          keyJson: row.keyJson,
          localPk: localPk,
          wireKey: wireKeyFromOversqliteLocalKey(table, localPk),
          rowVersion: row.rowVersion,
          payload: row.payload,
        ),
      );
    }
    return stagedRows;
  }

  Future<void> deleteSnapshotStage(String snapshotId) {
    return _connection.execute(
      'DELETE FROM _sync_snapshot_stage WHERE snapshot_id = ?',
      parameters: [snapshotId],
    );
  }
}

final class OversqliteStagedSnapshotRow {
  const OversqliteStagedSnapshotRow({
    required this.tableName,
    required this.keyJson,
    required this.localPk,
    required this.wireKey,
    required this.rowVersion,
    required this.payload,
  });

  final String tableName;
  final String keyJson;
  final String localPk;
  final SyncKey wireKey;
  final int rowVersion;
  final String payload;
}

final class _StoredStagedSnapshotRow {
  const _StoredStagedSnapshotRow({
    required this.tableName,
    required this.keyJson,
    required this.rowVersion,
    required this.payload,
  });

  final String tableName;
  final String keyJson;
  final int rowVersion;
  final String payload;
}

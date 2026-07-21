import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';

import 'local_runtime.dart';
import 'local_row_store.dart';
import 'payload_codec.dart';
import 'protocol.dart';
import 'protocol_models.dart' show checkedAddOversqliteInt64;

final class OversqliteDownloadStageStore {
  OversqliteDownloadStageStore({
    required SqliteNowConnection connection,
    required OversqliteLocalRowStore localStore,
  }) : _connection = connection,
       _localStore = localStore;

  final SqliteNowConnection _connection;
  final OversqliteLocalRowStore _localStore;

  Future<void> Function()? afterMetadataPageLoadedForTest;

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

  Future<({int rowCount, int retainedTextBytes})> snapshotStageTotals(
    String snapshotId,
  ) async {
    final rows = await _connection.select(
      '''SELECT COUNT(*),
       COALESCE(SUM(
         length(CAST(schema_name AS BLOB)) +
         length(CAST(table_name AS BLOB)) +
         length(CAST(key_json AS BLOB)) +
         length(CAST(payload AS BLOB))
       ), 0)
FROM _sync_snapshot_stage
WHERE snapshot_id = ?''',
      (row) => (rowCount: row.readInt(0), retainedTextBytes: row.readInt(1)),
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
            'snapshot row '
            '${safeSyncTableDiagnosticIdentifier(row.schema, row.table)} '
            'does not match configured schema '
            '${safeSnapshotDiagnosticIdentifier(validated.schema)}',
          );
        }
        final configuredTables = validated.tables
            .map((table) => table.tableName)
            .toSet();
        if (!configuredTables.contains(row.table)) {
          throw OversqliteProtocolException(
            'snapshot row '
            '${safeSyncTableDiagnosticIdentifier(row.schema, row.table)} '
            'is not configured for sync',
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

  Future<OversqliteStagedSnapshotPage> loadStagedSnapshotPage({
    required OversqliteValidatedConfig validated,
    required String snapshotId,
    required int afterRowOrdinal,
    required int maxRows,
    required int maxBytes,
  }) async {
    if (maxRows <= 0 || maxBytes <= 0) {
      throw ArgumentError('snapshot apply page limits must be positive');
    }
    final metadata = await _connection.select(
      '''SELECT row_ordinal,
       length(CAST(schema_name AS BLOB))
       + length(CAST(table_name AS BLOB))
       + length(CAST(key_json AS BLOB))
       + length(CAST(payload AS BLOB)) AS staged_byte_count
FROM _sync_snapshot_stage
WHERE snapshot_id = ?
  AND row_ordinal > ?
ORDER BY row_ordinal
LIMIT ?''',
      (row) => _StagedSnapshotMetadata(
        rowOrdinal: row.readInt(0),
        retainedTextBytes: row.readInt(1),
      ),
      parameters: [snapshotId, afterRowOrdinal, maxRows],
    );
    if (metadata.isEmpty) {
      return const OversqliteStagedSnapshotPage(
        rows: [],
        retainedTextBytes: 0,
        metadataRowCount: 0,
        driverRowCount: 0,
      );
    }
    final selected = <_StagedSnapshotMetadata>[];
    var retainedTextBytes = 0;
    for (final item in metadata) {
      final nextBytes = checkedAddOversqliteInt64(
        retainedTextBytes,
        item.retainedTextBytes,
        'snapshot apply page byte total',
      );
      if (nextBytes > maxBytes) {
        if (selected.isEmpty) {
          throw SnapshotApplyRowTooLargeException(
            rowOrdinal: item.rowOrdinal,
            retainedTextBytes: item.retainedTextBytes,
            limit: maxBytes,
          );
        }
        break;
      }
      retainedTextBytes = nextBytes;
      selected.add(item);
    }
    final selectedLastOrdinal = selected.last.rowOrdinal;
    await afterMetadataPageLoadedForTest?.call();
    final storedRows = await _connection.select(
      '''SELECT row_ordinal, schema_name, table_name, key_json, row_version, payload
FROM _sync_snapshot_stage
WHERE snapshot_id = ?
  AND row_ordinal > ?
  AND row_ordinal <= ?
ORDER BY row_ordinal''',
      (row) => _StoredStagedSnapshotRow(
        rowOrdinal: row.readInt(0),
        schemaName: row.readString(1),
        tableName: row.readString(2),
        keyJson: row.readString(3),
        rowVersion: row.readInt(4),
        payload: row.readString(5),
      ),
      parameters: [snapshotId, afterRowOrdinal, selectedLastOrdinal],
    );
    if (storedRows.length != selected.length ||
        storedRows.isEmpty ||
        storedRows.last.rowOrdinal != selectedLastOrdinal) {
      throw const OversqliteProtocolException(
        'snapshot staged page changed between metadata and row load',
      );
    }
    final stagedRows = <OversqliteStagedSnapshotRow>[];
    final configuredTables = validated.tables
        .map((table) => table.tableName)
        .toSet();
    for (final row in storedRows) {
      try {
        if (row.schemaName != validated.schema) {
          throw OversqliteProtocolException(
            'staged snapshot row '
            '${safeSyncTableDiagnosticIdentifier(row.schemaName, row.tableName)} '
            'does not match configured schema '
            '${safeSnapshotDiagnosticIdentifier(validated.schema)}',
          );
        }
        if (!configuredTables.contains(row.tableName)) {
          throw OversqliteProtocolException(
            'staged snapshot row '
            '${safeSyncTableDiagnosticIdentifier(row.schemaName, row.tableName)} '
            'is not configured for sync',
          );
        }
        final table = await _localStore.tableInfo(row.tableName);
        final localPk = localPkFromOversqliteKeyJson(table, row.keyJson);
        stagedRows.add(
          OversqliteStagedSnapshotRow(
            rowOrdinal: row.rowOrdinal,
            schemaName: row.schemaName,
            tableName: row.tableName,
            keyJson: row.keyJson,
            localPk: localPk,
            wireKey: wireKeyFromOversqliteLocalKey(table, localPk),
            rowVersion: row.rowVersion,
            payload: row.payload,
          ),
        );
      } catch (_) {
        throw SnapshotRowApplyException(
          rowOrdinal: row.rowOrdinal,
          schemaName: row.schemaName,
          tableName: row.tableName,
        );
      }
    }
    return OversqliteStagedSnapshotPage(
      rows: stagedRows,
      retainedTextBytes: retainedTextBytes,
      metadataRowCount: metadata.length,
      driverRowCount: storedRows.length,
    );
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
    required this.rowOrdinal,
    required this.schemaName,
    required this.tableName,
    required this.keyJson,
    required this.localPk,
    required this.wireKey,
    required this.rowVersion,
    required this.payload,
  });

  final int rowOrdinal;
  final String schemaName;
  final String tableName;
  final String keyJson;
  final String localPk;
  final SyncKey wireKey;
  final int rowVersion;
  final String payload;
}

final class OversqliteStagedSnapshotPage {
  const OversqliteStagedSnapshotPage({
    required this.rows,
    required this.retainedTextBytes,
    required this.metadataRowCount,
    required this.driverRowCount,
  });

  final List<OversqliteStagedSnapshotRow> rows;
  final int retainedTextBytes;
  final int metadataRowCount;
  final int driverRowCount;

  int? get lastRowOrdinal => rows.isEmpty ? null : rows.last.rowOrdinal;
}

final class _StagedSnapshotMetadata {
  const _StagedSnapshotMetadata({
    required this.rowOrdinal,
    required this.retainedTextBytes,
  });

  final int rowOrdinal;
  final int retainedTextBytes;
}

final class _StoredStagedSnapshotRow {
  const _StoredStagedSnapshotRow({
    required this.rowOrdinal,
    required this.schemaName,
    required this.tableName,
    required this.keyJson,
    required this.rowVersion,
    required this.payload,
  });

  final int rowOrdinal;
  final String schemaName;
  final String tableName;
  final String keyJson;
  final int rowVersion;
  final String payload;
}

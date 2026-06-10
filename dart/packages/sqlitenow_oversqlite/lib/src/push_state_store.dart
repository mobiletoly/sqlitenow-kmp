import 'dart:convert';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';

import 'local_runtime.dart';
import 'local_row_store.dart';
import 'payload_codec.dart';
import 'protocol.dart';
import 'runtime_state_store.dart';

const pushOutboxStateNone = 'none';
const pushOutboxStatePrepared = 'prepared';
const pushOutboxStateCommittedRemote = 'committed_remote';

final class OversqlitePushStateStore {
  OversqlitePushStateStore({
    required SqliteNowConnection connection,
    required OversqliteApplyRunner applyRunner,
  }) : _connection = connection,
       _applyRunner = applyRunner,
       _localStore = OversqliteLocalRowStore(connection);

  final SqliteNowConnection _connection;
  final OversqliteApplyRunner _applyRunner;
  final OversqliteLocalRowStore _localStore;

  Future<List<OversqliteDirtyCapture>> loadDirtyRows(
    OversqliteValidatedConfig validated,
  ) async {
    final tableOrder = validated.tableOrder;
    final rows = await _connection.select(
      '''SELECT d.schema_name, d.table_name, d.key_json, d.base_row_version, d.payload,
       d.dirty_ordinal,
       CASE WHEN rs.key_json IS NULL THEN 0 ELSE 1 END,
       COALESCE(rs.deleted, 0)
FROM _sync_dirty_rows d
LEFT JOIN _sync_row_state rs
  ON rs.schema_name = d.schema_name
 AND rs.table_name = d.table_name
 AND rs.key_json = d.key_json
ORDER BY d.dirty_ordinal, d.table_name, d.key_json''',
      (row) => _DirtySnapshot(
        schemaName: row.readString(0),
        tableName: row.readString(1),
        keyJson: row.readString(2),
        baseRowVersion: row.readInt(3),
        payload: row.readNullableString(4),
        dirtyOrdinal: row.readInt(5),
        stateExists: row.readInt(6) == 1,
        stateDeleted: row.readInt(7) == 1,
      ),
    );
    final captures = <OversqliteDirtyCapture>[];
    for (final row in rows) {
      final table = await _localStore.tableInfo(row.tableName);
      final localPk = localPkFromOversqliteKeyJson(table, row.keyJson);
      if (row.payload == null && (!row.stateExists || row.stateDeleted)) {
        continue;
      }
      final op = row.payload == null
          ? 'DELETE'
          : row.stateExists && !row.stateDeleted
          ? 'UPDATE'
          : 'INSERT';
      captures.add(
        OversqliteDirtyCapture(
          schemaName: row.schemaName,
          tableName: row.tableName,
          keyJson: row.keyJson,
          localPk: localPk,
          op: op,
          baseRowVersion: row.baseRowVersion,
          localPayload: row.payload,
          dirtyOrdinal: row.dirtyOrdinal,
        ),
      );
    }
    captures.sort((left, right) {
      final leftDelete = left.op == 'DELETE';
      final rightDelete = right.op == 'DELETE';
      if (leftDelete != rightDelete) {
        return leftDelete ? 1 : -1;
      }
      final leftOrder = tableOrder[left.tableName] ?? 1 << 30;
      final rightOrder = tableOrder[right.tableName] ?? 1 << 30;
      if (leftOrder != rightOrder) {
        return leftDelete ? rightOrder - leftOrder : leftOrder - rightOrder;
      }
      return left.dirtyOrdinal.compareTo(right.dirtyOrdinal);
    });
    return captures;
  }

  Future<void> updateRowState({
    required String schema,
    required String table,
    required String keyJson,
    required int rowVersion,
    required bool deleted,
  }) async {
    await _connection.execute(
      '''INSERT INTO _sync_row_state(schema_name, table_name, key_json, row_version, deleted, updated_at)
VALUES(?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
  row_version = excluded.row_version,
  deleted = excluded.deleted,
  updated_at = excluded.updated_at''',
      parameters: [schema, table, keyJson, rowVersion, deleted ? 1 : 0],
    );
  }

  Future<int?> loadRowVersion({
    required String schema,
    required String table,
    required String keyJson,
  }) async {
    final rows = await _connection.select(
      '''SELECT row_version
FROM _sync_row_state
WHERE schema_name = ? AND table_name = ? AND key_json = ?''',
      (row) => row.readInt(0),
      parameters: [schema, table, keyJson],
    );
    return rows.isEmpty ? null : rows.single;
  }

  Future<void> clearRowStateForTable({
    required String schema,
    required String table,
  }) {
    return _connection.execute(
      'DELETE FROM _sync_row_state WHERE schema_name = ? AND table_name = ?',
      parameters: [schema, table],
    );
  }

  Future<void> requeueDirty({
    required String schema,
    required String table,
    required String keyJson,
    required String op,
    required int baseRowVersion,
    required String? payload,
  }) async {
    await _connection.execute(
      '''INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal, updated_at)
VALUES (
  ?, ?, ?, ?, ?, ?,
  COALESCE(
    (SELECT dirty_ordinal FROM _sync_dirty_rows WHERE schema_name = ? AND table_name = ? AND key_json = ?),
    (SELECT COALESCE(MAX(dirty_ordinal), 0) + 1 FROM _sync_dirty_rows)
  ),
  strftime('%Y-%m-%dT%H:%M:%fZ','now')
)
ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
  op = excluded.op,
  base_row_version = excluded.base_row_version,
  payload = excluded.payload,
  updated_at = excluded.updated_at''',
      parameters: [
        schema,
        table,
        keyJson,
        op,
        baseRowVersion,
        payload,
        schema,
        table,
        keyJson,
      ],
    );
  }

  Future<void> deleteDirty({
    required String schema,
    required String table,
    required String keyJson,
  }) {
    return _connection.execute(
      'DELETE FROM _sync_dirty_rows WHERE schema_name = ? AND table_name = ? AND key_json = ?',
      parameters: [schema, table, keyJson],
    );
  }

  Future<void> clearDirtyRows() {
    return _connection.execute('DELETE FROM _sync_dirty_rows');
  }

  Future<int> countDirtyRows() {
    return scalarInt('SELECT COUNT(*) FROM _sync_dirty_rows');
  }

  Future<int> nextSourceBundleId(String sourceId) async {
    final rows = await _connection.select(
      'SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = ?',
      (row) => row.readInt(0),
      parameters: [sourceId],
    );
    return rows.single;
  }

  Future<OversqliteOutboxBundle> loadOutboxBundle() async {
    final rows = await _connection.select(
      '''SELECT state, source_id, source_bundle_id, initialization_id,
       canonical_request_hash, row_count, remote_bundle_hash, remote_bundle_seq
FROM _sync_outbox_bundle
WHERE singleton_key = 1''',
      (row) => OversqliteOutboxBundle(
        state: row.readString(0),
        sourceId: row.readString(1),
        sourceBundleId: row.readInt(2),
        initializationId: row.readString(3),
        canonicalRequestHash: row.readString(4),
        rowCount: row.readInt(5),
        remoteBundleHash: row.readString(6),
        remoteBundleSeq: row.readInt(7),
      ),
    );
    return rows.single;
  }

  Future<int> countOutboxRows() {
    return scalarInt('SELECT COUNT(*) FROM _sync_outbox_rows');
  }

  Future<OversqlitePushSnapshot> loadPersistedSnapshot(
    OversqliteOutboxBundle outbox,
  ) async {
    if (outbox.state == pushOutboxStateCommittedRemote) {
      if (outbox.remoteBundleSeq <= 0) {
        throw StateError(
          'committed_remote outbox is missing remote_bundle_seq',
        );
      }
      if (outbox.remoteBundleHash.trim().isEmpty) {
        throw StateError(
          'committed_remote outbox is missing remote_bundle_hash',
        );
      }
    }
    final rows = await _connection.select(
      '''SELECT source_bundle_id, row_ordinal, schema_name, table_name, key_json,
       wire_key_json, op, base_row_version, local_payload, wire_payload
FROM _sync_outbox_rows
ORDER BY source_bundle_id, row_ordinal''',
      (row) => OversqliteOutboxRow(
        sourceBundleId: row.readInt(0),
        rowOrdinal: row.readInt(1),
        schemaName: row.readString(2),
        tableName: row.readString(3),
        keyJson: row.readString(4),
        wireKeyJson: row.readString(5),
        op: row.readString(6),
        baseRowVersion: row.readInt(7),
        localPayload: row.readNullableString(8),
        wirePayload: row.readNullableString(9),
      ),
    );
    if (rows.isEmpty) {
      throw StateError(
        '_sync_outbox_bundle state ${outbox.state} exists without rows',
      );
    }
    if (rows.any((row) => row.sourceBundleId != outbox.sourceBundleId)) {
      throw StateError(
        '_sync_outbox_rows source_bundle_id does not match _sync_outbox_bundle',
      );
    }
    if (rows.length != outbox.rowCount) {
      throw StateError(
        '_sync_outbox_rows row count ${rows.length} does not match _sync_outbox_bundle ${outbox.rowCount}',
      );
    }
    final canonicalRequestHash = _computeCanonicalRequestHash(rows);
    if (canonicalRequestHash != outbox.canonicalRequestHash) {
      throw StateError(
        'persisted canonical outbox hash $canonicalRequestHash does not match expected ${outbox.canonicalRequestHash}',
      );
    }
    return OversqlitePushSnapshot(
      state: outbox.state,
      sourceId: outbox.sourceId,
      sourceBundleId: outbox.sourceBundleId,
      initializationId: outbox.initializationId,
      canonicalRequestHash: canonicalRequestHash,
      remoteBundleHash: outbox.remoteBundleHash,
      remoteBundleSeq: outbox.remoteBundleSeq,
      rows: rows,
    );
  }

  Future<void> persistOutboxBundle(OversqliteOutboxBundle bundle) {
    return _connection.execute(
      '''UPDATE _sync_outbox_bundle
SET state = ?,
    source_id = ?,
    source_bundle_id = ?,
    initialization_id = ?,
    canonical_request_hash = ?,
    row_count = ?,
    remote_bundle_hash = ?,
    remote_bundle_seq = ?
WHERE singleton_key = 1''',
      parameters: [
        bundle.state,
        bundle.sourceId,
        bundle.sourceBundleId,
        bundle.initializationId,
        bundle.canonicalRequestHash,
        bundle.rowCount,
        bundle.remoteBundleHash,
        bundle.remoteBundleSeq,
      ],
    );
  }

  Future<void> persistCommittedRemote(
    OversqlitePushSnapshot snapshot,
    OversqliteCommittedPush committed,
  ) {
    return persistOutboxBundle(
      OversqliteOutboxBundle(
        state: pushOutboxStateCommittedRemote,
        sourceId: committed.sourceId,
        sourceBundleId: snapshot.sourceBundleId,
        initializationId: snapshot.initializationId,
        canonicalRequestHash: snapshot.canonicalRequestHash,
        rowCount: snapshot.rows.length,
        remoteBundleHash: committed.bundleHash,
        remoteBundleSeq: committed.bundleSeq,
      ),
    );
  }

  Future<void> insertOutboxRow(OversqliteOutboxRow row) {
    return _connection.execute(
      '''INSERT INTO _sync_outbox_rows(
  source_bundle_id, row_ordinal, schema_name, table_name, key_json,
  wire_key_json, op, base_row_version, local_payload, wire_payload
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
      parameters: [
        row.sourceBundleId,
        row.rowOrdinal,
        row.schemaName,
        row.tableName,
        row.keyJson,
        row.wireKeyJson,
        row.op,
        row.baseRowVersion,
        row.localPayload,
        row.wirePayload,
      ],
    );
  }

  Future<void> clearOutbox() async {
    await _connection.execute('DELETE FROM _sync_outbox_rows');
    await persistOutboxBundle(const OversqliteOutboxBundle());
  }

  Future<void> rebindOutboxSource({
    required String sourceId,
    required int sourceBundleId,
  }) async {
    await _connection.execute(
      '''UPDATE _sync_outbox_bundle
SET source_id = ?,
    source_bundle_id = ?
WHERE singleton_key = 1''',
      parameters: [sourceId, sourceBundleId],
    );
    await _connection.execute(
      'UPDATE _sync_outbox_rows SET source_bundle_id = ?',
      parameters: [sourceBundleId],
    );
  }

  Future<OversqliteDirtyUploadState?> loadDirtyUploadState({
    required String schema,
    required String table,
    required String keyJson,
  }) async {
    final rows = await _connection.select(
      '''SELECT op, base_row_version, payload
FROM _sync_dirty_rows
WHERE schema_name = ? AND table_name = ? AND key_json = ?''',
      (row) => OversqliteDirtyUploadState(
        op: row.readString(0),
        baseRowVersion: row.readInt(1),
        payload: row.readNullableString(2),
      ),
      parameters: [schema, table, keyJson],
    );
    return rows.isEmpty ? null : rows.single;
  }

  Future<void> restoreSnapshotToDirtyRows(
    OversqlitePushSnapshot snapshot,
  ) async {
    await _applyRunner.inApplyModeTransaction(() async {
      for (final row in snapshot.rows) {
        await requeueDirty(
          schema: row.schemaName,
          table: row.tableName,
          keyJson: row.keyJson,
          op: row.op,
          baseRowVersion: row.baseRowVersion,
          payload: row.localPayload,
        );
      }
      await clearOutbox();
    });
  }

  Future<int> scalarInt(String sql) async {
    final rows = await _connection.select(sql, (row) => row.readInt(0));
    return rows.single;
  }
}

final class OversqlitePushSnapshot {
  const OversqlitePushSnapshot({
    required this.state,
    required this.sourceId,
    required this.sourceBundleId,
    required this.initializationId,
    required this.canonicalRequestHash,
    required this.remoteBundleHash,
    required this.remoteBundleSeq,
    required this.rows,
  });

  final String state;
  final String sourceId;
  final int sourceBundleId;
  final String initializationId;
  final String canonicalRequestHash;
  final String remoteBundleHash;
  final int remoteBundleSeq;
  final List<OversqliteOutboxRow> rows;
}

final class OversqliteCommittedPush {
  const OversqliteCommittedPush({
    required this.bundleSeq,
    required this.sourceId,
    required this.sourceBundleId,
    required this.rowCount,
    required this.bundleHash,
    required this.requiresStrictOutboxMatch,
  });

  final int bundleSeq;
  final String sourceId;
  final int sourceBundleId;
  final int rowCount;
  final String bundleHash;
  final bool requiresStrictOutboxMatch;
}

final class OversqliteOutboxBundle {
  const OversqliteOutboxBundle({
    this.state = pushOutboxStateNone,
    this.sourceId = '',
    this.sourceBundleId = 0,
    this.initializationId = '',
    this.canonicalRequestHash = '',
    this.rowCount = 0,
    this.remoteBundleHash = '',
    this.remoteBundleSeq = 0,
  });

  final String state;
  final String sourceId;
  final int sourceBundleId;
  final String initializationId;
  final String canonicalRequestHash;
  final int rowCount;
  final String remoteBundleHash;
  final int remoteBundleSeq;
}

final class OversqliteOutboxRow {
  const OversqliteOutboxRow({
    required this.sourceBundleId,
    required this.rowOrdinal,
    required this.schemaName,
    required this.tableName,
    required this.keyJson,
    required this.wireKeyJson,
    required this.op,
    required this.baseRowVersion,
    required this.localPayload,
    required this.wirePayload,
  });

  final int sourceBundleId;
  final int rowOrdinal;
  final String schemaName;
  final String tableName;
  final String keyJson;
  final String wireKeyJson;
  final String op;
  final int baseRowVersion;
  final String? localPayload;
  final String? wirePayload;

  PushRequestRow toPushRequestRow() {
    return PushRequestRow(
      schema: schemaName,
      table: tableName,
      key: syncKeyFromOversqliteJson(wireKeyJson),
      op: op,
      baseRowVersion: baseRowVersion,
      payload: wirePayload == null ? null : jsonDecode(wirePayload!),
    );
  }

  Map<String, Object?> canonicalRequestJson() {
    final json = <String, Object?>{
      'row_ordinal': rowOrdinal,
      'schema': schemaName,
      'table': tableName,
      'key': syncKeyFromOversqliteJson(wireKeyJson),
      'op': op,
      'base_row_version': baseRowVersion,
    };
    if (wirePayload != null) {
      json['payload'] = jsonDecode(wirePayload!);
    }
    return json;
  }
}

final class _DirtySnapshot {
  const _DirtySnapshot({
    required this.schemaName,
    required this.tableName,
    required this.keyJson,
    required this.baseRowVersion,
    required this.payload,
    required this.dirtyOrdinal,
    required this.stateExists,
    required this.stateDeleted,
  });

  final String schemaName;
  final String tableName;
  final String keyJson;
  final int baseRowVersion;
  final String? payload;
  final int dirtyOrdinal;
  final bool stateExists;
  final bool stateDeleted;
}

final class OversqliteDirtyCapture {
  const OversqliteDirtyCapture({
    required this.schemaName,
    required this.tableName,
    required this.keyJson,
    required this.localPk,
    required this.op,
    required this.baseRowVersion,
    required this.localPayload,
    required this.dirtyOrdinal,
  });

  final String schemaName;
  final String tableName;
  final String keyJson;
  final String localPk;
  final String op;
  final int baseRowVersion;
  final String? localPayload;
  final int dirtyOrdinal;
}

final class OversqliteDirtyUploadState {
  const OversqliteDirtyUploadState({
    required this.op,
    required this.baseRowVersion,
    required this.payload,
  });

  final String op;
  final int baseRowVersion;
  final String? payload;
}

String _computeCanonicalRequestHash(List<OversqliteOutboxRow> rows) {
  if (rows.isEmpty) {
    return '';
  }
  return sha256Hex(
    canonicalizeOversqliteProtocolJson([
      for (final row in rows) row.canonicalRequestJson(),
    ]),
  );
}

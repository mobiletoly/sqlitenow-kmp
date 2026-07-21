import 'wire_int64.dart';

const _hiddenSyncScopeColumnName = '_sync_scope_id';
const oversqliteProtocolVersion = 'v1';
const maxOversqliteInt64 = maxWireOversqliteInt64;

final class ProtocolVersionMismatchException implements Exception {
  const ProtocolVersionMismatchException({
    this.expected = oversqliteProtocolVersion,
    required this.actual,
  });

  final String expected;
  final String actual;

  @override
  String toString() =>
      'oversqlite protocol version mismatch: expected "$expected", actual "$actual"';
}

final class SyncKeyContractMismatch {
  SyncKeyContractMismatch({
    required this.qualifiedTable,
    required List<String> clientSyncKeyColumns,
    required List<String> serverSyncKeyColumns,
  }) : clientSyncKeyColumns = List.unmodifiable(clientSyncKeyColumns),
       serverSyncKeyColumns = List.unmodifiable(serverSyncKeyColumns);

  final String qualifiedTable;
  final List<String> clientSyncKeyColumns;
  final List<String> serverSyncKeyColumns;
}

final class SyncTableContractMismatchException implements Exception {
  SyncTableContractMismatchException({
    required Iterable<String> serverOnlyTables,
    required Iterable<String> clientOnlyTables,
    required Iterable<SyncKeyContractMismatch> syncKeyMismatches,
  }) : serverOnlyTables = List.unmodifiable(
         serverOnlyTables.toSet().toList()..sort(),
       ),
       clientOnlyTables = List.unmodifiable(
         clientOnlyTables.toSet().toList()..sort(),
       ),
       syncKeyMismatches = List.unmodifiable(
         syncKeyMismatches.toList()..sort(
           (left, right) => left.qualifiedTable.compareTo(right.qualifiedTable),
         ),
       );

  final List<String> serverOnlyTables;
  final List<String> clientOnlyTables;
  final List<SyncKeyContractMismatch> syncKeyMismatches;

  @override
  String toString() {
    final safeServerOnly = serverOnlyTables
        .map(_safeQualifiedTableDiagnosticIdentifier)
        .toList();
    final safeClientOnly = clientOnlyTables
        .map(_safeQualifiedTableDiagnosticIdentifier)
        .toList();
    final safeKeyMismatches = syncKeyMismatches.map((mismatch) {
      final clientKeys = mismatch.clientSyncKeyColumns
          .map(safeSnapshotDiagnosticIdentifier)
          .toList();
      final serverKeys = mismatch.serverSyncKeyColumns
          .map(safeSnapshotDiagnosticIdentifier)
          .toList();
      return '${_safeQualifiedTableDiagnosticIdentifier(mismatch.qualifiedTable)}'
          '(client=$clientKeys,server=$serverKeys)';
    }).toList();
    return 'oversqlite sync table contract mismatch: '
        'server_only=$safeServerOnly, client_only=$safeClientOnly, '
        'sync_key_mismatches=$safeKeyMismatches';
  }
}

String _safeQualifiedTableDiagnosticIdentifier(String value) {
  final parts = value.split('.');
  if (parts.length != 2) return '<redacted>.<redacted>';
  return '${safeSnapshotDiagnosticIdentifier(parts[0])}.'
      '${safeSnapshotDiagnosticIdentifier(parts[1])}';
}

String safeSyncTableDiagnosticIdentifier(String schema, String table) =>
    '${safeSnapshotDiagnosticIdentifier(schema)}.'
    '${safeSnapshotDiagnosticIdentifier(table)}';

final class OversqliteProtocolException implements Exception {
  const OversqliteProtocolException(this.message);

  final String message;

  @override
  String toString() => message;
}

final class SnapshotCapabilitiesException implements Exception {
  const SnapshotCapabilitiesException(this.message);

  final String message;

  @override
  String toString() => message;
}

final class SnapshotResponseBodyTooLargeException implements Exception {
  const SnapshotResponseBodyTooLargeException({
    required this.operation,
    required this.limit,
  });

  final String operation;
  final int limit;

  @override
  String toString() => '$operation decoded response body exceeds $limit bytes';
}

final class SnapshotUnsupportedContentEncodingException implements Exception {
  const SnapshotUnsupportedContentEncodingException({required this.operation});

  final String operation;

  @override
  String toString() => '$operation response uses unsupported content encoding';
}

final class SnapshotResponseDecodeException implements Exception {
  const SnapshotResponseDecodeException(this.operation);

  final String operation;

  @override
  String toString() => '$operation response could not be decoded';
}

enum SnapshotSemanticFailure {
  duplicateObjectMember,
  excessiveNesting,
  invalidSession,
  invalidChunk,
  invalidRow,
}

final class SnapshotSemanticException implements Exception {
  const SnapshotSemanticException(this.failure);

  final SnapshotSemanticFailure failure;

  @override
  String toString() =>
      'snapshot response failed semantic validation: ${failure.name}';
}

final class InvalidOversqliteSourceIdException implements Exception {
  const InvalidOversqliteSourceIdException();

  @override
  String toString() =>
      'oversqlite source id must contain one or more visible ASCII characters';
}

final class SnapshotChunkTooSmallException implements Exception {
  const SnapshotChunkTooSmallException({
    required this.configuredBytes,
    required this.requiredBytes,
  });

  final int configuredBytes;
  final int requiredBytes;

  @override
  String toString() =>
      'snapshot chunk byte budget $configuredBytes is too small for the next row requiring $requiredBytes bytes; increase snapshotChunkBytes';
}

final class SnapshotFinalApplyGateException implements Exception {
  const SnapshotFinalApplyGateException({
    required this.mode,
    required this.reason,
    required this.cause,
  });

  final String mode;
  final String reason;
  final Object cause;

  @override
  String toString() => 'snapshot final apply gate rejected $mode mode: $reason';
}

final class SnapshotRowApplyException implements Exception {
  const SnapshotRowApplyException({
    required this.rowOrdinal,
    required this.schemaName,
    required this.tableName,
  });

  final int rowOrdinal;
  final String schemaName;
  final String tableName;

  @override
  String toString() =>
      'failed to apply snapshot row ordinal=$rowOrdinal '
      'schema=${safeSnapshotDiagnosticIdentifier(schemaName)} '
      'table=${safeSnapshotDiagnosticIdentifier(tableName)}';
}

final class SnapshotApplyRowTooLargeException implements Exception {
  const SnapshotApplyRowTooLargeException({
    required this.rowOrdinal,
    required this.retainedTextBytes,
    required this.limit,
  });

  final int rowOrdinal;
  final int retainedTextBytes;
  final int limit;

  @override
  String toString() =>
      'snapshot staged row ordinal=$rowOrdinal requires '
      '$retainedTextBytes retained text bytes, exceeding limit $limit';
}

String safeSnapshotDiagnosticIdentifier(String value) {
  if (value.isEmpty || value.length > 63) return '<redacted>';
  final first = value.codeUnitAt(0);
  final validFirst =
      first == 0x5f ||
      (first >= 0x41 && first <= 0x5a) ||
      (first >= 0x61 && first <= 0x7a);
  if (!validFirst) return '<redacted>';
  for (var index = 1; index < value.length; index++) {
    final code = value.codeUnitAt(index);
    final valid =
        code == 0x5f ||
        (code >= 0x30 && code <= 0x39) ||
        (code >= 0x41 && code <= 0x5a) ||
        (code >= 0x61 && code <= 0x7a);
    if (!valid) return '<redacted>';
  }
  return value;
}

final class SnapshotSessionLimitExceededException implements Exception {
  const SnapshotSessionLimitExceededException({
    required this.dimension,
    required this.actual,
    required this.limit,
  });

  final String dimension;
  final int actual;
  final int limit;

  @override
  String toString() =>
      'snapshot session exceeds server $dimension limit: actual=$actual limit=$limit';
}

enum SourceRecoveryReason {
  historyPruned,
  sourceSequenceOutOfOrder,
  sourceSequenceChanged,
  sourceRetired,
}

extension SourceRecoveryReasonWireName on SourceRecoveryReason {
  String get wireName {
    return switch (this) {
      SourceRecoveryReason.historyPruned => 'history_pruned',
      SourceRecoveryReason.sourceSequenceOutOfOrder =>
        'source_sequence_out_of_order',
      SourceRecoveryReason.sourceSequenceChanged => 'source_sequence_changed',
      SourceRecoveryReason.sourceRetired => 'source_retired',
    };
  }
}

final class RebuildRequiredException implements Exception {
  const RebuildRequiredException();

  @override
  String toString() => 'client checkpoint recovery is in progress';
}

final class CheckpointAheadException implements Exception {
  const CheckpointAheadException(this.message);

  final String message;

  @override
  String toString() => message;
}

enum CheckpointRecoveryBlockedReason { uploadPaused, pendingWork, pushFailed }

final class CheckpointRecoveryBlockedException implements Exception {
  const CheckpointRecoveryBlockedException({
    required this.reason,
    required this.dirtyCount,
    required this.outboundCount,
    required this.replayState,
    this.cause,
  });

  final CheckpointRecoveryBlockedReason reason;
  final int dirtyCount;
  final int outboundCount;
  final String replayState;
  final Object? cause;

  @override
  String toString() =>
      'checkpoint recovery is blocked ($reason): dirty_rows=$dirtyCount '
      'outbox_rows=$outboundCount replay_state="$replayState"'
      '${cause == null ? '' : ': $cause'}';
}

final class SourceSequenceMismatchException implements Exception {
  const SourceSequenceMismatchException(this.message);

  final String message;

  @override
  String toString() => message;
}

final class SourceRecoveryRequiredException implements Exception {
  const SourceRecoveryRequiredException(this.reason);

  final SourceRecoveryReason reason;

  @override
  String toString() =>
      'source recovery is required (${reason.wireName}); run rebuild() before syncing';
}

final class SourceReplacementDivergedException implements Exception {
  const SourceReplacementDivergedException();

  @override
  String toString() =>
      'replacement source diverged between local and server recovery state';
}

final class SourceRecoveryRequiredHttpException implements Exception {
  const SourceRecoveryRequiredHttpException({required this.reason});

  final SourceRecoveryReason reason;

  @override
  String toString() =>
      'source recovery is required (${reason.wireName}); run rebuild() before syncing';
}

final class _SourceRecoveryRequiredHttpSignal
    extends SourceRecoveryRequiredHttpException {
  const _SourceRecoveryRequiredHttpSignal(
    SourceRecoveryReason reason,
    this._replacementSourceId,
  ) : super(reason: reason);

  final String? _replacementSourceId;
}

SourceRecoveryRequiredHttpException sourceRecoveryRequiredHttpSignal({
  required SourceRecoveryReason reason,
  String? replacementSourceId,
}) => _SourceRecoveryRequiredHttpSignal(reason, replacementSourceId);

String? sourceRecoveryReplacementSourceId(
  SourceRecoveryRequiredHttpException error,
) => error is _SourceRecoveryRequiredHttpSignal
    ? error._replacementSourceId
    : null;

final class CommittedReplayPrunedException implements Exception {
  const CommittedReplayPrunedException(this.body);

  final String body;

  @override
  String toString() => body;
}

final class CommittedBundleNotFoundException implements Exception {
  const CommittedBundleNotFoundException(this.body);

  final String body;

  @override
  String toString() => body;
}

final class HistoryPrunedException implements Exception {
  const HistoryPrunedException(this.message);

  final String message;

  @override
  String toString() => message;
}

final class SourceReplacementInvalidException implements Exception {
  const SourceReplacementInvalidException(this.message);

  final String message;

  @override
  String toString() => message;
}

final class ConnectLifecycleUnsupportedException implements Exception {
  const ConnectLifecycleUnsupportedException([this.reason]);

  final String? reason;

  @override
  String toString() {
    final detail = reason;
    if (detail == null || detail.trim().isEmpty) {
      return 'server does not support the oversqlite connect lifecycle';
    }
    return 'server does not support the oversqlite connect lifecycle: $detail';
  }
}

final class ConnectBindingConflictException implements Exception {
  const ConnectBindingConflictException({
    required this.attachedUserId,
    required this.requestedUserId,
  });

  final String attachedUserId;
  final String requestedUserId;

  @override
  String toString() =>
      'local database is already attached to user "$attachedUserId"; detach before attaching user "$requestedUserId"';
}

final class ConnectLocalStateConflictException implements Exception {
  const ConnectLocalStateConflictException(this.reason);

  final String reason;

  @override
  String toString() =>
      'local sync state is incompatible with the requested attach lifecycle: $reason';
}

final class RemoteDataTransferDeferredException implements Exception {
  const RemoteDataTransferDeferredException(this.reason);

  final String reason;

  @override
  String toString() => reason;
}

final class ConnectRequest {
  const ConnectRequest({required this.hasLocalPendingRows});

  final bool hasLocalPendingRows;

  Map<String, Object?> toJson() {
    return {'has_local_pending_rows': hasLocalPendingRows};
  }
}

final class ConnectResponse {
  const ConnectResponse({
    required this.resolution,
    this.initializationId = '',
    this.leaseExpiresAt = '',
    this.retryAfterSeconds = 0,
  });

  final String resolution;
  final String initializationId;
  final String leaseExpiresAt;
  final int retryAfterSeconds;

  factory ConnectResponse.fromJson(Map<String, Object?> json) {
    final resolution = json['resolution'];
    if (resolution is! String || resolution.trim().isEmpty) {
      throw const OversqliteProtocolException(
        'connect response is missing resolution',
      );
    }
    return ConnectResponse(
      resolution: resolution,
      initializationId: (json['initialization_id'] as String?) ?? '',
      leaseExpiresAt: (json['lease_expires_at'] as String?) ?? '',
      retryAfterSeconds: (json['retry_after_seconds'] as int?) ?? 0,
    );
  }
}

typedef SyncKey = Map<String, String>;

final class PushRequestRow {
  const PushRequestRow({
    required this.schema,
    required this.table,
    required this.key,
    required this.op,
    required this.baseRowVersion,
    this.payload,
  });

  final String schema;
  final String table;
  final SyncKey key;
  final String op;
  final int baseRowVersion;
  final Object? payload;

  Map<String, Object?> toJson() {
    final json = <String, Object?>{
      'schema': schema,
      'table': table,
      'key': key,
      'op': op,
      'base_row_version': baseRowVersion,
    };
    if (payload != null) {
      json['payload'] = payload;
    }
    return json;
  }
}

final class BundleRow {
  const BundleRow({
    required this.schema,
    required this.table,
    required this.key,
    required this.op,
    required this.rowVersion,
    this.payload,
  });

  final String schema;
  final String table;
  final SyncKey key;
  final String op;
  final int rowVersion;
  final Object? payload;

  factory BundleRow.fromJson(Map<String, Object?> json) {
    final row = BundleRow(
      schema: json['schema']! as String,
      table: json['table']! as String,
      key: (json['key']! as Map).cast<String, String>(),
      op: json['op']! as String,
      rowVersion: json['row_version']! as int,
      payload: json['payload'],
    );
    _validateBundleRow(row);
    return row;
  }
}

final class Bundle {
  const Bundle({
    required this.bundleSeq,
    required this.sourceId,
    required this.sourceBundleId,
    required this.rows,
  });

  final int bundleSeq;
  final String sourceId;
  final int sourceBundleId;
  final List<BundleRow> rows;

  factory Bundle.fromJson(Map<String, Object?> json) {
    final bundle = Bundle(
      bundleSeq: json['bundle_seq']! as int,
      sourceId: json['source_id']! as String,
      sourceBundleId: json['source_bundle_id']! as int,
      rows: (json['rows']! as List<Object?>)
          .cast<Map<String, Object?>>()
          .map(BundleRow.fromJson)
          .toList(),
    );
    _validateBundle(bundle);
    return bundle;
  }
}

final class PullResponse {
  const PullResponse({
    required this.stableBundleSeq,
    required this.bundles,
    required this.hasMore,
  });

  final int stableBundleSeq;
  final List<Bundle> bundles;
  final bool hasMore;

  factory PullResponse.fromJson(Map<String, Object?> json) {
    return PullResponse(
      stableBundleSeq: json['stable_bundle_seq']! as int,
      bundles: (json['bundles']! as List<Object?>)
          .cast<Map<String, Object?>>()
          .map(Bundle.fromJson)
          .toList(),
      hasMore: json['has_more']! as bool,
    );
  }
}

final class SnapshotRow {
  const SnapshotRow({
    required this.schema,
    required this.table,
    required this.key,
    required this.rowVersion,
    required this.payload,
  });

  final String schema;
  final String table;
  final SyncKey key;
  final int rowVersion;
  final Object? payload;

  factory SnapshotRow.fromJson(Map<String, Object?> json) {
    try {
      final row = SnapshotRow(
        schema: json['schema']! as String,
        table: json['table']! as String,
        key: (json['key']! as Map).cast<String, String>(),
        rowVersion: json['row_version']! as int,
        payload: json['payload'],
      );
      validateSnapshotRow(row);
      return row;
    } on SnapshotSemanticException {
      rethrow;
    } catch (_) {
      throw const SnapshotSemanticException(SnapshotSemanticFailure.invalidRow);
    }
  }
}

final class SnapshotSession {
  const SnapshotSession({
    required this.snapshotId,
    required this.snapshotBundleSeq,
    required this.rowCount,
    required this.byteCount,
    required this.expiresAt,
  });

  final String snapshotId;
  final int snapshotBundleSeq;
  final int rowCount;
  final int byteCount;
  final String expiresAt;

  factory SnapshotSession.fromJson(Map<String, Object?> json) {
    final snapshotId = json['snapshot_id'];
    final expiresAt = json['expires_at'];
    if (snapshotId is! String || expiresAt is! String) {
      throw const SnapshotSemanticException(
        SnapshotSemanticFailure.invalidSession,
      );
    }
    final session = SnapshotSession(
      snapshotId: snapshotId,
      snapshotBundleSeq: readWireOversqliteInt64(
        json,
        'snapshot_bundle_seq',
        required: true,
        error: OversqliteProtocolException.new,
      ),
      rowCount: readWireOversqliteInt64(
        json,
        'row_count',
        required: true,
        error: OversqliteProtocolException.new,
      ),
      byteCount: readWireOversqliteInt64(
        json,
        'byte_count',
        required: true,
        error: OversqliteProtocolException.new,
      ),
      expiresAt: expiresAt,
    );
    _validateSnapshotSession(session);
    return session;
  }
}

final class SnapshotSourceReplacement {
  const SnapshotSourceReplacement({
    required this.previousSourceId,
    required this.newSourceId,
    required this.reason,
  });

  final String previousSourceId;
  final String newSourceId;
  final String reason;

  Map<String, Object?> toJson() {
    return {
      'previous_source_id': previousSourceId,
      'new_source_id': newSourceId,
      'reason': reason,
    };
  }
}

final class SnapshotSessionCreateRequest {
  const SnapshotSessionCreateRequest({this.sourceReplacement});

  final SnapshotSourceReplacement? sourceReplacement;

  Map<String, Object?> toJson() {
    return {
      if (sourceReplacement != null)
        'source_replacement': sourceReplacement!.toJson(),
    };
  }
}

final class SnapshotChunkResponse {
  const SnapshotChunkResponse({
    required this.snapshotId,
    required this.snapshotBundleSeq,
    required this.rows,
    required this.nextRowOrdinal,
    required this.hasMore,
    required this.byteCount,
  });

  final String snapshotId;
  final int snapshotBundleSeq;
  final List<SnapshotRow> rows;
  final int nextRowOrdinal;
  final bool hasMore;
  final int byteCount;

  factory SnapshotChunkResponse.fromJson(Map<String, Object?> json) {
    return SnapshotChunkResponse(
      snapshotId: json['snapshot_id']! as String,
      snapshotBundleSeq: json['snapshot_bundle_seq']! as int,
      rows: (json['rows']! as List<Object?>)
          .cast<Map<String, Object?>>()
          .map(SnapshotRow.fromJson)
          .toList(),
      nextRowOrdinal: json['next_row_ordinal']! as int,
      hasMore: json['has_more']! as bool,
      byteCount: json['byte_count']! as int,
    );
  }
}

final class PushSessionCreateResponse {
  const PushSessionCreateResponse({
    required this.status,
    this.pushId = '',
    this.plannedRowCount = 0,
    this.nextExpectedRowOrdinal = 0,
    this.bundleSeq = 0,
    this.sourceId = '',
    this.sourceBundleId = 0,
    this.rowCount = 0,
    this.bundleHash = '',
    required this.canonicalRequestHash,
  });

  final String status;
  final String pushId;
  final int plannedRowCount;
  final int nextExpectedRowOrdinal;
  final int bundleSeq;
  final String sourceId;
  final int sourceBundleId;
  final int rowCount;
  final String bundleHash;
  final String canonicalRequestHash;

  factory PushSessionCreateResponse.fromJson(Map<String, Object?> json) {
    return PushSessionCreateResponse(
      status: json['status']! as String,
      pushId: (json['push_id'] as String?) ?? '',
      plannedRowCount: (json['planned_row_count'] as int?) ?? 0,
      nextExpectedRowOrdinal: (json['next_expected_row_ordinal'] as int?) ?? 0,
      bundleSeq: (json['bundle_seq'] as int?) ?? 0,
      sourceId: (json['source_id'] as String?) ?? '',
      sourceBundleId: (json['source_bundle_id'] as int?) ?? 0,
      rowCount: (json['row_count'] as int?) ?? 0,
      bundleHash: (json['bundle_hash'] as String?) ?? '',
      canonicalRequestHash: json['canonical_request_hash']! as String,
    );
  }
}

final class PushSessionChunkResponse {
  const PushSessionChunkResponse({
    required this.pushId,
    required this.nextExpectedRowOrdinal,
  });

  final String pushId;
  final int nextExpectedRowOrdinal;

  factory PushSessionChunkResponse.fromJson(Map<String, Object?> json) {
    return PushSessionChunkResponse(
      pushId: json['push_id']! as String,
      nextExpectedRowOrdinal: json['next_expected_row_ordinal']! as int,
    );
  }
}

final class PushSessionCommitResponse {
  const PushSessionCommitResponse({
    required this.bundleSeq,
    required this.sourceId,
    required this.sourceBundleId,
    required this.rowCount,
    required this.bundleHash,
    required this.canonicalRequestHash,
  });

  final int bundleSeq;
  final String sourceId;
  final int sourceBundleId;
  final int rowCount;
  final String bundleHash;
  final String canonicalRequestHash;

  factory PushSessionCommitResponse.fromJson(Map<String, Object?> json) {
    return PushSessionCommitResponse(
      bundleSeq: json['bundle_seq']! as int,
      sourceId: json['source_id']! as String,
      sourceBundleId: json['source_bundle_id']! as int,
      rowCount: json['row_count']! as int,
      bundleHash: json['bundle_hash']! as String,
      canonicalRequestHash: json['canonical_request_hash']! as String,
    );
  }
}

final class CommittedBundleRowsResponse {
  const CommittedBundleRowsResponse({
    required this.bundleSeq,
    required this.sourceId,
    required this.sourceBundleId,
    required this.rowCount,
    required this.bundleHash,
    required this.canonicalRequestHash,
    required this.rows,
    required this.nextRowOrdinal,
    required this.hasMore,
  });

  final int bundleSeq;
  final String sourceId;
  final int sourceBundleId;
  final int rowCount;
  final String bundleHash;
  final String canonicalRequestHash;
  final List<BundleRow> rows;
  final int nextRowOrdinal;
  final bool hasMore;

  factory CommittedBundleRowsResponse.fromJson(Map<String, Object?> json) {
    return CommittedBundleRowsResponse(
      bundleSeq: json['bundle_seq']! as int,
      sourceId: json['source_id']! as String,
      sourceBundleId: json['source_bundle_id']! as int,
      rowCount: json['row_count']! as int,
      bundleHash: json['bundle_hash']! as String,
      canonicalRequestHash: json['canonical_request_hash']! as String,
      rows: (json['rows']! as List<Object?>)
          .cast<Map<String, Object?>>()
          .map(BundleRow.fromJson)
          .toList(),
      nextRowOrdinal: json['next_row_ordinal']! as int,
      hasMore: json['has_more']! as bool,
    );
  }
}

final class PushConflictDetails {
  const PushConflictDetails({
    required this.schema,
    required this.table,
    required this.key,
    required this.op,
    required this.baseRowVersion,
    required this.serverRowVersion,
    required this.serverRowDeleted,
    this.serverRow,
  });

  final String schema;
  final String table;
  final SyncKey key;
  final String op;
  final int baseRowVersion;
  final int serverRowVersion;
  final bool serverRowDeleted;
  final Object? serverRow;

  factory PushConflictDetails.fromJson(Map<String, Object?> json) {
    final details = PushConflictDetails(
      schema: json['schema']! as String,
      table: json['table']! as String,
      key: (json['key']! as Map).cast<String, String>(),
      op: json['op']! as String,
      baseRowVersion: json['base_row_version']! as int,
      serverRowVersion: json['server_row_version']! as int,
      serverRowDeleted: json['server_row_deleted']! as bool,
      serverRow: json['server_row'],
    );
    _validatePushConflictDetails(details);
    return details;
  }
}

final class PushConflictException implements Exception {
  const PushConflictException({required this.message, required this.conflict});

  final String message;
  final PushConflictDetails conflict;

  @override
  String toString() => message;
}

void _validatePushConflictDetails(PushConflictDetails details) {
  if (details.schema.trim().isEmpty) {
    throw const OversqliteProtocolException(
      'push conflict schema must be non-empty',
    );
  }
  if (details.table.trim().isEmpty) {
    throw const OversqliteProtocolException(
      'push conflict table must be non-empty',
    );
  }
  _validateVisibleWireKey(details.key, 'push conflict key');
  if (!_isSupportedRowOp(details.op)) {
    throw OversqliteProtocolException(
      'push conflict op ${details.op} is unsupported',
    );
  }
  if (details.baseRowVersion < 0) {
    throw OversqliteProtocolException(
      'push conflict base_row_version ${details.baseRowVersion} must be non-negative',
    );
  }
  if (details.serverRowVersion < 0) {
    throw OversqliteProtocolException(
      'push conflict server_row_version ${details.serverRowVersion} must be non-negative',
    );
  }
  final serverRow = details.serverRow;
  if (serverRow != null) {
    _validateVisibleWirePayload(serverRow, 'push conflict server_row');
  }
}

void _validateBundleRow(BundleRow row) {
  if (row.schema.trim().isEmpty) {
    throw const OversqliteProtocolException(
      'bundle row schema must be non-empty',
    );
  }
  if (row.table.trim().isEmpty) {
    throw const OversqliteProtocolException(
      'bundle row table must be non-empty',
    );
  }
  _validateVisibleWireKey(row.key, 'bundle row key');
  if (row.rowVersion <= 0) {
    throw OversqliteProtocolException(
      'bundle row row_version ${row.rowVersion} must be positive',
    );
  }
  if (!_isSupportedRowOp(row.op)) {
    throw OversqliteProtocolException('bundle row op ${row.op} is unsupported');
  }
  if (row.op != 'DELETE') {
    if (row.payload == null) {
      throw OversqliteProtocolException(
        'bundle row payload must be present for ${row.op}',
      );
    }
    _validateVisibleWirePayload(row.payload, 'bundle row payload');
  }
}

void _validateBundle(Bundle bundle) {
  if (bundle.bundleSeq <= 0) {
    throw OversqliteProtocolException(
      'bundle_seq ${bundle.bundleSeq} must be positive',
    );
  }
  if (bundle.sourceId.trim().isEmpty) {
    throw const OversqliteProtocolException(
      'bundle source_id must be non-empty',
    );
  }
  if (bundle.sourceBundleId <= 0) {
    throw OversqliteProtocolException(
      'bundle source_bundle_id ${bundle.sourceBundleId} must be positive',
    );
  }
}

void validatePullResponse(PullResponse response, int afterBundleSeq) {
  if (response.stableBundleSeq < 0) {
    throw OversqliteProtocolException(
      'pull response stable_bundle_seq ${response.stableBundleSeq} must be non-negative',
    );
  }
  if (response.stableBundleSeq < afterBundleSeq) {
    throw OversqliteProtocolException(
      'pull response stable_bundle_seq ${response.stableBundleSeq} is behind requested after_bundle_seq $afterBundleSeq',
    );
  }
  if (response.bundles.isNotEmpty && response.stableBundleSeq <= 0) {
    throw const OversqliteProtocolException(
      'pull response missing stable_bundle_seq for non-empty bundle list',
    );
  }
  var previous = afterBundleSeq;
  for (final bundle in response.bundles) {
    _validateBundle(bundle);
    for (var index = 0; index < bundle.rows.length; index++) {
      try {
        _validateBundleRow(bundle.rows[index]);
      } on OversqliteProtocolException catch (error) {
        throw OversqliteProtocolException(
          'invalid bundle row $index: ${error.message}',
        );
      }
    }
    if (bundle.bundleSeq <= previous) {
      throw OversqliteProtocolException(
        'pull response bundle_seq ${bundle.bundleSeq} is not strictly greater than previous $previous',
      );
    }
    if (bundle.bundleSeq > response.stableBundleSeq) {
      throw OversqliteProtocolException(
        'pull response bundle_seq ${bundle.bundleSeq} exceeds stable_bundle_seq ${response.stableBundleSeq}',
      );
    }
    previous = bundle.bundleSeq;
  }
}

void _validateSnapshotRow(SnapshotRow row) {
  if (row.schema.trim().isEmpty) {
    throw const OversqliteProtocolException(
      'snapshot row schema must be non-empty',
    );
  }
  if (row.table.trim().isEmpty) {
    throw const OversqliteProtocolException(
      'snapshot row table must be non-empty',
    );
  }
  _validateVisibleWireKey(row.key, 'snapshot row key');
  if (row.rowVersion <= 0) {
    throw OversqliteProtocolException(
      'snapshot row row_version ${row.rowVersion} must be positive',
    );
  }
  if (row.payload == null) {
    throw const OversqliteProtocolException(
      'snapshot row payload must be present',
    );
  }
  _validateVisibleWirePayload(row.payload, 'snapshot row payload');
}

void validateSnapshotRow(SnapshotRow row) {
  try {
    _validateSnapshotRow(row);
  } on SnapshotSemanticException {
    rethrow;
  } catch (_) {
    throw const SnapshotSemanticException(SnapshotSemanticFailure.invalidRow);
  }
}

void _validateSnapshotSession(SnapshotSession session) {
  if (session.snapshotId.trim().isEmpty) {
    throw const OversqliteProtocolException(
      'snapshot session response missing snapshot_id',
    );
  }
  if (session.snapshotBundleSeq < 0) {
    throw OversqliteProtocolException(
      'snapshot session snapshot_bundle_seq ${session.snapshotBundleSeq} must be non-negative',
    );
  }
  if (session.rowCount < 0) {
    throw OversqliteProtocolException(
      'snapshot session row_count ${session.rowCount} must be non-negative',
    );
  }
  if (session.rowCount > 0 && session.snapshotBundleSeq <= 0) {
    throw const OversqliteProtocolException(
      'snapshot session missing snapshot_bundle_seq for non-empty row set',
    );
  }
  if (session.byteCount < 0) {
    throw OversqliteProtocolException(
      'snapshot session byte_count ${session.byteCount} must be non-negative',
    );
  }
  if (session.rowCount == 0 && session.byteCount != 0) {
    throw const SnapshotSemanticException(
      SnapshotSemanticFailure.invalidSession,
    );
  }
  if (session.rowCount > 0 && session.byteCount == 0) {
    throw const SnapshotSemanticException(
      SnapshotSemanticFailure.invalidSession,
    );
  }
  if (!_isCanonicalSnapshotTimestamp(session.expiresAt)) {
    throw const SnapshotSemanticException(
      SnapshotSemanticFailure.invalidSession,
    );
  }
}

final RegExp _canonicalSnapshotTimestamp = RegExp(
  r'^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})'
  r'(?:\.\d{1,9})?(?:Z|([+-])(\d{2}):(\d{2}))$',
);

bool _isCanonicalSnapshotTimestamp(String value) {
  final match = _canonicalSnapshotTimestamp.firstMatch(value);
  if (match == null) return false;
  final year = int.parse(match.group(1)!);
  final month = int.parse(match.group(2)!);
  final day = int.parse(match.group(3)!);
  final hour = int.parse(match.group(4)!);
  final minute = int.parse(match.group(5)!);
  final second = int.parse(match.group(6)!);
  final offsetHour = match.group(8) == null ? 0 : int.parse(match.group(8)!);
  final offsetMinute = match.group(9) == null ? 0 : int.parse(match.group(9)!);
  if (month < 1 ||
      month > 12 ||
      day < 1 ||
      hour > 23 ||
      minute > 59 ||
      second > 59 ||
      offsetHour > 23 ||
      offsetMinute > 59) {
    return false;
  }
  final normalized = DateTime.utc(year, month, day);
  return normalized.year == year &&
      normalized.month == month &&
      normalized.day == day;
}

String requireValidOversqliteSourceId(String value) {
  if (value.isEmpty ||
      value.codeUnits.any((unit) => unit < 0x21 || unit > 0x7e)) {
    throw const InvalidOversqliteSourceIdException();
  }
  return value;
}

int checkedAddOversqliteInt64(int left, int right, String operation) {
  if (left < minWireOversqliteInt64 ||
      left > maxOversqliteInt64 ||
      right < minWireOversqliteInt64 ||
      right > maxOversqliteInt64) {
    throw OversqliteProtocolException('$operation overflow');
  }
  if ((right > 0 && left > maxOversqliteInt64 - right) ||
      (right < 0 && left < minWireOversqliteInt64 - right)) {
    throw OversqliteProtocolException('$operation overflow');
  }
  return left + right;
}

void _validateVisibleWireKey(SyncKey key, String label) {
  if (key.isEmpty) {
    throw OversqliteProtocolException('$label must be non-empty');
  }
  if (key.containsKey(_hiddenSyncScopeColumnName)) {
    throw OversqliteProtocolException(
      '$label must not include hidden server column $_hiddenSyncScopeColumnName',
    );
  }
  for (final entry in key.entries) {
    if (entry.key.trim().isEmpty || entry.value.trim().isEmpty) {
      throw OversqliteProtocolException(
        '$label contains an empty column/value',
      );
    }
  }
}

void _validateVisibleWirePayload(Object? payload, String label) {
  if (payload is! Map) {
    throw OversqliteProtocolException('$label must be a JSON object');
  }
  if (payload.containsKey(_hiddenSyncScopeColumnName)) {
    throw OversqliteProtocolException(
      '$label must not include hidden server column $_hiddenSyncScopeColumnName',
    );
  }
}

bool _isSupportedRowOp(String op) {
  return op == 'INSERT' || op == 'UPDATE' || op == 'DELETE';
}

const _hiddenSyncScopeColumnName = '_sync_scope_id';

final class OversqliteProtocolException implements Exception {
  const OversqliteProtocolException(this.message);

  final String message;

  @override
  String toString() => message;
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
  String toString() =>
      'client rebuild is required; run rebuild() before syncing';
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
  const SourceReplacementDivergedException({
    required this.localReplacementSourceId,
    required this.remoteReplacementSourceId,
  });

  final String localReplacementSourceId;
  final String remoteReplacementSourceId;

  @override
  String toString() =>
      'replacement source diverged between local and server recovery state: '
      'local="$localReplacementSourceId" remote="$remoteReplacementSourceId"';
}

final class SourceRecoveryRequiredHttpException implements Exception {
  const SourceRecoveryRequiredHttpException({
    required this.reason,
    required this.body,
    this.replacementSourceId,
  });

  final SourceRecoveryReason reason;
  final String body;
  final String? replacementSourceId;

  @override
  String toString() => body;
}

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
    final row = SnapshotRow(
      schema: json['schema']! as String,
      table: json['table']! as String,
      key: (json['key']! as Map).cast<String, String>(),
      rowVersion: json['row_version']! as int,
      payload: json['payload'],
    );
    _validateSnapshotRow(row);
    return row;
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
    final session = SnapshotSession(
      snapshotId: json['snapshot_id']! as String,
      snapshotBundleSeq: json['snapshot_bundle_seq']! as int,
      rowCount: json['row_count']! as int,
      byteCount: (json['byte_count'] as int?) ?? 0,
      expiresAt: json['expires_at']! as String,
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
  });

  final String snapshotId;
  final int snapshotBundleSeq;
  final List<SnapshotRow> rows;
  final int nextRowOrdinal;
  final bool hasMore;

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
  });

  final int bundleSeq;
  final String sourceId;
  final int sourceBundleId;
  final int rowCount;
  final String bundleHash;

  factory PushSessionCommitResponse.fromJson(Map<String, Object?> json) {
    return PushSessionCommitResponse(
      bundleSeq: json['bundle_seq']! as int,
      sourceId: json['source_id']! as String,
      sourceBundleId: json['source_bundle_id']! as int,
      rowCount: json['row_count']! as int,
      bundleHash: json['bundle_hash']! as String,
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
    required this.rows,
    required this.nextRowOrdinal,
    required this.hasMore,
  });

  final int bundleSeq;
  final String sourceId;
  final int sourceBundleId;
  final int rowCount;
  final String bundleHash;
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
  if (session.expiresAt.trim().isEmpty) {
    throw const OversqliteProtocolException(
      'snapshot session response missing expires_at',
    );
  }
  try {
    DateTime.parse(session.expiresAt);
  } on FormatException {
    throw OversqliteProtocolException(
      "oversqlite timestamp must be RFC3339/ISO-8601 instant: '${session.expiresAt}'",
    );
  }
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

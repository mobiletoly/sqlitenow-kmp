import 'dart:convert';
import 'dart:io';

import 'protocol_models.dart';
import 'protocol_transport.dart';
import 'watch_protocol.dart';

final class OversqliteRemoteApi {
  const OversqliteRemoteApi(this._http);

  final OversqliteHttpClient _http;

  Future<CapabilitiesResponse> fetchCapabilities(String sourceId) async {
    final response = await _http.get('sync/capabilities', sourceId: sourceId);
    final body = _requireOkJsonObject(
      response,
      operation: 'capabilities request',
      method: 'GET',
      path: '/sync/capabilities',
    );
    return CapabilitiesResponse.fromJson(body);
  }

  Future<ConnectResponse> connect({
    required String sourceId,
    required bool hasLocalPendingRows,
  }) async {
    final response = await _http.postJson(
      'sync/connect',
      sourceId: sourceId,
      body: ConnectRequest(hasLocalPendingRows: hasLocalPendingRows).toJson(),
    );
    final body = _requireOkJsonObject(
      response,
      operation: 'connect request',
      method: 'POST',
      path: '/sync/connect',
    );
    final connect = ConnectResponse.fromJson(body);
    switch (connect.resolution) {
      case 'retry_later':
      case 'initialize_empty':
      case 'initialize_local':
      case 'remote_authoritative':
        return connect;
      default:
        throw OversqliteProtocolException(
          'unexpected connect resolution ${connect.resolution}',
        );
    }
  }

  Future<PushSessionCreateResponse> createPushSession({
    required String sourceId,
    required int sourceBundleId,
    required int plannedRowCount,
    required String canonicalRequestHash,
    String? initializationId,
  }) async {
    final response = await _http.postJson(
      'sync/push-sessions',
      sourceId: sourceId,
      body: {
        'source_bundle_id': sourceBundleId,
        'planned_row_count': plannedRowCount,
        'canonical_request_hash': canonicalRequestHash,
        if (initializationId != null && initializationId.isNotEmpty)
          'initialization_id': initializationId,
      },
    );
    if (response.statusCode != HttpStatus.ok) {
      final recovery = _decodeSourceRecoveryRequired(response);
      if (recovery != null) {
        throw recovery;
      }
      _throwHttp(
        response,
        operation: 'push session request',
        method: 'POST',
        path: '/sync/push-sessions',
      );
    }
    final body = _requireOkJsonObject(
      response,
      operation: 'push session request',
      method: 'POST',
      path: '/sync/push-sessions',
    );
    final create = PushSessionCreateResponse.fromJson(body);
    _validatePushSessionCreateResponse(
      create,
      sourceId: sourceId,
      sourceBundleId: sourceBundleId,
      plannedRowCount: plannedRowCount,
      canonicalRequestHash: canonicalRequestHash,
    );
    return create;
  }

  Future<PushSessionChunkResponse> uploadPushChunk({
    required String pushId,
    required String sourceId,
    required int startRowOrdinal,
    required List<PushRequestRow> rows,
  }) async {
    final response = await _http.postJson(
      'sync/push-sessions/$pushId/chunks',
      sourceId: sourceId,
      body: {
        'start_row_ordinal': startRowOrdinal,
        'rows': [for (final row in rows) row.toJson()],
      },
    );
    final body = _requireOkJsonObject(
      response,
      operation: 'push chunk request',
      method: 'POST',
      path: '/sync/push-sessions/$pushId/chunks',
    );
    return PushSessionChunkResponse.fromJson(body);
  }

  Future<PushSessionCommitResponse> commitPushSession({
    required String pushId,
    required String sourceId,
  }) async {
    final response = await _http.postJson(
      'sync/push-sessions/$pushId/commit',
      sourceId: sourceId,
      body: null,
    );
    if (response.statusCode != HttpStatus.ok) {
      final conflict = _decodePushConflict(response);
      if (conflict != null) {
        throw conflict;
      }
      final recovery = _decodeSourceRecoveryRequired(response);
      if (recovery != null) {
        throw recovery;
      }
      _throwHttp(
        response,
        operation: 'push commit request',
        method: 'POST',
        path: '/sync/push-sessions/$pushId/commit',
      );
    }
    final body = _requireOkJsonObject(
      response,
      operation: 'push commit request',
      method: 'POST',
      path: '/sync/push-sessions/$pushId/commit',
    );
    return PushSessionCommitResponse.fromJson(body);
  }

  Future<CommittedBundleRowsResponse> fetchCommittedBundleChunk({
    required int bundleSeq,
    required String sourceId,
    required int? afterRowOrdinal,
    required int maxRows,
  }) async {
    final query = <String>[
      if (afterRowOrdinal != null) 'after_row_ordinal=$afterRowOrdinal',
      'max_rows=$maxRows',
    ].join('&');
    final path = 'sync/committed-bundles/$bundleSeq/rows?$query';
    final response = await _http.get(path, sourceId: sourceId);
    if (response.statusCode != HttpStatus.ok) {
      final notFound = _decodeCommittedBundleNotFound(response);
      if (notFound != null) {
        throw notFound;
      }
      final pruned = _decodeCommittedReplayPruned(response);
      if (pruned != null) {
        throw pruned;
      }
      _throwHttp(
        response,
        operation: 'committed bundle chunk request',
        method: 'GET',
        path: '/$path',
      );
    }
    final body = _requireOkJsonObject(
      response,
      operation: 'committed bundle chunk request',
      method: 'GET',
      path: '/$path',
    );
    return CommittedBundleRowsResponse.fromJson(body);
  }

  Future<PullResponse> sendPullRequest({
    required String sourceId,
    required int afterBundleSeq,
    required int maxBundles,
    required int targetBundleSeq,
  }) async {
    final query = <String>[
      'after_bundle_seq=$afterBundleSeq',
      'max_bundles=$maxBundles',
      if (targetBundleSeq > 0) 'target_bundle_seq=$targetBundleSeq',
    ].join('&');
    final path = 'sync/pull?$query';
    final response = await _http.get(path, sourceId: sourceId);
    if (response.statusCode != HttpStatus.ok) {
      final historyPruned = _decodeHistoryPruned(response);
      if (historyPruned != null) {
        throw historyPruned;
      }
      final checkpointAhead = _decodeCheckpointAhead(response);
      if (checkpointAhead != null) {
        throw checkpointAhead;
      }
      _throwHttp(response, operation: 'pull', method: 'GET', path: '/$path');
    }
    final body = _requireOkJsonObject(
      response,
      operation: 'pull',
      method: 'GET',
      path: '/$path',
    );
    return PullResponse.fromJson(body);
  }

  Future<SnapshotSession> createSnapshotSession({
    required String sourceId,
    SnapshotSessionCreateRequest? request,
  }) async {
    final response = await _http.postJson(
      'sync/snapshot-sessions',
      sourceId: sourceId,
      body: request?.toJson(),
    );
    if (response.statusCode != HttpStatus.ok) {
      final recovery = _decodeSourceRecoveryRequired(response);
      if (recovery != null) {
        throw recovery;
      }
      final replacementInvalid = _decodeSourceReplacementInvalid(response);
      if (replacementInvalid != null) {
        throw replacementInvalid;
      }
      _throwHttp(
        response,
        operation: 'snapshot session request',
        method: 'POST',
        path: '/sync/snapshot-sessions',
      );
    }
    final body = _requireOkJsonObject(
      response,
      operation: 'snapshot session request',
      method: 'POST',
      path: '/sync/snapshot-sessions',
    );
    return SnapshotSession.fromJson(body);
  }

  Future<SnapshotChunkResponse> fetchSnapshotChunk({
    required String snapshotId,
    required String sourceId,
    required int snapshotBundleSeq,
    required int afterRowOrdinal,
    required int maxRows,
  }) async {
    final query = 'after_row_ordinal=$afterRowOrdinal&max_rows=$maxRows';
    final path = 'sync/snapshot-sessions/$snapshotId?$query';
    final response = await _http.get(path, sourceId: sourceId);
    final body = _requireOkJsonObject(
      response,
      operation: 'snapshot chunk request',
      method: 'GET',
      path: '/$path',
    );
    final chunk = SnapshotChunkResponse.fromJson(body);
    _validateSnapshotChunkResponse(
      chunk,
      snapshotId: snapshotId,
      snapshotBundleSeq: snapshotBundleSeq,
      afterRowOrdinal: afterRowOrdinal,
    );
    return chunk;
  }

  Future<void> deleteSnapshotSessionBestEffort({
    required String snapshotId,
    required String sourceId,
  }) async {
    if (snapshotId.trim().isEmpty) {
      return;
    }
    try {
      await _http.delete(
        'sync/snapshot-sessions/$snapshotId',
        sourceId: sourceId,
      );
    } catch (_) {
      return;
    }
  }

  Future<void> deletePushSessionBestEffort({
    required String pushId,
    required String sourceId,
  }) async {
    if (pushId.trim().isEmpty) {
      return;
    }
    try {
      await _http.delete('sync/push-sessions/$pushId', sourceId: sourceId);
    } catch (_) {
      return;
    }
  }
}

Map<String, Object?> _requireOkJsonObject(
  OversqliteHttpResponse response, {
  required String operation,
  required String method,
  required String path,
}) {
  if (response.statusCode != HttpStatus.ok) {
    _throwHttp(response, operation: operation, method: method, path: path);
  }
  final decoded = jsonDecode(response.body);
  if (decoded is! Map) {
    throw OversqliteProtocolException(
      '$operation returned a non-object JSON body',
    );
  }
  return decoded.cast<String, Object?>();
}

Never _throwHttp(
  OversqliteHttpResponse response, {
  required String operation,
  required String method,
  required String path,
}) {
  final errorResponse = _decodeErrorResponse(response.body);
  throw OversqliteHttpException(
    operation: operation,
    method: method,
    path: path,
    statusCode: response.statusCode,
    body: response.body,
    errorResponse: errorResponse,
  );
}

ErrorResponse? _decodeErrorResponse(String body) {
  try {
    final decoded = jsonDecode(body);
    if (decoded is Map) {
      return ErrorResponse.fromJson(decoded.cast<String, Object?>());
    }
  } on FormatException {
    return null;
  }
  return null;
}

PushConflictException? _decodePushConflict(OversqliteHttpResponse response) {
  if (response.statusCode != HttpStatus.conflict) {
    return null;
  }
  try {
    final decoded = jsonDecode(response.body);
    if (decoded is! Map) {
      return null;
    }
    final body = decoded.cast<String, Object?>();
    if (body['error'] != 'push_conflict') {
      return null;
    }
    final conflict = body['conflict'];
    if (conflict is! Map) {
      return null;
    }
    return PushConflictException(
      message: (body['message'] as String?) ?? 'push conflict',
      conflict: PushConflictDetails.fromJson(conflict.cast<String, Object?>()),
    );
  } on FormatException {
    return null;
  }
}

SourceRecoveryRequiredHttpException? _decodeSourceRecoveryRequired(
  OversqliteHttpResponse response,
) {
  if (response.statusCode != HttpStatus.conflict) {
    return null;
  }
  final decoded = _decodeErrorResponse(response.body);
  if (decoded == null) {
    return null;
  }
  if (decoded.error == 'source_retired') {
    String? replacementSourceId;
    try {
      final body = jsonDecode(response.body);
      if (body is Map) {
        replacementSourceId = body['replaced_by_source_id'] as String?;
      }
    } on FormatException {
      replacementSourceId = null;
    }
    return SourceRecoveryRequiredHttpException(
      reason: SourceRecoveryReason.sourceRetired,
      body: response.body,
      replacementSourceId: replacementSourceId?.trim().isEmpty == true
          ? null
          : replacementSourceId,
    );
  }
  final reason = switch (decoded.error) {
    'history_pruned' => SourceRecoveryReason.historyPruned,
    'source_sequence_out_of_order' =>
      SourceRecoveryReason.sourceSequenceOutOfOrder,
    'source_sequence_changed' => SourceRecoveryReason.sourceSequenceChanged,
    _ => null,
  };
  if (reason == null) {
    return null;
  }
  return SourceRecoveryRequiredHttpException(
    reason: reason,
    body: response.body,
  );
}

CommittedBundleNotFoundException? _decodeCommittedBundleNotFound(
  OversqliteHttpResponse response,
) {
  if (response.statusCode != HttpStatus.notFound) {
    return null;
  }
  final decoded = _decodeErrorResponse(response.body);
  if (decoded?.error != 'committed_bundle_not_found') {
    return null;
  }
  return CommittedBundleNotFoundException(response.body);
}

CommittedReplayPrunedException? _decodeCommittedReplayPruned(
  OversqliteHttpResponse response,
) {
  if (response.statusCode != HttpStatus.conflict) {
    return null;
  }
  final decoded = _decodeErrorResponse(response.body);
  if (decoded?.error != 'history_pruned') {
    return null;
  }
  return CommittedReplayPrunedException(response.body);
}

HistoryPrunedException? _decodeHistoryPruned(OversqliteHttpResponse response) {
  if (response.statusCode != HttpStatus.conflict) {
    return null;
  }
  final decoded = _decodeErrorResponse(response.body);
  if (decoded?.error != 'history_pruned') {
    return null;
  }
  return HistoryPrunedException(decoded?.message ?? response.body);
}

CheckpointAheadException? _decodeCheckpointAhead(
  OversqliteHttpResponse response,
) {
  if (response.statusCode != HttpStatus.conflict) {
    return null;
  }
  final decoded = _decodeErrorResponse(response.body);
  if (decoded?.error != 'checkpoint_ahead') {
    return null;
  }
  return CheckpointAheadException(decoded?.message ?? response.body);
}

SourceReplacementInvalidException? _decodeSourceReplacementInvalid(
  OversqliteHttpResponse response,
) {
  if (response.statusCode != HttpStatus.conflict) {
    return null;
  }
  final decoded = _decodeErrorResponse(response.body);
  if (decoded?.error != 'source_replacement_invalid') {
    return null;
  }
  return SourceReplacementInvalidException(decoded?.message ?? response.body);
}

void _validatePushSessionCreateResponse(
  PushSessionCreateResponse response, {
  required String sourceId,
  required int sourceBundleId,
  required int plannedRowCount,
  required String canonicalRequestHash,
}) {
  switch (response.status) {
    case 'staging':
      if (response.pushId.trim().isEmpty) {
        throw const OversqliteProtocolException(
          'push session response missing push_id',
        );
      }
      if (response.plannedRowCount != plannedRowCount) {
        throw OversqliteProtocolException(
          'push session response planned_row_count ${response.plannedRowCount} does not match requested $plannedRowCount',
        );
      }
      if (response.nextExpectedRowOrdinal != 0) {
        throw OversqliteProtocolException(
          'push session response next_expected_row_ordinal ${response.nextExpectedRowOrdinal} must be 0',
        );
      }
      if (response.canonicalRequestHash != canonicalRequestHash) {
        throw const SourceSequenceMismatchException(
          'push session response canonical_request_hash does not match request',
        );
      }
    case 'already_committed':
      if (response.bundleSeq <= 0) {
        throw const OversqliteProtocolException(
          'push session already_committed response missing bundle_seq',
        );
      }
      if (response.sourceId != sourceId) {
        throw OversqliteProtocolException(
          'push session already_committed response source_id ${response.sourceId} does not match client $sourceId',
        );
      }
      if (response.sourceBundleId != sourceBundleId) {
        throw OversqliteProtocolException(
          'push session already_committed response source_bundle_id ${response.sourceBundleId} does not match requested $sourceBundleId',
        );
      }
      if (response.rowCount < 0) {
        throw const OversqliteProtocolException(
          'push session already_committed response missing row_count',
        );
      }
      if (response.bundleHash.trim().isEmpty) {
        throw const OversqliteProtocolException(
          'push session already_committed response missing bundle_hash',
        );
      }
      if (response.canonicalRequestHash != canonicalRequestHash) {
        throw const SourceSequenceMismatchException(
          'push session already_committed canonical_request_hash does not match request',
        );
      }
    default:
      throw OversqliteProtocolException(
        'push session response returned unsupported status ${response.status}',
      );
  }
}

void _validateSnapshotChunkResponse(
  SnapshotChunkResponse chunk, {
  required String snapshotId,
  required int snapshotBundleSeq,
  required int afterRowOrdinal,
}) {
  if (chunk.snapshotId != snapshotId) {
    throw OversqliteProtocolException(
      'snapshot chunk response snapshot_id ${chunk.snapshotId} does not match requested $snapshotId',
    );
  }
  if (chunk.snapshotBundleSeq != snapshotBundleSeq) {
    throw OversqliteProtocolException(
      'snapshot chunk response snapshot_bundle_seq ${chunk.snapshotBundleSeq} does not match session $snapshotBundleSeq',
    );
  }
  if (chunk.rows.isNotEmpty && chunk.snapshotBundleSeq <= 0) {
    throw const OversqliteProtocolException(
      'snapshot chunk response missing snapshot_bundle_seq for non-empty row set',
    );
  }
  if (chunk.nextRowOrdinal != afterRowOrdinal + chunk.rows.length) {
    throw OversqliteProtocolException(
      'snapshot chunk response next_row_ordinal ${chunk.nextRowOrdinal} does not match expected ${afterRowOrdinal + chunk.rows.length}',
    );
  }
  if (chunk.hasMore && chunk.rows.isEmpty) {
    throw const OversqliteProtocolException(
      'snapshot chunk response with has_more=true must include at least one row',
    );
  }
}

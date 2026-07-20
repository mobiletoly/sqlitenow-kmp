import 'dart:convert';
import 'dart:io';

import 'protocol_models.dart';
import 'protocol_transport.dart';
import 'snapshot_capacity_retry.dart';
import 'snapshot_diagnostics.dart';
import 'snapshot_utf8.dart';
import 'snapshot_wire_decoder.dart';
import 'watch_protocol.dart';
import 'wire_int64.dart';

const _ordinaryBounds = OversqliteHttpRequestBounds(
  errorBodyBytes: OversqliteRemoteApi.snapshotControlBodyLimit,
);

final class OversqliteRemoteApi {
  OversqliteRemoteApi(
    this._http, {
    SnapshotDiagnosticsRecorder? snapshotDiagnostics,
  }) : snapshotDiagnostics =
           snapshotDiagnostics ?? SnapshotDiagnosticsRecorder();

  static const snapshotCapabilitiesBodyLimit = 4 * 1024 * 1024;
  static const snapshotControlBodyLimit = 64 * 1024;
  static const snapshotBodyEnvelopeBytes = 64 * 1024;
  static const snapshotRetirementTimeout = Duration(seconds: 5);

  final OversqliteHttpClient _http;
  final SnapshotDiagnosticsRecorder snapshotDiagnostics;

  Future<CapabilitiesResponse> fetchCapabilities(String sourceId) async {
    final response = await _http.get(
      'sync/capabilities',
      sourceId: requireValidOversqliteSourceId(sourceId),
      operation: 'capabilities request',
      bounds: const OversqliteHttpRequestBounds(
        successBodyBytes: snapshotCapabilitiesBodyLimit,
        errorBodyBytes: snapshotControlBodyLimit,
      ),
    );
    return _useSnapshotResponse(response, () async {
      try {
        _requireSnapshotResponseIngress(
          response,
          operation: 'capabilities request',
          successLimit: snapshotCapabilitiesBodyLimit,
          errorLimit: snapshotControlBodyLimit,
          diagnostics: snapshotDiagnostics,
        );
        final body = _decodeSnapshotJsonObject(
          response,
          operation: 'capabilities request',
        );
        return requireOversqliteProtocol(CapabilitiesResponse.fromJson(body));
      } on SnapshotCapabilitiesException {
        rethrow;
      } on ProtocolVersionMismatchException {
        rethrow;
      } on OversqliteHttpException {
        rethrow;
      } on SnapshotCapacityException {
        rethrow;
      } on SnapshotResponseBodyTooLargeException {
        rethrow;
      } on SnapshotUnsupportedContentEncodingException {
        rethrow;
      } on SnapshotResponseDecodeException {
        rethrow;
      } on SnapshotSemanticException {
        rethrow;
      } catch (_) {
        throw const SnapshotResponseDecodeException('capabilities request');
      }
    });
  }

  Future<ConnectResponse> connect({
    required String sourceId,
    required bool hasLocalPendingRows,
  }) async {
    final response = await _http.postJson(
      'sync/connect',
      sourceId: sourceId,
      body: ConnectRequest(hasLocalPendingRows: hasLocalPendingRows).toJson(),
      operation: 'connect request',
      bounds: _ordinaryBounds,
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
      operation: 'push session request',
      bounds: _ordinaryBounds,
    );
    if (response.statusCode != HttpStatus.ok) {
      final errorDocument = _decodeErrorDocument(response.body);
      final recovery = _decodeSourceRecoveryRequired(
        response,
        expectedSourceId: sourceId,
        document: errorDocument,
      );
      if (recovery != null) {
        throw recovery;
      }
      _throwHttp(
        response,
        operation: 'push session request',
        method: 'POST',
        path: '/sync/push-sessions',
        decodedError: errorDocument,
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
      operation: 'push chunk request',
      bounds: _ordinaryBounds,
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
      operation: 'push commit request',
      bounds: _ordinaryBounds,
    );
    if (response.statusCode != HttpStatus.ok) {
      final errorDocument = _decodeErrorDocument(response.body);
      final conflict = _decodePushConflict(response, document: errorDocument);
      if (conflict != null) {
        throw conflict;
      }
      final recovery = _decodeSourceRecoveryRequired(
        response,
        expectedSourceId: sourceId,
        document: errorDocument,
      );
      if (recovery != null) {
        throw recovery;
      }
      _throwHttp(
        response,
        operation: 'push commit request',
        method: 'POST',
        path: '/sync/push-sessions/$pushId/commit',
        decodedError: errorDocument,
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
    final response = await _http.get(
      path,
      sourceId: sourceId,
      operation: 'committed bundle chunk request',
      bounds: _ordinaryBounds,
    );
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
    final response = await _http.get(
      path,
      sourceId: sourceId,
      operation: 'pull',
      bounds: _ordinaryBounds,
    );
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
    final replacement = request?.sourceReplacement;
    if (replacement != null) {
      requireValidOversqliteSourceId(replacement.previousSourceId);
      requireValidOversqliteSourceId(replacement.newSourceId);
    }
    final response = await _http.postJson(
      'sync/snapshot-sessions',
      sourceId: requireValidOversqliteSourceId(sourceId),
      body: request?.toJson(),
      operation: 'snapshot session request',
      bounds: const OversqliteHttpRequestBounds(
        successBodyBytes: snapshotControlBodyLimit,
        errorBodyBytes: snapshotControlBodyLimit,
      ),
    );
    return _useSnapshotResponse(response, () async {
      _requireSnapshotResponseIngress(
        response,
        operation: 'snapshot session request',
        successLimit: snapshotControlBodyLimit,
        errorLimit: snapshotControlBodyLimit,
        diagnostics: snapshotDiagnostics,
        sourceId: sourceId,
      );
      final body = _decodeSnapshotJsonObject(
        response,
        operation: 'snapshot session request',
      );
      final createdSnapshotId = body['snapshot_id'] is String
          ? body['snapshot_id']! as String
          : null;
      try {
        final session = SnapshotSession.fromJson(body);
        snapshotDiagnostics.recordSession();
        return session;
      } catch (error) {
        if (createdSnapshotId != null && createdSnapshotId.isNotEmpty) {
          await deleteSnapshotSessionBestEffort(
            snapshotId: createdSnapshotId,
            sourceId: sourceId,
          );
        }
        if (error is SnapshotSemanticException ||
            error is OversqliteProtocolException) {
          rethrow;
        }
        throw const SnapshotSemanticException(
          SnapshotSemanticFailure.invalidSession,
        );
      }
    });
  }

  Future<SnapshotChunkResponse> fetchSnapshotChunk({
    required String snapshotId,
    required String sourceId,
    required int snapshotBundleSeq,
    required int afterRowOrdinal,
    required int maxRows,
    required int maxBytes,
  }) async {
    if (maxRows <= 0 || maxBytes <= 0) {
      throw const SnapshotSemanticException(
        SnapshotSemanticFailure.invalidChunk,
      );
    }
    final bodyLimit = checkedSnapshotChunkBodyLimit(maxBytes, maxRows);
    final query =
        'after_row_ordinal=$afterRowOrdinal&max_rows=$maxRows&max_bytes=$maxBytes';
    final encodedSnapshotId = encodeOversqliteSessionIdPathSegment(snapshotId);
    final path = 'sync/snapshot-sessions/$encodedSnapshotId?$query';
    final response = await _http.get(
      path,
      sourceId: requireValidOversqliteSourceId(sourceId),
      operation: 'snapshot chunk request',
      bounds: OversqliteHttpRequestBounds(
        successBodyBytes: bodyLimit,
        errorBodyBytes: snapshotControlBodyLimit,
      ),
    );
    return _useSnapshotResponse(response, () async {
      _requireSnapshotResponseIngress(
        response,
        operation: 'snapshot chunk request',
        successLimit: bodyLimit,
        errorLimit: snapshotControlBodyLimit,
        diagnostics: snapshotDiagnostics,
        configuredBytes: maxBytes,
        sourceId: sourceId,
      );
      final decoded = decodeSnapshotChunkWire(response.body);
      final chunk = decoded.chunk;
      if (chunk.byteCount != decoded.rowWireByteCount) {
        throw const SnapshotSemanticException(
          SnapshotSemanticFailure.invalidChunk,
        );
      }
      _validateSnapshotChunkResponse(
        chunk,
        snapshotId: snapshotId,
        snapshotBundleSeq: snapshotBundleSeq,
        afterRowOrdinal: afterRowOrdinal,
        maxRows: maxRows,
        maxBytes: maxBytes,
      );
      snapshotDiagnostics.recordValidatedChunk(
        chunk.rows.length,
        chunk.byteCount,
      );
      return chunk;
    });
  }

  Future<void> deleteSnapshotSessionBestEffort({
    required String snapshotId,
    required String sourceId,
  }) async {
    if (snapshotId.isEmpty) {
      return;
    }
    try {
      await (() async {
        final response = await _http.delete(
          'sync/snapshot-sessions/${encodeOversqliteSessionIdPathSegment(snapshotId)}',
          sourceId: requireValidOversqliteSourceId(sourceId),
          operation: 'delete snapshot session',
          bounds: const OversqliteHttpRequestBounds(
            errorBodyBytes: snapshotControlBodyLimit,
            discardBody: true,
          ),
        );
        await response.close();
      })().timeout(snapshotRetirementTimeout);
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
      final response = await _http.delete(
        'sync/push-sessions/$pushId',
        sourceId: sourceId,
        operation: 'delete push session',
        bounds: const OversqliteHttpRequestBounds(
          errorBodyBytes: snapshotControlBodyLimit,
          discardBody: true,
        ),
      );
      await response.close();
    } catch (_) {
      return;
    }
  }
}

Future<T> _useSnapshotResponse<T>(
  OversqliteHttpResponse response,
  Future<T> Function() use,
) async {
  Object? primaryError;
  StackTrace? primaryStackTrace;
  T? result;
  try {
    result = await use();
  } catch (error, stackTrace) {
    primaryError = error;
    primaryStackTrace = stackTrace;
  }

  try {
    await response.close();
  } catch (_) {
    if (primaryError == null) {
      throw const OversqliteProtocolException(
        'snapshot response cleanup failed',
      );
    }
  }

  if (primaryError != null) {
    Error.throwWithStackTrace(primaryError, primaryStackTrace!);
  }
  return result as T;
}

void _requireSnapshotResponseIngress(
  OversqliteHttpResponse response, {
  required String operation,
  required int successLimit,
  required int errorLimit,
  required SnapshotDiagnosticsRecorder diagnostics,
  int? configuredBytes,
  String? sourceId,
}) {
  final encoding = response.header('content-encoding')?.trim().toLowerCase();
  if (encoding != null &&
      encoding.isNotEmpty &&
      encoding != 'identity' &&
      encoding != 'gzip') {
    throw SnapshotUnsupportedContentEncodingException(operation: operation);
  }
  final actual = snapshotUtf8ByteLength(response.body);
  if (response.decodedBodyBytes != null &&
      response.decodedBodyBytes != actual) {
    throw SnapshotResponseDecodeException(operation);
  }
  final limit = response.statusCode == HttpStatus.ok
      ? successLimit
      : errorLimit;
  if (actual > limit) {
    throw SnapshotResponseBodyTooLargeException(
      operation: operation,
      limit: limit,
    );
  }
  diagnostics.recordCompletelyDecodedBody(actual);
  if (response.statusCode != HttpStatus.ok) {
    _throwSnapshotHttp(
      response,
      operation: operation,
      configuredBytes: configuredBytes,
      sourceId: sourceId,
    );
  }
}

Map<String, Object?> _decodeSnapshotJsonObject(
  OversqliteHttpResponse response, {
  required String operation,
}) {
  requireUniqueSnapshotJsonObjectMembers(response.body, operation: operation);
  try {
    final decoded = jsonDecode(response.body);
    if (decoded is! Map) {
      throw SnapshotResponseDecodeException(operation);
    }
    return decoded.cast<String, Object?>();
  } on SnapshotResponseDecodeException {
    rethrow;
  } on FormatException {
    throw SnapshotResponseDecodeException(operation);
  }
}

Never _throwSnapshotHttp(
  OversqliteHttpResponse response, {
  required String operation,
  int? configuredBytes,
  String? sourceId,
}) {
  Map<String, Object?>? body;
  try {
    requireUniqueSnapshotJsonObjectMembers(response.body, operation: operation);
    final decoded = jsonDecode(response.body);
    if (decoded is Map) body = decoded.cast<String, Object?>();
  } catch (_) {
    body = null;
  }
  final rawError = body?['error'];
  final rawMessage = body?['message'];
  final safeErrorCode =
      rawError is String &&
          rawMessage is String &&
          _knownSnapshotErrorCodes.contains(rawError)
      ? rawError
      : 'invalid_error_response';
  if (response.statusCode == HttpStatus.tooManyRequests &&
      _snapshotCapacityErrorCodes.contains(safeErrorCode)) {
    throw SnapshotCapacityException(
      statusCode: response.statusCode,
      errorCode: safeErrorCode,
      retryAfter: parseSnapshotRetryAfter(response.header('retry-after')),
    );
  }
  if (response.statusCode == HttpStatus.badRequest &&
      safeErrorCode == 'snapshot_chunk_too_small' &&
      configuredBytes != null) {
    final requiredBytes = _readSnapshotErrorInt64(body, 'required_byte_count');
    if (requiredBytes != null && requiredBytes > configuredBytes) {
      throw SnapshotChunkTooSmallException(
        configuredBytes: configuredBytes,
        requiredBytes: requiredBytes,
      );
    }
    throw SnapshotHttpException(
      statusCode: response.statusCode,
      errorCode: 'invalid_error_response',
    );
  }
  if (response.statusCode == HttpStatus.conflict &&
      safeErrorCode == 'snapshot_session_limit_exceeded') {
    const dimensions = {'row_count', 'byte_count', 'row_byte_count'};
    final dimension = body?['dimension'];
    final actual = _readSnapshotErrorInt64(body, 'actual');
    final limit = _readSnapshotErrorInt64(body, 'limit');
    if (dimension is String &&
        dimensions.contains(dimension) &&
        actual != null &&
        limit != null &&
        actual >= 0 &&
        limit > 0 &&
        actual > limit) {
      throw SnapshotSessionLimitExceededException(
        dimension: dimension,
        actual: actual,
        limit: limit,
      );
    }
    throw SnapshotHttpException(
      statusCode: response.statusCode,
      errorCode: 'invalid_error_response',
    );
  }
  if (sourceId != null &&
      response.statusCode == HttpStatus.conflict &&
      safeErrorCode != 'invalid_error_response') {
    SourceRecoveryRequiredHttpException? recovery;
    try {
      recovery = _decodeSourceRecoveryRequired(
        response,
        expectedSourceId: sourceId,
        document: _DecodedErrorDocument(body: body),
      );
    } on OversqliteProtocolException {
      throw SnapshotHttpException(
        statusCode: response.statusCode,
        errorCode: 'invalid_error_response',
      );
    }
    if (recovery != null) {
      throw recovery;
    }
    if (safeErrorCode == 'source_replacement_invalid') {
      throw const SourceReplacementInvalidException(
        'server rejected the source replacement request',
      );
    }
  }
  throw SnapshotHttpException(
    statusCode: response.statusCode,
    errorCode: safeErrorCode,
  );
}

int? _readSnapshotErrorInt64(Map<String, Object?>? body, String field) {
  if (body == null) return null;
  try {
    return readWireOversqliteInt64(
      body,
      field,
      required: true,
      error: FormatException.new,
    );
  } catch (_) {
    return null;
  }
}

const _snapshotCapacityErrorCodes = {
  'snapshot_build_capacity',
  'snapshot_chunk_capacity',
};

const _knownSnapshotErrorCodes = {
  'history_pruned',
  'initialization_expired',
  'initialization_stale',
  'snapshot_build_capacity',
  'snapshot_chunk_capacity',
  'snapshot_chunk_too_small',
  'snapshot_session_limit_exceeded',
  'source_replacement_invalid',
  'source_retired',
  'source_sequence_changed',
  'source_sequence_out_of_order',
};

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
  _DecodedErrorDocument? decodedError,
}) {
  final errorResponse = decodedError == null
      ? _decodeErrorResponse(response.body)
      : decodedError.errorResponse;
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
  return _decodeErrorDocument(body).errorResponse;
}

final class _DecodedErrorDocument {
  const _DecodedErrorDocument({required this.body, this.errorResponse});

  final Map<String, Object?>? body;
  final ErrorResponse? errorResponse;
}

_DecodedErrorDocument _decodeErrorDocument(String body) {
  final decoded = _decodeJsonObject(body);
  if (decoded == null) {
    return const _DecodedErrorDocument(body: null);
  }
  try {
    return _DecodedErrorDocument(
      body: decoded,
      errorResponse: ErrorResponse.fromJson(decoded),
    );
  } catch (_) {
    return _DecodedErrorDocument(body: decoded);
  }
}

PushConflictException? _decodePushConflict(
  OversqliteHttpResponse response, {
  _DecodedErrorDocument? document,
}) {
  if (response.statusCode != HttpStatus.conflict) {
    return null;
  }
  final decoded = document == null
      ? _decodeJsonObject(response.body)
      : document.body;
  if (decoded == null || decoded['error'] != 'push_conflict') {
    return null;
  }
  final conflict = decoded['conflict'];
  if (conflict is! Map) {
    return null;
  }
  return PushConflictException(
    message: (decoded['message'] as String?) ?? 'push conflict',
    conflict: PushConflictDetails.fromJson(conflict.cast<String, Object?>()),
  );
}

SourceRecoveryRequiredHttpException? _decodeSourceRecoveryRequired(
  OversqliteHttpResponse response, {
  required String expectedSourceId,
  _DecodedErrorDocument? document,
}) {
  if (response.statusCode != HttpStatus.conflict) {
    return null;
  }
  final decoded = document == null
      ? _decodeJsonObject(response.body)
      : document.body;
  if (decoded == null) {
    return null;
  }
  final errorCode = decoded['error'];
  if (errorCode == 'source_retired') {
    if (decoded['message'] is! String || decoded['source_id'] is! String) {
      throw const OversqliteProtocolException(
        'source retired response is malformed',
      );
    }
    final responseSourceId = decoded['source_id']! as String;
    try {
      requireValidOversqliteSourceId(responseSourceId);
      if (responseSourceId != expectedSourceId) {
        throw const FormatException('source mismatch');
      }
    } catch (_) {
      throw const OversqliteProtocolException(
        'source retired response is malformed',
      );
    }
    String? replacementSourceId;
    if (decoded.containsKey('replaced_by_source_id')) {
      final replacement = decoded['replaced_by_source_id'];
      if (replacement is! String) {
        throw const OversqliteProtocolException(
          'source retired response is malformed',
        );
      }
      try {
        replacementSourceId = requireValidOversqliteSourceId(replacement);
      } catch (_) {
        throw const OversqliteProtocolException(
          'source retired response is malformed',
        );
      }
    }
    return sourceRecoveryRequiredHttpSignal(
      reason: SourceRecoveryReason.sourceRetired,
      replacementSourceId: replacementSourceId,
    );
  }
  final reason = switch (errorCode) {
    'history_pruned' => SourceRecoveryReason.historyPruned,
    'source_sequence_out_of_order' =>
      SourceRecoveryReason.sourceSequenceOutOfOrder,
    'source_sequence_changed' => SourceRecoveryReason.sourceSequenceChanged,
    _ => null,
  };
  if (reason == null) {
    return null;
  }
  return sourceRecoveryRequiredHttpSignal(reason: reason);
}

Map<String, Object?>? _decodeJsonObject(String body) {
  try {
    final decoded = jsonDecode(body);
    return decoded is Map ? decoded.cast<String, Object?>() : null;
  } on FormatException {
    return null;
  }
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
  required int maxRows,
  required int maxBytes,
}) {
  try {
    if (chunk.snapshotId != snapshotId ||
        chunk.snapshotBundleSeq != snapshotBundleSeq ||
        chunk.rows.length > maxRows ||
        chunk.byteCount < 0 ||
        chunk.byteCount > maxBytes ||
        (chunk.rows.isEmpty && chunk.byteCount != 0) ||
        (chunk.rows.isNotEmpty && chunk.byteCount == 0)) {
      throw const SnapshotSemanticException(
        SnapshotSemanticFailure.invalidChunk,
      );
    }
    if (chunk.rows.isNotEmpty && chunk.snapshotBundleSeq <= 0) {
      throw const OversqliteProtocolException(
        'snapshot chunk missing snapshot_bundle_seq for non-empty row set',
      );
    }
    if (chunk.hasMore && chunk.rows.isEmpty) {
      throw const OversqliteProtocolException(
        'snapshot chunk has_more=true must include at least one row',
      );
    }
    final expectedOrdinal = checkedAddOversqliteInt64(
      afterRowOrdinal,
      chunk.rows.length,
      'snapshot chunk next ordinal',
    );
    if (chunk.nextRowOrdinal != expectedOrdinal) {
      throw OversqliteProtocolException(
        'snapshot chunk next_row_ordinal ${chunk.nextRowOrdinal} does not match expected $expectedOrdinal',
      );
    }
    for (final row in chunk.rows) {
      validateSnapshotRow(row);
    }
  } on SnapshotSemanticException {
    rethrow;
  } on OversqliteProtocolException {
    rethrow;
  } catch (_) {
    throw const SnapshotSemanticException(SnapshotSemanticFailure.invalidChunk);
  }
}

int checkedSnapshotChunkBodyLimit(int maxBytes, int maxRows) {
  if (maxBytes <= 0 || maxRows <= 0) {
    throw ArgumentError('snapshot chunk body budget requires positive limits');
  }
  final withRows = checkedAddOversqliteInt64(
    maxBytes,
    maxRows,
    'snapshot chunk body budget',
  );
  return checkedAddOversqliteInt64(
    withRows,
    OversqliteRemoteApi.snapshotBodyEnvelopeBytes,
    'snapshot chunk body budget',
  );
}

String encodeOversqliteSessionIdPathSegment(String value) {
  final output = StringBuffer();
  const hexadecimal = '0123456789ABCDEF';
  for (final byte in utf8.encode(value)) {
    final unescaped =
        (byte >= 0x61 && byte <= 0x7a) ||
        (byte >= 0x41 && byte <= 0x5a) ||
        (byte >= 0x30 && byte <= 0x39) ||
        byte == 0x2d ||
        byte == 0x5f ||
        byte == 0x7e;
    if (unescaped) {
      output.writeCharCode(byte);
    } else {
      output
        ..write('%')
        ..write(hexadecimal[byte >> 4])
        ..write(hexadecimal[byte & 0x0f]);
    }
  }
  return output.toString();
}

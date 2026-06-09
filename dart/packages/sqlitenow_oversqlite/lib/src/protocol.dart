import 'dart:convert';
import 'dart:io';

const oversyncSourceIdHeaderName = 'Oversync-Source-ID';
const _hiddenSyncScopeColumnName = '_sync_scope_id';

final class OversqliteHttpResponse {
  const OversqliteHttpResponse({required this.statusCode, required this.body});

  final int statusCode;
  final String body;
}

abstract interface class OversqliteHttpClient {
  Future<OversqliteHttpResponse> get(String path, {required String sourceId});

  Future<OversqliteHttpResponse> postJson(
    String path, {
    required String sourceId,
    required Object? body,
  });

  Future<OversqliteHttpResponse> delete(
    String path, {
    required String sourceId,
  });
}

final class OversqliteBundleChangeWatchResponse {
  const OversqliteBundleChangeWatchResponse({
    required this.statusCode,
    required this.lines,
    required this.close,
    this.body = '',
  });

  final int statusCode;
  final Stream<String> lines;
  final Future<void> Function() close;
  final String body;
}

abstract interface class OversqliteBundleChangeWatchTransport {
  Future<OversqliteBundleChangeWatchResponse> watchBundleChanges({
    required String sourceId,
    required int afterBundleSeq,
  });
}

final class IoOversqliteHttpClient
    implements OversqliteHttpClient, OversqliteBundleChangeWatchTransport {
  IoOversqliteHttpClient({
    required Uri baseUri,
    HttpClient? httpClient,
    Map<String, String> defaultHeaders = const {},
  }) : _baseUri = _withTrailingSlash(baseUri),
       _httpClient = httpClient ?? HttpClient(),
       _defaultHeaders = Map.unmodifiable(defaultHeaders),
       _ownsHttpClient = httpClient == null;

  final Uri _baseUri;
  final HttpClient _httpClient;
  final Map<String, String> _defaultHeaders;
  final bool _ownsHttpClient;

  @override
  Future<OversqliteHttpResponse> get(String path, {required String sourceId}) {
    return _send(method: 'GET', path: path, sourceId: sourceId);
  }

  @override
  Future<OversqliteHttpResponse> postJson(
    String path, {
    required String sourceId,
    required Object? body,
  }) {
    return _send(method: 'POST', path: path, sourceId: sourceId, body: body);
  }

  @override
  Future<OversqliteHttpResponse> delete(
    String path, {
    required String sourceId,
  }) {
    return _send(method: 'DELETE', path: path, sourceId: sourceId);
  }

  @override
  Future<OversqliteBundleChangeWatchResponse> watchBundleChanges({
    required String sourceId,
    required int afterBundleSeq,
  }) async {
    final uri = _resolve('sync/watch?after_bundle_seq=$afterBundleSeq');
    final request = await _httpClient.openUrl('GET', uri);
    for (final entry in _defaultHeaders.entries) {
      request.headers.set(entry.key, entry.value);
    }
    request.headers.set(oversyncSourceIdHeaderName, sourceId);
    final response = await request.close();
    if (response.statusCode != 200) {
      return OversqliteBundleChangeWatchResponse(
        statusCode: response.statusCode,
        body: await utf8.decoder.bind(response).join(),
        lines: const Stream<String>.empty(),
        close: () async {},
      );
    }
    return OversqliteBundleChangeWatchResponse(
      statusCode: response.statusCode,
      lines: response.transform(utf8.decoder).transform(const LineSplitter()),
      close: () async {},
    );
  }

  void close({bool force = false}) {
    if (_ownsHttpClient) {
      _httpClient.close(force: force);
    }
  }

  Future<OversqliteHttpResponse> _send({
    required String method,
    required String path,
    required String sourceId,
    Object? body,
  }) async {
    final request = await _httpClient.openUrl(method, _resolve(path));
    for (final entry in _defaultHeaders.entries) {
      request.headers.set(entry.key, entry.value);
    }
    request.headers.set(oversyncSourceIdHeaderName, sourceId);
    if (body != null) {
      request.headers.contentType = ContentType.json;
      request.write(jsonEncode(body));
    }
    final response = await request.close();
    return OversqliteHttpResponse(
      statusCode: response.statusCode,
      body: await utf8.decoder.bind(response).join(),
    );
  }

  Uri _resolve(String path) {
    return _baseUri.resolve(path.startsWith('/') ? path.substring(1) : path);
  }

  static Uri _withTrailingSlash(Uri uri) {
    if (uri.path.endsWith('/')) {
      return uri;
    }
    return uri.replace(path: '${uri.path}/');
  }
}

final class OversqliteHttpException implements Exception {
  const OversqliteHttpException({
    required this.operation,
    required this.method,
    required this.path,
    required this.statusCode,
    required this.body,
    this.errorResponse,
  });

  final String operation;
  final String method;
  final String path;
  final int statusCode;
  final String body;
  final ErrorResponse? errorResponse;

  @override
  String toString() =>
      '$operation failed: HTTP $statusCode${body.isEmpty ? '' : ' - $body'}';
}

final class ErrorResponse {
  const ErrorResponse({required this.error, required this.message});

  final String error;
  final String message;

  factory ErrorResponse.fromJson(Map<String, Object?> json) {
    return ErrorResponse(
      error: (json['error'] as String?) ?? '',
      message: (json['message'] as String?) ?? '',
    );
  }
}

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

final class BundleCapabilitiesLimits {
  const BundleCapabilitiesLimits({
    required this.maxRowsPerBundle,
    required this.maxBytesPerBundle,
    required this.maxBundlesPerPull,
    required this.defaultRowsPerPushChunk,
    required this.maxRowsPerPushChunk,
    required this.pushSessionTtlSeconds,
    required this.defaultRowsPerCommittedBundleChunk,
    required this.maxRowsPerCommittedBundleChunk,
    required this.defaultRowsPerSnapshotChunk,
    required this.maxRowsPerSnapshotChunk,
    required this.snapshotSessionTtlSeconds,
    required this.maxRowsPerSnapshotSession,
    required this.maxBytesPerSnapshotSession,
    required this.initializationLeaseTtlSeconds,
  });

  final int maxRowsPerBundle;
  final int maxBytesPerBundle;
  final int maxBundlesPerPull;
  final int defaultRowsPerPushChunk;
  final int maxRowsPerPushChunk;
  final int pushSessionTtlSeconds;
  final int defaultRowsPerCommittedBundleChunk;
  final int maxRowsPerCommittedBundleChunk;
  final int defaultRowsPerSnapshotChunk;
  final int maxRowsPerSnapshotChunk;
  final int snapshotSessionTtlSeconds;
  final int maxRowsPerSnapshotSession;
  final int maxBytesPerSnapshotSession;
  final int initializationLeaseTtlSeconds;

  factory BundleCapabilitiesLimits.fromJson(Map<String, Object?> json) {
    return BundleCapabilitiesLimits(
      maxRowsPerBundle: _readInt(json, 'max_rows_per_bundle'),
      maxBytesPerBundle: _readInt(json, 'max_bytes_per_bundle'),
      maxBundlesPerPull: _readInt(json, 'max_bundles_per_pull'),
      defaultRowsPerPushChunk: _readInt(json, 'default_rows_per_push_chunk'),
      maxRowsPerPushChunk: _readInt(json, 'max_rows_per_push_chunk'),
      pushSessionTtlSeconds: _readInt(json, 'push_session_ttl_seconds'),
      defaultRowsPerCommittedBundleChunk: _readInt(
        json,
        'default_rows_per_committed_bundle_chunk',
      ),
      maxRowsPerCommittedBundleChunk: _readInt(
        json,
        'max_rows_per_committed_bundle_chunk',
      ),
      defaultRowsPerSnapshotChunk: _readInt(
        json,
        'default_rows_per_snapshot_chunk',
      ),
      maxRowsPerSnapshotChunk: _readInt(json, 'max_rows_per_snapshot_chunk'),
      snapshotSessionTtlSeconds: _readInt(json, 'snapshot_session_ttl_seconds'),
      maxRowsPerSnapshotSession: _readInt(
        json,
        'max_rows_per_snapshot_session',
      ),
      maxBytesPerSnapshotSession: _readInt(
        json,
        'max_bytes_per_snapshot_session',
      ),
      initializationLeaseTtlSeconds: _readInt(
        json,
        'initialization_lease_ttl_seconds',
      ),
    );
  }
}

final class CapabilitiesResponse {
  const CapabilitiesResponse({
    this.protocolVersion = '',
    this.schemaVersion = 0,
    this.features = const {},
    this.bundleLimits,
  });

  final String protocolVersion;
  final int schemaVersion;
  final Map<String, bool> features;
  final BundleCapabilitiesLimits? bundleLimits;

  bool get connectLifecycleSupported => features['connect_lifecycle'] ?? false;

  bool get bundleChangeWatchSupported =>
      features['bundle_change_watch'] ?? false;

  factory CapabilitiesResponse.fromJson(Map<String, Object?> json) {
    final rawFeatures = json['features'];
    return CapabilitiesResponse(
      protocolVersion: (json['protocol_version'] as String?) ?? '',
      schemaVersion: (json['schema_version'] as int?) ?? 0,
      features: rawFeatures is Map
          ? {
              for (final entry in rawFeatures.entries)
                entry.key.toString(): entry.value == true,
            }
          : const {},
      bundleLimits: json['bundle_limits'] is Map
          ? BundleCapabilitiesLimits.fromJson(
              (json['bundle_limits'] as Map).cast<String, Object?>(),
            )
          : null,
    );
  }
}

final class BundleChangeEvent {
  const BundleChangeEvent({
    required this.bundleSeq,
    this.sourceId = '',
    this.sourceBundleId = 0,
  });

  final int bundleSeq;
  final String sourceId;
  final int sourceBundleId;

  factory BundleChangeEvent.fromJson(Map<String, Object?> json) {
    final bundleSeq = json['bundle_seq'];
    if (bundleSeq is! int) {
      throw const OversqliteProtocolException(
        'bundle change event is missing bundle_seq',
      );
    }
    if (bundleSeq <= 0) {
      throw OversqliteProtocolException(
        'bundle change event bundle_seq $bundleSeq must be positive',
      );
    }
    return BundleChangeEvent(
      bundleSeq: bundleSeq,
      sourceId: (json['source_id'] as String?) ?? '',
      sourceBundleId: (json['source_bundle_id'] as int?) ?? 0,
    );
  }

  @override
  bool operator ==(Object other) {
    return other is BundleChangeEvent &&
        other.bundleSeq == bundleSeq &&
        other.sourceId == sourceId &&
        other.sourceBundleId == sourceBundleId;
  }

  @override
  int get hashCode => Object.hash(bundleSeq, sourceId, sourceBundleId);
}

Stream<BundleChangeEvent> parseBundleChangeEventStream(Stream<String> lines) {
  final parser = _BundleChangeWatchSseParser();
  return lines.expand(parser.accept);
}

List<BundleChangeEvent> parseBundleChangeEventLines(Iterable<String> lines) {
  final parser = _BundleChangeWatchSseParser();
  final events = <BundleChangeEvent>[];
  for (final line in lines) {
    events.addAll(parser.accept(line));
  }
  parser.finish();
  return events;
}

final class _BundleChangeWatchSseParser {
  String _eventName = '';
  final List<String> _dataLines = [];

  Iterable<BundleChangeEvent> accept(String line) sync* {
    if (line.isEmpty) {
      final event = _dispatch();
      if (event != null) {
        yield event;
      }
      return;
    }
    if (line.startsWith(':')) {
      return;
    }

    final separator = line.indexOf(':');
    final field = separator < 0 ? line : line.substring(0, separator);
    var value = separator < 0 ? '' : line.substring(separator + 1);
    if (value.startsWith(' ')) {
      value = value.substring(1);
    }

    switch (field) {
      case 'event':
        _eventName = value;
      case 'data':
        _dataLines.add(value);
    }
  }

  void finish() {
    _eventName = '';
    _dataLines.clear();
  }

  BundleChangeEvent? _dispatch() {
    final shouldEmit = _eventName == 'bundle';
    final data = _dataLines.join('\n');
    finish();
    if (!shouldEmit) {
      return null;
    }
    if (data.isEmpty) {
      throw const OversqliteProtocolException(
        'bundle change event is missing data',
      );
    }
    final decoded = jsonDecode(data);
    if (decoded is! Map) {
      throw const OversqliteProtocolException(
        'bundle change event data must be a JSON object',
      );
    }
    return BundleChangeEvent.fromJson(decoded.cast<String, Object?>());
  }
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
    String? initializationId,
  }) async {
    final response = await _http.postJson(
      'sync/push-sessions',
      sourceId: sourceId,
      body: {
        'source_bundle_id': sourceBundleId,
        'planned_row_count': plannedRowCount,
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

int _readInt(Map<String, Object?> json, String key) {
  final value = json[key];
  if (value is int) {
    return value;
  }
  return 0;
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
    default:
      throw OversqliteProtocolException(
        'push session response returned unsupported status ${response.status}',
      );
  }
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
  for (var index = 0; index < chunk.rows.length; index++) {
    try {
      _validateSnapshotRow(chunk.rows[index]);
    } on OversqliteProtocolException catch (error) {
      throw OversqliteProtocolException(
        'invalid snapshot row $index: ${error.message}',
      );
    }
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

import 'dart:convert';
import 'dart:io';

import 'payload_codec.dart';
import 'protocol_models.dart';

const oversyncSourceIdHeaderName = 'Oversync-Source-ID';

final class OversqliteHttpResponse {
  const OversqliteHttpResponse({
    required this.statusCode,
    required this.body,
    this.headers = const {},
    this.decodedBodyBytes,
    Future<void> Function()? close,
  }) : _close = close;

  final int statusCode;
  final String body;
  final Map<String, String> headers;
  final int? decodedBodyBytes;
  final Future<void> Function()? _close;

  String? header(String name) {
    final normalized = name.toLowerCase();
    return headers[normalized] ??
        headers.entries
            .where((entry) => entry.key.toLowerCase() == normalized)
            .map((entry) => entry.value)
            .firstOrNull;
  }

  Future<void> close() async {
    await _close?.call();
  }
}

final class OversqliteHttpRequestBounds {
  const OversqliteHttpRequestBounds({
    required this.errorBodyBytes,
    this.successBodyBytes,
    this.discardBody = false,
  }) : assert(errorBodyBytes >= 0),
       assert(successBodyBytes == null || successBodyBytes >= 0);

  final int? successBodyBytes;
  final int errorBodyBytes;
  final bool discardBody;

  int? limitForStatus(int statusCode) =>
      statusCode == HttpStatus.ok ? successBodyBytes : errorBodyBytes;
}

abstract interface class OversqliteHttpClient {
  Future<OversqliteHttpResponse> get(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  });

  Future<OversqliteHttpResponse> postJson(
    String path, {
    required String sourceId,
    required Object? body,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  });

  Future<OversqliteHttpResponse> delete(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
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
       _ownsHttpClient = httpClient == null {
    _httpClient.autoUncompress = false;
  }

  final Uri _baseUri;
  final HttpClient _httpClient;
  final Map<String, String> _defaultHeaders;
  final bool _ownsHttpClient;

  @override
  Future<OversqliteHttpResponse> get(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) {
    return _send(
      method: 'GET',
      path: path,
      sourceId: sourceId,
      operation: operation,
      bounds: bounds,
    );
  }

  @override
  Future<OversqliteHttpResponse> postJson(
    String path, {
    required String sourceId,
    required Object? body,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) {
    return _send(
      method: 'POST',
      path: path,
      sourceId: sourceId,
      body: body,
      operation: operation,
      bounds: bounds,
    );
  }

  @override
  Future<OversqliteHttpResponse> delete(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) {
    return _send(
      method: 'DELETE',
      path: path,
      sourceId: sourceId,
      operation: operation,
      bounds: bounds,
    );
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
    Future<void>? closeFuture;
    Future<void> closeResponse() {
      return closeFuture ??= () async {
        request.abort();
        final socket = await response.detachSocket();
        socket.destroy();
      }();
    }

    return OversqliteBundleChangeWatchResponse(
      statusCode: response.statusCode,
      lines: response.transform(utf8.decoder).transform(const LineSplitter()),
      close: closeResponse,
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
    required String operation,
    required OversqliteHttpRequestBounds bounds,
    Object? body,
  }) async {
    HttpClientRequest? request;
    var aborted = false;
    void abortBestEffort() {
      final activeRequest = request;
      if (activeRequest == null || aborted) return;
      aborted = true;
      try {
        activeRequest.abort();
      } catch (_) {}
    }

    try {
      final activeRequest = await _httpClient.openUrl(method, _resolve(path));
      request = activeRequest;
      for (final entry in _defaultHeaders.entries) {
        activeRequest.headers.set(entry.key, entry.value);
      }
      activeRequest.headers.set(oversyncSourceIdHeaderName, sourceId);
      activeRequest.headers.set(
        HttpHeaders.acceptEncodingHeader,
        'gzip, identity',
      );
      if (body != null) {
        activeRequest.headers.contentType = ContentType.json;
        activeRequest.write(canonicalizeOversqliteProtocolJson(body));
      }
      final response = await activeRequest.close();
      final headers = <String, String>{};
      response.headers.forEach((name, values) {
        headers[name.toLowerCase()] = values.join(',');
      });
      if (bounds.discardBody) {
        abortBestEffort();
        return OversqliteHttpResponse(
          statusCode: response.statusCode,
          body: '',
          headers: Map.unmodifiable(headers),
          decodedBodyBytes: 0,
        );
      }
      final encoding = response.headers
          .value(HttpHeaders.contentEncodingHeader)
          ?.trim()
          .toLowerCase();
      if (encoding != null &&
          encoding.isNotEmpty &&
          encoding != 'identity' &&
          encoding != 'gzip') {
        throw SnapshotUnsupportedContentEncodingException(operation: operation);
      }
      final Stream<List<int>> decoded = encoding == 'gzip'
          ? gzip.decoder.bind(response)
          : response;
      final decodedBody = await _readDecodedBody(
        decoded,
        operation: operation,
        limit: bounds.limitForStatus(response.statusCode),
      );
      return OversqliteHttpResponse(
        statusCode: response.statusCode,
        body: decodedBody.body,
        headers: Map.unmodifiable(headers),
        decodedBodyBytes: decodedBody.byteCount,
      );
    } catch (error) {
      abortBestEffort();
      if (error is SnapshotResponseBodyTooLargeException ||
          error is SnapshotUnsupportedContentEncodingException ||
          error is SnapshotResponseDecodeException ||
          error is OversqliteProtocolException) {
        rethrow;
      }
      throw const OversqliteProtocolException('snapshot transport failed');
    }
  }

  Future<({String body, int byteCount})> _readDecodedBody(
    Stream<List<int>> stream, {
    required String operation,
    required int? limit,
  }) async {
    var byteCount = 0;
    final body = StringBuffer();
    final decoder = utf8.decoder.startChunkedConversion(
      StringConversionSink.fromStringSink(body),
    );
    try {
      await for (final chunk in stream) {
        if (limit != null && chunk.length > limit - byteCount) {
          byteCount = limit + 1;
          throw SnapshotResponseBodyTooLargeException(
            operation: operation,
            limit: limit,
          );
        }
        byteCount += chunk.length;
        decoder.add(chunk);
      }
      decoder.close();
      return (body: body.toString(), byteCount: byteCount);
    } on SnapshotResponseBodyTooLargeException {
      rethrow;
    } on FormatException {
      throw SnapshotResponseDecodeException(operation);
    }
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

class OversqliteHttpException implements Exception {
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
  const ErrorResponse({
    required this.error,
    required this.message,
    this.requiredByteCount,
  });

  final String error;
  final String message;
  final int? requiredByteCount;

  factory ErrorResponse.fromJson(Map<String, Object?> json) {
    return ErrorResponse(
      error: (json['error'] as String?) ?? '',
      message: (json['message'] as String?) ?? '',
      requiredByteCount: json['required_byte_count'] as int?,
    );
  }
}

final class SnapshotHttpException extends OversqliteHttpException {
  const SnapshotHttpException({
    required super.statusCode,
    required this.errorCode,
  }) : super(
         operation: 'snapshot request',
         method: '',
         path: '',
         body: '',
         errorResponse: const ErrorResponse(error: '', message: ''),
       );

  final String errorCode;

  @override
  String toString() =>
      'snapshot request failed: HTTP $statusCode error=$errorCode';
}

final class SnapshotCapacityException implements Exception {
  const SnapshotCapacityException({
    required this.statusCode,
    required this.errorCode,
    this.retryAfter,
  });

  final int statusCode;
  final String errorCode;
  final Duration? retryAfter;

  @override
  String toString() =>
      'snapshot request temporarily unavailable: HTTP $statusCode error=$errorCode';
}

final class SnapshotCapacityRetryExhaustedException implements Exception {
  const SnapshotCapacityRetryExhaustedException({
    required this.operation,
    required this.errorCode,
    required this.waited,
  });

  final String operation;
  final String errorCode;
  final Duration waited;

  @override
  String toString() =>
      'oversqlite snapshot capacity retry exhausted for $operation';
}

final class SnapshotCapacityRetryCancelledException implements Exception {
  const SnapshotCapacityRetryCancelledException();

  @override
  String toString() => 'oversqlite snapshot capacity retry was cancelled';
}

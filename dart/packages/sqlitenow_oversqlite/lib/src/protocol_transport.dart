import 'dart:convert';
import 'dart:io';

const oversyncSourceIdHeaderName = 'Oversync-Source-ID';

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

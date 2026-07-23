import 'dart:async';
import 'dart:io';

import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';

import 'sample_auth.dart';

final class SampleSyncTransport
    implements OversqliteHttpClient, OversqliteBundleChangeWatchTransport {
  SampleSyncTransport({
    required Uri baseUri,
    required SampleAuthSession authSession,
  }) : _baseUri = baseUri,
       _authSession = authSession;

  final Uri _baseUri;
  final SampleAuthSession _authSession;
  final Set<IoOversqliteHttpClient> _watchClients = {};
  bool _closed = false;

  @override
  Future<OversqliteHttpResponse> get(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) {
    return _send(
      (client) => client.get(
        path,
        sourceId: sourceId,
        operation: operation,
        bounds: bounds,
      ),
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
      (client) => client.postJson(
        path,
        sourceId: sourceId,
        body: body,
        operation: operation,
        bounds: bounds,
      ),
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
      (client) => client.delete(
        path,
        sourceId: sourceId,
        operation: operation,
        bounds: bounds,
      ),
    );
  }

  Future<OversqliteHttpResponse> _send(
    Future<OversqliteHttpResponse> Function(IoOversqliteHttpClient client)
    operation,
  ) async {
    _ensureOpen();
    final firstToken = await _authSession.bearerToken();
    final first = await _sendOnce(firstToken, operation);
    if (first.statusCode != HttpStatus.unauthorized) return first;
    await first.close();
    final nextToken = await _authSession.bearerToken(
      forceRefresh: true,
      failedToken: firstToken,
    );
    return _sendOnce(nextToken, operation);
  }

  Future<OversqliteHttpResponse> _sendOnce(
    String token,
    Future<OversqliteHttpResponse> Function(IoOversqliteHttpClient client)
    operation,
  ) async {
    final client = _newClient(token);
    try {
      return await operation(client);
    } finally {
      client.close(force: true);
    }
  }

  @override
  Future<OversqliteBundleChangeWatchResponse> watchBundleChanges({
    required String sourceId,
    required int afterBundleSeq,
  }) async {
    _ensureOpen();
    final firstToken = await _authSession.bearerToken();
    final first = await _watchOnce(
      firstToken,
      sourceId: sourceId,
      afterBundleSeq: afterBundleSeq,
    );
    if (first.response.statusCode != HttpStatus.unauthorized) {
      return _ownWatch(first);
    }
    await first.response.close();
    first.client.close(force: true);
    final nextToken = await _authSession.bearerToken(
      forceRefresh: true,
      failedToken: firstToken,
    );
    return _ownWatch(
      await _watchOnce(
        nextToken,
        sourceId: sourceId,
        afterBundleSeq: afterBundleSeq,
      ),
    );
  }

  Future<
    ({
      IoOversqliteHttpClient client,
      OversqliteBundleChangeWatchResponse response,
    })
  >
  _watchOnce(
    String token, {
    required String sourceId,
    required int afterBundleSeq,
  }) async {
    final client = _newClient(token);
    try {
      final response = await client.watchBundleChanges(
        sourceId: sourceId,
        afterBundleSeq: afterBundleSeq,
      );
      return (client: client, response: response);
    } catch (_) {
      client.close(force: true);
      rethrow;
    }
  }

  OversqliteBundleChangeWatchResponse _ownWatch(
    ({
      IoOversqliteHttpClient client,
      OversqliteBundleChangeWatchResponse response,
    })
    owned,
  ) {
    _watchClients.add(owned.client);
    var watchClosed = false;
    Future<void> close() async {
      if (watchClosed) return;
      watchClosed = true;
      try {
        await owned.response.close();
      } finally {
        _watchClients.remove(owned.client);
        owned.client.close(force: true);
      }
    }

    return OversqliteBundleChangeWatchResponse(
      statusCode: owned.response.statusCode,
      lines: owned.response.lines,
      body: owned.response.body,
      close: close,
    );
  }

  IoOversqliteHttpClient _newClient(String token) {
    return IoOversqliteHttpClient(
      baseUri: _baseUri,
      defaultHeaders: {HttpHeaders.authorizationHeader: 'Bearer $token'},
    );
  }

  void close() {
    if (_closed) return;
    _closed = true;
    for (final client in _watchClients.toList(growable: false)) {
      client.close(force: true);
    }
    _watchClients.clear();
  }

  void _ensureOpen() {
    if (_closed) throw StateError('SampleSyncTransport is closed.');
  }
}

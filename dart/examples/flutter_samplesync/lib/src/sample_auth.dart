import 'dart:async';
import 'dart:convert';
import 'dart:io';

typedef SampleClock = DateTime Function();
typedef SampleHttpClientFactory = HttpClient Function();

final class SampleAuthToken {
  const SampleAuthToken({required this.value, required this.expiresAt});

  final String value;
  final DateTime expiresAt;
}

abstract interface class SampleAuthApi {
  Future<void> ensureSampleSyncServer();

  Future<SampleAuthToken> issueToken({
    required String user,
    required String sourceId,
    required String password,
  });
}

final class IoSampleAuthApi implements SampleAuthApi {
  IoSampleAuthApi({
    required Uri baseUri,
    SampleClock? now,
    SampleHttpClientFactory? httpClientFactory,
  }) : _baseUri = _withTrailingSlash(baseUri),
       _now = now ?? DateTime.now,
       _httpClientFactory = httpClientFactory ?? HttpClient.new;

  static const expectedAppName = 'samplesync-server';

  final Uri _baseUri;
  final SampleClock _now;
  final SampleHttpClientFactory _httpClientFactory;

  @override
  Future<void> ensureSampleSyncServer() async {
    final response = await _send('GET', 'syncx/status');
    if (response.statusCode != HttpStatus.ok) {
      throw StateError(
        'SampleSync server check failed: HTTP ${response.statusCode} '
        '${response.body}',
      );
    }
    final decoded = jsonDecode(response.body) as Map<String, Object?>;
    final appName = decoded['app_name'];
    if (appName != expectedAppName) {
      throw StateError(
        "Expected SampleSync server '$expectedAppName' at $_baseUri, "
        "but got '$appName'. Start examples/samplesync_server.",
      );
    }
  }

  @override
  Future<SampleAuthToken> issueToken({
    required String user,
    required String sourceId,
    required String password,
  }) async {
    final response = await _send(
      'POST',
      'dummy-signin',
      body: {'user': user, 'password': password, 'device': sourceId},
    );
    if (response.statusCode != HttpStatus.ok) {
      throw StateError(
        'Sample auth token request failed: HTTP ${response.statusCode} '
        '${response.body}',
      );
    }
    final decoded = jsonDecode(response.body) as Map<String, Object?>;
    final token = decoded['token'];
    final expiresIn = decoded['expires_in'];
    if (token is! String || token.isEmpty || expiresIn is! num) {
      throw const FormatException('Malformed SampleSync sign-in response.');
    }
    return SampleAuthToken(
      value: token,
      expiresAt: _now().toUtc().add(
        Duration(seconds: expiresIn.toInt().clamp(0, 1 << 31)),
      ),
    );
  }

  Future<({int statusCode, String body})> _send(
    String method,
    String path, {
    Object? body,
  }) async {
    final client = _httpClientFactory();
    client.connectionTimeout = const Duration(seconds: 10);
    try {
      final request = await client.openUrl(method, _baseUri.resolve(path));
      if (body != null) {
        request.headers.contentType = ContentType.json;
        request.write(jsonEncode(body));
      }
      final response = await request.close().timeout(
        const Duration(seconds: 30),
      );
      return (
        statusCode: response.statusCode,
        body: await utf8.decoder.bind(response).join(),
      );
    } finally {
      client.close(force: true);
    }
  }

  static Uri _withTrailingSlash(Uri uri) {
    return uri.path.endsWith('/') ? uri : uri.replace(path: '${uri.path}/');
  }
}

final class SampleAuthSession {
  SampleAuthSession({
    required SampleAuthApi api,
    required String user,
    required String sourceId,
    required String password,
    required SampleAuthToken initialToken,
    SampleClock? now,
  }) : _api = api,
       _user = user,
       _sourceId = sourceId,
       _password = password,
       _token = initialToken,
       _now = now ?? DateTime.now;

  static const refreshLead = Duration(seconds: 60);
  static const minimumRefreshDelay = Duration(seconds: 5);
  static const refreshRetryDelay = Duration(seconds: 30);

  final SampleAuthApi _api;
  final String _user;
  final String _sourceId;
  final String _password;
  final SampleClock _now;

  SampleAuthToken _token;
  Future<SampleAuthToken>? _refreshing;
  Timer? _refreshTimer;
  bool _closed = false;

  String get currentToken => _token.value;

  DateTime get expiresAt => _token.expiresAt;

  Future<String> bearerToken({
    bool forceRefresh = false,
    String? failedToken,
  }) async {
    _ensureOpen();
    if (forceRefresh && failedToken != null && failedToken != _token.value) {
      return _token.value;
    }
    if (forceRefresh || _shouldRefresh()) {
      await _refresh();
    }
    return _token.value;
  }

  void startProactiveRefresh(void Function(Object error) onError) {
    _ensureOpen();
    _refreshTimer?.cancel();
    _scheduleRefresh(onError);
  }

  Future<SampleAuthToken> _refresh() {
    final active = _refreshing;
    if (active != null) return active;
    final next = _api.issueToken(
      user: _user,
      sourceId: _sourceId,
      password: _password,
    );
    _refreshing = next;
    return next
        .then((token) {
          if (!_closed) _token = token;
          return token;
        })
        .whenComplete(() {
          _refreshing = null;
        });
  }

  bool _shouldRefresh() {
    return !_now().toUtc().isBefore(_token.expiresAt.subtract(refreshLead));
  }

  void _scheduleRefresh(void Function(Object error) onError) {
    if (_closed) return;
    final remaining = _token.expiresAt.difference(_now().toUtc());
    final lead = remaining ~/ 2 < refreshLead ? remaining ~/ 2 : refreshLead;
    final delay = remaining - lead;
    final effectiveDelay = delay < minimumRefreshDelay
        ? minimumRefreshDelay
        : delay;
    _refreshTimer = Timer(effectiveDelay, () async {
      try {
        await _refresh();
        _scheduleRefresh(onError);
      } catch (error) {
        onError(error);
        if (!_closed) {
          _refreshTimer = Timer(
            refreshRetryDelay,
            () => _scheduleRefresh(onError),
          );
        }
      }
    });
  }

  void close() {
    _closed = true;
    _refreshTimer?.cancel();
    _refreshTimer = null;
  }

  void _ensureOpen() {
    if (_closed) throw StateError('SampleAuthSession is closed.');
  }
}

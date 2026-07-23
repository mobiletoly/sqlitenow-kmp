import 'dart:async';

import 'package:flutter/foundation.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';

import 'db/generated/now_sample_sync_database.dart';
import 'sample_auth.dart';
import 'sample_models.dart';
import 'sample_repository.dart';
import 'sample_sync_transport.dart';
import 'session_preferences.dart';

const defaultSampleSyncBaseUrl = String.fromEnvironment(
  'SAMPLESYNC_BASE_URL',
  defaultValue: 'http://10.0.2.2:8080',
);

typedef SampleAuthApiFactory = SampleAuthApi Function(Uri baseUri);

enum SampleControllerState {
  opening,
  signedOut,
  signingIn,
  signedIn,
  signingOut,
  failed,
  closed,
}

final class UpdatedAtWinsResolver implements Resolver {
  const UpdatedAtWinsResolver();

  @override
  MergeResult resolve(ConflictContext conflict) {
    final local = _updatedAt(conflict.localPayload);
    final server = _updatedAt(conflict.serverRow);
    if (local != null && server != null && local.isAfter(server)) {
      return const KeepLocal();
    }
    return const AcceptServer();
  }

  DateTime? _updatedAt(Map<String, Object?>? row) {
    final value = row?['updated_at'];
    if (value is! String) return null;
    return DateTime.tryParse(value)?.toUtc();
  }
}

final class SampleSyncController extends ChangeNotifier {
  SampleSyncController({
    required SampleSyncRepository repository,
    required SampleSessionPreferences preferences,
    String baseUrl = defaultSampleSyncBaseUrl,
    SampleAuthApiFactory? authApiFactory,
  }) : _repository = repository,
       _preferences = preferences,
       baseUri = Uri.parse(baseUrl),
       _authApiFactory =
           authApiFactory ?? ((uri) => IoSampleAuthApi(baseUri: uri));

  factory SampleSyncController.persistent({
    String baseUrl = defaultSampleSyncBaseUrl,
  }) {
    return SampleSyncController(
      repository: SampleSyncRepository.persistent(),
      preferences: SharedPreferencesSampleSessionPreferences(),
      baseUrl: baseUrl,
    );
  }

  factory SampleSyncController.inMemory({
    String baseUrl = defaultSampleSyncBaseUrl,
    SampleSessionPreferences? preferences,
    SampleAuthApiFactory? authApiFactory,
  }) {
    return SampleSyncController(
      repository: SampleSyncRepository.inMemory(),
      preferences: preferences ?? MemorySampleSessionPreferences(),
      baseUrl: baseUrl,
      authApiFactory: authApiFactory,
    );
  }

  static const _attachAttempts = 3;

  final SampleSyncRepository _repository;
  final SampleSessionPreferences _preferences;
  final SampleAuthApiFactory _authApiFactory;
  final Uri baseUri;

  late final SampleAuthApi _authApi = _authApiFactory(baseUri);
  SampleControllerState _state = SampleControllerState.opening;
  SampleSyncMode _mode = SampleSyncMode.watch;
  String _username = '';
  String _sourceId = '';
  String? _errorMessage;
  String? _reportMessage;
  bool _skippedSignIn = false;
  bool _initialized = false;
  bool _closed = false;
  _SampleSyncSession? _session;
  Future<SyncReport>? _syncFuture;
  Timer? _localSyncTimer;

  SampleControllerState get state => _state;

  SampleSyncMode get mode => _mode;

  String get username => _username;

  String get sourceId => _sourceId;

  String? get errorMessage => _errorMessage;

  String? get reportMessage => _reportMessage;

  bool get initialized => _initialized;

  bool get signedIn => _state == SampleControllerState.signedIn;

  bool get busy =>
      _state == SampleControllerState.opening ||
      _state == SampleControllerState.signingIn ||
      _state == SampleControllerState.signingOut;

  bool get skippedSignIn => _skippedSignIn;

  Stream<List<PersonRow>> watchPeople() => _repository.watchPeople();

  Future<List<PersonRow>> listPeople() => _repository.listPeople();

  Stream<List<SampleComment>> watchComments(Uint8List personId) {
    return _repository.watchComments(personId);
  }

  Future<List<PersonAddressRow>> listAddresses(Uint8List personId) {
    return _repository.listAddresses(personId);
  }

  Future<void> initialize() async {
    if (_initialized || _closed) return;
    try {
      await _repository.open();
      final bootstrap = _newClient(mode: SampleSyncMode.polling);
      try {
        await bootstrap.open();
        _sourceId = (await bootstrap.sourceInfo()).currentSourceId;
      } finally {
        await bootstrap.close();
      }
      _mode = SampleSyncMode.fromStorage(await _preferences.readMode());
      _username = (await _preferences.readUsername()) ?? '';
      _initialized = true;
      _state = SampleControllerState.signedOut;
      _notify();

      if (_username.trim().isNotEmpty) {
        await signIn(_username, '', restoring: true);
      }
    } catch (error) {
      _initialized = true;
      _state = SampleControllerState.failed;
      _setError('Database open failed: $error');
    }
  }

  Future<void> signIn(
    String username,
    String password, {
    bool restoring = false,
  }) async {
    _ensureUsable();
    if (busy || signedIn) return;
    final user = username.trim().isEmpty ? 'user-sample' : username.trim();
    _state = SampleControllerState.signingIn;
    _errorMessage = null;
    _skippedSignIn = false;
    _notify();

    SampleAuthSession? authSession;
    SampleSyncTransport? transport;
    DefaultOversqliteClient? client;
    try {
      await _authApi.ensureSampleSyncServer();
      final token = await _authApi.issueToken(
        user: user,
        sourceId: _sourceId,
        password: password,
      );
      authSession = SampleAuthSession(
        api: _authApi,
        user: user,
        sourceId: _sourceId,
        password: password,
        initialToken: token,
      );
      transport = SampleSyncTransport(
        baseUri: baseUri,
        authSession: authSession,
      );
      client = _newClient(mode: _mode, transport: transport);
      await client.open();
      await _attachUntilConnected(client, user);
      await _initialSync(client);

      final session = _SampleSyncSession(
        client: client,
        transport: transport,
        authSession: authSession,
      );
      _session = session;
      _username = user;
      _state = SampleControllerState.signedIn;
      await _preferences.writeUsername(user);
      await _preferences.writeMode(_mode.storageValue);
      authSession.startProactiveRefresh((error) {
        _setError('Auth refresh failed: $error');
      });
      _startAutomaticDownloads(session);
      _notify();
    } catch (error) {
      await client?.close();
      transport?.close();
      authSession?.close();
      _session = null;
      _state = SampleControllerState.signedOut;
      _setError(
        restoring ? 'Session restore failed: $error' : 'Sign in failed: $error',
      );
    }
  }

  Future<void> setMode(SampleSyncMode nextMode) async {
    _ensureUsable();
    if (_mode == nextMode || busy) return;
    final previousMode = _mode;
    final active = _session;
    if (active == null) {
      _mode = nextMode;
      await _preferences.writeMode(nextMode.storageValue);
      _notify();
      return;
    }

    _mode = nextMode;
    _state = SampleControllerState.signingIn;
    _notify();
    await _stopAutomaticDownloads(active);
    DefaultOversqliteClient? candidate;
    try {
      candidate = _newClient(mode: nextMode, transport: active.transport);
      await candidate.open();
      await _attachUntilConnected(candidate, _username);
      await candidate.sync();
      final previousClient = active.client;
      active.client = candidate;
      await previousClient.close();
      await _preferences.writeMode(nextMode.storageValue);
      _state = SampleControllerState.signedIn;
      _startAutomaticDownloads(active);
      _notify();
    } catch (error) {
      await candidate?.close();
      _mode = previousMode;
      _state = SampleControllerState.signedIn;
      await _preferences.writeMode(previousMode.storageValue);
      _startAutomaticDownloads(active);
      _setError('Mode change failed: $error');
    }
  }

  void skipSignIn() {
    if (_closed) return;
    _skippedSignIn = true;
    _errorMessage = null;
    _notify();
  }

  Future<void> addRandomPerson() {
    return _runMutation(_repository.addRandomPerson);
  }

  Future<void> randomizePerson(PersonRow person) {
    return _runMutation(() => _repository.randomizePerson(person));
  }

  Future<void> addRandomAddress(Uint8List personId) {
    return _runMutation(() => _repository.addRandomAddress(personId));
  }

  Future<void> addRandomComment(Uint8List personId) {
    return _runMutation(() => _repository.addRandomComment(personId));
  }

  Future<void> deletePerson(Uint8List personId) {
    return _runMutation(() => _repository.deletePerson(personId));
  }

  Future<void> _runMutation(Future<void> Function() operation) async {
    _ensureUsable();
    try {
      await operation();
      _scheduleLocalSync();
    } catch (error) {
      _setError('Local database operation failed: $error');
      rethrow;
    }
  }

  Future<void> manualSync() async {
    _ensureUsable();
    if (!signedIn) {
      _setError('Not signed in.');
      return;
    }
    try {
      final report = await _sync();
      _reportMessage = _formatSyncReport(report);
      _errorMessage = null;
      _notify();
    } catch (error) {
      _setError('Sync failed: $error');
    }
  }

  Future<SyncReport> _sync() {
    final active = _syncFuture;
    if (active != null) return active;
    final session = _session;
    if (session == null) {
      return Future<SyncReport>.error(StateError('Not signed in.'));
    }
    final next = () async {
      await _stopAutomaticDownloads(session);
      try {
        await _authApi.ensureSampleSyncServer();
        return await session.client.sync();
      } finally {
        if (!_closed && identical(_session, session) && signedIn) {
          _startAutomaticDownloads(session);
        }
      }
    }();
    _syncFuture = next;
    return next.whenComplete(() {
      if (identical(_syncFuture, next)) _syncFuture = null;
    });
  }

  Future<void> signOut() async {
    _ensureUsable();
    final session = _session;
    if (session == null || busy) return;
    _state = SampleControllerState.signingOut;
    _notify();
    _localSyncTimer?.cancel();
    await _stopAutomaticDownloads(session);
    try {
      final result = await session.client.syncThenDetach();
      _reportMessage = _formatDetachReport(result);
      if (result.isSuccess) {
        await _closeSession(session);
        _session = null;
        await _preferences.clearUsername();
        _username = '';
        _state = SampleControllerState.signedOut;
        _skippedSignIn = false;
      } else {
        _state = SampleControllerState.signedIn;
        _startAutomaticDownloads(session);
        _errorMessage =
            'Sign out blocked: ${result.remainingPendingRowCount} '
            'pending row(s) still need sync.';
      }
      _notify();
    } catch (error) {
      _state = SampleControllerState.signedIn;
      _startAutomaticDownloads(session);
      _setError('Sign out failed: $error');
    }
  }

  Future<void> setForeground(bool foreground) async {
    final session = _session;
    if (session == null || _closed) return;
    if (!foreground) {
      await _stopAutomaticDownloads(session);
      return;
    }
    _startAutomaticDownloads(session);
    _scheduleLocalSync();
  }

  void clearError() {
    _errorMessage = null;
    _notify();
  }

  void clearReport() {
    _reportMessage = null;
    _notify();
  }

  void _scheduleLocalSync() {
    if (!signedIn) return;
    _localSyncTimer?.cancel();
    _localSyncTimer = Timer(const Duration(milliseconds: 700), () async {
      try {
        await _sync();
      } catch (error) {
        _setError('Automatic sync failed: $error');
      }
    });
  }

  Future<AttachConnected> _attachUntilConnected(
    OversqliteClient client,
    String user,
  ) async {
    for (var attempt = 0; attempt < _attachAttempts; attempt++) {
      final result = await client.attach(user);
      if (result is AttachConnected) return result;
      final retry = result as AttachRetryLater;
      if (attempt == _attachAttempts - 1) {
        throw StateError(
          'Attach kept asking to retry after $_attachAttempts attempts.',
        );
      }
      await Future<void>.delayed(
        Duration(
          seconds: retry.retryAfterSeconds < 1 ? 1 : retry.retryAfterSeconds,
        ),
      );
    }
    throw StateError('Attach attempts exhausted unexpectedly.');
  }

  Future<SyncReport> _initialSync(OversqliteClient client) async {
    try {
      return await client.sync();
    } on RebuildRequiredException {
      await client.rebuild();
      return client.sync();
    }
  }

  DefaultOversqliteClient _newClient({
    required SampleSyncMode mode,
    SampleSyncTransport? transport,
  }) {
    return DefaultOversqliteClient(
      database: _repository.database.runtimeDatabase,
      config: _repository.database.buildOversqliteConfig(
        schema: 'business',
        verboseLogs: true,
        automaticDownloadInterval: mode.automaticDownloadInterval,
        bundleChangeWatchMode: mode == SampleSyncMode.watch
            ? BundleChangeWatchMode.auto
            : BundleChangeWatchMode.off,
      ),
      httpClient: transport,
      resolver: const UpdatedAtWinsResolver(),
    );
  }

  void _startAutomaticDownloads(_SampleSyncSession session) {
    if (!signedIn || session.automaticDownloads != null) return;
    final handle = session.client.startAutomaticDownloads();
    session.automaticDownloads = handle;
    unawaited(_observeAutomaticDownloads(session, handle));
  }

  Future<void> _observeAutomaticDownloads(
    _SampleSyncSession session,
    AutomaticDownloadsHandle handle,
  ) async {
    try {
      await handle.done;
    } catch (error) {
      if (!_closed && identical(session.automaticDownloads, handle)) {
        _setError('Automatic downloads stopped: $error');
      }
    } finally {
      if (identical(session.automaticDownloads, handle)) {
        session.automaticDownloads = null;
      }
    }
  }

  Future<void> _stopAutomaticDownloads(_SampleSyncSession session) async {
    final handle = session.automaticDownloads;
    session.automaticDownloads = null;
    await handle?.stop();
  }

  Future<void> _closeSession(_SampleSyncSession session) async {
    await _stopAutomaticDownloads(session);
    await session.client.close();
    session.transport.close();
    session.authSession.close();
  }

  String _formatSyncReport(SyncReport report) {
    final message = StringBuffer()
      ..writeln('Push: ${report.pushOutcome}')
      ..writeln('Pull: ${report.remoteOutcome}')
      ..write('Pending rows: ${report.status.pending.pendingRowCount}');
    final restore = report.restore;
    if (restore != null) {
      message
        ..writeln()
        ..write(
          'Restored snapshot: bundle=${restore.bundleSeq}, '
          'rows=${restore.rowCount}',
        );
    }
    return message.toString();
  }

  String _formatDetachReport(SyncThenDetachResult result) {
    if (result.isSuccess) {
      return 'Signed out successfully after ${result.syncRounds} sync round(s).';
    }
    return 'Sign out stayed attached after ${result.syncRounds} sync round(s). '
        '${result.remainingPendingRowCount} pending row(s) still remain.';
  }

  void _setError(String message) {
    if (_closed) return;
    _errorMessage = message;
    _notify();
  }

  void _notify() {
    if (!_closed) notifyListeners();
  }

  void _ensureUsable() {
    if (_closed) throw StateError('SampleSyncController is closed.');
    if (!_initialized) {
      throw StateError(
        'SampleSyncController.initialize() must be called first.',
      );
    }
  }

  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    _state = SampleControllerState.closed;
    _localSyncTimer?.cancel();
    final session = _session;
    _session = null;
    if (session != null) await _closeSession(session);
    await _repository.close();
  }

  @override
  void dispose() {
    unawaited(close());
    super.dispose();
  }
}

final class _SampleSyncSession {
  _SampleSyncSession({
    required this.client,
    required this.transport,
    required this.authSession,
  });

  DefaultOversqliteClient client;
  final SampleSyncTransport transport;
  final SampleAuthSession authSession;
  AutomaticDownloadsHandle? automaticDownloads;
}

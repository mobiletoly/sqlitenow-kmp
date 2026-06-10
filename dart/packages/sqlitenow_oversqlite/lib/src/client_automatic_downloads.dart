part of 'client.dart';

Future<void> _runAutomaticDownloads(
  DefaultOversqliteClient client,
  _DefaultAutomaticDownloadsHandle handle,
) async {
  final backoff = _AutomaticDownloadBackoff(
    minDelay: client._config.bundleChangeWatchReconnectMin,
    maxDelay: client._config.bundleChangeWatchReconnectMax,
  );
  while (!handle.isStopped) {
    if (client._downloadsPaused) {
      await _automaticDownloadDelay(
        handle,
        client._config.automaticDownloadInterval,
      );
      continue;
    }
    try {
      if (await _shouldUseBundleChangeWatch(client, handle)) {
        await _runBundleChangeWatchIteration(client, handle);
        await backoff.delayNext(handle);
      } else {
        await _runAutomaticPollingIteration(client, handle);
        backoff.reset();
      }
    } catch (_) {
      await _runAutomaticPullToStable(client, 'automatic downloads fallback');
      await backoff.delayNext(handle);
    }
  }
}

Future<bool> _shouldUseBundleChangeWatch(
  DefaultOversqliteClient client,
  _DefaultAutomaticDownloadsHandle handle,
) async {
  if (client._config.bundleChangeWatchMode != BundleChangeWatchMode.auto ||
      client._watchTransport == null ||
      handle.isStopped) {
    return false;
  }
  try {
    final state = await _automaticDownloadState(
      client,
      'automatic downloads capability check',
    );
    final capabilities = await client._runExclusive(
      operation: OversqliteOperation.fetchCapabilities,
      phase: OversqlitePhase.fetchingCapabilities,
      block: () => client._fetchCapabilitiesInternal(state.sourceId),
    );
    return capabilities.bundleChangeWatchSupported;
  } catch (_) {
    return false;
  }
}

Future<void> _runAutomaticPollingIteration(
  DefaultOversqliteClient client,
  _DefaultAutomaticDownloadsHandle handle,
) async {
  if (!client._downloadsPaused) {
    await _runAutomaticPullToStable(client, 'automatic downloads polling');
  }
  await _automaticDownloadDelay(
    handle,
    client._config.automaticDownloadInterval,
  );
}

Future<void> _runBundleChangeWatchIteration(
  DefaultOversqliteClient client,
  _DefaultAutomaticDownloadsHandle handle,
) async {
  final transport = client._watchTransport;
  if (transport == null) {
    return;
  }
  final state = await _automaticDownloadState(client, 'bundle change watch');
  final response = await transport.watchBundleChanges(
    sourceId: state.sourceId,
    afterBundleSeq: state.lastBundleSeqSeen,
  );
  if (response.statusCode != 200) {
    final statusCode = response.statusCode;
    final body = response.body;
    await response.close();
    throw OversqliteHttpException(
      operation: 'bundle change watch',
      method: 'GET',
      path: 'sync/watch',
      statusCode: statusCode,
      body: body,
    );
  }
  final iterator = StreamIterator(parseBundleChangeEventStream(response.lines));
  handle.setActiveCancel(() async {
    await iterator.cancel();
    await response.close();
  });
  try {
    while (!handle.isStopped && await iterator.moveNext()) {
      final event = iterator.current;
      if (event.bundleSeq > 0 && !client._downloadsPaused) {
        await _runAutomaticPullToStable(client, 'bundle change watch event');
      }
    }
  } finally {
    await iterator.cancel();
    await response.close();
    handle.clearActiveCancel();
  }
  if (!handle.isStopped && !client._downloadsPaused) {
    await _runAutomaticPullToStable(client, 'bundle change watch reconnect');
  }
}

Future<_AutomaticDownloadState> _automaticDownloadState(
  DefaultOversqliteClient client,
  String operation,
) async {
  client._ensureConnected(operation);
  final attachment = await client._clientStateStore.loadAttachmentState();
  return _AutomaticDownloadState(
    sourceId: attachment.currentSourceId,
    lastBundleSeqSeen: attachment.lastBundleSeqSeen,
  );
}

Future<bool> _runAutomaticPullToStable(
  DefaultOversqliteClient client,
  String operation,
) async {
  try {
    await client.pullToStable();
    return true;
  } catch (error) {
    if (error is SyncOperationInProgressException ||
        error is ConnectRequiredException ||
        error is OpenRequiredException ||
        error is RemoteAttachDeferredException) {
      return false;
    }
    return false;
  }
}

Future<bool> _automaticDownloadDelay(
  _DefaultAutomaticDownloadsHandle handle,
  Duration duration,
) async {
  if (handle.isStopped) {
    return false;
  }
  await Future.any([Future<void>.delayed(duration), handle.stopSignal]);
  return !handle.isStopped;
}

final class _AutomaticDownloadState {
  const _AutomaticDownloadState({
    required this.sourceId,
    required this.lastBundleSeqSeen,
  });

  final String sourceId;
  final int lastBundleSeqSeen;
}

final class _DefaultAutomaticDownloadsHandle
    implements AutomaticDownloadsHandle {
  final _stopCompleter = Completer<void>();
  final _doneCompleter = Completer<void>();
  Future<void> Function()? _activeCancel;

  bool get isStopped => _stopCompleter.isCompleted;

  Future<void> get stopSignal => _stopCompleter.future;

  @override
  Future<void> get done => _doneCompleter.future;

  @override
  Future<void> stop() async {
    if (!_stopCompleter.isCompleted) {
      _stopCompleter.complete();
    }
    final activeCancel = _activeCancel;
    if (activeCancel != null) {
      await activeCancel();
    }
    await done;
  }

  void setActiveCancel(Future<void> Function() cancel) {
    if (isStopped) {
      unawaited(cancel());
      return;
    }
    _activeCancel = cancel;
  }

  void clearActiveCancel() {
    _activeCancel = null;
  }

  void completeDone() {
    if (!_doneCompleter.isCompleted) {
      _doneCompleter.complete();
    }
  }

  void completeDoneError(Object error, StackTrace stackTrace) {
    if (!_doneCompleter.isCompleted) {
      _doneCompleter.completeError(error, stackTrace);
    }
  }
}

final class _AutomaticDownloadBackoff {
  _AutomaticDownloadBackoff({required this.minDelay, required this.maxDelay});

  final Duration minDelay;
  final Duration maxDelay;
  Duration _next = Duration.zero;

  void reset() {
    _next = Duration.zero;
  }

  Future<void> delayNext(_DefaultAutomaticDownloadsHandle handle) async {
    _next = _next == Duration.zero
        ? minDelay
        : Duration(
            microseconds: min(
              maxDelay.inMicroseconds,
              _next.inMicroseconds * 2,
            ),
          );
    await Future.any([Future<void>.delayed(_next), handle.stopSignal]);
  }
}

void _validateAutomaticDownloadConfig(OversqliteConfig config) {
  if (config.automaticDownloadInterval <= Duration.zero) {
    throw ArgumentError.value(
      config.automaticDownloadInterval,
      'automaticDownloadInterval',
      'must be positive',
    );
  }
  if (config.bundleChangeWatchReconnectMin <= Duration.zero) {
    throw ArgumentError.value(
      config.bundleChangeWatchReconnectMin,
      'bundleChangeWatchReconnectMin',
      'must be positive',
    );
  }
  if (config.bundleChangeWatchReconnectMax <= Duration.zero) {
    throw ArgumentError.value(
      config.bundleChangeWatchReconnectMax,
      'bundleChangeWatchReconnectMax',
      'must be positive',
    );
  }
  if (config.bundleChangeWatchReconnectMax <
      config.bundleChangeWatchReconnectMin) {
    throw ArgumentError.value(
      config.bundleChangeWatchReconnectMax,
      'bundleChangeWatchReconnectMax',
      'must be greater than or equal to bundleChangeWatchReconnectMin',
    );
  }
}

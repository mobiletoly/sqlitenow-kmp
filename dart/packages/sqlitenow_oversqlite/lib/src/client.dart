import 'dart:async';
import 'dart:math';

import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';

import 'config.dart';
import 'download.dart';
import 'local_runtime.dart';
import 'protocol.dart';
import 'push.dart';
import 'resolver.dart';
import 'runtime_state_store.dart';

const _attachmentBindingAttached = 'attached';
const _operationKindNone = 'none';
const _operationKindRemoteReplace = 'remote_replace';
const _operationKindSourceRecovery = 'source_recovery';

final class OpenRequiredException implements Exception {
  const OpenRequiredException(this.operation);

  final String operation;

  @override
  String toString() => 'open() must be called before $operation';
}

final class ConnectRequiredException implements Exception {
  const ConnectRequiredException(this.operation);

  final String operation;

  @override
  String toString() =>
      'attach(userId) must complete successfully before $operation';
}

final class RemoteAttachDeferredException implements Exception {
  const RemoteAttachDeferredException();

  @override
  String toString() => 'remote attach requires an OversqliteHttpClient';
}

final class SyncOperationInProgressException implements Exception {
  const SyncOperationInProgressException();

  @override
  String toString() =>
      'another sync operation is already in progress for this client';
}

enum AttachOutcome {
  resumedAttachedState,
  usedRemoteState,
  seededFromLocal,
  startedEmpty,
}

sealed class AttachResult {
  const AttachResult();
}

final class AttachConnected extends AttachResult {
  const AttachConnected({required this.outcome, required this.status});

  final AttachOutcome outcome;
  final SyncStatus status;
}

final class AttachRetryLater extends AttachResult {
  const AttachRetryLater({required this.retryAfterSeconds});

  final int retryAfterSeconds;
}

enum DetachOutcome { detached, blockedUnsyncedData }

enum AuthorityStatus {
  pendingLocalSeed,
  authoritativeEmpty,
  authoritativeMaterialized,
}

final class PendingSyncStatus {
  const PendingSyncStatus({
    required this.hasPendingSyncData,
    required this.pendingRowCount,
    required this.blocksDetach,
  });

  final bool hasPendingSyncData;
  final int pendingRowCount;
  final bool blocksDetach;
}

final class SyncStatus {
  const SyncStatus({
    required this.authority,
    required this.pending,
    required this.lastBundleSeqSeen,
  });

  final AuthorityStatus authority;
  final PendingSyncStatus pending;
  final int lastBundleSeqSeen;
}

final class RemoteSyncReport {
  const RemoteSyncReport({
    required this.outcome,
    required this.status,
    this.restore,
  });

  final RemoteSyncOutcome outcome;
  final SyncStatus status;
  final RestoreSummary? restore;
}

final class SyncReport {
  const SyncReport({
    required this.pushOutcome,
    required this.remoteOutcome,
    required this.status,
    this.restore,
  });

  final PushOutcome pushOutcome;
  final RemoteSyncOutcome remoteOutcome;
  final SyncStatus status;
  final RestoreSummary? restore;
}

final class SyncThenDetachResult {
  const SyncThenDetachResult({
    required this.lastSync,
    required this.detach,
    required this.syncRounds,
    required this.remainingPendingRowCount,
  });

  final SyncReport lastSync;
  final DetachOutcome detach;
  final int syncRounds;
  final int remainingPendingRowCount;

  bool get isSuccess => detach == DetachOutcome.detached;
}

abstract interface class AutomaticDownloadsHandle {
  Future<void> stop();

  Future<void> get done;
}

final class SourceInfo {
  const SourceInfo({
    required this.currentSourceId,
    required this.rebuildRequired,
    required this.sourceRecoveryRequired,
    this.sourceRecoveryReason,
  });

  final String currentSourceId;
  final bool rebuildRequired;
  final bool sourceRecoveryRequired;
  final String? sourceRecoveryReason;
}

enum OversqliteOperation {
  open,
  fetchCapabilities,
  attach,
  pushPending,
  pullToStable,
  sync,
  syncThenDetach,
  rebuild,
  detach,
}

enum OversqlitePhase {
  opening,
  fetchingCapabilities,
  attaching,
  seeding,
  pushing,
  pulling,
  stagingRemoteState,
  applyingRemoteState,
  cleaningLocalState,
}

sealed class OversqliteProgress {
  const OversqliteProgress();
}

final class OversqliteIdle extends OversqliteProgress {
  const OversqliteIdle();
}

final class OversqliteActive extends OversqliteProgress {
  const OversqliteActive({required this.operation, required this.phase});

  final OversqliteOperation operation;
  final OversqlitePhase phase;
}

abstract interface class OversqliteClient {
  OversqliteProgress get progress;

  Stream<OversqliteProgress> get progressStream;

  bool get uploadsPaused;

  bool get downloadsPaused;

  Future<void> open();

  Future<SourceInfo> sourceInfo();

  Future<CapabilitiesResponse> fetchCapabilities();

  Future<void> pauseUploads();

  Future<void> resumeUploads();

  Future<void> pauseDownloads();

  Future<void> resumeDownloads();

  Future<AttachResult> attach(String userId);

  Future<SyncStatus> syncStatus();

  Future<PushReport> pushPending();

  Future<RemoteSyncReport> pullToStable();

  Future<SyncReport> sync();

  Future<SyncThenDetachResult> syncThenDetach();

  Future<RemoteSyncReport> rebuild();

  Future<DetachOutcome> detach();

  Future<void> close();
}

final class DefaultOversqliteClient implements OversqliteClient {
  DefaultOversqliteClient({
    required SqliteNowDatabase database,
    required OversqliteConfig config,
    OversqliteHttpClient? httpClient,
    Resolver resolver = const ServerWinsResolver(),
  }) : _database = database,
       _config = config,
       _runtime = OversqliteLocalRuntime(database: database, config: config),
       _remoteApi = httpClient == null ? null : OversqliteRemoteApi(httpClient),
       _watchTransport = httpClient is OversqliteBundleChangeWatchTransport
           ? httpClient as OversqliteBundleChangeWatchTransport
           : null,
       _resolver = resolver;

  final SqliteNowDatabase _database;
  final OversqliteConfig _config;
  final OversqliteLocalRuntime _runtime;
  final OversqliteRemoteApi? _remoteApi;
  final OversqliteBundleChangeWatchTransport? _watchTransport;
  final Resolver _resolver;
  final _progressController = StreamController<OversqliteProgress>.broadcast();

  OversqliteValidatedConfig? _validated;
  bool _opened = false;
  bool _sessionConnected = false;
  String? _currentUserId;
  bool _operationRunning = false;
  bool _uploadsPaused = false;
  bool _downloadsPaused = false;
  OversqliteProgress _progress = const OversqliteIdle();
  _DefaultAutomaticDownloadsHandle? _automaticDownloads;

  SqliteNowConnection get _connection => _database.connection;
  OversqliteApplyRunner get _applyRunner => OversqliteApplyRunner(_connection);
  OversqliteAttachmentStateStore get _attachmentStore =>
      OversqliteAttachmentStateStore(_connection);
  OversqliteSourceStateStore get _sourceStore =>
      OversqliteSourceStateStore(_connection);

  @override
  OversqliteProgress get progress => _progress;

  @override
  Stream<OversqliteProgress> get progressStream => _progressController.stream;

  @override
  bool get uploadsPaused => _uploadsPaused;

  @override
  bool get downloadsPaused => _downloadsPaused;

  @override
  Future<void> open() {
    return _runExclusive(
      operation: OversqliteOperation.open,
      phase: OversqlitePhase.opening,
      block: () async {
        _validated = await _runtime.initialize();
        final attachment = await _loadAttachmentState();
        _opened = true;
        _sessionConnected = false;
        _currentUserId =
            attachment.bindingState == _attachmentBindingAttached &&
                attachment.attachedUserId.isNotEmpty
            ? attachment.attachedUserId
            : null;
      },
    );
  }

  @override
  Future<SourceInfo> sourceInfo() async {
    _ensureOpened('sourceInfo()');
    final attachment = await _loadAttachmentState();
    final operation = await _loadOperationState();
    final sourceId = attachment.currentSourceId;
    if (sourceId.isEmpty) {
      throw const OpenRequiredException('sourceInfo()');
    }
    return SourceInfo(
      currentSourceId: sourceId,
      rebuildRequired:
          attachment.rebuildRequired || operation.kind == 'source_recovery',
      sourceRecoveryRequired: operation.kind == 'source_recovery',
      sourceRecoveryReason: operation.reason.isEmpty ? null : operation.reason,
    );
  }

  @override
  Future<CapabilitiesResponse> fetchCapabilities() {
    return _runExclusive(
      operation: OversqliteOperation.fetchCapabilities,
      phase: OversqlitePhase.fetchingCapabilities,
      block: () async {
        _ensureOpened('fetchCapabilities()');
        return _fetchCapabilitiesInternal(await _currentSourceId());
      },
    );
  }

  @override
  Future<void> pauseUploads() async {
    _uploadsPaused = true;
  }

  @override
  Future<void> resumeUploads() async {
    _uploadsPaused = false;
  }

  @override
  Future<void> pauseDownloads() async {
    _downloadsPaused = true;
  }

  @override
  Future<void> resumeDownloads() async {
    _downloadsPaused = false;
  }

  AutomaticDownloadsHandle startAutomaticDownloads() {
    _validateAutomaticDownloadConfig(_config);
    final active = _automaticDownloads;
    if (active != null && !active.isStopped) {
      return active;
    }
    final handle = _DefaultAutomaticDownloadsHandle();
    _automaticDownloads = handle;
    unawaited(
      _runAutomaticDownloads(handle)
          .then(
            (_) => handle.completeDone(),
            onError: (Object error, StackTrace stackTrace) {
              handle.completeDoneError(error, stackTrace);
            },
          )
          .whenComplete(() {
            if (identical(_automaticDownloads, handle)) {
              _automaticDownloads = null;
            }
          }),
    );
    return handle;
  }

  @override
  Future<AttachResult> attach(String userId) {
    return _runExclusive(
      operation: OversqliteOperation.attach,
      phase: OversqlitePhase.attaching,
      block: () async {
        final requestedUserId = userId.trim();
        if (requestedUserId.isEmpty) {
          throw ArgumentError('userId must be provided');
        }
        _ensureOpened('attach(userId)');
        final attachment = await _loadAttachmentState();
        final operation = await _loadOperationState();
        if (operation.kind == _operationKindRemoteReplace) {
          if (operation.targetUserId != requestedUserId) {
            throw ConnectLocalStateConflictException(
              'pending remote_replace belongs to scope "${operation.targetUserId}"',
            );
          }
          final result = await _executeSnapshotRebuild(
            userId: requestedUserId,
            sourceId: attachment.currentSourceId,
            rotatedSourceId: null,
            sourceReplacementReason: null,
            outboxMode: SnapshotRebuildOutboxMode.clearAll,
            persistRemoteReplaceOperation: true,
          );
          if (result.updatedTables.isNotEmpty) {
            _runtime.reportManagedTableChanges(result.updatedTables);
          }
          _sessionConnected = true;
          _currentUserId = requestedUserId;
          return AttachConnected(
            outcome: AttachOutcome.usedRemoteState,
            status: await _syncStatusInternal(),
          );
        }
        if (attachment.bindingState == _attachmentBindingAttached &&
            attachment.attachedUserId == requestedUserId) {
          _sessionConnected = true;
          _currentUserId = requestedUserId;
          return AttachConnected(
            outcome: AttachOutcome.resumedAttachedState,
            status: await _syncStatusInternal(),
          );
        }
        if (attachment.bindingState == _attachmentBindingAttached &&
            attachment.attachedUserId != requestedUserId) {
          throw ConnectBindingConflictException(
            attachedUserId: attachment.attachedUserId,
            requestedUserId: requestedUserId,
          );
        }

        final sourceId = attachment.currentSourceId;
        await _fetchCapabilitiesInternal(sourceId);
        final connect = await _remoteApiOrThrow().connect(
          sourceId: sourceId,
          hasLocalPendingRows: (await _pendingSyncStatus()).hasPendingSyncData,
        );
        switch (connect.resolution) {
          case 'retry_later':
            _sessionConnected = false;
            return AttachRetryLater(
              retryAfterSeconds: connect.retryAfterSeconds,
            );
          case 'initialize_empty':
            await _persistConnectedLifecycleState(
              userId: requestedUserId,
              sourceId: sourceId,
              initializationId: '',
            );
            _sessionConnected = true;
            _currentUserId = requestedUserId;
            return AttachConnected(
              outcome: AttachOutcome.startedEmpty,
              status: await _syncStatusInternal(),
            );
          case 'initialize_local':
            await _persistConnectedLifecycleState(
              userId: requestedUserId,
              sourceId: sourceId,
              initializationId: connect.initializationId,
            );
            _sessionConnected = true;
            _currentUserId = requestedUserId;
            return AttachConnected(
              outcome: AttachOutcome.seededFromLocal,
              status: await _syncStatusInternal(),
            );
          case 'remote_authoritative':
            await _beginRemoteReplace(
              userId: requestedUserId,
              sourceId: sourceId,
            );
            _currentUserId = requestedUserId;
            final result = await _executeSnapshotRebuild(
              userId: requestedUserId,
              sourceId: sourceId,
              rotatedSourceId: null,
              sourceReplacementReason: null,
              outboxMode: SnapshotRebuildOutboxMode.clearAll,
              persistRemoteReplaceOperation: true,
            );
            if (result.updatedTables.isNotEmpty) {
              _runtime.reportManagedTableChanges(result.updatedTables);
            }
            _sessionConnected = true;
            return AttachConnected(
              outcome: AttachOutcome.usedRemoteState,
              status: await _syncStatusInternal(),
            );
          default:
            throw OversqliteProtocolException(
              'unexpected connect resolution ${connect.resolution}',
            );
        }
      },
    );
  }

  @override
  Future<SyncStatus> syncStatus() async {
    _ensureConnected('syncStatus()');
    return _syncStatusInternal();
  }

  @override
  Future<PushReport> pushPending() {
    return _runExclusive(
      operation: OversqliteOperation.pushPending,
      phase: OversqlitePhase.pushing,
      block: () async {
        _ensureConnected('pushPending()');
        final attachment = await _loadAttachmentState();
        final operation = await _loadOperationState();
        if (operation.kind == _operationKindSourceRecovery) {
          throw SourceRecoveryRequiredException(
            _sourceRecoveryReasonFromPersisted(operation.reason),
          );
        }
        if (attachment.rebuildRequired) {
          throw const RebuildRequiredException();
        }
        final report = await _executePush(attachment);
        if (report.updatedTables.isNotEmpty) {
          _runtime.reportManagedTableChanges(report.updatedTables);
        }
        return report;
      },
    );
  }

  @override
  Future<RemoteSyncReport> pullToStable() {
    return _runExclusive(
      operation: OversqliteOperation.pullToStable,
      phase: OversqlitePhase.pulling,
      block: () async {
        _ensureConnected('pullToStable()');
        final result = await _executePullToStable();
        if (result.updatedTables.isNotEmpty) {
          _runtime.reportManagedTableChanges(result.updatedTables);
        }
        return RemoteSyncReport(
          outcome: result.outcome,
          status: await _syncStatusInternal(),
          restore: result.restore,
        );
      },
    );
  }

  @override
  Future<SyncReport> sync() {
    return _runExclusive(
      operation: OversqliteOperation.sync,
      phase: OversqlitePhase.pushing,
      block: () async {
        _ensureConnected('sync()');
        final attachment = await _loadAttachmentState();
        final operation = await _loadOperationState();
        if (operation.kind == _operationKindSourceRecovery) {
          throw SourceRecoveryRequiredException(
            _sourceRecoveryReasonFromPersisted(operation.reason),
          );
        }
        if (attachment.rebuildRequired) {
          throw const RebuildRequiredException();
        }
        final pushReport = await _executePush(attachment);
        _setProgress(
          const OversqliteActive(
            operation: OversqliteOperation.sync,
            phase: OversqlitePhase.pulling,
          ),
        );
        final remoteResult = await _executePullToStable();
        final updated = {
          ...pushReport.updatedTables,
          ...remoteResult.updatedTables,
        };
        if (updated.isNotEmpty) {
          _runtime.reportManagedTableChanges(updated);
        }
        return SyncReport(
          pushOutcome: pushReport.outcome,
          remoteOutcome: remoteResult.outcome,
          status: await _syncStatusInternal(),
          restore: remoteResult.restore,
        );
      },
    );
  }

  @override
  Future<SyncThenDetachResult> syncThenDetach() {
    return _runExclusive(
      operation: OversqliteOperation.syncThenDetach,
      phase: OversqlitePhase.pushing,
      block: () async {
        _ensureConnected('syncThenDetach()');
        var previousPendingRowCount = 0x7fffffffffffffff;
        SyncReport? lastReport;
        for (var round = 1; round <= 3; round++) {
          final attachment = await _loadAttachmentState();
          final pushReport = await _executePush(attachment);
          _setProgress(
            const OversqliteActive(
              operation: OversqliteOperation.syncThenDetach,
              phase: OversqlitePhase.pulling,
            ),
          );
          final remoteResult = await _executePullToStable();
          lastReport = SyncReport(
            pushOutcome: pushReport.outcome,
            remoteOutcome: remoteResult.outcome,
            status: await _syncStatusInternal(),
            restore: remoteResult.restore,
          );
          final updated = {
            ...pushReport.updatedTables,
            ...remoteResult.updatedTables,
          };
          if (updated.isNotEmpty) {
            _runtime.reportManagedTableChanges(updated);
          }
          final detachOutcome = await _executeDetach();
          if (detachOutcome == DetachOutcome.detached) {
            return SyncThenDetachResult(
              lastSync: lastReport,
              detach: detachOutcome,
              syncRounds: round,
              remainingPendingRowCount: 0,
            );
          }
          final pending = await _pendingSyncStatus();
          if (pending.pendingRowCount >= previousPendingRowCount) {
            return SyncThenDetachResult(
              lastSync: lastReport,
              detach: DetachOutcome.blockedUnsyncedData,
              syncRounds: round,
              remainingPendingRowCount: pending.pendingRowCount,
            );
          }
          previousPendingRowCount = pending.pendingRowCount;
        }
        final pending = await _pendingSyncStatus();
        return SyncThenDetachResult(
          lastSync: lastReport!,
          detach: DetachOutcome.blockedUnsyncedData,
          syncRounds: 3,
          remainingPendingRowCount: pending.pendingRowCount,
        );
      },
    );
  }

  @override
  Future<RemoteSyncReport> rebuild() {
    return _runExclusive(
      operation: OversqliteOperation.rebuild,
      phase: OversqlitePhase.stagingRemoteState,
      block: () async {
        _ensureConnected('rebuild()');
        final result = await _executeRebuild();
        if (result.updatedTables.isNotEmpty) {
          _runtime.reportManagedTableChanges(result.updatedTables);
        }
        return RemoteSyncReport(
          outcome: result.outcome,
          status: await _syncStatusInternal(),
          restore: result.restore,
        );
      },
    );
  }

  @override
  Future<DetachOutcome> detach() {
    return _runExclusive(
      operation: OversqliteOperation.detach,
      phase: OversqlitePhase.cleaningLocalState,
      block: _executeDetach,
    );
  }

  @override
  Future<void> close() async {
    await _automaticDownloads?.stop();
    await _progressController.close();
  }

  Future<void> _runAutomaticDownloads(
    _DefaultAutomaticDownloadsHandle handle,
  ) async {
    final backoff = _AutomaticDownloadBackoff(
      minDelay: _config.bundleChangeWatchReconnectMin,
      maxDelay: _config.bundleChangeWatchReconnectMax,
    );
    while (!handle.isStopped) {
      if (_downloadsPaused) {
        await _automaticDownloadDelay(
          handle,
          _config.automaticDownloadInterval,
        );
        continue;
      }
      try {
        if (await _shouldUseBundleChangeWatch(handle)) {
          await _runBundleChangeWatchIteration(handle);
          await backoff.delayNext(handle);
        } else {
          await _runAutomaticPollingIteration(handle);
          backoff.reset();
        }
      } catch (_) {
        await _runAutomaticPullToStable('automatic downloads fallback');
        await backoff.delayNext(handle);
      }
    }
  }

  Future<bool> _shouldUseBundleChangeWatch(
    _DefaultAutomaticDownloadsHandle handle,
  ) async {
    if (_config.bundleChangeWatchMode != BundleChangeWatchMode.auto ||
        _watchTransport == null ||
        handle.isStopped) {
      return false;
    }
    try {
      final state = await _automaticDownloadState(
        'automatic downloads capability check',
      );
      final capabilities = await _runExclusive(
        operation: OversqliteOperation.fetchCapabilities,
        phase: OversqlitePhase.fetchingCapabilities,
        block: () => _fetchCapabilitiesInternal(state.sourceId),
      );
      return capabilities.bundleChangeWatchSupported;
    } catch (_) {
      return false;
    }
  }

  Future<void> _runAutomaticPollingIteration(
    _DefaultAutomaticDownloadsHandle handle,
  ) async {
    if (!_downloadsPaused) {
      await _runAutomaticPullToStable('automatic downloads polling');
    }
    await _automaticDownloadDelay(handle, _config.automaticDownloadInterval);
  }

  Future<void> _runBundleChangeWatchIteration(
    _DefaultAutomaticDownloadsHandle handle,
  ) async {
    final transport = _watchTransport;
    if (transport == null) {
      return;
    }
    final state = await _automaticDownloadState('bundle change watch');
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
    final iterator = StreamIterator(
      parseBundleChangeEventStream(response.lines),
    );
    handle.setActiveCancel(() async {
      await iterator.cancel();
      await response.close();
    });
    try {
      while (!handle.isStopped && await iterator.moveNext()) {
        final event = iterator.current;
        if (event.bundleSeq > 0 && !_downloadsPaused) {
          await _runAutomaticPullToStable('bundle change watch event');
        }
      }
    } finally {
      await iterator.cancel();
      await response.close();
      handle.clearActiveCancel();
    }
    if (!handle.isStopped && !_downloadsPaused) {
      await _runAutomaticPullToStable('bundle change watch reconnect');
    }
  }

  Future<_AutomaticDownloadState> _automaticDownloadState(
    String operation,
  ) async {
    _ensureConnected(operation);
    final attachment = await _loadAttachmentState();
    return _AutomaticDownloadState(
      sourceId: attachment.currentSourceId,
      lastBundleSeqSeen: attachment.lastBundleSeqSeen,
    );
  }

  Future<bool> _runAutomaticPullToStable(String operation) async {
    try {
      await pullToStable();
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

  Future<PushReport> _executePush(_AttachmentState attachment) async {
    final operation = await _loadOperationState();
    if (operation.kind == _operationKindSourceRecovery) {
      throw SourceRecoveryRequiredException(
        _sourceRecoveryReasonFromPersisted(operation.reason),
      );
    }
    if (attachment.rebuildRequired) {
      throw const RebuildRequiredException();
    }
    try {
      return await OversqlitePushRuntime(
        database: _database,
        config: _config,
        remoteApi: _remoteApiOrThrow(),
        resolver: _resolver,
      ).pushPending(
        validated: _requireValidatedConfig(),
        sourceId: attachment.currentSourceId,
        pendingInitializationId: attachment.pendingInitializationId,
      );
    } on SourceRecoveryRequiredHttpException catch (error) {
      await _persistSourceRecoveryRequiredState(
        currentSourceId: attachment.currentSourceId,
        reason: error.reason,
        replacementSourceId: error.replacementSourceId,
      );
      throw SourceRecoveryRequiredException(error.reason);
    } on CommittedReplayPrunedException {
      await _attachmentStore.markRebuildRequired();
      throw const RebuildRequiredException();
    } on SourceSequenceMismatchException {
      final rows = await _database.connection.select(
        'SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1',
        (row) => row.readString(0),
      );
      if (rows.single == 'committed_remote') {
        await _attachmentStore.markRebuildRequired();
        throw const RebuildRequiredException();
      }
      rethrow;
    }
  }

  Future<DownloadResult> _executePullToStable() async {
    final attachment = await _loadAttachmentState();
    final userId = _attachedUserIdOrThrow(attachment, 'pullToStable()');
    try {
      final result =
          await OversqliteDownloadRuntime(
            database: _database,
            config: _config,
            remoteApi: _remoteApiOrThrow(),
          ).pullToStable(
            validated: _requireValidatedConfig(),
            sourceId: attachment.currentSourceId,
            userId: userId,
          );
      _applyDownloadResult(result);
      return result;
    } on SourceRecoveryRequiredHttpException catch (error) {
      await _persistSourceRecoveryRequiredState(
        currentSourceId: attachment.currentSourceId,
        reason: error.reason,
        replacementSourceId: error.replacementSourceId,
      );
      throw SourceRecoveryRequiredException(error.reason);
    }
  }

  Future<DownloadResult> _executeRebuild() async {
    final attachment = await _loadAttachmentState();
    final operation = await _loadOperationState();
    final userId = _attachedUserIdOrThrow(attachment, 'rebuild()');
    if (operation.kind == _operationKindSourceRecovery) {
      if (operation.replacementSourceId.trim().isEmpty) {
        throw StateError(
          'source recovery operation state is missing replacement_source_id',
        );
      }
      if (operation.reason.trim().isEmpty) {
        throw StateError('source recovery operation state is missing reason');
      }
      return _executeSnapshotRebuild(
        userId: userId,
        sourceId: attachment.currentSourceId,
        rotatedSourceId: operation.replacementSourceId,
        sourceReplacementReason: operation.reason,
        outboxMode: SnapshotRebuildOutboxMode.preserveSourceRecovery,
      );
    }
    final outboxState = await _loadOutboxState();
    return _executeSnapshotRebuild(
      userId: userId,
      sourceId: attachment.currentSourceId,
      rotatedSourceId: null,
      sourceReplacementReason: null,
      outboxMode: outboxState == 'committed_remote'
          ? SnapshotRebuildOutboxMode.preserveCommittedRemote
          : SnapshotRebuildOutboxMode.clearAll,
    );
  }

  Future<DownloadResult> _executeSnapshotRebuild({
    required String userId,
    required String sourceId,
    required String? rotatedSourceId,
    required String? sourceReplacementReason,
    required SnapshotRebuildOutboxMode outboxMode,
    bool persistRemoteReplaceOperation = false,
  }) async {
    try {
      final result =
          await OversqliteDownloadRuntime(
            database: _database,
            config: _config,
            remoteApi: _remoteApiOrThrow(),
          ).rebuildFromSnapshot(
            validated: _requireValidatedConfig(),
            sourceId: sourceId,
            userId: userId,
            rotatedSourceId: rotatedSourceId,
            sourceReplacementReason: sourceReplacementReason,
            outboxMode: outboxMode,
            persistRemoteReplaceOperation: persistRemoteReplaceOperation,
          );
      _applyDownloadResult(result);
      _sessionConnected = true;
      _currentUserId = userId;
      return result;
    } on SourceRecoveryRequiredHttpException catch (error) {
      await _persistSourceRecoveryRequiredState(
        currentSourceId: sourceId,
        reason: error.reason,
        replacementSourceId: error.replacementSourceId,
      );
      throw SourceRecoveryRequiredException(error.reason);
    }
  }

  Future<DetachOutcome> _executeDetach() async {
    _ensureOpened('detach()');
    final attachment = await _loadAttachmentState();
    final operation = await _loadOperationState();
    if (operation.kind == _operationKindRemoteReplace) {
      if (operation.stagedSnapshotId.isNotEmpty) {
        await _database.connection.execute(
          'DELETE FROM _sync_snapshot_stage WHERE snapshot_id = ?',
          parameters: [operation.stagedSnapshotId],
        );
      } else {
        await _database.connection.execute('DELETE FROM _sync_snapshot_stage');
      }
      await _persistAnonymousState(attachment.currentSourceId);
      await _persistOperationState(_OperationState());
      _sessionConnected = false;
      _currentUserId = null;
      return DetachOutcome.detached;
    }

    final pending = await _pendingSyncStatus();
    if (pending.blocksDetach) {
      return DetachOutcome.blockedUnsyncedData;
    }

    final validated = _requireValidatedConfig();
    final managedTables = await _managedTableNames(validated);
    final previousSourceId = attachment.currentSourceId;
    final rotatedSourceId = _generateFreshSourceId();
    await _applyRunner.inApplyModeTransaction(() async {
      for (final table in managedTables) {
        if (await _sqliteTableExists(table)) {
          await _database.connection.execute(
            'DELETE FROM ${_quoteIdent(table)}',
          );
        }
        await _database.connection.execute(
          'DELETE FROM _sync_row_state WHERE schema_name = ? AND table_name = ?',
          parameters: [validated.schema, table],
        );
      }
      await _database.connection.execute('DELETE FROM _sync_dirty_rows');
      await _database.connection.execute('DELETE FROM _sync_outbox_rows');
      await _database.connection.execute('DELETE FROM _sync_snapshot_stage');
      await _database.connection.execute('''UPDATE _sync_outbox_bundle
SET state = 'none',
    source_id = '',
    source_bundle_id = 0,
    initialization_id = '',
    canonical_request_hash = '',
    row_count = 0,
    remote_bundle_hash = '',
    remote_bundle_seq = 0
WHERE singleton_key = 1''');
      await _sourceStore.ensureSource(rotatedSourceId);
      if (previousSourceId.isNotEmpty) {
        await _sourceStore.markSourceReplaced(
          sourceId: previousSourceId,
          replacedBySourceId: rotatedSourceId,
        );
      }
      await _persistAnonymousState(rotatedSourceId);
      await _persistOperationState(_OperationState());
    });
    _sessionConnected = false;
    _currentUserId = null;
    _runtime.reportManagedTableChanges(managedTables);
    return DetachOutcome.detached;
  }

  Future<T> _runExclusive<T>({
    required OversqliteOperation operation,
    required OversqlitePhase phase,
    required Future<T> Function() block,
  }) async {
    if (_operationRunning) {
      throw const SyncOperationInProgressException();
    }
    _operationRunning = true;
    _setProgress(OversqliteActive(operation: operation, phase: phase));
    try {
      return await block();
    } finally {
      _setProgress(const OversqliteIdle());
      _operationRunning = false;
    }
  }

  void _setProgress(OversqliteProgress progress) {
    _progress = progress;
    if (!_progressController.isClosed) {
      _progressController.add(progress);
    }
  }

  void _ensureOpened(String operation) {
    if (!_opened) {
      throw OpenRequiredException(operation);
    }
  }

  void _ensureConnected(String operation) {
    _ensureOpened(operation);
    if (!_sessionConnected || _currentUserId == null) {
      throw ConnectRequiredException(operation);
    }
  }

  OversqliteValidatedConfig _requireValidatedConfig() {
    final validated = _validated;
    if (validated == null) {
      throw const OpenRequiredException('local runtime');
    }
    return validated;
  }

  OversqliteRemoteApi _remoteApiOrThrow() {
    final remoteApi = _remoteApi;
    if (remoteApi == null) {
      throw const RemoteAttachDeferredException();
    }
    return remoteApi;
  }

  Future<CapabilitiesResponse> _fetchCapabilitiesInternal(
    String sourceId,
  ) async {
    final capabilities = await _remoteApiOrThrow().fetchCapabilities(sourceId);
    if (!capabilities.connectLifecycleSupported) {
      throw const ConnectLifecycleUnsupportedException(
        'connect_lifecycle capability is absent',
      );
    }
    return capabilities;
  }

  Future<String> _currentSourceId() async {
    final attachment = await _loadAttachmentState();
    if (attachment.currentSourceId.isEmpty) {
      throw const OpenRequiredException('source identity');
    }
    return attachment.currentSourceId;
  }

  String _attachedUserIdOrThrow(_AttachmentState attachment, String operation) {
    if (attachment.bindingState != _attachmentBindingAttached ||
        attachment.attachedUserId.isEmpty) {
      throw ConnectRequiredException(operation);
    }
    return attachment.attachedUserId;
  }

  void _applyDownloadResult(DownloadResult result) {
    final rotated = result.rotatedSourceId;
    if (rotated != null && rotated.isNotEmpty) {
      // The durable attachment row is updated by the download runtime; keep the
      // in-memory source view aligned for subsequent operations in this client.
    }
  }

  Future<SyncStatus> _syncStatusInternal() async {
    final attachment = await _loadAttachmentState();
    final authority = attachment.pendingInitializationId.isNotEmpty
        ? AuthorityStatus.pendingLocalSeed
        : await _liveStructuredRowCount() == 0
        ? AuthorityStatus.authoritativeEmpty
        : AuthorityStatus.authoritativeMaterialized;
    return SyncStatus(
      authority: authority,
      pending: await _pendingSyncStatus(),
      lastBundleSeqSeen: attachment.lastBundleSeqSeen,
    );
  }

  Future<PendingSyncStatus> _pendingSyncStatus() async {
    final dirtyCount = await _scalarInt(
      'SELECT COUNT(*) FROM _sync_dirty_rows',
    );
    final outboxCount = await _scalarInt(
      'SELECT COUNT(*) FROM _sync_outbox_rows',
    );
    final total = dirtyCount + outboxCount;
    final attachment = await _loadAttachmentState();
    return PendingSyncStatus(
      hasPendingSyncData: total > 0,
      pendingRowCount: total,
      blocksDetach:
          attachment.bindingState == _attachmentBindingAttached && total > 0,
    );
  }

  Future<int> _liveStructuredRowCount() {
    return _scalarInt('SELECT COUNT(*) FROM _sync_row_state WHERE deleted = 0');
  }

  Future<_AttachmentState> _loadAttachmentState() async {
    final rows = await _database.connection.select(
      '''SELECT current_source_id, binding_state, attached_user_id, schema_name,
       last_bundle_seq_seen, rebuild_required, pending_initialization_id
FROM _sync_attachment_state
WHERE singleton_key = 1''',
      (row) => _AttachmentState(
        currentSourceId: row.readString(0),
        bindingState: row.readString(1),
        attachedUserId: row.readString(2),
        schemaName: row.readString(3),
        lastBundleSeqSeen: row.readInt(4),
        rebuildRequired: row.readInt(5) == 1,
        pendingInitializationId: row.readString(6),
      ),
    );
    if (rows.isEmpty) {
      throw StateError('_sync_attachment_state singleton row is missing');
    }
    return rows.single;
  }

  Future<_OperationState> _loadOperationState() async {
    final rows = await _database.connection.select(
      '''SELECT kind, target_user_id, staged_snapshot_id, snapshot_bundle_seq, snapshot_row_count,
       reason, replacement_source_id
FROM _sync_operation_state
WHERE singleton_key = 1''',
      (row) => _OperationState(
        kind: row.readString(0),
        targetUserId: row.readString(1),
        stagedSnapshotId: row.readString(2),
        snapshotBundleSeq: row.readInt(3),
        snapshotRowCount: row.readInt(4),
        reason: row.readString(5),
        replacementSourceId: row.readString(6),
      ),
    );
    if (rows.isEmpty) {
      throw StateError('_sync_operation_state singleton row is missing');
    }
    return rows.single;
  }

  Future<void> _persistAnonymousState(String sourceId) {
    return _database.connection.execute(
      '''UPDATE _sync_attachment_state
SET current_source_id = ?,
    binding_state = 'anonymous',
    attached_user_id = '',
    schema_name = '',
    last_bundle_seq_seen = 0,
    rebuild_required = 0,
    pending_initialization_id = ''
WHERE singleton_key = 1''',
      parameters: [sourceId],
    );
  }

  Future<void> _persistConnectedLifecycleState({
    required String userId,
    required String sourceId,
    required String initializationId,
  }) {
    final validated = _requireValidatedConfig();
    return _database.connection.execute(
      '''UPDATE _sync_attachment_state
SET current_source_id = ?,
    binding_state = 'attached',
    attached_user_id = ?,
    schema_name = ?,
    rebuild_required = 0,
    pending_initialization_id = ?
WHERE singleton_key = 1''',
      parameters: [sourceId, userId, validated.schema, initializationId],
    );
  }

  Future<void> _beginRemoteReplace({
    required String userId,
    required String sourceId,
  }) async {
    await _persistAnonymousState(sourceId);
    await _persistOperationState(
      _OperationState(kind: _operationKindRemoteReplace, targetUserId: userId),
    );
  }

  Future<void> _persistOperationState(_OperationState state) {
    return _database.connection.execute(
      '''UPDATE _sync_operation_state
SET kind = ?,
    target_user_id = ?,
    staged_snapshot_id = ?,
    snapshot_bundle_seq = ?,
    snapshot_row_count = ?,
    reason = ?,
    replacement_source_id = ?
WHERE singleton_key = 1''',
      parameters: [
        state.kind,
        state.targetUserId,
        state.stagedSnapshotId,
        state.snapshotBundleSeq,
        state.snapshotRowCount,
        state.reason,
        state.replacementSourceId,
      ],
    );
  }

  Future<String> _loadOutboxState() async {
    final rows = await _database.connection.select(
      'SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1',
      (row) => row.readString(0),
    );
    return rows.single;
  }

  Future<void> _persistSourceRecoveryRequiredState({
    required String currentSourceId,
    required SourceRecoveryReason reason,
    String? replacementSourceId,
  }) async {
    await _database.connection.transaction(() async {
      await _attachmentStore.markRebuildRequired();
      final replacement = await _reserveReplacementSourceId(
        currentSourceId: currentSourceId,
        preferredReplacementSourceId: replacementSourceId,
      );
      await _persistOperationState(
        _OperationState(
          kind: _operationKindSourceRecovery,
          reason: reason.wireName,
          replacementSourceId: replacement,
        ),
      );
    }, mode: TransactionMode.immediate);
  }

  Future<String> _reserveReplacementSourceId({
    required String currentSourceId,
    String? preferredReplacementSourceId,
  }) async {
    final operation = await _loadOperationState();
    final existing = operation.replacementSourceId.trim();
    final preferred = preferredReplacementSourceId?.trim() ?? '';
    final reserved = switch ((existing, preferred)) {
      (final local, final remote)
          when local.isNotEmpty && remote.isNotEmpty && local != remote =>
        throw SourceReplacementDivergedException(
          localReplacementSourceId: local,
          remoteReplacementSourceId: remote,
        ),
      (final local, _) when local.isNotEmpty => local,
      (_, final remote) when remote.isNotEmpty => remote,
      _ => _generateFreshReplacementSourceId(currentSourceId),
    };
    await _ensureFreshReplacementSourceState(
      currentSourceId: currentSourceId,
      replacementSourceId: reserved,
    );
    return reserved;
  }

  Future<void> _ensureFreshReplacementSourceState({
    required String currentSourceId,
    required String replacementSourceId,
  }) async {
    if (replacementSourceId.trim().isEmpty) {
      throw StateError('replacement source id must not be blank');
    }
    if (replacementSourceId == currentSourceId) {
      throw StateError(
        'replacement source id must differ from current source id',
      );
    }
    await _sourceStore.ensureSource(replacementSourceId);
    final rows = await _database.connection.select(
      '''SELECT next_source_bundle_id, replaced_by_source_id
FROM _sync_source_state
WHERE source_id = ?''',
      (row) => (
        nextSourceBundleId: row.readInt(0),
        replacedBySourceId: row.readString(1),
      ),
      parameters: [replacementSourceId],
    );
    if (rows.isEmpty) {
      throw StateError(
        '_sync_source_state missing for replacement source $replacementSourceId',
      );
    }
    final state = rows.single;
    if (state.nextSourceBundleId != 1 ||
        state.replacedBySourceId.trim().isNotEmpty) {
      throw StateError(
        'replacement source $replacementSourceId is not fresh for source recovery',
      );
    }
  }

  String _generateFreshReplacementSourceId(String currentSourceId) {
    var candidate = _generateFreshSourceId();
    while (candidate == currentSourceId) {
      candidate = _generateFreshSourceId();
    }
    return candidate;
  }

  Future<Set<String>> _managedTableNames(
    OversqliteValidatedConfig validated,
  ) async {
    final rows = await _database.connection.select(
      'SELECT table_name FROM _sync_managed_tables WHERE schema_name = ? ORDER BY table_name',
      (row) => row.readString(0).toLowerCase(),
      parameters: [validated.schema],
    );
    return {
      ...rows,
      for (final table in validated.tables) table.tableName.toLowerCase(),
    };
  }

  Future<bool> _sqliteTableExists(String tableName) async {
    final rows = await _database.connection.select(
      "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
      (row) => row.readInt(0),
      parameters: [tableName],
    );
    return rows.single > 0;
  }

  Future<int> _scalarInt(String sql) async {
    final rows = await _database.connection.select(
      sql,
      (row) => row.readInt(0),
    );
    return rows.single;
  }
}

final class _AttachmentState {
  const _AttachmentState({
    required this.currentSourceId,
    required this.bindingState,
    required this.attachedUserId,
    required this.schemaName,
    required this.lastBundleSeqSeen,
    required this.rebuildRequired,
    required this.pendingInitializationId,
  });

  final String currentSourceId;
  final String bindingState;
  final String attachedUserId;
  final String schemaName;
  final int lastBundleSeqSeen;
  final bool rebuildRequired;
  final String pendingInitializationId;
}

final class _OperationState {
  _OperationState({
    this.kind = _operationKindNone,
    this.targetUserId = '',
    this.stagedSnapshotId = '',
    this.snapshotBundleSeq = 0,
    this.snapshotRowCount = 0,
    this.reason = '',
    this.replacementSourceId = '',
  });

  final String kind;
  final String targetUserId;
  final String stagedSnapshotId;
  final int snapshotBundleSeq;
  final int snapshotRowCount;
  final String reason;
  final String replacementSourceId;
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

String _quoteIdent(String identifier) =>
    '"${identifier.replaceAll('"', '""')}"';

String _generateFreshSourceId() {
  final random = Random.secure();
  final bytes = List<int>.generate(16, (_) => random.nextInt(256));
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  final hex = bytes
      .map((byte) => byte.toRadixString(16).padLeft(2, '0'))
      .join();
  return '${hex.substring(0, 8)}-${hex.substring(8, 12)}-${hex.substring(12, 16)}-${hex.substring(16, 20)}-${hex.substring(20)}';
}

SourceRecoveryReason _sourceRecoveryReasonFromPersisted(String value) {
  return switch (value) {
    'history_pruned' => SourceRecoveryReason.historyPruned,
    'source_sequence_out_of_order' =>
      SourceRecoveryReason.sourceSequenceOutOfOrder,
    'source_sequence_changed' => SourceRecoveryReason.sourceSequenceChanged,
    'source_retired' => SourceRecoveryReason.sourceRetired,
    _ => throw StateError(
      'source recovery operation state is missing a valid recovery reason',
    ),
  };
}

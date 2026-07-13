part of 'client.dart';

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
  OversqliteClientStateStore get _clientStateStore =>
      OversqliteClientStateStore(_connection);
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
        final attachment = await _clientStateStore.loadAttachmentState();
        _opened = true;
        _sessionConnected = false;
        _currentUserId =
            attachment.bindingState == oversqliteAttachmentBindingAttached &&
                attachment.attachedUserId.isNotEmpty
            ? attachment.attachedUserId
            : null;
      },
    );
  }

  @override
  Future<SourceInfo> sourceInfo() async {
    _ensureOpened('sourceInfo()');
    final attachment = await _clientStateStore.loadAttachmentState();
    final operation = await _clientStateStore.loadOperationState();
    final sourceId = attachment.currentSourceId;
    if (sourceId.isEmpty) {
      throw const OpenRequiredException('sourceInfo()');
    }
    return SourceInfo(
      currentSourceId: sourceId,
      rebuildRequired:
          attachment.rebuildRequired ||
          operation.kind == oversqliteOperationKindSourceRecovery,
      sourceRecoveryRequired:
          operation.kind == oversqliteOperationKindSourceRecovery,
      sourceRecoveryReason:
          operation.kind == oversqliteOperationKindSourceRecovery &&
              operation.reason.isNotEmpty
          ? operation.reason
          : null,
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
      _runAutomaticDownloads(this, handle)
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
        final attachment = await _clientStateStore.loadAttachmentState();
        final operation = await _clientStateStore.loadOperationState();
        if (operation.kind == oversqliteOperationKindRemoteReplace) {
          if (operation.targetUserId != requestedUserId) {
            throw ConnectLocalStateConflictException(
              'pending remote_replace belongs to scope "${operation.targetUserId}"',
            );
          }
        }
        if (attachment.bindingState == oversqliteAttachmentBindingAttached &&
            attachment.attachedUserId == requestedUserId) {
          _sessionConnected = true;
          _currentUserId = requestedUserId;
          return AttachConnected(
            outcome: AttachOutcome.resumedAttachedState,
            status: await _syncStatusInternal(),
          );
        }
        if (attachment.bindingState == oversqliteAttachmentBindingAttached &&
            attachment.attachedUserId != requestedUserId) {
          throw ConnectBindingConflictException(
            attachedUserId: attachment.attachedUserId,
            requestedUserId: requestedUserId,
          );
        }

        final sourceId = attachment.currentSourceId;
        await _fetchCapabilitiesInternal(sourceId);
        if (operation.kind == oversqliteOperationKindRemoteReplace) {
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
          _currentUserId = requestedUserId;
          return AttachConnected(
            outcome: AttachOutcome.usedRemoteState,
            status: await _syncStatusInternal(),
          );
        }

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
            await _clientStateStore.persistConnectedLifecycleState(
              userId: requestedUserId,
              sourceId: sourceId,
              schema: _requireValidatedConfig().schema,
              initializationId: '',
            );
            _sessionConnected = true;
            _currentUserId = requestedUserId;
            return AttachConnected(
              outcome: AttachOutcome.startedEmpty,
              status: await _syncStatusInternal(),
            );
          case 'initialize_local':
            await _clientStateStore.persistConnectedLifecycleState(
              userId: requestedUserId,
              sourceId: sourceId,
              schema: _requireValidatedConfig().schema,
              initializationId: connect.initializationId,
            );
            _sessionConnected = true;
            _currentUserId = requestedUserId;
            return AttachConnected(
              outcome: AttachOutcome.seededFromLocal,
              status: await _syncStatusInternal(),
            );
          case 'remote_authoritative':
            await _clientStateStore.beginRemoteReplace(
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
        await _fetchCapabilitiesInternal(await _currentSourceId());
        final attachment = await _clientStateStore.loadAttachmentState();
        final operation = await _clientStateStore.loadOperationState();
        if (operation.kind == oversqliteOperationKindSourceRecovery) {
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
        await _fetchCapabilitiesInternal(await _currentSourceId());
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
        await _fetchCapabilitiesInternal(await _currentSourceId());
        final attachment = await _clientStateStore.loadAttachmentState();
        final operation = await _clientStateStore.loadOperationState();
        if (operation.kind == oversqliteOperationKindSourceRecovery) {
          throw SourceRecoveryRequiredException(
            _sourceRecoveryReasonFromPersisted(operation.reason),
          );
        }
        if (attachment.rebuildRequired) {
          final remoteResult = await _executeCheckpointRecovery();
          if (remoteResult.updatedTables.isNotEmpty) {
            _runtime.reportManagedTableChanges(remoteResult.updatedTables);
          }
          return SyncReport(
            pushOutcome: PushOutcome.noChange,
            remoteOutcome: remoteResult.outcome,
            status: await _syncStatusInternal(),
            restore: remoteResult.restore,
          );
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
        await _fetchCapabilitiesInternal(await _currentSourceId());
        var previousPendingRowCount = 0x7fffffffffffffff;
        SyncReport? lastReport;
        for (var round = 1; round <= 3; round++) {
          final attachment = await _clientStateStore.loadAttachmentState();
          final operation = await _clientStateStore.loadOperationState();
          if (operation.kind == oversqliteOperationKindSourceRecovery) {
            throw SourceRecoveryRequiredException(
              _sourceRecoveryReasonFromPersisted(operation.reason),
            );
          }
          final PushReport pushReport;
          final DownloadResult remoteResult;
          if (attachment.rebuildRequired) {
            remoteResult = await _executeCheckpointRecovery();
            pushReport = PushReport(
              outcome: PushOutcome.noChange,
              updatedTables: const {},
            );
          } else {
            pushReport = await _executePush(attachment);
            _setProgress(
              const OversqliteActive(
                operation: OversqliteOperation.syncThenDetach,
                phase: OversqlitePhase.pulling,
              ),
            );
            remoteResult = await _executePullToStable();
          }
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
        await _fetchCapabilitiesInternal(await _currentSourceId());
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

  Future<PushReport> _executePush(
    OversqliteClientAttachmentState attachment, {
    bool allowCheckpointRecovery = false,
  }) async {
    final operation = await _clientStateStore.loadOperationState();
    if (operation.kind == oversqliteOperationKindSourceRecovery) {
      throw SourceRecoveryRequiredException(
        _sourceRecoveryReasonFromPersisted(operation.reason),
      );
    }
    if (attachment.rebuildRequired && !allowCheckpointRecovery) {
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
      await _clientStateStore.persistSourceRecoveryRequiredState(
        currentSourceId: attachment.currentSourceId,
        reason: error.reason,
        replacementSourceId: error.replacementSourceId,
      );
      throw SourceRecoveryRequiredException(error.reason);
    } on CommittedReplayPrunedException {
      await _attachmentStore.markRebuildRequired();
      if (allowCheckpointRecovery) rethrow;
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
    final attachment = await _clientStateStore.loadAttachmentState();
    final operation = await _clientStateStore.loadOperationState();
    if (attachment.rebuildRequired &&
        operation.kind != oversqliteOperationKindSourceRecovery) {
      return _executeCheckpointRecovery();
    }
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
      await _clientStateStore.persistSourceRecoveryRequiredState(
        currentSourceId: attachment.currentSourceId,
        reason: error.reason,
        replacementSourceId: error.replacementSourceId,
      );
      throw SourceRecoveryRequiredException(error.reason);
    }
  }

  Future<DownloadResult> _executeCheckpointRecovery() async {
    var dirtyCount = await _clientStateStore.scalarInt(
      'SELECT COUNT(*) FROM _sync_dirty_rows',
    );
    var outboundCount = await _clientStateStore.scalarInt(
      'SELECT COUNT(*) FROM _sync_outbox_rows',
    );
    var replayState = await _clientStateStore.loadOutboxState();
    if (dirtyCount > 0 || outboundCount > 0) {
      if (_uploadsPaused) {
        throw CheckpointRecoveryBlockedException(
          reason: CheckpointRecoveryBlockedReason.uploadPaused,
          dirtyCount: dirtyCount,
          outboundCount: outboundCount,
          replayState: replayState,
        );
      }
      try {
        final attachment = await _clientStateStore.loadAttachmentState();
        await _executePush(attachment, allowCheckpointRecovery: true);
      } on SourceRecoveryRequiredException {
        rethrow;
      } on CommittedReplayPrunedException {
        return _executeRebuild();
      } catch (error) {
        if (error is OversqliteProtocolException) rethrow;
        if (error is OversqliteHttpException &&
            (error.statusCode == 401 ||
                error.statusCode == 403 ||
                error.statusCode == 408 ||
                error.statusCode == 429 ||
                error.statusCode >= 500)) {
          rethrow;
        }
        dirtyCount = await _clientStateStore.scalarInt(
          'SELECT COUNT(*) FROM _sync_dirty_rows',
        );
        outboundCount = await _clientStateStore.scalarInt(
          'SELECT COUNT(*) FROM _sync_outbox_rows',
        );
        replayState = await _clientStateStore.loadOutboxState();
        throw CheckpointRecoveryBlockedException(
          reason: CheckpointRecoveryBlockedReason.pushFailed,
          dirtyCount: dirtyCount,
          outboundCount: outboundCount,
          replayState: replayState,
          cause: error,
        );
      }
      dirtyCount = await _clientStateStore.scalarInt(
        'SELECT COUNT(*) FROM _sync_dirty_rows',
      );
      outboundCount = await _clientStateStore.scalarInt(
        'SELECT COUNT(*) FROM _sync_outbox_rows',
      );
      replayState = await _clientStateStore.loadOutboxState();
      if (dirtyCount > 0 || outboundCount > 0) {
        throw CheckpointRecoveryBlockedException(
          reason: CheckpointRecoveryBlockedReason.pendingWork,
          dirtyCount: dirtyCount,
          outboundCount: outboundCount,
          replayState: replayState,
        );
      }
    }
    return _executeRebuild();
  }

  Future<DownloadResult> _executeRebuild() async {
    final attachment = await _clientStateStore.loadAttachmentState();
    final operation = await _clientStateStore.loadOperationState();
    final userId = _attachedUserIdOrThrow(attachment, 'rebuild()');
    if (operation.kind == oversqliteOperationKindSourceRecovery) {
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
    final outboxState = await _clientStateStore.loadOutboxState();
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
      await _clientStateStore.persistSourceRecoveryRequiredState(
        currentSourceId: sourceId,
        reason: error.reason,
        replacementSourceId: error.replacementSourceId,
      );
      throw SourceRecoveryRequiredException(error.reason);
    }
  }

  Future<DetachOutcome> _executeDetach() async {
    _ensureOpened('detach()');
    final attachment = await _clientStateStore.loadAttachmentState();
    final operation = await _clientStateStore.loadOperationState();
    if (operation.kind == oversqliteOperationKindRemoteReplace) {
      if (operation.stagedSnapshotId.isNotEmpty) {
        await _database.connection.execute(
          'DELETE FROM _sync_snapshot_stage WHERE snapshot_id = ?',
          parameters: [operation.stagedSnapshotId],
        );
      } else {
        await _database.connection.execute('DELETE FROM _sync_snapshot_stage');
      }
      await _clientStateStore.persistAnonymousState(attachment.currentSourceId);
      await _clientStateStore.persistOperationState(
        OversqliteClientOperationState(),
      );
      _sessionConnected = false;
      _currentUserId = null;
      return DetachOutcome.detached;
    }

    final pending = await _pendingSyncStatus();
    if (pending.blocksDetach) {
      return DetachOutcome.blockedUnsyncedData;
    }

    final validated = _requireValidatedConfig();
    final managedTables = await _clientStateStore.managedTableNames(validated);
    final previousSourceId = attachment.currentSourceId;
    final rotatedSourceId = _clientStateStore.generateFreshSourceId();
    await _applyRunner.inApplyModeTransaction(() async {
      for (final table in managedTables) {
        if (await _clientStateStore.sqliteTableExists(table)) {
          await _database.connection.execute(
            'DELETE FROM ${OversqliteClientStateStore.quoteIdentifier(table)}',
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
      await _clientStateStore.persistAnonymousState(rotatedSourceId);
      await _clientStateStore.persistOperationState(
        OversqliteClientOperationState(),
      );
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
    final attachment = await _clientStateStore.loadAttachmentState();
    if (attachment.currentSourceId.isEmpty) {
      throw const OpenRequiredException('source identity');
    }
    return attachment.currentSourceId;
  }

  String _attachedUserIdOrThrow(
    OversqliteClientAttachmentState attachment,
    String operation,
  ) {
    if (attachment.bindingState != oversqliteAttachmentBindingAttached ||
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
    final attachment = await _clientStateStore.loadAttachmentState();
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
    final dirtyCount = await _clientStateStore.scalarInt(
      'SELECT COUNT(*) FROM _sync_dirty_rows',
    );
    final outboxCount = await _clientStateStore.scalarInt(
      'SELECT COUNT(*) FROM _sync_outbox_rows',
    );
    final total = dirtyCount + outboxCount;
    final attachment = await _clientStateStore.loadAttachmentState();
    return PendingSyncStatus(
      hasPendingSyncData: total > 0,
      pendingRowCount: total,
      blocksDetach:
          attachment.bindingState == oversqliteAttachmentBindingAttached &&
          total > 0,
    );
  }

  Future<int> _liveStructuredRowCount() {
    return _clientStateStore.scalarInt(
      'SELECT COUNT(*) FROM _sync_row_state WHERE deleted = 0',
    );
  }
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

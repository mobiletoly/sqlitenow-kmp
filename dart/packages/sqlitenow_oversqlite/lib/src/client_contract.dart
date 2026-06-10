part of 'client.dart';

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

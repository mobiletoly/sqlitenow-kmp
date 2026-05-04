import 'protocol.dart';

abstract interface class Resolver {
  MergeResult resolve(ConflictContext conflict);
}

sealed class MergeResult {
  const MergeResult();
}

final class AcceptServer extends MergeResult {
  const AcceptServer();
}

final class KeepLocal extends MergeResult {
  const KeepLocal();
}

final class KeepMerged extends MergeResult {
  const KeepMerged(this.mergedPayload);

  final Map<String, Object?> mergedPayload;
}

final class ConflictContext {
  const ConflictContext({
    required this.schema,
    required this.table,
    required this.key,
    required this.localOp,
    required this.localPayload,
    required this.baseRowVersion,
    required this.serverRowVersion,
    required this.serverRowDeleted,
    required this.serverRow,
  });

  final String schema;
  final String table;
  final SyncKey key;
  final String localOp;
  final Map<String, Object?>? localPayload;
  final int baseRowVersion;
  final int serverRowVersion;
  final bool serverRowDeleted;
  final Map<String, Object?>? serverRow;
}

final class ServerWinsResolver implements Resolver {
  const ServerWinsResolver();

  @override
  MergeResult resolve(ConflictContext conflict) => const AcceptServer();
}

final class ClientWinsResolver implements Resolver {
  const ClientWinsResolver();

  @override
  MergeResult resolve(ConflictContext conflict) => const KeepLocal();
}

final class InvalidConflictResolutionException implements Exception {
  const InvalidConflictResolutionException(this.message);

  final String message;

  @override
  String toString() => message;
}

final class PushConflictRetryExhaustedException implements Exception {
  const PushConflictRetryExhaustedException({
    required this.retryCount,
    required this.remainingDirtyCount,
  });

  final int retryCount;
  final int remainingDirtyCount;

  @override
  String toString() =>
      'push conflict auto-retry exhausted after $retryCount retries; $remainingDirtyCount dirty rows remain replayable';
}

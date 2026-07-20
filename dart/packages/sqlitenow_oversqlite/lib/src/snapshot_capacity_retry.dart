import 'dart:async';
import 'dart:math';

import 'config.dart';
import 'protocol_transport.dart';
import 'snapshot_cancellation.dart';
import 'snapshot_diagnostics.dart';

Duration? parseSnapshotRetryAfter(String? value) {
  final seconds = int.tryParse(value?.trim() ?? '');
  if (seconds == null || seconds <= 0) return null;
  final milliseconds = seconds * Duration.millisecondsPerSecond;
  if (milliseconds > 0x7fffffffffffffff) return null;
  return Duration(milliseconds: milliseconds);
}

Future<T> withSnapshotCapacityRetry<T>({
  required OversqliteSnapshotCapacityRetryPolicy policy,
  required String operation,
  required Future<T> Function() attempt,
  SnapshotDiagnosticsRecorder? diagnostics,
  Random? random,
  Future<void> Function(Duration duration)? delay,
  bool Function()? isCancelled,
  Future<void>? cancellationSignal,
  Duration Function()? elapsedSinceStart,
}) async {
  final stopwatch = elapsedSinceStart == null ? (Stopwatch()..start()) : null;
  Duration elapsed() => elapsedSinceStart?.call() ?? stopwatch!.elapsed;
  final jitterRandom = random ?? Random.secure();
  final wait = delay ?? Future<void>.delayed;
  final cancellation = SnapshotCancellationScope.current;
  final cancelled = isCancelled ?? cancellation.isCancelled;
  final cancelledSignal = cancellationSignal ?? cancellation.cancellationSignal;
  while (true) {
    if (cancelled()) {
      throw const SnapshotCapacityRetryCancelledException();
    }
    try {
      return await attempt();
    } on SnapshotCapacityException catch (error) {
      diagnostics?.recordCapacityResponse();
      final elapsedBeforeRetry = elapsed();
      if (!policy.enabled) {
        throw SnapshotCapacityRetryExhaustedException(
          operation: operation,
          errorCode: error.errorCode,
          waited: elapsedBeforeRetry,
        );
      }
      final base = error.retryAfter ?? policy.fallbackDelay;
      final remaining = policy.maxWait - elapsedBeforeRetry;
      if (base <= Duration.zero || base > remaining) {
        throw SnapshotCapacityRetryExhaustedException(
          operation: operation,
          errorCode: error.errorCode,
          waited: elapsedBeforeRetry,
        );
      }
      final jitterMicros = (base.inMicroseconds * policy.positiveJitterRatio)
          .floor();
      final sampledJitterMicros = jitterMicros == 0
          ? 0
          : min(
              jitterMicros,
              (jitterRandom.nextDouble() * (jitterMicros + 1)).floor(),
            );
      final jitter = Duration(microseconds: sampledJitterMicros);
      final retryDelay = base + jitter;
      if (retryDelay > remaining) {
        throw SnapshotCapacityRetryExhaustedException(
          operation: operation,
          errorCode: error.errorCode,
          waited: elapsedBeforeRetry,
        );
      }
      if (cancelled()) {
        throw const SnapshotCapacityRetryCancelledException();
      }
      diagnostics?.recordCapacityRetry();
      final waitStartedAt = elapsed();
      final waitFuture = wait(retryDelay);
      if (cancelledSignal == null) {
        await waitFuture;
      } else {
        await Future.any<void>([waitFuture, cancelledSignal]);
      }
      final measuredWait = elapsed() - waitStartedAt;
      diagnostics?.recordCapacityWait(
        measuredWait < Duration.zero ? Duration.zero : measuredWait,
      );
      if (cancelled()) {
        throw const SnapshotCapacityRetryCancelledException();
      }
      final elapsedAfterWait = elapsed();
      if (elapsedAfterWait > policy.maxWait) {
        throw SnapshotCapacityRetryExhaustedException(
          operation: operation,
          errorCode: error.errorCode,
          waited: elapsedAfterWait,
        );
      }
    }
  }
}

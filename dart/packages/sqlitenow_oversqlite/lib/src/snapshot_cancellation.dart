import 'dart:async';

final class SnapshotCancellationScope {
  const SnapshotCancellationScope._({
    required this.isCancelled,
    required this.cancellationSignal,
  });

  static final _zoneKey = Object();
  static final _none = SnapshotCancellationScope._(
    isCancelled: () => false,
    cancellationSignal: null,
  );

  final bool Function() isCancelled;
  final Future<void>? cancellationSignal;

  static SnapshotCancellationScope get current =>
      Zone.current[_zoneKey] as SnapshotCancellationScope? ?? _none;

  static Future<T> run<T>({
    required bool Function() isCancelled,
    required Future<void> cancellationSignal,
    required Future<T> Function() action,
  }) {
    final scope = SnapshotCancellationScope._(
      isCancelled: isCancelled,
      cancellationSignal: cancellationSignal,
    );
    return runZoned<Future<T>>(action, zoneValues: {_zoneKey: scope});
  }
}

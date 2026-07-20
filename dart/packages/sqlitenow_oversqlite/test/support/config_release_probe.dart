import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';

void main() {
  final invalidConfigs = <({String field, Object Function() create})>[
    (
      field: 'snapshotChunkRows',
      create: () => OversqliteConfig(
        schema: 'main',
        syncTables: const [],
        snapshotChunkRows: 0,
      ),
    ),
    (
      field: 'snapshotChunkBytes',
      create: () => OversqliteConfig(
        schema: 'main',
        syncTables: const [],
        snapshotChunkBytes: -1,
      ),
    ),
    (
      field: 'snapshotApplyBatchRows',
      create: () => OversqliteConfig(
        schema: 'main',
        syncTables: const [],
        snapshotApplyBatchRows: 0,
      ),
    ),
    (
      field: 'snapshotApplyBatchBytes',
      create: () => OversqliteConfig(
        schema: 'main',
        syncTables: const [],
        snapshotApplyBatchBytes: -1,
      ),
    ),
  ];
  for (final scenario in invalidConfigs) {
    _expectArgumentError(scenario.field, scenario.create);
  }

  final invalidPolicies = <({String field, Object Function() create})>[
    (
      field: 'maxWait',
      create: () =>
          OversqliteSnapshotCapacityRetryPolicy(maxWait: Duration.zero),
    ),
    (
      field: 'fallbackDelay',
      create: () => OversqliteSnapshotCapacityRetryPolicy(
        fallbackDelay: const Duration(microseconds: -1),
      ),
    ),
    for (final value in [double.nan, double.infinity, -0.1, 1.1])
      (
        field: 'positiveJitterRatio',
        create: () =>
            OversqliteSnapshotCapacityRetryPolicy(positiveJitterRatio: value),
      ),
  ];
  for (final scenario in invalidPolicies) {
    _expectArgumentError(scenario.field, scenario.create);
  }
}

void _expectArgumentError(String field, Object Function() create) {
  try {
    create();
  } on ArgumentError catch (error) {
    if (error.name != field || error.invalidValue == null) {
      throw StateError(
        'invalid $field used incomplete ArgumentError.value metadata',
      );
    }
    return;
  }
  throw StateError('invalid $field did not throw with assertions disabled');
}

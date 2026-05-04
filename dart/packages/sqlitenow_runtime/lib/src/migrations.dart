import 'dart:async';

import 'sqlite_connection.dart';

typedef SqliteNowMigrationBody =
    FutureOr<void> Function(SqliteNowConnection connection);

final class SqliteNowMigrationStep {
  const SqliteNowMigrationStep(this.version, this.migrate)
    : freshOnly = false,
      assert(version >= 0, 'Migration version must be non-negative');

  const SqliteNowMigrationStep.fresh(this.version, this.migrate)
    : freshOnly = true,
      assert(version >= 0, 'Migration version must be non-negative');

  final int version;
  final SqliteNowMigrationBody migrate;
  final bool freshOnly;
}

final class SqliteNowMigrationPlan {
  SqliteNowMigrationPlan(Iterable<SqliteNowMigrationStep> steps)
    : steps = List.unmodifiable(_validateSteps(steps));

  final List<SqliteNowMigrationStep> steps;

  int get latestVersion {
    if (steps.isEmpty) return 0;
    return steps.map((step) => step.version).reduce((a, b) => a > b ? a : b);
  }

  Future<int> apply(SqliteNowConnection connection, int currentVersion) async {
    if (currentVersion == -1) {
      final freshSteps = steps.where((step) => step.freshOnly);
      if (freshSteps.isNotEmpty) {
        for (final step in freshSteps) {
          await step.migrate(connection);
        }
        return latestVersion;
      }
    }

    var newVersion = currentVersion;
    for (final step in steps) {
      if (!step.freshOnly && step.version > currentVersion) {
        await step.migrate(connection);
        newVersion = step.version;
      }
    }
    if (newVersion == -1) return latestVersion;
    return newVersion;
  }
}

List<SqliteNowMigrationStep> _validateSteps(
  Iterable<SqliteNowMigrationStep> steps,
) {
  final sorted = [...steps]..sort((a, b) => a.version.compareTo(b.version));
  int? previous;
  for (final step in sorted) {
    if (step.freshOnly) continue;
    if (previous == step.version) {
      throw ArgumentError('Duplicate migration version ${step.version}');
    }
    previous = step.version;
  }
  return sorted;
}

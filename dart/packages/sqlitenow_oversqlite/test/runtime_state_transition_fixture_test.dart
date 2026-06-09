import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

import 'support/behavior_fixture_support.dart';
import 'support/runtime_state_fixture_support.dart';

void main() {
  final fixture =
      readRuntimeStateFixture(
            'oversqlite-contracts/runtime-state/transitions/basic.json',
          )!
          as Map<String, Object?>;

  test(
    'Dart shared runtime-state transition fixture matches runtime',
    () async {
      expect(fixture['formatVersion'], 1);
      for (final rawCase
          in (fixture['cases']! as List<Object?>)
              .cast<Map<String, Object?>>()) {
        await _runCase(rawCase);
      }
    },
  );
}

Future<void> _runCase(Map<String, Object?> fixture) async {
  final database = await openUsersDatabase();
  final server = _newServer(fixture);
  var client = newRuntimeStateClient(database, server);
  try {
    for (final step
        in (fixture['steps']! as List<Object?>).cast<Map<String, Object?>>()) {
      Object? error;
      try {
        switch (step['action']) {
          case 'open':
            await client.open();
          case 'attach':
            await client.attach('user-1');
          case 'localSql':
            await executeRuntimeStateSql(
              database,
              (step['sql'] as List<Object?>? ?? const []).cast<String>(),
            );
          case 'pushPending':
            await client.pushPending();
          case 'pullToStable':
            await client.pullToStable();
          case 'reopen':
            await client.close();
            client = newRuntimeStateClient(database, server);
            await client.open();
            await client.attach('user-1');
          case 'sourceInfo':
            await client.sourceInfo();
          default:
            fail('${fixture['name']}: unknown action ${step['action']}');
        }
      } catch (caught) {
        error = caught;
      }
      _expectException(
        fixture['name']! as String,
        step['expectedException'] as String? ?? 'none',
        error,
      );
      expect(
        await dumpRuntimeState(database),
        step['expectedState'],
        reason: '${fixture['name']}/${step['action']}',
      );
    }
  } finally {
    await client.close();
    await database.close();
  }
}

OversqliteHttpClient _newServer(Map<String, Object?> fixture) {
  final script =
      (fixture['serverScript'] as Map<Object?, Object?>?)
          ?.cast<String, Object?>() ??
      const {'kind': 'default'};
  switch (script['kind']) {
    case 'default':
      return PushFixtureServer();
    case 'precommit_retry':
      return PushFixtureServer(failFirstCommit: true);
    case 'committed_replay_first_fetch_http_error':
      return PushFixtureServer(failFirstCommittedFetch: true);
    case 'committed_bundle_seq_gap':
      return PushFixtureServer(committedBundleSeq: script['bundleSeq']! as int);
    case 'source_retired_on_push_create':
      return PushFixtureServer(
        sourceRetiredOnCreate: true,
        replacementSourceId: script['replacementSourceId']! as String,
      );
    case 'pull_incremental_users':
      return PullFixtureServer(
        pullResponse: (script['response']! as Map).cast<String, Object?>(),
      );
    default:
      fail('${fixture['name']}: unknown server script ${script['kind']}');
  }
}

void _expectException(String name, String expected, Object? error) {
  switch (expected) {
    case 'none':
      expect(error, isNull, reason: name);
    case 'http_error':
      expect(error, isA<OversqliteHttpException>(), reason: name);
    case 'rebuild_required':
      expect(error, isA<RebuildRequiredException>(), reason: name);
    case 'source_recovery_required':
      expect(error, isA<SourceRecoveryRequiredException>(), reason: name);
    case 'source_sequence_mismatch':
      expect(error, isA<SourceSequenceMismatchException>(), reason: name);
    default:
      fail('$name: unknown expected exception $expected');
  }
}

import 'dart:convert';
import 'dart:io';

import 'package:test/test.dart';

import 'support/behavior_fixture_support.dart';

void main() {
  final cases = _readWatchBehaviorCases();

  test('Dart shared watch behavior fixtures execute against runtime', () async {
    for (final fixture in cases) {
      await _runWatchCase(fixture);
    }
  });
}

Future<void> _runWatchCase(Map<String, Object?> fixture) async {
  final expected = (fixture['expectedState']! as Map).cast<String, Object?>();
  final script = (fixture['serverScript']! as Map).cast<String, Object?>();
  final config = expected.containsKey('watchAttemptsExactly')
      ? heartbeatWatchConfig
      : usersConfig;
  final env = await newWatchFixtureEnv(
    config: config,
    bundleChangeWatchSupported:
        script['bundleChangeWatchSupported'] as bool? ?? true,
  );
  try {
    _configureServer(env.server, fixture);
    final handle = env.client.startAutomaticDownloads();
    try {
      await _deliverWakeBundlesAfterWatchStart(env, script);
      await eventually(() async {
        return await _expectedReached(env, expected);
      });
      final observeForMillis = expected['observeForMillis'] as int?;
      if (observeForMillis != null) {
        await Future<void>.delayed(Duration(milliseconds: observeForMillis));
      }
      if (expected['stopActiveWatchBeforeAssert'] == true) {
        await handle.stop();
      }
      await _assertWatchState(env, fixture['name']! as String, expected);
    } finally {
      await handle.stop();
    }
  } finally {
    await env.close();
  }
}

void _configureServer(WatchFixtureServer server, Map<String, Object?> fixture) {
  final script = (fixture['serverScript']! as Map).cast<String, Object?>();
  for (final bundle
      in ((script['remoteBundles'] as List<Object?>?) ?? const [])) {
    final rows = ((bundle! as Map)['rows']! as List<Object?>)
        .cast<Map<String, Object?>>();
    server.addRemoteBundle(rows);
  }
  for (final bundle
      in ((script['startBundles'] as List<Object?>?) ?? const [])) {
    final rows = ((bundle! as Map)['rows']! as List<Object?>)
        .cast<Map<String, Object?>>();
    server.addRemoteBundle(rows);
  }
  switch (script['kind']) {
    case 'hold_watch_open':
      break;
    case 'unsupported_capability':
      break;
    case 'non_ok_watch_response':
      final response = (script['response']! as Map).cast<String, Object?>();
      server.enqueueWatchResponse(
        statusCode: response['status']! as int,
        body: jsonEncode(response['body']),
      );
    case 'watch_lines':
      server.enqueueWatchLines(
        (script['lines']! as List<Object?>).cast<String>(),
      );
    default:
      fail('${fixture['name']}: unknown server script ${script['kind']}');
  }
}

Future<void> _deliverWakeBundlesAfterWatchStart(
  WatchFixtureEnv env,
  Map<String, Object?> script,
) async {
  final wakeBundles = ((script['wakeBundles'] as List<Object?>?) ?? const []);
  if (wakeBundles.isEmpty) {
    return;
  }
  await eventually(() async => env.server.watchAfterBundleSeqs.isNotEmpty);
  for (final bundle in wakeBundles) {
    final rows = ((bundle! as Map)['rows']! as List<Object?>)
        .cast<Map<String, Object?>>();
    env.server.addRemoteBundle(rows);
  }
}

Future<bool> _expectedReached(
  WatchFixtureEnv env,
  Map<String, Object?> expected,
) async {
  for (final user in ((expected['users'] as List<Object?>?) ?? const [])) {
    final row = (user! as Map).cast<String, Object?>();
    final count = await scalarInt(
      env.database,
      "SELECT COUNT(*) FROM users WHERE id = '${row['id']}' AND name = '${row['name']}'",
    );
    if (count != 1) {
      return false;
    }
  }
  final attemptsAtLeast = expected['watchAttemptsAtLeast'] as int?;
  if (attemptsAtLeast != null &&
      env.server.watchAfterBundleSeqs.length < attemptsAtLeast) {
    return false;
  }
  final attemptsExactly = expected['watchAttemptsExactly'] as int?;
  if (attemptsExactly != null &&
      env.server.watchAfterBundleSeqs.length != attemptsExactly) {
    return false;
  }
  final afterSeqContains =
      ((expected['watchAfterBundleSeqsContains'] as List<Object?>?) ?? const [])
          .cast<int>();
  for (final afterSeq in afterSeqContains) {
    if (!env.server.watchAfterBundleSeqs.contains(afterSeq)) {
      return false;
    }
  }
  final pullsAtLeast = expected['fallbackPullsAtLeast'] as int?;
  if (pullsAtLeast != null && env.server.pullRequestCount < pullsAtLeast) {
    return false;
  }
  final pullsExactly = expected['fallbackPullsExactly'] as int?;
  if (pullsExactly != null && env.server.pullRequestCount != pullsExactly) {
    return false;
  }
  return true;
}

Future<void> _assertWatchState(
  WatchFixtureEnv env,
  String name,
  Map<String, Object?> expected,
) async {
  if (expected.containsKey('watchCloseCount')) {
    expect(
      env.server.watchCloseCount,
      expected['watchCloseCount'],
      reason: name,
    );
  }
  if (expected.containsKey('fallbackPullsAtLeast')) {
    expect(
      env.server.pullRequestCount,
      greaterThanOrEqualTo(expected['fallbackPullsAtLeast']! as int),
      reason: name,
    );
  }
  if (expected.containsKey('fallbackPullsExactly')) {
    expect(
      env.server.pullRequestCount,
      expected['fallbackPullsExactly'],
      reason: name,
    );
  }
  if (expected.containsKey('watchAttemptsAtLeast')) {
    expect(
      env.server.watchAfterBundleSeqs.length,
      greaterThanOrEqualTo(expected['watchAttemptsAtLeast']! as int),
      reason: name,
    );
  }
  if (expected.containsKey('watchAttemptsExactly')) {
    expect(
      env.server.watchAfterBundleSeqs.length,
      expected['watchAttemptsExactly'],
      reason: name,
    );
  }
  final afterSeqContains =
      ((expected['watchAfterBundleSeqsContains'] as List<Object?>?) ?? const [])
          .cast<int>();
  for (final afterSeq in afterSeqContains) {
    expect(env.server.watchAfterBundleSeqs, contains(afterSeq), reason: name);
  }
}

List<Map<String, Object?>> _readWatchBehaviorCases() {
  final file = repoRoot().uri
      .resolve('oversqlite-contracts/watch/behavior/automatic-downloads.json')
      .toFilePath();
  final raw = jsonDecode(File(file).readAsStringSync()) as Map<String, Object?>;
  return (raw['cases']! as List<Object?>).cast<Map<String, Object?>>();
}

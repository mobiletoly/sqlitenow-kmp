import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

void main() {
  final fixture = _readWatchFixture();

  test('watch capability fixtures decode', () {
    for (final item in fixture.capabilitiesCases) {
      final capabilities = CapabilitiesResponse.fromJson(item.response.body);
      expect(
        capabilities.bundleChangeWatchSupported,
        item.expectedBundleChangeWatchSupported,
        reason: item.name,
      );
    }
  });

  test('watch SSE fixtures parse from line stream', () async {
    for (final item in fixture.sseCases) {
      final stream = Stream<String>.fromIterable(item.lines);
      if (item.expectError) {
        await expectLater(
          parseBundleChangeEventStream(stream).drain<void>(),
          throwsA(anything),
          reason: item.name,
        );
      } else {
        expect(
          await parseBundleChangeEventStream(stream).toList(),
          item.expectedEvents,
          reason: item.name,
        );
        expect(
          parseBundleChangeEventLines(item.lines),
          item.expectedEvents,
          reason: '${item.name} sync parser',
        );
      }
    }
  });

  test('watch setup error fixtures remain structured', () {
    expect(fixture.setupErrorResponses.map((item) => item.status), [
      400,
      401,
      403,
      409,
      409,
      503,
      500,
    ]);
    for (final item in fixture.setupErrorResponses) {
      expect(item.body['error'], item.expectedError, reason: '${item.status}');
    }
  });

  test('watch runtime fixtures remain structured', () {
    expect(fixture.runtimeCases.map((item) => item.name), [
      'non-ok-watch-response-closes-before-fallback',
    ]);
    for (final item in fixture.runtimeCases) {
      expect(item.expectedCloseCount, 1, reason: item.name);
      expect(item.expectedFallbackPullsAtLeast, 1, reason: item.name);
      expect(item.response.body['error'], 'bundle_change_watch_disabled');
    }
  });
}

_WatchFixture _readWatchFixture() {
  final file = _repoRoot().uri
      .resolve('oversqlite-contracts/watch/basic.json')
      .toFilePath();
  final raw = jsonDecode(File(file).readAsStringSync()) as Map<String, Object?>;
  return _WatchFixture.fromJson(raw);
}

Directory _repoRoot() {
  var current = Directory.current;
  while (true) {
    if (File.fromUri(current.uri.resolve('settings.gradle.kts')).existsSync()) {
      return current;
    }
    final parent = current.parent;
    if (parent.path == current.path) {
      throw StateError(
        'Could not locate repository root from ${Directory.current.path}',
      );
    }
    current = parent;
  }
}

final class _WatchFixture {
  const _WatchFixture({
    required this.formatVersion,
    required this.capabilitiesCases,
    required this.sseCases,
    required this.setupErrorResponses,
    required this.runtimeCases,
  });

  final int formatVersion;
  final List<_CapabilityCase> capabilitiesCases;
  final List<_SseCase> sseCases;
  final List<_SetupErrorResponse> setupErrorResponses;
  final List<_RuntimeCase> runtimeCases;

  factory _WatchFixture.fromJson(Map<String, Object?> json) {
    return _WatchFixture(
      formatVersion: json['formatVersion']! as int,
      capabilitiesCases: (json['capabilitiesCases']! as List<Object?>)
          .cast<Map<String, Object?>>()
          .map(_CapabilityCase.fromJson)
          .toList(),
      sseCases: (json['sseCases']! as List<Object?>)
          .cast<Map<String, Object?>>()
          .map(_SseCase.fromJson)
          .toList(),
      setupErrorResponses: (json['setupErrorResponses']! as List<Object?>)
          .cast<Map<String, Object?>>()
          .map(_SetupErrorResponse.fromJson)
          .toList(),
      runtimeCases: (json['runtimeCases']! as List<Object?>)
          .cast<Map<String, Object?>>()
          .map(_RuntimeCase.fromJson)
          .toList(),
    );
  }
}

final class _CapabilityCase {
  const _CapabilityCase({
    required this.name,
    required this.response,
    required this.expectedBundleChangeWatchSupported,
  });

  final String name;
  final _FixtureHttpResponse response;
  final bool expectedBundleChangeWatchSupported;

  factory _CapabilityCase.fromJson(Map<String, Object?> json) {
    return _CapabilityCase(
      name: json['name']! as String,
      response: _FixtureHttpResponse.fromJson(
        json['response']! as Map<String, Object?>,
      ),
      expectedBundleChangeWatchSupported:
          json['expectedBundleChangeWatchSupported']! as bool,
    );
  }
}

final class _SseCase {
  const _SseCase({
    required this.name,
    required this.lines,
    required this.expectedEvents,
    required this.expectError,
  });

  final String name;
  final List<String> lines;
  final List<BundleChangeEvent> expectedEvents;
  final bool expectError;

  factory _SseCase.fromJson(Map<String, Object?> json) {
    return _SseCase(
      name: json['name']! as String,
      lines: (json['lines']! as List<Object?>).cast<String>(),
      expectedEvents: ((json['expectedEvents'] as List<Object?>?) ?? const [])
          .cast<Map<String, Object?>>()
          .map(BundleChangeEvent.fromJson)
          .toList(),
      expectError: (json['expectError'] as bool?) ?? false,
    );
  }
}

final class _SetupErrorResponse {
  const _SetupErrorResponse({
    required this.status,
    required this.body,
    required this.expectedError,
  });

  final int status;
  final Map<String, Object?> body;
  final String expectedError;

  factory _SetupErrorResponse.fromJson(Map<String, Object?> json) {
    return _SetupErrorResponse(
      status: json['status']! as int,
      body: (json['body']! as Map).cast<String, Object?>(),
      expectedError: json['expectedError']! as String,
    );
  }
}

final class _RuntimeCase {
  const _RuntimeCase({
    required this.name,
    required this.response,
    required this.expectedCloseCount,
    required this.expectedFallbackPullsAtLeast,
  });

  final String name;
  final _FixtureHttpResponse response;
  final int expectedCloseCount;
  final int expectedFallbackPullsAtLeast;

  factory _RuntimeCase.fromJson(Map<String, Object?> json) {
    return _RuntimeCase(
      name: json['name']! as String,
      response: _FixtureHttpResponse.fromJson(
        (json['response']! as Map).cast<String, Object?>(),
      ),
      expectedCloseCount: json['expectedCloseCount']! as int,
      expectedFallbackPullsAtLeast:
          json['expectedFallbackPullsAtLeast']! as int,
    );
  }
}

final class _FixtureHttpResponse {
  const _FixtureHttpResponse({required this.status, required this.body});

  final int status;
  final Map<String, Object?> body;

  factory _FixtureHttpResponse.fromJson(Map<String, Object?> json) {
    return _FixtureHttpResponse(
      status: json['status']! as int,
      body: (json['body']! as Map).cast<String, Object?>(),
    );
  }
}

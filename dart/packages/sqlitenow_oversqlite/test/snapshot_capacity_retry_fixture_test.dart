import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:sqlitenow_oversqlite/src/snapshot_capacity_retry.dart';
import 'package:test/test.dart';

import 'support/behavior_fixture_support.dart';

void main() {
  final fixture =
      jsonDecode(
            File.fromUri(
              repoRoot().uri.resolve(
                'oversqlite-contracts/snapshot-capacity/retry.json',
              ),
            ).readAsStringSync(),
          )
          as Map<String, Object?>;

  test('Dart capacity policy consumes the canonical defaults', () {
    expect(fixture['contract'], 'snapshot-capacity-retry-v1');
    final defaults = (fixture['defaults']! as Map).cast<String, Object?>();
    final policy = OversqliteSnapshotCapacityRetryPolicy();

    expect(policy.enabled, defaults['enabled']);
    expect(policy.maxWait.inMilliseconds, defaults['max_wait_millis']);
    expect(
      policy.fallbackDelay.inMilliseconds,
      defaults['fallback_delay_millis'],
    );
    expect(policy.positiveJitterRatio, defaults['positive_jitter_ratio']);
  });

  test('Retry-After accepts only canonical positive delta seconds', () {
    for (final raw
        in (fixture['retry_after_cases']! as List<Object?>)
            .cast<Map<String, Object?>>()) {
      expect(
        parseSnapshotRetryAfter(raw['wire']! as String)?.inMilliseconds,
        raw['expected_millis'],
        reason: raw['wire']! as String,
      );
    }
  });

  test(
    'capacity retry is elapsed-budgeted and independent of attempts',
    () async {
      var attempts = 0;
      final delays = <Duration>[];
      final diagnostics = SnapshotDiagnosticsRecorder();

      final result = await withSnapshotCapacityRetry(
        policy: OversqliteSnapshotCapacityRetryPolicy(
          maxWait: Duration(seconds: 5),
          fallbackDelay: Duration(milliseconds: 1),
          positiveJitterRatio: 0,
        ),
        operation: 'snapshot create',
        diagnostics: diagnostics,
        random: Random(1),
        delay: (duration) async => delays.add(duration),
        attempt: () async {
          attempts++;
          if (attempts < 4) {
            throw const SnapshotCapacityException(
              statusCode: 429,
              errorCode: 'snapshot_build_capacity',
            );
          }
          return 'ok';
        },
      );

      expect(result, 'ok');
      expect(attempts, 4);
      expect(delays, [
        const Duration(milliseconds: 1),
        const Duration(milliseconds: 1),
        const Duration(milliseconds: 1),
      ]);
      expect(diagnostics.snapshot().capacityResponseCount, 3);
      expect(diagnostics.snapshot().capacityRetryCount, 3);
    },
  );

  test('positive jitter stays in the inclusive configured interval', () async {
    var attempts = 0;
    final delays = <Duration>[];
    final result = await withSnapshotCapacityRetry(
      policy: OversqliteSnapshotCapacityRetryPolicy(
        maxWait: Duration(seconds: 1),
        fallbackDelay: Duration(milliseconds: 10),
        positiveJitterRatio: 0.5,
      ),
      operation: 'snapshot chunk',
      random: const _MaximumRandom(),
      delay: (duration) async => delays.add(duration),
      attempt: () async {
        attempts++;
        if (attempts == 1) {
          throw const SnapshotCapacityException(
            statusCode: 429,
            errorCode: 'snapshot_chunk_capacity',
          );
        }
        return 'ok';
      },
    );

    expect(result, 'ok');
    expect(delays, [const Duration(milliseconds: 15)]);
    expect(
      delays.single,
      allOf(
        greaterThanOrEqualTo(const Duration(milliseconds: 10)),
        lessThanOrEqualTo(const Duration(milliseconds: 15)),
      ),
    );
  });

  test(
    'injected monotonic clock exhausts the cumulative wait budget',
    () async {
      var now = Duration.zero;
      var attempts = 0;
      final delays = <Duration>[];

      await expectLater(
        withSnapshotCapacityRetry<void>(
          policy: OversqliteSnapshotCapacityRetryPolicy(
            maxWait: Duration(milliseconds: 25),
            fallbackDelay: Duration(milliseconds: 10),
            positiveJitterRatio: 0,
          ),
          operation: 'snapshot chunk',
          elapsedSinceStart: () => now,
          delay: (duration) async {
            delays.add(duration);
            now += duration;
          },
          attempt: () async {
            attempts++;
            throw const SnapshotCapacityException(
              statusCode: 429,
              errorCode: 'snapshot_chunk_capacity',
            );
          },
        ),
        throwsA(
          isA<SnapshotCapacityRetryExhaustedException>().having(
            (error) => error.waited,
            'waited',
            const Duration(milliseconds: 20),
          ),
        ),
      );

      expect(attempts, 3);
      expect(delays, [
        const Duration(milliseconds: 10),
        const Duration(milliseconds: 10),
      ]);
    },
  );

  test(
    'post-delay monotonic overshoot records actual wait and stops retry',
    () async {
      var now = Duration.zero;
      var attempts = 0;
      final diagnostics = SnapshotDiagnosticsRecorder();

      await expectLater(
        withSnapshotCapacityRetry<void>(
          policy: OversqliteSnapshotCapacityRetryPolicy(
            maxWait: const Duration(milliseconds: 10),
            fallbackDelay: const Duration(milliseconds: 10),
            positiveJitterRatio: 0,
          ),
          operation: 'snapshot chunk',
          diagnostics: diagnostics,
          elapsedSinceStart: () => now,
          delay: (duration) async {
            expect(duration, const Duration(milliseconds: 10));
            now += const Duration(milliseconds: 11);
          },
          attempt: () async {
            attempts++;
            throw const SnapshotCapacityException(
              statusCode: 429,
              errorCode: 'snapshot_chunk_capacity',
            );
          },
        ),
        throwsA(
          isA<SnapshotCapacityRetryExhaustedException>().having(
            (error) => error.waited,
            'waited',
            const Duration(milliseconds: 11),
          ),
        ),
      );

      expect(attempts, 1);
      expect(
        diagnostics.snapshot().capacityWait,
        const Duration(milliseconds: 11),
      );
    },
  );

  test('post-delay exact deadline still permits one retry', () async {
    var now = Duration.zero;
    var attempts = 0;

    final result = await withSnapshotCapacityRetry(
      policy: OversqliteSnapshotCapacityRetryPolicy(
        maxWait: const Duration(milliseconds: 10),
        fallbackDelay: const Duration(milliseconds: 10),
        positiveJitterRatio: 0,
      ),
      operation: 'snapshot create',
      elapsedSinceStart: () => now,
      delay: (duration) async => now += duration,
      attempt: () async {
        attempts++;
        if (attempts == 1) {
          throw const SnapshotCapacityException(
            statusCode: 429,
            errorCode: 'snapshot_build_capacity',
          );
        }
        return 'ok';
      },
    );

    expect(result, 'ok');
    expect(attempts, 2);
  });

  test(
    'disabled and over-budget capacity fail with a typed redacted error',
    () async {
      for (final policy in [
        OversqliteSnapshotCapacityRetryPolicy(enabled: false),
        OversqliteSnapshotCapacityRetryPolicy(
          maxWait: Duration(milliseconds: 10),
          fallbackDelay: Duration(milliseconds: 20),
          positiveJitterRatio: 0,
        ),
      ]) {
        final error = await expectLater(
          withSnapshotCapacityRetry<void>(
            policy: policy,
            operation: 'snapshot chunk',
            attempt: () async => throw const SnapshotCapacityException(
              statusCode: 429,
              errorCode: 'snapshot_chunk_capacity',
            ),
          ),
          throwsA(
            isA<SnapshotCapacityRetryExhaustedException>().having(
              (value) => value.errorCode,
              'errorCode',
              'snapshot_chunk_capacity',
            ),
          ),
        );
        expect(error, isNull);
      }
    },
  );

  test(
    'large Retry-After is rejected before default jitter sampling',
    () async {
      await expectLater(
        withSnapshotCapacityRetry<void>(
          policy: OversqliteSnapshotCapacityRetryPolicy(),
          operation: 'snapshot create',
          random: const _FailIfSampledRandom(),
          attempt: () async => throw const SnapshotCapacityException(
            statusCode: 429,
            errorCode: 'snapshot_build_capacity',
            retryAfter: Duration(seconds: 4295),
          ),
        ),
        throwsA(
          isA<SnapshotCapacityRetryExhaustedException>().having(
            (error) => error.waited,
            'waited',
            lessThan(const Duration(seconds: 1)),
          ),
        ),
      );
    },
  );

  test(
    'long in-budget Retry-After jitter does not use bounded nextInt',
    () async {
      var attempts = 0;
      final delays = <Duration>[];
      final result = await withSnapshotCapacityRetry(
        policy: OversqliteSnapshotCapacityRetryPolicy(
          maxWait: const Duration(hours: 3),
        ),
        operation: 'snapshot create',
        random: const _MaximumRandom(),
        delay: (duration) async => delays.add(duration),
        attempt: () async {
          attempts++;
          if (attempts == 1) {
            throw const SnapshotCapacityException(
              statusCode: 429,
              errorCode: 'snapshot_build_capacity',
              retryAfter: Duration(seconds: 4295),
            );
          }
          return 'ok';
        },
      );

      expect(result, 'ok');
      expect(attempts, 2);
      expect(
        delays.single,
        greaterThanOrEqualTo(const Duration(seconds: 4295)),
      );
      expect(delays.single, lessThanOrEqualTo(const Duration(seconds: 8590)));
    },
  );

  test('only canonical capacity codes become retryable', () async {
    final codes = (fixture['capacity_error_codes']! as List<Object?>)
        .cast<String>();
    for (final code in codes) {
      final api = OversqliteRemoteApi(
        _StaticHttpClient(
          OversqliteHttpResponse(
            statusCode: 429,
            body: jsonEncode({'error': code, 'message': 'busy'}),
          ),
        ),
      );
      await expectLater(
        api.fetchCapabilities('source-1'),
        throwsA(
          isA<SnapshotCapacityException>().having(
            (error) => error.errorCode,
            'errorCode',
            code,
          ),
        ),
      );
    }

    final legacy = OversqliteRemoteApi(
      _StaticHttpClient(
        OversqliteHttpResponse(
          statusCode: 429,
          body: jsonEncode({
            'error': 'snapshot_capacity',
            'message': 'must not retry',
          }),
        ),
      ),
    );
    await expectLater(
      legacy.fetchCapabilities('source-1'),
      throwsA(isA<OversqliteHttpException>()),
    );
  });

  test('malformed capacity envelope is terminal after one request', () async {
    var requests = 0;
    final api = OversqliteRemoteApi(
      _StaticHttpClient(
        OversqliteHttpResponse(
          statusCode: 429,
          body: jsonEncode({'error': 'snapshot_build_capacity'}),
        ),
        onRequest: () => requests++,
      ),
    );

    await expectLater(
      withSnapshotCapacityRetry<void>(
        policy: OversqliteSnapshotCapacityRetryPolicy(
          fallbackDelay: const Duration(milliseconds: 1),
          positiveJitterRatio: 0,
        ),
        operation: 'snapshot create',
        attempt: () async {
          await api.createSnapshotSession(sourceId: 'source-1');
        },
      ),
      throwsA(
        isA<SnapshotHttpException>().having(
          (error) => error.errorCode,
          'errorCode',
          'invalid_error_response',
        ),
      ),
    );
    expect(requests, 1);
  });

  test('capacity body is closed before delay and next attempt', () async {
    var firstClosed = false;
    var attempts = 0;
    final transport = _SequenceHttpClient([
      OversqliteHttpResponse(
        statusCode: 429,
        body: jsonEncode({
          'error': 'snapshot_build_capacity',
          'message': 'busy',
        }),
        headers: const {'Retry-After': '1'},
        close: () async => firstClosed = true,
      ),
      OversqliteHttpResponse(
        statusCode: 200,
        body: jsonEncode({
          'snapshot_id': 'snapshot-1',
          'snapshot_bundle_seq': 0,
          'row_count': 0,
          'byte_count': 0,
          'expires_at': '2099-01-01T00:00:00Z',
        }),
      ),
    ]);
    final api = OversqliteRemoteApi(transport);

    final session = await withSnapshotCapacityRetry(
      policy: OversqliteSnapshotCapacityRetryPolicy(
        maxWait: Duration(seconds: 2),
        positiveJitterRatio: 0,
      ),
      operation: 'snapshot create',
      delay: (duration) async {
        expect(duration, const Duration(seconds: 1));
        expect(firstClosed, isTrue);
      },
      attempt: () {
        attempts++;
        if (attempts == 2) expect(firstClosed, isTrue);
        return api.createSnapshotSession(sourceId: 'source-1');
      },
    );

    expect(session.snapshotId, 'snapshot-1');
    expect(attempts, 2);
  });

  test(
    'cancellation is checked before delay and before the next attempt',
    () async {
      var cancelled = false;
      var attempts = 0;

      await expectLater(
        withSnapshotCapacityRetry<void>(
          policy: OversqliteSnapshotCapacityRetryPolicy(
            fallbackDelay: Duration(milliseconds: 1),
            positiveJitterRatio: 0,
          ),
          operation: 'snapshot chunk',
          isCancelled: () => cancelled,
          delay: (_) async => cancelled = true,
          attempt: () async {
            attempts++;
            throw const SnapshotCapacityException(
              statusCode: 429,
              errorCode: 'snapshot_chunk_capacity',
            );
          },
        ),
        throwsA(isA<SnapshotCapacityRetryCancelledException>()),
      );
      expect(attempts, 1);

      await expectLater(
        withSnapshotCapacityRetry<void>(
          policy: OversqliteSnapshotCapacityRetryPolicy(),
          operation: 'snapshot create',
          isCancelled: () => true,
          attempt: () async => fail('cancelled operation attempted'),
        ),
        throwsA(isA<SnapshotCapacityRetryCancelledException>()),
      );
    },
  );

  test('active capacity wait races the cancellation signal', () async {
    var cancelled = false;
    var attempts = 0;
    final signal = Completer<void>();
    final waitStarted = Completer<void>();
    final retry = withSnapshotCapacityRetry<void>(
      policy: OversqliteSnapshotCapacityRetryPolicy(
        maxWait: const Duration(hours: 1),
        fallbackDelay: const Duration(minutes: 30),
        positiveJitterRatio: 0,
      ),
      operation: 'snapshot create',
      isCancelled: () => cancelled,
      cancellationSignal: signal.future,
      delay: (_) {
        waitStarted.complete();
        return Completer<void>().future;
      },
      attempt: () async {
        attempts++;
        throw const SnapshotCapacityException(
          statusCode: 429,
          errorCode: 'snapshot_build_capacity',
        );
      },
    );

    await waitStarted.future;
    cancelled = true;
    signal.complete();

    await expectLater(
      retry.timeout(const Duration(seconds: 1)),
      throwsA(isA<SnapshotCapacityRetryCancelledException>()),
    );
    expect(attempts, 1);
  });
}

final class _MaximumRandom implements Random {
  const _MaximumRandom();

  @override
  bool nextBool() => true;

  @override
  double nextDouble() => 0.9999999999999999;

  @override
  int nextInt(int max) => max - 1;
}

final class _FailIfSampledRandom implements Random {
  const _FailIfSampledRandom();

  Never _fail() => throw StateError('jitter must not be sampled');

  @override
  bool nextBool() => _fail();

  @override
  double nextDouble() => _fail();

  @override
  int nextInt(int max) => _fail();
}

final class _StaticHttpClient implements OversqliteHttpClient {
  const _StaticHttpClient(this.response, {this.onRequest});

  final OversqliteHttpResponse response;
  final void Function()? onRequest;

  @override
  Future<OversqliteHttpResponse> get(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async {
    onRequest?.call();
    return response;
  }

  @override
  Future<OversqliteHttpResponse> postJson(
    String path, {
    required String sourceId,
    required Object? body,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async {
    onRequest?.call();
    return response;
  }

  @override
  Future<OversqliteHttpResponse> delete(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async {
    onRequest?.call();
    return response;
  }
}

final class _SequenceHttpClient implements OversqliteHttpClient {
  _SequenceHttpClient(this.responses);

  final List<OversqliteHttpResponse> responses;

  OversqliteHttpResponse _next() => responses.removeAt(0);

  @override
  Future<OversqliteHttpResponse> get(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async => _next();

  @override
  Future<OversqliteHttpResponse> postJson(
    String path, {
    required String sourceId,
    required Object? body,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async => _next();

  @override
  Future<OversqliteHttpResponse> delete(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async => _next();
}

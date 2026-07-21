import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

import 'support/behavior_fixture_support.dart';

void main() {
  test('snapshot operations carry independent decoded-body bounds', () async {
    final transport = _BoundRecordingClient();
    final api = OversqliteRemoteApi(transport);

    await api.fetchCapabilities('source-1');
    await api.createSnapshotSession(sourceId: 'source-1');
    await api.fetchSnapshotChunk(
      snapshotId: 'snapshot-1',
      sourceId: 'source-1',
      snapshotBundleSeq: 0,
      afterRowOrdinal: 0,
      maxRows: 2,
      maxBytes: 32,
    );

    expect(
      transport.bounds['capabilities request']!.successBodyBytes,
      4 * 1024 * 1024,
    );
    expect(
      transport.bounds['snapshot session request']!.successBodyBytes,
      64 * 1024,
    );
    expect(
      transport.bounds['snapshot chunk request']!.successBodyBytes,
      32 + 2 + 64 * 1024,
    );
    for (final bounds in transport.bounds.values) {
      expect(bounds.errorBodyBytes, 64 * 1024);
    }
  });

  test('custom transports are defensively bounded and always closed', () async {
    var closed = false;
    final api = OversqliteRemoteApi(
      _StaticHttpClient(
        OversqliteHttpResponse(
          statusCode: 200,
          body: 'x' * (64 * 1024 + 1),
          close: () async => closed = true,
        ),
      ),
    );

    await expectLater(
      api.createSnapshotSession(sourceId: 'source-1'),
      throwsA(
        isA<SnapshotResponseBodyTooLargeException>().having(
          (error) => error.limit,
          'limit',
          64 * 1024,
        ),
      ),
    );
    expect(closed, isTrue);
  });

  test(
    'invalid created sessions are retired without replacing the error',
    () async {
      final transport = _InvalidSessionClient();
      final api = OversqliteRemoteApi(transport);

      await expectLater(
        api.createSnapshotSession(sourceId: 'source-1'),
        throwsA(isA<SnapshotSemanticException>()),
      );

      expect(transport.retiredSnapshotIds, ['retire-me']);
      expect(transport.deleteBounds.single.discardBody, isTrue);
      expect(transport.createdResponseCloseCount, 1);
      expect(transport.retirementResponseCloseCount, 1);
    },
  );

  test('whitespace-only created snapshot ID is retired exactly', () async {
    final transport = _InvalidSessionClient(snapshotId: ' \t');
    final api = OversqliteRemoteApi(transport);

    await expectLater(
      api.createSnapshotSession(sourceId: 'source-1'),
      throwsA(isA<SnapshotSemanticException>()),
    );

    expect(transport.retiredSnapshotIds, ['%20%09']);
    expect(transport.createdResponseCloseCount, 1);
    expect(transport.retirementResponseCloseCount, 1);
  });

  test(
    'retirement is independently bounded and preserves the primary error',
    () async {
      final transport = _InvalidSessionClient(hangRetirement: true);
      final api = OversqliteRemoteApi(transport);
      final stopwatch = Stopwatch()..start();

      await expectLater(
        api.createSnapshotSession(sourceId: 'source-1'),
        throwsA(isA<SnapshotSemanticException>()),
      );
      stopwatch.stop();

      expect(
        OversqliteRemoteApi.snapshotRetirementTimeout,
        const Duration(seconds: 5),
      );
      expect(
        stopwatch.elapsed,
        greaterThanOrEqualTo(const Duration(milliseconds: 4900)),
      );
      expect(stopwatch.elapsed, lessThan(const Duration(seconds: 8)));
      expect(transport.deleteBounds.single.discardBody, isTrue);
      expect(transport.retirementResponseCloseCount, 1);
    },
    timeout: const Timeout(Duration(seconds: 10)),
  );

  test(
    'diagnostics include complete terminal bodies but exclude oversize',
    () async {
      final recorder = SnapshotDiagnosticsRecorder();
      final first = jsonEncode({
        'error': 'snapshot_chunk_capacity',
        'message': 'retry',
      });
      final second = jsonEncode({'error': 'terminal', 'message': 'stop'});
      final transport = _QueueHttpClient([
        OversqliteHttpResponse(statusCode: 429, body: first),
        OversqliteHttpResponse(statusCode: 409, body: second),
        OversqliteHttpResponse(statusCode: 500, body: 'x' * (64 * 1024 + 1)),
      ]);
      final api = OversqliteRemoteApi(transport, snapshotDiagnostics: recorder);

      await expectLater(
        _fetchEmptyChunk(api),
        throwsA(isA<SnapshotCapacityException>()),
      );
      await expectLater(
        _fetchEmptyChunk(api),
        throwsA(isA<SnapshotHttpException>()),
      );
      final beforeOversize = recorder.snapshot().maxCompletelyDecodedBodyBytes;
      await expectLater(
        _fetchEmptyChunk(api),
        throwsA(isA<SnapshotResponseBodyTooLargeException>()),
      );

      expect(beforeOversize, greaterThanOrEqualTo(utf8.encode(first).length));
      expect(beforeOversize, greaterThanOrEqualTo(utf8.encode(second).length));
      expect(recorder.snapshot().maxCompletelyDecodedBodyBytes, beforeOversize);
    },
  );

  test('terminal snapshot errors are redacted', () async {
    const sentinel = 'SECRET-REMOTE-BODY-AND-TOKEN';
    final api = OversqliteRemoteApi(
      _StaticHttpClient(
        OversqliteHttpResponse(
          statusCode: 409,
          body: jsonEncode({'error': 'hostile', 'message': sentinel}),
        ),
      ),
    );

    Object? error;
    try {
      await _fetchEmptyChunk(api);
    } catch (caught) {
      error = caught;
    }
    expect(error, isA<SnapshotHttpException>());
    expect(error.toString(), isNot(contains(sentinel)));
  });

  test('source retirement decoding is exact and token-free', () async {
    const sourceId = 'source-1';
    const replacementSentinel = 'replacement-SECRET-token';
    final accepted = <Map<String, Object?>>[
      {'error': 'source_retired', 'message': 'retired', 'source_id': sourceId},
      {
        'error': 'source_retired',
        'message': 'retired',
        'source_id': sourceId,
        'replaced_by_source_id': replacementSentinel,
      },
    ];
    for (final body in accepted) {
      Object? caught;
      try {
        await OversqliteRemoteApi(
          _StaticHttpClient(
            OversqliteHttpResponse(statusCode: 409, body: jsonEncode(body)),
          ),
        ).createSnapshotSession(sourceId: sourceId);
      } catch (error) {
        caught = error;
      }
      expect(caught, isA<SourceRecoveryRequiredHttpException>());
      final error = caught! as SourceRecoveryRequiredHttpException;
      expect(error.reason, SourceRecoveryReason.sourceRetired);
      expect(error.toString(), isNot(contains(replacementSentinel)));
      expect(error.toString(), isNot(contains('source-1')));
    }

    final malformed = <Map<String, Object?>>[
      {'error': 'source_retired', 'source_id': sourceId},
      {'error': 'source_retired', 'message': null, 'source_id': sourceId},
      {'error': 'source_retired', 'message': 7, 'source_id': sourceId},
      {'error': 'source_retired', 'message': 'retired', 'source_id': null},
      {'error': 'source_retired', 'message': 'retired', 'source_id': ''},
      {
        'error': 'source_retired',
        'message': 'retired',
        'source_id': ' source-1',
      },
      {
        'error': 'source_retired',
        'message': 'retired',
        'source_id': 'source-1 ',
      },
      {
        'error': 'source_retired',
        'message': 'retired',
        'source_id': 'source-2',
      },
      {
        'error': 'source_retired',
        'message': 'retired',
        'source_id': sourceId,
        'replaced_by_source_id': null,
      },
      {
        'error': 'source_retired',
        'message': 'retired',
        'source_id': sourceId,
        'replaced_by_source_id': '',
      },
      {
        'error': 'source_retired',
        'message': 'retired',
        'source_id': sourceId,
        'replaced_by_source_id': 7,
      },
      {
        'error': 'source_retired',
        'message': 'retired',
        'source_id': sourceId,
        'replaced_by_source_id': ' replacement',
      },
      {
        'error': 'source_retired',
        'message': 'retired',
        'source_id': sourceId,
        'replaced_by_source_id': 'replacement ',
      },
      {
        'error': 'source_retired',
        'message': 'retired',
        'source_id': sourceId,
        'replaced_by_source_id': '   ',
      },
      {
        'error': 'source_retired',
        'message': 'retired',
        'source_id': sourceId,
        'replaced_by_source_id': 'replacé',
      },
    ];
    for (final body in malformed) {
      await expectLater(
        OversqliteRemoteApi(
          _StaticHttpClient(
            OversqliteHttpResponse(statusCode: 409, body: jsonEncode(body)),
          ),
        ).createSnapshotSession(sourceId: sourceId),
        throwsA(
          isA<SnapshotHttpException>().having(
            (error) => error.errorCode,
            'errorCode',
            'invalid_error_response',
          ),
        ),
        reason: jsonEncode(body),
      );
    }

    const publicError = SourceRecoveryRequiredHttpException(
      reason: SourceRecoveryReason.sourceRetired,
    );
    const divergence = SourceReplacementDivergedException();
    expect(publicError.toString(), isNot(contains(replacementSentinel)));
    expect(divergence.toString(), isNot(contains(replacementSentinel)));
  });

  test(
    'capabilities sentinel is redacted from every public error field',
    () async {
      const sentinel = 'SECRET-CAPABILITIES-BODY-AND-TOKEN';
      final api = OversqliteRemoteApi(
        _StaticHttpClient(
          OversqliteHttpResponse(
            statusCode: 500,
            body: jsonEncode({
              'error': 'hostile-$sentinel',
              'message': sentinel,
            }),
          ),
        ),
      );

      Object? caught;
      try {
        await api.fetchCapabilities('source-1');
      } catch (error) {
        caught = error;
      }

      expect(caught, isA<SnapshotHttpException>());
      final error = caught! as SnapshotHttpException;
      expect(error.errorCode, 'invalid_error_response');
      expect(error.body, isEmpty);
      expect(error.errorResponse?.error, isEmpty);
      expect(error.errorResponse?.message, isEmpty);
      expect(error.toString(), isNot(contains(sentinel)));
    },
  );

  test(
    'locked snapshot error shapes preserve their typed exceptions',
    () async {
      final chunkTooSmall = OversqliteRemoteApi(
        _StaticHttpClient(
          OversqliteHttpResponse(
            statusCode: 400,
            body: jsonEncode({
              'error': 'snapshot_chunk_too_small',
              'message': 'increase max_bytes',
              'required_byte_count': 2,
            }),
          ),
        ),
      );
      await expectLater(
        _fetchEmptyChunk(chunkTooSmall),
        throwsA(
          isA<SnapshotChunkTooSmallException>()
              .having((error) => error.configuredBytes, 'configuredBytes', 1)
              .having((error) => error.requiredBytes, 'requiredBytes', 2),
        ),
      );

      final sessionLimit = OversqliteRemoteApi(
        _StaticHttpClient(
          OversqliteHttpResponse(
            statusCode: 409,
            body: jsonEncode({
              'error': 'snapshot_session_limit_exceeded',
              'message': 'session too large',
              'dimension': 'row_count',
              'actual': 2,
              'limit': 1,
            }),
          ),
        ),
      );
      await expectLater(
        sessionLimit.createSnapshotSession(sourceId: 'source-1'),
        throwsA(
          isA<SnapshotSessionLimitExceededException>()
              .having((error) => error.dimension, 'dimension', 'row_count')
              .having((error) => error.actual, 'actual', 2)
              .having((error) => error.limit, 'limit', 1),
        ),
      );
    },
  );

  test('typed snapshot errors fail closed on malformed fields', () async {
    Future<void> expectInvalid(
      int status,
      Map<String, Object?> body, {
      bool chunk = false,
    }) async {
      final api = OversqliteRemoteApi(
        _StaticHttpClient(
          OversqliteHttpResponse(statusCode: status, body: jsonEncode(body)),
        ),
      );
      await expectLater(
        chunk
            ? _fetchEmptyChunk(api)
            : api.createSnapshotSession(sourceId: 'source-1'),
        throwsA(
          isA<SnapshotHttpException>().having(
            (error) => error.errorCode,
            'errorCode',
            'invalid_error_response',
          ),
        ),
        reason: jsonEncode(body),
      );
    }

    for (final code in ['snapshot_build_capacity', 'snapshot_chunk_capacity']) {
      for (final message in <Object?>[null, 7]) {
        await expectInvalid(429, {'error': code, 'message': message});
      }
      await expectInvalid(429, {'error': code});
    }

    for (final requiredBytes in <Object?>[
      null,
      '2',
      -1,
      _jsonNumber('9223372036854775808'),
    ]) {
      await expectInvalid(400, {
        'error': 'snapshot_chunk_too_small',
        'message': 'increase max_bytes',
        'required_byte_count': requiredBytes,
      }, chunk: true);
    }
    await expectInvalid(400, {
      'error': 'snapshot_chunk_too_small',
      'message': 'increase max_bytes',
    }, chunk: true);

    for (final field in ['actual', 'limit']) {
      for (final value in <Object?>[
        null,
        '2',
        -1,
        _jsonNumber('9223372036854775808'),
      ]) {
        await expectInvalid(409, {
          'error': 'snapshot_session_limit_exceeded',
          'message': 'session too large',
          'dimension': 'row_count',
          'actual': 2,
          'limit': 1,
          field: value,
        });
      }
    }
  });

  test(
    'custom response closure is exactly once for every ingress outcome',
    () async {
      final scenarios =
          <
            ({
              String name,
              OversqliteHttpResponse Function(Future<void> Function() close)
              response,
              Future<void> Function(OversqliteRemoteApi api) invoke,
              Matcher outcome,
            })
          >[
            (
              name: 'success',
              response: (close) => OversqliteHttpResponse(
                statusCode: 200,
                body: jsonEncode(
                  phase4CapabilitiesResponse(
                    registeredTableSpecs: phase4RegisteredTableSpecs(['users']),
                  ),
                ),
                close: close,
              ),
              invoke: (api) async => api.fetchCapabilities('source-1'),
              outcome: completes,
            ),
            (
              name: 'http error',
              response: (close) => OversqliteHttpResponse(
                statusCode: 500,
                body: '{"error":"unknown"}',
                close: close,
              ),
              invoke: (api) async => api.fetchCapabilities('source-1'),
              outcome: throwsA(isA<SnapshotHttpException>()),
            ),
            (
              name: 'oversize',
              response: (close) => OversqliteHttpResponse(
                statusCode: 200,
                body: 'x' * (64 * 1024 + 1),
                close: close,
              ),
              invoke: (api) async =>
                  api.createSnapshotSession(sourceId: 'source-1'),
              outcome: throwsA(isA<SnapshotResponseBodyTooLargeException>()),
            ),
            (
              name: 'unsupported encoding',
              response: (close) => OversqliteHttpResponse(
                statusCode: 200,
                body: '{}',
                headers: const {'content-encoding': 'br'},
                close: close,
              ),
              invoke: (api) async => api.fetchCapabilities('source-1'),
              outcome: throwsA(
                isA<SnapshotUnsupportedContentEncodingException>(),
              ),
            ),
            (
              name: 'decoded count mismatch',
              response: (close) => OversqliteHttpResponse(
                statusCode: 200,
                body: '{}',
                decodedBodyBytes: 1,
                close: close,
              ),
              invoke: (api) async => api.fetchCapabilities('source-1'),
              outcome: throwsA(isA<SnapshotResponseDecodeException>()),
            ),
            (
              name: 'malformed JSON',
              response: (close) => OversqliteHttpResponse(
                statusCode: 200,
                body: '{',
                close: close,
              ),
              invoke: (api) async => api.fetchCapabilities('source-1'),
              outcome: throwsA(isA<SnapshotResponseDecodeException>()),
            ),
            (
              name: 'semantic failure',
              response: (close) => OversqliteHttpResponse(
                statusCode: 200,
                body:
                    '{"snapshot_id":"snapshot-1","snapshot_bundle_seq":0,'
                    '"rows":[],"byte_count":1,"next_row_ordinal":0,'
                    '"has_more":false}',
                close: close,
              ),
              invoke: (api) async => _fetchEmptyChunk(api),
              outcome: throwsA(isA<SnapshotSemanticException>()),
            ),
          ];

      for (final scenario in scenarios) {
        var closeCount = 0;
        final response = scenario.response(() async => closeCount++);
        final api = OversqliteRemoteApi(_StaticHttpClient(response));

        await expectLater(
          scenario.invoke(api),
          scenario.outcome,
          reason: scenario.name,
        );

        expect(closeCount, 1, reason: scenario.name);
      }
    },
  );

  test(
    'throwing close is secondary to processing and safe on success',
    () async {
      const closeSentinel = 'SECRET-close-failure';

      var successCloseCount = 0;
      Object? successError;
      try {
        await OversqliteRemoteApi(
          _StaticHttpClient(
            OversqliteHttpResponse(
              statusCode: 200,
              body: jsonEncode(
                phase4CapabilitiesResponse(
                  registeredTableSpecs: phase4RegisteredTableSpecs(['users']),
                ),
              ),
              close: () async {
                successCloseCount++;
                throw StateError(closeSentinel);
              },
            ),
          ),
        ).fetchCapabilities('source-1');
      } catch (error) {
        successError = error;
      }
      expect(successCloseCount, 1);
      expect(successError, isA<OversqliteProtocolException>());
      expect(successError.toString(), 'snapshot response cleanup failed');
      expect(successError.toString(), isNot(contains(closeSentinel)));

      var capacityCloseCount = 0;
      Object? capacityError;
      try {
        await OversqliteRemoteApi(
          _StaticHttpClient(
            OversqliteHttpResponse(
              statusCode: 429,
              body: jsonEncode({
                'error': 'snapshot_build_capacity',
                'message': 'busy',
              }),
              close: () async {
                capacityCloseCount++;
                throw StateError(closeSentinel);
              },
            ),
          ),
        ).createSnapshotSession(sourceId: 'source-1');
      } catch (error) {
        capacityError = error;
      }
      expect(capacityCloseCount, 1);
      expect(capacityError, isA<SnapshotCapacityException>());
      expect(
        (capacityError! as SnapshotCapacityException).errorCode,
        'snapshot_build_capacity',
      );
      expect(capacityError.toString(), isNot(contains(closeSentinel)));

      var semanticCloseCount = 0;
      Object? semanticError;
      try {
        await _fetchEmptyChunk(
          OversqliteRemoteApi(
            _StaticHttpClient(
              OversqliteHttpResponse(
                statusCode: 200,
                body:
                    '{"snapshot_id":"snapshot-1","snapshot_bundle_seq":0,'
                    '"rows":[],"byte_count":1,"next_row_ordinal":0,'
                    '"has_more":false}',
                close: () async {
                  semanticCloseCount++;
                  throw StateError(closeSentinel);
                },
              ),
            ),
          ),
        );
      } catch (error) {
        semanticError = error;
      }
      expect(semanticCloseCount, 1);
      expect(semanticError, isA<SnapshotSemanticException>());
      expect(
        (semanticError! as SnapshotSemanticException).failure,
        SnapshotSemanticFailure.invalidChunk,
      );
      expect(semanticError.toString(), isNot(contains(closeSentinel)));
    },
  );

  test('IO transport bounds identity, gzip, encoding, and UTF-8', () async {
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    server.listen((request) async {
      switch (request.uri.path) {
        case '/exact':
          request.response.write('x' * 32);
        case '/over':
          request.response.write('x' * 33);
        case '/gzip-exact':
          request.response.headers.set(
            HttpHeaders.contentEncodingHeader,
            'gzip',
          );
          request.response.add(gzip.encode(utf8.encode('x' * 32)));
        case '/gzip-over':
          request.response.headers.set(
            HttpHeaders.contentEncodingHeader,
            'gzip',
          );
          request.response.add(gzip.encode(utf8.encode('x' * 33)));
        case '/unsupported':
          request.response.headers.set(HttpHeaders.contentEncodingHeader, 'br');
          request.response.write('{}');
        case '/invalid-utf8':
          request.response.add(const [0xc3, 0x28]);
        default:
          request.response.statusCode = 404;
      }
      await request.response.close();
    });
    addTearDown(() => server.close(force: true));
    final client = IoOversqliteHttpClient(baseUri: _baseUri(server));
    addTearDown(() => client.close(force: true));
    const bounds = OversqliteHttpRequestBounds(
      successBodyBytes: 32,
      errorBodyBytes: 32,
    );

    final exact = await client.get(
      'exact',
      sourceId: 'source-1',
      operation: 'exact',
      bounds: bounds,
    );
    expect(exact.decodedBodyBytes, 32);
    await exact.close();
    final gzipExact = await client.get(
      'gzip-exact',
      sourceId: 'source-1',
      operation: 'gzip-exact',
      bounds: bounds,
    );
    expect(gzipExact.decodedBodyBytes, 32);
    expect(gzipExact.body, 'x' * 32);
    await gzipExact.close();
    await expectLater(
      client.get(
        'over',
        sourceId: 'source-1',
        operation: 'over',
        bounds: bounds,
      ),
      throwsA(isA<SnapshotResponseBodyTooLargeException>()),
    );
    await expectLater(
      client.get(
        'gzip-over',
        sourceId: 'source-1',
        operation: 'gzip-over',
        bounds: bounds,
      ),
      throwsA(isA<SnapshotResponseBodyTooLargeException>()),
    );
    await expectLater(
      client.get(
        'unsupported',
        sourceId: 'source-1',
        operation: 'unsupported',
        bounds: bounds,
      ),
      throwsA(isA<SnapshotUnsupportedContentEncodingException>()),
    );
    await expectLater(
      client.get(
        'invalid-utf8',
        sourceId: 'source-1',
        operation: 'invalid-utf8',
        bounds: bounds,
      ),
      throwsA(isA<SnapshotResponseDecodeException>()),
    );
  });

  test('unsupported encoding aborts its IO request exactly once', () async {
    final http = _AbortCountingHttpClient(
      _FakeHttpClientResponse(
        headers: _FakeHttpHeaders({
          HttpHeaders.contentEncodingHeader: ['br'],
        }),
        chunks: const [
          [0x7b, 0x7d],
        ],
      ),
    );
    final client = IoOversqliteHttpClient(
      baseUri: Uri.parse('http://transport.invalid/'),
      httpClient: http,
    );

    await expectLater(
      client.get(
        'unsupported',
        sourceId: 'source-1',
        operation: 'unsupported',
        bounds: const OversqliteHttpRequestBounds(
          successBodyBytes: 32,
          errorBodyBytes: 32,
        ),
      ),
      throwsA(isA<SnapshotUnsupportedContentEncodingException>()),
    );

    expect(http.request.abortCount, 1);
  });

  test(
    'crossing body chunk cancels before any remainder is retained',
    () async {
      var cancelled = false;
      final listened = Completer<void>();
      final chunks = StreamController<List<int>>(
        sync: true,
        onCancel: () => cancelled = true,
      );
      addTearDown(chunks.close);
      final http = _AbortCountingHttpClient(
        _FakeStreamingHttpClientResponse(
          headers: _FakeHttpHeaders({}),
          stream: chunks.stream,
          listened: listened,
        ),
      );
      final client = IoOversqliteHttpClient(
        baseUri: Uri.parse('http://transport.invalid/'),
        httpClient: http,
      );

      final response = client.get(
        'over',
        sourceId: 'source-1',
        operation: 'over',
        bounds: const OversqliteHttpRequestBounds(
          successBodyBytes: 32,
          errorBodyBytes: 32,
        ),
      );
      await listened.future;
      chunks.add(List<int>.filled(33, 0x78));

      await expectLater(
        response,
        throwsA(isA<SnapshotResponseBodyTooLargeException>()),
      );
      expect(cancelled, isTrue);
    },
  );

  test(
    'truncated snapshot transport failure is fixed, redacted, and reusable',
    () async {
      const sessionSentinel = 'snapshot-SECRET-path-token';
      var requestCount = 0;
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      server.listen((request) async {
        requestCount++;
        if (request.uri.path.contains(sessionSentinel)) {
          request.response.contentLength = 128;
          request.response.add(utf8.encode('{"snapshot_id":'));
          try {
            await request.response.close();
          } catch (_) {}
          return;
        }
        request.response.write(
          jsonEncode(
            phase4CapabilitiesResponse(
              registeredTableSpecs: phase4RegisteredTableSpecs(['users']),
            ),
          ),
        );
        await request.response.close();
      });
      addTearDown(() => server.close(force: true));
      final client = IoOversqliteHttpClient(baseUri: _baseUri(server));
      addTearDown(() => client.close(force: true));
      final api = OversqliteRemoteApi(client);

      Object? caught;
      try {
        await api.fetchSnapshotChunk(
          snapshotId: sessionSentinel,
          sourceId: 'source-1',
          snapshotBundleSeq: 0,
          afterRowOrdinal: 0,
          maxRows: 1,
          maxBytes: 32,
        );
      } catch (error) {
        caught = error;
      }

      expect(caught, isA<OversqliteProtocolException>());
      expect(caught.toString(), 'snapshot transport failed');
      expect(caught.toString(), isNot(contains(sessionSentinel)));
      expect(caught.toString(), isNot(contains('http://')));
      expect((await api.fetchCapabilities('source-1')).protocolVersion, 'v1');
      expect(requestCount, 2);
    },
  );

  test(
    'malformed gzip and truncated reads exclude diagnostics and remain reusable',
    () async {
      var requestCount = 0;
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      server.listen((request) async {
        requestCount++;
        if (requestCount == 1) {
          request.response.headers.set(
            HttpHeaders.contentEncodingHeader,
            'gzip',
          );
          request.response.add(const [0x1f, 0x8b, 0x00, 0x00]);
          await request.response.close();
          return;
        }
        if (requestCount == 2) {
          request.response.contentLength = 128;
          request.response.add(utf8.encode('{"protocol_version":'));
          try {
            await request.response.close();
          } catch (_) {}
          return;
        }
        request.response.write(
          jsonEncode(
            phase4CapabilitiesResponse(
              registeredTableSpecs: phase4RegisteredTableSpecs(['users']),
            ),
          ),
        );
        await request.response.close();
      });
      addTearDown(() => server.close(force: true));
      final client = IoOversqliteHttpClient(baseUri: _baseUri(server));
      addTearDown(() => client.close(force: true));
      final diagnostics = SnapshotDiagnosticsRecorder();
      final api = OversqliteRemoteApi(client, snapshotDiagnostics: diagnostics);

      await expectLater(
        api.fetchCapabilities('source-1'),
        throwsA(isA<SnapshotResponseDecodeException>()),
      );
      expect(diagnostics.snapshot().maxCompletelyDecodedBodyBytes, 0);
      await expectLater(
        api.fetchCapabilities('source-1'),
        throwsA(
          isA<OversqliteProtocolException>().having(
            (error) => error.message,
            'message',
            'snapshot transport failed',
          ),
        ),
      );
      expect(diagnostics.snapshot().maxCompletelyDecodedBodyBytes, 0);

      final capabilities = await api.fetchCapabilities('source-1');

      expect(capabilities.protocolVersion, 'v1');
      expect(requestCount, 3);
      expect(
        diagnostics.snapshot().maxCompletelyDecodedBodyBytes,
        greaterThan(0),
      );
    },
  );

  test(
    'real loopback socket closes oversize early and client remains reusable',
    () async {
      const fullBodyBytes = 512 * 1024;
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      server.listen((request) async {
        if (request.uri.path == '/small') {
          request.response.write('ok');
          await request.response.close();
          return;
        }
        var sent = 0;
        try {
          while (sent < fullBodyBytes) {
            request.response.add(List<int>.filled(1024, 0x78));
            sent += 1024;
            await request.response.flush();
            await Future<void>.delayed(const Duration(milliseconds: 10));
          }
          await request.response.close();
        } catch (_) {}
      });
      addTearDown(() => server.close(force: true));
      final client = IoOversqliteHttpClient(baseUri: _baseUri(server));
      addTearDown(() => client.close(force: true));

      final elapsed = Stopwatch()..start();
      await expectLater(
        client.get(
          'large',
          sourceId: 'source-1',
          operation: 'large',
          bounds: const OversqliteHttpRequestBounds(
            successBodyBytes: 32,
            errorBodyBytes: 32,
          ),
        ),
        throwsA(isA<SnapshotResponseBodyTooLargeException>()),
      );
      elapsed.stop();
      final small = await client.get(
        'small',
        sourceId: 'source-1',
        operation: 'small',
        bounds: const OversqliteHttpRequestBounds(
          successBodyBytes: 2,
          errorBodyBytes: 32,
        ),
      );

      expect(small.body, 'ok');
      expect(elapsed.elapsed, lessThan(const Duration(seconds: 1)));
    },
  );

  test('fully decoded loopback responses reuse the socket', () async {
    final remotePorts = <int>[];
    var count = 0;
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    server.listen((request) async {
      remotePorts.add(request.connectionInfo!.remotePort);
      request.response
        ..statusCode = count++ == 0 ? 429 : 200
        ..write('{}');
      await request.response.close();
    });
    addTearDown(() => server.close(force: true));
    final client = IoOversqliteHttpClient(baseUri: _baseUri(server));
    addTearDown(() => client.close(force: true));
    const bounds = OversqliteHttpRequestBounds(
      successBodyBytes: 2,
      errorBodyBytes: 2,
    );

    final first = await client.get(
      'first',
      sourceId: 'source-1',
      operation: 'first',
      bounds: bounds,
    );
    final second = await client.get(
      'second',
      sourceId: 'source-1',
      operation: 'second',
      bounds: bounds,
    );

    expect(first.statusCode, 429);
    expect(second.statusCode, 200);
    expect(remotePorts, hasLength(2));
    expect(remotePorts[1], remotePorts[0]);
  });
}

Future<SnapshotChunkResponse> _fetchEmptyChunk(OversqliteRemoteApi api) {
  return api.fetchSnapshotChunk(
    snapshotId: 'snapshot-1',
    sourceId: 'source-1',
    snapshotBundleSeq: 0,
    afterRowOrdinal: 0,
    maxRows: 1,
    maxBytes: 1,
  );
}

Uri _baseUri(HttpServer server) {
  return Uri.parse('http://${server.address.host}:${server.port}/');
}

Object _jsonNumber(String token) {
  return (jsonDecode('{"value":$token}')! as Map)['value']!;
}

final class _StaticHttpClient implements OversqliteHttpClient {
  const _StaticHttpClient(this.response);

  final OversqliteHttpResponse response;

  @override
  Future<OversqliteHttpResponse> get(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async => response;

  @override
  Future<OversqliteHttpResponse> postJson(
    String path, {
    required String sourceId,
    required Object? body,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async => response;

  @override
  Future<OversqliteHttpResponse> delete(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async => response;
}

final class _QueueHttpClient implements OversqliteHttpClient {
  _QueueHttpClient(this.responses);

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

final class _BoundRecordingClient implements OversqliteHttpClient {
  final bounds = <String, OversqliteHttpRequestBounds>{};

  @override
  Future<OversqliteHttpResponse> get(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async {
    this.bounds[operation] = bounds;
    if (path == 'sync/capabilities') {
      return OversqliteHttpResponse(
        statusCode: 200,
        body: jsonEncode(
          phase4CapabilitiesResponse(
            registeredTableSpecs: phase4RegisteredTableSpecs(['users']),
          ),
        ),
      );
    }
    return const OversqliteHttpResponse(
      statusCode: 200,
      body:
          '{"snapshot_id":"snapshot-1","snapshot_bundle_seq":0,'
          '"rows":[],"byte_count":0,"next_row_ordinal":0,"has_more":false}',
    );
  }

  @override
  Future<OversqliteHttpResponse> postJson(
    String path, {
    required String sourceId,
    required Object? body,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async {
    this.bounds[operation] = bounds;
    return const OversqliteHttpResponse(
      statusCode: 200,
      body:
          '{"snapshot_id":"snapshot-1","snapshot_bundle_seq":0,'
          '"row_count":0,"byte_count":0,"expires_at":"2099-01-01T00:00:00Z"}',
    );
  }

  @override
  Future<OversqliteHttpResponse> delete(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async {
    this.bounds[operation] = bounds;
    return const OversqliteHttpResponse(statusCode: 204, body: '');
  }
}

final class _InvalidSessionClient implements OversqliteHttpClient {
  _InvalidSessionClient({
    this.snapshotId = 'retire-me',
    this.hangRetirement = false,
  });

  final String snapshotId;
  final bool hangRetirement;
  final retiredSnapshotIds = <String>[];
  final deleteBounds = <OversqliteHttpRequestBounds>[];
  var createdResponseCloseCount = 0;
  var retirementResponseCloseCount = 0;

  @override
  Future<OversqliteHttpResponse> get(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async => fail('unexpected GET $path');

  @override
  Future<OversqliteHttpResponse> postJson(
    String path, {
    required String sourceId,
    required Object? body,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async {
    return OversqliteHttpResponse(
      statusCode: 200,
      body: jsonEncode({
        'snapshot_id': snapshotId,
        'snapshot_bundle_seq': 1,
        'row_count': 1,
        'byte_count': 1,
      }),
      close: () async => createdResponseCloseCount++,
    );
  }

  @override
  Future<OversqliteHttpResponse> delete(
    String path, {
    required String sourceId,
    required String operation,
    required OversqliteHttpRequestBounds bounds,
  }) async {
    retiredSnapshotIds.add(path.split('/').last);
    deleteBounds.add(bounds);
    return OversqliteHttpResponse(
      statusCode: 204,
      body: '',
      close: () {
        retirementResponseCloseCount++;
        if (hangRetirement) return Completer<void>().future;
        return Future.value();
      },
    );
  }
}

final class _AbortCountingHttpClient implements HttpClient {
  _AbortCountingHttpClient(HttpClientResponse response)
    : request = _AbortCountingHttpClientRequest(response);

  final _AbortCountingHttpClientRequest request;
  bool _autoUncompress = true;

  @override
  bool get autoUncompress => _autoUncompress;

  @override
  set autoUncompress(bool value) => _autoUncompress = value;

  @override
  Future<HttpClientRequest> openUrl(String method, Uri url) async => request;

  @override
  void close({bool force = false}) {}

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

final class _AbortCountingHttpClientRequest implements HttpClientRequest {
  _AbortCountingHttpClientRequest(this.response);

  final HttpClientResponse response;
  final HttpHeaders _headers = _FakeHttpHeaders({});
  var abortCount = 0;

  @override
  HttpHeaders get headers => _headers;

  @override
  Future<HttpClientResponse> close() async => response;

  @override
  void abort([Object? exception, StackTrace? stackTrace]) {
    abortCount++;
  }

  @override
  void write(Object? object) {}

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

final class _FakeHttpClientResponse extends Stream<List<int>>
    implements HttpClientResponse {
  _FakeHttpClientResponse({required this.headers, required this.chunks});

  @override
  final HttpHeaders headers;
  final List<List<int>> chunks;

  @override
  int get statusCode => HttpStatus.ok;

  @override
  StreamSubscription<List<int>> listen(
    void Function(List<int> event)? onData, {
    Function? onError,
    void Function()? onDone,
    bool? cancelOnError,
  }) {
    return Stream<List<int>>.fromIterable(chunks).listen(
      onData,
      onError: onError,
      onDone: onDone,
      cancelOnError: cancelOnError,
    );
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

final class _FakeStreamingHttpClientResponse extends Stream<List<int>>
    implements HttpClientResponse {
  _FakeStreamingHttpClientResponse({
    required this.headers,
    required this.stream,
    required this.listened,
  });

  @override
  final HttpHeaders headers;
  final Stream<List<int>> stream;
  final Completer<void> listened;

  @override
  int get statusCode => HttpStatus.ok;

  @override
  StreamSubscription<List<int>> listen(
    void Function(List<int> event)? onData, {
    Function? onError,
    void Function()? onDone,
    bool? cancelOnError,
  }) {
    if (!listened.isCompleted) listened.complete();
    return stream.listen(
      onData,
      onError: onError,
      onDone: onDone,
      cancelOnError: cancelOnError,
    );
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

final class _FakeHttpHeaders implements HttpHeaders {
  _FakeHttpHeaders(Map<String, List<String>> values)
    : _values = {
        for (final entry in values.entries)
          entry.key.toLowerCase(): List<String>.of(entry.value),
      };

  final Map<String, List<String>> _values;

  @override
  ContentType? get contentType => null;

  @override
  set contentType(ContentType? value) {
    if (value == null) {
      _values.remove(HttpHeaders.contentTypeHeader);
    } else {
      set(HttpHeaders.contentTypeHeader, value.toString());
    }
  }

  @override
  void forEach(void Function(String name, List<String> values) action) {
    _values.forEach(action);
  }

  @override
  void set(String name, Object value, {bool preserveHeaderCase = false}) {
    _values[name.toLowerCase()] = [value.toString()];
  }

  @override
  String? value(String name) => _values[name.toLowerCase()]?.join(',');

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

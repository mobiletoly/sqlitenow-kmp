import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

void main() {
  test(
    'Dart pull and snapshot fixtures decode against protocol models',
    () async {
      final fixtures = _readPullSnapshotFixtures();
      for (final fixture in fixtures) {
        final name = fixture['name']! as String;
        final afterBundleSeq = (fixture['afterBundleSeq'] as int?) ?? 0;

        final pull = fixture['pullResponse'];
        if (pull is Map) {
          await _expectFixtureResult(
            name,
            fixture['expectedPullErrorContains'] as String?,
            () {
              final response = PullResponse.fromJson(
                pull.cast<String, Object?>(),
              );
              validatePullResponse(response, afterBundleSeq);
              final expectedFinalState = fixture['expectedFinalState'];
              if (expectedFinalState is Map &&
                  expectedFinalState.containsKey('lastBundleSeqSeen')) {
                expect(
                  response.stableBundleSeq,
                  expectedFinalState['lastBundleSeqSeen'],
                  reason: name,
                );
              }
            },
          );
        }

        final historyPruned = fixture['historyPrunedResponse'];
        if (historyPruned is Map) {
          final http = _FixtureHttpClient(
            getResponses: {
              'sync/pull?after_bundle_seq=$afterBundleSeq&max_bundles=10':
                  _fixtureResponse(historyPruned),
            },
          );
          await expectLater(
            OversqliteRemoteApi(http).sendPullRequest(
              sourceId: 'source-1',
              afterBundleSeq: afterBundleSeq,
              maxBundles: 10,
              targetBundleSeq: 0,
            ),
            throwsA(isA<HistoryPrunedException>()),
            reason: name,
          );
        }

        final checkpointAhead = fixture['checkpointAheadResponse'];
        if (checkpointAhead is Map) {
          final http = _FixtureHttpClient(
            getResponses: {
              'sync/pull?after_bundle_seq=$afterBundleSeq&max_bundles=10':
                  _fixtureResponse(checkpointAhead),
            },
          );
          await expectLater(
            OversqliteRemoteApi(http).sendPullRequest(
              sourceId: 'source-1',
              afterBundleSeq: afterBundleSeq,
              maxBundles: 10,
              targetBundleSeq: 0,
            ),
            throwsA(isA<CheckpointAheadException>()),
            reason: name,
          );
        }

        final sessionJson = fixture['snapshotSession'];
        if (sessionJson is Map) {
          final createRequestJson = fixture['snapshotSessionCreateRequest'];
          final http = _FixtureHttpClient(
            postResponses: {
              'sync/snapshot-sessions': OversqliteHttpResponse(
                statusCode: 200,
                body: jsonEncode(sessionJson),
              ),
            },
          );
          final request = createRequestJson is Map
              ? _snapshotCreateRequestFromJson(
                  createRequestJson.cast<String, Object?>(),
                )
              : null;
          SnapshotSession? session;
          await _expectFixtureResult(
            name,
            fixture['expectedSnapshotSessionErrorContains'] as String?,
            () async {
              session = await OversqliteRemoteApi(
                http,
              ).createSnapshotSession(sourceId: 'source-1', request: request);
              expect(
                session!.snapshotId,
                sessionJson['snapshot_id'],
                reason: name,
              );
              if (createRequestJson is Map) {
                expect(
                  http.postedBodies.single,
                  createRequestJson,
                  reason: name,
                );
              }
            },
          );

          final chunkJson = fixture['snapshotChunkResponse'];
          if (chunkJson is Map && session != null) {
            final afterRowOrdinal =
                (fixture['snapshotChunkAfterRowOrdinal'] as int?) ?? 0;
            final chunkHttp = _FixtureHttpClient(
              getResponses: {
                'sync/snapshot-sessions/${session!.snapshotId}?after_row_ordinal=$afterRowOrdinal&max_rows=1000':
                    OversqliteHttpResponse(
                      statusCode: 200,
                      body: jsonEncode(chunkJson),
                    ),
              },
            );
            await _expectFixtureResult(
              name,
              fixture['expectedSnapshotChunkErrorContains'] as String?,
              () async {
                final chunk = await OversqliteRemoteApi(chunkHttp)
                    .fetchSnapshotChunk(
                      snapshotId: session!.snapshotId,
                      sourceId: 'source-1',
                      snapshotBundleSeq: session!.snapshotBundleSeq,
                      afterRowOrdinal: afterRowOrdinal,
                      maxRows: 1000,
                    );
                expect(
                  chunk.nextRowOrdinal,
                  chunkJson['next_row_ordinal'],
                  reason: name,
                );
              },
            );
          }
        }

        final sourceReplacementInvalid =
            fixture['sourceReplacementInvalidResponse'];
        if (sourceReplacementInvalid is Map) {
          final http = _FixtureHttpClient(
            postResponses: {
              'sync/snapshot-sessions': _fixtureResponse(
                sourceReplacementInvalid,
              ),
            },
          );
          await expectLater(
            OversqliteRemoteApi(
              http,
            ).createSnapshotSession(sourceId: 'source-1'),
            throwsA(isA<SourceReplacementInvalidException>()),
            reason: name,
          );
        }
      }
    },
  );
}

Future<void> _expectFixtureResult(
  String name,
  String? expectedMessage,
  FutureOr<void> Function() block,
) async {
  Object? error;
  try {
    await Future<void>.sync(block);
  } catch (caught) {
    error = caught;
  }
  if (expectedMessage == null) {
    expect(error, isNull, reason: '$name expected validation success');
    return;
  }
  expect(error, isNotNull, reason: '$name expected validation error');
  expect(
    error.toString(),
    contains(expectedMessage),
    reason: '$name expected validation error',
  );
}

SnapshotSessionCreateRequest _snapshotCreateRequestFromJson(
  Map<String, Object?> json,
) {
  final sourceReplacement = json['source_replacement'];
  if (sourceReplacement is! Map) {
    return const SnapshotSessionCreateRequest();
  }
  final replacement = sourceReplacement.cast<String, Object?>();
  return SnapshotSessionCreateRequest(
    sourceReplacement: SnapshotSourceReplacement(
      previousSourceId: replacement['previous_source_id']! as String,
      newSourceId: replacement['new_source_id']! as String,
      reason: replacement['reason']! as String,
    ),
  );
}

OversqliteHttpResponse _fixtureResponse(Map<Object?, Object?> response) {
  return OversqliteHttpResponse(
    statusCode: response['status']! as int,
    body: jsonEncode(response['body']),
  );
}

List<Map<String, Object?>> _readPullSnapshotFixtures() {
  final file = _repoRoot().uri
      .resolve('oversqlite-contracts/pull-snapshot/basic.json')
      .toFilePath();
  final raw = jsonDecode(File(file).readAsStringSync()) as Map<String, Object?>;
  return (raw['cases']! as List<Object?>).cast<Map<String, Object?>>();
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

final class _FixtureHttpClient implements OversqliteHttpClient {
  _FixtureHttpClient({
    this.getResponses = const {},
    this.postResponses = const {},
  });

  final Map<String, OversqliteHttpResponse> getResponses;
  final Map<String, OversqliteHttpResponse> postResponses;
  final postedBodies = <Object?>[];

  @override
  Future<OversqliteHttpResponse> get(
    String path, {
    required String sourceId,
  }) async {
    final response = getResponses[path];
    if (response == null) {
      throw StateError('No fixture GET response for $path');
    }
    return response;
  }

  @override
  Future<OversqliteHttpResponse> postJson(
    String path, {
    required String sourceId,
    required Object? body,
  }) async {
    postedBodies.add(body);
    final response = postResponses[path];
    if (response == null) {
      throw StateError('No fixture POST response for $path');
    }
    return response;
  }

  @override
  Future<OversqliteHttpResponse> delete(
    String path, {
    required String sourceId,
  }) async {
    return const OversqliteHttpResponse(statusCode: 204, body: '');
  }
}

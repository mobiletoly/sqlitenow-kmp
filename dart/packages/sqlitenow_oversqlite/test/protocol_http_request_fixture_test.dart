import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

import 'support/behavior_fixture_support.dart';

void main() {
  final spec = _readFixture();
  final cases = (spec['cases']! as List<Object?>).cast<Map<String, Object?>>();

  test('Dart shared protocol HTTP request fixtures match runtime', () async {
    for (final fixture in cases) {
      await _runCase(fixture);
    }
  });

  test('push request DELETE rows omit payload field', () {
    final deleteRow = const PushRequestRow(
      schema: 'main',
      table: 'users',
      key: {'id': 'user-1'},
      op: 'DELETE',
      baseRowVersion: 1,
    ).toJson();

    expect(deleteRow.containsKey('payload'), isFalse);
  });
}

Future<void> _runCase(Map<String, Object?> fixture) async {
  final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
  final captured = Completer<_CapturedRequest>();
  final baseUri = Uri.parse(
    'http://${server.address.host}:${server.port}/api/v1/',
  );
  final client = IoOversqliteHttpClient(baseUri: baseUri);
  unawaited(
    server.forEach((request) async {
      final body = await utf8.decoder.bind(request).join();
      if (!captured.isCompleted) {
        captured.complete(
          _CapturedRequest(
            method: request.method,
            path: request.uri.path,
            query: request.uri.queryParameters,
            sourceHeader: request.headers.value(oversyncSourceIdHeaderName),
            body: body.trim().isEmpty ? null : jsonDecode(body),
          ),
        );
      }
      await _respond(request, fixture);
    }),
  );
  try {
    await _executeOperation(client, fixture);
    _assertCaptured(fixture, await captured.future);
  } finally {
    client.close(force: true);
    await server.close(force: true);
  }
}

Future<void> _executeOperation(
  IoOversqliteHttpClient client,
  Map<String, Object?> fixture,
) async {
  final api = OversqliteRemoteApi(client);
  final sourceId = fixture['sourceId']! as String;
  final args = (fixture['args'] as Map?)?.cast<String, Object?>() ?? const {};
  switch (fixture['operation']) {
    case 'capabilities':
      await api.fetchCapabilities(sourceId);
    case 'connect':
      await api.connect(
        sourceId: sourceId,
        hasLocalPendingRows: args['hasLocalPendingRows']! as bool,
      );
    case 'pull':
      await api.sendPullRequest(
        sourceId: sourceId,
        afterBundleSeq: args['afterBundleSeq']! as int,
        maxBundles: args['maxBundles']! as int,
        targetBundleSeq: args['targetBundleSeq']! as int,
      );
    case 'pushSessionCreate':
      await api.createPushSession(
        sourceId: sourceId,
        sourceBundleId: args['sourceBundleId']! as int,
        plannedRowCount: args['plannedRowCount']! as int,
        canonicalRequestHash: 'a' * 64,
        initializationId: args['initializationId'] as String?,
      );
    case 'pushSessionChunk':
      await api.uploadPushChunk(
        pushId: args['pushId']! as String,
        sourceId: sourceId,
        startRowOrdinal: args['startRowOrdinal']! as int,
        rows: [_fixturePushRow()],
      );
    case 'pushSessionCommit':
      await api.commitPushSession(
        pushId: args['pushId']! as String,
        sourceId: sourceId,
      );
    case 'committedRows':
      await api.fetchCommittedBundleChunk(
        bundleSeq: args['bundleSeq']! as int,
        sourceId: sourceId,
        afterRowOrdinal: args['afterRowOrdinal']! as int,
        maxRows: args['maxRows']! as int,
      );
    case 'snapshotSession':
      await api.createSnapshotSession(sourceId: sourceId);
    case 'snapshotChunk':
      await api.fetchSnapshotChunk(
        snapshotId: args['snapshotId']! as String,
        sourceId: sourceId,
        snapshotBundleSeq: args['snapshotBundleSeq']! as int,
        afterRowOrdinal: args['afterRowOrdinal']! as int,
        maxRows: args['maxRows']! as int,
      );
    case 'sourceReplacement':
      await api.createSnapshotSession(
        sourceId: sourceId,
        request: SnapshotSessionCreateRequest(
          sourceReplacement: SnapshotSourceReplacement(
            previousSourceId: args['previousSourceId']! as String,
            newSourceId: args['newSourceId']! as String,
            reason: args['reason']! as String,
          ),
        ),
      );
    case 'watch':
      final response = await client.watchBundleChanges(
        sourceId: sourceId,
        afterBundleSeq: args['afterBundleSeq']! as int,
      );
      await response.lines.drain<void>();
      await response.close();
    default:
      fail('${fixture['name']}: unknown operation ${fixture['operation']}');
  }
}

Future<void> _respond(HttpRequest request, Map<String, Object?> fixture) async {
  if (fixture['operation'] == 'watch') {
    request.response.statusCode = HttpStatus.ok;
    await request.response.close();
    return;
  }
  final body = _responseBody(fixture);
  request.response
    ..statusCode = HttpStatus.ok
    ..headers.contentType = ContentType.json
    ..write(jsonEncode(body));
  await request.response.close();
}

Map<String, Object?> _responseBody(Map<String, Object?> fixture) {
  final sourceId = fixture['sourceId']! as String;
  final args = (fixture['args'] as Map?)?.cast<String, Object?>() ?? const {};
  switch (fixture['operation']) {
    case 'capabilities':
      return {
        'protocol_version': 'v0',
        'schema_version': 1,
        'features': {'connect_lifecycle': true, 'bundle_change_watch': true},
      };
    case 'connect':
      return {'resolution': 'initialize_empty'};
    case 'pull':
      return {
        'stable_bundle_seq': args['targetBundleSeq'] ?? 1,
        'bundles': <Object?>[],
        'has_more': false,
      };
    case 'pushSessionCreate':
      return {
        'status': 'staging',
        'push_id': 'push-fixture',
        'planned_row_count': args['plannedRowCount'],
        'next_expected_row_ordinal': 0,
        'canonical_request_hash': 'a' * 64,
      };
    case 'pushSessionChunk':
      return {'push_id': args['pushId'], 'next_expected_row_ordinal': 1};
    case 'pushSessionCommit':
      return {
        'bundle_seq': 1,
        'source_id': sourceId,
        'source_bundle_id': 1,
        'row_count': 1,
        'bundle_hash': 'fixture-hash',
        'canonical_request_hash': 'a' * 64,
      };
    case 'committedRows':
      return {
        'bundle_seq': args['bundleSeq'],
        'source_id': sourceId,
        'source_bundle_id': 1,
        'row_count': 0,
        'bundle_hash': 'fixture-hash',
        'canonical_request_hash': 'a' * 64,
        'rows': <Object?>[],
        'next_row_ordinal': args['afterRowOrdinal'],
        'has_more': false,
      };
    case 'snapshotSession':
    case 'sourceReplacement':
      return {
        'snapshot_id': 'snapshot-fixture',
        'snapshot_bundle_seq': 6,
        'row_count': 0,
        'byte_count': 0,
        'expires_at': '2099-01-01T00:00:00Z',
      };
    case 'snapshotChunk':
      return {
        'snapshot_id': args['snapshotId'],
        'snapshot_bundle_seq': args['snapshotBundleSeq'],
        'rows': <Object?>[],
        'next_row_ordinal': args['afterRowOrdinal'],
        'has_more': false,
      };
    default:
      throw StateError('unknown operation ${fixture['operation']}');
  }
}

void _assertCaptured(Map<String, Object?> fixture, _CapturedRequest captured) {
  final expected = (fixture['expected']! as Map).cast<String, Object?>();
  expect(
    captured.method,
    expected['method'],
    reason: fixture['name']! as String,
  );
  expect(captured.path, expected['path'], reason: fixture['name']! as String);
  expect(captured.query, expected['query'], reason: fixture['name']! as String);
  expect(
    captured.sourceHeader,
    expected['sourceHeader'],
    reason: fixture['name']! as String,
  );
  expect(captured.body, expected['body'], reason: fixture['name']! as String);
  for (final key
      in (expected['bodyMustNotContain'] as List<Object?>? ?? const [])
          .cast<String>()) {
    expect(
      _containsKeyRecursive(captured.body, key),
      isFalse,
      reason: '${fixture['name']}: body must not contain $key',
    );
  }
}

PushRequestRow _fixturePushRow() {
  return const PushRequestRow(
    schema: 'main',
    table: 'users',
    key: {'id': 'user-1'},
    op: 'INSERT',
    baseRowVersion: 0,
    payload: {'id': 'user-1', 'name': 'Ada'},
  );
}

bool _containsKeyRecursive(Object? value, String key) {
  if (value is Map) {
    return value.containsKey(key) ||
        value.values.any((child) => _containsKeyRecursive(child, key));
  }
  if (value is List) {
    return value.any((child) => _containsKeyRecursive(child, key));
  }
  return false;
}

Map<String, Object?> _readFixture() {
  final file = File.fromUri(
    repoRoot().uri.resolve('oversqlite-contracts/protocol-http/requests.json'),
  );
  return jsonDecode(file.readAsStringSync()) as Map<String, Object?>;
}

final class _CapturedRequest {
  const _CapturedRequest({
    required this.method,
    required this.path,
    required this.query,
    required this.sourceHeader,
    required this.body,
  });

  final String method;
  final String path;
  final Map<String, String> query;
  final String? sourceHeader;
  final Object? body;
}

import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

const _githubActionsGuardMessage =
    'Phase 4C snapshot memory workloads are local-only and must not run in GitHub Actions.';

Future<void> main(List<String> arguments) async {
  if (Platform.environment['GITHUB_ACTIONS'] == 'true') {
    stderr.writeln(_githubActionsGuardMessage);
    exitCode = 64;
    return;
  }

  final options = _Options.parse(arguments);
  final rows = _SnapshotRows(options.targetRowBytes);
  final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
  final sampler = _RssSampler.start();
  final retired = Completer<void>();
  var chunkCount = 0;
  var maxChunkRows = 0;
  var maxChunkBytes = 0;

  stdout.writeln(
    jsonEncode({
      'event': 'snapshot_memory_server_ready',
      'pid': pid,
      'port': server.port,
      'rows': options.rowCount,
      'target_row_bytes': options.targetRowBytes,
      'declared_total_bytes': options.rowCount * options.targetRowBytes,
      'baseline_rss_bytes': sampler.baselineRssBytes,
    }),
  );

  final requestLoop = () async {
    await for (final request in server) {
      try {
        final path = request.uri.path;
        if (request.method == 'GET' && path == '/sync/capabilities') {
          await _writeJson(request.response, _capabilities());
          continue;
        }
        if (request.method == 'POST' && path == '/sync/snapshot-sessions') {
          await request.drain<void>();
          await _writeJson(request.response, {
            'snapshot_id': 'snapshot-phase4c-memory',
            'snapshot_bundle_seq': 1,
            'row_count': options.rowCount,
            'byte_count': options.rowCount * options.targetRowBytes,
            'expires_at': '2099-01-01T00:00:00Z',
          });
          continue;
        }
        if (request.method == 'GET' &&
            path == '/sync/snapshot-sessions/snapshot-phase4c-memory') {
          final after = _requiredNonNegativeInt(
            request.uri.queryParameters,
            'after_row_ordinal',
          );
          final maxRows = _requiredPositiveInt(
            request.uri.queryParameters,
            'max_rows',
          );
          final maxBytes = _requiredPositiveInt(
            request.uri.queryParameters,
            'max_bytes',
          );
          if (after > options.rowCount) {
            throw const FormatException('after_row_ordinal exceeds row count');
          }
          final admittedByBytes = maxBytes ~/ options.targetRowBytes;
          if (admittedByBytes == 0 && after < options.rowCount) {
            request.response.statusCode = HttpStatus.unprocessableEntity;
            await _writeJson(request.response, {
              'error': 'snapshot_chunk_too_small',
              'message': 'configured max_bytes cannot admit one row',
              'required_byte_count': options.targetRowBytes,
            });
            continue;
          }
          final remaining = options.rowCount - after;
          final count = min(remaining, min(maxRows, admittedByBytes));
          final next = after + count;
          final chunkBytes = count * options.targetRowBytes;
          chunkCount++;
          maxChunkRows = max(maxChunkRows, count);
          maxChunkBytes = max(maxChunkBytes, chunkBytes);
          request.response.headers.contentType = ContentType.json;
          request.response.write(
            rows.chunkJson(
              firstOrdinal: after + 1,
              rowCount: count,
              nextOrdinal: next,
              chunkBytes: chunkBytes,
              hasMore: next < options.rowCount,
            ),
          );
          await request.response.close();
          continue;
        }
        if (request.method == 'DELETE' &&
            path == '/sync/snapshot-sessions/snapshot-phase4c-memory') {
          request.response.statusCode = HttpStatus.noContent;
          await request.response.close();
          if (!retired.isCompleted) retired.complete();
          continue;
        }
        request.response.statusCode = HttpStatus.notFound;
        await _writeJson(request.response, {
          'error': 'not_found',
          'message': 'fixture route not found',
        });
      } catch (_) {
        try {
          request.response.statusCode = HttpStatus.badRequest;
          await _writeJson(request.response, {
            'error': 'invalid_request',
            'message': 'fixture request is invalid',
          });
        } catch (_) {
          await request.response.close();
        }
      }
    }
  }();

  try {
    await retired.future.timeout(const Duration(hours: 2));
  } finally {
    await server.close(force: true);
    await requestLoop;
  }
  final metrics = sampler.stop();
  stdout.writeln(
    jsonEncode({
      'event': 'snapshot_memory_server_result',
      'pid': pid,
      'rows': options.rowCount,
      'target_row_bytes': options.targetRowBytes,
      'chunk_count': chunkCount,
      'max_chunk_rows': maxChunkRows,
      'max_chunk_bytes': maxChunkBytes,
      'baseline_rss_bytes': metrics.baselineRssBytes,
      'peak_rss_bytes': metrics.peakRssBytes,
      'adjusted_rss_bytes': metrics.peakRssBytes - metrics.baselineRssBytes,
      'sample_count': metrics.sampleCount,
    }),
  );
}

Map<String, Object?> _capabilities() => {
  'protocol_version': 'v1',
  'schema_version': 1,
  'features': {'connect_lifecycle': true},
  'bundle_limits': {
    'max_rows_per_bundle': 1000,
    'max_bytes_per_bundle': 4194304,
    'max_bundles_per_pull': 100,
    'default_rows_per_push_chunk': 1000,
    'max_rows_per_push_chunk': 1000,
    'push_session_ttl_seconds': 300,
    'default_rows_per_committed_bundle_chunk': 1000,
    'max_rows_per_committed_bundle_chunk': 1000,
    'default_rows_per_snapshot_chunk': 1000,
    'max_rows_per_snapshot_chunk': 1000,
    'snapshot_session_ttl_seconds': 300,
    'max_rows_per_snapshot_session': 1000000,
    'max_bytes_per_snapshot_session': 4294967296,
    'default_bytes_per_snapshot_chunk': 4194304,
    'max_bytes_per_snapshot_chunk': 4194304,
    'max_bytes_per_snapshot_row': 1024,
    'snapshot_materialization_batch_rows': 1000,
    'snapshot_materialization_batch_bytes': 4194304,
    'max_concurrent_snapshot_builds': 1,
    'max_concurrent_snapshot_chunk_requests': 1,
    'initialization_lease_ttl_seconds': 300,
  },
};

Future<void> _writeJson(
  HttpResponse response,
  Map<String, Object?> body,
) async {
  response.headers.contentType = ContentType.json;
  response.write(jsonEncode(body));
  await response.close();
}

int _requiredNonNegativeInt(Map<String, String> values, String name) {
  final value = int.tryParse(values[name] ?? '');
  if (value == null || value < 0) throw FormatException(name);
  return value;
}

int _requiredPositiveInt(Map<String, String> values, String name) {
  final value = int.tryParse(values[name] ?? '');
  if (value == null || value <= 0) throw FormatException(name);
  return value;
}

final class _Options {
  const _Options({required this.rowCount, required this.targetRowBytes});

  final int rowCount;
  final int targetRowBytes;

  static _Options parse(List<String> arguments) {
    final values = <String, String>{};
    for (var index = 0; index < arguments.length; index += 2) {
      if (index + 1 >= arguments.length || !arguments[index].startsWith('--')) {
        throw ArgumentError('expected --name value arguments');
      }
      values[arguments[index].substring(2)] = arguments[index + 1];
    }
    final rowCount = int.tryParse(values['rows'] ?? '');
    final targetRowBytes = int.tryParse(values['target-row-bytes'] ?? '');
    if (rowCount == null || rowCount <= 0 || rowCount > 1000000) {
      throw ArgumentError.value(values['rows'], 'rows');
    }
    if (targetRowBytes == null ||
        targetRowBytes < 256 ||
        targetRowBytes > 1024) {
      throw ArgumentError.value(values['target-row-bytes'], 'target-row-bytes');
    }
    return _Options(rowCount: rowCount, targetRowBytes: targetRowBytes);
  }
}

final class _SnapshotRows {
  _SnapshotRows(this.targetRowBytes) {
    final base = _rowJson(1, '');
    final baseBytes = utf8.encode(base).length;
    if (baseBytes > targetRowBytes) {
      throw StateError(
        'snapshot row base size $baseBytes exceeds target $targetRowBytes',
      );
    }
    padding = 'x' * (targetRowBytes - baseBytes);
    if (utf8.encode(_rowJson(1, padding)).length != targetRowBytes) {
      throw StateError('snapshot row byte sizing is not exact');
    }
  }

  final int targetRowBytes;
  late final String padding;

  String chunkJson({
    required int firstOrdinal,
    required int rowCount,
    required int nextOrdinal,
    required int chunkBytes,
    required bool hasMore,
  }) {
    final body = StringBuffer(
      '{"snapshot_id":"snapshot-phase4c-memory",'
      '"snapshot_bundle_seq":1,"rows":[',
    );
    for (var offset = 0; offset < rowCount; offset++) {
      if (offset > 0) body.write(',');
      body.write(_rowJson(firstOrdinal + offset, padding));
    }
    body
      ..write('],"next_row_ordinal":$nextOrdinal')
      ..write(',"byte_count":$chunkBytes')
      ..write(',"has_more":$hasMore}');
    return body.toString();
  }

  String _rowJson(int ordinal, String body) {
    final id = 'user-${ordinal.toString().padLeft(9, '0')}';
    return '{"schema":"main","table":"users","key":{"id":"$id"},'
        '"row_version":1,"payload":{"id":"$id","body":"$body"}}';
  }
}

final class _RssMetrics {
  const _RssMetrics({
    required this.baselineRssBytes,
    required this.peakRssBytes,
    required this.sampleCount,
  });

  final int baselineRssBytes;
  final int peakRssBytes;
  final int sampleCount;
}

final class _RssSampler {
  _RssSampler._(this.baselineRssBytes, this._peak, this._timer);

  final int baselineRssBytes;
  final List<int> _peak;
  final Timer _timer;
  var _sampleCount = 1;

  static _RssSampler start() {
    final baseline = ProcessInfo.currentRss;
    final peak = <int>[baseline];
    late final _RssSampler sampler;
    final timer = Timer.periodic(const Duration(milliseconds: 10), (_) {
      sampler._sampleCount++;
      peak[0] = max(peak[0], ProcessInfo.currentRss);
    });
    sampler = _RssSampler._(baseline, peak, timer);
    return sampler;
  }

  _RssMetrics stop() {
    _timer.cancel();
    _peak[0] = max(_peak[0], ProcessInfo.currentRss);
    return _RssMetrics(
      baselineRssBytes: baselineRssBytes,
      peakRssBytes: _peak[0],
      sampleCount: _sampleCount,
    );
  }
}

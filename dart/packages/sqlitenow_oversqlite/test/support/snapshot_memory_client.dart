import 'dart:async';
import 'dart:convert';
import 'dart:developer' as developer;
import 'dart:io';
import 'dart:math';

import 'package:sqlitenow_oversqlite/src/config.dart';
import 'package:sqlitenow_oversqlite/src/download.dart';
import 'package:sqlitenow_oversqlite/src/local_runtime.dart';
import 'package:sqlitenow_oversqlite/src/protocol.dart';
import 'package:sqlitenow_oversqlite/src/runtime_state_store.dart';
import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';

const _githubActionsGuardMessage =
    'Phase 4C snapshot memory workloads are local-only and must not run in GitHub Actions.';

Future<void> main(List<String> arguments) async {
  if (Platform.environment['GITHUB_ACTIONS'] == 'true') {
    stderr.writeln(_githubActionsGuardMessage);
    exitCode = 64;
    return;
  }

  final options = _Options.parse(arguments);
  final sampler = await _DartMemorySampler.start();
  final databaseFile = File(options.databasePath);
  final database = SqliteNowDatabase(path: databaseFile.path);
  final config = OversqliteConfig(
    schema: 'main',
    syncTables: [SyncTable(tableName: 'users', syncKeyColumnName: 'id')],
    snapshotChunkRows: 1000,
    snapshotChunkBytes: 4 * 1024 * 1024,
    snapshotApplyBatchRows: 256,
    snapshotApplyBatchBytes: 4 * 1024 * 1024,
  );
  final diagnostics = SnapshotDiagnosticsRecorder();
  final transport = IoOversqliteHttpClient(baseUri: options.baseUri);
  final remoteApi = OversqliteRemoteApi(
    transport,
    snapshotDiagnostics: diagnostics,
  );
  final stopwatch = Stopwatch()..start();
  var appliedRows = 0;
  var sqliteBytes = 0;
  var sqliteWalBytes = 0;
  try {
    await database.open(
      preInit: (connection) async {
        await connection.execute(
          'CREATE TABLE users ('
          'id TEXT PRIMARY KEY NOT NULL, '
          'body TEXT NOT NULL)',
        );
        await connection.execute('PRAGMA journal_mode = WAL');
      },
    );
    final localRuntime = OversqliteLocalRuntime(
      database: database,
      config: config,
    );
    final validated = await localRuntime.initialize();
    final sourceId = (await localRuntime.sourceInfo()).currentSourceId;
    await OversqliteAttachmentStateStore(
      database.connection,
    ).persistAttachedState(
      sourceId: sourceId,
      userId: 'phase4c-memory-user',
      schema: 'main',
      lastBundleSeqSeen: 0,
    );
    final capabilities = await remoteApi.fetchCapabilities(sourceId);
    final runtime = OversqliteDownloadRuntime(
      database: database,
      config: config,
      remoteApi: remoteApi,
      capabilities: capabilities,
    );
    final result = await runtime.rebuildFromSnapshot(
      validated: validated,
      sourceId: sourceId,
      userId: 'phase4c-memory-user',
    );
    if (result.outcome != RemoteSyncOutcome.appliedSnapshot ||
        result.restore?.rowCount != options.rowCount) {
      throw StateError('snapshot restore did not report the expected result');
    }
    appliedRows = await _scalarInt(
      database.connection,
      'SELECT COUNT(*) FROM users',
    );
    if (appliedRows != options.rowCount) {
      throw StateError(
        'snapshot applied $appliedRows rows, expected ${options.rowCount}',
      );
    }
    if (await _scalarInt(
          database.connection,
          'SELECT COUNT(*) FROM _sync_snapshot_stage',
        ) !=
        0) {
      throw StateError('snapshot stage was not cleaned after commit');
    }
    final attachment = await OversqliteClientStateStore(
      database.connection,
    ).loadAttachmentState();
    if (attachment.bindingState != oversqliteAttachmentBindingAttached ||
        attachment.attachedUserId != 'phase4c-memory-user' ||
        attachment.lastBundleSeqSeen != 1) {
      throw StateError('snapshot lifecycle state was not published on commit');
    }
    final walFile = File('${databaseFile.path}-wal');
    sqliteBytes = databaseFile.existsSync() ? databaseFile.lengthSync() : 0;
    sqliteWalBytes = walFile.existsSync() ? walFile.lengthSync() : 0;
  } finally {
    stopwatch.stop();
    transport.close(force: true);
    if (database.isOpen) await database.close();
  }

  final memory = await sampler.stop();
  final snapshot = diagnostics.snapshot();
  _verifyBounds(snapshot, options);
  final elapsedMicros = max(stopwatch.elapsedMicroseconds, 1);
  final result = <String, Object?>{
    'event': 'snapshot_memory_client_result',
    'runtime': 'dart-vm',
    'label': options.label,
    'pid': pid,
    'rows': options.rowCount,
    'target_row_bytes': options.targetRowBytes,
    'declared_total_bytes': options.rowCount * options.targetRowBytes,
    'applied_rows': appliedRows,
    'session_count': snapshot.sessionCount,
    'fetched_chunk_count': snapshot.fetchedChunkCount,
    'max_validated_chunk_rows': snapshot.maxValidatedChunkRows,
    'max_declared_chunk_bytes': snapshot.maxDeclaredChunkBytes,
    'max_completely_decoded_body_bytes': snapshot.maxCompletelyDecodedBodyBytes,
    'apply_page_count': snapshot.applyPageCount,
    'max_apply_page_rows': snapshot.maxApplyPageRows,
    'max_apply_page_staged_text_bytes': snapshot.maxApplyPageBytes,
    'max_apply_metadata_rows': snapshot.maxApplyMetadataRows,
    'max_apply_driver_rows': snapshot.maxApplyDriverRows,
    'max_apply_decoded_rows': snapshot.maxApplyDecodedRows,
    'staged_row_count': snapshot.stagedRowCount,
    'applied_row_count': snapshot.appliedRowCount,
    'restore_duration_micros': snapshot.restoreDuration.inMicroseconds,
    'elapsed_micros': elapsedMicros,
    'throughput_rows_per_second':
        options.rowCount * Duration.microsecondsPerSecond / elapsedMicros,
    'baseline_heap_bytes': memory.baselineHeapBytes,
    'peak_heap_bytes': memory.peakHeapBytes,
    'adjusted_heap_bytes': memory.peakHeapBytes - memory.baselineHeapBytes,
    'baseline_external_bytes': memory.baselineExternalBytes,
    'peak_external_bytes': memory.peakExternalBytes,
    'baseline_rss_bytes': memory.baselineRssBytes,
    'peak_rss_bytes': memory.peakRssBytes,
    'adjusted_rss_bytes': memory.peakRssBytes - memory.baselineRssBytes,
    'vm_max_rss_bytes': ProcessInfo.maxRss,
    'sampler_samples': memory.sampleCount,
    'sqlite_bytes': sqliteBytes,
    'sqlite_wal_bytes': sqliteWalBytes,
  };
  final encoded = '${jsonEncode(result)}\n';
  await File(options.resultPath).writeAsString(encoded, flush: true);
  stdout.write(encoded);
}

void _verifyBounds(SnapshotRestoreDiagnostics diagnostics, _Options options) {
  if (diagnostics.sessionCount != 1 ||
      diagnostics.stagedRowCount != options.rowCount ||
      diagnostics.appliedRowCount != options.rowCount) {
    throw StateError('snapshot diagnostic final totals are inconsistent');
  }
  if (diagnostics.maxValidatedChunkRows > 1000 ||
      diagnostics.maxDeclaredChunkBytes > 4 * 1024 * 1024 ||
      diagnostics.maxCompletelyDecodedBodyBytes >
          4 * 1024 * 1024 + 1000 + 64 * 1024) {
    throw StateError('snapshot ingress bound was exceeded');
  }
  if (diagnostics.maxApplyPageRows > 256 ||
      diagnostics.maxApplyMetadataRows > 256 ||
      diagnostics.maxApplyDriverRows > 256 ||
      diagnostics.maxApplyDecodedRows > 256 ||
      diagnostics.maxApplyPageBytes > 4 * 1024 * 1024) {
    throw StateError('snapshot apply-page bound was exceeded');
  }
}

Future<int> _scalarInt(SqliteNowConnection connection, String sql) async {
  final rows = await connection.select(sql, (row) => row.readInt(0));
  return rows.single;
}

final class _Options {
  const _Options({
    required this.label,
    required this.rowCount,
    required this.targetRowBytes,
    required this.baseUri,
    required this.databasePath,
    required this.resultPath,
  });

  final String label;
  final int rowCount;
  final int targetRowBytes;
  final Uri baseUri;
  final String databasePath;
  final String resultPath;

  static _Options parse(List<String> arguments) {
    final values = <String, String>{};
    for (var index = 0; index < arguments.length; index += 2) {
      if (index + 1 >= arguments.length || !arguments[index].startsWith('--')) {
        throw ArgumentError('expected --name value arguments');
      }
      values[arguments[index].substring(2)] = arguments[index + 1];
    }
    String requiredValue(String name) {
      final value = values[name];
      if (value == null || value.isEmpty) {
        throw ArgumentError('missing --$name');
      }
      return value;
    }

    final rowCount = int.tryParse(requiredValue('rows'));
    final targetRowBytes = int.tryParse(requiredValue('target-row-bytes'));
    if (rowCount == null || rowCount <= 0 || rowCount > 1000000) {
      throw ArgumentError.value(values['rows'], 'rows');
    }
    if (targetRowBytes == null ||
        targetRowBytes < 256 ||
        targetRowBytes > 1024) {
      throw ArgumentError.value(values['target-row-bytes'], 'target-row-bytes');
    }
    final baseUri = Uri.parse(requiredValue('base-url'));
    if (!baseUri.isScheme('http') ||
        !const {'127.0.0.1', 'localhost', '::1'}.contains(baseUri.host)) {
      throw ArgumentError('base-url must be an HTTP loopback URI');
    }
    return _Options(
      label: requiredValue('label'),
      rowCount: rowCount,
      targetRowBytes: targetRowBytes,
      baseUri: baseUri,
      databasePath: requiredValue('database-file'),
      resultPath: requiredValue('result-file'),
    );
  }
}

final class _DartMemoryMetrics {
  const _DartMemoryMetrics({
    required this.baselineHeapBytes,
    required this.peakHeapBytes,
    required this.baselineExternalBytes,
    required this.peakExternalBytes,
    required this.baselineRssBytes,
    required this.peakRssBytes,
    required this.sampleCount,
  });

  final int baselineHeapBytes;
  final int peakHeapBytes;
  final int baselineExternalBytes;
  final int peakExternalBytes;
  final int baselineRssBytes;
  final int peakRssBytes;
  final int sampleCount;
}

final class _DartMemorySampler {
  _DartMemorySampler._({
    required Uri serviceUri,
    required String isolateId,
    required int baselineHeapBytes,
    required int baselineExternalBytes,
    required int baselineRssBytes,
  }) : _serviceUri = serviceUri,
       _isolateId = isolateId,
       _baselineHeapBytes = baselineHeapBytes,
       _baselineExternalBytes = baselineExternalBytes,
       _baselineRssBytes = baselineRssBytes,
       _peakHeapBytes = baselineHeapBytes,
       _peakExternalBytes = baselineExternalBytes,
       _peakRssBytes = baselineRssBytes;

  final Uri _serviceUri;
  final String _isolateId;
  final int _baselineHeapBytes;
  final int _baselineExternalBytes;
  final int _baselineRssBytes;
  int _peakHeapBytes;
  int _peakExternalBytes;
  int _peakRssBytes;
  Timer? _timer;
  Future<void>? _activeSample;
  var _sampleCount = 1;

  static Future<_DartMemorySampler> start() async {
    final info = await developer.Service.getInfo();
    final serviceUri = info.serverUri;
    if (serviceUri == null) {
      throw StateError(
        'Dart VM service is required; run the client with --enable-vm-service=0',
      );
    }
    final vm = await _rpc(serviceUri, 'getVM');
    final isolates = vm['isolates'];
    if (isolates is! List || isolates.isEmpty) {
      throw StateError('Dart VM service returned no application isolate');
    }
    final isolateId = (isolates.first as Map)['id'];
    if (isolateId is! String || isolateId.isEmpty) {
      throw StateError('Dart VM service returned an invalid isolate id');
    }
    final initial = await _memoryUsage(serviceUri, isolateId);
    final sampler = _DartMemorySampler._(
      serviceUri: serviceUri,
      isolateId: isolateId,
      baselineHeapBytes: initial.heapUsage,
      baselineExternalBytes: initial.externalUsage,
      baselineRssBytes: ProcessInfo.currentRss,
    );
    sampler._timer = Timer.periodic(const Duration(milliseconds: 10), (_) {
      if (sampler._activeSample != null) return;
      sampler._activeSample = sampler._sample().whenComplete(() {
        sampler._activeSample = null;
      });
    });
    return sampler;
  }

  Future<void> _sample() async {
    final current = await _memoryUsage(_serviceUri, _isolateId);
    _sampleCount++;
    _peakHeapBytes = max(_peakHeapBytes, current.heapUsage);
    _peakExternalBytes = max(_peakExternalBytes, current.externalUsage);
    _peakRssBytes = max(_peakRssBytes, ProcessInfo.currentRss);
  }

  Future<_DartMemoryMetrics> stop() async {
    _timer?.cancel();
    await _activeSample;
    await _sample();
    return _DartMemoryMetrics(
      baselineHeapBytes: _baselineHeapBytes,
      peakHeapBytes: _peakHeapBytes,
      baselineExternalBytes: _baselineExternalBytes,
      peakExternalBytes: _peakExternalBytes,
      baselineRssBytes: _baselineRssBytes,
      peakRssBytes: _peakRssBytes,
      sampleCount: _sampleCount,
    );
  }

  static Future<Map<String, Object?>> _rpc(
    Uri serviceUri,
    String method, [
    Map<String, String> parameters = const {},
  ]) async {
    final client = HttpClient();
    try {
      final uri = serviceUri
          .resolve(method)
          .replace(queryParameters: parameters.isEmpty ? null : parameters);
      final request = await client.getUrl(uri);
      final response = await request.close();
      final body = await utf8.decoder.bind(response).join();
      if (response.statusCode != HttpStatus.ok) {
        throw StateError(
          'Dart VM service $method failed with HTTP ${response.statusCode}',
        );
      }
      final decoded = jsonDecode(body);
      if (decoded is! Map || decoded['result'] is! Map) {
        throw StateError('Dart VM service $method returned invalid JSON');
      }
      return (decoded['result']! as Map).cast<String, Object?>();
    } finally {
      client.close(force: true);
    }
  }

  static Future<({int heapUsage, int externalUsage})> _memoryUsage(
    Uri serviceUri,
    String isolateId,
  ) async {
    final result = await _rpc(serviceUri, 'getMemoryUsage', {
      'isolateId': isolateId,
    });
    final heapUsage = result['heapUsage'];
    final externalUsage = result['externalUsage'];
    if (heapUsage is! int || heapUsage < 0) {
      throw StateError('Dart VM service returned invalid heap usage');
    }
    if (externalUsage is! int || externalUsage < 0) {
      throw StateError('Dart VM service returned invalid external usage');
    }
    return (heapUsage: heapUsage, externalUsage: externalUsage);
  }
}

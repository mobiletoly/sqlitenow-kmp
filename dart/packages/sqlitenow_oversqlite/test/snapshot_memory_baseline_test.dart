import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:test/test.dart';

void main() {
  test('Dart VM snapshot memory baseline', () async {
    final rawRows = Platform.environment['OVERSQLITE_MEMORY_BASELINE_ROWS'];
    if (rawRows == null) return;
    final rowCount = int.tryParse(rawRows);
    if (rowCount == null || !const {1000, 100000, 1000000}.contains(rowCount)) {
      fail(
        'OVERSQLITE_MEMORY_BASELINE_ROWS must be 1000, 100000, or 1000000, got $rawRows',
      );
    }

    final directory = await Directory.systemTemp.createTemp(
      'oversqlite-dart-memory-baseline-',
    );
    final databaseFile = File('${directory.path}/oversqlite.sqlite');
    final database = SqliteNowDatabase(path: databaseFile.path);
    await database.open(
      preInit: (connection) async {
        await connection.execute(
          'CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)',
        );
        await connection.execute('PRAGMA journal_mode = WAL');
      },
    );
    final server = _SnapshotMemoryServer(rowCount);
    final client = DefaultOversqliteClient(
      database: database,
      config: const OversqliteConfig(
        schema: 'main',
        syncTables: [SyncTable(tableName: 'users', syncKeyColumnName: 'id')],
      ),
      httpClient: server,
    );
    final sampler = _DartMemorySampler.start();
    final stopwatch = Stopwatch()..start();
    var appliedRows = 0;
    var sqliteBytes = 0;
    var sqliteWalBytes = 0;
    try {
      await client.open();
      await client.attach('memory-baseline-user');
      await client.rebuild();
      appliedRows = await _scalarInt(
        database.connection,
        'SELECT COUNT(*) FROM users',
      );
      expect(appliedRows, rowCount);
      expect(
        await _scalarInt(
          database.connection,
          'SELECT COUNT(*) FROM _sync_snapshot_stage',
        ),
        0,
      );
      // Measure SQLite artifacts before close can checkpoint and remove the WAL.
      final walFile = File('${databaseFile.path}-wal');
      sqliteBytes = databaseFile.existsSync() ? databaseFile.lengthSync() : 0;
      sqliteWalBytes = walFile.existsSync() ? walFile.lengthSync() : 0;
    } finally {
      stopwatch.stop();
      await client.close();
      await database.close();
    }
    final metrics = sampler.stop();
    stdout.writeln(
      jsonEncode({
        'event': 'snapshot_memory_baseline',
        'runtime': 'dart-vm',
        'rows': rowCount,
        'applied_rows': appliedRows,
        'chunks': server.chunkCount,
        'max_chunk_rows': server.maxChunkRows,
        'driver_selected_rows_high_water': rowCount,
        'converted_apply_rows_high_water': rowCount,
        'elapsed_ms': stopwatch.elapsedMilliseconds,
        'baseline_rss_bytes': metrics.baselineRssBytes,
        'peak_rss_bytes': metrics.peakRssBytes,
        'vm_max_rss_bytes': ProcessInfo.maxRss,
        'sqlite_bytes': sqliteBytes,
        'sqlite_wal_bytes': sqliteWalBytes,
      }),
    );
  });

  test('expected-red Dart staged apply does not retain whole snapshot', () {
    if (Platform.environment['OVERSQLITE_EXPECTED_RED_MEMORY_BOUND'] !=
        'true') {
      return;
    }
    final source = File('lib/src/download_stage_store.dart').readAsStringSync();
    final returnsFullList = RegExp(
      r'Future<List<OversqliteStagedSnapshotRow>>\s+loadStagedSnapshotRows',
    ).hasMatch(source);
    final buildsSecondList = source.contains(
      'final stagedRows = <OversqliteStagedSnapshotRow>[];',
    );
    expect(
      returnsFullList || buildsSecondList,
      isFalse,
      reason:
          'expected bounded staged apply, observed a driver result list and a second complete converted list; current structural high water is two snapshot-sized row collections',
    );
  });
}

Future<int> _scalarInt(SqliteNowConnection connection, String sql) async {
  final rows = await connection.select(sql, (row) => row.readInt(0));
  return rows.single;
}

final class _SnapshotMemoryServer implements OversqliteHttpClient {
  _SnapshotMemoryServer(this.rowCount);

  final int rowCount;
  var chunkCount = 0;
  var maxChunkRows = 0;

  @override
  Future<OversqliteHttpResponse> get(
    String path, {
    required String sourceId,
  }) async {
    if (path == 'sync/capabilities') {
      return _json({
        'protocol_version': 'v0',
        'schema_version': 1,
        'features': {'connect_lifecycle': true},
      });
    }
    if (path.startsWith('sync/snapshot-sessions/snapshot-memory-baseline')) {
      final uri = Uri.parse(path);
      final after = int.parse(uri.queryParameters['after_row_ordinal']!);
      final requested = int.parse(uri.queryParameters['max_rows']!);
      final rowsInChunk = min(requested, rowCount - after);
      chunkCount++;
      maxChunkRows = max(maxChunkRows, rowsInChunk);
      final next = after + rowsInChunk;
      return _json({
        'snapshot_id': 'snapshot-memory-baseline',
        'snapshot_bundle_seq': 1,
        'rows': [
          for (var offset = 0; offset < rowsInChunk; offset++)
            _row(after + offset + 1),
        ],
        'next_row_ordinal': next,
        'has_more': next < rowCount,
      });
    }
    return _json({'error': 'not_found', 'message': path}, statusCode: 404);
  }

  @override
  Future<OversqliteHttpResponse> postJson(
    String path, {
    required String sourceId,
    required Object? body,
  }) async {
    if (path == 'sync/connect') {
      return _json({'resolution': 'initialize_empty'});
    }
    if (path == 'sync/snapshot-sessions') {
      return _json({
        'snapshot_id': 'snapshot-memory-baseline',
        'snapshot_bundle_seq': 1,
        'row_count': rowCount,
        'byte_count': 0,
        'expires_at': '2099-01-01T00:00:00Z',
      });
    }
    return _json({'error': 'not_found', 'message': path}, statusCode: 404);
  }

  @override
  Future<OversqliteHttpResponse> delete(
    String path, {
    required String sourceId,
  }) async {
    return const OversqliteHttpResponse(statusCode: 204, body: '');
  }

  Map<String, Object?> _row(int ordinal) {
    final id = 'user-${ordinal.toString().padLeft(9, '0')}';
    return {
      'schema': 'main',
      'table': 'users',
      'key': {'id': id},
      'row_version': ordinal,
      'payload': {'id': id, 'name': 'Snapshot User'},
    };
  }

  OversqliteHttpResponse _json(
    Map<String, Object?> body, {
    int statusCode = 200,
  }) {
    return OversqliteHttpResponse(
      statusCode: statusCode,
      body: jsonEncode(body),
    );
  }
}

final class _DartMemoryMetrics {
  const _DartMemoryMetrics({
    required this.baselineRssBytes,
    required this.peakRssBytes,
  });

  final int baselineRssBytes;
  final int peakRssBytes;
}

final class _DartMemorySampler {
  _DartMemorySampler._(this._baselineRssBytes, this._timer, this._peakRssBytes);

  final int _baselineRssBytes;
  final Timer _timer;
  final List<int> _peakRssBytes;

  static _DartMemorySampler start() {
    final baseline = ProcessInfo.currentRss;
    final peak = <int>[baseline];
    final timer = Timer.periodic(const Duration(milliseconds: 5), (_) {
      peak[0] = max(peak[0], ProcessInfo.currentRss);
    });
    return _DartMemorySampler._(baseline, timer, peak);
  }

  _DartMemoryMetrics stop() {
    _timer.cancel();
    _peakRssBytes[0] = max(_peakRssBytes[0], ProcessInfo.currentRss);
    return _DartMemoryMetrics(
      baselineRssBytes: _baselineRssBytes,
      peakRssBytes: _peakRssBytes[0],
    );
  }
}

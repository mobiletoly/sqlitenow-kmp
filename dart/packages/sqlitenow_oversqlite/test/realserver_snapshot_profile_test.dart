import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:sqlitenow_oversqlite/src/config.dart';
import 'package:sqlitenow_oversqlite/src/download.dart';
import 'package:sqlitenow_oversqlite/src/local_runtime.dart';
import 'package:sqlitenow_oversqlite/src/protocol.dart';
import 'package:sqlitenow_oversqlite/src/runtime_state_store.dart';
import 'package:sqlitenow_runtime/sqlitenow_runtime.dart';
import 'package:test/test.dart';

import 'realserver_support.dart';

const _rowsEnvironment = 'OVERSQLITE_PHASE5_REALSERVER_ROWS';
const _rowBytesEnvironment = 'OVERSQLITE_PHASE5_REALSERVER_ROW_BYTES';
const _runLabelEnvironment = 'OVERSQLITE_PHASE5_REALSERVER_RUN_LABEL';

void main() {
  final configuredRows = Platform.environment[_rowsEnvironment];
  final skipReason = configuredRows == null
      ? 'Set $_rowsEnvironment to 10000 or 100000 for a Phase 5 profile.'
      : null;

  test(
    'preseeded snapshot profile restores through the real server',
    skip: skipReason,
    timeout: const Timeout(Duration(minutes: 5)),
    () async {
      if (Platform.environment['GITHUB_ACTIONS'] == 'true') {
        throw StateError(
          'Phase 5 realserver profiles are local-heavy and must not run with GITHUB_ACTIONS=true',
        );
      }
      if (!flagEnabled('OVERSQLITE_REALSERVER_HEAVY')) {
        throw StateError(
          'Phase 5 realserver profiles require OVERSQLITE_REALSERVER_HEAVY=true',
        );
      }

      final rows = int.tryParse(configuredRows!);
      if (rows != 10000 && rows != 100000) {
        throw StateError('$_rowsEnvironment must be 10000 or 100000');
      }
      final rowCount = rows!;
      final rowBytes = int.tryParse(
        Platform.environment[_rowBytesEnvironment] ?? '',
      );
      if (rowBytes != 256 && rowBytes != 1024) {
        throw StateError('$_rowBytesEnvironment must be 256 or 1024');
      }
      final targetRowBytes = rowBytes!;
      final runLabel = switch (Platform.environment[_runLabelEnvironment]
          ?.trim()) {
        final value? when value.isNotEmpty => value,
        _ => 'manual-${rowCount}x$targetRowBytes',
      };
      final userId = 'phase5-${rowCount}x$targetRowBytes';
      final realServer = await requireRealServerConfig();
      final directory = await Directory.systemTemp.createTemp(
        'oversqlite-phase5-realserver-',
      );
      final databaseFile = File('${directory.path}/oversqlite.sqlite');
      final database = SqliteNowDatabase(path: databaseFile.path);
      final diagnostics = SnapshotDiagnosticsRecorder();
      IoOversqliteHttpClient? transport;
      final stopwatch = Stopwatch();
      try {
        await database.open(
          preInit: (connection) async {
            await connection.execute('''CREATE TABLE users (
  id TEXT PRIMARY KEY NOT NULL,
  name TEXT NOT NULL,
  email TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
)''');
            await connection.execute('PRAGMA journal_mode = WAL');
          },
        );
        final config = OversqliteConfig(
          schema: 'business',
          syncTables: const [
            SyncTable(tableName: 'users', syncKeyColumnName: 'id'),
          ],
          snapshotChunkRows: 1000,
          snapshotChunkBytes: 4 * 1024 * 1024,
          snapshotApplyBatchRows: 256,
          snapshotApplyBatchBytes: 4 * 1024 * 1024,
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
          userId: userId,
          schema: 'business',
          lastBundleSeqSeen: 0,
        );
        final token = await issueDummySigninToken(
          realServer.baseUrl,
          userId,
          sourceId,
        );
        transport = IoOversqliteHttpClient(
          baseUri: Uri.parse(realServer.baseUrl),
          defaultHeaders: {HttpHeaders.authorizationHeader: 'Bearer $token'},
        );
        final remoteApi = OversqliteRemoteApi(
          transport,
          snapshotDiagnostics: diagnostics,
        );
        final capabilities = await remoteApi.fetchCapabilities(sourceId);
        final runtime = OversqliteDownloadRuntime(
          database: database,
          config: config,
          remoteApi: remoteApi,
          capabilities: capabilities,
        );

        stopwatch.start();
        final result = await runtime.rebuildFromSnapshot(
          validated: validated,
          sourceId: sourceId,
          userId: userId,
        );
        stopwatch.stop();
        expect(result.outcome, RemoteSyncOutcome.appliedSnapshot);
        expect(result.restore?.rowCount, rowCount);
        expect(
          await _scalarInt(database, 'SELECT COUNT(*) FROM users'),
          rowCount,
        );
        expect(
          await _scalarInt(
            database,
            'SELECT COUNT(*) FROM _sync_snapshot_stage',
          ),
          0,
        );

        final snapshot = diagnostics.snapshot();
        expect(snapshot.sessionCount, 1);
        expect(snapshot.stagedRowCount, rowCount);
        expect(snapshot.appliedRowCount, rowCount);
        expect(snapshot.fetchedChunkCount, greaterThan(1));
        expect(snapshot.maxValidatedChunkRows, lessThanOrEqualTo(1000));
        expect(
          snapshot.maxDeclaredChunkBytes,
          lessThanOrEqualTo(4 * 1024 * 1024),
        );
        expect(
          snapshot.maxCompletelyDecodedBodyBytes,
          lessThanOrEqualTo(4 * 1024 * 1024 + 64 * 1024),
        );
        expect(snapshot.maxApplyPageRows, lessThanOrEqualTo(256));
        expect(snapshot.maxApplyPageBytes, lessThanOrEqualTo(4 * 1024 * 1024));
        expect(snapshot.capacityResponseCount, 0);
        expect(snapshot.capacityRetryCount, 0);

        final walFile = File('${databaseFile.path}-wal');
        final elapsedMicros = max(stopwatch.elapsedMicroseconds, 1);
        stdout.writeln(
          jsonEncode({
            'event': 'phase5_realserver_profile',
            'runtime': 'dart-vm',
            'label': runLabel,
            'rows': rowCount,
            'approximate_row_bytes': targetRowBytes,
            'elapsed_ms': stopwatch.elapsedMilliseconds,
            'throughput_rows_per_second': rowCount * 1000000 / elapsedMicros,
            'sessions': snapshot.sessionCount,
            'chunks': snapshot.fetchedChunkCount,
            'max_chunk_rows': snapshot.maxValidatedChunkRows,
            'max_chunk_wire_bytes': snapshot.maxDeclaredChunkBytes,
            'max_chunk_decoded_body_bytes':
                snapshot.maxCompletelyDecodedBodyBytes,
            'apply_pages': snapshot.applyPageCount,
            'max_apply_page_rows': snapshot.maxApplyPageRows,
            'max_apply_page_staged_text_bytes': snapshot.maxApplyPageBytes,
            'sqlite_bytes': databaseFile.existsSync()
                ? databaseFile.lengthSync()
                : 0,
            'sqlite_wal_bytes': walFile.existsSync() ? walFile.lengthSync() : 0,
          }),
        );
      } finally {
        stopwatch.stop();
        transport?.close(force: true);
        if (database.isOpen) await database.close();
        await directory.delete(recursive: true);
      }
    },
  );
}

Future<int> _scalarInt(SqliteNowDatabase database, String sql) async {
  final rows = await database.connection.select(sql, (row) => row.readInt(0));
  return rows.single;
}

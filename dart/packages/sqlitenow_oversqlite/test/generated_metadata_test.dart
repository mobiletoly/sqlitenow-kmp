import 'generated/sync_dart_db.dart';
import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

void main() {
  test('generated database exposes Oversqlite metadata only', () {
    expect(SyncDartDb.syncTables, hasLength(1));
    expect(SyncDartDb.syncTables.single.tableName, 'docs');
    expect(SyncDartDb.syncTables.single.syncKeyColumnName, 'doc_id');

    final database = SyncDartDb.inMemory();
    final config = database.buildOversqliteConfig(schema: 'main');

    expect(config.schema, 'main');
    expect(config.syncTables, SyncDartDb.syncTables);
    expect(config.uploadLimit, 200);
    expect(config.downloadLimit, 1000);
    expect(config.verboseLogs, isFalse);

    final client = database.newOversqliteClient(
      schema: 'main',
      httpClient: _NoopHttpClient(),
    );
    expect(client, isA<DefaultOversqliteClient>());
  });
}

final class _NoopHttpClient implements OversqliteHttpClient {
  @override
  Future<OversqliteHttpResponse> get(String path, {required String sourceId}) {
    throw UnimplementedError();
  }

  @override
  Future<OversqliteHttpResponse> postJson(
    String path, {
    required String sourceId,
    required Object? body,
  }) {
    throw UnimplementedError();
  }

  @override
  Future<OversqliteHttpResponse> delete(
    String path, {
    required String sourceId,
  }) {
    throw UnimplementedError();
  }
}

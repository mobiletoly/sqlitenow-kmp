import 'generated/rich_real_server_db.dart';
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
    expect(config.automaticDownloadInterval, const Duration(seconds: 60));
    expect(config.bundleChangeWatchMode, BundleChangeWatchMode.off);
    expect(config.bundleChangeWatchReconnectMin, const Duration(seconds: 1));
    expect(config.bundleChangeWatchReconnectMax, const Duration(seconds: 60));

    final watchConfig = database.buildOversqliteConfig(
      schema: 'main',
      automaticDownloadInterval: const Duration(milliseconds: 25),
      bundleChangeWatchMode: BundleChangeWatchMode.auto,
      bundleChangeWatchReconnectMin: const Duration(milliseconds: 10),
      bundleChangeWatchReconnectMax: const Duration(milliseconds: 20),
    );
    expect(
      watchConfig.automaticDownloadInterval,
      const Duration(milliseconds: 25),
    );
    expect(watchConfig.bundleChangeWatchMode, BundleChangeWatchMode.auto);
    expect(
      watchConfig.bundleChangeWatchReconnectMin,
      const Duration(milliseconds: 10),
    );
    expect(
      watchConfig.bundleChangeWatchReconnectMax,
      const Duration(milliseconds: 20),
    );

    final client = database.newOversqliteClient(
      schema: 'main',
      httpClient: _NoopHttpClient(),
      automaticDownloadInterval: const Duration(milliseconds: 25),
      bundleChangeWatchMode: BundleChangeWatchMode.auto,
    );
    expect(client, isA<DefaultOversqliteClient>());
  });

  test('generated rich realserver database exposes full sync metadata', () {
    expect(RichRealServerDb.syncTables.map((table) => table.tableName), [
      'categories',
      'file_reviews',
      'files',
      'posts',
      'team_members',
      'teams',
      'typed_rows',
      'users',
    ]);
    expect(
      RichRealServerDb.syncTables.map((table) => table.syncKeyColumnName),
      everyElement('id'),
    );

    final database = RichRealServerDb.inMemory();
    final config = database.buildOversqliteConfig(
      schema: 'business',
      uploadLimit: 8,
      downloadLimit: 8,
    );

    expect(config.schema, 'business');
    expect(config.syncTables, RichRealServerDb.syncTables);
    expect(config.uploadLimit, 8);
    expect(config.downloadLimit, 8);
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

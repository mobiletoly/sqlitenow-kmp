import 'package:sqlitenow_oversqlite/sqlitenow_oversqlite.dart';
import 'package:test/test.dart';

void main() {
  test('SyncTable stores explicit sync key metadata', () {
    const table = SyncTable(tableName: 'docs', syncKeyColumnName: 'doc_id');

    expect(table.tableName, 'docs');
    expect(table.syncKeyColumnName, 'doc_id');
  });

  test('OversqliteConfig keeps generated metadata and default limits', () {
    const table = SyncTable(tableName: 'docs', syncKeyColumnName: 'doc_id');
    const config = OversqliteConfig(schema: 'main', syncTables: [table]);

    expect(config.schema, 'main');
    expect(config.syncTables, [table]);
    expect(config.uploadLimit, 200);
    expect(config.downloadLimit, 1000);
    expect(config.verboseLogs, isFalse);
    expect(config.automaticDownloadInterval, const Duration(seconds: 60));
    expect(config.bundleChangeWatchMode, BundleChangeWatchMode.off);
    expect(config.bundleChangeWatchReconnectMin, const Duration(seconds: 1));
    expect(config.bundleChangeWatchReconnectMax, const Duration(seconds: 60));
  });
}

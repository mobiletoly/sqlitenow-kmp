part of 'local_runtime.dart';

const _hiddenSyncScopeColumnName = '_sync_scope_id';
const _operationKindNone = 'none';
const _operationKindRemoteReplace = 'remote_replace';
const _attachmentBindingAnonymous = 'anonymous';
const _attachmentBindingAttached = 'attached';

final class OversqliteValidatedTable {
  const OversqliteValidatedTable({
    required this.tableName,
    required this.syncKeyColumnName,
  });

  final String tableName;
  final String syncKeyColumnName;
}

final class OversqliteValidatedConfig {
  const OversqliteValidatedConfig({
    required this.schema,
    required this.tables,
    required this.tableOrder,
  });

  final String schema;
  final List<OversqliteValidatedTable> tables;
  final Map<String, int> tableOrder;
}

final class OversqliteSourceInfo {
  const OversqliteSourceInfo({required this.currentSourceId});

  final String currentSourceId;
}

enum _ColumnKind { text, integer, real, blob, uuidBlob }

extension on _ColumnKind {
  bool get isBlobKind =>
      this == _ColumnKind.blob || this == _ColumnKind.uuidBlob;
}

_ColumnKind _classifyColumnKind(
  String declaredType, {
  required bool isPrimaryKey,
  required bool isBlobReference,
}) {
  final type = declaredType.toLowerCase();
  if (type.contains('blob') && (isPrimaryKey || isBlobReference)) {
    return _ColumnKind.uuidBlob;
  }
  if (type.contains('blob')) return _ColumnKind.blob;
  if (type.contains('real') ||
      type.contains('float') ||
      type.contains('double')) {
    return _ColumnKind.real;
  }
  if (type.contains('int')) return _ColumnKind.integer;
  return _ColumnKind.text;
}

final class _RawColumnInfo {
  const _RawColumnInfo({
    required this.name,
    required this.declaredType,
    required this.isPrimaryKey,
    required this.notNull,
    required this.defaultValue,
  });

  final String name;
  final String declaredType;
  final bool isPrimaryKey;
  final bool notNull;
  final String? defaultValue;
}

final class _ColumnInfo {
  const _ColumnInfo({
    required this.name,
    required this.declaredType,
    required this.isPrimaryKey,
    required this.notNull,
    required this.defaultValue,
    required this.kind,
  });

  final String name;
  final String declaredType;
  final bool isPrimaryKey;
  final bool notNull;
  final String? defaultValue;
  final _ColumnKind kind;
}

final class _ForeignKeyInfo {
  const _ForeignKeyInfo({
    required this.seq,
    required this.refTable,
    required this.fromCol,
    required this.toCol,
  });

  final int seq;
  final String refTable;
  final String fromCol;
  final String toCol;
}

final class _TableInfo {
  const _TableInfo({
    required this.table,
    required this.columns,
    required this.foreignKeys,
    required this.foreignKeyColumnsLower,
  });

  final String table;
  final List<_ColumnInfo> columns;
  final List<_ForeignKeyInfo> foreignKeys;
  final Set<String> foreignKeyColumnsLower;

  List<String> get columnNamesLower =>
      columns.map((column) => column.name.toLowerCase()).toList();
}

final class _AttachmentState {
  const _AttachmentState({
    required this.currentSourceId,
    required this.bindingState,
    required this.attachedUserId,
    required this.schemaName,
    required this.lastBundleSeqSeen,
    required this.rebuildRequired,
    required this.pendingInitializationId,
  });

  final String currentSourceId;
  final String bindingState;
  final String attachedUserId;
  final String schemaName;
  final int lastBundleSeqSeen;
  final bool rebuildRequired;
  final String pendingInitializationId;
}

final class _OperationState {
  const _OperationState({
    required this.kind,
    required this.targetUserId,
    required this.stagedSnapshotId,
    required this.snapshotBundleSeq,
    required this.snapshotRowCount,
    required this.snapshotByteCount,
    required this.snapshotStageComplete,
    this.reason = '',
    this.replacementSourceId = '',
  });

  final String kind;
  final String targetUserId;
  final String stagedSnapshotId;
  final int snapshotBundleSeq;
  final int snapshotRowCount;
  final int snapshotByteCount;
  final bool snapshotStageComplete;
  final String reason;
  final String replacementSourceId;
}

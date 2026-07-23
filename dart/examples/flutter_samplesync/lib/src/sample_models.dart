import 'dart:convert';

import 'db/generated/now_sample_sync_database.dart';

enum AddressType {
  home('home'),
  work('work');

  const AddressType(this.sqlValue);

  final String sqlValue;

  static AddressType fromSql(String value) {
    return values.firstWhere(
      (type) => type.sqlValue == value.toLowerCase(),
      orElse: () => throw FormatException('Unknown address type: $value'),
    );
  }
}

enum SampleSyncMode {
  polling(
    storageValue: 'polling',
    label: 'Polling',
    automaticDownloadInterval: Duration(seconds: 10),
  ),
  watch(
    storageValue: 'watch',
    label: 'Watch',
    automaticDownloadInterval: Duration(seconds: 60),
  );

  const SampleSyncMode({
    required this.storageValue,
    required this.label,
    required this.automaticDownloadInterval,
  });

  final String storageValue;
  final String label;
  final Duration automaticDownloadInterval;

  static SampleSyncMode fromStorage(String? value) {
    return values.firstWhere(
      (mode) => mode.storageValue == value,
      orElse: () => SampleSyncMode.watch,
    );
  }
}

final class SampleComment {
  const SampleComment({
    required this.id,
    required this.comment,
    required this.createdAt,
    required this.tags,
  });

  factory SampleComment.fromRow(CommentRow row) {
    final decoded = row.tags == null
        ? const <Object?>[]
        : jsonDecode(row.tags!) as List<Object?>;
    return SampleComment(
      id: row.id,
      comment: row.comment,
      createdAt: row.createdAt,
      tags: decoded.cast<String>(),
    );
  }

  final String id;
  final String comment;
  final DateTime createdAt;
  final List<String> tags;
}

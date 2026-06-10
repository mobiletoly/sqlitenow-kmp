import 'dart:convert';

import 'protocol_models.dart';

final class BundleCapabilitiesLimits {
  const BundleCapabilitiesLimits({
    required this.maxRowsPerBundle,
    required this.maxBytesPerBundle,
    required this.maxBundlesPerPull,
    required this.defaultRowsPerPushChunk,
    required this.maxRowsPerPushChunk,
    required this.pushSessionTtlSeconds,
    required this.defaultRowsPerCommittedBundleChunk,
    required this.maxRowsPerCommittedBundleChunk,
    required this.defaultRowsPerSnapshotChunk,
    required this.maxRowsPerSnapshotChunk,
    required this.snapshotSessionTtlSeconds,
    required this.maxRowsPerSnapshotSession,
    required this.maxBytesPerSnapshotSession,
    required this.initializationLeaseTtlSeconds,
  });

  final int maxRowsPerBundle;
  final int maxBytesPerBundle;
  final int maxBundlesPerPull;
  final int defaultRowsPerPushChunk;
  final int maxRowsPerPushChunk;
  final int pushSessionTtlSeconds;
  final int defaultRowsPerCommittedBundleChunk;
  final int maxRowsPerCommittedBundleChunk;
  final int defaultRowsPerSnapshotChunk;
  final int maxRowsPerSnapshotChunk;
  final int snapshotSessionTtlSeconds;
  final int maxRowsPerSnapshotSession;
  final int maxBytesPerSnapshotSession;
  final int initializationLeaseTtlSeconds;

  factory BundleCapabilitiesLimits.fromJson(Map<String, Object?> json) {
    return BundleCapabilitiesLimits(
      maxRowsPerBundle: _readInt(json, 'max_rows_per_bundle'),
      maxBytesPerBundle: _readInt(json, 'max_bytes_per_bundle'),
      maxBundlesPerPull: _readInt(json, 'max_bundles_per_pull'),
      defaultRowsPerPushChunk: _readInt(json, 'default_rows_per_push_chunk'),
      maxRowsPerPushChunk: _readInt(json, 'max_rows_per_push_chunk'),
      pushSessionTtlSeconds: _readInt(json, 'push_session_ttl_seconds'),
      defaultRowsPerCommittedBundleChunk: _readInt(
        json,
        'default_rows_per_committed_bundle_chunk',
      ),
      maxRowsPerCommittedBundleChunk: _readInt(
        json,
        'max_rows_per_committed_bundle_chunk',
      ),
      defaultRowsPerSnapshotChunk: _readInt(
        json,
        'default_rows_per_snapshot_chunk',
      ),
      maxRowsPerSnapshotChunk: _readInt(json, 'max_rows_per_snapshot_chunk'),
      snapshotSessionTtlSeconds: _readInt(json, 'snapshot_session_ttl_seconds'),
      maxRowsPerSnapshotSession: _readInt(
        json,
        'max_rows_per_snapshot_session',
      ),
      maxBytesPerSnapshotSession: _readInt(
        json,
        'max_bytes_per_snapshot_session',
      ),
      initializationLeaseTtlSeconds: _readInt(
        json,
        'initialization_lease_ttl_seconds',
      ),
    );
  }
}

final class CapabilitiesResponse {
  const CapabilitiesResponse({
    this.protocolVersion = '',
    this.schemaVersion = 0,
    this.features = const {},
    this.bundleLimits,
  });

  final String protocolVersion;
  final int schemaVersion;
  final Map<String, bool> features;
  final BundleCapabilitiesLimits? bundleLimits;

  bool get connectLifecycleSupported => features['connect_lifecycle'] ?? false;

  bool get bundleChangeWatchSupported =>
      features['bundle_change_watch'] ?? false;

  factory CapabilitiesResponse.fromJson(Map<String, Object?> json) {
    final rawFeatures = json['features'];
    return CapabilitiesResponse(
      protocolVersion: (json['protocol_version'] as String?) ?? '',
      schemaVersion: (json['schema_version'] as int?) ?? 0,
      features: rawFeatures is Map
          ? {
              for (final entry in rawFeatures.entries)
                entry.key.toString(): entry.value == true,
            }
          : const {},
      bundleLimits: json['bundle_limits'] is Map
          ? BundleCapabilitiesLimits.fromJson(
              (json['bundle_limits'] as Map).cast<String, Object?>(),
            )
          : null,
    );
  }
}

final class BundleChangeEvent {
  const BundleChangeEvent({
    required this.bundleSeq,
    this.sourceId = '',
    this.sourceBundleId = 0,
  });

  final int bundleSeq;
  final String sourceId;
  final int sourceBundleId;

  factory BundleChangeEvent.fromJson(Map<String, Object?> json) {
    final bundleSeq = json['bundle_seq'];
    if (bundleSeq is! int) {
      throw const OversqliteProtocolException(
        'bundle change event is missing bundle_seq',
      );
    }
    if (bundleSeq <= 0) {
      throw OversqliteProtocolException(
        'bundle change event bundle_seq $bundleSeq must be positive',
      );
    }
    return BundleChangeEvent(
      bundleSeq: bundleSeq,
      sourceId: (json['source_id'] as String?) ?? '',
      sourceBundleId: (json['source_bundle_id'] as int?) ?? 0,
    );
  }

  @override
  bool operator ==(Object other) {
    return other is BundleChangeEvent &&
        other.bundleSeq == bundleSeq &&
        other.sourceId == sourceId &&
        other.sourceBundleId == sourceBundleId;
  }

  @override
  int get hashCode => Object.hash(bundleSeq, sourceId, sourceBundleId);
}

Stream<BundleChangeEvent> parseBundleChangeEventStream(Stream<String> lines) {
  final parser = _BundleChangeWatchSseParser();
  return lines.expand(parser.accept);
}

List<BundleChangeEvent> parseBundleChangeEventLines(Iterable<String> lines) {
  final parser = _BundleChangeWatchSseParser();
  final events = <BundleChangeEvent>[];
  for (final line in lines) {
    events.addAll(parser.accept(line));
  }
  parser.finish();
  return events;
}

final class _BundleChangeWatchSseParser {
  String _eventName = '';
  final List<String> _dataLines = [];

  Iterable<BundleChangeEvent> accept(String line) sync* {
    if (line.isEmpty) {
      final event = _dispatch();
      if (event != null) {
        yield event;
      }
      return;
    }
    if (line.startsWith(':')) {
      return;
    }

    final separator = line.indexOf(':');
    final field = separator < 0 ? line : line.substring(0, separator);
    var value = separator < 0 ? '' : line.substring(separator + 1);
    if (value.startsWith(' ')) {
      value = value.substring(1);
    }

    switch (field) {
      case 'event':
        _eventName = value;
      case 'data':
        _dataLines.add(value);
    }
  }

  void finish() {
    _eventName = '';
    _dataLines.clear();
  }

  BundleChangeEvent? _dispatch() {
    final shouldEmit = _eventName == 'bundle';
    final data = _dataLines.join('\n');
    finish();
    if (!shouldEmit) {
      return null;
    }
    if (data.isEmpty) {
      throw const OversqliteProtocolException(
        'bundle change event is missing data',
      );
    }
    final decoded = jsonDecode(data);
    if (decoded is! Map) {
      throw const OversqliteProtocolException(
        'bundle change event data must be a JSON object',
      );
    }
    return BundleChangeEvent.fromJson(decoded.cast<String, Object?>());
  }
}

int _readInt(Map<String, Object?> json, String key) {
  final value = json[key];
  if (value is int) {
    return value;
  }
  return 0;
}

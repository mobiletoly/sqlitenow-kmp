import 'dart:convert';

import 'config.dart';
import 'protocol_models.dart';
import 'wire_int64.dart';

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
    required this.defaultBytesPerSnapshotChunk,
    required this.maxBytesPerSnapshotChunk,
    required this.maxBytesPerSnapshotRow,
    this.snapshotMaterializationBatchRows = 0,
    this.snapshotMaterializationBatchBytes = 0,
    required this.maxConcurrentSnapshotBuilds,
    required this.maxConcurrentSnapshotChunkRequests,
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
  final int defaultBytesPerSnapshotChunk;
  final int maxBytesPerSnapshotChunk;
  final int maxBytesPerSnapshotRow;
  final int snapshotMaterializationBatchRows;
  final int snapshotMaterializationBatchBytes;
  final int maxConcurrentSnapshotBuilds;
  final int maxConcurrentSnapshotChunkRequests;
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
      defaultRowsPerSnapshotChunk: _readRequiredInt(
        json,
        'default_rows_per_snapshot_chunk',
      ),
      maxRowsPerSnapshotChunk: _readRequiredInt(
        json,
        'max_rows_per_snapshot_chunk',
      ),
      snapshotSessionTtlSeconds: _readInt(json, 'snapshot_session_ttl_seconds'),
      maxRowsPerSnapshotSession: _readInt(
        json,
        'max_rows_per_snapshot_session',
      ),
      maxBytesPerSnapshotSession: _readInt(
        json,
        'max_bytes_per_snapshot_session',
      ),
      defaultBytesPerSnapshotChunk: _readRequiredInt(
        json,
        'default_bytes_per_snapshot_chunk',
      ),
      maxBytesPerSnapshotChunk: _readRequiredInt(
        json,
        'max_bytes_per_snapshot_chunk',
      ),
      maxBytesPerSnapshotRow: _readRequiredInt(
        json,
        'max_bytes_per_snapshot_row',
      ),
      snapshotMaterializationBatchRows: _readInt(
        json,
        'snapshot_materialization_batch_rows',
      ),
      snapshotMaterializationBatchBytes: _readInt(
        json,
        'snapshot_materialization_batch_bytes',
      ),
      maxConcurrentSnapshotBuilds: _readRequiredInt(
        json,
        'max_concurrent_snapshot_builds',
      ),
      maxConcurrentSnapshotChunkRequests: _readRequiredInt(
        json,
        'max_concurrent_snapshot_chunk_requests',
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
    required this.protocolVersion,
    required this.schemaVersion,
    required this.features,
    required this.bundleLimits,
  });

  final String protocolVersion;
  final int schemaVersion;
  final Map<String, bool> features;
  final BundleCapabilitiesLimits bundleLimits;

  bool get connectLifecycleSupported => features['connect_lifecycle'] ?? false;

  bool get bundleChangeWatchSupported =>
      features['bundle_change_watch'] ?? false;

  factory CapabilitiesResponse.fromJson(Map<String, Object?> json) {
    final protocolVersion = json['protocol_version'];
    final rawFeatures = json['features'];
    final rawLimits = json['bundle_limits'];
    if (protocolVersion is! String ||
        rawFeatures is! Map ||
        rawLimits is! Map) {
      throw const SnapshotCapabilitiesException(
        'capabilities response is missing required protocol or limit fields',
      );
    }
    return CapabilitiesResponse(
      protocolVersion: protocolVersion,
      schemaVersion: readWireOversqliteInt64(
        json,
        'schema_version',
        required: true,
        error: SnapshotCapabilitiesException.new,
      ),
      features: {
        for (final entry in rawFeatures.entries)
          entry.key.toString(): entry.value == true,
      },
      bundleLimits: BundleCapabilitiesLimits.fromJson(
        rawLimits.cast<String, Object?>(),
      ),
    );
  }
}

CapabilitiesResponse requireOversqliteProtocol(
  CapabilitiesResponse capabilities,
) {
  if (capabilities.protocolVersion != oversqliteProtocolVersion) {
    throw ProtocolVersionMismatchException(
      actual: capabilities.protocolVersion,
    );
  }
  return capabilities;
}

final class SnapshotNegotiation {
  const SnapshotNegotiation({
    required this.maxRows,
    required this.maxBytes,
    required this.maxRowBytes,
  });

  final int maxRows;
  final int maxBytes;
  final int maxRowBytes;
}

SnapshotNegotiation negotiateSnapshotLimits(
  CapabilitiesResponse capabilities,
  OversqliteConfig config,
) {
  requireOversqliteProtocol(capabilities);
  final limits = capabilities.bundleLimits;
  if (limits.defaultRowsPerSnapshotChunk <= 0 ||
      limits.maxRowsPerSnapshotChunk <= 0) {
    throw const SnapshotCapabilitiesException(
      'snapshot capabilities require positive default_rows_per_snapshot_chunk and max_rows_per_snapshot_chunk',
    );
  }
  if (limits.defaultRowsPerSnapshotChunk > limits.maxRowsPerSnapshotChunk) {
    throw const SnapshotCapabilitiesException(
      'snapshot capability default_rows_per_snapshot_chunk exceeds max_rows_per_snapshot_chunk',
    );
  }
  if (limits.defaultBytesPerSnapshotChunk <= 0 ||
      limits.maxBytesPerSnapshotChunk <= 0 ||
      limits.maxBytesPerSnapshotRow <= 0) {
    throw const SnapshotCapabilitiesException(
      'snapshot capabilities require positive default/max chunk byte and max row byte limits',
    );
  }
  if (limits.defaultBytesPerSnapshotChunk > limits.maxBytesPerSnapshotChunk) {
    throw const SnapshotCapabilitiesException(
      'snapshot capability default_bytes_per_snapshot_chunk exceeds max_bytes_per_snapshot_chunk',
    );
  }
  if (limits.maxBytesPerSnapshotRow > limits.maxBytesPerSnapshotChunk) {
    throw const SnapshotCapabilitiesException(
      'snapshot capability max_bytes_per_snapshot_row exceeds max_bytes_per_snapshot_chunk',
    );
  }
  if (limits.maxConcurrentSnapshotBuilds <= 0 ||
      limits.maxConcurrentSnapshotChunkRequests <= 0) {
    throw const SnapshotCapabilitiesException(
      'snapshot capabilities require positive max_concurrent_snapshot_builds and max_concurrent_snapshot_chunk_requests',
    );
  }
  final effectiveRows =
      config.snapshotChunkRows < limits.maxRowsPerSnapshotChunk
      ? config.snapshotChunkRows
      : limits.maxRowsPerSnapshotChunk;
  final effectiveBytes =
      config.snapshotChunkBytes < limits.maxBytesPerSnapshotChunk
      ? config.snapshotChunkBytes
      : limits.maxBytesPerSnapshotChunk;
  if (effectiveBytes < limits.maxBytesPerSnapshotRow) {
    throw const SnapshotCapabilitiesException(
      'effective snapshot chunk byte budget is below server max_bytes_per_snapshot_row; increase snapshotChunkBytes',
    );
  }
  if (config.snapshotApplyBatchBytes < limits.maxBytesPerSnapshotRow) {
    throw const SnapshotCapabilitiesException(
      'snapshot apply byte budget is below server max_bytes_per_snapshot_row; increase snapshotApplyBatchBytes',
    );
  }
  return SnapshotNegotiation(
    maxRows: effectiveRows,
    maxBytes: effectiveBytes,
    maxRowBytes: limits.maxBytesPerSnapshotRow,
  );
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
  return readWireOversqliteInt64(
    json,
    key,
    required: false,
    error: SnapshotCapabilitiesException.new,
  );
}

int _readRequiredInt(Map<String, Object?> json, String key) {
  return readWireOversqliteInt64(
    json,
    key,
    required: true,
    error: SnapshotCapabilitiesException.new,
  );
}

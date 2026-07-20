final class SnapshotRestoreDiagnostics {
  const SnapshotRestoreDiagnostics({
    this.sessionCount = 0,
    this.fetchedChunkCount = 0,
    this.maxValidatedChunkRows = 0,
    this.maxDeclaredChunkBytes = 0,
    this.maxCompletelyDecodedBodyBytes = 0,
    this.capacityResponseCount = 0,
    this.capacityRetryCount = 0,
    this.capacityWait = Duration.zero,
    this.restoreAttemptCount = 0,
    this.applyPageCount = 0,
    this.maxApplyPageRows = 0,
    this.maxApplyPageBytes = 0,
    this.maxApplyMetadataRows = 0,
    this.maxApplyDriverRows = 0,
    this.maxApplyDecodedRows = 0,
    this.stagedRowCount = 0,
    this.appliedRowCount = 0,
    this.restoreDuration = Duration.zero,
  });

  final int sessionCount;
  final int fetchedChunkCount;
  final int maxValidatedChunkRows;
  final int maxDeclaredChunkBytes;
  final int maxCompletelyDecodedBodyBytes;
  final int capacityResponseCount;
  final int capacityRetryCount;
  final Duration capacityWait;
  final int restoreAttemptCount;
  final int applyPageCount;
  final int maxApplyPageRows;
  final int maxApplyPageBytes;
  final int maxApplyMetadataRows;
  final int maxApplyDriverRows;
  final int maxApplyDecodedRows;
  final int stagedRowCount;
  final int appliedRowCount;
  final Duration restoreDuration;
}

final class SnapshotDiagnosticsRecorder {
  int _sessionCount = 0;
  int _fetchedChunkCount = 0;
  int _maxValidatedChunkRows = 0;
  int _maxDeclaredChunkBytes = 0;
  int _maxCompletelyDecodedBodyBytes = 0;
  int _capacityResponseCount = 0;
  int _capacityRetryCount = 0;
  Duration _capacityWait = Duration.zero;
  int _restoreAttemptCount = 0;
  int _applyPageCount = 0;
  int _maxApplyPageRows = 0;
  int _maxApplyPageBytes = 0;
  int _maxApplyMetadataRows = 0;
  int _maxApplyDriverRows = 0;
  int _maxApplyDecodedRows = 0;
  int _stagedRowCount = 0;
  int _appliedRowCount = 0;
  Duration _restoreDuration = Duration.zero;

  void markRestoreAttempt() => _restoreAttemptCount++;

  void recordApplyPage({
    required int rows,
    required int retainedTextBytes,
    required int metadataRows,
    required int driverRows,
    required int decodedRows,
  }) {
    _applyPageCount++;
    if (rows > _maxApplyPageRows) _maxApplyPageRows = rows;
    if (retainedTextBytes > _maxApplyPageBytes) {
      _maxApplyPageBytes = retainedTextBytes;
    }
    if (metadataRows > _maxApplyMetadataRows) {
      _maxApplyMetadataRows = metadataRows;
    }
    if (driverRows > _maxApplyDriverRows) _maxApplyDriverRows = driverRows;
    if (decodedRows > _maxApplyDecodedRows) {
      _maxApplyDecodedRows = decodedRows;
    }
  }

  void recordStagedRows(int rows) => _stagedRowCount = rows;

  void recordAppliedRows(int rows) => _appliedRowCount = rows;

  void recordRestoreDuration(Duration duration) {
    if (duration >= Duration.zero) _restoreDuration = duration;
  }

  void recordSession() => _sessionCount++;

  void recordCompletelyDecodedBody(int decodedBytes) {
    if (decodedBytes < 0) {
      throw ArgumentError.value(decodedBytes, 'decodedBytes');
    }
    if (decodedBytes > _maxCompletelyDecodedBodyBytes) {
      _maxCompletelyDecodedBodyBytes = decodedBytes;
    }
  }

  void recordValidatedChunk(int rows, int declaredBytes) {
    _fetchedChunkCount++;
    if (rows > _maxValidatedChunkRows) _maxValidatedChunkRows = rows;
    if (declaredBytes > _maxDeclaredChunkBytes) {
      _maxDeclaredChunkBytes = declaredBytes;
    }
  }

  void recordCapacityResponse() => _capacityResponseCount++;

  void recordCapacityRetry() => _capacityRetryCount++;

  void recordCapacityWait(Duration duration) {
    if (duration > Duration.zero) _capacityWait += duration;
  }

  SnapshotRestoreDiagnostics snapshot() => SnapshotRestoreDiagnostics(
    sessionCount: _sessionCount,
    fetchedChunkCount: _fetchedChunkCount,
    maxValidatedChunkRows: _maxValidatedChunkRows,
    maxDeclaredChunkBytes: _maxDeclaredChunkBytes,
    maxCompletelyDecodedBodyBytes: _maxCompletelyDecodedBodyBytes,
    capacityResponseCount: _capacityResponseCount,
    capacityRetryCount: _capacityRetryCount,
    capacityWait: _capacityWait,
    restoreAttemptCount: _restoreAttemptCount,
    applyPageCount: _applyPageCount,
    maxApplyPageRows: _maxApplyPageRows,
    maxApplyPageBytes: _maxApplyPageBytes,
    maxApplyMetadataRows: _maxApplyMetadataRows,
    maxApplyDriverRows: _maxApplyDriverRows,
    maxApplyDecodedRows: _maxApplyDecodedRows,
    stagedRowCount: _stagedRowCount,
    appliedRowCount: _appliedRowCount,
    restoreDuration: _restoreDuration,
  );
}

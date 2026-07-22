import 'dart:convert';
import 'dart:io';

import 'package:crypto/crypto.dart';
import 'package:test/test.dart';

import 'support/snapshot_memory_matrix.dart';
import 'support/snapshot_memory_reverify.dart';

void main() {
  group('Phase 4C heavy-lane policy', () {
    test(
      'snapshot memory entrypoint fails before setup in GitHub Actions',
      () async {
        final root = _repoRoot();
        final artifactPath =
            '${Directory.systemTemp.path}/oversqlite-guard-$pid-${DateTime.now().microsecondsSinceEpoch}';
        final result = await Process.run(
          'bash',
          [
            '${root.path}/dart/scripts/oversqlite_snapshot_memory.sh',
            artifactPath,
          ],
          environment: {...Platform.environment, 'GITHUB_ACTIONS': 'true'},
        );

        expect(result.exitCode, 64);
        expect(result.stderr, contains(githubActionsGuardMessage));
        expect(Directory(artifactPath).existsSync(), isFalse);
      },
    );

    test(
      'realserver heavy entrypoint fails before setup in GitHub Actions',
      () async {
        final root = _repoRoot();
        final result = await Process.run(
          'bash',
          ['${root.path}/dart/scripts/oversqlite_realserver_all_heavy.sh'],
          environment: {...Platform.environment, 'GITHUB_ACTIONS': 'true'},
        );

        expect(result.exitCode, 64);
        expect(
          result.stderr,
          contains(
            'Oversqlite realserver heavy workloads are local-only and must not run in GitHub Actions.',
          ),
        );
      },
    );

    test('base realserver script cannot bypass the heavy guard', () async {
      final root = _repoRoot();
      final result = await Process.run(
        'bash',
        ['${root.path}/dart/scripts/oversqlite_realserver_all.sh'],
        environment: {
          ...Platform.environment,
          'GITHUB_ACTIONS': 'true',
          'OVERSQLITE_REALSERVER_HEAVY': 'true',
        },
      );

      expect(result.exitCode, 64);
      expect(
        result.stderr,
        contains(
          'Oversqlite realserver heavy workloads are local-only and must not run in GitHub Actions.',
        ),
      );
    });

    test('active workflows do not opt into local-heavy entrypoints', () {
      final workflowDirectory = Directory(
        '${_repoRoot().path}/.github/workflows',
      );
      final workflows =
          workflowDirectory
              .listSync()
              .whereType<File>()
              .where(
                (file) =>
                    file.path.endsWith('.yml') || file.path.endsWith('.yaml'),
              )
              .toList()
            ..sort((left, right) => left.path.compareTo(right.path));
      expect(workflows, isNotEmpty);
      const forbidden = [
        'OVERSQLITE_REALSERVER_HEAVY',
        'OVERSQLITE_MEMORY_BASELINE_ROWS',
        'oversqlite_realserver_all_heavy.sh',
        'oversqlite_snapshot_memory.sh',
        'test/support/snapshot_memory_matrix.dart',
        'oversqliteRealserverJvmHeavy',
        'oversqliteRealserverAllHeavy',
        'oversqliteRealserverAndroidHeavy',
        'oversqliteRealserverJvmHarnessHeavy',
        'oversqliteRealserverIosSimulatorArm64Heavy',
        'oversqliteRealserverMacosArm64Heavy',
        'oversqliteRealserverJsNodeHeavy',
        'oversqliteRealserverWasmBrowserHeavy',
        'jvmRealServerSharedConnectionStress',
        'jvmRealServerSharedStress',
        'oversqliteSnapshotMemorySamplerControl',
        'oversqliteSnapshotMemory10k256',
        'oversqliteSnapshotMemory10k1k',
        'oversqliteSnapshotMemory100k256Warmup',
        'oversqliteSnapshotMemory100k256Run1',
        'oversqliteSnapshotMemory100k256Run2',
        'oversqliteSnapshotMemory100k256Run3',
        'oversqliteSnapshotMemory100k1k',
        'oversqliteSnapshotMemory1m256',
        'verifyOversqliteSnapshotMemoryResults',
        'oversqliteSnapshotMemoryJvmLocalHeavy',
      ];
      for (final workflow in workflows) {
        final source = workflow.readAsStringSync();
        for (final token in forbidden) {
          expect(
            source,
            isNot(contains(token)),
            reason: '${workflow.path} must not opt into $token',
          );
        }
      }
    });
  });

  test('snapshot memory verifier enforces completeness and resource gates', () {
    final accepted = _acceptedResults();
    expect(verifySnapshotMemoryResults(accepted)['status'], 'pass');

    expect(
      () => verifySnapshotMemoryResults(accepted.sublist(1)),
      throwsStateError,
      reason: 'missing profiles must fail closed',
    );
    expect(
      () => verifySnapshotMemoryResults([...accepted, accepted.first]),
      throwsStateError,
      reason: 'duplicate profiles must fail closed',
    );
    for (final osPeakRss in [
      dartClientRssCeilingBytes,
      dartClientRssCeilingBytes + 1,
    ]) {
      expect(
        () => verifySnapshotMemoryResults(
          _replace(
            accepted,
            'measured-1m-256',
            (result) => _copy(result, vmMaxRssBytes: osPeakRss),
          ),
        ),
        throwsStateError,
        reason: 'OS high-water RSS at or above the ceiling must fail',
      );
    }
    expect(
      () => verifySnapshotMemoryResults(
        _replace(
          accepted,
          'representative-10k-256',
          (result) => _copy(result, vmMaxRssBytes: result.peakRssBytes - 1),
        ),
      ),
      throwsStateError,
      reason: 'OS high-water RSS below the sampled peak must fail',
    );
    expect(
      () => verifySnapshotMemoryResults(
        _replace(
          accepted,
          'measured-100k-256-1',
          (result) => _copy(
            result,
            peakRssBytes: result.baselineRssBytes + 4 * 1024 * 1024,
            adjustedRssBytes: 4 * 1024 * 1024,
            vmMaxRssBytes: result.baselineRssBytes + 8 * 1024 * 1024,
          ),
        ),
      ),
      throwsStateError,
      reason: 'OS high-water RSS scaling overflow must fail',
    );
    expect(
      () => verifySnapshotMemoryResults(
        _replace(
          accepted,
          'measured-1m-256',
          (result) => _copy(
            result,
            peakHeapBytes: 200 * 1024 * 1024,
            adjustedHeapBytes: 200 * 1024 * 1024 - result.baselineHeapBytes,
          ),
        ),
      ),
      throwsStateError,
      reason: 'heap scaling overflow must fail',
    );
    expect(
      () => verifySnapshotMemoryResults(
        _replace(
          accepted,
          'representative-10k-256',
          (result) => _copy(result, serverPid: result.clientPid),
        ),
      ),
      throwsStateError,
      reason: 'client/server PID identity must fail',
    );
  });

  test('snapshot memory evidence requires a valid OS high-water RSS', () {
    final profile = snapshotMemoryProfiles.first;
    final result = _acceptedResults().first;
    final client = _clientEvidence(result);
    final server = <String, Object?>{'pid': result.serverPid};

    client.remove('vm_max_rss_bytes');
    expect(
      () => SnapshotMemoryResult.fromEvidence(
        profile: profile,
        client: client,
        server: server,
      ),
      throwsStateError,
    );

    client['vm_max_rss_bytes'] = 'not-an-integer';
    expect(
      () => SnapshotMemoryResult.fromEvidence(
        profile: profile,
        client: client,
        server: server,
      ),
      throwsStateError,
    );
  });

  test('retained Phase 4C OS high-water values pass corrected gates', () {
    var retained = _acceptedResults();
    retained = _replace(
      retained,
      'measured-100k-256-1',
      (result) => _copy(
        result,
        baselineHeapBytes: 18820144,
        peakHeapBytes: 66083472,
        adjustedHeapBytes: 47263328,
        baselineRssBytes: 77807616,
        peakRssBytes: 149651456,
        adjustedRssBytes: 71843840,
        vmMaxRssBytes: 201998336,
      ),
    );
    retained = _replace(
      retained,
      'measured-100k-256-2',
      (result) => _copy(
        result,
        baselineHeapBytes: 18817392,
        peakHeapBytes: 68160064,
        adjustedHeapBytes: 49342672,
        baselineRssBytes: 77398016,
        peakRssBytes: 151715840,
        adjustedRssBytes: 74317824,
        vmMaxRssBytes: 202752000,
      ),
    );
    retained = _replace(
      retained,
      'measured-100k-256-3',
      (result) => _copy(
        result,
        baselineHeapBytes: 18817312,
        peakHeapBytes: 67019712,
        adjustedHeapBytes: 48202400,
        baselineRssBytes: 77414400,
        peakRssBytes: 151748608,
        adjustedRssBytes: 74334208,
        vmMaxRssBytes: 202326016,
      ),
    );
    retained = _replace(
      retained,
      'measured-1m-256',
      (result) => _copy(
        result,
        baselineHeapBytes: 18817296,
        peakHeapBytes: 68376368,
        adjustedHeapBytes: 49559072,
        baselineRssBytes: 77332480,
        peakRssBytes: 154501120,
        adjustedRssBytes: 77168640,
        vmMaxRssBytes: 234389504,
      ),
    );

    final gates = verifySnapshotMemoryResults(retained);
    expect(gates['million_os_peak_rss_bytes'], 234389504);
    expect(gates['conservative_100k_os_adjusted_rss_bytes'], 124190720);
    expect(gates['million_os_adjusted_rss_bytes'], 157057024);
    expect(gates['os_rss_scaling_limit_bytes'], 315490304);
    expect(gates['million_adjusted_heap_bytes'], 49559072);
  });

  test(
    'retained artifact verifier binds hashes and refuses overwrite',
    () async {
      final directory = await Directory.systemTemp.createTemp(
        'oversqlite-retained-reverify-',
      );
      addTearDown(() async {
        if (directory.existsSync()) await directory.delete(recursive: true);
      });
      final manifestFile = File('${directory.path}/manifest.json');
      final checksumsFile = File('${directory.path}/SHA256SUMS');
      final results = _acceptedResults();
      await manifestFile.writeAsString(
        '${jsonEncode({
          'event': 'snapshot_memory_matrix_result',
          'status': 'pass',
          'profiles': [for (final result in results) _profileEvidence(result)],
        })}\n',
      );
      await checksumsFile.writeAsString('original retained checksum index\n');
      final manifestSha = sha256
          .convert(manifestFile.readAsBytesSync())
          .toString();
      final checksumsSha = sha256
          .convert(checksumsFile.readAsBytesSync())
          .toString();
      final output = File('${directory.path}/rss-gate-reverification.json');

      final verified = await reverifySnapshotMemoryArtifact(
        artifactDirectory: directory,
        expectedManifestSha256: manifestSha,
        expectedOriginalChecksumsSha256: checksumsSha,
        outputFile: output,
      );
      expect(verified['status'], 'pass');
      expect(output.existsSync(), isTrue);
      expect(
        () => reverifySnapshotMemoryArtifact(
          artifactDirectory: directory,
          expectedManifestSha256: manifestSha,
          expectedOriginalChecksumsSha256: checksumsSha,
          outputFile: output,
        ),
        throwsStateError,
        reason: 'retained reverification output must not be overwritten',
      );
      expect(
        () => reverifySnapshotMemoryArtifact(
          artifactDirectory: directory,
          expectedManifestSha256: List.filled(64, '0').join(),
          expectedOriginalChecksumsSha256: checksumsSha,
          outputFile: File('${directory.path}/wrong-hash.json'),
        ),
        throwsStateError,
        reason: 'a different retained manifest must fail closed',
      );
      expect(
        () => reverifySnapshotMemoryArtifact(
          artifactDirectory: directory,
          expectedManifestSha256: manifestSha,
          expectedOriginalChecksumsSha256: checksumsSha,
          outputFile: File('${directory.path}/../escaped-output.json'),
        ),
        throwsArgumentError,
        reason: 'lexical parent traversal must not escape the artifact root',
      );
    },
  );

  test('Dart staged apply has no complete-snapshot loader', () {
    final source = File(
      '${_repoRoot().path}/dart/packages/sqlitenow_oversqlite/lib/src/download_stage_store.dart',
    ).readAsStringSync();
    expect(
      RegExp(
        r'Future<List<OversqliteStagedSnapshotRow>>\s+loadStagedSnapshotRows',
      ).hasMatch(source),
      isFalse,
    );
    expect(source, contains('LIMIT ?'));
    expect(source, contains('row_ordinal > ?'));
    expect(source, contains('row_ordinal <= ?'));
  });
}

List<SnapshotMemoryResult> _acceptedResults() {
  return [
    for (var index = 0; index < snapshotMemoryProfiles.length; index++)
      () {
        final profile = snapshotMemoryProfiles[index];
        final million = profile.label == 'measured-1m-256';
        final baselineHeap = 32 * 1024 * 1024;
        final peakHeap = (million ? 80 : 64) * 1024 * 1024;
        final baselineRss = 128 * 1024 * 1024;
        final peakRss = (million ? 224 : 192) * 1024 * 1024;
        final vmMaxRss = (million ? 240 : 200) * 1024 * 1024;
        return SnapshotMemoryResult(
          label: profile.label,
          rowCount: profile.rowCount,
          targetRowBytes: profile.targetRowBytes,
          clientPid: 1000 + index,
          serverPid: 2000 + index,
          baselineHeapBytes: baselineHeap,
          peakHeapBytes: peakHeap,
          adjustedHeapBytes: peakHeap - baselineHeap,
          baselineRssBytes: baselineRss,
          peakRssBytes: peakRss,
          adjustedRssBytes: peakRss - baselineRss,
          vmMaxRssBytes: vmMaxRss,
        );
      }(),
  ];
}

List<SnapshotMemoryResult> _replace(
  List<SnapshotMemoryResult> results,
  String label,
  SnapshotMemoryResult Function(SnapshotMemoryResult) replacement,
) {
  return [
    for (final result in results)
      if (result.label == label) replacement(result) else result,
  ];
}

SnapshotMemoryResult _copy(
  SnapshotMemoryResult result, {
  int? clientPid,
  int? serverPid,
  int? baselineHeapBytes,
  int? peakHeapBytes,
  int? adjustedHeapBytes,
  int? baselineRssBytes,
  int? peakRssBytes,
  int? adjustedRssBytes,
  int? vmMaxRssBytes,
}) {
  return SnapshotMemoryResult(
    label: result.label,
    rowCount: result.rowCount,
    targetRowBytes: result.targetRowBytes,
    clientPid: clientPid ?? result.clientPid,
    serverPid: serverPid ?? result.serverPid,
    baselineHeapBytes: baselineHeapBytes ?? result.baselineHeapBytes,
    peakHeapBytes: peakHeapBytes ?? result.peakHeapBytes,
    adjustedHeapBytes: adjustedHeapBytes ?? result.adjustedHeapBytes,
    baselineRssBytes: baselineRssBytes ?? result.baselineRssBytes,
    peakRssBytes: peakRssBytes ?? result.peakRssBytes,
    adjustedRssBytes: adjustedRssBytes ?? result.adjustedRssBytes,
    vmMaxRssBytes: vmMaxRssBytes ?? result.vmMaxRssBytes,
  );
}

Map<String, Object?> _profileEvidence(SnapshotMemoryResult result) {
  return {
    'label': result.label,
    'client': _clientEvidence(result),
    'server': {'pid': result.serverPid},
  };
}

Map<String, Object?> _clientEvidence(SnapshotMemoryResult result) {
  return {
    'rows': result.rowCount,
    'target_row_bytes': result.targetRowBytes,
    'pid': result.clientPid,
    'baseline_heap_bytes': result.baselineHeapBytes,
    'peak_heap_bytes': result.peakHeapBytes,
    'adjusted_heap_bytes': result.adjustedHeapBytes,
    'baseline_rss_bytes': result.baselineRssBytes,
    'peak_rss_bytes': result.peakRssBytes,
    'adjusted_rss_bytes': result.adjustedRssBytes,
    'vm_max_rss_bytes': result.vmMaxRssBytes,
  };
}

Directory _repoRoot() {
  var current = Directory.current.absolute;
  while (true) {
    if (File('${current.path}/settings.gradle.kts').existsSync()) {
      return current;
    }
    if (current.parent.path == current.path) {
      throw StateError(
        'repository root not found from ${Directory.current.path}',
      );
    }
    current = current.parent;
  }
}

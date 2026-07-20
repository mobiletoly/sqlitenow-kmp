import 'dart:async';
import 'dart:convert';
import 'dart:io';

const githubActionsGuardMessage =
    'Phase 4C snapshot memory workloads are local-only and must not run in GitHub Actions.';
const dartClientRssCeilingBytes = 256 * 1024 * 1024;
const heapScalingAllowanceBytes = 32 * 1024 * 1024;
const rssScalingAllowanceBytes = 64 * 1024 * 1024;

const snapshotMemoryProfiles = <SnapshotMemoryProfile>[
  SnapshotMemoryProfile('representative-10k-256', 10000, 256),
  SnapshotMemoryProfile('representative-10k-1k', 10000, 1024),
  SnapshotMemoryProfile('warmup-100k-256', 100000, 256),
  SnapshotMemoryProfile('measured-100k-256-1', 100000, 256),
  SnapshotMemoryProfile('measured-100k-256-2', 100000, 256),
  SnapshotMemoryProfile('measured-100k-256-3', 100000, 256),
  SnapshotMemoryProfile('measured-100k-1k', 100000, 1024),
  SnapshotMemoryProfile('measured-1m-256', 1000000, 256),
];

Future<void> main(List<String> arguments) async {
  if (Platform.environment['GITHUB_ACTIONS'] == 'true') {
    stderr.writeln(githubActionsGuardMessage);
    exitCode = 64;
    return;
  }

  final artifactDirectory = _artifactDirectory(arguments);
  if (artifactDirectory.existsSync() &&
      artifactDirectory.listSync().isNotEmpty) {
    throw StateError(
      'artifact directory must be absent or empty: ${artifactDirectory.path}',
    );
  }
  artifactDirectory.createSync(recursive: true);
  final clientKernel = await _compileClientKernel(artifactDirectory);

  final results = <SnapshotMemoryResult>[];
  final profiles = <Map<String, Object?>>[];
  for (final profile in snapshotMemoryProfiles) {
    final run = await _runProfile(profile, artifactDirectory, clientKernel);
    results.add(run.result);
    profiles.add(run.evidence);
    stdout.writeln(
      jsonEncode({
        'event': 'snapshot_memory_profile_complete',
        ...run.evidence,
      }),
    );
  }

  final gate = verifySnapshotMemoryResults(results);
  final manifest = <String, Object?>{
    'event': 'snapshot_memory_matrix_result',
    'status': 'pass',
    'runtime': 'dart-vm',
    'dart_version': Platform.version,
    'operating_system': Platform.operatingSystem,
    'operating_system_version': Platform.operatingSystemVersion,
    'local_hostname': Platform.localHostname,
    'processor_count': Platform.numberOfProcessors,
    'artifact_directory': artifactDirectory.absolute.path,
    'client_kernel': clientKernel.absolute.path,
    'client_kernel_compile_command': [
      Platform.resolvedExecutable,
      'compile',
      'kernel',
      '-o',
      clientKernel.absolute.path,
      'packages/sqlitenow_oversqlite/test/support/snapshot_memory_client.dart',
    ],
    'profiles': profiles,
    'gates': gate,
  };
  final manifestFile = File('${artifactDirectory.path}/manifest.json');
  await manifestFile.writeAsString(
    '${const JsonEncoder.withIndent('  ').convert(manifest)}\n',
    flush: true,
  );
  stdout.writeln(jsonEncode(manifest));
}

Directory _artifactDirectory(List<String> arguments) {
  if (arguments.length != 2 || arguments.first != '--artifact-dir') {
    throw ArgumentError('expected --artifact-dir PATH');
  }
  if (arguments[1].isEmpty) throw ArgumentError('artifact path is empty');
  return Directory(arguments[1]);
}

Future<File> _compileClientKernel(Directory artifactDirectory) async {
  const source =
      'packages/sqlitenow_oversqlite/test/support/snapshot_memory_client.dart';
  final output = File('${artifactDirectory.path}/snapshot_memory_client.dill');
  final arguments = ['compile', 'kernel', '-o', output.absolute.path, source];
  final process = await Process.run(
    Platform.resolvedExecutable,
    arguments,
    workingDirectory: Directory.current.path,
  );
  await File(
    '${artifactDirectory.path}/client-kernel-compile.stdout.log',
  ).writeAsString('${process.stdout}', flush: true);
  await File(
    '${artifactDirectory.path}/client-kernel-compile.stderr.log',
  ).writeAsString('${process.stderr}', flush: true);
  if (process.exitCode != 0 || !output.existsSync()) {
    throw StateError('failed to compile the snapshot memory client kernel');
  }
  stdout.writeln(
    jsonEncode({
      'event': 'snapshot_memory_client_kernel_ready',
      'command': [Platform.resolvedExecutable, ...arguments],
      'path': output.absolute.path,
      'bytes': output.lengthSync(),
    }),
  );
  return output;
}

Future<({SnapshotMemoryResult result, Map<String, Object?> evidence})>
_runProfile(
  SnapshotMemoryProfile profile,
  Directory artifactDirectory,
  File clientKernel,
) async {
  final profileDirectory = Directory(
    '${artifactDirectory.path}/${profile.label}',
  )..createSync();
  final serverStdoutFile = File('${profileDirectory.path}/server.stdout.log');
  final serverStderrFile = File('${profileDirectory.path}/server.stderr.log');
  final clientStdoutFile = File('${profileDirectory.path}/client.stdout.log');
  final clientStderrFile = File('${profileDirectory.path}/client.stderr.log');
  final clientResultFile = File('${profileDirectory.path}/client-result.json');
  final databaseFile = File('${profileDirectory.path}/client.sqlite');
  const serverProgram =
      'packages/sqlitenow_oversqlite/test/support/snapshot_memory_fixture_server.dart';
  final serverArguments = [
    'run',
    serverProgram,
    '--rows',
    '${profile.rowCount}',
    '--target-row-bytes',
    '${profile.targetRowBytes}',
  ];
  final server = await Process.start(
    Platform.resolvedExecutable,
    serverArguments,
    workingDirectory: Directory.current.path,
  );
  final serverStdoutSink = serverStdoutFile.openWrite();
  final serverIterator = StreamIterator<String>(
    server.stdout.transform(utf8.decoder).transform(const LineSplitter()).map((
      line,
    ) {
      serverStdoutSink.writeln(line);
      return line;
    }),
  );
  final serverStderrFuture = _captureLines(server.stderr, serverStderrFile);

  Process? client;
  try {
    final ready = await _nextEvent(
      serverIterator,
      'snapshot_memory_server_ready',
    ).timeout(const Duration(seconds: 30));
    final port = _requiredInt(ready, 'port');
    if (_requiredInt(ready, 'pid') != server.pid) {
      throw StateError('fixture server PID evidence is inconsistent');
    }
    final clientArguments = [
      '--enable-vm-service=0',
      clientKernel.absolute.path,
      '--label',
      profile.label,
      '--rows',
      '${profile.rowCount}',
      '--target-row-bytes',
      '${profile.targetRowBytes}',
      '--base-url',
      'http://127.0.0.1:$port/',
      '--database-file',
      databaseFile.absolute.path,
      '--result-file',
      clientResultFile.absolute.path,
    ];
    client = await Process.start(
      Platform.resolvedExecutable,
      clientArguments,
      workingDirectory: Directory.current.path,
    );
    final clientStdoutFuture = _captureLines(client.stdout, clientStdoutFile);
    final clientStderrFuture = _captureLines(client.stderr, clientStderrFile);
    final clientExit = await client.exitCode.timeout(const Duration(hours: 2));
    final clientStdout = await clientStdoutFuture;
    final clientStderr = await clientStderrFuture;
    if (clientExit != 0) {
      throw StateError(
        'snapshot memory client ${profile.label} exited $clientExit: '
        '${clientStderr.join(' | ')}',
      );
    }
    final clientEvent = _eventFromLines(
      clientStdout,
      'snapshot_memory_client_result',
    );
    final serverFinal = await _nextEvent(
      serverIterator,
      'snapshot_memory_server_result',
    ).timeout(const Duration(seconds: 30));
    while (await serverIterator.moveNext()) {}
    final serverExit = await server.exitCode.timeout(
      const Duration(seconds: 30),
    );
    final serverStderr = await serverStderrFuture;
    if (serverExit != 0) {
      throw StateError(
        'snapshot fixture server ${profile.label} exited $serverExit: '
        '${serverStderr.join(' | ')}',
      );
    }
    if (_requiredInt(clientEvent, 'pid') != client.pid ||
        _requiredInt(serverFinal, 'pid') != server.pid ||
        client.pid == server.pid) {
      throw StateError('client/server PID separation evidence is invalid');
    }
    final persisted = jsonDecode(clientResultFile.readAsStringSync());
    if (persisted is! Map || jsonEncode(persisted) != jsonEncode(clientEvent)) {
      throw StateError('persisted client result differs from stdout evidence');
    }
    final result = SnapshotMemoryResult.fromEvidence(
      profile: profile,
      client: clientEvent,
      server: serverFinal,
    );
    final evidence = <String, Object?>{
      'label': profile.label,
      'rows': profile.rowCount,
      'target_row_bytes': profile.targetRowBytes,
      'client_pid': client.pid,
      'server_pid': server.pid,
      'client_command': [Platform.resolvedExecutable, ...clientArguments],
      'server_command': [Platform.resolvedExecutable, ...serverArguments],
      'client': clientEvent,
      'server': serverFinal,
      'client_stdout': clientStdoutFile.absolute.path,
      'client_stderr': clientStderrFile.absolute.path,
      'server_stdout': serverStdoutFile.absolute.path,
      'server_stderr': serverStderrFile.absolute.path,
      'client_result': clientResultFile.absolute.path,
      'database_file': databaseFile.absolute.path,
    };
    await File('${profileDirectory.path}/profile.json').writeAsString(
      '${const JsonEncoder.withIndent('  ').convert(evidence)}\n',
      flush: true,
    );
    return (result: result, evidence: evidence);
  } finally {
    if (client != null) client.kill(ProcessSignal.sigkill);
    server.kill(ProcessSignal.sigkill);
    await serverIterator.cancel();
    await serverStdoutSink.close();
  }
}

Future<List<String>> _captureLines(
  Stream<List<int>> source,
  File destination,
) async {
  final lines = <String>[];
  final sink = destination.openWrite();
  try {
    await for (final line
        in source.transform(utf8.decoder).transform(const LineSplitter())) {
      lines.add(line);
      sink.writeln(line);
    }
  } finally {
    await sink.close();
  }
  return lines;
}

Future<Map<String, Object?>> _nextEvent(
  StreamIterator<String> iterator,
  String event,
) async {
  while (await iterator.moveNext()) {
    final decoded = _tryJsonObject(iterator.current);
    if (decoded?['event'] == event) return decoded!;
  }
  throw StateError('process ended before event $event');
}

Map<String, Object?> _eventFromLines(List<String> lines, String event) {
  final matches = [
    for (final line in lines)
      if (_tryJsonObject(line)?['event'] == event) _tryJsonObject(line)!,
  ];
  if (matches.length != 1) {
    throw StateError('expected exactly one $event, found ${matches.length}');
  }
  return matches.single;
}

Map<String, Object?>? _tryJsonObject(String line) {
  try {
    final decoded = jsonDecode(line);
    return decoded is Map ? decoded.cast<String, Object?>() : null;
  } catch (_) {
    return null;
  }
}

int _requiredInt(Map<String, Object?> values, String name) {
  final value = values[name];
  if (value is! int) throw StateError('result field $name is not an integer');
  return value;
}

Map<String, Object?> verifySnapshotMemoryResults(
  List<SnapshotMemoryResult> results,
) {
  if (results.length != snapshotMemoryProfiles.length) {
    throw StateError(
      'expected ${snapshotMemoryProfiles.length} memory results, '
      'found ${results.length}',
    );
  }
  final byLabel = <String, SnapshotMemoryResult>{};
  for (final result in results) {
    if (byLabel[result.label] != null) {
      throw StateError('duplicate memory result ${result.label}');
    }
    byLabel[result.label] = result;
  }
  final expectedLabels = snapshotMemoryProfiles
      .map((profile) => profile.label)
      .toSet();
  if (byLabel.length != expectedLabels.length ||
      !expectedLabels.every(byLabel.containsKey)) {
    throw StateError('snapshot memory result labels are incomplete');
  }
  for (final profile in snapshotMemoryProfiles) {
    final result = byLabel[profile.label]!;
    if (result.rowCount != profile.rowCount ||
        result.targetRowBytes != profile.targetRowBytes) {
      throw StateError('${profile.label} has an unexpected fixture shape');
    }
    result.verifyMetrics();
  }

  final million = byLabel['measured-1m-256']!;
  if (million.osPeakRssBytes >= dartClientRssCeilingBytes) {
    throw StateError(
      'measured-1m-256 reached the 256 MiB client OS high-water RSS ceiling',
    );
  }
  final measured100k = [
    byLabel['measured-100k-256-1']!,
    byLabel['measured-100k-256-2']!,
    byLabel['measured-100k-256-3']!,
  ];
  final conservativeHeap = measured100k
      .map((result) => result.adjustedHeapBytes)
      .reduce((left, right) => left < right ? left : right);
  final conservativeOsRss = measured100k
      .map((result) => result.osAdjustedRssBytes)
      .reduce((left, right) => left < right ? left : right);
  final heapLimit = conservativeHeap * 2 + heapScalingAllowanceBytes;
  final osRssLimit = conservativeOsRss * 2 + rssScalingAllowanceBytes;
  if (million.adjustedHeapBytes > heapLimit) {
    throw StateError(
      'measured-1m-256 exceeds the conservative 2x + 32 MiB heap gate',
    );
  }
  if (million.osAdjustedRssBytes > osRssLimit) {
    throw StateError(
      'measured-1m-256 exceeds the conservative 2x + 64 MiB OS high-water RSS gate',
    );
  }
  return {
    'status': 'pass',
    'client_rss_ceiling_bytes': dartClientRssCeilingBytes,
    'million_sampled_peak_rss_bytes': million.peakRssBytes,
    'million_os_peak_rss_bytes': million.osPeakRssBytes,
    'conservative_100k_adjusted_heap_bytes': conservativeHeap,
    'million_adjusted_heap_bytes': million.adjustedHeapBytes,
    'heap_scaling_limit_bytes': heapLimit,
    'conservative_100k_os_adjusted_rss_bytes': conservativeOsRss,
    'million_os_adjusted_rss_bytes': million.osAdjustedRssBytes,
    'os_rss_scaling_limit_bytes': osRssLimit,
  };
}

final class SnapshotMemoryProfile {
  const SnapshotMemoryProfile(this.label, this.rowCount, this.targetRowBytes);

  final String label;
  final int rowCount;
  final int targetRowBytes;
}

final class SnapshotMemoryResult {
  const SnapshotMemoryResult({
    required this.label,
    required this.rowCount,
    required this.targetRowBytes,
    required this.clientPid,
    required this.serverPid,
    required this.baselineHeapBytes,
    required this.peakHeapBytes,
    required this.adjustedHeapBytes,
    required this.baselineRssBytes,
    required this.peakRssBytes,
    required this.adjustedRssBytes,
    required this.vmMaxRssBytes,
  });

  final String label;
  final int rowCount;
  final int targetRowBytes;
  final int clientPid;
  final int serverPid;
  final int baselineHeapBytes;
  final int peakHeapBytes;
  final int adjustedHeapBytes;
  final int baselineRssBytes;
  final int peakRssBytes;
  final int adjustedRssBytes;
  final int vmMaxRssBytes;

  int get osPeakRssBytes => vmMaxRssBytes;

  int get osAdjustedRssBytes => osPeakRssBytes - baselineRssBytes;

  factory SnapshotMemoryResult.fromEvidence({
    required SnapshotMemoryProfile profile,
    required Map<String, Object?> client,
    required Map<String, Object?> server,
  }) {
    return SnapshotMemoryResult(
      label: profile.label,
      rowCount: _requiredInt(client, 'rows'),
      targetRowBytes: _requiredInt(client, 'target_row_bytes'),
      clientPid: _requiredInt(client, 'pid'),
      serverPid: _requiredInt(server, 'pid'),
      baselineHeapBytes: _requiredInt(client, 'baseline_heap_bytes'),
      peakHeapBytes: _requiredInt(client, 'peak_heap_bytes'),
      adjustedHeapBytes: _requiredInt(client, 'adjusted_heap_bytes'),
      baselineRssBytes: _requiredInt(client, 'baseline_rss_bytes'),
      peakRssBytes: _requiredInt(client, 'peak_rss_bytes'),
      adjustedRssBytes: _requiredInt(client, 'adjusted_rss_bytes'),
      vmMaxRssBytes: _requiredInt(client, 'vm_max_rss_bytes'),
    );
  }

  void verifyMetrics() {
    if (clientPid <= 0 || serverPid <= 0 || clientPid == serverPid) {
      throw StateError('$label does not prove separate client/server PIDs');
    }
    if (baselineHeapBytes <= 0 ||
        peakHeapBytes < baselineHeapBytes ||
        adjustedHeapBytes != peakHeapBytes - baselineHeapBytes) {
      throw StateError('$label contains invalid Dart heap metrics');
    }
    if (baselineRssBytes <= 0 ||
        peakRssBytes < baselineRssBytes ||
        adjustedRssBytes != peakRssBytes - baselineRssBytes) {
      throw StateError('$label contains invalid sampled client RSS metrics');
    }
    if (vmMaxRssBytes <= 0 ||
        vmMaxRssBytes < baselineRssBytes ||
        vmMaxRssBytes < peakRssBytes) {
      throw StateError('$label contains invalid OS high-water RSS metrics');
    }
  }
}

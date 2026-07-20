import 'dart:convert';
import 'dart:io';

import 'package:crypto/crypto.dart';

import 'snapshot_memory_matrix.dart';

const snapshotMemoryRetainedReverificationEvent =
    'snapshot_memory_retained_reverification';

Future<void> main(List<String> arguments) async {
  final options = _ReverificationOptions.parse(arguments);
  final result = await reverifySnapshotMemoryArtifact(
    artifactDirectory: options.artifactDirectory,
    expectedManifestSha256: options.expectedManifestSha256,
    expectedOriginalChecksumsSha256: options.expectedOriginalChecksumsSha256,
    outputFile: options.outputFile,
  );
  stdout.writeln(jsonEncode(result));
}

Future<Map<String, Object?>> reverifySnapshotMemoryArtifact({
  required Directory artifactDirectory,
  required String expectedManifestSha256,
  required String expectedOriginalChecksumsSha256,
  required File outputFile,
}) async {
  final artifactRoot = artifactDirectory.absolute;
  if (!artifactRoot.existsSync()) {
    throw StateError('artifact directory does not exist: ${artifactRoot.path}');
  }
  final manifestFile = File('${artifactRoot.path}/manifest.json');
  final originalChecksumsFile = File('${artifactRoot.path}/SHA256SUMS');
  if (!manifestFile.existsSync()) {
    throw StateError('retained manifest does not exist: ${manifestFile.path}');
  }
  if (!originalChecksumsFile.existsSync()) {
    throw StateError(
      'original checksum index does not exist: ${originalChecksumsFile.path}',
    );
  }

  final output = outputFile.absolute;
  if (!output.parent.existsSync()) {
    throw StateError('output parent does not exist: ${output.parent.path}');
  }
  final resolvedArtifactRoot = artifactRoot.resolveSymbolicLinksSync();
  final resolvedOutputParent = output.parent.resolveSymbolicLinksSync();
  if (resolvedOutputParent != resolvedArtifactRoot) {
    throw ArgumentError(
      'output must be directly inside the retained artifact directory',
    );
  }
  final outputName = output.uri.pathSegments.last;
  if (outputName == 'manifest.json' || outputName == 'SHA256SUMS') {
    throw ArgumentError('output must not replace original retained evidence');
  }
  if (FileSystemEntity.typeSync(output.path, followLinks: false) !=
      FileSystemEntityType.notFound) {
    throw StateError('output already exists: ${output.path}');
  }

  final expectedManifest = _requiredSha256(
    expectedManifestSha256,
    'expected manifest SHA-256',
  );
  final expectedOriginalChecksums = _requiredSha256(
    expectedOriginalChecksumsSha256,
    'expected original checksum-index SHA-256',
  );
  final actualManifest = _sha256File(manifestFile);
  final actualOriginalChecksums = _sha256File(originalChecksumsFile);
  if (actualManifest != expectedManifest) {
    throw StateError(
      'retained manifest SHA-256 mismatch: expected $expectedManifest, '
      'found $actualManifest',
    );
  }
  if (actualOriginalChecksums != expectedOriginalChecksums) {
    throw StateError(
      'original checksum-index SHA-256 mismatch: expected '
      '$expectedOriginalChecksums, found $actualOriginalChecksums',
    );
  }

  final manifest = _jsonObject(manifestFile.readAsStringSync(), 'manifest');
  if (manifest['event'] != 'snapshot_memory_matrix_result' ||
      manifest['status'] != 'pass') {
    throw StateError('retained manifest is not a passing memory matrix result');
  }
  final profiles = manifest['profiles'];
  if (profiles is! List) {
    throw StateError('retained manifest profiles are not a list');
  }
  final results = <SnapshotMemoryResult>[];
  for (final entry in profiles) {
    if (entry is! Map) {
      throw StateError('retained manifest profile is not an object');
    }
    final evidence = entry.cast<String, Object?>();
    final label = evidence['label'];
    final matches = snapshotMemoryProfiles.where(
      (profile) => profile.label == label,
    );
    if (matches.length != 1) {
      throw StateError('retained manifest contains an unknown profile: $label');
    }
    results.add(
      SnapshotMemoryResult.fromEvidence(
        profile: matches.single,
        client: _requiredObject(evidence, 'client'),
        server: _requiredObject(evidence, 'server'),
      ),
    );
  }

  final gates = verifySnapshotMemoryResults(results);
  final result = <String, Object?>{
    'event': snapshotMemoryRetainedReverificationEvent,
    'status': 'pass',
    'artifact_directory': artifactRoot.path,
    'source_manifest': manifestFile.absolute.path,
    'source_manifest_sha256': actualManifest,
    'original_checksum_index': originalChecksumsFile.absolute.path,
    'original_checksum_index_sha256': actualOriginalChecksums,
    'rss_peak_source': 'vm_max_rss_bytes',
    'sampled_rss_fields_role': 'diagnostic_only',
    'gates': gates,
  };
  await output.create(exclusive: true);
  await output.writeAsString(
    '${const JsonEncoder.withIndent('  ').convert(result)}\n',
    flush: true,
  );
  return result;
}

Map<String, Object?> _jsonObject(String source, String description) {
  final decoded = jsonDecode(source);
  if (decoded is! Map) {
    throw StateError('$description is not a JSON object');
  }
  return decoded.cast<String, Object?>();
}

Map<String, Object?> _requiredObject(Map<String, Object?> values, String name) {
  final value = values[name];
  if (value is! Map) {
    throw StateError('retained manifest field $name is not an object');
  }
  return value.cast<String, Object?>();
}

String _sha256File(File file) =>
    sha256.convert(file.readAsBytesSync()).toString();

String _requiredSha256(String value, String description) {
  final normalized = value.toLowerCase();
  if (!RegExp(r'^[0-9a-f]{64}$').hasMatch(normalized)) {
    throw ArgumentError('$description must be 64 hexadecimal characters');
  }
  return normalized;
}

final class _ReverificationOptions {
  const _ReverificationOptions({
    required this.artifactDirectory,
    required this.expectedManifestSha256,
    required this.expectedOriginalChecksumsSha256,
    required this.outputFile,
  });

  final Directory artifactDirectory;
  final String expectedManifestSha256;
  final String expectedOriginalChecksumsSha256;
  final File outputFile;

  static _ReverificationOptions parse(List<String> arguments) {
    if (arguments.length != 8) {
      throw ArgumentError(
        'expected --artifact-dir PATH --expected-manifest-sha256 HASH '
        '--expected-original-checksums-sha256 HASH --output PATH',
      );
    }
    final values = <String, String>{};
    for (var index = 0; index < arguments.length; index += 2) {
      final name = arguments[index];
      final value = arguments[index + 1];
      if (!const {
        '--artifact-dir',
        '--expected-manifest-sha256',
        '--expected-original-checksums-sha256',
        '--output',
      }.contains(name)) {
        throw ArgumentError('unknown option: $name');
      }
      if (value.isEmpty || values[name] != null) {
        throw ArgumentError('option must appear once with a value: $name');
      }
      values[name] = value;
    }
    if (values.length != 4) {
      throw ArgumentError('all retained-artifact options are required');
    }
    return _ReverificationOptions(
      artifactDirectory: Directory(values['--artifact-dir']!),
      expectedManifestSha256: values['--expected-manifest-sha256']!,
      expectedOriginalChecksumsSha256:
          values['--expected-original-checksums-sha256']!,
      outputFile: File(values['--output']!),
    );
  }
}

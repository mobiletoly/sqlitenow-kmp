import 'dart:convert';
import 'dart:io';

import 'package:package_config/package_config.dart';
import 'package:path/path.dart' as p;

import 'config.dart';

final class CompilerInvocationException implements Exception {
  const CompilerInvocationException(this.message);

  final String message;

  @override
  String toString() => message;
}

final class CompilerRequest {
  const CompilerRequest({
    required this.databaseName,
    required this.sqlDirectory,
    required this.packageName,
    required this.outputDirectory,
    required this.oversqlite,
    this.schemaDatabaseFile,
  });

  final String databaseName;
  final String sqlDirectory;
  final String packageName;
  final String outputDirectory;
  final bool oversqlite;
  final String? schemaDatabaseFile;

  factory CompilerRequest.fromConfig(
    SqliteNowDatabaseConfig config,
    Directory configDirectory,
  ) {
    return CompilerRequest(
      databaseName: config.databaseName,
      sqlDirectory: config.input,
      packageName: config.packageName,
      outputDirectory: config.output,
      oversqlite: config.oversqlite,
      schemaDatabaseFile: config.schemaDatabaseFile,
    );
  }

  Map<String, Object?> toJson() {
    return {
      'databaseName': databaseName,
      'sqlDirectory': sqlDirectory,
      'packageName': packageName,
      'outputDirectory': outputDirectory,
      'schemaDatabaseFile': schemaDatabaseFile,
      'debug': false,
      'oversqlite': oversqlite,
      'backend': 'dart',
    };
  }
}

final class CompilerResult {
  const CompilerResult({
    required this.success,
    required this.generatedFiles,
    required this.warnings,
    required this.diagnostics,
    this.failureMessage,
  });

  final bool success;
  final List<String> generatedFiles;
  final List<String> warnings;
  final List<CompilerDiagnostic> diagnostics;
  final String? failureMessage;

  factory CompilerResult.fromJson(Map<String, Object?> json) {
    final failure = json['failure'];
    return CompilerResult(
      success: json['success'] == true,
      generatedFiles: [
        for (final value in (json['generatedFiles'] as List? ?? const []))
          value.toString(),
      ],
      warnings: [
        for (final value in (json['warnings'] as List? ?? const []))
          value.toString(),
      ],
      diagnostics: [
        for (final value in (json['diagnostics'] as List? ?? const []))
          CompilerDiagnostic.fromJson((value as Map).cast<String, Object?>()),
      ],
      failureMessage: failure is Map ? failure['message']?.toString() : null,
    );
  }
}

final class CompilerDiagnostic {
  const CompilerDiagnostic({required this.severity, required this.message});

  final String severity;
  final String message;

  factory CompilerDiagnostic.fromJson(Map<String, Object?> json) {
    return CompilerDiagnostic(
      severity: json['severity']?.toString() ?? 'WARNING',
      message: json['message']?.toString() ?? '',
    );
  }
}

final class JavaCheck {
  const JavaCheck({required this.available, this.message});

  final bool available;
  final String? message;
}

abstract interface class CompilerProcessRunner {
  Future<JavaCheck> checkJava(String javaExecutable);

  Future<CompilerResult> runCompiler({
    required String javaExecutable,
    required File compilerJar,
    required CompilerRequest request,
    required Directory workingDirectory,
  });
}

final class ProcessCompilerRunner implements CompilerProcessRunner {
  const ProcessCompilerRunner();

  @override
  Future<JavaCheck> checkJava(String javaExecutable) async {
    try {
      final result = await Process.run(javaExecutable, ['-version']);
      if (result.exitCode != 0) {
        return JavaCheck(
          available: false,
          message: result.stderr.toString().trim(),
        );
      }
      return const JavaCheck(available: true);
    } on ProcessException catch (error) {
      return JavaCheck(available: false, message: error.message);
    }
  }

  @override
  Future<CompilerResult> runCompiler({
    required String javaExecutable,
    required File compilerJar,
    required CompilerRequest request,
    required Directory workingDirectory,
  }) async {
    final tempDir = await Directory.systemTemp.createTemp(
      'sqlitenow-dart-cli-',
    );
    try {
      final requestFile = File(p.join(tempDir.path, 'request.json'));
      await requestFile.writeAsString(jsonEncode(request.toJson()));

      final result = await Process.run(javaExecutable, [
        '-jar',
        compilerJar.path,
        '--request',
        requestFile.path,
      ], workingDirectory: workingDirectory.path);

      final stdoutText = result.stdout.toString().trim();
      if (stdoutText.isEmpty) {
        throw CompilerInvocationException(
          'Compiler produced no JSON response. stderr: ${result.stderr}',
        );
      }

      final decoded = jsonDecode(stdoutText);
      if (decoded is! Map) {
        throw const CompilerInvocationException(
          'Compiler response was not a JSON object.',
        );
      }
      final compilerResult = CompilerResult.fromJson(
        decoded.cast<String, Object?>(),
      );
      if (result.exitCode != 0 && compilerResult.success) {
        throw CompilerInvocationException(
          'Compiler process failed with exit code ${result.exitCode}.',
        );
      }
      return compilerResult;
    } on FormatException catch (error) {
      throw CompilerInvocationException(
        'Compiler response was not valid JSON: ${error.message}',
      );
    } finally {
      await tempDir.delete(recursive: true);
    }
  }
}

Future<File?> findBundledCompilerJar(Directory workingDirectory) async {
  final packageConfig = await findPackageConfig(workingDirectory);
  final configuredPackageRoot = packageConfig?['sqlitenow_cli']?.root;
  if (configuredPackageRoot != null && configuredPackageRoot.isScheme('file')) {
    final packageCandidate = File.fromUri(
      configuredPackageRoot.resolve('lib/src/compiler/sqlitenow-compiler.jar'),
    );
    if (packageCandidate.existsSync()) return packageCandidate;
  }

  final script = Platform.script;
  if (!script.isScheme('file')) return null;

  final scriptFile = File.fromUri(script).absolute;
  final packageRoot = scriptFile.parent.parent;
  final candidate = File(
    p.join(
      packageRoot.path,
      'lib',
      'src',
      'compiler',
      'sqlitenow-compiler.jar',
    ),
  );
  if (candidate.existsSync()) return candidate;
  return null;
}

File? findDevelopmentCompilerJar(Directory start) {
  Directory? current = start.absolute;
  while (current != null) {
    final libs = Directory(
      p.join(current.path, 'sqlitenow-compiler', 'build', 'libs'),
    );
    if (libs.existsSync()) {
      final candidates =
          libs
              .listSync()
              .whereType<File>()
              .where((file) => file.path.endsWith('-compiler.jar'))
              .toList()
            ..sort(
              (a, b) => b.lastModifiedSync().compareTo(a.lastModifiedSync()),
            );
      if (candidates.isNotEmpty) return candidates.first;
    }
    final parent = current.parent;
    if (parent.path == current.path) break;
    current = parent;
  }
  return null;
}

import 'dart:io';

import 'package:args/args.dart';

import 'compiler.dart';
import 'config.dart';

final class SqliteNowDartCli {
  SqliteNowDartCli({
    CompilerProcessRunner? runner,
    IOSink? out,
    IOSink? err,
    Directory? workingDirectory,
  }) : _runner = runner ?? const ProcessCompilerRunner(),
       _out = out ?? stdout,
       _err = err ?? stderr,
       _workingDirectory = workingDirectory ?? Directory.current;

  final CompilerProcessRunner _runner;
  final IOSink _out;
  final IOSink _err;
  final Directory _workingDirectory;

  Future<int> run(List<String> arguments) async {
    final parser = _buildParser();
    late ArgResults parsed;
    try {
      parsed = parser.parse(arguments);
    } on FormatException catch (error) {
      _err.writeln(error.message);
      _err.writeln();
      _err.writeln(_usage(parser));
      return 64;
    }

    if (parsed['help'] as bool) {
      _out.writeln(_usage(parser));
      return 0;
    }

    final command = parsed.command;
    if (command?.name != 'generate') {
      _err.writeln('Missing command. Expected: generate');
      _err.writeln();
      _err.writeln(_usage(parser));
      return 64;
    }

    return _runGenerate(command!);
  }

  Future<int> _runGenerate(ArgResults command) async {
    try {
      final configPath = command['config'] as String;
      final configFile = _resolveFile(configPath);
      final config = SqliteNowCliConfig.load(configFile);
      final compilerJar = await _resolveCompilerJar(
        command['compiler-jar'] as String?,
        config.compilerJar,
      );
      final javaExecutable = command['java'] as String? ?? 'java';

      final javaCheck = await _runner.checkJava(javaExecutable);
      if (!javaCheck.available) {
        _err.writeln(
          'Java/JDK 17 or newer is required to run the SQLiteNow compiler.',
        );
        if (javaCheck.message != null) {
          _err.writeln(javaCheck.message);
        }
        return 69;
      }

      for (final database in config.databases) {
        final request = CompilerRequest.fromConfig(database, configFile.parent);
        final result = await _runner.runCompiler(
          javaExecutable: javaExecutable,
          compilerJar: compilerJar,
          request: request,
          workingDirectory: _workingDirectory,
        );

        for (final warning in result.warnings) {
          _err.writeln('warning: $warning');
        }
        for (final diagnostic in result.diagnostics) {
          _err.writeln(
            '${diagnostic.severity.toLowerCase()}: ${diagnostic.message}',
          );
        }

        if (!result.success) {
          _err.writeln(result.failureMessage ?? 'SQLiteNow compiler failed.');
          return 1;
        }

        await _formatGeneratedDartFiles(result.generatedFiles);

        _out.writeln(
          'Generated ${result.generatedFiles.length} file(s) for ${database.databaseName}.',
        );
      }

      return 0;
    } on CliConfigException catch (error) {
      _err.writeln(error.message);
      return 64;
    } on CompilerInvocationException catch (error) {
      _err.writeln(error.message);
      return 69;
    }
  }

  File _resolveFile(String path) {
    final file = File(path);
    if (file.isAbsolute) return file;
    return File('${_workingDirectory.path}${Platform.pathSeparator}$path');
  }

  Future<void> _formatGeneratedDartFiles(List<String> generatedFiles) async {
    final dartFiles = [
      for (final path in generatedFiles)
        if (path.endsWith('.dart') && _resolveFile(path).existsSync())
          _resolveFile(path).path,
    ];
    if (dartFiles.isEmpty) return;

    final result = await Process.run(Platform.resolvedExecutable, [
      'format',
      ...dartFiles,
    ], workingDirectory: _workingDirectory.path);
    if (result.exitCode != 0) {
      throw CompilerInvocationException(
        'Generated Dart formatting failed: ${result.stderr}',
      );
    }
  }

  Future<File> _resolveCompilerJar(String? option, String? configValue) async {
    final raw =
        option ?? configValue ?? Platform.environment['SQLITENOW_COMPILER_JAR'];
    if (raw != null && raw.trim().isNotEmpty) {
      final file = _resolveFile(raw);
      if (!file.existsSync()) {
        throw CompilerInvocationException(
          'Compiler jar not found: ${file.path}',
        );
      }
      return file;
    }

    final discovered = findDevelopmentCompilerJar(_workingDirectory);
    if (discovered != null) return discovered;

    final bundled = await findBundledCompilerJar(_workingDirectory);
    if (bundled != null) return bundled;

    throw CompilerInvocationException(
      'Compiler jar not found. Rebuild the Dart CLI package with the embedded compiler jar, pass --compiler-jar, or set SQLITENOW_COMPILER_JAR.',
    );
  }

  ArgParser _buildParser() {
    final parser = ArgParser()
      ..addFlag('help', abbr: 'h', negatable: false, help: 'Show help.');
    parser.addCommand('generate')
      ..addOption(
        'config',
        abbr: 'c',
        defaultsTo: 'sqlitenow.yaml',
        help: 'Path to sqlitenow.yaml.',
      )
      ..addOption('compiler-jar', help: 'Path to the SQLiteNow compiler jar.')
      ..addOption('java', help: 'Java executable to run.');
    return parser;
  }

  String _usage(ArgParser parser) {
    return 'Usage: dart run sqlitenow_cli generate [options]\n\n${parser.usage}';
  }
}

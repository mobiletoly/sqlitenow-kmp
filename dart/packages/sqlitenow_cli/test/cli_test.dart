import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:sqlitenow_cli/src/cli.dart';
import 'package:sqlitenow_cli/src/compiler.dart';
import 'package:test/test.dart';

void main() {
  test(
    'generate validates config and invokes compiler with dart backend',
    () async {
      final tempDir = await Directory.systemTemp.createTemp(
        'sqlitenow-cli-test-',
      );
      addTearDown(() => tempDir.delete(recursive: true));
      _writeFixture(tempDir);
      File('${tempDir.path}/sqlitenow.yaml').writeAsStringSync('''
databases:
  app_database:
    input: sql/AppDatabase
    output: lib/generated
    package: app.db
    runtime: dart
    oversqlite: false
    schemaDatabaseFile: build/schema/app.db
''');

      final runner = _FakeRunner();
      final compilerJar = File(
        '${tempDir.path}/sqlitenow-compiler/build/libs/sqlitenow-compiler-test-compiler.jar',
      )..createSync(recursive: true);
      compilerJar.writeAsStringSync('jar');
      final out = _StringSink();
      final err = _StringSink();

      final exitCode = await SqliteNowDartCli(
        runner: runner,
        out: out,
        err: err,
        workingDirectory: tempDir,
      ).run(['generate']);

      expect(exitCode, 0);
      expect(runner.requests, hasLength(1));
      expect(runner.requests.single.toJson(), containsPair('backend', 'dart'));
      expect(
        runner.requests.single.toJson(),
        containsPair(
          'schemaDatabaseFile',
          '${tempDir.path}/build/schema/app.db',
        ),
      );
      expect(runner.requests.single.databaseName, 'AppDatabase');
      expect(runner.requests.single.outputDirectory, endsWith('lib/generated'));
      expect(Directory('${tempDir.path}/build/schema').existsSync(), isTrue);
      expect(runner.compilerJars.single.path, compilerJar.path);
      expect(out.text, contains('Generated 1 file(s) for AppDatabase.'));
      expect(err.text, contains('warning: fixture warning'));
      expect(Directory('${tempDir.path}/lib/generated').existsSync(), isTrue);
    },
  );

  test('generate reports missing config', () async {
    final tempDir = await Directory.systemTemp.createTemp(
      'sqlitenow-cli-test-',
    );
    addTearDown(() => tempDir.delete(recursive: true));

    final err = _StringSink();
    final exitCode = await SqliteNowDartCli(
      runner: _FakeRunner(),
      err: err,
      workingDirectory: tempDir,
    ).run(['generate']);

    expect(exitCode, 64);
    expect(err.text, contains('Config file not found'));
  });

  test('generate reports missing java', () async {
    final tempDir = await Directory.systemTemp.createTemp(
      'sqlitenow-cli-test-',
    );
    addTearDown(() => tempDir.delete(recursive: true));
    _writeFixture(tempDir);
    final compilerJar = File('${tempDir.path}/compiler.jar')
      ..writeAsStringSync('jar');
    File('${tempDir.path}/sqlitenow.yaml').writeAsStringSync('''
compilerJar: ${compilerJar.path}
databases:
  AppDatabase:
    input: sql/AppDatabase
    output: generated
    package: app.db
''');

    final err = _StringSink();
    final exitCode = await SqliteNowDartCli(
      runner: _FakeRunner(javaAvailable: false),
      err: err,
      workingDirectory: tempDir,
    ).run(['generate']);

    expect(exitCode, 69);
    expect(err.text, contains('Java/JDK 17 or newer is required'));
  });

  test('generate honors compiler jar command override', () async {
    final tempDir = await Directory.systemTemp.createTemp(
      'sqlitenow-cli-test-',
    );
    addTearDown(() => tempDir.delete(recursive: true));
    _writeFixture(tempDir);
    final compilerJar = File('${tempDir.path}/override.jar')
      ..writeAsStringSync('jar');
    File('${tempDir.path}/sqlitenow.yaml').writeAsStringSync('''
databases:
  AppDatabase:
    input: sql/AppDatabase
    output: generated
    package: app.db
''');

    final runner = _FakeRunner();
    final exitCode = await SqliteNowDartCli(
      runner: runner,
      workingDirectory: tempDir,
    ).run(['generate', '--compiler-jar', compilerJar.path]);

    expect(exitCode, 0);
    expect(runner.compilerJars.single.path, compilerJar.path);
  });

  test(
    'real generator writes analyzable Dart and schema database',
    () async {
      final tempDir = await Directory.systemTemp.createTemp(
        'sqlitenow-cli-real-',
      );
      addTearDown(() => tempDir.delete(recursive: true));

      final runtimePath = _existingDirectory([
        '../sqlitenow_runtime',
        'packages/sqlitenow_runtime',
      ]).absolute.path;
      File('${tempDir.path}/pubspec.yaml').writeAsStringSync('''
name: sqlitenow_cli_generated_smoke
publish_to: none

environment:
  sdk: ^3.11.0

dependencies:
  sqlitenow_runtime:
    path: $runtimePath
''');
      _writeFixture(tempDir);
      File('${tempDir.path}/sqlitenow.yaml').writeAsStringSync('''
databases:
  AppDatabase:
    input: sql/AppDatabase
    output: lib/generated
    package: app.db
    runtime: dart
    schemaDatabaseFile: build/schema/app.db
''');

      final cli = _existingFile([
        'bin/sqlitenow_cli.dart',
        'packages/sqlitenow_cli/bin/sqlitenow_cli.dart',
      ]).absolute.path;
      final generate = await Process.run('dart', [
        cli,
        'generate',
      ], workingDirectory: tempDir.path);

      expect(
        generate.exitCode,
        0,
        reason: 'stdout:\n${generate.stdout}\nstderr:\n${generate.stderr}',
      );
      expect(
        File('${tempDir.path}/lib/generated/app_database.dart').existsSync(),
        isTrue,
      );
      expect(File('${tempDir.path}/build/schema/app.db').existsSync(), isTrue);

      final pubGet = await Process.run('dart', [
        'pub',
        'get',
      ], workingDirectory: tempDir.path);
      expect(
        pubGet.exitCode,
        0,
        reason: 'stdout:\n${pubGet.stdout}\nstderr:\n${pubGet.stderr}',
      );

      final analyze = await Process.run('dart', [
        'analyze',
      ], workingDirectory: tempDir.path);
      expect(
        analyze.exitCode,
        0,
        reason: 'stdout:\n${analyze.stdout}\nstderr:\n${analyze.stderr}',
      );
    },
    timeout: const Timeout(Duration(minutes: 2)),
  );
}

void _writeFixture(Directory tempDir) {
  Directory(
    '${tempDir.path}/sql/AppDatabase/schema',
  ).createSync(recursive: true);
  File('${tempDir.path}/sql/AppDatabase/schema/task.sql').writeAsStringSync('''
CREATE TABLE task (
  id INTEGER PRIMARY KEY NOT NULL,
  title TEXT NOT NULL
);
''');
}

Directory _existingDirectory(List<String> candidates) {
  for (final candidate in candidates) {
    final directory = Directory(candidate);
    if (directory.existsSync()) return directory;
  }
  throw StateError('None of these directories exist: $candidates');
}

File _existingFile(List<String> candidates) {
  for (final candidate in candidates) {
    final file = File(candidate);
    if (file.existsSync()) return file;
  }
  throw StateError('None of these files exist: $candidates');
}

final class _FakeRunner implements CompilerProcessRunner {
  _FakeRunner({this.javaAvailable = true});

  final bool javaAvailable;
  final List<CompilerRequest> requests = [];
  final List<File> compilerJars = [];

  @override
  Future<JavaCheck> checkJava(String javaExecutable) async {
    return JavaCheck(available: javaAvailable, message: 'missing java');
  }

  @override
  Future<CompilerResult> runCompiler({
    required String javaExecutable,
    required File compilerJar,
    required CompilerRequest request,
    required Directory workingDirectory,
  }) async {
    requests.add(request);
    compilerJars.add(compilerJar);
    return const CompilerResult(
      success: true,
      generatedFiles: ['generated/app_database.dart'],
      warnings: ['fixture warning'],
      diagnostics: [],
    );
  }
}

final class _StringSink implements IOSink {
  final _buffer = StringBuffer();

  String get text => _buffer.toString();

  @override
  void write(Object? object) => _buffer.write(object);

  @override
  void writeln([Object? object = '']) => _buffer.writeln(object);

  @override
  void writeAll(Iterable<Object?> objects, [String separator = '']) {
    _buffer.writeAll(objects, separator);
  }

  @override
  void writeCharCode(int charCode) => _buffer.writeCharCode(charCode);

  @override
  Encoding get encoding => utf8;

  @override
  set encoding(Encoding encoding) {}

  @override
  Future<void> addStream(Stream<List<int>> stream) async {}

  @override
  void add(List<int> data) {}

  @override
  void addError(Object error, [StackTrace? stackTrace]) {}

  @override
  Future<void> close() async {}

  @override
  Future<void> get done async {}

  @override
  Future<void> flush() async {}
}

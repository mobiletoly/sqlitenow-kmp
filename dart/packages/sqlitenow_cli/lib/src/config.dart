import 'dart:io';

import 'package:path/path.dart' as p;
import 'package:yaml/yaml.dart';

final class CliConfigException implements Exception {
  const CliConfigException(this.message);

  final String message;

  @override
  String toString() => message;
}

final class SqliteNowCliConfig {
  const SqliteNowCliConfig({required this.databases, this.compilerJar});

  final List<SqliteNowDatabaseConfig> databases;
  final String? compilerJar;

  static SqliteNowCliConfig load(File file) {
    if (!file.existsSync()) {
      throw CliConfigException('Config file not found: ${file.path}');
    }
    final yaml = loadYaml(file.readAsStringSync());
    if (yaml is! YamlMap) {
      throw const CliConfigException('Config root must be a map.');
    }

    final databasesNode = yaml['databases'];
    if (databasesNode is! YamlMap || databasesNode.isEmpty) {
      throw const CliConfigException(
        'Config must contain a non-empty databases map.',
      );
    }

    final databases = <SqliteNowDatabaseConfig>[];
    for (final entry in databasesNode.entries) {
      final key = entry.key;
      final value = entry.value;
      if (key is! String || key.trim().isEmpty) {
        throw const CliConfigException(
          'Database names must be non-empty strings.',
        );
      }
      if (value is! YamlMap) {
        throw CliConfigException("Database '$key' must be a map.");
      }
      databases.add(SqliteNowDatabaseConfig.fromYaml(key, value, file.parent));
    }

    return SqliteNowCliConfig(
      databases: databases,
      compilerJar: _optionalString(yaml, 'compilerJar'),
    );
  }
}

final class SqliteNowDatabaseConfig {
  const SqliteNowDatabaseConfig({
    required this.databaseName,
    required this.input,
    required this.output,
    required this.packageName,
    required this.oversqlite,
    this.schemaDatabaseFile,
  });

  final String databaseName;
  final String input;
  final String output;
  final String packageName;
  final bool oversqlite;
  final String? schemaDatabaseFile;

  static SqliteNowDatabaseConfig fromYaml(
    String key,
    YamlMap yaml,
    Directory configDirectory,
  ) {
    final runtime = _optionalString(yaml, 'runtime') ?? 'dart';
    if (runtime != 'dart') {
      throw CliConfigException(
        "Database '$key' has unsupported runtime '$runtime'. Expected 'dart'.",
      );
    }

    final input = _requiredString(yaml, 'input', key);
    final output = _requiredString(yaml, 'output', key);
    final packageName = _requiredString(yaml, 'package', key);
    final databaseName =
        _optionalString(yaml, 'databaseName') ?? _pascalize(key);
    final oversqlite = _optionalBool(yaml, 'oversqlite') ?? false;
    final schemaDatabaseFile = _optionalString(yaml, 'schemaDatabaseFile');

    final inputDir = _resolveDirectory(configDirectory, input);
    if (!inputDir.existsSync()) {
      throw CliConfigException(
        "Database '$key' input directory not found: ${inputDir.path}",
      );
    }
    final schemaDir = Directory(p.join(inputDir.path, 'schema'));
    if (!schemaDir.existsSync()) {
      throw CliConfigException(
        "Database '$key' input must contain a schema directory: ${schemaDir.path}",
      );
    }

    final outputDir = _resolveDirectory(configDirectory, output);
    if (!outputDir.existsSync()) {
      outputDir.createSync(recursive: true);
    }

    final resolvedSchemaDatabaseFile = schemaDatabaseFile == null
        ? null
        : _resolveFile(configDirectory, schemaDatabaseFile);
    resolvedSchemaDatabaseFile?.parent.createSync(recursive: true);

    return SqliteNowDatabaseConfig(
      databaseName: databaseName,
      input: inputDir.path,
      output: outputDir.path,
      packageName: packageName,
      oversqlite: oversqlite,
      schemaDatabaseFile: resolvedSchemaDatabaseFile?.path,
    );
  }
}

String _requiredString(YamlMap yaml, String name, String databaseName) {
  final value = _optionalString(yaml, name);
  if (value == null) {
    throw CliConfigException(
      "Database '$databaseName' is missing required '$name'.",
    );
  }
  return value;
}

String? _optionalString(YamlMap yaml, String name) {
  final value = yaml[name];
  if (value == null) return null;
  if (value is! String || value.trim().isEmpty) {
    throw CliConfigException(
      "Config field '$name' must be a non-empty string.",
    );
  }
  return value;
}

bool? _optionalBool(YamlMap yaml, String name) {
  final value = yaml[name];
  if (value == null) return null;
  if (value is! bool) {
    throw CliConfigException("Config field '$name' must be a boolean.");
  }
  return value;
}

Directory _resolveDirectory(Directory base, String rawPath) {
  if (p.isAbsolute(rawPath)) return Directory(rawPath);
  return Directory(p.normalize(p.join(base.path, rawPath)));
}

File _resolveFile(Directory base, String rawPath) {
  if (p.isAbsolute(rawPath)) return File(rawPath);
  return File(p.normalize(p.join(base.path, rawPath)));
}

String _pascalize(String value) {
  return value
      .split(RegExp(r'[_\-\s]+'))
      .where((part) => part.isNotEmpty)
      .map((part) => part[0].toUpperCase() + part.substring(1))
      .join();
}

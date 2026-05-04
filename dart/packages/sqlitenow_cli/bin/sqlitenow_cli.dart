import 'dart:io';

import 'package:sqlitenow_cli/src/cli.dart';

Future<void> main(List<String> arguments) async {
  final exitCode = await SqliteNowDartCli().run(arguments);
  if (exitCode != 0) {
    exit(exitCode);
  }
}

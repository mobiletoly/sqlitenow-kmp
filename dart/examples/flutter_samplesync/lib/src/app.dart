import 'package:flutter/material.dart';

import 'sample_sync_controller.dart';
import 'sample_sync_screen.dart';

class SampleSyncApp extends StatelessWidget {
  const SampleSyncApp({super.key, this.controller});

  final SampleSyncController? controller;

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'SQLiteNow SampleSync',
      debugShowCheckedModeBanner: false,
      theme: ThemeData(
        colorScheme: ColorScheme.fromSeed(seedColor: const Color(0xFF276EF1)),
        useMaterial3: true,
      ),
      home: SampleSyncScreen(controller: controller),
    );
  }
}

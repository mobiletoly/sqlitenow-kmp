import 'dart:async';
import 'dart:typed_data';

import 'package:flutter/material.dart';

import 'db/generated/now_sample_sync_database.dart';
import 'sample_models.dart';
import 'sample_sync_controller.dart';

class SampleSyncScreen extends StatefulWidget {
  const SampleSyncScreen({super.key, this.controller});

  final SampleSyncController? controller;

  @override
  State<SampleSyncScreen> createState() => _SampleSyncScreenState();
}

class _SampleSyncScreenState extends State<SampleSyncScreen>
    with WidgetsBindingObserver {
  late final SampleSyncController _controller =
      widget.controller ?? SampleSyncController.persistent();
  late final bool _ownsController = widget.controller == null;
  var _autoPromptPending = true;
  var _signInDialogVisible = false;

  @override
  void initState() {
    super.initState();
    WidgetsBinding.instance.addObserver(this);
    _controller.addListener(_handleControllerChange);
    unawaited(_controller.initialize());
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    unawaited(_controller.setForeground(state == AppLifecycleState.resumed));
  }

  @override
  void dispose() {
    WidgetsBinding.instance.removeObserver(this);
    _controller.removeListener(_handleControllerChange);
    if (_ownsController) unawaited(_controller.close());
    super.dispose();
  }

  void _handleControllerChange() {
    if (!mounted) return;
    setState(() {});
    _maybePromptForSignIn();
  }

  void _maybePromptForSignIn() {
    if (!_autoPromptPending ||
        _signInDialogVisible ||
        !_controller.initialized ||
        _controller.busy ||
        _controller.signedIn ||
        _controller.skippedSignIn) {
      return;
    }
    _autoPromptPending = false;
    WidgetsBinding.instance.addPostFrameCallback((_) {
      if (mounted) unawaited(_showSignInDialog());
    });
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      key: const ValueKey('samplesync-scaffold'),
      appBar: AppBar(
        title: const Text('SQLiteNow SampleSync'),
        actions: [
          if (_controller.signedIn)
            TextButton.icon(
              key: const ValueKey('samplesync-sign-out'),
              onPressed: _controller.busy ? null : _signOut,
              icon: const Icon(Icons.logout),
              label: Text(_controller.username),
            )
          else if (_controller.initialized)
            TextButton.icon(
              key: const ValueKey('samplesync-sign-in'),
              onPressed: _controller.busy ? null : _showSignInDialog,
              icon: const Icon(Icons.login),
              label: const Text('Sign in'),
            ),
        ],
      ),
      body: SafeArea(
        child: LayoutBuilder(
          builder: (context, constraints) {
            if (!_controller.initialized ||
                _controller.state == SampleControllerState.opening) {
              return const Center(child: CircularProgressIndicator());
            }
            if (_controller.state == SampleControllerState.failed) {
              return Center(
                child: Padding(
                  padding: const EdgeInsets.all(24),
                  child: Text(
                    _controller.errorMessage ?? 'Database open failed.',
                    key: const ValueKey('samplesync-fatal-error'),
                    textAlign: TextAlign.center,
                  ),
                ),
              );
            }
            return Padding(
              padding: EdgeInsets.symmetric(
                horizontal: constraints.maxWidth >= 900 ? 32 : 12,
                vertical: 12,
              ),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.stretch,
                children: [
                  _StatusCard(controller: _controller),
                  const SizedBox(height: 8),
                  if (_controller.errorMessage case final error?)
                    _ErrorCard(error: error, onDismiss: _controller.clearError),
                  _ActionBar(
                    controller: _controller,
                    onAddPerson: () => _runAction(_controller.addRandomPerson),
                    onSync: _manualSync,
                  ),
                  const SizedBox(height: 8),
                  Expanded(
                    child: StreamBuilder<List<PersonRow>>(
                      stream: _controller.watchPeople(),
                      builder: (context, snapshot) {
                        if (snapshot.hasError) {
                          return Center(
                            child: Text(
                              'People query failed: ${snapshot.error}',
                            ),
                          );
                        }
                        if (!snapshot.hasData) {
                          return const Center(
                            child: CircularProgressIndicator(),
                          );
                        }
                        final people = snapshot.data!;
                        if (people.isEmpty) {
                          return const Center(
                            key: ValueKey('samplesync-empty'),
                            child: Text(
                              'No people yet. Add one locally to get started.',
                            ),
                          );
                        }
                        return ListView.separated(
                          key: const ValueKey('samplesync-people-list'),
                          itemCount: people.length,
                          separatorBuilder: (_, _) => const SizedBox(height: 8),
                          itemBuilder: (context, index) {
                            final person = people[index];
                            return _PersonCard(
                              person: person,
                              controller: _controller,
                              runAction: _runAction,
                            );
                          },
                        );
                      },
                    ),
                  ),
                ],
              ),
            );
          },
        ),
      ),
    );
  }

  Future<void> _runAction(Future<void> Function() action) async {
    try {
      await action();
    } catch (_) {
      // The controller owns the user-facing error text.
    }
  }

  Future<void> _manualSync() async {
    await _controller.manualSync();
    if (!mounted) return;
    await _showReportIfPresent();
  }

  Future<void> _signOut() async {
    await _controller.signOut();
    if (!mounted) return;
    await _showReportIfPresent();
  }

  Future<void> _showReportIfPresent() async {
    final report = _controller.reportMessage;
    if (report == null || !mounted) return;
    await showDialog<void>(
      context: context,
      builder: (context) => AlertDialog(
        key: const ValueKey('samplesync-report-dialog'),
        title: const Text('Sync Report'),
        content: SelectableText(report),
        actions: [
          TextButton(
            key: const ValueKey('samplesync-report-close'),
            onPressed: () => Navigator.of(context).pop(),
            child: const Text('Close'),
          ),
        ],
      ),
    );
    _controller.clearReport();
  }

  Future<void> _showSignInDialog() async {
    if (_signInDialogVisible || !mounted) return;
    _signInDialogVisible = true;
    final username = TextEditingController(text: _controller.username);
    final password = TextEditingController();
    var selectedMode = _controller.mode;
    String? dialogError;
    var submitting = false;

    await showDialog<void>(
      context: context,
      barrierDismissible: false,
      builder: (dialogContext) {
        return StatefulBuilder(
          builder: (context, setDialogState) {
            return AlertDialog(
              key: const ValueKey('samplesync-sign-in-dialog'),
              title: const Text('SampleSync sign in'),
              content: SizedBox(
                width: 440,
                child: SingleChildScrollView(
                  child: Column(
                    mainAxisSize: MainAxisSize.min,
                    crossAxisAlignment: CrossAxisAlignment.stretch,
                    children: [
                      TextField(
                        key: const ValueKey('samplesync-username'),
                        controller: username,
                        enabled: !submitting,
                        decoration: const InputDecoration(
                          labelText: 'Username',
                          hintText: 'u10',
                        ),
                      ),
                      const SizedBox(height: 12),
                      TextField(
                        key: const ValueKey('samplesync-password'),
                        controller: password,
                        enabled: !submitting,
                        obscureText: true,
                        decoration: const InputDecoration(
                          labelText: 'Password',
                          helperText:
                              'The demo server accepts an empty password.',
                        ),
                      ),
                      const SizedBox(height: 16),
                      SegmentedButton<SampleSyncMode>(
                        key: const ValueKey('samplesync-mode-selector'),
                        segments: const [
                          ButtonSegment(
                            value: SampleSyncMode.polling,
                            label: Text('Polling'),
                          ),
                          ButtonSegment(
                            value: SampleSyncMode.watch,
                            label: Text('Watch'),
                          ),
                        ],
                        selected: {selectedMode},
                        onSelectionChanged: submitting
                            ? null
                            : (selection) {
                                setDialogState(() {
                                  selectedMode = selection.single;
                                });
                              },
                      ),
                      if (dialogError != null) ...[
                        const SizedBox(height: 12),
                        Text(
                          dialogError!,
                          key: const ValueKey('samplesync-sign-in-error'),
                          style: TextStyle(
                            color: Theme.of(context).colorScheme.error,
                          ),
                        ),
                      ],
                    ],
                  ),
                ),
              ),
              actions: [
                TextButton(
                  key: const ValueKey('samplesync-skip-sign-in'),
                  onPressed: submitting
                      ? null
                      : () {
                          _controller.skipSignIn();
                          Navigator.of(dialogContext).pop();
                        },
                  child: const Text('Skip'),
                ),
                FilledButton(
                  key: const ValueKey('samplesync-submit-sign-in'),
                  onPressed: submitting
                      ? null
                      : () async {
                          setDialogState(() {
                            submitting = true;
                            dialogError = null;
                          });
                          await _controller.setMode(selectedMode);
                          await _controller.signIn(
                            username.text,
                            password.text,
                          );
                          if (!dialogContext.mounted) return;
                          if (_controller.signedIn) {
                            Navigator.of(dialogContext).pop();
                          } else {
                            setDialogState(() {
                              submitting = false;
                              dialogError =
                                  _controller.errorMessage ?? 'Sign in failed.';
                            });
                          }
                        },
                  child: submitting
                      ? const SizedBox.square(
                          dimension: 18,
                          child: CircularProgressIndicator(strokeWidth: 2),
                        )
                      : const Text('Sign in'),
                ),
              ],
            );
          },
        );
      },
    );
    username.dispose();
    password.dispose();
    _signInDialogVisible = false;
  }
}

class _StatusCard extends StatelessWidget {
  const _StatusCard({required this.controller});

  final SampleSyncController controller;

  @override
  Widget build(BuildContext context) {
    final status = controller.signedIn
        ? 'Signed in as ${controller.username}'
        : controller.skippedSignIn
        ? 'Local-only mode'
        : 'Signed out';
    return Card(
      child: Padding(
        padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 12),
        child: Wrap(
          spacing: 16,
          runSpacing: 8,
          crossAxisAlignment: WrapCrossAlignment.center,
          children: [
            Text(
              status,
              key: const ValueKey('samplesync-status'),
              style: Theme.of(context).textTheme.titleMedium,
            ),
            Text('Mode: ${controller.mode.label}'),
            if (controller.sourceId.isNotEmpty)
              Text(
                'Source: ${controller.sourceId}',
                overflow: TextOverflow.ellipsis,
              ),
          ],
        ),
      ),
    );
  }
}

class _ErrorCard extends StatelessWidget {
  const _ErrorCard({required this.error, required this.onDismiss});

  final String error;
  final VoidCallback onDismiss;

  @override
  Widget build(BuildContext context) {
    return Card(
      color: Theme.of(context).colorScheme.errorContainer,
      child: ListTile(
        key: const ValueKey('samplesync-error'),
        leading: const Icon(Icons.error_outline),
        title: Text(error),
        trailing: IconButton(
          key: const ValueKey('samplesync-error-close'),
          onPressed: onDismiss,
          icon: const Icon(Icons.close),
        ),
      ),
    );
  }
}

class _ActionBar extends StatelessWidget {
  const _ActionBar({
    required this.controller,
    required this.onAddPerson,
    required this.onSync,
  });

  final SampleSyncController controller;
  final VoidCallback onAddPerson;
  final VoidCallback onSync;

  @override
  Widget build(BuildContext context) {
    return Wrap(
      spacing: 8,
      runSpacing: 8,
      children: [
        FilledButton.icon(
          key: const ValueKey('samplesync-add-person'),
          onPressed: controller.busy ? null : onAddPerson,
          icon: const Icon(Icons.person_add),
          label: const Text('Add Person'),
        ),
        OutlinedButton.icon(
          key: const ValueKey('samplesync-manual-sync'),
          onPressed: controller.signedIn && !controller.busy ? onSync : null,
          icon: const Icon(Icons.sync),
          label: const Text('Sync'),
        ),
        SegmentedButton<SampleSyncMode>(
          key: const ValueKey('samplesync-toolbar-mode'),
          segments: const [
            ButtonSegment(
              value: SampleSyncMode.polling,
              label: Text('Polling'),
            ),
            ButtonSegment(value: SampleSyncMode.watch, label: Text('Watch')),
          ],
          selected: {controller.mode},
          onSelectionChanged: controller.busy
              ? null
              : (selection) {
                  unawaited(controller.setMode(selection.single));
                },
        ),
      ],
    );
  }
}

class _PersonCard extends StatelessWidget {
  const _PersonCard({
    required this.person,
    required this.controller,
    required this.runAction,
  });

  final PersonRow person;
  final SampleSyncController controller;
  final Future<void> Function(Future<void> Function() action) runAction;

  @override
  Widget build(BuildContext context) {
    final id = _hex(person.id);
    return Card(
      key: ValueKey('samplesync-person-$id'),
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            Text(
              '${person.myFirstName} ${person.myLastName.toUpperCase()}',
              style: Theme.of(context).textTheme.titleMedium,
            ),
            Text(person.email),
            if (person.phone case final phone?) Text(phone),
            const SizedBox(height: 8),
            Wrap(
              spacing: 4,
              runSpacing: 4,
              children: [
                TextButton(
                  key: ValueKey('samplesync-randomize-$id'),
                  onPressed: () =>
                      runAction(() => controller.randomizePerson(person)),
                  child: const Text('Rnd'),
                ),
                TextButton(
                  key: ValueKey('samplesync-address-$id'),
                  onPressed: () =>
                      runAction(() => controller.addRandomAddress(person.id)),
                  child: const Text('Addr'),
                ),
                TextButton(
                  key: ValueKey('samplesync-comment-$id'),
                  onPressed: () =>
                      runAction(() => controller.addRandomComment(person.id)),
                  child: const Text('Cmnt'),
                ),
                TextButton(
                  key: ValueKey('samplesync-delete-$id'),
                  onPressed: () =>
                      runAction(() => controller.deletePerson(person.id)),
                  child: const Text('Del'),
                ),
              ],
            ),
            StreamBuilder<List<SampleComment>>(
              stream: controller.watchComments(person.id),
              builder: (context, snapshot) {
                final comments = snapshot.data ?? const <SampleComment>[];
                if (comments.isEmpty) {
                  return const SizedBox.shrink();
                }
                return Padding(
                  padding: const EdgeInsets.only(top: 8),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Text(
                        'Comments',
                        style: Theme.of(context).textTheme.labelLarge,
                      ),
                      for (final comment in comments)
                        Text(
                          '• ${comment.comment}',
                          key: ValueKey('samplesync-comment-${comment.id}'),
                        ),
                    ],
                  ),
                );
              },
            ),
          ],
        ),
      ),
    );
  }
}

String _hex(Uint8List bytes) {
  return bytes.map((value) => value.toRadixString(16).padLeft(2, '0')).join();
}

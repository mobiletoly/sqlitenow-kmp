# 0.17.0

- Align generated Oversqlite database facades with SQLiteNow 0.17.0 migration callbacks.

# 0.16.0

- Add KMP-parity numeric and Boolean transport coverage for the
  `jcs_uniform_numeric_strings_v1` Oversqlite wire contract.
- Add Flutter real-server regression coverage for rich numeric schemas.

# 0.15.0

- Reject future checkpoints with `checkpoint_ahead` and automatically resume durable authoritative
  snapshot recovery for pruned or poisoned checkpoints. Recovery preserves pending offline work
  behind an actionable blocker and advances the checkpoint only after atomic snapshot apply.

# 0.10.0

- Add Dart Oversqlite bundle-change watch support and live-server parity
  coverage for richer generated sync schemas.

# 0.9.0

- Initial Dart Oversqlite runtime with local sync metadata, lifecycle state,
  push, pull, snapshot rebuild, conflict resolution, and realserver coverage.

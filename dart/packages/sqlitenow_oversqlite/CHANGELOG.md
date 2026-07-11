# Unreleased

- Reject future checkpoints with `checkpoint_ahead` and automatically resume durable authoritative
  snapshot recovery for pruned or poisoned checkpoints. Recovery preserves pending offline work
  behind an actionable blocker and advances the checkpoint only after atomic snapshot apply.

# 0.10.0

- Add Dart Oversqlite bundle-change watch support and live-server parity
  coverage for richer generated sync schemas.

# 0.9.0

- Initial Dart Oversqlite runtime with local sync metadata, lifecycle state,
  push, pull, snapshot rebuild, conflict resolution, and realserver coverage.

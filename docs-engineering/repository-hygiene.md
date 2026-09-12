# Repository hygiene

Tracked source, tests, generated owners, build tasks, qualification tools, samples, and durable
documentation use descriptive behavior names. Temporary milestone identifiers, numbered phase
names, and disposable specification IDs are not durable API or package vocabulary.

Semantic versions and stable case IDs are allowed when they describe a real format or test matrix:
library and protocol versions, schema generations, U0-U8 lifecycle states, the defined M01-M30
compatibility matrix, and C01-C12 mixed-generation row keys. Method and path names should state the
scenario instead of repeating those keys.

Root `spec/` and `specs/` are local, ignored authoring areas. No file below either root may be in
the Git index or delivered commit. Builds, default tests, qualification planning, and documentation
must work when those directories are absent. A caller may select a temporary authority explicitly;
that selection is plan input, not a default dependency.

The coordinated-opening journal format introduced by the descriptive-name cutover is version 2.
Its digest domains changed deliberately. Databases with the earlier format are rejected before
managed writes with a fresh-database diagnostic; they are not migrated, reset, deleted, or silently
converted. Databases created by the current format can reopen and resume normal generation and
recovery flows.

Run `python3 tools/check_repository_hygiene.py` for the worktree/index candidate and
`python3 tools/check_repository_hygiene.py --revision <commit>` for a committed tree. The guard
reports exact path, line, and reason; narrow documented exceptions must match exact path, token,
occurrence count, and durable reason, and unused exceptions are errors.
Revision mode reads the exception registry from that same commit and fails closed when the
committed registry is missing or invalid; worktree exceptions cannot alter revision results.

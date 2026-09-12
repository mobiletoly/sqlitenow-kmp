# SQLiteNow contributor notes

Use `drymint-coding` before supported source edits. Preserve unrelated worktree state and keep planning, implementation, qualification, review, acceptance, and delivery as separate gates.

The repository retains Kotlin Multiplatform libraries, the Gradle generator, Swift products, samples, and Oversqlite. Run `./gradlew :sqlitenow-gradle-plugin:test` after plugin changes. Run compiler and plugin tests separately.

For generated code, keep fixtures behavior-defining and mechanical setup shared. For Oversqlite, treat platform and live-server lanes as separate evidence; a host suite does not replace them. The sibling Go repository is read-only unless separately authorized.

## Proportionate validation

Use `docs-engineering/oversqlite-validation.md` to distinguish development, maintenance-candidate, and initial-protocol/release profiles. Select maintenance lanes manually by affected behavior and dependencies; record fresh, reused, subsumed, not-applicable, and blocked evidence. An aggregate must name its governing section, command, and trigger. Common-source changes retain platform/live obligations for affected paths; unchanged build outputs may be reused while invalidated test leaves execute. Documentation-only changes refresh authority/review identity without rerunning runtime suites unless their requirements changed. Full protocol/release matrices and historical acceptance records remain intact.

## Bounded execution

- Before implementation, map mandatory exit criteria to available commands, fixtures, counters and runtime access. Resolve critical prerequisites first. If a required proof has no authorized route, report the blocker immediately and stop dependent implementation; adjacent improvements do not justify continuing an unreachable gate.
- For autonomous goals, agree the scope and verification surface before activation. Use progress checkpoints after coherent batches and during long-running work; continue authorized work while it advances the agreed exit criteria. Report material changes to scope, expected cost or prerequisite availability before proceeding with affected work, and obtain authorization for work outside the agreed scope. Apply session time, token or cost limits only when explicitly set by the owner; do not invent a default cutoff or require an extension merely because time has elapsed. Preserve any explicit limits and the original start across continuation and context compaction. A budget limit is not completion. Per-command timeouts and validation budgets remain in force.
- Each coherent batch must advance a named exit criterion and have evidence that can close it. Run affected checks after the batch and required broader checks at the milestone. Repeat only for invalidated evidence or a diagnosed failure with changed conditions. Optional tooling findings are not a reason for endless reviews or index rebuilds.
- Capture tracked and untracked contents before edits and enforce an explicit path allowlist with `tools/check_change_scope.py`. Use `--test-only` for test/support work. Check before broader validation and at handoff; do not recapture the baseline or widen its scope to hide drift. Keep test fixtures/hooks out of normal product source and builds; separately review build wiring, dependencies and exported interfaces.
- Keep one current requirements/status ledger and necessary raw attempts. Preserve historical evidence without repeatedly cloning inventories or narrative receipts. After each batch, report criteria closed, critical blockers, elapsed time and the next action that can advance exit.
- Keep evidence work proportional to the current batch. Start from the latest verified ledger/handoff and authority overlay, check current identity and relevant dependency/runner deltas, and inherit previously verified predecessor bindings through an intact seal. Reopen or recursively rehash historical packets only for a changed, missing or inconsistent binding or an explicit governing requirement. Audit an unchanged dependency set once per frozen source and reference existing receipts. Preserve required baseline and final scope checks.
- End prerequisite mapping once the current batch has an exact scope, exit criterion, command, fixture/assertions, dependency bindings and runtime route. Execute the reachable authorized batch; record later lanes as pending and broader gates as open. A mandatory blocker for that batch still stops dependent work. If a progress checkpoint reports only bookkeeping without resolving a new prerequisite or evidence decision, stop repeating it and take the next executable step or report the exact blocker.

## Required maintenance workflow

- You MUST declare the exact edit scope, mandatory exit criteria and manually selected validation commands before source edits. For every lane, record whether fresh execution is required or completed evidence can be reused, with its dependency and provenance basis. A small correction MUST NOT silently become a broad validation campaign.
- You MUST inspect the affected fixture's schema, state, counters and lifecycle assumptions together before changing an isolated assertion. Reconcile them with the governing contract and current owners; do not remove assertions, rewrite frozen expectations from observed output, or change production behavior to make a stale fixture pass.
- You MUST keep prerequisite fixture or infrastructure repair separate from production optimization. A failed mandatory prerequisite stops dependent implementation. A continuation MUST address the recorded blocker first; it MUST NOT automatically restore an archived production candidate or restart its campaign.
- You MUST reuse intact completed evidence when relevant source, assertions, fixtures, toolchain, runtime, selectors and runner contracts match and the governing gate permits reuse. A new Codex session, a documentation edit or elapsed time alone MUST NOT invalidate runtime evidence. Unchanged build outputs may be reused while invalidated test leaves execute afresh; cached, skipped or absent tests MUST NOT be reported as fresh execution.
- You MUST rerun only invalidated lanes and independently required final checks. Any expansion, repeat or aggregate MUST identify its governing requirement or diagnosed cause, changed conditions and the exit criterion it can close before launch. Existing explicit qualification, release and final-source requirements still apply.
- Every fresh-session handoff MUST link the current ledger, exact blocker, scope, immutable preimages, evidence classifications, remaining commands and any explicit owner-set limits, including the original start and renewals when applicable. State when no session limit was set. The receiving session MUST verify those bindings and continue from that state rather than reconstructing or rerunning the campaign by default.

Scope checker (exact files or directory prefixes ending in `/`; baseline must be ignored or outside the checkout):

```sh
python3 tools/check_change_scope.py snapshot --baseline .artifacts/change-scope.json --allow path/to/test.kt --test-only
python3 tools/check_change_scope.py check --baseline .artifacts/change-scope.json
```

The checker enforces file scope and repository/index identity, not elapsed time, semantic test isolation or approval. Ignored build/evidence directories are excluded; explicitly allowed ignored files are included. Authorizing a pinned authority edit requires a new identity/overlay record while the sealed packet remains intact.

## Paired Oversqlite Contract Workflow

- Treat the governing Go and SQLiteNow specifications, the Go OpenAPI document, and `oversqlite-contracts/` as one versioned contract boundary. When work in both repositories is authorized, capture both tracked and untracked baselines and verify both identities before editing; stop on external drift or ambiguous overlap.
- Record normative wire decisions in the governing specifications and OpenAPI before implementing a runtime consumer. Update active fixtures, explicit fixture classifications, the sorted corpus manifest, and pinned hashes together from the settled contract.
- Active legacy references are allowed only as explicit absence assertions or clearly classified historical fixture input. Contract consumers must fail closed when the configured corpus root is absent or its manifest does not match, and must execute every fixture family they claim to support.
- Run a fast paired-contract gate before expensive runtime suites: specification and OpenAPI hashes, corpus manifest integrity, active fixture consumers, and focused wire-boundary tests. JVM contract tests validate the shared boundary; they are not KMP runtime implementation and do not replace platform or live-server evidence.
- Keep a validation ledger with focused, package, platform, live-server, cross-repository, superseded, and rerun-required lanes. Give database-backed families independent budgets and avoid concurrent execution against shared database resources unless isolation has been proved.
- Treat an aggregate timeout as inconclusive until the affected family is isolated. Do not retry the same unchanged aggregate merely to seek a pass; record the cause, changed execution conditions, and constituent evidence. After a late change, rerun only invalidated lanes plus any mandatory final aggregate.

Do not stage, commit, push, publish, deploy, rewrite history, or mutate package registries without explicit authorization.

# Qualification planning and execution

SQLiteNow qualification is an advisory, source-bound workflow. Planning, lane authorization,
execution, evidence review, owner acceptance, and delivery are separate gates. The tools never
accept a candidate, mutate the read-only Go companion, or turn historical evidence into a new pass.

## Planner contract

`python3 tools/qualification/planner.py plan --base HEAD` reads the version-3 lane manifest,
classifies the current candidate, expands lane dependencies, records toolchain and repository
identities, and emits a deterministic plan. Planning does not launch product commands. Product
lanes require explicit execution authorization; Go-owned lanes remain planning-only.

The default governing inputs are this document, `docs-engineering/repository-hygiene.md`,
`tools/qualification/README.md`, `docs/sync/schema-evolution.md`, and the Go companion's tracked
`integration/schema_evolution/QUALIFICATION.md` and `integration/schema_evolution/ROLLOUT.md`.
Each configured repository's root `AGENTS.md` is recorded separately as an optional instruction
input. Its absence is valid, but any captured presence, absence, or content change makes a saved
plan stale.

Temporary authority is opt-in:

```sh
python3 tools/qualification/planner.py plan --base HEAD \
  --governing-input sqlite-now:path/to/local-authority.md
```

The repository ID and contained regular-file path are canonicalized, deduplicated, sorted, and
stored in `explicit_governing_inputs`. Missing, unreadable, symlinked, escaped, changed, or deleted
selected authority invalidates execution. Unselected root specifications do not affect a default
plan.

## Execution and records

Plans and manifests use schema version 3. Attempt and campaign JSONL records remain version 2.
Historical version-2 plans and changed manifest identities are readable as files but cannot be
executed or resumed; create a new plan. Existing attempt and campaign history is append-only and is
never rewritten to match a new candidate.

Every launched lane uses the exact saved argv, repository-relative working directory, allow-listed
environment, timeout, toolchain identity, dependency digest, and relevant-input digest. The runner
checks repository, manifest, governing, instruction, and lane identities before and after a child.
Identity drift is terminal. Timeouts and signals terminate only the owned process group, and logs
are content-addressed in the append-only record.

`campaign.py` schedules only explicitly selected, campaign-eligible SQLiteNow lanes. Dependency
order and declared resource capacities control concurrency. Failed dependencies block descendants;
retry and continue-on-failure choices are explicit. Resume retains only a complete successful lane
whose plan, manifest, inputs, environment, repositories, and log still match.

Reuse is advice for a qualification reviewer. Missing, malformed, incomplete, stale, or changed
records remain unavailable evidence. A skipped target, zero-test success, absent platform, or
unexecuted live-server row cannot be relabeled passed.

## Read-only boundaries

The configured Go repository is identity-checked and read-only to ordinary planner/campaign
execution. Cross-repository fixture implementation requires separate authority outside these tools.
Live lanes must use run-owned processes, databases, device state, and artifact directories, and
must clean only those owned resources. Credentials are supplied through documented environment or
CLI inputs and must not be written to reports.

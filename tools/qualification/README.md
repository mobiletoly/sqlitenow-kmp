# Qualification planner

`planner.py` maps changed SQLiteNow paths to retained, advisory qualification lanes. It records lane selection, immutable identities, resource locks, and reviewer-facing evidence without running product validation during planning.

The lane manifest is `lanes.json`; the manifest and plan schema version is 3, while the attempt and campaign record schema version remains 2. `campaign.py` runs explicitly accepted lanes and records the campaign separately. The tools have no acceptance or closure authority and cannot mutate the sibling Go repository. A qualification reviewer must confirm every reuse recommendation.

Run planner tests with:

```sh
python3 tools/qualification/planner.py plan --base HEAD
python3 tools/qualification/planner.py validate-manifest
python3 -m unittest discover -s tools/qualification/tests
```

Use `campaign.py --accept-selected` only after an explicit decision. Campaign records append evidence; the planner remains advisory.

The planner may recommend core, compiler, plugin, Swift, local platform, live-server, or Go conformance lanes. A reviewer must confirm each recommendation, establish live-server identity before live lanes, and preserve the Go repository as read-only.

Planning uses only controlled inspection commands. The manifest forbids product-validation prefixes so planning cannot become an accidental build or test run.

## Focused Oversqlite maintenance smokes

`oversqlite_smoke.py go-direct-sync` and `oversqlite_smoke.py swift-cancellation`
are explicit standalone commands, not planner or campaign replacements. They require
`OVERSYNC_EXAMPLE_TEST_DATABASE_URL` for an existing PostgreSQL service and record
redacted logs, named test results, preparation/runtime/cleanup timings and paired
identities. See [the validation runbook](../../docs-engineering/oversqlite-validation.md)
for selection, prerequisites, limits and evidence reuse. These commands run only
when explicitly requested; infrastructure tests use controlled children.

## Focused platform verifier and categories replay

`python3 -B tools/qualification/oversqlite_platform.py` provides `prepare`,
`verifier` and `replay` commands. It never invokes historical campaign controllers
or grants acceptance. Source/build/runtime/selector equivalence still needs review.

Prepare the current Go categories authority once with `prepare --go-repo <go>
--output <new-directory>`. Runtime commands require `--runtime` (`jvm`, `android`,
`macos`, `ios`, `node`, `browser`, `wasm`), `--freeze <json>` and `--output <new>`.
The freeze contains a nonempty `files` map of absolute dependency paths to SHA256.
It must cover the candidate, tooling, authorities and fixture bindings. The runner
checks it before and after native execution and records repository identities.
`replay` also requires `--fixture <prepared/categories-authority>` and one or more
exact `--method committedReplayWith1000RowsWidth16` selectors (the four current
1K/10K by16/1024 methods). Android accepts one replay method per invocation;
`verifier` always selects all nine controls.

The runner uses existing Gradle tasks with explicit filters, `--rerun` for the
execution task, current build type, and unchanged1800s native wrapper. It retains
fresh native XML, exact identities and ENTER/terminal/workload witnesses. Missing,
wrong, duplicated, guarded or skipped entries fail closed. Explicit worker/reopen
nonbodies receive no execution credit. Body limits remain180s verifier and900s
replay, with existing web framework limits intact. Phase timings not independently
instrumented are recorded as unknown; testcase and wrapper times remain distinct.

Live runs own a new durable PostgreSQL volume/container with normal durability,
one categories authority and a synthetic database. Readiness remains60s. Android
owns a fresh private AVD and an explicit emulator serial; iOS creates and targets a
fresh simulator using the test task's `--device` option. Existing devices and AVDs
are never selected or stopped. While an owned iOS simulator is alive, global
process inspection is deferred because it can block on simulator services; direct
launched groups and inherited ownership tokens remain recorded. Cleanup deletes
the exact simulator, recovers and joins escaped tagged descendants, then repeats
the census before success. Other runtimes retain continuous process inspection.
All invocations have a shared30s cleanup budget;
resource absence and cleanup failures remain visible. The host needs Docker,
Go1.25.12, the existing Android33 arm64 ATD image for Android, and an installed
iOS27.0/iPhone18Pro simulator runtime for iOS. Set an appropriate Java home for
Gradle. No prerequisite is installed by the runner.

Run infrastructure controls without product suites:

```sh
python3 -B -m unittest tools.qualification.tests.test_oversqlite_platform tools.qualification.tests.test_oversqlite_smoke
```

## Opt-in browser/macOS preparation pilot

The existing `verifier` command accepts `--record-preparation` on `browser` and
`macos`. After exact native body accounting and successful cleanup it writes
`preparation.json` in the new attempt directory. `--reuse-preparation <that-file>`
checks the binding and runs the same nine verifier cases again with `--rerun`.
Use a new output directory for every invocation. These options grant no test-result
reuse, qualification-profile adoption or automatic acceptance.

The binding covers paired source identity, the explicit frozen dependency map,
caller environment (per-key hashes, excluding the runner's ownership token and
transport-only `CODEX_TOOL_CALL_ID`), runtime
and tool versions, Java/Chrome engine files, and Gradle's complete selected graph
input and dependency-output inventories. Directory membership, file modes,
symlink targets, generated files and resolved dependency bytes are included.
Included-build compiler/plugin artifacts and local Gradle initialization settings
are also bound, together with the actual Gradle version and Java home reported
by Gradle. The reused manifest's bytes remain pinned through finalization.
Its producer receipt must be successful and match the manifest's native/cleanup
evidence; both files remain pinned throughout reuse. An incomplete or failed
producer cannot grant preparation reuse, even if it left a manifest behind.
Selected native result/report outputs are excluded. The trace is
written to a private structured file, independently of redacted human logs.

Every invocation retains new native XML and exact ENTER/PASS witnesses. Browser
executes nine bodies; macOS executes eight plus the existing explicit non-worker
entry. The latter earns no body credit. The reuse path rejects dependency drift,
changed runtime/environment, cached/absent/partial task traces, altered closure,
and any compile/link task that unexpectedly executes. Re-record preparation after
an intentional input change. Existing180s body,1800s wrapper and30s shared cleanup
limits remain unchanged. Preparation records are published only after success.
New metadata phases share one cumulative120s allowance, paused during native
execution and cleanup, with an interrupting deadline for blocked probes/hash I/O.
Streaming hashes avoid loading whole artifacts into memory. Native graph task
timings cover only the observed graph; total elapsed includes all binding,
included-build, startup, execution and cleanup work.
Pilot waits use registered launch groups and inherited ownership tokens; the
mandatory final census discovers and joins even detached/reparented children.
This avoids blocking global process polling during compilation/browser startup.

For an explicitly selected pilot baseline, `--record-preparation
--rebuild-dependencies` reproduces the dependency rerun policy used by existing web
Exec wrappers while keeping the same focused verifier selection. It is not a
default and does not represent the duration of an entire historical aggregate.
The ordinary focused macOS command already permits incremental compilation;
compare it with verified reuse honestly, including binding overhead. Do not claim
a speedup unless measured. Task timing comes from the observed existing graph;
native setup/operation/assertion splits remain unknown when not instrumented.

```sh
python3 -B tools/qualification/oversqlite_platform.py verifier --runtime browser \
  --freeze /absolute/frozen-files.json --output /absolute/new-attempt \
  --record-preparation
python3 -B tools/qualification/oversqlite_platform.py verifier --runtime browser \
  --freeze /absolute/frozen-files.json --output /absolute/another-new-attempt \
  --reuse-preparation /absolute/new-attempt/preparation.json
```

Run the preparation controls with
`python3 -B -m unittest tools.qualification.tests.test_platform_preparation`.

## Full-profile entry adapter

`oversqlite_platform.py lane --profile <file> --entry <id> --freeze <file>
--bindings <private-file> --output <new-directory>` reads the adopted version2
schedule. Static profile data never authorizes execution. The independent freeze
must bind the profile, an owner authorization record, current coordinator sources,
selected body-policy sources, execution scripts/engines and private typed bindings.
The authorization record identifies stage `E42`, the exact `profile_sha256` and
the actual `owner_request`; its path/hash appear in the freeze's `authorization`.
All dependency paths in the freeze's nonempty `files` map are absolute.

The binding file must have private permissions. Its `values` map resolves literal
`binding.*`/`lease.*` tokens without a shell. Delegated leases require an independently
frozen private ownership receipt and a live registered supervisor; a child never
adopts or deletes a supervisor's resources. Worker copies require explicit frozen
canonical-to-worker `root_rebindings`. Environment contexts are selected before
rebasing. Ambient activation flags and authority routes are cleared.

Preparation entries require `--context <declared-context>`. Their complete observed
nonbody graph, requested task set, source/environment/runtime binding and actual
input/output inventory are sealed in `preparation.json` only after successful
cleanup. Native entries require matching producers through repeatable
`--prepared <preparation.json>` options. Missing required producers fail before
child startup. The consumer executes fresh native leaves and leaves verified
predecessors enabled for Gradle provider resolution. Its terminal audit rejects
actual compile/link work and rechecks producer receipts, artifacts and its actual graph
through cleanup. A class/report count alone never supplies body credit: each owner
needs a frozen source classification, and witness policies require exact nonempty
markers. Guarded and capability entries retain zero body credit.

Normal entry wall budgets include binding, execution, observation and cleanup;
they remain at most300seconds, with the prescribed process cap and at most30seconds
cleanup. Go builds require new explicitly owned output paths. Swift entries append
fresh xUnit capture to the existing Exec task. Only the declared generated live
task receives the reviewed `SyncFixtureRealserverTests` filter. Existing task
dependencies, working directories and environments are preserved.

The named `load-prepare-1` and `load-prepare-2` recipes are selectable build jobs,
with fixed new outputs `load-authority` and `load-client.test` inside their attempt.
The client recipe generates its overlay from the reviewed, independently frozen
`oversqlite/upgrade.go` preimage and named scenario source. It adds only the FULL
writer DSN setting and actual per-opening FULL/WAL/FK witness, preserving pool4.
An inherited overlay or an existing source at the virtual test path is refused.
Generated bytes and their inputs remain bound through cleanup. Go jobs pin both
the launcher and actual Go1.25.12 engine/version file. Load build success earns
zero native body credit; only the complete admitted four-phase sequence can
qualify load. Public phase invocation remains rejected.

`--entry load-sequence` verifies the frozen, independently reviewed seven-role
Go prerequisite union and actual ACK/storage measurement before preparation or
fixture creation. Its two build jobs precede one host-exclusive1200-second fixture
lease; the four complete1/5/10/25-user members run serially against that same
authority and reset domain. Each phase includes exact native/user/file checks,
byte-preserving external HTTP ACK observation and read-only PostgreSQL sampling.
Observer copies and SQL overhead are included in the phase costs and are separate
from SDK memory claims. Member receipts remain `sequence_complete=false`; the
joint receipt can become complete only after all four members, preserved evidence,
source/build bindings, clock reconciliation and shared cleanup pass. Missing or
stale prerequisites fail before native work; no predecessor receipt stitching is
accepted. Docker name collisions and changed ownership/identity refuse deletion.

The opt-in Go fixture readiness channel uses `OVERSYNC_QUALIFICATION_READY_FILE`,
`OVERSYNC_QUALIFICATION_READY_NONCE` and the existing process-owner token. It
atomically publishes a0600 record in a canonical private directory, binding the
producer PID/group, binary hash, profile, actual database and generation, listener
and reset nonce. The receiver rejects stale, partial, duplicate, public, symlinked
or mismatched records and checks the reached authority/database before clients.
Ordinary fixture invocation keeps its existing log output and lifecycle.

`campaign.py profile --selection <private-json> --profile <profile-json>
--freeze <independent-freeze> --output <new-directory>` exports a manually selected
union to the existing campaign scheduler. The frozen selection names `requests`,
`selection_reason`, `workers`, `jobs`, and explicit `retained`/`blocked` rows.
Each job names one normal catalog `entry`, its `worker`, optional preparation
`context`, `depends_on` and `preparation_jobs`. Worker records bind absolute
`root`, `go`, `profile`, `freeze` and private `bindings` paths and their effective
environment. A private worker also declares its canonical private `directory`;
its Gradle, Konan, Kotlin, Yarn, npm and Go caches and `TMPDIR` must reside there. Complete
tracked/untracked source contents, HEAD/index and explicit root rebindings must
match. Mutable directories cannot overlap between private workers.

Retained evidence requires independently frozen passing `receipt` and eligible
`review` records with exact entry/hash and dependency reasons. Different command
coverage needs an explicit reviewed equivalence reason. Fresh root constituents
need reviewed `timing` records before a cumulative root launches; unknown costs
or a measured cap mismatch stop export. Context preparations are charged too.
The exporter preserves the Go planning/read-only boundary, normal300s caps,
global capacity-one device/measurement locks and capacity-one private worker
locks. It writes a standard `manifest.json`, version3 `plan.json` and explicit
selection ledger; `campaign.py run --manifest ... --plan ... --accept-selected
--authorize-product --max-workers 2 --records ...` remains the dispatcher.
An export or scheduler pass never grants native coverage or reuse acceptance.

The raw wire route retains its SDK writer. Browser/Wasm snapshot setup writes
the same five200-row batches through the ordinary server-write API before private
readiness, binding the nonce/user, frozen row hash and five committed bundle refs.
Setup earns zero native credit. The actual fresh follower download, worker apply
and full typed-state verification remain inside the unchanged180s body cap.
Only the mixed writer sets its caller soft push target to201 to preserve the
approved single201-row body; snapshot transfer/config limits remain defaults.

Core Browser/Wasm invocations also capture raw Mocha completion. Passing XML
and a zero Gradle exit code cannot hide a late failure or abort. The run-owned
hook contains only the reviewed deliberately injected worker faults: it
binds the creating test, exact module URL and Worker object, then cancels only
the matching expected ErrorEvent. Transport listeners and assertions still run;
unmatched worker/page errors reject admission. Hook/configuration bytes and raw
observations remain bound through cleanup.

This adapter is still under qualification. The complete selected native union
remains open in the current evidence ledger; the reviewed private rehearsal
retains its own frozen inputs and limited isolation claim.
Unsupported entries stop before launching native work. Linux runtime obligations
remain blocked by the owner's current disposition. A successful supported entry
does not close the full-profile gate or grant acceptance; the complete ledger and
independent review remain required. The original focused verifier/replay defaults
and their separate evidence remain unchanged.

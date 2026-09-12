# Proportionate Oversqlite validation

This runbook defines the owner-authorized maintenance policy for SQLiteNow and
Go-Oversync. Lane selection is manual and reviewer-confirmed. It does not accept a
product candidate, replace independent review, or authorize delivery. The ignored
paired protocol and convergent-sync specifications carry matching amendments;
historical campaign records retain their original requirements and results.

## Current replay workload roster

The owner retired the 50,000-row replay workloads on 2026-10-04. Active mock,
live, memory and platform replay tests cover 1,000 and 10,000 rows at name-field
widths of 16 and 1,024 characters. Use these four configurations when selecting
current replay validation. Larger-volume reintroduction needs a new owner request.

Historical 50K requirements, timeouts and receipts retain their original meaning.
Their preservation does not schedule a current 50K run or grant qualification
credit for its removal. Other governing correctness and qualification requirements
remain in effect. A charging budget of 50,000 work units is independent of row
volume and remains unchanged.

## Choose the gate before choosing commands

| Profile | Required evidence |
| --- | --- |
| Development | Exact regression and adjacent invariants; deterministic races and cancellation where relevant. |
| Maintenance candidate | Affected owning packages, supported platform scenarios and live boundaries, with documented reuse of unaffected evidence. |
| Initial protocol qualification / release | The complete expressly required matrix, including heavy, transport, Swift, supported database versions, generated consumers and packaging. |

Maintenance means a bounded correction or consumer change under an existing
accepted contract. A new wire or persistence format, changed safety/boundedness
contract, or supported-runtime boundary requires its own accepted qualification
profile; it cannot be classified as routine maintenance to bypass the full matrix.
Implementation, candidate qualification, independent review, owner acceptance and
release are separate gates. An owner-approved maintenance exception is prospective:
it never relabels old source-revision evidence as a current pass.

## Select the smallest sound set

Compare both complete worktrees with the last evidenced candidate, including
untracked tests and ignored authority. Map the change through production and test
dependencies, then select the union of these rows:

| Change | Fresh maintenance evidence |
| --- | --- |
| Common coordinator/recovery | Owning package; affected scenarios on supported runtimes executing the path; narrow live recovery/sequence coverage. |
| Persistence, transactions or replay | Durable rows, dirty/outbox, checkpoint/source sequence and restart/replay; affected drivers/platforms and live recovery. |
| Wire/codec/admission/compression implementation | Paired contract consumers first; affected target transports and live boundaries; byte/hash, raw/Fetch and browser network/CORS evidence as applicable. A contract change needs its own profile. |
| Swift ownership/cancellation | Swift ownership tests and the live caller-cancellation regression. |
| Direct Go sync/drain | Client package, focused concurrency checks, and one-call PostgreSQL regression. |
| Target-specific adapter | That target's platform and live tests; Swift if the Apple bridge is reached. |
| Generator/compiler/plugin | Owning tests, separate compiler/plugin invocations, affected generated consumers; production-output verification when outputs change. |
| Sample presentation only | Affected sample checks; expand if sync semantics or API usage changes. |
| Server/schema | Owning server/database families, contract-required PostgreSQL versions, relevant example flows and affected live clients. |

Buffering, batching, resource lifetime, unbounded-loop or transaction-scheduling
changes also select affected stress/boundedness lanes. Uncertain impact selects the
lane and records the uncertainty. Preserve deterministic barrier tests. For changed
Go concurrency, include focused `-race -count=1` evidence; ordinary race-scenario
tests and race-detector instrumentation are different claims. Keep compiler and
Gradle-plugin tests separate, and run the plugin task after plugin changes.

Before expensive runtime lanes, check paired specification/OpenAPI identity, corpus
manifest integrity, active consumers and focused wire boundaries. A host pass does
not replace platform or live evidence. Root Go `./...` does not cover the nested
mobile-flow module. The simulator's caller drain loop does not replace one public
`Sync(ctx)` call. Swift compilation or Kotlin cancellation does not replace Swift
caller-owned cancellation.

## Invalidation and reuse

Give every candidate lane one disposition: **fresh**, **reused**, **subsumed**,
**not applicable**, or **blocked**. Also retain failed/superseded attempts and
rerun-required entries. Each decision names the changed dependency or explains why
the lane is unaffected. A mandatory aggregate names its specification section,
exact command and trigger; an aggregate label alone is not a requirement.

Reuse requires matching relevant production dependencies, assertions, fixtures,
generated inputs, toolchain, runtime, command/selector and runner contract, with a
completed result, actual test counts and intact logs. Live evidence additionally
binds server artifact, database version/durability, configuration, fresh initial
state, authority scope and cleanup. The same URL is not authority identity.

Common-source changes cannot retain old platform results because the diff is small.
Execute affected scenarios on each relevant supported runtime. Unchanged build
outputs may be reused while invalidated test leaves execute afresh. Gradle
`UP-TO-DATE`/`FROM-CACHE`, Go `(cached)`, a skip and zero matching tests are not fresh
execution evidence. A broader run subsumes a required leaf only if its selector,
instrumentation, runtime, test count and authority conditions were actually covered.

Documentation/instruction-only edits refresh authority and review identity, not
runtime suites, unless requirements or behavior changed. Shared test-helper edits
invalidate their consumers; test changes do not automatically invalidate unrelated
server database families. Missing dependency/provenance evidence blocks reuse.

After a late fix: stop the frozen campaign, compare against that freeze, run focused
checks, record invalidated lanes, and refreeze. Maintenance reruns affected leaves
and only aggregates independently required by its accepted profile. Initial
protocol qualification and release keep their full final aggregates. Do not widen
maintenance merely to rebuild a whole campaign label.

## Focused review remediation

Use this loop for a bounded review correction under an accepted contract. It does
not change the qualification profile, waive required runtime evidence, or combine
review, owner acceptance and checkpoint gates.

1. Map each finding to its contract requirement, owning functions and regression.
   Read additional material when that map exposes uncertainty. Verify the paired
   candidate and capture complete dirty baselines before editing; reconcile drift
   explicitly rather than silently replacing the frozen identity.
2. Select validation before editing. Name affected dependencies and fresh, reused,
   subsumed, not-applicable and blocked lanes. Expand platform, live or aggregate
   coverage when the change invalidates it or the governing contract requires it.
3. Reproduce the counterexample, fix it, and finish source inspection before final
   validation. Include adjacent invariants such as error precedence, cancellation,
   preservation and the unchanged valid-input path.
4. Reuse packet helpers for a remediation delta. Generate identities, paired diffs,
   native receipt audits and seals from the declared path set. Reference intact
   prior evidence instead of duplicating whole campaigns; preserve failed and
   superseded receipts and keep sealed packets unchanged.
5. Copy reports immediately after each invocation, before another task can replace
   them. Match native case names to selectors and reconcile failures and skips;
   a successful wrapper or stale report directory is insufficient evidence.
6. Follow Drymint's emitted recovery once for the same unchanged request. If
   attribution remains unavailable, record the limitation and use its supported
   fallback; never label unattributed current-state inspection a clean delta.
   Exclude evidence and build/cache trees from indexing. Source preimages must
   live outside indexed scopes or use a non-source suffix such as `.go.preimage`.
7. Freeze after code, inspection and validation settle. Derive path counts from
   inventories. A late edit invalidates its affected leaves and requires a new
   identity; avoid provisional seals that imply completed validation.

Report scope expansion promptly: its cause, invalidated evidence, completed work,
remaining lanes and revised estimate. Retain the paired 45–60 minute overrun
checkpoint. This process reduces repeated work without accepting evidence reuse
or skipping any governing safety, platform or delivery gate automatically.

## Focused live commands

Run from the SQLiteNow root with Python 3.10+, Git, Go 1.25.12 and an explicit
`OVERSYNC_EXAMPLE_TEST_DATABASE_URL` pointing to an existing PostgreSQL service.
The service must allow creation/drop of isolated databases. Supply the URL through
the environment; do not put credentials in command arguments or evidence files.
Swift additionally needs Java, Xcode/Swift and the existing Gradle prerequisites.
These commands never provision Docker and never reuse an external HTTP authority.

```sh
python3 tools/qualification/oversqlite_smoke.py go-direct-sync
python3 tools/qualification/oversqlite_smoke.py swift-cancellation
```

Both accept `--go-repo /path/to/go-oversync` and `--output /new/artifact/directory`.
Defaults are the sibling Go checkout and a unique ignored `.artifacts/oversqlite-smoke/`
directory. An explicit output directory must be new and outside the checkouts or
Git-ignored. Results are `summary.md`, redacted raw logs and native Go JSON/Swift
xUnit evidence. No planner, scheduler, manifest or reuse-acceptance interface changes.

The launcher pins Go 1.25.12 and builds the checked-out Go fixture for both commands.
Go runs only `TestMobileFlow_DirectSyncDrainsSeveralAdmittedBatchesOnce`, uncached,
in the nested module. Swift incrementally prepares the existing debug XCFramework
and test bundle, starts its own fixture, then runs only the named second-push
cancellation test. Preparation can cost much more than test execution.
Swift uses the XCTest parallel runner with one worker to obtain native xUnit
output. It also requires the named start/pass in the verbose XCTest log, because
SwiftPM's XML alone does not distinguish skipped tests. Unknown output fails closed.

Every invocation assigns one unpredictable `oversync_smoke_<32 lowercase hex>`
database name. The integration helper creates that database and reports cleanup
errors. On exit, the launcher stops its owned children and invokes the same built
fixture in cleanup mode to drop only that exact database. It never enumerates or
drops other databases. Cleanup failure makes the invocation fail and leaves the
database name in the summary for explicit recovery. Do not run two launchers against
the same Swift build outputs concurrently.

| Boundary | Limit |
| --- | --- |
| Go fixture preparation plus test command | 240 seconds total; Go test timeout 3m30s, bounded further by remaining outer time |
| Swift fixture/runtime/test-bundle preparation | 15 minutes total |
| Swift authority readiness | 60 seconds |
| Selected Swift test command | 240 seconds |
| Owned process/database cleanup | 30 seconds, with forced process reap if graceful shutdown fails |

Limits are operational stop rules, not performance claims. At the first failure,
timeout, skipped/absent required test or identity mismatch, stop. Retain the attempt,
diagnose its owning stage and retry only with a recorded cause plus corrective
change or changed environment. Never automatically extend deadlines or retry an
unchanged aggregate. Database-backed families have independent budgets; do not
parallelize shared database/listener/application identities. Preserve normal
PostgreSQL durability and the existing macOS Docker-volume default.

## Ledger and policy examples

Use the existing campaign reporter for queue, lock-wait, execution, cleanup and
attempt records. Do not sum overlapping lane durations into campaign wall time.
Record unknown stage timings as unknown, never zero. Distinguish:

| Candidate/profile and lane | Disposition + dependency reason | Command/selector + counts/skips | Build/setup/test/cleanup wall time | Attempt + raw evidence | Identity/runtime/authority + reviewer |
| --- | --- | --- | --- | --- | --- |
| Current candidate | fresh / reused / subsumed / not applicable / blocked | Exact leaves and instrumentation | Separate spans or unknown | Retain every failed/superseded attempt | Both revisions, worktree digests and relevant hashes |

For the campaign record, add invocation elapsed, repeated-attempt elapsed, total
campaign elapsed, baseline exclusions, remaining blockers and explicit acceptance
status. Report measured savings and avoided invocations separately. No percentage
speedup or one-hour campaign budget is established by the current evidence.

| Example | Decision |
| --- | --- |
| Documentation-only correction | Document/link/authority checks; explain retained runtime evidence. No database suite. |
| Common recovery branch correction | Exact durable regression, client package, affected runtime recovery scenarios and narrow live recovery; preserve affected concurrency/cancellation checks. No unrelated compiler or server-schema sweep. |
| Swift cancellation ownership | Ownership races and fresh live second-push cancellation; affected Apple bridge/runtime checks. Kotlin-only evidence is insufficient. |
| Wire shape change | Settle paired specs/OpenAPI/corpus; select protocol qualification, both consumers and actual platform/live transports. No maintenance exemption. |
| Android adapter correction | Android focused/platform/live coverage; retain unrelated targets with dependency evidence. |
| Server schema change | Owning transactional/database families on required PostgreSQL majors plus affected real clients; mocks cannot replace database evidence. |

Harness acceptance uses controlled-process infrastructure tests, focused Go cleanup
tests, then one Go direct-sync and one Swift cancellation smoke sequentially. It
does not qualify the pending convergent-sync candidate. Independent review and owner
acceptance follow separately. Full campaign redesign, automatic lane selection,
cache redesign and multi-hour historical campaign reconstruction are deferred.

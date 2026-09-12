# SQLiteNow/Oversqlite cancellation execution and evidence

Date: 2026-09-30.

Status: **supported test-only slices implemented; focused development evidence
recorded below.** Full platform/live validation and the cancellation bound remain
open. The owner supplied CLEAN independent source reviews for the Swift/worker
remediation, core fixture consolidation, ordinary cancellation split and supported
replay/process slice. Those reviews grant no qualification, acceptance or delivery.
The owner also supplied a CLEAN independent source review of the subsequent
worker-crash/caller-cleanup slice. The checklist reconciliation below changes
documentation only; it is not a new runtime result. The later Swift live-runner
batch below now supplies five cancellation passes and the separate second-push
pass on macOS; its review findings were fixed and the owner declined rereview, as recorded below. Whole-caller death, remaining
platform/live obligations and the charged-work bound remain open. Gate U remains blocked.

The owner requested: “ok, prepare the spec and implement it”. This authorizes this
work in SQLiteNow; the sibling Go checkout remains read-only. The owner then
clarified: “if something is impossible to test (e.g. AndroidX driver limitation)
- then it is what it is, we should not try to implement some weird workarounds”.
Accordingly, keep existing drivers and supported facilities. No driver fork, raw
pointer reflection, substitute backend, custom cancellation framework or new
production observation API is part of this implementation. A capability gap
blocks its proof, not independent improvements to existing tests. This is not
a claim of compliance with an unproved runtime guarantee.

## Authority and scope

This is a focused implementation supplement to
[the local dependency-planning contract](oversqlite-dependency-planning.md),
[the SQLiteNow program specification](../specs/oversqlite-fk-safe-sync-and-automatic-resolution.md)
and [paired p0001](../../go-oversync/specs/p0001-oversqlite-fk-safe-sync-and-automatic-resolution.md).
It preserves their requirements; it does not replace the 256-work-unit bound
with a latency or VM-instruction bound. If a proposed implementation needs a
different guarantee, stop and settle a paired contract amendment first.

Preserve the sealed A3-05 plan (manifest SHA-256
`fd88e56797b6126198e91a4d72b70429eeb1acdf98016250754bbda736a69fa9`),
the authorized AGENTS overlay and existing Gate U evidence. The current
[author gap map](../.artifacts/p0001-fk-safe-sync-a3-05-gate-u/development/author-owner-gap-map.json)
remains the requirement-to-test/runtime ledger. Do not duplicate its twenty rows
in another inventory. This document supplies design decisions and test
dispositions, not replacement qualification receipts.
Its Swift owner/path/hash were refreshed under the
[naming overlay](../.artifacts/cancellation-simplification-2026-09-30/authority-overlay.json);
all requirement statuses and qualification credit remain unchanged. The overlay
retains the prior map verbatim for historical hash verification.

Scope: honest evidence for existing core SQLite execution, Oversqlite logical
work and cleanup, affected test orchestration, and Swift caller ownership. This
implementation changes tests and this specification only; production ownership,
accounting and backend behavior remain unchanged. No wire, persisted-format, public
page-size or new durable-journal change. No new generic operation coordinator.
Stage B reconciliation/network-resolution obligations remain outside Stage A;
they cannot be silently satisfied by the existing second-push Swift test.

## Required behavior

1. **Cancellation cadence:** check at every page boundary and within 256 charged
   logical work units, including final apply. Cover actual visits, failed and
   duplicate lookups, comparisons, conversions, mutations, restore/recheck
   passes and cleanup. Do not hide scans, sorts, recursive queries or trigger
   work inside one nominal step.
2. **SQLite interruption:** cancellation must reach the executing statement,
   including admitted trigger work. Establish the scope of preparation,
   execution, locks/busy waits, native callbacks/UDFs and I/O separately. A
   progress callback does not prove interruption while SQLite is outside VM
   dispatch. Unsupported required paths remain blocked, not implicitly exempt.
3. **Outcome truth:** before commit, cancellation preserves the required
   pre-transaction primary state. After a successful commit, preserve that
   commit and its exact prefix. An ambiguous remote response retains the exact
   frozen request; cancellation does not authorize a new source sequence.
4. **Cleanup and ownership:** finish or explicitly fail statement/transaction,
   buffer, reservation and HTTP cleanup before releasing the owning operation
   or lease. Retain ownership when cleanup is incomplete; discard a connection
   whose safe state cannot be established. Preserve primary error identity and
   ordered, nonduplicated cleanup failures.
5. **Replay:** preserve complete typed business data, A/B/provenance/relationships,
   desired L/dirty, outbox bytes/hash/source, source next-ID/checkpoint, and
   applicable opening/contract/admission/recovery identity. Assert the exact
   committed prefix, permitted durable staging, transient reclamation and
   replay outcome. State exclusions must be explicit and justified.
6. **Swift:** real caller Task cancellation maps to `CancellationError`, never a
   pending/limit result. Completion and ownership remain pending until Kotlin
   rollback/cleanup completes. Predispatch, reached apply and HTTP cancellation
   are distinct witnesses.

The 256-unit rule governs checking cadence. It is not evidence that rollback
finishes in 256 units or that wall-clock response is bounded. Cleanup work still
needs its own accounting, interruption policy and termination evidence; naming
it non-cancellable does not waive boundedness or justify early lease release.

## Architecture

### Preserve existing owners

| Owner | Responsibility |
| --- | --- |
| `SafeSQLiteConnection` and its backend adapter | Serialized execution, statement lifetime, interruption binding, authoritative transaction outcome, cleanup and reuse/discard decision. |
| `OversqliteClientOperationController` | One public invocation's admission budget, actual bounded execution/check discipline, cancellation/error precedence and committed progress. |
| `OversqliteApplyExecutor`, workspace and resource owners | Apply-mode/cache cleanup, buffer/reservation settlement, HTTP children and lease lifetime. Keep their distinct resources; make completion ordering explicit. |
| Swift operation handle/completion bridge | Forward caller cancellation and wait for the existing owned Kotlin operation to terminate. |

Do not introduce another cancellation registry, execution engine or persistence
owner. Keep Swift's current two-job forwarding and `cancelAndJoin` initially.
A one-job lifecycle is optional follow-up work, justified only by demonstrated
simplification while preserving client-close and background-operation semantics.

### Separate admission from execution

Keep reservation accounting for admission and public diagnostics. A reservation
may conservatively cover future work; its size is not a measurement of work
performed or of time between checks. Do not change existing `used`/`reserved`
wire or API semantics to make a test pass.

If production work resumes under a separately scoped change, express logical
execution in bounded chunks at existing owner loops. For each
chunk, document the conservative maximum actually executable between checks,
including nested work and SQL-induced visits. Check cancellation before entering
a chunk and at its completion; no unchecked sequence may cover more than 256
logical units, and every page boundary must check. Larger work is divided at
safe execution boundaries without splitting an atomic commit. Atomic allowance
remains admitted before destructive work.

Prefer analytic bounds plus independently counted fixture visits over an
always-on per-step telemetry framework. If an existing supported observer exposes real checks or execution boundaries,
use it; adding new observation infrastructure is outside this test-only slice. It must not report assigned costs as
independent proof of those same costs. Review wide rows, deep values, duplicate
lookups, maximum pages and restoration passes, not only one-row pages.

### Backend capability boundary

Record the capabilities of the existing backend; do not manufacture missing
capabilities to satisfy this test effort. If a supported upstream seam becomes
available in future, assess it beneath `SafeSQLiteConnection` in a separate
implementation change. The desirable contract is described here for assessment,
not as an instruction to build another adapter now. It must bind cancellation to the correct executing
operation, avoid late cancellation affecting a successor, quiesce/disarm the
operation's interruption mechanism before cleanup, and report authoritative
transaction/cleanup outcome. Do not expose a public raw-pointer API or use
private-pointer reflection as the production route.

Keep VM progress observations in VM units. Neither the current 1,000-instruction
worker callback interval nor changing it to 256 proves the logical bound. The
implementation needs a conservative argument connecting the admitted SQL work
to the required logical-work discipline, as well as real backend interruption
evidence. A second VM guarantee may supplement, but cannot replace, the existing
contract without a separately settled contract change.

| Backend inspected | Current capability | Prerequisite |
| --- | --- | --- |
| Default JS/Wasm worker | Shared atomic cancellation flag and SQLite progress handler around page stepping. | Prove interruption after actual VM execution starts, preparation/lock behavior, cleanup disarming, and logical-work coverage. Existing pre-step hold is insufficient. |
| JVM/Android bundled AndroidX 2.7.1 | Inspected wrappers expose prepare/step/close and private native pointers; non-web async extensions call synchronously. | Record running-SQL interruption/cadence as unproved with this wrapper. No source patch/fork or private pointer access in this work. |
| Kotlin/Native bundled AndroidX 2.7.1 | Native connection/statement wrappers call SQLite directly but hide connection ownership. | Record the same limitation on Apple/Linux. Do not switch SQLite identity or bypass core ownership. Dynamic extension loading is not a portable workaround; inspected source handles its omission on iOS. |
| Sibling Go driver | Pinned go-sqlite3 context execution invokes interrupt and joins its executing goroutine. | Context for design only. Its call-entry race and charged-work coverage still need owning evidence; no Go modification or current proof is implied. |

Source anchors: [bundled opener](../library-core/src/bundledDriverMain/kotlin/dev/goquick/sqlitenow/core/BundledOpen.bundledDriver.kt),
[worker](../library-core/src/webMain/resources/sqlitenow-worker-v1/sqlite-3.53.0-build1/worker.mjs),
[operation controller](../library-oversqlite/src/commonMain/kotlin/dev/goquick/sqlitenow/oversqlite/OversqliteClientOperationController.kt),
[final apply](../library-oversqlite/src/commonMain/kotlin/dev/goquick/sqlitenow/oversqlite/OversqliteApplySurvivors.kt).
The pinned dependency sources were inspected in Gradle's 2.7.1 source jars:
`SQLiteAsync.nonWeb.kt`, `BundledSQLiteConnection.jvmAndAndroid.kt`,
`NativeSQLiteConnection.kt`, `NativeSQLiteStatement.kt` and `NativeSQLite.kt`.

### Observable completion, without another framework

Use operation-local observation at existing execution/ownership boundaries:
execution actually started, cancellation observed, transaction terminal outcome,
cleanup complete. Use existing facilities only; if a required event is not observable, retain an
explicit limitation. Keep observations bounded and separate from scheduler control.
Backend callbacks must not suspend or re-enter the connection. A deterministic
VM witness can signal actual progress to an independent caller and continue
running until that caller cancels; a hold before `step` is not that witness.

Retain genuine failure injection for failures that cannot be reproduced reliably,
such as finalization failure or lost response. Classify injected error transport
separately from physical cleanup failure and real caller cancellation. Do not
delete web response acknowledgements and undelivered-handle reclamation: they
protect ownership across asynchronous delivery and late successful COMMIT.

The authorized Swift bridge remains until an equivalent reached-apply witness
exists. Make that witness separately identifiable and fail or report blocked
when its required capability is absent. A test variant silently compiling out
the central assertion must not retain a passing required witness identity.
Moving its hook into an adapter is not a simplification by itself.

## Test consolidation and required assertion preservation

### Name behavior, not the campaign

Test files, classes and methods describe their behavior and owning layer. Do not
introduce plan-stage prefixes (`A1`, `A2`, `A3`), review-round prefixes or date
labels in active test identities. Use `Jvm`, `Worker`, `Live` or `Swift` only when
the runtime or boundary explains distinct coverage. Requirements belong in the
existing requirement map, not in test names.

This change renames `A1SurvivorsTest` to `SurvivorApplyTest` and
`SyncA3CancellationTests` to `SyncCancellationStateTests`. The four `review…`
method prefixes in the survivor suite are also removed. Each method retains its
behavior; the old names are recorded here solely for migration. Existing native
reports and sealed selectors retain their historical names. New runs must use
the new identities; do not add compatibility test aliases that execute twice.

Other campaign-named suites should migrate when their owning behavior is changed,
with active selector references updated in the same batch. Preserve historical
receipts. The existing `A3_TEST_BRIDGE` build flag and exported probe are separately
wired interfaces; renaming them requires a coordinated build/interface change,
not a text replacement inside a test-only cleanup.

### Preserve assertions while reducing setup

Use shared mechanical fixtures and a canonical, explicitly ordered typed state
oracle. Keep behavior assertions visible in their owning tests. A common test
body can have several runtime leaves; host execution does not replace those
leaves. Retire a test only after mapping every assertion to an executed replacement
at the required owning layer/runtime. Preserve historical failed/superseded data.

| Existing family | Required disposition |
| --- | --- |
| Core isolation, close and statement-close tests | Keep cancellation/commit outcomes, cleanup failure ordering, sharing/reentrancy, reuse/discard and real-connection exclusion. Consolidate recording-backend and barrier setup. |
| Worker long-query and wrapper transaction tests | Keep as page-boundary rollback tests with small SQL. The wrapper hold now arms after BEGIN and the write; assert exactly one worker rollback and zero statements. A true in-VM witness remains unproved and is not supplied by these tests. |
| Worker late COMMIT, queued/active cancellation, undelivered open/prepare/page and invalid reconciliation | Keep each outcome; parameterize repeated acquisition/reclamation mechanics. Appended failure strings prove transport, not real finalize/rollback failure. |
| A1 rollback and common mock-cancellation tests | Require the precise cancellation outcome, not any failed Result. Consolidate duplicate frozen-intent/replay setup while preserving pull checkpoint, gate reuse, source IDs and request counts. |
| A2 graph/resume and A3 workspace/handle tests | Keep planning, useful progress and reservation/lease assertions. Do not relabel advance reservation or injected failure as caller responsiveness. |
| A3 progress-finalization/public progress-failure JVM tests | One owner-level error-precedence table plus minimal public wiring cases; replace reflective private mutation and literal-SQL reachability where a constructed provider suffices. Preserve typed cause, suppression once, state and gate reuse. |
| A3 ordinary response-barrier and ten apply cuts | Keep genuine caller cancellation, joined cleanup, full primary rollback, permitted completed snapshot stage, zero transient reservations/apply mode, exact frozen request and sole confirmed source increment. Split refusal from cancellation; expose cuts as identifiable leaves with shared setup. |
| A1/A3 replay and ordinary recreation/death | Consolidate duplicate state/replay assertions, but retain separate same-lifetime replay, orderly connection reopen and actual process/worker death. Node transient replay is not durable restart. |
| A1/A3 real-server response loss | Keep real committed-response-loss and server-vector evidence independently of mocks. Reuse mechanics; no credit for disabled registration. |
| WorkerOversqliteEvidenceTest six-phase cancellation table | Separate identifiable leaves; preserve backend cleanup/reuse. Label injected exceptions accurately so one early failure cannot hide later coverage. |
| Gzip fake-engine and real JVM compressor tests | Keep error classification separately from actual first-write cancellation, partial consumption, close exactly once, zero HTTP, frozen intent and exact replay. No automatic non-JVM credit. |
| Swift OpeningCompletionTest and static/generated ownership scenarios | Keep completion/installation/close races and generated consumer coverage. Reject successful completion of an already-cancelled Task; shared source already exists and need not be duplicated. |
| Swift A3 cancellation workflow | Split admission/predispatch from reached conflict and transport witnesses. Replace the incomplete unordered vector; compare original/replayed request identity and bytes, not hash length plus eventual drain. Keep the bridge until replacement is proved. |
| Swift second-push live test | Keep its separately required fixture and selector. It proves caller HTTP cancellation/no third push; the fixture holds before forwarding and does not prove committed-response replay. |
| HTTP ownership, statement cache, opening, detach/watch and migration cancellation | Keep at the resource/lifecycle owner; database replay does not subsume them. Share mechanics only. |

Use the existing parent runner deadline for potentially stuck native work. Bound
arrival and post-cancel completion observation; on failure retain the last reached
phase and native result, then terminate only run-owned resources. Do not put an
uncooperative child inside a timeout scope that cannot return until that child
finishes. A watchdog timeout is inconclusive/failure evidence, never a work-bound
pass. Avoid one bespoke watchdog or receipt protocol per scenario.

## Actionable checklist and current disposition

This checklist replaces the coarse batch sequence. Its supported batches were
subsequently authorized and source-reviewed as recorded below. It does not
authorize unchecked capabilities, contract amendments or qualification. Checked
items mean **implemented and development checked**, not independently reviewed,
qualified or accepted. The gap map remains
the sole requirement-status ledger; this checklist describes changes and exits.
No new cancellation guarantee or platform exemption is introduced here.

### Completed development work

- [x] **Correct cancellation and rollback assertions.** Core worker tests now
  arm the hold after the transaction write, use small SQL, and check exactly one
  worker rollback plus zero live statements. `SurvivorApplyTest` distinguishes
  cancellation from the injected ordinary failure. Preserve the inverse late
  successful COMMIT case. Development evidence: three core Node leaves and all
  eighteen survivor JVM tests pass; the restored negative control confirms the
  runner observes asynchronous assertion failures. These are boundary and
  rollback witnesses, not in-VM or charged-work-bound evidence.
- [x] **Simplify worker orchestration and affected names.**
  `WorkerOversqliteEvidenceTest` has six independently reported leaves sharing
  storage cleanup; remove its callback table/scenario class and ineffective
  fixed-iteration close-barrier polling. All six Node leaves pass. Rename the
  affected survivor and Swift suites and local helpers by behavior, remove the
  four `review` method prefixes, and update the current Swift owner reference.
  Historical receipts retain their original names.
- [x] **Strengthen Swift observations without new production hooks.**
  `SyncOwnershipScenarios` rejects successful pre-cancelled completion and drops
  an identical conditional branch. `SyncCancellationStateTests` uses ordered,
  typed state, tests integer/REAL/blob/null preservation, and compares retained
  frozen request/source state across two cancelled attempts. Two local checks
  pass. The live body only compiles and skips; its new assertions remain unproved
  at runtime. Missing bridge support cannot silently produce a passing live test.

### Review and capability decisions

- [x] **Review the spec, checklist and existing diff together before more code.**
  The reviewer checks every disposition against the current gap map and governing
  contract, inspects the negative control and native cases, and distinguishes
  admission reservations, performed logical work and VM instructions. Confirm
  which checked items are correct and which need remediation. Exit: explicit
  findings and a review disposition, including unperformed live/platform checks;
  no inferred acceptance or Gate U completion. Review is read-only unless
  remediation is separately directed. The owner supplied CLEAN source re-reviews
  of remediation, core fixtures, ordinary cancellation, replay/process recovery
  and worker-crash/caller cleanup. The supplied reviews cover their declared
  deltas, not new changes; no owner acceptance is inferred.
- [ ] **Settle the qualification consequence of unavailable proof.** The owner
  decides whether the existing 256-charged-step requirement continues to block
  qualification, or requests a separate amendment to the paired governing
  contracts. Identify the affected guarantee and runtime explicitly. Accepting
  a testing limitation alone does not waive a behavioral requirement. Exit:
  record that decision in the current gap map; an amendment, if chosen, must be
  settled before dependent production work. No AndroidX fork, private-pointer
  access, alternative driver or extra counter framework is an option in this
  checklist. Independent test cleanup below does not close this decision.

### Implementation items after review

- [x] **Consolidate core cleanup fixtures at the connection owner.** Work in
  [SafeSQLiteConnectionIsolationTest](../library-core/src/jvmTest/kotlin/dev/goquick/sqlitenow/core/SafeSQLiteConnectionIsolationTest.kt)
  and [SqliteWorkerDriverTest](../library-core/src/webTest/kotlin/dev/goquick/sqlitenow/core/worker/SqliteWorkerDriverTest.kt).
  Share repeated recording-backend and resource-delivery setup within each
  owning suite; keep separate leaves for rollback, successful late COMMIT,
  cleanup failure order, reuse/discard, queued cancellation and undelivered
  open/prepare/page handles. Remove a helper or duplicate only when its entire
  assertion union has an executed replacement. Exit: focused isolation JVM and
  worker Node cases preserve those outcomes, with affected web cases additionally
  run on their supported browser lanes before claiming platform equivalence.
  Reuse existing acknowledgements and holds; do not create a generic scheduler.
  Development checks completed: 44 isolation JVM cases, 8 focused Node cases,
  and the 3 changed delivery cases on both JS and Wasm ChromeHeadless. See the
  fixture-consolidation record below. The owner subsequently supplied a CLEAN
  source review of that slice; this does not review later changes.
- [ ] **Replace reflective progress-failure setup where existing construction permits.**
  Owners: [ProgressFinalizationJvmTest](../library-oversqlite/src/jvmTest/kotlin/dev/goquick/sqlitenow/oversqlite/ProgressFinalizationJvmTest.kt)
  and [PublicProgressFailureJvmTest](../library-oversqlite/src/jvmTest/kotlin/dev/goquick/sqlitenow/oversqlite/PublicProgressFailureJvmTest.kt).
  First establish that the existing connection/provider construction can inject
  the failure without mutating private `ref` fields. Then keep one owner-level
  error-precedence table and only the public cases needed to prove wiring.
  Preserve exact typed cause, cancellation precedence, suppression exactly once,
  full state and released operation ownership. Exit: both focused JVM suites
  pass with the assertion union mapped before deletions. If construction cannot
  reach a case, retain it and report that limitation; no new production hook.
  Assessed: blocked by the current cross-module construction boundary. Both
  suites and all their assertions remain unchanged; see the evidence below.
- [x] **Make ordinary cancellation cuts independent and share their fixture.**
  Owners: [OrdinarySafetyPlatformTest](../platform-oversqlite-test/composeApp/src/commonTest/kotlin/dev/goquick/sqlitenow/oversqlite/platform/OrdinarySafetyPlatformTest.kt),
  [OrdinaryFixtureSupport](../platform-oversqlite-test/composeApp/src/commonTest/kotlin/dev/goquick/sqlitenow/oversqlite/platform/OrdinaryFixtureSupport.kt)
  and [DatabaseStateAssertions](../platform-oversqlite-test/composeApp/src/commonTest/kotlin/dev/goquick/sqlitenow/oversqlite/platform/DatabaseStateAssertions.kt).
  Separate admission refusal, response-barrier cancellation and each of the ten
  apply cuts into native leaves, sharing mechanical seeding and typed state
  capture. Preserve full primary rollback, permitted completed snapshot staging,
  cleared reservations/apply mode, exact frozen replay and the sole confirmed
  source increment. Keep assertions visible in each behavioral case. Exit:
  JVM and Node focused leaves execute with no cut hidden by an earlier failure;
  the current runtime map selects the remaining affected platform/live leaves.
  An unavailable required leaf stays open and prevents retirement of its witness.
  The ordinary split below supplies development evidence for the replacement
  leaves. Mobile/live obligations and the unavailable Node durable-reopen branch
  remain explicit; this check mark closes the development implementation only.
- [x] **Split the Swift workflow by capability and behavior.** Owner:
  [SyncCancellationStateTests](../swift/support/Tests/SQLiteNowSyncSupportTests/SyncCancellationStateTests.swift).
  Extract one local fixture owner for opening/client/core cleanup; separate
  admission/predispatch, reached-conflict rollback, frozen-push retry and snapshot
  cancellation into native tests with independent initial state. Scope the bridge
  prerequisite to the reached-conflict leaf; identify the live authority required
  by every other leaf. Preserve `CancellationError`, joined Kotlin cleanup,
  typed state, retained pending decision and replay assertions. Keep the existing
  second-push live regression separate. Exit: static ownership/vector checks and
  generated consumers that actually compile the changed branches pass; both
  bridge configurations register the intended leaves. Execute affected live
  leaves only with an identified compatible authority. Compilation or a skip
  cannot close this item, and arrival counts cannot prove HTTP-byte equality.
  Source split and per-leaf prerequisites are now implemented by the review
  remediation below. The later live-runner batch executes all five leaves on
  macOS: four with the default runtime and reached-conflict with the bridge. This
  closes the decomposition development exit, not qualification or runner review.
- [ ] **Share replay setup while keeping different failure boundaries.**
  Owners: `SettlementReplayTest`, `ProcessDeathRecoveryJvmTest`,
  `WorkerCrashReplayTest` and `OrdinarySafetyLiveTest` from the inventory
  above. Reuse seeding, typed state capture and exact request/source comparisons;
  keep distinct same-lifetime replay, orderly reopen, real process/worker death
  and server-committed response loss cases. Preserve committed prefixes, durable
  stage identity, byte/hash/source equality and independent server readback.
  Exit: each removed assertion has a passing replacement in its original runtime
  and failure boundary. Use existing parent-process/worker and live fixtures;
  transient Node storage never supplies durable restart evidence. Leave gzip,
  HTTP, cache, opening, detach/watch and migration owner tests in place unless
  their separate assertion union is also demonstrated. The settlement/client/reopen
  and JVM process portions are implemented below. Worker crash followed by
  surviving-caller cleanup now passes on both browser targets. Whole-caller death
  and live replay remain open; Android/iOS replay execution remains unperformed.
- [ ] **Bound orchestration failure using existing runner ownership.** Inspect
  arrival waits and cancellation joins in the affected core, ordinary and Swift
  suites and their current parent runners. Use existing per-run deadlines and
  run-owned process cleanup so a stuck native call does not strand the runner.
  Preserve the primary failure and last reached phase. Exit: a controlled missing
  arrival or noncompletion produces a bounded failed/inconclusive native result
  and releases run-owned resources. A coroutine timeout that still waits forever
  for its child is insufficient. If the existing runner cannot supply this,
  record the unsupported route; do not add a per-test watchdog framework or
  count a timeout as cancellation-bound proof. The ordinary split below now
  checks controlled missing arrival and unconditional barrier release/join;
  the JVM process runner now checks missing arrival and forced child termination
  as described below. Uninterruptible calls in the parent and the other runner
  families remain open.
- [ ] **Finish behavior-based naming in each changed family.** Rename remaining
  active stage-prefixed test files/classes/methods when performing the owning
  items above; update active selectors and current gap-map references together.
  Use names such as `ProgressFinalizationTest`, `CallerCancellationTest` and
  `WorkerRecoveryTest` only where they describe the actual suite. Remove obsolete
  aliases rather than running both names. Exit: no active reference to a retired
  selector, expected native leaves register and execute, and no assertion is
  dropped. Keep sealed/historical names intact. Bridge flags, exported symbols
  and source-set names remain a separate build/interface change; renaming them
  is not required to close test-file naming. Implemented slices rename the ordinary
  safety, settlement replay, JVM process-recovery and worker-crash suites and
  active selectors.
  The dedicated cleanup below also renames the remaining 17 local `A3` Kotlin
  test/helper files and their active consumers. Live and heavy discovery is
  separate from executed behavior; those execution obligations remain open.
  The paired follow-up below also renames both shared-contract test consumers.
  Shared fixture names and bridge/build interfaces retain their existing names.
- [ ] **Close only the implemented slice with proportionate evidence.** After
  each coherent batch, compare the original captured dirty baseline and exact
  allowlist, update affected dispositions in the existing gap map and run the
  owning checks described above. Before marking the slice ready for independent
  review, inspect the final diff and retained failure/skip reports, account for
  fresh/reused/subsumed/not-applicable/blocked leaves, and verify required hashes
  and overlays. Exit: every removed test/hook has an evidenced replacement;
  every unavailable proof is still explicit. Qualification, acceptance and
  delivery remain separate. Do not retire evidence merely to make the list shorter.
  Supported source slices now have retained development evidence and
  owner-supplied CLEAN source reviews. Overall closure stays unchecked because
  required live/platform replacements and capability exits remain unproved.
  This documentation reconciliation is a later two-path delta, not covered by
  those source reviews.

### Blocked or conditional work; no workaround implementation

- [ ] **Running-SQL and charged-work proof — blocked.** Keep non-web backend
  interruption and the cross-layer 256-step proof open until the owner decision
  above or a supported upstream capability supplies a route. For the worker,
  first determine whether existing facilities can observe execution after the
  VM starts; a pre-step hold is insufficient. VM instruction counts and admission
  reservations are not performed Oversqlite work. Exit requires evidence for the
  actual retained guarantee, or an explicitly approved contract amendment; no
  test-only proxy can close it.
- [ ] **Remove the Swift bridge or merge lifecycle owners — conditional.** Do
  not begin this as cosmetic consolidation. First show an existing supported
  replacement for reached-apply observation and preservation of cancellation
  forwarding, `cancelAndJoin`, client close and background-operation semantics.
  Separately review production/build/export impact. Without that evidence, keep
  the authorized bridge and owners; do not move the same machinery behind a new
  adapter and call it removed.

### Remaining work after the completed source reviews

The owner supplied CLEAN independent reviews for all implemented source slices,
including the four-path worker-crash change. These are reported reviewer
conclusions, not new independent review or runtime execution by this author.
Earlier sections retain their historical author-time review requests; this is the
current disposition. Unchecked items are not an undifferentiated coding backlog.

| Remaining checklist item | Current disposition and prerequisite |
| --- | --- |
| Qualification consequence / running-SQL and charged-work proof | Owner contract decision remains open. Preserve the current requirement and blocked status by default. Any amendment must name the affected runtime and guarantee and settle the paired contract before dependent implementation. Existing worker interruption is real; its VM interval and reserved-work counters do not measure performed Oversqlite charged work. |
| Reflective progress setup | Retain both existing suites and their assertions. Current construction does not offer an equivalent supported injection route. Reassess only with a concrete supported construction capability; no new hook or visibility bypass. |
| Swift split | Source implementation and source review are complete. The later batch below supplies five live macOS passes: four default-runtime leaves and reached-conflict with the bridge. The new runner delta awaits independent review; no qualification credit is added. |
| Replay consolidation | Same-lifetime/client/orderly-reopen, JVM process cuts and worker-crash-with-caller-cleanup slices are implemented and reviewed. Later live slices below supply JVM/macOS and normal Wasm-browser committed-response replay/readback. The six configurations are now independent tests. Three JS configurations pass; diagnostic timing places the 10k/1024 timeout during final readback after a 151.1-second successful replay call. Both 50k cases remain unexecuted. Android/iOS, Node and remaining heavy live replay stay open. Whole-caller death and complete public-phase coverage need a separately scoped feasibility/design decision; the worker result cannot replace them. |
| Runner failure bounds | Controlled missing-arrival checks exist for worker close, ordinary barriers and JVM child processes. Termination of uninterruptible parent work and the remaining Swift/runner families are unproved. Require an existing external runner route and an executable negative case before a further implementation batch; no per-test watchdog framework. |
| Remaining behavior names | All 19 Kotlin test/helper files from the cleanup inventory now use behavior names, including the two paired shared-contract consumers. Current selectors and helper callers follow the new names. Shared fixture names, production accounting names and bridge/build interfaces remain separate; renamed live/heavy discovery does not close execution obligations. |
| Swift bridge / lifecycle consolidation | Conditional; retain the separately authorized bridge and current owners. No supported replacement preserving reached-apply observation and lifecycle behavior has been established. |

At reconciliation time no further source batch was selected. This was a bounded
assessment of the current checklist and inspected facilities, not a claim that
future supported routes are impossible. The smallest useful next work is a
read-only live-authority readiness assessment: identify the existing users-mode
fixture, required registration/barrier capabilities, server/database identity,
cleanup ownership and exact Swift selectors. A URL alone is insufficient. Only
if those prerequisites resolve should a bounded live execution batch be proposed;
otherwise report the concrete mismatch without changing the fixture or driver.
That assessment would not resolve the separate charged-work contract decision.

That reconciliation session checked only environment-variable presence: no configured Swift/live
URL or database URL was present. It made no endpoint calls and established no
server availability or incompatibility. No runtime, device or authority was
launched. Under the validation policy's documentation-only rule, no tests/builds
were rerun because requirements and source behavior did not change. Current
requirement statuses, qualification credit and Gate U's blocked state remain
unchanged. The [reconciliation verification](../.artifacts/cancellation-checklist-reconciliation-2026-09-30/verification.json)
and [authority overlay](../.artifacts/cancellation-checklist-reconciliation-2026-09-30/authority-overlay.json)
bind this documentation update to its captured preimages and prior author receipt.

## Development validation before review remediation (historical)

Follow [proportionate validation](oversqlite-validation.md). A backend or guarantee
change is not routine maintenance merely because its patch is small. Retain all
applicable initial-protocol/release obligations; development probes do not count
as qualification. Required platform/live leaves remain explicit.

Focused author commands executed (Gradle commands also use
`--rerun --console=plain --no-watch-fs`):

```sh
./gradlew :library-oversqlite:jvmTest --tests '*SurvivorApplyTest*'
./gradlew :library-core:jsNodeTest --tests '*transactionsRollbackAndConnectionRemainReusableAfterActiveCancellation*' --tests '*safeTransactionCancellationRollsBackOnceAndKeepsConnectionReusable*' --tests '*lateSuccessfulCommitCancellationKeepsCommittedDataAndConnectionReusable*'
OVERSQLITE_PLATFORM_TESTS=1 OVERSQLITE_REALSERVER_TESTS=false OVERSQLITE_REALSERVER_HEAVY_TESTS=false ./gradlew :platform-oversqlite-test:composeApp:jsNodeTest --tests '*injected*CancellationCleansUpAndRemainsReusable*' --tests '*callerCancellationDuringWorkerCloseCleansUpAndRemainsReusable*'
env -u OVERSQLITE_SWIFT_CONVERGENT_BASE_URL -u OVERSQLITE_A3_SWIFT_TEST_BRIDGE swift test --package-path swift/support --filter 'SyncCancellationStateTests|testAlreadyCancelledTaskAndHandleInstallationRace'
```

The Swift command was also compiled/executed with
`OVERSQLITE_A3_SWIFT_TEST_BRIDGE=1`. Both modes use existing runtime frameworks;
their development results do not qualify those reused artifacts.

| Lane / disposition | Observed result and evidence |
| --- | --- |
| Survivor JVM — fresh | 18 executed, 0 failures/skips: [native report](../.artifacts/cancellation-simplification-2026-09-30/survivor-apply-jvm.xml). |
| Core worker Node — fresh | 3 executed, 0 failures/skips: [native report](../.artifacts/cancellation-simplification-2026-09-30/core-node-final.xml). A temporary deliberately false rollback assertion produced the expected failure after asynchronous execution, then was restored before the final pass: [negative control](../.artifacts/cancellation-simplification-2026-09-30/core-node-negative-control.xml). |
| Oversqlite worker Node — fresh | 6 separately registered leaves pass, with all six completion markers: [native report](../.artifacts/cancellation-simplification-2026-09-30/platform-node.xml). The build log also contains Wasm-loading warnings outside these native cases; this is no credit for other Wasm/browser paths. |
| Swift default — fresh local checks; live blocked | Ownership and typed-vector checks pass; the live workflow explicitly skips without the bridge: [log](../.artifacts/cancellation-simplification-2026-09-30/swift-default-final.log). |
| Swift isolated bridge — fresh local checks; live blocked | The reached-conflict branch compiles; 2 local checks pass and the live workflow skips because no authority was launched: [log](../.artifacts/cancellation-simplification-2026-09-30/swift-typed-vector-final.log). |
| Generated Swift selector — not applicable to this changed method | The shared source compiles, but this method belongs to its static-support conditional branch. The attempted generated selector matched 0 tests and earns no execution credit: [log](../.artifacts/cancellation-simplification-2026-09-30/swift-generated-ownership.log). |
| Production, compiler/plugin and server packages — unchanged | No production or build wiring was edited. Existing build outputs were reused for these development leaves; no historical runtime result is promoted to fresh proof. |
| Other platform/live leaves — blocked/unperformed | Renamed or changed bodies invalidate their old selector/body evidence. Android, Apple/Linux KMP, durable browser and live results are still required by their governing profile. No host substitution or bound proof. |

JVM/Node are development witnesses, not replacements for Android, Apple, Linux
or durable browser paths.
Use the existing sealed commands/runtime map for required native/live leaves;
settle any new backend task before claiming it is runnable. Swift's existing
`tools/qualification/oversqlite_smoke.py swift-cancellation` route is separate
from the A3 users-mode fixture. Neither fixture readiness nor a full qualification
run is asserted here. Do not launch an authority merely to discover its identity.

Continue recording fresh, reused, subsumed, not-applicable, blocked and invalidated
evidence in the current gap map when qualification proceeds. A mandatory
aggregate must name its governing section, command and trigger. Test registration,
compilation, method/tag counts and old passes cannot substitute for executed
behavior. Preserve failed attempts; repeat only after a diagnosed change.

## Current limitations and follow-up boundary

Non-web in-statement interruption, the relationship between SQL work and the
256-unit checking requirement, and a true in-VM worker witness remain unproved.
The owner directs this effort to accept such testing limitations rather than
add workarounds. These limitations do not become passes, deleted requirements or
implicit platform exemptions. Any later release/qualification decision must
address them explicitly under its own authority.

The Swift proxy exposes arrival counts, not request bodies. The strengthened
Swift test asserts complete retained frozen request/source state across two
cancelled attempts and eventual drain; that live body has not been executed.
It does not independently observe replayed HTTP bytes. Existing owning Kotlin/live byte-replay requirements stay
in the gap map. No sibling fixture change is introduced to manufacture that proof.

Keep the authorized Swift bridge and current job/lease implementation. Broader
reflection removal in JVM diagnostic tests, full fixture consolidation, bounded
post-cancel runner observation and independently named Swift subscenarios remain
follow-up work if they can use existing facilities and preserve their assertion
union. Do not declare this small test change to be a full architectural redesign.

Historical source hashes and native rosters for changed tests remain historical;
they cannot be reused for changed bodies. Leave the sealed packet intact. Record
new validation separately, with the changed source hashes and explicit limitations.
No Gate U completion, qualification, independent review or acceptance is inferred.

## Focused remediation of independent source-review findings

The owner authorized fixing the two P2 findings and the P3 attribution limit.
Only `SyncCancellationStateTests.swift`, `WorkerOversqliteEvidenceTest.kt`, this
document and the current Swift requirement-map references change in this batch.
No production, build wiring, sibling Go source or backend capability changes.

- **Swift prerequisite scope (P2):** replace the combined live method with five
  independently registered tests. Only `testReachedConflictCancellationRollsBackOwnedState`
  uses `A3_TEST_BRIDGE`. Snapshot admission/predispatch, conflict
  admission/predispatch, push retry and snapshot cancellation require the users-mode
  live authority but no bridge. Shared local helpers own connection open/close,
  seeding and conflict preparation; each test gets an independent user and files.
  Cancellation tasks are cancelled on fixture-body exit. The reached-conflict
  test retains same-owner retry after rollback. No HTTP-byte observation is added.
- **Worker arrival deadline (P2):** the existing close test now waits inside
  `withContext(Dispatchers.Default)` and `withTimeout`, polling with `delay`
  instead of `yield` on the virtual test dispatcher. This follows the existing
  real-time wait pattern in `RealServerHarnessBundleChangeWatchTest`. The close
  child is cancelled and joined in `finally`, including when arrival fails.
  `missingWorkerCloseResponseTimesOutWithoutAdvancingVirtualTime` deliberately
  never signals arrival and requires a timeout, bounded real elapsed time and
  unchanged virtual time. The 50ms regression deadline is a fixture timeout,
  not an Oversqlite work-bound assertion. Its initial lower-bound duration check
  failed and was replaced by the relevant completion bound; that attempt is retained.
- **Original Swift attribution (P3):** recovered the exact dirty preimage from
  this chat's recorded tool output. Its SHA-256 is
  `5b5c3b640adab1eca6fe94387feb976fa73fed65feacc33e0e999c650cc7ecbb`,
  matching the original entry baseline. This is not a reconstruction from HEAD.
  The [recovery record](../.artifacts/cancellation-review-fixes-2026-09-30/preimage-recovery.json),
  [source contents](../.artifacts/cancellation-review-fixes-2026-09-30/SyncA3CancellationTests.swift.original.preimage)
  and [original-to-current diff](../.artifacts/cancellation-review-fixes-2026-09-30/swift-original-to-current.diff)
  make the previously unavailable assertion comparison reviewable.

The source comparison retains these original assertion groups:

| Original dirty-source region | Current owner and preservation |
| --- | --- |
| Snapshot admission/predispatch, lines 44–65 | `testSnapshotAdmissionAndPredispatchCancellationPreserveOwnedState`: same limit dimension/reservation/progress checks, cancellation error, pending count and original business row. |
| Peer conflict setup and limited decision, lines 70–117 | `prepareConflict` plus `testConflictAdmissionAndPredispatchCancellationPreservePendingDecision`: independently committed peer update, required deferred conflict, typed limit, complete before/after state and retained decision after caller cancellation. |
| Reached conflict, lines 123–150 | `testReachedConflictCancellationRollsBackOwnedState`: probe arm/release, actual caller cancellation, full rollback vector/pending decision, zero reservations/apply mode and FK failures; same-owner resolution/retry follows. |
| Frozen push, lines 152–168 | `testPushCancellationRetainsFrozenRequestAcrossRetry`: cancellation, one pending row, hash length, complete retained request/source equality across two attempts and eventual outbox drain. The old nonempty-vector check is subsumed by required-table validation and comparison of the complete named vectors. |
| Snapshot transport/resume, lines 169–182 | `testSnapshotCancellationPreservesStateAndResumes`: cancellation, complete normalized vector, explicit retained `rebuild_required`, successful resume and final local-intent value. |
| Fixture and state helpers | Authentication/ready/pending values remain required; barrier HTTP status remains checked. Typed state adds required-table validation, deterministic ordering and REAL precision. Closure-owned openings preserve cleanup while removing sequential test dependencies. |

This is an authored source comparison, not independent acceptance or execution of
the live assertions. Both Swift variants compile and register six state-suite
tests (one local vector test plus five live leaves). With no authority launched,
the five live leaves skip; only the default build's reached-conflict leaf cites
the bridge. The local ownership and vector checks pass in each variant.
Focused Node validation covers the existing close/reuse case and missing-arrival
regression. Current commands, native results, source hashes and scope verification
are in the [remediation verification](../.artifacts/cancellation-review-fixes-2026-09-30/verification.json).
The [current-map overlay](../.artifacts/cancellation-review-fixes-2026-09-30/authority-overlay.json)
replaces the combined Swift selector with the five current owners and refreshes
their source hashes. All requirement statuses and qualification credit stay unchanged.

The earlier author reports remain historical. Re-review this remediation delta;
the Swift live body, affected browser/platform leaves and cancellation work bound
remain unproved. No Gate U completion or acceptance is inferred.


## Core cleanup fixture consolidation

After the owner supplied the CLEAN source re-review of the preceding remediation,
the owner authorized the next implementation slice. That earlier disposition
does not review this new delta. This slice changes only the two core test files
and this document; the current requirement map and all production/build files
remain unchanged.

`SafeSQLiteConnectionIsolationTest` now constructs the safe connection, persistence
owner and execution context in one typed `RecordingFixture`. Its two small
factories still select distinct raw backends: only the worker-shaped backend
implements suspend cleanup. No scenario, failure injection or assertion was
removed. The two explicitly typed local fixtures merely gain the raw type parameter.

`SqliteWorkerDriverTest` retains three independently reported undelivered-resource
cases. `cancelAfterCompletedResponse` shares their existing hold, completion
acknowledgement, cancellation and exact cancellation-type check, and cancels/joins
its child in `finally`. Each leaf still checks its own resource count and cleared
completed-response record; prepare/page retain connection reuse, page retains
manual rollback exactly once, and open retains zero-resource shutdown. The shared
helper uses the existing bounded diagnostic polling and adds no scheduler or
production hook. Rollback, successful late COMMIT, ordered cleanup failures and
queued cancellation remain separate adjacent leaves.

Development validation under `oversqlite-validation.md`'s Development profile:

| Lane | Fresh result |
| --- | --- |
| Isolation JVM, entire owning class | 44 pass, no failures/skips. |
| Worker Node, delivery plus adjacent cancellation leaves | 8 pass, no failures/skips. |
| JS ChromeHeadless, changed delivery leaves | 3 pass, no failures/skips. |
| Wasm ChromeHeadless, changed delivery leaves | 3 pass, no failures/skips. |

Exact commands, native reports, preimages, source hashes and scope checks are in
[the development verification](../.artifacts/cancellation-fixture-consolidation-2026-09-30/verification.json).
Unchanged build dependencies were reused; these are fresh executions of the
selected leaves, not fresh proof of unrelated platform/live behavior. No aggregate
qualification trigger applies to this test-only development slice. The remaining
native/live and cancellation-bound obligations are unchanged.

The reflective progress setup cannot be replaced through the existing supported
construction API. `SafeSQLiteConnection`'s constructor is internal to core;
`SqliteConnectionProvider` returns an already constructed safe connection;
`SqliteConnectionConfig` offers persistence and execution-context configuration,
not raw driver injection. The bundled provider constructs the raw connection
internally, and Oversqlite's test module has no core friend access. Public client
configuration alone cannot replace the final-progress SQL failure injection.
Therefore `A3ProgressFinalizationJvmTest` and `A3PublicProgressFailureJvmTest`
remain intact and were not rerun in this slice. Their conditional checklist item
stays open. No visibility bypass, reflective-constructor substitute, new product
hook or weakened assertion was introduced. A future removal needs a separately
reviewed construction/API decision; this finding is not an AndroidX interruption
or performed-work-bound proof. Their naming migration remains coupled to a future
change of that family, as specified above.


## Ordinary cancellation split and failure-path development checks

The owner authorized ordinary cancellation decomposition together with naming and
relevant runner checks. `A3OrdinarySafetyPlatformTest` becomes
`OrdinarySafetyPlatformTest`; the old identity is historical only. Its 37 other
methods retain their behavior, with only gate-helper and diagnostic-prefix naming
changes. Shared `A3OrdinaryFixtureSupport` and `A3StateAssertions` remain unchanged;
the new seeding helper is private to the ordinary suite.

| Previous behavioral group | Current independent owners and preserved assertions |
| --- | --- |
| Intrinsic refusal followed by response cancellation | `admissionRefusalPreservesFrozenPrimaryState` checks the typed atomic-work refusal, complete retained state and zero reservations, then retries under the ordinary configuration. `responseCancellationPreservesFrozenPrimaryState` independently seeds and freezes the request, cancels the real caller before releasing the response, checks complete rollback/reservations and retries. Both retain exact request equality, business relationships, cleared dirty/outbox rows, checkpoint and sole source increment. |
| Eight rebuild cuts in one loop | Six `rebuildCancellationAfterPage/Row1/2/3` leaves plus cache-close and before-commit leaves. Each retains exact reached-cut counts, complete primary/attachment/source rollback, permitted completed snapshot staging, zero apply mode/reservations, no hidden push, snapshot replay, frozen request equality, confirmed source increment and complete final cleanup. |
| Two push cuts in that loop | `pushCancellationAfterCacheClosed` and `pushCancellationBeforeCommit` retain complete primary rollback, unchanged snapshot stage, zero snapshot page/row counts, exact frozen replay, source increment and final cleanup. |
| Unbounded arrival wait with release only on success | `cancelAtBarrier` uses the existing real dispatcher and a 15-second arrival deadline. It cancels before releasing, requires `CancellationException`, and unconditionally releases and joins in `finally`. `missingCancellationBarrierReleasesAndJoinsCaller` holds a child in `NonCancellable`, omits arrival, and requires a real-time timeout without virtual-time advancement, released barrier and completed child cleanup. |

`withFrozenIntent` shares only mechanical seeding and owns database/client,
transport and worker-storage cleanup. Cleanup attempts all owners, preserves the
primary failure and suppresses subsequent cleanup failures. The response test
clears its server callback; apply tests clear their fault injector after joining.
Existing observation hooks and full typed-state capture remain in use. No
production hook, driver replacement, generic scheduler or watchdog was introduced.

A controlled negative run temporarily omitted the apply arrival acknowledgement
at the cache-close cut. The existing runner emitted one failed native case with
the expected 15-second `TimeoutCancellationException`; the case completed in
16.3 seconds, and the process completed normally with a failure exit. The exact
source was restored and the same native case passed again. This retained attempt
proves that the controlled failure reaches the runner; it does not prove forced
termination of stuck native SQL. The missing-arrival regression separately proves
that its deliberately held child has completed cleanup before returning.

Current commands, native reports, completion markers, unchanged-method comparison,
preimages and scope/authority identities are recorded in
[the batch verification](../.artifacts/ordinary-cancellation-split-2026-09-30/verification.json).
Node registers the admission-refusal case but explicitly returns because its
fixture storage is transient. That return earns no behavioral execution credit;
it does not supply durable reopen evidence. Remaining Android/iOS, live and
uninterruptible-native-runner obligations stay open. No reservation count, VM
instruction count or fixture timeout is treated as a performed-work bound.

The current gap map updates active owners, selectors, source identities and
per-runtime declaration counts. Prior ordinary source/runtime audits remain
historical unless separately revalidated; no requirement status or qualification
credit changes. The new [authority overlay](../.artifacts/ordinary-cancellation-split-2026-09-30/authority-overlay.json)
links the preserved preimages to this update. This development slice requires
independent review and does not complete Gate U or grant acceptance or delivery.


## Replay boundaries and parent-owned process recovery

The owner authorized the second batch: replay setup consolidation with affected
naming and runner checks. The supported portion changes tests only. The former
`A3ReplayTest.physicalRestart` name mixed a same-lifetime Node retry with graceful
database close/reopen elsewhere; neither was process death. The replay method
moved from `SurvivorApplyTest` recreated only the client on the same connection.
Its other 17 methods are byte-for-byte unchanged.

| Independent owner | Preserved boundary and assertions |
| --- | --- |
| `SettlementReplayTest.retriesLostCommittedResponseInSameLifetime` | Retry with the original client/connection after mock-server commit and lost response; now runs on every selected runtime. |
| `SettlementReplayTest.replaysLostCommittedResponseAfterClientRecreation` | Close/recreate the client while retaining the connection; frozen canonical request hash survives, then exact request replay settles. |
| `SettlementReplayTest.coordinatedDatabaseReopenReclaimsOnlyOrphanedReservations` | Graceful close and durable reopen. Ordinary client opening cannot reclaim the injected reservation ledger; the coordinated lifetime can. The complete original baseline vector survives. Node explicitly reports unavailable durable storage. |
| `ProcessDeathRecoveryJvmTest` | Twelve independent native tests retain all original reservation, stage/page/row, cache, commit and five planner cuts, old-or-new typed state, preserved primary state, retained stage identity, planner resume, reservation reclamation and FK checks. The existing child process is forcibly killed, with exit 137 required. |

A private lost-response fixture and final settlement assertion remove three
copies of the same preparation. Each replay requires precise cancellation,
retained outbox/checkpoint, survivor relationships, exact request equality,
three requests, source next-ID/checkpoint and final dirty/outbox/reservation
cleanup. Full-vector and canonical-hash assertions remain at their distinct
reopen boundaries. The mock server remains a mock; these checks do not establish
actual HTTP-byte equality or independent live-server readback.

The process runner owns one child and directory from creation through cleanup.
Arrival uses a monotonic 30-second deadline; termination has a five-second wait.
Cleanup attempts all child streams and file removal, checks termination/removal
results, and preserves the primary failure with suppressed cleanup failures.
`missingArrivalTerminatesOwnedProcess` uses that same runner, requires the absent
marker failure, and verifies the child is dead and its directory gone. A separate
negative control omits the real reservation-cut marker and retains the native
failed report; exact source restoration and a passing rerun are recorded in the
verification. This proves bounded missing-arrival orchestration, not termination
of uninterruptible work in the parent or a performed-work cancellation bound.

The existing `a3.platform.jvmTest.classpath` build property is retained. Renaming
that wired interface or the shared state-vector helpers is outside this test-file
migration. The five planner cuts still use the existing internal owner path;
they do not become complete public-phase coverage through test decomposition.

### Historical worker prerequisite failure and remaining live gap

The attempted worker fixture consolidation failed in JS ChromeHeadless with
`OversqliteOpeningBusyException: upgrade_busy`. The exact original dirty preimage
was restored and independently reproduced the same failure. At that batch boundary,
`A3OrdinaryWorkerDeathTest` was restored unchanged, with its assertion union intact;
no passing replacement or renaming was claimed. The attempted shared platform
seed helper was removed as well, since there was no longer a second validated
consumer. The failed attempts and their exact candidate/preimage are retained.

The source explains a plausible ownership mismatch: worker force-termination
only calls transport termination, while `OversqliteActiveClientRegistry` lives in
the caller runtime and rejects another lease group for the same storage identity.
See [worker termination](../library-core/src/webMain/kotlin/dev/goquick/sqlitenow/core/worker/SqliteWorkerDriver.kt)
and [active-client ownership](../library-oversqlite/src/commonMain/kotlin/dev/goquick/sqlitenow/oversqlite/OversqliteClientResourceOwner.kt).
The subsequent investigation confirmed that the surviving caller retains its
active-client lease. The owner then authorized the narrower supported boundary
below: real worker crash followed by coordinated surviving-caller cleanup. This
cannot establish abrupt whole-caller death. Registry clearing, substitute drivers
and a new isolated caller-context harness remain outside this slice.

No compatible live authority was configured. `A3OrdinarySafetyLiveTest`, its gzip,
wire/source equality, committed-prefix and independent readback assertions stay
unchanged. Android/iOS replay execution remains open. Node durable restart remains
unavailable. Neither unavailable capability nor a historical report waives a
requirement.

Selected development commands, native results, failed worker attempts, the process
negative control, exact preimages/diffs and identity checks are in
[the replay verification](../.artifacts/replay-consolidation-2026-09-30/verification.json).
That batch updated the gap map with new process owners and the worker failure without
changing requirement statuses or qualification credit. The
[authority overlay](../.artifacts/replay-consolidation-2026-09-30/authority-overlay.json)
links the preserved specification/map preimages. Unchanged build outputs are reused;
only selected executed test leaves earn fresh development evidence. No aggregate
qualification trigger applies to this test-only Development-profile slice.


## Worker crash, surviving-caller cleanup and replay

The owner authorized this narrow implementation after read-only investigation.
[`WorkerCrashReplayTest`](../platform-oversqlite-test/composeApp/src/webTest/kotlin/dev/goquick/sqlitenow/oversqlite/platform/WorkerCrashReplayTest.kt)
replaces the historical worker-death name with an explicit behavioral boundary.
It forcibly terminates the actual default SQLite worker after mock-server commit,
requires `CancellationException`, and checks that the dead worker released the
origin lock before closing the surviving caller through its existing coordinator.
No production source, build wiring, driver, registry reset or new hook changes.

Close must report the exact existing worker-termination `SqliteException`, with no
suppressed cleanup errors. Before reopening, the test requires no pending client
cleanup and completed core connection/execution-context close. It therefore does
not silently discard a close error or assume that an exception means cleanup was
incomplete. The successor opens before the final cleanup retry of the fixture
wrapper; that retry does not enable replay.

The original full frozen-state comparison, exact mock request equality, source
bundle identity, survivor relationship, dirty/outbox/reservation cleanup, source
next-ID/checkpoint, foreign-key and three-request assertions remain. Final cleanup
attempts every fixture owner in `NonCancellable`, preserves the primary failure,
and exposes additional failures. The completion marker follows that cleanup.
The returned `runTest` result keeps asynchronous execution attached to the runner.

Development profile ([validation policy](oversqlite-validation.md)): the selected
`*WorkerCrashReplayTest*` leaf passes once each under `jsBrowserTest` and
`wasmJsBrowserTest`, with zero failures/errors/skips and matching entry, cleanup
and completion markers. Unchanged build outputs were reused; no aggregate trigger
applies. Node durable restart is unavailable and receives no fresh execution
credit. Mobile, live, whole-caller death and complete public-phase obligations
remain open. Browser XML durations provide no latency evidence.

[Verification](../.artifacts/worker-crash-replay-2026-09-30/verification.json)
retains exact commands, native reports, the first close-diagnostic failure, the
corrected passing probe and a compile-only import correction. The
[authority overlay](../.artifacts/worker-crash-replay-2026-09-30/authority-overlay.json)
links the preserved specification/map preimages. Requirement statuses and
qualification credit are unchanged. This proves worker-crash recovery with a
surviving caller's cleanup; abrupt whole-caller death remains unproved. It adds no
performed charged-work cancellation bound, VM-instruction equivalence, live
HTTP-byte proof, Gate U completion, acceptance or delivery authorization.


## Swift live cancellation through the existing authority

The owner authorized the next runner/live-validation batch. The existing local
PostgreSQL 17.10 Homebrew service was already available; a read-only connection
check established the required local role privileges. The existing Go
`swift_cancel_fixture` then started and cleaned up its isolated database. A missing
`OVERSYNC_EXAMPLE_TEST_DATABASE_URL` in the agent environment was configuration
missing from that invocation, not evidence that new infrastructure was needed.
The users-only schema generation is supported with the larger registered catalog;
no Go fixture or production change was necessary.

[`oversqlite_smoke.py`](../tools/qualification/oversqlite_smoke.py) adds the explicit
`swift-cancellation-state` mode. It reuses the existing authority, process groups,
deadlines, redacted logs, native XML capture and exact database cleanup. It runs
the four bridge-independent `SyncCancellationStateTests` against the default
runtime, then builds the isolated debug bridge and runs only reached-conflict
cancellation. One XCTest worker keeps the shared authority barriers serial. Native
XML must contain every selected case exactly once, with matching class/counts and
no errors/failures/skips; verbose native logs must also show one start and pass per
case. Each runtime group retains its own report. Ambient bridge/fixture flags are
normalized so the existing second-push mode stays in its separate default mode.

With the database URL supplied through the process environment, the explicit
commands are:

```sh
python3 -B tools/qualification/oversqlite_smoke.py swift-cancellation-state
python3 -B tools/qualification/oversqlite_smoke.py swift-cancellation
```

Development results: all five cancellation leaves pass live on macOS, and the
separate second-push regression passes against a fresh default-mode authority.
The 19 focused Python runner tests pass, including missing/duplicate/skipped
result rejection, mode isolation, controlled child termination and cleanup failure.
No Swift test body, Kotlin source, build wiring or Go source changed. Existing
framework outputs were reused where valid; affected runtime variants and Swift
test bundles were prepared by the runner. No qualification aggregate applies.

Both live invocations report successful shutdown and cleanup. A final read-only
query restricted to the three recorded database names (readiness probe and two
live invocations) confirms all three are absent. Existing databases and the local
PostgreSQL service were retained. The [verification](../.artifacts/swift-cancellation-live-2026-09-30/verification.json)
links native reports, exact commands, timings, source/preimage hashes and the
[authority overlay](../.artifacts/swift-cancellation-live-2026-09-30/authority-overlay.json).

These results replace the earlier unexecuted status for these six macOS leaves
only. Arrival counters and retained request state still do not prove HTTP-byte
equality or post-server-commit response-loss replay. Performed charged work,
AndroidX running-statement interruption, whole-caller death and other required
platform/live proofs remain open. Requirement statuses and qualification credit
are unchanged; this runner delta needs independent review and grants no owner
acceptance, delivery authorization or Gate U completion.


## Behavior-based Kotlin test names

The owner authorized a dedicated naming cleanup. Seventeen local Kotlin test and
helper files now use behavior names: snapshot FK lifecycle/recovery, pending
reconciliation, connection reservations, reservation format, response/workspace
admission, workspace counting, settlement/live/memory, progress finalization,
public progress failure, reservation-owner death, ordinary live safety, ordinary
fixtures and database-state assertions.
Their three additional Kotlin callers use the renamed helpers. No compatibility
alias, new hook, assertion removal or production change is introduced.

The [initial rename map and captured scope](../.artifacts/behavior-test-names-2026-09-30/plan.json)
identify every proposed old/new path; the [deferred pair](../.artifacts/behavior-test-names-2026-09-30/deferred.json)
records the two restored files. Complete source comparison permits only the
declared identifier replacements.
Method declarations, test logic, fixture values, diagnostic markers, deadlines and
resource ownership are preserved. Historical paragraphs and receipts above retain
the names used at the time; current links in the checklist and current gap-map
owners use the new names. Those historical selectors are not current commands.

Two existing Gradle live/stress selectors now point to `SettlementLiveTest`.
The two shared-contract filenames/classes remain `SharedA3WorkspaceContractTest`
and `SharedA3PendingContractTest`: the read-only Go consumer asserts those exact
class names at `oversync/contract_a3_test.go:50`. Their attempted source/metadata
renames were restored byte-for-byte after that dependency was found. No alias or
relaxed assertion was added. Renaming them needs a paired Go/JVM authorization.
Shared fixture filenames, families, corpus classifications/manifest, Go consumers
and all fixture contents remain unchanged. The
[authority overlay](../.artifacts/behavior-test-names-2026-09-30/authority-overlay.json)
updates only the specification/map identity; sealed pins stay intact.

Development validation and the exact source comparison are retained in the
[verification record](../.artifacts/behavior-test-names-2026-09-30/verification.json).
The selected library JVM tests pass all 32 cases, and platform helper consumers
pass all 63 JVM cases. Nine heavy/live leaves are separately discovered with
Gradle test dry-run and receive no behavioral execution credit. Both modules
compile their tests for JS,
Wasm, macOS, both iOS targets and Android. Compilation is not platform behavior.
The first JVM attempt exposed a dynamically constructed shared consumer name;
that failed attempt and the intermediate passing correction are retained. Final
validation uses the restored shared-contract sources and original corpus, and
only final-source reports receive current evidence credit.

This is the development profile in [the validation policy](oversqlite-validation.md).
Unchanged production/build dependencies may be reused. Full protocol, live,
heavy and mobile runtime aggregates are not triggered by exact name substitution;
their existing obligations remain open. No Gradle plugin implementation changed.
The separate Swift runner slice still awaits independent review. This cleanup
also needs independent review, including its two selector changes. Requirement
statuses and qualification credit are unchanged;
no cancellation bound, Gate U completion, acceptance or delivery is inferred.


## Paired shared-contract consumer names

The owner separately authorized the coordinated follow-up proposed after the local
cleanup. `SharedWorkspaceContractTest` and `SharedPendingContractTest` replace the
two remaining `SharedA3…` Kotlin filenames/classes; their local helpers use
behavior names too. Kotlin's constructed consumer-name assertion, Go's exact JVM
consumer assertion in `oversync/contract_a3_test.go`, and the two `jvmConsumer`
metadata values now agree. The corpus manifest changes only the classification
file's checksum. Fixture filenames, IDs, contents, Go test selectors and all
assertions other than the expected JVM names remain unchanged. No alias or
compatibility fallback is introduced.

The prior section's deferral is historical: this slice supplies the narrow Go
write authorization it required. Earlier cleanup and Swift-runner slices still
await independent review; this authorization does not accept either of them.

Development validation passes six JVM cases (the two consumers and manifest
checks), plus four Go top-level contract tests with 39 subtests and no skips or
failures. The Go command uses `GOTOOLCHAIN=go1.25.12`, `GOWORK=off`, the explicit
shared corpus root, `-count=1` and a 120-second native timeout. Both consumers
retain complete fixture-case execution and fail-closed manifest checks. These are
pure contract tests; no PostgreSQL, server, browser or device was launched. The
exact source substitution, commands and native reports are in the
[verification record](../.artifacts/paired-contract-names-2026-09-30/verification.json).

The [authority overlay](../.artifacts/paired-contract-names-2026-09-30/authority-overlay.json)
binds the two changed corpus metadata pins and the current specification/map to
captured preimages. The sealed plan is unchanged. Both complete dirty checkouts
are scope-checked: the Go delta is one expected-name string in one test file;
Kotlin changes only the two test files, corpus metadata/checksum and current
specification/map. No production or build wiring changes occur.

The development profile in [the validation policy](oversqlite-validation.md)
selects these affected pure consumers. Unchanged runtime/build dependencies are
reused; platform/live, full protocol and database aggregates are not triggered by
this test-name substitution. Existing runtime and cancellation-bound obligations
remain open. All requirement statuses and qualification credit remain unchanged.
This paired slice is ready for independent review, not qualification, owner
acceptance, delivery or Gate U completion.


## Swift runner review remediation

The independent review found the naming slices clean and requested two Swift
runner fixes. Both findings are valid: clearing the Swift environment flag did
not override an inherited Gradle project property, and comparing only the class
suffix allowed a different XCTest module to satisfy result validation. The earlier
six live passes remain historical development evidence; they did not establish
those two isolation properties.

Default runtime preparation now explicitly passes `-Pa3SwiftTestBridge=false`;
the reached-conflict preparation retains explicit `true`. XML and native log
validation both require the full `SQLiteNowSyncSupportTests.<class>` identity.
No runtime, Swift test body, build wiring, Go source or qualification requirement
changes. The existing Python tests now use qualified native XML, verify both
preparation commands with an ambient bridge property, and reject wrong-module,
extra-prefix and unqualified identities in XML, logs and both together.

The corrected regression-only run fails against the original runner with 16
assertion failures and no errors. Its first attempt also contained a test-placement
error; that attempt is retained separately and is not used as reproduction proof.
After the runner fix, all 20 Python tests pass and all 19 previous test methods
remain. Both existing live modes then pass with
`ORG_GRADLE_PROJECT_a3SwiftTestBridge=true`: four default-runtime cancellation
cases, one bridge reached-conflict case, and the separate default-runtime
second-push case. Every native case has the required full module/class identity,
with zero failures/skips. Both run-owned databases were cleaned up and separately
confirmed absent by a read-only query restricted to their recorded names.

The [remediation record](../.artifacts/swift-runner-review-fixes-2026-09-30/verification.json)
retains preimages, diffs, failures, final Python/native reports, exact commands,
cleanup confirmation and the [authority overlay](../.artifacts/swift-runner-review-fixes-2026-09-30/authority-overlay.json).
Drymint has no enabled Python adapter in this checkout; source inspection and
these checks supply the evidence without changing indexing configuration.
The development profile reruns the affected Python runner and both Swift live
modes; unchanged build dependencies are reused. No full protocol aggregate is
triggered. Naming review dispositions remain clean, while the owner subsequently elected to proceed without another independent
review of this four-path runner remediation. Requirement statuses and qualification
credit remain unchanged; no owner acceptance, delivery or Gate U completion is
inferred.

## Live committed-response replay development slice

The existing `OrdinarySafetyLiveTest.backlogBoundAndReplay` now has fresh JVM and
macOS development execution against the unchanged Go survivor authority and the
existing PostgreSQL service. The selected normal leaf retains the 1,000-row,
16-byte backlog; all original heavy sizes, widths, counter limits and assertions
remain. The other four methods in the file are unchanged. No new fixture, product
hook, runner mode, Go source or build wiring was added.

The existing receive-pipeline cancellation still happens after a successful push
response. The complete independent server readback oracle is now shared between
before replay and after settlement. Before replay it proves that the original
request already committed, while the caller still retains its frozen request and
unadvanced source/checkpoint. After orderly database reopen, the test compares
encoded outgoing HTTP body bytes and content encoding, in addition to the existing
decoded text, canonical hash, source header, durable vector, request-count and
cleanup assertions. This observes the Ktor send pipeline, not an external packet
capture. It does not represent an abrupt whole-caller crash.

The first enabled run exposed an existing oracle error: predicates expected the
SQLite namespace `main` in sync metadata, although the admitted survivor generation
uses the wire schema `business`. Only this method's expectations were corrected;
an explicit committed seed and four-row schema assertion now identify that setup
boundary. Every prior assertion is retained, with that schema correction and the
readback relocation. Both original and peer cleanup attempt all fixture owners in
`NonCancellable`, preserve primary failures and expose cleanup errors. Completion
is emitted after owned cleanup, without claiming that an uninterruptible native
call can be terminated by coroutine cancellation.

A real pre-dispatch-loss negative control failed in the independent pre-replay
readback: the server retained four seed rows rather than the committed three-row
survivor state. The exact source was restored and the final JVM case passed. The
final macOS native case also passed. Its author collector initially used a JVM
report filename; the fresh target-prefixed native report was recovered immediately
and validated separately, preserving the failed collection summary. No runtime
rerun was needed for that artifact-path correction. All five run-owned PostgreSQL
databases, including failed attempts, were independently confirmed absent.

The [verification record](../.artifacts/live-committed-replay-2026-09-30/verification.json)
retains commands, native reports, the negative control, preimages and exact scope.
The development profile supplies two final native passes, one per selected runtime;
unchanged build dependencies are reused. Browser, Node, Android/iOS and heavy live
configurations remain unperformed for this changed source. The four sibling live
methods and complete public-phase union remain open. Admission reservations and
existing diagnostic work counters are not performed charged-work cancellation
proof. No requirement status, qualification credit or cancellation contract changes.
The owner declined another independent review of the preceding Swift runner fixes;
that decision is not recorded as a clean review or qualification acceptance.

## Browser live replay execution

The unchanged live replay source was executed against the same existing Go survivor
fixture on JS and Wasm browsers. JS retained all six 1k/10k/50k × 16/1024 cases;
the normal Wasm leaf retained its single 1k/16 case. Existing browser, coroutine
and outer-runner timeouts were unchanged. No source, build, fixture, driver or
runner-mode change was made.

The Wasm normal leaf passed with one exact native case, no failures/errors/skips,
and entry, pre-replay commitment, scenario and after-cleanup completion markers.
It provides fresh browser development evidence for orderly durable reopen, exact
outgoing body replay, unchanged frozen identity, independent server readback and
cleanup at that configuration.

The JS leaf failed at the existing 180-second Mocha timeout. Both 1k cases emitted
scenario-complete markers; 10k/16 emitted pre-replay commitment but did not complete.
The last marker does not locate the stall within subsequent backlog preparation,
reopen or replay. There was no final completion marker, so the aggregate receives
no pass credit and final in-test worker/storage cleanup is unconfirmed. The owned
server process/database cleanup completed, and a separate read-only query confirmed
both run-owned PostgreSQL databases absent. Near-zero browser XML durations are
not latency evidence. The unchanged aggregate was not retried and its timeout was
not raised.

The [browser evidence](../.artifacts/browser-live-replay-2026-09-30/verification.json)
retains both attempts and their native reports. This identifies an orchestration
and/or workload-duration problem, not proof of an absent backend capability. The
next bounded work is to isolate the backlog configurations as independently
reported tests, preserving all assertions and all six configurations, before
attributing the timeout or considering any budget change. This execution slice
does not implement that decomposition. Wasm heavy, Node, mobile and remaining live
families remain open. Requirement statuses, qualification credit and the performed
charged-work cancellation-bound decision are unchanged.


## Independently reported live replay configurations

The owner authorized splitting the six backlog configurations and isolating the
browser timeout. Six `committedReplayWith…RowsWidth…` tests replace the historical
`backlogBoundAndReplay` aggregate. Each returns the same shared `runTest` scenario,
so browser execution remains attached to the runner. The entire scenario body,
including assertions and cleanup, is unchanged except for indentation. The four
sibling methods are byte-identical. The original runtime roster is preserved:
JS browser runs all six configurations; other runtimes run 1k/16 normally and all
six in heavy mode. Ineligible registrations emit `A3_NOT_EXECUTED` and receive no
behavioral credit. No product, Go, fixture, build or authored runner change occurs.

Fresh development evidence is separately attributable:

| Runtime/configuration | Result |
| --- | --- |
| JVM normal roster | Six registrations; 1k/16 completes, five heavy-disabled bodies do not execute. |
| JS browser 1k/16, 1k/1024, 10k/16 | Three independent native passes, each with commitment, scenario and after-cleanup completion markers. |
| JS browser 10k/1024 | One native failure at the unchanged 180-second Mocha timeout; commitment marker present, scenario/completion markers absent. |
| Wasm browser normal 1k/16 | One native pass with all markers. |
| macOS normal 1k/16 | One native pass with all markers. |
| JS browser 50k/16 and 50k/1024 | Preserved and registered; not executed after the isolated 10k/1024 timeout. |

This closes the configuration-isolation task, not the heavy browser completion
gap. The 10k/16 case now completes independently, while 10k/1024 still exceeds the
existing browser limit. Its last marker does not distinguish backlog preparation,
reopen or replay. No timeout was raised and no unchanged failing case was retried.
Final in-test worker/storage cleanup for that failed case remains unconfirmed.
All seven run-owned server/database cleanup operations completed; a separate
read-only query confirmed all seven PostgreSQL databases absent. Browser XML
durations supply no latency or cancellation-bound proof.

The [verification record](../.artifacts/live-replay-case-split-2026-09-30/verification.json)
contains the exact source comparison, selectors, native reports, failed attempt,
cleanup confirmation and authority overlay. The development profile in
[the validation policy](oversqlite-validation.md) selects these affected live
leaves. Existing product/build dependencies and the unchanged survivor authority
are reused. Mobile, Node, remaining heavy configurations and the four sibling live
methods retain their separate obligations; no full protocol aggregate is claimed.

The next bounded step is to locate the cost or stall in the single 10k/1024 JS
case using existing runtime diagnostics and source inspection before choosing a
fixture improvement or an explicit workload-budget decision. This result does
not establish an absent backend capability. No additional hooks or driver
workarounds are justified by the timeout alone. Requirement statuses and
qualification credit remain unchanged. Performed charged work, reservations and
SQLite VM instructions remain distinct; Gate U, acceptance and delivery remain open.


## Isolated JS replay timeout diagnosis

The owner confirmed that Kotlin/JS remains supported and authorized diagnosis of
the 10k/1024 timeout. One diagnostic execution added only temporary monotonic
phase prints to the existing test. It used the same workload, assertions, driver,
authority and deadlines. All 19 diagnostic lines were then removed, restoring
the exact source bytes from the preceding case-split receipt. No lasting test,
product, build, runner or Go change was made.

The native case again failed at the existing 180-second Mocha timeout. Its
monotonic phase timings show:

| Boundary | Elapsed since diagnostic start |
| --- | --- |
| Backlog insert begins, after committed-response readback | 2.823s |
| Backlog creation and dirty update complete | 7.737s |
| Full and unrelated pre-reopen vectors complete | 14.707s |
| Durable reopen and reopened-state comparison complete | 18.558s |
| Public replay begins | 18.625s |
| Public replay returns successfully | 169.739s |
| Unrelated-state comparison completes | 173.016s |
| Settled-state vector completes | 176.315s |
| Final independent server readback begins | 176.316s |

The public `pushPending().getOrThrow()` call took 151.114s in this diagnostic run.
The timeout happened after that call, during final independent readback. It did
not report a replay exception, but the complete behavioral test still failed:
final readback and in-test cleanup have no completion witness. This is one
observation, not a latency guarantee or a benchmark distribution. Native browser
XML durations are not the source of these elapsed times.

Source inspection identifies a plausible cost in the real replay path:
`OversqliteApplySurvivors.prepareSettlementIntent` and `restorePrimaryIntent`
visit primary intents individually, using `nextIntent`, workspace checks,
`capture` and (during preparation) `save`. Multiple SQLite operations per row may
amplify worker round-trip cost. The phase trace does not attribute time to those
queries, CPU work or transport, so that explanation remains a hypothesis. No
production performance change is justified solely by this trace.

There is no demonstrated setup-only fix that resolves the dominant cost. A few
seconds shaved from verification would leave little margin and would not establish
completion for either 50k case. Both remain unexecuted; no unchanged retry or
timeout extension was performed. Before those runs, the owner should choose a
separate heavy-case runtime budget, or prioritize a bounded investigation of
production replay cost if this observed latency is unacceptable. Ordinary tests
should retain their existing budget. Any budget change must preserve complete
readback/cleanup assertions and an external stop limit; it grants no cancellation
or performance guarantee.

The [diagnostic receipt](../.artifacts/js-replay-timeout-2026-09-30/verification.json)
retains the temporary source and diff, native failure, monotonic timings and exact
restoration. The run-owned authority process/database cleanup completed, and a
read-only query confirmed that database absent. Existing passing case-split
results remain tied to their identical restored source; this failed diagnostic
run adds no pass or qualification credit. All twenty requirement statuses,
protected authority pins and paired checkout identities remain unchanged except
for this specification/map overlay. Gate U, the performed charged-work bound,
acceptance and delivery remain open.


## JS replay performance investigation

The owner authorized a diagnostic investigation after the timeout was located.
One fresh run used Chrome's built-in CPU profiler for the first 30 seconds of
replay at a 1 ms sampling interval. The test page, SQLite worker and two OPFS child
workers produced complete profiles. Four temporary test-only lines marked replay
and printed existing counters; they were removed afterward, restoring the exact
case-split source. Product, build, driver protocol and Go sources are unchanged.
The existing 180-second browser timeout was retained.

| Profiled execution context | V8-classified idle share of sampled time |
| --- | --- |
| Kotlin/JS test page | 43.16% |
| SQLite worker | 64.45% |
| OPFS helper | 99.77% |
| OPFS worker-lock helper | 99.93% |

Message posting, response handling, coroutine resumption and serialization are
prominent in the active stacks. SQLite Wasm frames occur in 6.46% of the SQLite
worker's sampled time. These overlapping thread profiles cannot be added into a
wall-time breakdown; idle is the profiler's classification, not an exact measure
of IPC delay. The sampled prefix supports request/protocol overhead as an
optimization direction, without attributing every millisecond or excluding every
possible I/O cost later in replay. The collector also attached to test-Chrome
background targets, which are excluded from analysis. Detached setup workers
produced session-not-found diagnostics; all four selected replay profiles completed.

The public replay returned in 157.436s with profiling enabled. Existing diagnostics
reported 40,000 primary reads, 80,000 indexed visits and 3 restoration passes for the
10,000-row backlog: preparation plus three restoration/verification passes. The
full native case again timed out without final completion. This perturbed single
run is neither a performance benchmark nor a behavioral pass. Its work counter
remains distinct from performed charged-work cancellation proof.

Source explains why this is expensive. `OversqliteApplySurvivors.nextIntent`
checks workspace usage before fetching payload; `capture` checks usage again
before serializing the live row. Each `pushWorkspace` call supplies a known apply
charge, but `OversqliteWorkspace.usage` still separately reads operation metadata
and retained push metadata. Survivor `one`/`exec` and workspace `one` use uncached
`withPreparedStatement`; the local serializer also defaults to no cache. The
worker executes prepare/page requests with response acknowledgement and reconciles
queued finalization before later requests. These details agree with the sampled
message and serialization costs.

Recommended next implementation candidate: consolidate the bounded workspace
metadata reads inside the existing `OversqliteWorkspace.usage` owner. Preserve
all charge calculations, overflow/limit checks, missing-owner behavior and
measure-before-payload allocation. Read current data each time; do not memoize
mutable reservation or plan state. The inspected per-row path would need fewer
SQL/worker requests without changing the number of preservation passes or any
wire/worker protocol. Actual savings require a controlled before/after run.

Transaction-owned statement reuse is a separate candidate. The existing apply
executor already owns and closes a `StatementCache`, but passing it through is
not automatically a request-count win: worker cache release sends both reset and
clear-bindings commands. Evaluate compile/serialization savings separately and
bound the retained statement set. Do not introduce a global cache, remove scans,
bypass workspace checks, or drop acknowledgement/finalization to obtain a pass.
The scans protect intent against application-trigger side effects and fixed-point
restoration; removing one requires a separate correctness argument.

For the next candidate, use existing focused workspace accounting/refusal tests,
missing-owner/error cases, rollback and statement/cleanup tests, then the unchanged
JS 10k/1024 leaf and affected platform/live leaves. Any query change must preserve
exact outcomes and bounded materialization; compilation or a faster partial run
cannot replace complete readback and cleanup. The 50k leaves remain pending and
may still require a separate heavy runtime budget even after optimization.

The [profile evidence](../.artifacts/js-replay-profile-2026-09-30/verification.json)
retains native failure, CPU profiles, analysis, diagnostic source/restoration,
source pins and cleanup. The authority database was confirmed absent and the owned
Chrome debugging endpoint closed after runner cleanup. Only the current spec/map
change persists. No optimization, requirement waiver, cancellation-bound proof,
qualification, acceptance or delivery is inferred.

## Workspace metadata query consolidation

The owner authorized this common-code optimization on 2026-10-01, with measured
improvement as the development exit rather than a new cancellation guarantee.
`OversqliteWorkspace.usage` now reads one current scalar snapshot rooted in the
operation singleton. It joins apply/push singletons only when their charges are
not supplied by the caller. Missing plans contribute zero; present empty plans
retain their overhead. The two separate retained-charge query helpers are gone.
Checked Kotlin arithmetic and its order, UTF-8 lengths, abandoned-reservation
handling, missing-operation failure and override behavior are preserved. The
statement remains owned through checked charge calculation so accounting failures
stay primary if close also fails; explicit cancellation checks remain before
stored apply/push calculations. No cache, payload scan, production
probe, wire/schema change, build variant or new resource owner was introduced.

The replay path makes one workspace metadata query instead of two; the general
path makes one instead of three. This is a source-backed query count, not a new
worker counter. Six focused common tests use the real retained-table DDL directly
and passed first against the old implementation, then against the candidate on
all five available runtimes. They cover exact owner/override charges, absent and
empty plans, missing operation state, abandoned reservations, UTF-8 and nullable
metadata, current reads, limit/overflow diagnostics and failure precedence.
Existing tests and assertions are unchanged.

The following results and timing comparison belong to the original consolidation
receipt; current-source remediation results follow below.

| Runtime | Focused library evidence | Focused platform evidence | Final-source live replay |
| --- | --- | --- | --- |
| JVM | 27 passes, including statement/HTTP cleanup | 16 passes | 1k/16 pass |
| Node | 16 registrations; 15 supported executions | 16 registrations; 15 supported executions | Not selected |
| JS browser | 16 passes | 16 passes | 1k/16 and 10k/1024 pass |
| Wasm browser | 16 passes | 16 passes | 1k/16 pass |
| macOS | 16 passes | 16 passes | 1k/16 pass |

Node's durable-reopen leaf/portions remain explicitly unavailable; green native
registrations do not grant those behaviors credit. Platform checks retain
workspace/refusal vectors, ledger rollback, settlement/recreation and the existing
apply/push cancellation cuts. All executed native reports have zero failures,
errors and XML skips; the Node nonexecution markers are separately accounted for.

One matched unprofiled JS comparison measured public replay at 154.577s before
and 138.096s after, a 10.66% reduction. Existing accounting diagnostics are identical,
including 40,000 primary reads and three restoration passes. The stable baseline
hit the unchanged 180s full-test deadline; the timed candidate and the subsequent
marker-free final source both completed readback and cleanup within that deadline.
The original live test was restored byte-for-byte. This is one development
comparison, not a statistical benchmark or a speedup claim for other platforms;
browser XML durations establish no latency bound. An initial 158.018s baseline was
invalidated when the author created the test draft during execution; that attempt
is retained and excluded from the comparison. The stable repeat held source fixed.

All eight run-owned PostgreSQL databases were independently confirmed absent;
normal durability settings remained enabled. Evidence, exact dirty preimages,
diffs and the authorized spec/map overlay are retained in
`.artifacts/workspace-query-consolidation-2026-10-01/verification.json`.
Android/iOS/Linux execution, Swift live coverage, remaining heavy leaves and broader
qualification remain open. Earlier runtime reports are historical, not fresh
proof for the changed common dependency. Reservations, performed Oversqlite charged
work and SQLite VM instructions remain distinct. All twenty requirement statuses
and qualification credit are unchanged; no Gate U completion, independent review,
owner acceptance, commit or delivery is inferred.

## Workspace accounting cleanup precedence review remediation

The 2026-10-01 review found that closing the joined statement before checking
stored charges could replace an overflow or invalid-metadata diagnostic with a
close failure. Charge calculation now runs inside the existing prepared-statement
callback. Its existing cleanup owner preserves the accounting failure and adds
close failure as suppressed; valid accounting still exposes a close failure.
The joined SQL, one-query count, checked arithmetic order and explicit cancellation
checks remain. The temporary scalar array is removed. This uses the helper's
uniform body-failure precedence, including operation and missing-owner failures;
it does not recreate the separate statements' historical cleanup boundaries.

Four independent JVM regressions cover apply overflow, invalid apply metadata,
push overflow and valid accounting with a one-time raw close failure. They extend
the existing cleanup fixture rather than add a production hook or reflection seam.
They check diagnostic fields, exactly one close sentinel in the failure graph,
suppression ownership, one query, the fatal-cleanup close retry, connection disposal
and absence of subsequent statement work. The unchanged production source failed
three diagnostic cases and passed the close-only control. The first candidate
passed 30/31 JVM cases; its remaining assertion inspected a coroutine recovery
wrapper rather than the original IllegalArgumentException. The corrected assertion
checks the original primary's suppressed failure. Both attempts are retained.

Fresh final-source development checks: 31 JVM library cases; 16 library registrations
each on Node, JS browser, Wasm browser and macOS; 16 selected platform registrations
each on JVM, Node, JS browser, Wasm browser and macOS. Node provides 15 supported
library and 15 supported platform completions; durable-reopen omissions remain
explicit and receive no credit. Five live replay leaves complete with readback and
cleanup: 1k/16 on JVM, JS, Wasm and macOS, plus unchanged JS10k/1024 within the existing
180-second deadline. All five run-owned server databases were confirmed absent.
No new performance comparison was made; the earlier timing comparison is historical
preceding-source evidence, not a remeasurement of this remediation.

The current requirements map preserves the old profiling text in an explicitly
historical record and states current remaining work at the parent level. Exact
preimages, native attempts, selectors, cleanup and authority overlays are retained
in `.artifacts/workspace-cleanup-precedence-2026-10-01/verification.json`.
Android/iOS/Linux, Swift live, remaining heavy/platform obligations, whole-caller
death, public-phase union and the performed charged-work bound remain open.
All twenty requirement statuses and qualification credit are unchanged. This is
review remediation with selected development evidence; no independent-review
acceptance, qualification, Gate U completion, owner acceptance or delivery follows.


## JS 50k replay development validation

After the owner supplied a CLEAN focused rereview of the workspace cleanup
precedence and status-map fixes, the owner authorized two existing JS replay
leaves in sequence, stopping on the first failure. No production, test, build,
fixture, Go source or timeout change was authorized or made.

The 50k-row/width-16 leaf executed once and failed at the existing 180,000ms
browser deadline. The native report contains one case, one failure and no errors
or skips. Its entry and `LIVE_REPLAY_COMMITTED` markers are present; the final
scenario and completion markers are absent. The commitment marker follows the
initial independent server readback, before backlog preparation and replay.
Thus the report establishes that earlier assertion group, but does not locate
failure among backlog preparation, physical reopen, replay or final verification.
It proves neither a hang nor a particular performance bottleneck. Replay return,
accounting diagnostics, final assertions/readback and in-test client/OPFS cleanup
remain unconfirmed. Native browser XML durations provide no latency evidence.

The 50k-row/width-1024 leaf was not launched under the first-failure stop rule.
Existing external process/database cleanup passed. A separate read-only check
confirmed exactly the run-owned PostgreSQL database absent at confirmation time;
normal durability settings remained enabled. External cleanup is not evidence
that the interrupted test completed its own cleanup.

Raw output, native report, selector, source identity, diagnosis and cleanup are in
`.artifacts/js-50k-replay-2026-10-01/verification.json`. Earlier successful runtime
leaves are retained development evidence; this batch changed only this document
and the existing requirement map. The smallest next diagnostic slice is bounded
phase localization within the unresolved interval, before a retry or another
optimization. No timeout extension or workaround follows from this failure.
All twenty requirement statuses and qualification credit are unchanged; no
cancellation-bound proof, Gate U completion, qualification, acceptance or delivery
is inferred.


## JS 50k timeout phase localization

The owner authorized localization of the failed JS50k/16 leaf. The author reused
the earlier temporary monotonic phase logger on that exact test, with all
assertions and deadlines unchanged. The first diagnostic reached replay entry at
51.018s and never reported successful return. Because it did not mark the outer
catch/finally, a second diagnostic added only those two logs to distinguish a
pending replay from a replay error followed by stalled cleanup. This was changed
observation, not an unchanged retry seeking a pass. Both native cases timed out at
the existing 180,000ms browser deadline.

The refined run observed these intervals:

| Observation | Monotonic diagnostic time |
| --- | --- |
| Initial commitment and independent readback complete | 2.633s since test clock start |
| Bulk backlog preparation | 13.674s |
| Three state-vector capture/comparison intervals combined | 33.375s |
| Physical close and reopen | 0.684s |
| Public replay entry | 50.598s since test clock start |

After replay entry, no successful-return, outer-failure or outer-cleanup-entry
marker appeared before the browser timeout. This supports localization to the
pending public `client.pushPending().getOrThrow()` boundary. Final verification
and final independent readback were not entered. It does not identify the internal
SQL/transport/worker phase, prove ongoing progress or a hang, or provide a completed
replay duration. About 129.4s of the nominal browser budget remained at replay
entry; that subtraction is approximate because framework and diagnostic clocks
start at slightly different points. Phase-log overhead is included. Native browser
XML durations provide no latency evidence.

The test was restored byte-for-byte after each attempt; stripping only diagnostic
lines from either retained source reproduces the entry source. No production,
build, fixture, Go source or deadline changed. No unchanged marker-free retry was
run solely to reproduce the known timeout. The 50k/1024 case remains unrun.
External cleanup passed, and exactly the two run-owned PostgreSQL databases were
confirmed absent at confirmation time. In-test client/OPFS cleanup is unconfirmed.

Evidence is in `.artifacts/js-50k-phase-diagnosis-2026-10-01/verification.json`.
The next bounded investigation should examine work inside the current public
replay boundary using existing source/profiler facilities before selecting an
optimization. The 33.4s vector cost is separate test overhead; removing it cannot
be claimed to make the incomplete replay meet its deadline. All twenty requirement
statuses and qualification credit remain unchanged. No cancellation-bound proof,
Gate U completion, qualification, acceptance, commit or delivery is inferred.


## JS 50k replay internal-cost investigation

The owner authorized investigation inside the pending public replay call. One
JS50k/16 diagnostic reused the existing four test-only replay markers and Chrome
CPU sampler, capturing the first 30 seconds of replay on the page, SQLite worker
and two OPFS helper workers. All assertions and the 180-second deadline remained
unchanged. The native leaf again timed out: replay entry is present, successful
return, accounting diagnostics and final completion are absent. This is a useful
profile of a failed run, not a runtime pass or completed replay measurement.

Mapped Kotlin coroutine frames show survivor FK-index preparation, workspace
accounting and plan saves across the sampled interval. The page profile includes
message handling, serialization and coroutine dispatch. The SQLite worker records
67.6% profiler-classified idle time and 7.4% Wasm-frame time; the page records 39.3%
idle time. These observations support investigating repeated small SQL requests
and host protocol overhead. Idle classifications do not identify the cause of
waiting. Inclusive samples overlap, threads overlap, sampling perturbs execution,
and the first 30 seconds cannot establish the full-call bottleneck or a hang.

Current common-source traversal explains a concrete removable cost. Before dirty
intent preservation, `OversqliteApplySurvivors.preparePlan` indexes physical
business-row foreign keys. For a nonfinal row in a one-FK table whose FK is NULL:

| Existing transition | SQL statements |
| --- | ---: |
| Read next key: size, workspace admission, key, outer workspace check, two plan writes | 6 |
| Check null FK, outer workspace check, two plan writes | 4 |
| Advance FK cursor, outer workspace check, two plan writes | 3 |
| Total | 13 |

This is a source-derived path count, not a measured executed-query count. The
nominal count for 50,000 such rows is 650,000 SQL statements, excluding setup,
end probes, actual edges, traversal, preservation and restoration. Reservations,
performed Oversqlite charged work and SQLite VM instructions remain distinct.
`reserveSyncWork` admits work and checks cancellation; it is not another SQL call.

The smallest recommended implementation candidate is to merge the two writes in
`OversqliteApplySurvivors.save` into one complete UPSERT at every existing save
checkpoint. That retains the owner and checkpoint frequency, reducing this path
from 13 to 10 SQL statements per row (nominally 150,000 fewer statements for this
backlog). Other phases also save one statement per checkpoint. This common Kotlin
change would reduce statements on all drivers; platform speedup and a 180-second
pass are unproved. No optimization was implemented in this investigation.

Before implementing, specify and test complete initial/existing plan persistence:
all cursors, guard, kind, accounting, atomic-work and preservation fields; preserve
failure precedence, transaction/rollback ownership and cancellation/work-limit
resume behavior. Use focused existing owner fixtures, affected platform/live
leaves and a matched performance comparison with final readback and cleanup.
Removing a database call must not silently remove a required cancellation boundary.

A larger row/FK transition consolidation could save more, but needs a separate
design for persisted cursor compatibility, partial progress on work refusal,
charging, cancellation checks and admission before payload reads. Existing
relationship projections are real, but equivalence to the physical business-FK
graph, including trigger-modified state, is not established. They cannot simply
replace this traversal. Existing statement caching still sends page, reset and
clear-bindings messages per warm use, so fewer worker messages cannot be assumed.
The inspected worker protocol has no fused prepare/page/close command; adding one
would be a separate core protocol and resource-lifecycle change. Neither direction
should be bundled into the small plan-write candidate. Removing state-vector
assertions or increasing deadlines is not recommended.

The test was restored byte-for-byte. No lasting production, test, build, fixture,
Go source or deadline change remains. Existing external cleanup passed; exactly
the run-owned PostgreSQL database was confirmed absent and the owned debugger
endpoint closed. Interrupted in-test client/OPFS cleanup remains unproved.
Source references, sampled frames, raw profiles, native failure and verification
are retained in `.artifacts/js-50k-replay-profile-2026-10-01/verification.json`.
JS50k/1024 is unrun; full replay duration, other runtime obligations and the central
performed charged-work cancellation bound remain open. All twenty requirement
statuses and qualification credit are unchanged. This investigation grants no
implementation, qualification, Gate U completion, acceptance, commit or delivery.


## Implementation plan: consolidate apply-plan saves

Status: **revised after plan review; ready for plan re-review; implementation not authorized by this document**.
The owner requested this specification after the internal-cost investigation.
This is one bounded common-source optimization under the existing persistence and
cancellation contracts. The investigation receipt above is its evidence baseline;
the profile is not a benchmark or proof that this change resolves JS50k/16.
The two P2 plan findings are patched below: failure evidence is assigned to
supported owners, and rollback is limited to failures before successful commit.
This is an author revision awaiting plan re-review, not an independent PASS.
Its documentation-only identity refresh is recorded in
`.artifacts/apply-plan-save-spec-review-fixes-2026-10-01/verification.json`;
the original plan packet and runtime evidence remain historical and intact.

### Objective and scope

Replace the two SQL writes in `OversqliteApplySurvivors.save` with one complete
UPSERT. Retain every save call, traversal transition, workspace check, reservation
and transaction boundary. The current null-FK indexing path then falls from 13 to
10 SQL statements per row; that structural reduction is distinct from elapsed-time
improvement and performed charged work. All platforms execute this common owner.
Only the selected runtime measurements can establish their respective speedups.

Production scope is `library-oversqlite/src/commonMain/kotlin/dev/goquick/sqlitenow/oversqlite/OversqliteApplySurvivors.kt`.
Use existing common survivor/replay fixtures and the statement/core owner seams
mapped below for focused tests. Prefer descriptive `ApplyPlanPersistenceTest` and
`ApplyPlanPersistenceCleanupTest` names if separate files improve registration.
Keep `ApplyPlan` and `save` private; exercise them through their existing owner.
Do not use reflection to construct the new save-failure witnesses or introduce
public/test APIs, production hooks, counters or a new fixture framework merely to
expose these private details. Any inability to reach a required assertion with
existing seams is a prerequisite gap to report before
production edits, not permission to omit it.

No public API, schema/migration, wire, Go, core driver, Swift bridge or build change
is included. Statement caching, fused worker commands, traversal consolidation,
relationship-projection substitution, reduced preservation or state-vector
assertions, new materialization and increased deadlines are excluded.

### Persistence and failure contract

Use an explicit INSERT column list, singleton key `1`, and
`ON CONFLICT(singleton_key) DO UPDATE` assignments from `excluded` for every mutable
column. Do not use REPLACE, a preliminary existence query or a follow-up UPDATE.
Keep the existing binding owner and types. The exact persisted field union is:

| Group | Columns and representation |
| --- | --- |
| Identity/state | `unit`, `guard`, `phase`, `operation_kind`: TEXT |
| Traversal | `root_cursor`, `visit_cursor`, `table_cursor`, `fk_cursor`: INTEGER; `row_cursor`: nullable TEXT |
| Accounting | `entries`, `bytes`, `edge_entries`, `edge_bytes`: INTEGER |
| Preservation | `preserve_schema`, `preserve_table`, `preserve_key`: TEXT; `atomic_work`: INTEGER; `preservation_complete`: INTEGER 0 or 1 |

Insert and conflict-update must write the same complete plan. Preserve null versus
empty text, Unicode bytes, Long precision and zero/false values. Existing durable
plans remain readable without migration; no new persisted phase/cursor values.
No SQL charge arithmetic or counter reinterpretation is introduced.

Keep `withPreparedStatement` as the statement owner and the current apply executor
as the apply/staging orchestrator; `SafeSQLiteConnection.transaction` owns commit
and rollback. A step/binding failure remains primary if close also
fails; close-only failure remains visible. Preserve categorized public failures
and diagnostic fields where the current contract defines them. Do not promise the
same raw SQLite diagnostic when two malformed fields now fail in one statement;
if such precedence is externally required, stop and resolve it before proceeding.

The two-write intermediate row is intentionally removed. It is not a new committed
checkpoint and must not become an asserted compatibility requirement. The existing
executor's intentional staging commit on `ApplyProgressPending` must still retain
useful resumable progress. Cancellation and ordinary failures observed before a
successful commit must still roll back the active transaction, including plan,
business, retained-ledger and source/checkpoint state. Earlier committed staging
and bundles remain usable. After a successful commit, preserve that commit and its
exact prefix even if cancellation is delivered before the caller observes success;
do not claim rollback or manufacture a second source/checkpoint advance. Retain
the existing pre-commit cancellation and late-successful-COMMIT regressions.
If rollback or cleanup fails, retain primary/suppressed failure ordering and the
existing unsafe-connection disposal behavior; do not report successful rollback
or safe reuse without evidence.
Verify that every production save path runs under its existing transaction owner;
an uncovered production autocommit path is a stop condition for this design.

Removing the second database call removes an internal suspension/check opportunity.
Retain cancellation observation before the save and after the completed save,
before subsequent traversal or success is reported, using existing coroutine
checks if needed. Keep outer per-transition checks and cleanup behavior. Do not add
a yield, timer, hook or database call to simulate the removed boundary. Check that
failure precedence is retained when cancellation and cleanup overlap. This specifies
cooperative boundary behavior; it promises neither native statement interruption
where unsupported nor the unresolved performed charged-work cancellation bound.

### Failure-evidence ownership

`JvmStatementCleanupFault.install` replaces `SafeSQLiteConnection.ref` through
reflection and currently injects reset/clear/close failures, not a plan-write step
or cancellation cut. It is not the construction route for new save-failure tests.
Preserve existing reflective diagnostic suites without expanding them or treating
them as new private-save evidence. Use these existing owners instead:

| Required evidence | Supported owner/seam and proof boundary |
| --- | --- |
| Actual plan-write failure and rollback | Existing survivor fixture and apply executor. A test-local SQLite `RAISE(ABORT,...)` trigger on plan INSERT/UPDATE, installed after fixture opening, can fail the actual write without a raw driver replacement. Compare the complete pre-transaction vector, require the failure and no later apply work, remove the trigger and verify reuse/resume. A database execution fault does not prove a binding or finalize fault. |
| Execution/body plus close failure, close-only failure and cancellation plus close failure | The uncached `withPreparedStatement` owner already accepts a statement body and `closeStatement` callback. Use the normal provider, close the real statement before throwing a close sentinel, and require primary/suppressed identity exactly once, rollback, no later body work and reuse. Use genuine caller cancellation at a bounded test-body barrier; distinguish a body sentinel from an actual SQLite execution fault. These tests exercise the shared helper, not raw finalize failure inside private `save`. |
| Raw cleanup retry/disposal, rollback failure and commit outcome | Existing nonreflective `SafeSQLiteConnectionIsolationTest` recording fixtures and `StatementClosePlatformTest` own these assertions. Keep their raw failure identity/order, physical close retries and rejection of unsafe reuse; retain worker late-COMMIT evidence separately. A synthetic helper close sentinel is not a fatal driver-cleanup witness. |

Source inspection must connect `save` to the unchanged uncached helper and existing
transaction owner, including the before/after-save cancellation checks. Existing
owner callbacks or work-limit checkpoints may observe completed saves; they do
not observe interruption inside the UPSERT. This is composed owner evidence, not
a claim of direct raw-fault injection into private `save`. Before production edits,
map exact cases/selectors to every required assertion. If a governing requirement
needs a direct binding/finalize or exact save-cancellation witness that these seams
cannot supply, retain that proof as blocked and settle the evidence requirement
under separate owner authority; do not substitute a helper test or add a workaround.

### Actionable sequence and required assertions

- [ ] **Establish entry and feasibility.** Reverify the latest author receipt,
  paired identities/inventories, sealed plan and applicable authority overlays,
  including AGENTS. Read `drymint-coding` before supported source edits. Map the
  assertions below to existing owner calls, fixtures, commands and available
  runtimes; resolve critical test seams and performance baseline before production
  edits. Capture exact dirty preimages and a production-capable explicit allowlist.
  Keep Go read-only and preserve historical attempts.
- [ ] **Characterize persistence on the old source.** Add focused common tests
  through existing owner operations. Cover absent-plan insertion and existing-plan
  update, including non-default kind/preservation fields and clearing previous
  nonzero/nonempty/true values to zero/empty/false/null. Query the complete persisted
  column union with independently chosen expected values. Cover pull, settlement,
  snapshot and conflict owner paths without changing their progression. Observe
  plans at existing work-limit checkpoints before successful cleanup deletes them.
  Include saved-plan resume/reopen and changed-context invalidation. Record which
  existing tests already provide assertions; do not duplicate them merely to add
  a new test name. These behavioral tests must pass against the old implementation.
- [ ] **Capture the matched performance baseline** described below, then make
  the single-owner UPSERT change. Inspect all 18 mutable column bindings and
  conflict-update assignments; preserve save call sites and cancellation checks.
  Structurally confirm one statement per save without adding a runtime counter.
- [ ] **Verify failure and cleanup at the mapped owners.** Cover the actual
  plan-write execution fault, helper execution/body plus close and close-only
  failures, and genuine caller cancellation at a supported owner boundary. Use
  the failure-evidence table to record which assertion each case supplies and its
  limitations. Require primary/suppressed identity, pre-commit rollback, no later
  operation work, cleanup and reuse/resume where safe; raw cleanup failure retains
  the existing retry/disposal contract. Preserve successful late commits. Verify
  that non-default expected values make a missing UPSERT update assignment
  observable; avoid tests that merely mirror SQL text. Do not label composed owner
  evidence a direct private-save fault or an in-statement cancellation witness.
- [ ] **Execute affected development lanes.** Run the owner/failure tests first,
  then the platform and live selections below. Inspect native case names, counts,
  failures/skips and actual completion markers. Archive reports before later
  invocations can overwrite them. A registration or cached task is not execution.
- [ ] **Measure and hand off.** Complete the matched candidate comparison, restore
  timing-only source exactly, and run the final-source live leaves. Update the
  existing requirement map with fresh/reused/subsumed/not-applicable/blocked lanes,
  scope checks and raw attempts. Preserve all twenty requirement statuses and
  qualification credit. Report review readiness separately from runtime blockers.

### Development validation surface

This slice uses the Development profile and the common recovery/persistence rows
of `docs-engineering/oversqlite-validation.md`. It is not a release aggregate.
Before execution, resolve exact new selectors from the authored tests; the following
existing tasks and cases define the minimum affected surface, not permission to
silently skip dependencies discovered during implementation.

| Lane | Selection and reason |
| --- | --- |
| Common owner | New persistence tests; `SurvivorApplyTest`, `SettlementReplayTest`, `SnapshotForeignKeyLifecycleTest`, `SnapshotRecoveryTest`, `WorkspaceAdmissionTest`, `WorkspaceCountingTest`, `WorkspaceUsageTest`, `ReservationFormatTest`: save/resume, guarded traversal, retained accounting and replay |
| JVM/helper faults | New owner/helper failure tests mapped above; existing `WorkspaceUsageCleanupTest`, `ProgressFinalizationJvmTest`, `PublicProgressFailureJvmTest` remain adjacent regressions, not new reflection-free save-fault seams |
| Core owners | `SafeSQLiteConnectionIsolationTest.closeRetriesCallerStatementCloseFailureBeforeLaterOwnedResources`, `cleanupFailure_marksConnectionFatalAndClosesLiveStatementsInReverseOrder`, `bodyFailureWithRawCloseFailure_preservesPrimaryAndSuppressedIdentityOrder`, `rollbackFailureIsSuppressedAndDiscardsConnection`, `cancellationDeliveredDuringPreCommitCleanupRollsBackInsteadOfCommitting`, `postCommitCancellation_propagatesWithoutRollbackOrFatalClose`; `StatementClosePlatformTest.callerAndOwnerShareRetryProgressOnTheActualPlatformStatement`; worker `SqliteWorkerDriverTest.lateSuccessfulCommitCancellationKeepsCommittedDataAndConnectionReusable` |
| Platform | `OrdinarySafetyPlatformTest.pullAndSnapshotActionSurvivors`, `workspaceEntryByteAndWorkBoundaries`, `lowerRetainedWorkspaceLimitsRefuseOpeningWithoutMutation`, `retainedLedgerTamperAndRollback`, `settlementCancellationAndRecreation`, `onePublicInvocationSharesWorkMeterAndRetainsUsefulFrozenProgress`, all eight `rebuildCancellation*` and both `pushCancellation*` cuts; `reachedDeleteConflictRetainsDirtyDependentBasesAcrossAdmittedActions` for conflict preparation |
| Live | `OrdinarySafetyLiveTest.committedReplayWith1000RowsWidth16` on JVM, JS browser, Wasm browser and macOS; matched JS10k/16 below; final-source JS10k/16 then JS50k/16 |

Common tests use `./gradlew --no-configuration-cache :library-oversqlite:<target>Test
--tests '<fully-qualified selector>' --rerun`. Platform tests use
`OVERSQLITE_PLATFORM_TESTS=true ./gradlew --no-configuration-cache
:platform-oversqlite-test:composeApp:<target>Test --tests '<fully-qualified selector>'
--rerun`. Resolve `<target>` to `jvm`, `jsNode`, `jsBrowser`, `wasmJsBrowser` and
`macosArm64`, applying only source-set-supported tests. JVM-only faults run on JVM.
Unsupported Node durable reopen gets no behavioral credit; execute its supported
same-lifetime cases and retain the limitation explicitly.
Core leaves use `:library-core:jvmTest`, `:library-core:<target>Test` for supported
common statement-close leaves, and the existing web tasks for the worker leaf,
with fully qualified selectors and `--rerun`. Record dependency-confirmed reuse of
unchanged core evidence under the validation runbook; missing provenance blocks
reuse. No core source, fixture or build edit is included in this optimization.

Reuse the existing isolated live launcher from
`.artifacts/js-50k-replay-profile-2026-10-01/run-case.py`, pinned by that receipt, in
a new ignored attempt directory at the same root-relative depth. Invoke
`python3 -B <attempt-packet>/run-case.py <target> <unique-label> 1000x16` (or
`10000x16` / `50000x16` for JS) with the existing PostgreSQL service URL supplied
through `OVERSYNC_EXAMPLE_TEST_DATABASE_URL`. Verify prerequisites and the launcher's
current task/selector behavior before execution; no new server provisioning or Go
edit. Retain normal database durability and independently owned fixture cleanup.
Never include credentials in artifacts. Run live families sequentially.

Android/iOS/Linux, Swift live and other required heavy leaves remain affected
obligations where unavailable. Record available access and exact blocked reason;
no substituted driver or workaround is required. This bounded development packet
may retain those gaps but cannot claim broader maintenance-candidate or platform
qualification. It must not treat unsupported interruption as a passed requirement.

### Performance experiment and completion criteria

Use **JS10k/16** for one fresh baseline/candidate comparison: it exercises the same
null-FK backlog shape at a size intended to finish under the existing 180-second
full-test limit. Its feasibility is not assumed: the unchanged baseline must
complete replay, assertions, independent readback and cleanup before production
edits. If it cannot, stop and report the missing measurement route; do not silently
shrink the case, increase a deadline or use a timed-out lower bound as a duration.

Use identical minimal test-only monotonic markers around the public replay call
for both runs, with the existing accounting diagnostics. No CPU profiler in this
comparison. Match source except for the candidate, toolchain, browser configuration,
server fixture inputs, test data, normal durability and fresh isolated initial
state. Record completed replay duration, native outcome, diagnostic values and
cleanup. Do not derive latency from browser XML case duration or compare with the
historical sampled 50k run. A single pair is development evidence, not a benchmark.

The candidate must preserve accounting/behavior, reduce the structural save count
from two to one, and have a lower completed replay duration in this matched pair.
If duration does not improve, or correctness/cleanup regresses, stop and report;
no opportunistic follow-on optimization or unchanged retries seeking a pass.
After exact timing-source restoration, final-source JS10k/16 must complete.
Then run the unchanged JS50k/16 leaf once: it still needs its complete assertion
union, readback and cleanup within 180 seconds. A remaining 50k timeout is an explicit
unresolved workload blocker, even if the smaller comparison improves. Stop there;
JS50k/1024 and further optimization require a separately selected next slice.

Completion is reported in separate parts: source/correctness evidence, measured
improvement, final 50k outcome and unavailable runtime obligations. Do not call the
50k problem resolved without its passing final-source leaf. An incomplete required
lane stays incomplete; no timeout is successful readback or in-test cleanup.

Use a 60-minute implementation window starting at implementation entry; report at
45 minutes. Stop earlier on unexplained relevant drift, unavailable critical
prerequisites or a correctness failure. Diagnose within scope; do not widen scope
or extend the clock without owner direction. No persistent goal is activated by
this plan. Implementation authorization, independent review, owner acceptance,
commit/delivery and Gate U completion remain separate decisions.


## Current UPSERT-source JS50k diagnosis (2026-10-03)

The owner authorized one bounded investigation and a narrow replay/workspace fix
only if a safe proof route could be established. Entry verification matched both
complete worktrees, the receipt/preimage chain,24 sealed artifacts and340 protected
pins with the explicit AGENTS and subsequent authorized overlays. The current
apply-plan UPSERT remained unchanged. The owner-supplied CLEAN disposition for its
previous scoped delta is preserved; it grants no acceptance to a new optimization.

One JS50k/16 diagnostic retained every assertion and the180-second deadline. It
reused the phase logger and CPU sampler with changed observation: two30-second
windows, approximately0-30s and60-90s after public replay entry. The current source
reached replay at47.110s; three state-vector intervals total30.923s separately.
Both windows include physical FK indexing, workspace scalar queries and plan-save
execution through their final seconds. The worker has execution/postMessage
samples. This establishes continuing sampled work inside production replay; it
does not prove durable cursor advancement, exact waiting causes, a hang, or a
whole-call bottleneck decomposition. Profiler idle classifications remain causal
unknowns. No successful replay return, outer failure, cleanup-entry, final readback
or completion marker appeared before the native180000ms timeout.

The current nonfinal null-FK row still has ten source-derived SQL calls across its
three existing transitions: five for next key, three for NULL FK, two for advance.
This excludes end probes, real edges and later phases; it is not an executed-query,
performed-work or VM-instruction measurement. Admission reservations, performed
Oversqlite charged work and SQLite VM instructions remain distinct. The inspected
Web backend has bounded paging, worker request/row metrics and a1000-VM-instruction
progress handler; no general capability absence on other backends is inferred.

No safe scoped production candidate was established. Combining size/key SELECTs
would transfer key TEXT through the worker before Kotlin workspace admission;
caching or fusing requests changes resource/core protocol handling; coalescing
row/FK transitions changes the separately governed progress/admission boundary.
No speculative source edit was made. The smallest proposed next design is one
worker-owned zero-row execution of the existing plan UPSERT, with unchanged save
checkpoints/fields and separately reviewed fault/cancellation/cleanup ownership.
It crosses this slice's explicit core protocol/resource exclusion and is recorded
for separate review in the new packet's `next-design.md`.

Diagnostic source was restored byte-for-byte to
`e1002a5025b3c8109dfa0183cc32e5cc960231167be79c202730616e0d30c244`.
The owned fixture/database cleanup passed, the exact run-owned PostgreSQL database
was independently absent, and the debugger endpoint closed. In-test client/OPFS
cleanup remains unconfirmed. No performance pair was run because no production
candidate was selected; the historical two-write baseline is not reused. No
unchanged marker-free timeout retry, deadline increase, assertion removal or
JS50k/1024 run followed restoration. The previous final-source timeout and this
current-source diagnostic failure leave JS50k unresolved.

Evidence: `.artifacts/js50k-current-replay-2026-10-03/verification.json`.
Only this documentation and the current status map change permanently, with new
overlays. All twenty requirement objects and qualification credit remain unchanged.
No persistent goal, Gate U resumption/completion, qualification, independent review
of a future optimization, owner acceptance, commit or delivery is inferred.


## Worker UPSERT feasibility investigation (2026-10-03)

The follow-up investigation revised the proposed one-request execution design.
Current Web close queues a one-way `closeStatement`; a subsequent worker barrier
reports its failures. Finalizing inside the awaited UPSERT response would move
that boundary. The smaller candidate for independent design review combines
prepare/bind/one zero-row execution and retains the existing owned close path: three
SQL-operation requests become two for each selected save, with no measured speedup.

Oversqlite and Core are separate modules, so the proposed bound-execution entry
point also needs an explicit cross-module API disposition. Cancellation phases,
command compatibility and an authorized test-only route for exact binding/finalize
faults remain prerequisites. The full findings, prospective owners and proof map
are in [the revised design](../.artifacts/worker-upsert-feasibility-2026-10-03/design.md);
[the receipt](../.artifacts/worker-upsert-feasibility-2026-10-03/verification.json)
binds the entry audit, source and native evidence.

Four unchanged JS browser worker leaves passed with zero failures/errors/skips,
covering ordered cleanup diagnostics, cancelled flush, late page cancellation and
ordinary page failure. This is Development evidence of existing behavior, not
proof of a new command or an actual raw-finalize fault. Production and test source
remain unchanged. JS50k/16 remains unresolved at180s; no timing comparison or
unchanged replay retry ran. The prior scoped CLEAN review and all twenty complete
qualification objects remain intact, with zero additional qualification credit.

## Worker bound-write design review revision (2026-10-03)

The independent feasibility review returned **CHANGES REQUIRED**, with ownership/
rollback, real fault proof, exported API and mixed-asset compatibility findings.
[The revised proposal](../.artifacts/worker-upsert-design-revision-2026-10-03/design.md)
now specifies each handoff phase, synchronous Safe registration before release,
non-cancellable close, command-local cleanup attempts, ordered errors and explicit
terminal disposal for uncertain cleanup. That disposal rule is a proposed semantic
decision, not established equivalence to the existing page path.

The draft also fixes an exact additive opt-in Core signature, consistent pre-step
row rejection and typed Int64 bindings; proposes wire v2 with fail-closed mixed-peer
behavior; and separates existing v1 durable marker identity from wire version.
Resource paths, durable filenames and lock identities stay unchanged. Pending API/
policy rows BW-1/2/3 are recorded in the replatforming specification as REVISE.

[The proof map](../.artifacts/worker-upsert-design-revision-2026-10-03/proof-map.md)
and exact prospective allowlist describe test-only generated worker/vendor fixtures
for actual thrown finalize, SQLite-denied rollback and active VM cancellation.
Logical map removal, native finalize entry/return and forced termination are distinct
oracles. Existing injected cleanup strings do not prove these cases. Fixture viability
and publication isolation must be established before dependent product work; runtime
metadata shows available routes, not passing Android/native execution.

This is a design-only revision pending independent rereview, owner disposition and
an accepted qualification profile for the wire/safety change. No production/test
source, timing source or runtime evidence was changed or generated. Prior browser
passes, current-source JS50k timeout, earlier UPSERT CLEAN and all twenty qualification
objects retain their original scope. JS50k/16 remains unresolved at 180 seconds;
no timing pair or replay retry ran. The receipt records fresh identity/authority/
scope verification, documentation overlays and historical chain limitations.


### Worker bound-write test-only prerequisite slice, 2026-10-03

The owner waived another design rereview and authorized the prerequisite slice before dependent production work. [The new evidence packet](../.artifacts/worker-bound-write-prerequisite-2026-10-03/verification.json) retains the immutable ten-file allowlist, complete entry preimages, Drymint evidence, failed attempts, final native outcomes, generated fixture bindings and authority overlay. The earlier UPSERT CLEAN disposition remains confined to its reviewed delta; all twenty qualification requirement objects and qualification credit remain unchanged.

Six authored test files generate isolated copies of the existing client/worker/vendor under a test-only build directory and invoke the existing raw worker API. Test-resource Gradle/Karma wiring serves those copies separately. Production worker/client/vendor bytes, protocol/cache/durable identities, safe owners, replay owners and the eighteen-field save remain unchanged. Native-finalize invocation/return is observed separately from logical statement-map removal. Throws before/after the real native call establish the missing cleanup fault route without introducing production finalize-return-code handling. Actual SQLite authorizer denial establishes one attempted rollback and zero successful rollbacks with an active transaction. An existing SQLite progress callback observes cancellation during active VM execution, returns interruption and produces SQLITE_INTERRUPT with SQLite automatic rollback; that is not an explicit worker rollback count or charged-work measurement.

Final native reports contain fifteen new raw fault cases (five each on JS Node, JS browser and Wasm browser), eight existing browser ownership/error cases, and two JVM statement-close cases with zero failures/errors/skips. Existing Android API31, macOS ARM64, iOS simulator ARM64 and Linux ARM64 routes each completed two statement-close cases. These establish executable routes and raw fixture prerequisites, not the proposed combined command, Safe acceptance/fatal-disposal behavior, a new API fallback, portable interruption timing or mixed-peer compatibility. Browser fixtures additionally terminate the owned workers, assert zero client ownership counters, reopen the owned durable database with zero uncommitted rows and delete it. Node memory execution does not grant durable evidence. Run-owned Android and iOS resources were stopped; no PostgreSQL resources were created. Explicit-route PostgreSQL preflight confirmed connectivity, superuser privileges and fsync/synchronous_commit/full_page_writes enabled; no live replay ran.

The earlier standalone publication verifier pass is retained. The final combined selection completed its native test leaves but failed publication verification because the generated production JS package was missing after the existing production-output cleanup task. Final publication isolation is therefore blocked, and dependent production implementation stops. The smallest next validation design is to separate the existing production-output cleanup from forced regeneration of the required production packages/distributions, then execute the unchanged publication verifier under changed task conditions; inspect the actual Gradle dependency/order route first. Do not turn this into an unrelated build redesign. No unchanged retry ran in this slice.

The machine entered clamshell sleep at 10:03:11 UTC and woke by lid activity at 12:36:08 UTC, with intervening dark wakes. The final log also contains a Chrome 180-second capture timeout followed by Karma's built-in startup recovery and completed tests. Sleep plausibly affected startup; it does not explain the separate missing-package failure. Wall time exceeded the requested from-entry maximum while the laptop slept; active execution time was not instrumented and is not equated with wall time. No substantive work resumed after that was observed; cleanup and evidence closeout followed. Timing source was not edited in this slice, and the prior exact restoration remains bound. No performance pair or final replay was run. JS50k/16 remains unresolved at the existing 180-second deadline. Fresh independent review of the new test/build delta, final publication proof and all proposed product/performance/platform/live obligations remain open; no qualification or owner acceptance is granted.


### Worker bound-write publication prerequisite follow-up, 2026-10-03

The owner authorized the next bounded publication-validation step. [The follow-up receipt](../.artifacts/worker-publication-validation-2026-10-03/verification.json) closes the final publication blocker under changed execution conditions. No product, fixture, build-script or runner source changed. The immutable allowlist covers only this execution-document append and a new entry in the existing current ledger; prior packets and all twenty qualification requirement objects remain intact.

The actual Gradle task graph identifies package-json and production-sync tasks as owners of the generated JS/Wasm package outputs, and identifies the existing cleanup task as a sibling dependency of distribution tasks. The preceding combined test/publication log shows both production sync tasks finishing before cleanup deletes those package directories, followed by the missing-package failure. The follow-up ran the existing core production-output cleanup alone, confirmed its three output roots absent, then executed both package-json and production-sync tasks to regenerate their required outputs. It finally ran `./gradlew --no-configuration-cache :library-core:verifySqliteWorkerPublicationAssets -x :library-core:cleanSqliteWorkerGeneratedProductionOutputs`. Only the cleanup already completed separately was excluded; both distribution tasks and the entire unchanged verifier executed successfully. Fixture-absence, resource-manifest, main-klib, dependency and production-output assertions remain intact. No upload or package publication occurred. The build ordering itself remains unchanged; this evidence establishes the explicit phased validation route, not an automatic combined-graph fix.

The prior thirty-three completed Development leaves are preserved and reused as unchanged prerequisite evidence, not relabeled as new runtime execution. Fresh paired identities, complete inventories/preimages, 1,343 historical artifact hashes, predecessor byte bindings, current authority overlays including AGENTS, twenty-four sealed Go artifacts and 340 protected pins were checked. Fifteen older predecessor edges remain historically unpinned. No PostgreSQL/live fixture was needed or launched. Timing source stayed byte-for-byte unchanged, and no JS10k comparison or JS50k replay ran. JS50k/16 remains unresolved at its existing 180-second deadline. Fresh independent review of the earlier test/build delta and the proposed product ownership/API/compatibility/performance/platform/live obligations remain separate gates; qualification credit remains zero.


### Authorized bound-write implementation: precondition stop, 2026-10-03

The owner supplied **CLEAN** for the exact preceding eight-file test/build delta and then explicitly authorized the separately scoped production implementation including Core API/protocol changes. [The new entry packet](../.artifacts/worker-bound-write-implementation-2026-10-03/verification.json) preserves that disposition and authorization. The prerequisite review and phased publication pass remain intact and do not establish the future combined-command or Safe-owner contract.

The required precondition audit found a concrete counterexample before production edits. `WorkspaceCountingTest.snapshotCountingIsChargedAndResumes` calls `owner.prepare` at line 54 on a ready plan after its counting transaction has ended. `SurvivorTestSupport` invokes the fixture block directly; `runOwnedWebDatabaseTest` supplies the unchanged 60-second test deadline and cleanup, not a transaction. `preparePlan` calls `save` before later atomic-work admission raises the expected `SyncWorkspaceLimitException`. The unchanged existing JVM leaf executed freshly and passed as one native case without failures/errors/skips. The proposed BW-1 contract requires an active Safe-owned transaction and rejects its absence before dispatch, so an unconditional save replacement would change this existing path's exception/admission behavior. This is a confirmed common fixture/owner path, not a claim that a production public replay currently saves outside its apply transaction. The production caller audit stopped at this required contract counterexample.

The revised design expressly states that a counterexample blocks the API contract rather than adding an implicit transaction. No production/test/build/timing source was edited, no performance baseline or candidate run began, and no JS50k retry ran. The smallest next design is to preserve current execution for plan saves outside a Safe-owned transaction and select the combined command only under established owner eligibility before dispatch, within the same exclusive owner. Settle whether that choice belongs in save or the common helper and update BW-1/proof selection accordingly; do not wrap/weaken the existing fixture, change checkpoints/charging/transaction ownership, or fall back after dispatch. [The concrete correction](../.artifacts/worker-bound-write-implementation-2026-10-03/smallest-next-design.md) is a proposal, not implementation or acceptance.

Paired identities/inventories, current/preimage and historical receipt hashes, predecessor bindings, overlays including AGENTS, twenty-four sealed Go artifacts and 340 protected pins were reverified. The two-file scope contains only this execution-document append and one new current-ledger entry; complete tracked/untracked entry contents were captured before these updates. All twenty qualification objects and qualification credit remain unchanged. Fifteen older predecessor edges remain historically unpinned. Timing bytes remain exactly restored; no live PostgreSQL/runtime fixture was required for this isolated JVM case. JS50k/16 remains unresolved at 180 seconds. Implementation authority persists, but this precondition stop must be resolved before dependent production work; no owner acceptance, qualification or delivery is granted.


### Bound-write candidate: compatibility prerequisite stop and restoration, 2026-10-03

The owner approved the BW-1 correction and resumed the separately authorized Core API/protocol slice. [The current packet](../.artifacts/worker-bound-write-candidate-2026-10-03/verification.json) retains the transaction decision: choose the combined capability only under established Safe-owned eligibility in the same exclusive owner; retain existing prepare/bind/step/close otherwise. The specification append is a design correction, not acceptance of a production implementation.

A fresh completed reviewed UPSERT-source JS10k/16 baseline measured the public replay at120996980000ns (120.997s), with the complete assertion union, readback and run-owned PostgreSQL/server cleanup. Minimal monotonic timing and performed-work accounting were identical prospective observations for a candidate pair. The candidate was drafted before its mandatory mixed-cohort route was resolved. Partial JVM/Node checks passed after two phase-oracle corrections and a JVM test return-type correction; failed native attempts remain preserved. The changed phase observations distinguish completed-command selection from an incorrectly awaited condition, and do not diagnose a replay hang. [The native outcomes](../.artifacts/worker-bound-write-candidate-2026-10-03/native-outcomes.json) and [findings](../.artifacts/worker-bound-write-candidate-2026-10-03/findings.md) state exact scopes and limitations.

The governing design requires Kotlin v2 with the real old-client/old-worker cohort and sticky terminal incompatibility before database allocation. Existing transport bridges statically bind the packaged client; the factory's worker URL override alone cannot supply that cohort. A bounded observation of the SHA-pinned legacy peers rejected a v2 metrics request and then successfully answered a v1 metrics request with zero databases, zero termination attempts and a nonterminal client before manual cleanup. Manual cleanup confirmed one worker termination and zero ownership counters. This is raw-peer evidence, not a Kotlin/Safe owner proof. The ordinary worker-error path and missing protocol identity on an error leave the intended Kotlin startup policy unestablished. [The smallest next design](../.artifacts/worker-bound-write-candidate-2026-10-03/smallest-next-design.md) proposes an existing-command Kotlin-owned protocol probe before open plus an isolated test-client cohort adapter; neither was implemented here.

All eight draft production paths, twelve test/fixture paths, one build path and temporary timing path were restored exactly from immutable entry contents. The complete draft and diff remain in the packet; the earlier reviewed source and test/build prerequisite are preserved. Timing source is again byte-for-byte e1002a5025b3c8109dfa0183cc32e5cc960231167be79c202730616e0d30c244. No candidate timing run, lower completed-duration pair, final-source JS10k or final-source JS50k ran. JS50k/16 remains unresolved at the prior unchanged180s deadline. The source-derived three-to-two SQL-request hypothesis is not a causal cost breakdown, performed-work measurement, SQLite VM instruction count or measured speedup. Backend VM accounting support was inspected but not collected in the minimal timing marker.

Only the authorized BW-1 specification append, this execution-document append and a current-ledger entry remain, with new authority overlays. All twenty qualification objects, historical receipts/pins and supplied CLEAN dispositions retain their exact prior scope. Ignored build outputs from the draft are invalidated by restored source and carry no final-source/publication claim. Compatibility/phase/fault/platform/live/export/publication/performance obligations and fresh independent review remain open. No qualification credit, persistent goal, Gate U, staging, commit, push, publication, deployment, owner acceptance or delivery occurred.

## Bound-write continuation: isolated client route, no production delta (2026-10-03)

The [current receipt](../.artifacts/worker-bound-write-resumed-2026-10-03/verification.json) records a fresh completed reviewed-UPSERT JS10k/16 baseline: public replay119.688445seconds, performed charged work11,043,864, complete assertions/readback/cleanup. Timing source was restored byte-for-byte. This supplies one Development baseline, no matched speedup, VM-instruction count or causal overhead breakdown. The initial launcher readiness-log race and cleanup outcome are retained separately.

The generated static client adapter selects a SHA-pinned old client only via an explicit cohort URL and preserves startup Promise ownership. Eight selected Node, eight browser and eight Wasm cases completed on v1 source; Node preceded the browser-only webpackIgnore correction. Raw native-finalize/rollback/VM evidence remains limited to existing commands and manual termination. The compiled Kotlin-v1 JS and Wasm test trees are captured for a later actual reverse-cohort proof; v2 execution has not occurred. Both adapter regressions and all failed attempts are retained.

The owner authorized the Kotlin startup metrics probe; implementation remains pending. A clock read advanced from15:09:27UTC to16:20:54UTC, beyond the14:51:36UTC entry and15:51:36UTC hard stop. No extension was supplied. Substantive work stopped on observation; cleanup archived and restored all ten test/build draft paths and retained native outcomes. The45-minute checkpoint was crossed during that unobserved interval; no subtraction from wall elapsed or timely-stop claim is made. Publication exclusion and mandatory combined-command proof/validation/performance exits remain open. Final-source JS10k and JS50k were not run; JS50k/16 remains historically unresolved at180seconds.


## Bound-write completion attempt: shared fixture gate stop (2026-10-03)

The [new packet](../.artifacts/worker-bound-write-completion-2026-10-03/verification.json) preserves one immutable32-path scope and the explicit additional60-minute window granted during the initial slice. The original45-minute report was approximately four minutes late; the extension checkpoint was reported around45minutes. No persistent goal or Gate U was activated.

A fresh completed reviewed-UPSERT JS10k/16 baseline measured public replay122.903715seconds with performed charged work11,043,864 and complete assertions, readback and isolated PostgreSQL/server cleanup. The cross-module eighteen-field fixture completed an isolated3-to-2 SQL-request observation, excluding the metrics control request and confirming zero page requests for the completed combined write. That establishes a structural request reduction, not a causal overhead fraction or measured speedup. Admission reservations, performed charged work, worker requests and SQLite VM instructions remain distinct. The generated vendor exposes sqlite3_stmt_status; VM instructions were not collected.

The restored adapter's phased publication verifier passed before product edits. Actual captured Kotlin-v1/v2 cohorts were exercised on Node/JS/Wasm. New ownership proofs cover startup worker errors, cancellation after entering a lost metrics request, real binding and trigger failures, completed-handle initialization, held/lost responses, acknowledgement cancellation, asynchronous release failure, malformed replies and bounded termination retries. Browser global error reports from intentionally thrown worker faults required a generated test-only preventDefault while preserving the real transport failure handler. The old/new durable fixture observes shared lifecycle-lock exclusion, exact Int64 readback and owned deletion. Its first Wasm attempt reopened before asynchronous old-worker lock disposal; the corrected observation waits for that owned Web Lock release before one subsequent open. Failed attempts remain retained.

Selected Core/JVM51 and Node90 cases passed. JS Core89 cases and the separate corrected upgrade leaf passed; Wasm89 unchanged passing leaves plus its corrected upgrade leaf supply constituent Development evidence, with the original failed90-case attempt preserved. Core4 and Oversqlite26 cases passed on macOS/iOS/LinuxARM64; Oversqlite26 also passed on JVM/Node/JS/Wasm. Android executed the two new Core value cases and three plan-persistence cases; native reports of the comma selection omitted adjacent classes, so those obligations remain open. These counts do not grant qualification or replace live evidence.

The mandatory provider selection completed11 cases:10 passed, including the request-count fixture; the existing lifecycle/schema case failed because its shared exact _sync_conflict_state column oracle omitted baseline_revision. The unchanged entry DDL includes that field. Both the shared assertion and DDL match captured entry hashes, so this is an established pre-existing fixture mismatch rather than a demonstrated candidate schema regression. The shared assertion owner, MigratedOversqliteStateTest.kt, is outside this slice's allowlist. Dependent work stopped rather than changing that owner or removing an assertion. [The smallest next correction](../.artifacts/worker-bound-write-completion-2026-10-03/proposed-shared-fixture-correction.diff) adds only the missing expected INTEGER/NOTNULL column and requires a separately established scope.

All32 paths, including previously missing files, production owners and timing source, were restored exactly after archiving the complete candidate and diff. Only this status append and one current-ledger entry remain with new overlays. Run-owned Android/iOS resources were stopped. Candidate/final publication outputs are invalidated; no final-source/runtime claim is made for restored build artifacts. No candidate timing pair, final-source10k or final-source50k ran after the mandatory gate stop. JS50k/16 remains unresolved at its historical180-second deadline. All twenty qualification objects, prior scoped CLEAN reviews, historical receipts/seals/pins and unrelated work remain preserved. Fresh independent review and all affected qualification/owner/delivery obligations remain open; no staging, commit, push, upload or deployment occurred.


## Fixture follow-up and required maintenance guidance (2026-10-03)

The owner authorized the next prerequisite correction and explicitly required persistent **MUST** guidance. `AGENTS.md` now requires validation selection before edits, contract-backed inspection of affected fixture assumptions together, prerequisite/production separation, provenance-based evidence reuse, justified reruns, and continuation from a bound current ledger in fresh sessions. The [new packet](../.artifacts/worker-bound-write-fixture-followup-2026-10-03/verification.json) retains guidance preimages, its new authority overlay, immutable entry scopes and the predecessor seal. No existing qualification requirement or mandatory final check was waived.

The one-column correction was insufficient: a 13-case JS selection completed in31.620seconds with4failures, exposing stale row-state columns and the old14-table count. Inspection of unchanged authored DDL identified21control tables, changed baseline fields and superseded indexes. A single-file test-only draft reconciled static column/index expectations, table count and row capture, with fixture setup, frozen hashes and all production owners unchanged. A changed-source 13-case selection completed in24.446seconds: all10provider cases and1migration case passed;2migration cases still assumed an empty row baseline while the current initializer creates an unpublished baseline. These failures occur before worker import and do not establish a worker regression. The draft and both native attempts are retained; test source was restored exactly because the complete affected-consumer gate remains open.

The smallest next prerequisite is an independently specified never-published baseline oracle, preserving exact provenance and before/after import/reopen equality, followed by the same13JS and13Wasm consumers. Do not seed away that baseline, rewrite a frozen request hash, weaken assertions, restore the archived production optimization by default, or launch its platform/live/performance campaign during this fixture repair. Current Kotlin/Go identities, historical evidence and all twenty qualification objects remain protected. No fresh performance pair or final replay case ran; JS50k/16 remains historically unresolved at180seconds.


### Browser migration fixture baseline repair, 2026-10-03

The owner authorized the reduced prerequisite batch: one web-test fixture owner and the existing13JS/13Wasm consumer selection. [The receipt](../.artifacts/worker-bound-write-fixture-baseline-2026-10-03/verification.json) records immutable preimages, contract/source bindings, Drymint attribution, native reports and scope/authority checks. The fixture now asserts the current21control-table columns/indexes and the complete never-published row baseline. Initial capture precedes source binding; admission remaps the schema and binds the generation, then attachment binds the account. Expected provenance and revision1/4 come from those existing owners and the governing absent-base contract. Full row capture retains server kind/payload/provenance, relationships and revision; continuity capture also retains the baseline revision and historical dirty base. The completed row assertion uses the current live kind. Fixture seeding, frozen initialization/request hashes, exact import/reopen equality, lifecycle/cancellation assertions and cleanup remain intact.

Both unchanged selectors completed freshly:13JS and13Wasm cases, zero failures/errors/skips, with invocation times34.923s and27.901s including build/setup. These are Development prerequisite results, not replay timing or qualification. Prior failed attempts remain sealed. Production/build/assets, Go source and timing source are unchanged; the optimization stays archived. NoJS10k/JS50k ran, and JS50k/16 remains unresolved at the historical180-second deadline. Fresh independent review of this fixture delta remains separate; all twenty qualification objects, prior scoped CLEAN dispositions and delivery restrictions remain unchanged.


### Bounded cross-platform live replay measurement, 2026-10-03

The owner selected the existing1000-row/width16 case for a reduced timing batch. [The current packet](../.artifacts/replay-platform-1000x16-timing-2026-10-03/verification.json) retains one sequential isolated PostgreSQL/Go-server launch per configured platform, manually selected under the Development profile. This replays a small committed response amid1000 unrelated dirty rows;16 is the ASCII character width of the name field, not a column count or complete payload size.

| Platform | Public replay seconds | Full case seconds | Launcher seconds |
| --- | ---: | ---: | ---: |
| JVM | 0.803 | 2.404 | 36.044 |
| macOS ARM64 | 4.661 | 15.753 | 56.681 |
| JS browser | 13.066 | 19.102 | 37.762 |
| Wasm browser | 12.722 | 19.011 | 30.501 |
| iOS simulator ARM64 | 4.710 | 16.458 | 59.832 |
| Android emulator | 3.324 | 8.914 | 75.372 |

All six cases completed with the full assertion union, final readback and client/server/database cleanup, zero failures/errors/skips. Existing accounting is identical, including performed charged work1,107,864; this is distinct from admission reservations and SQLite VM instructions. One sample per platform is Development evidence, not statistical/portable performance qualification or a causal explanation. Android/iOS use an emulator/simulator on this host. Linux remains blocked because the shared live-test module has no Linux target; no build wiring was added.

The two temporary monotonic markers were restored to exact entry bytes, e1002a5025b3c8109dfa0183cc32e5cc960231167be79c202730616e0d30c244; retained timing diff and native reports show the observation. Product source/assets/build wiring and Go source remain unchanged. Only this append and one current-ledger entry remain, with new overlays. All twenty qualification objects, historical evidence, explicit AGENTS overlay and prior scoped CLEAN dispositions remain protected. NoJS10k/JS50k retry or production optimization occurred; JS50k/16 remains unresolved at its historical180second whole-test deadline and its actual completed replay time is unknown. The retained prior fixture correction still requires independent review; future production work requires its own selection/review/qualification. No goal, Gate U, staging, commit, push, publication, deployment, owner acceptance or delivery.


### Selected 10K/16 live replay measurements, 2026-10-03

The owner authorized the existing 10,000-row/width-16 case on JVM, macOS ARM64, iOS simulator ARM64 and Android emulator, excluding JS, Wasm and Linux. [The new packet](../.artifacts/replay-platform-10000x16-timing-2026-10-03/verification.json) records a manually selected Development batch with four sequential isolated local PostgreSQL/Go HTTP fixture launches. The existing heavy opt-in enables this exact case; no other heavy selector ran, and the 900-second native test deadline and complete assertion union remain unchanged. This replays a small committed response with 10,000 unrelated dirty rows, rather than uploading those rows; width 16 is the name ASCII-character count.

| Platform | Replay seconds | Full test seconds | Launcher seconds |
| --- | ---: | ---: | ---: |
| JVM | 7.046 | 10.903 | 16.219 |
| macOS ARM64 | 44.044 | 137.547 | 141.082 |
| iOS simulator ARM64 | 44.429 | 139.501 | 144.427 |
| Android emulator | 27.748 | 68.172 | 85.817 |

All four named native cases completed with zero failures/errors/skips, complete assertions, independent server readback and owned cleanup. Accounting is identical, including performed Oversqlite charged work 11,043,864; admission reservations and SQLite VM instructions remain separate. Full test includes setup/assertions/readback/client cleanup; compilation/server startup and server/database cleanup belong to the outer launcher. These single samples are Development measurements, not statistical/portable qualification or a causal overhead breakdown. Prior 1K source/fixture/runner/build provenance remains intact and was reused without rerunning that case.

The identical two monotonic markers were restored byte-for-byte to e1002a5025b3c8109dfa0183cc32e5cc960231167be79c202730616e0d30c244. No production, build wiring, resource, schema, wire, charging, checkpoint or Go-source change remains. Only this append and one current-ledger entry remain with new authority overlays. All twenty qualification objects, historical sealed evidence, protected pins, inherited explicit AGENTS overlay and earlier scoped CLEAN dispositions retain their scope. JS50k/16 was not selected or rerun; it remains historically unresolved at the 180-second whole-test deadline and its completed duration is unknown. The retained earlier fixture delta still requires independent review, and future production source requires fresh review. No goal/Gate U, staging, commit, push, publication, deployment, owner acceptance or delivery.


### Owner-directed 50K workload retirement, 2026-10-04

The owner removed 50,000-row workloads from the active test roster pending a future explicit reintroduction. [The retirement packet](../.artifacts/replay-50k-retirement-2026-10-04/verification.json) records the exact test-only delta: the mock and live settlement loops now select 1,000 and 10,000 rows; the memory subclass inherits that roster; the platform live test retains four registrations at widths 16 and 1024. All retained test bodies, assertions, shared helpers, deadlines and production behavior are unchanged. The unrelated 50,000-unit charging budget remains intact.

Fresh Development checks completed all three selected JVM library cases, each exercising the four retained configurations, and all four platform JVM replay cases against an isolated local PostgreSQL/Go HTTP fixture. Native reports show seven completed cases, zero failures/errors/skips, full original completion markers and successful owned cleanup. Affected JS, Wasm, macOS ARM64, iOS simulator ARM64 and Android test compilations completed for both modules; these are compilation evidence, not fresh platform runtime results. Unchanged retained helper/body and product/fixture dependencies permit reuse of prior platform evidence within its original scope. No 50K runtime, timing pair or release aggregate ran.

This prospective roster change supersedes active 50K scheduling. Historical specifications, timeouts, measurements, receipts and protected evidence remain intact: the old JS50k/16 timeout is retired from the active roster, not resolved or converted to a pass. All twenty qualification requirement objects and earlier scoped CLEAN dispositions remain unchanged. The new test/documentation delta has no independent review or qualification credit; prior pending review obligations remain separate. Production/build/Go source and unrelated worktree/index state are preserved. No goal, Gate U, staging, commit, push, publication, deployment, acceptance or delivery.

# Oversqlite convergent interactive sync

Status: **REVISED PROPOSAL — findings addressed in draft; not independently rereviewed, accepted, or authorized for implementation**

Created: 2026-09-24

Proposed FK/resolution amendment (2026-09-25):
[FK-safe sync](oversqlite-fk-safe-sync-and-automatic-resolution.md) and its Go p0001 authority extend
the behavior below. On acceptance, their indexed server-visible relationship contract replaces
conservative legacy-capture grouping; their targeted endpoint addresses the larger FK-resolution
scope. The earlier observation that convergence alone needed no new endpoint remains historical,
not a prohibition on that extension. Preserve the existing push budget, pause, cancellation and
recovery rules. Ordinary settlement still preserves pending intent; only the explicit group policy
may discard affected intent with its atomic report. This proposed amendment grants no runtime
authorization and does not change the historical review/acceptance record below.

Revision: 2026-09-24, addressing a findings-bearing independent review of first-draft SHA-256 `76aa0fb7484a99e8b2d5d46795e6fa4d0384f819ed2afc3a15bec571a8a972e7`. Review input: `/Users/pochkin/.codex/attachments/0fea85ad-a678-4720-9912-98c2f7bc1a87/Pasted text.txt`.

Planning home: `/Users/pochkin/Projects/my/sqlitenow-kmp`

Paired client: `/Users/pochkin/Projects/my/go-oversync/oversqlite`

This is one proposed behavior contract for the Kotlin Multiplatform and direct Go Oversqlite clients. It incorporates the ten findings in the read-only review of the first draft. The sibling Go checkout remains read-only during this revision. Before paired implementation, record the decision in both governing specifications, reconcile affected documentation, and obtain explicit authority for changes in each repository. The review found no demonstrated need for a new HTTP endpoint, OpenAPI field, corpus wire fixture, or server behavior.

At drafting time SQLiteNow HEAD is `99eb0c6c88b061972f4fc2b9932acf7c3d3c0f99` with no reported tracked changes. Go HEAD is `c5a9f4198be938401758216587a9901ddd54aaaa` with pre-existing edits to `integration/mobile_flow/docs/complex-multi-batch-scenario.md` and `integration/mobile_flow/simulator/scenario_complex_multi_batch.go`. These are observations, not implementation pins. Recheck both complete tracked, untracked, and ignored governing state before any later edit; preserve unrelated work.

## 1. Problem and decision

Today each high-level `sync()` / `Sync(ctx)` makes one bounded push and then attempts an incremental pull. A push freezes only a subset of dirty rows when row or byte limits apply. A local write can also arrive after the freeze. In either case, the pull correctly rejects outstanding dirty rows, but the high-level sync reports an error for ordinary activity. In the 2026-09-24 `samplesync-kmp` log, two rows were pushed, one new row remained, and the first pull raised `DirtyStateRejectedException`; the queued second sync completed.

The direct Go client has the same single-push path. Its optional `Start(ctx)` uploader repeatedly attempts `PushPending`, but direct `Sync(ctx)` does not drain a backlog. The KMP automatic download worker is download-only and cannot substitute for an upload drain.

**Decision:** high-level interactive sync owns bounded local upload drainage and the transition to pull. An application calls sync once for an ordinary backlog or a write racing a push; it does not implement a retry loop for this normal case. The existing `pushPending`/`PushPending` one-bundle primitive and `pullToStable`/`PullToStable` fail-closed primitive remain available. Push settlement, pull apply, and recovery must preserve every newer local intent, including rows outside the frozen bundle.

## 2. Normative behavior

### 2.1 Safe one-bundle primitives are prerequisites

The high-level loop cannot make an unsafe one-bundle primitive safe by repeating it. Implement and test these prerequisites before claiming convergence:

1. **KMP committed-push settlement:** plan every authoritative effect, including extra-effect keys that were not in the uploaded bundle, and capture pending local intent that an effect or application trigger could disturb. If an extra-effect key is locally dirty, preserve its desired business row and dirty intent, update its authoritative base metadata, and requeue it as needed. After all effects in the same transaction, restore every preserved desired row whose business state changed and its dirty disposition before clearing the frozen outbox. Cover same-key newer writes, dirty keys outside the batch, cross-effect triggers, and restart replay. Go's existing extra-effect and restoration behavior is a parity reference; test trigger reachability in both clients rather than assuming it covers every unrelated dirty key.
2. **Go dependency-safe freeze:** select indivisible FK dependency groups, including deferred cycles, rather than stopping on an individual row at the soft byte target. Send a complete group that exceeds the soft target when its complete compact request fits the admitted hard row and decoded-byte limits. If the group cannot fit the hard limit, return a typed unsendable-group failure before freezing an outbox or consuming a source-bundle ID. Preserve the one-bundle `PushPending` API. KMP's existing intact-group behavior is the parity reference.

3. **Both pull appliers:** enforce the local dirty/outbox gate inside the same short write transaction that applies each fetched bundle, before enabling apply mode or changing business rows or checkpoint. A write committed after the pre-HTTP gate but before this transaction must prevent that bundle's apply and checkpoint advance. Earlier fully committed bundles may remain applied. The explicit pull APIs retain their typed local-work rejection.

For FK-bearing dirty rows, both clients retain the current typed local capture and the server-base typed capture from the first unsent write as a two-element local payload. Subsequent writes before freeze preserve that base. A legacy capture without a trustworthy base is grouped conservatively. Push wire rows still contain only the current payload, and wire deletes remain payload-free. Batch preparation reads a bounded initial page and expands only dependencies needed by that page; a complete group may cross a soft target but not an admitted hard limit.

### 2.2 Invocation accounting and upload drain

1. Create one invocation state outside KMP's whole-operation transient/snapshot-capacity retry wrappers and outside Go's HTTP retry machinery. It persists across retries of the same public call, checkpoint recovery, and `syncThenDetach` rounds. No retry starts a fresh work allowance or erases confirmed progress.
2. High-level sync repeatedly uses the safe bounded push primitive under the existing exclusive client operation. Reuse a prepared outbox before freezing more rows. After each settlement, read durable dirty and outbound state; zero permits a pull attempt, positive work starts another push. Each bundle still has its own admitted row/byte limits, source sequence, idempotent hash, and wire request. The current no-change and one-row traces remain one pull and one push followed by one pull.
3. The **64-attempt allowance** counts each entry to push preparation/settlement that could freeze, replay, or send one bundle. Count recursive conflict reentry, frozen-outbox replay, recovery pushes, and whole-operation retries against the same allowance. A local no-op pass counts too. Transport retries of the *same* prepared HTTP request remain separately bounded by the existing retry policy and do not reset or multiply this allowance. Check the allowance before attempt 65. A push that finishes attempt 64 with zero pending rows may still pull; a late write requiring another push returns incomplete. No loop may reset the counter under a wrapper.
4. Track confirmed committed bundles by durable `(source_id, source_bundle_id)`, not by invocation or HTTP response count. Ambiguous replay of the same committed bundle counts once when confirmation arrives. Track the aggregate push outcome as `COMMITTED` if any unique bundle was confirmed, including before a transport retry, recovery branch, or final no-op. Aggregate updated tables and notifications across the whole invocation; a later failure does not hide already durable changes. The final successful pull/rebuild determines remote outcome and final status.
5. If work remains at the allowance, return a public typed incomplete-sync error with current dirty/outbound counts, confirmed unique committed-bundle count, and reason `push_attempt_limit`. If a no-change pass leaves the *same durable work identity* unchanged, return reason `no_progress` without spinning. Counts alone do not prove no progress: pruning, conflict resolution, or concurrent replacement may keep the same count while changing keys, ordinals, payloads, outbox state, or source sequence. Determine identity with bounded memory; do not materialize an unlimited dirty backlog solely for this check. If identity cannot be proved unchanged, continue subject to the allowance. Already committed effects and remaining intent stay durable; do not attempt pull over local work or report full success.
6. A conflict, rejection, source-recovery gate, generation retirement, authentication failure, unsendable row/group, or other terminal precondition retains its own error and precedence. Do not classify all `STATE` errors as retryable. Caller cancellation, Go context deadline, and existing request timeouts retain priority over an incomplete result.

### 2.3 Writes racing pull and recovery

High-level sync treats only a typed dirty/outbox rejection from the pull apply gate as a request to resume its bounded drain. It does not retry arbitrary pull, transport, or lifecycle errors. After an apparently successful pull, it checks pending state again: a write during a zero-bundle response or after the last applied bundle causes another drain. A successful return is linearized at that final local read; a later committed application write is new work. Do not hold a SQLite write transaction across network requests.

Checkpoint recovery has explicit precedence over ordinary push/pull. When `rebuildRequired` is set, use the recovery-authorized push path to drain *all* replayable dirty/outbound work within the same invocation allowance before snapshot creation/final apply; ordinary push still rejects this state. If new local work appears at the snapshot final gate, leave the staged snapshot and durable recovery marker safe, return to **recovery-authorized** drainage, and retry the recovery transition within the remaining allowance. Do not fall through to ordinary push. Distinguish this local-work gate from source replacement, push-replay-history-pruned, authentication, and lifecycle errors; preserve their current precedence and prepared-outbox recovery modes. A recovery push that committed contributes to the aggregate `COMMITTED` outcome. Direct `rebuild()` and direct pull retain fail-closed semantics unless their existing recovery contract explicitly permits the transition.

### 2.4 Pause policy by client and entry point

| Client and entry point | Upload pause | Download pause |
| --- | --- | --- |
| KMP explicit `pushPending()`, `pullToStable()`, `sync()`, `syncThenDetach()`, `rebuild()` and recovery | Background-only pause; explicit work continues, including between batches. Remove the current contradictory explicit checkpoint-recovery pause check. | Background-only pause; explicit work continues. |
| KMP automatic downloads/watch | This worker never uploads. | Suppress pull while paused; retain the pending watch target and resume later. |
| Go direct `PushPending(ctx)` | Return its existing skipped-paused outcome without a push. | No effect on this upload-only entry point. |
| Go direct `PullToStable(ctx)` and `Rebuild(ctx)` | No effect on these download/recovery entry points. | Preserve their skipped-paused behavior after local safety gates; never apply over dirty/outbound work. |
| Go `Sync(ctx)` and its checkpoint-recovery path | Stop before another push when paused. If local work remains, return typed incomplete reason `uploads_paused` without pulling. If no local work remains, proceed to the download phase. | Preserve `SKIPPED_PAUSED` remote reporting; do not claim full remote convergence. |
| Go `SyncThenDetach(ctx)` | Follow the Go sync rule, but map an incomplete sync to blocked detach. | Map a skipped remote phase to blocked detach; do not detach. |
| Go `Start(ctx)` uploader/downloader | Preserve pause behavior without spinning. | Preserve pause behavior without spinning. |

Go incomplete `uploads_paused` includes accurate pending counts and confirmed progress. It does not consume an extra push attempt merely to observe the pause. KMP explicit operations ignore pause flags even if they change during a call. Neither client may apply a remote bundle over pending local work. Paused behavior must be documented through KMP, Go, and Swift public surfaces rather than inferred from a generic error category.

### 2.5 `syncThenDetach` composition

Both clients share **one 64-attempt allowance across the entire `syncThenDetach` invocation**, not one allowance per round. Keep at most three sync/detach rounds. `syncRounds` counts started rounds, including an incomplete final round. Each round uses the same cumulative committed-bundle and notification accounting. If drainage reaches its allowance, cannot progress, or is stopped by the applicable Go pause, do not attempt detach; return the existing blocked-detach outcome with accurate pending count and a new typed blocked reason (`push_attempt_limit`, `no_progress`, or `uploads_paused`). If Go downloads are paused, return a blocked result with reason `downloads_paused` before detach. A new write between a completed sync and detach may start another round within the shared allowance; count equality alone is not a stop rule. Stop only at the round/attempt bound or when durable work identity proves no progress.

Make `lastSync` optional in KMP, Go, and Swift results: it is absent if no round completed a full sync, otherwise it is the last completed report. Keep conflict, recovery, auth, and cancellation failures as errors rather than converting them to blocked detach. A blocked outcome never cleans up local attached state. Update callers and live tests that assume two writes or a count-only stopping rule.

### 2.6 Cancellation, notifications, and background liveness

- KMP and Go cancellation stops scheduling another batch, recovery attempt, or detach after the current cancellation boundary; durable partial effects and frozen intent remain replayable. Swift's facade must make a caller's cancelled `sync()`/`syncThenDetach()` cancel and join the owned KMP operation before cancellation returns, not merely stop awaiting an independently launched `SupervisorJob` child. Client close remains a distinct lifetime operation.
- Notify observers for every transaction that durably changes managed tables, even if a later step returns an error or KMP's whole-operation wrapper retries. Replay of the same committed bundle may not double-count confirmed bundles; notifications must at least cover every unique durable change. Aggregate report fields must not revert to `NO_CHANGE` after a committed push followed by retry.
- Keep source identity, source-bundle sequence, structured row metadata, business rows, dirty intent, outbox, and remote checkpoint consistent across every success, partial failure, cancellation, and restart. Preserve exclusive client operations without holding a SQLite write transaction across HTTP or an application callback.
- KMP watch mode retains a high-water target until the local checkpoint reaches it or a terminal condition explicitly ends the worker. Contention, transient failure, and download pause leave a pending target. Use a bounded, cancellation-aware wake-up while the watch HTTP stream remains open, and also wake on resume or operation-release signals where available. Equal high-water heartbeats need not be treated as new events. Do not busy-poll or apply while paused. This is part of the same user-visible sync liveness work.
- Go paused polling downloader currently loops without waiting. Repair it as a **separate focused child**: use a cancellation-aware wait/resume mechanism and prove prompt shutdown while paused. Keep its test and evidence separate from interactive-drain acceptance; do not silently count it as proof of the new `Sync(ctx)` behavior.

## 3. Public surfaces and consumers

- **KMP:** update committed-push replay, the pull apply gate, common coordinator, invocation accounting, recovery transition, progress, public incomplete type, and documentation. `syncThenDetach()` shares the invocation state without nesting an exclusive operation. Direct `pushPending()` and `pullToStable()` retain their one-bundle/fail-closed meanings. Align explicit recovery with the documented background-only pause contract.
- **Go:** fix dependency-safe push selection and update `Client.Sync(ctx)`, recovery, `SyncThenDetach`, typed incomplete result, optional last-sync report, and documentation. `Start(ctx)` continues to own its uploader/downloader policy; it must not accidentally run a second competing interactive drain. Direct `PushPending(ctx)` and `PullToStable(ctx)` retain their primitive contracts. The paused polling fix is separately scoped in section 2.6.
- **Swift:** the facade delegates to KMP but owns caller cancellation through completion or cleanup. Map the typed incomplete error, optional last-sync result, blocked reason, and aggregate outcome. Update public documentation and tests; do not implement a separate Swift retry loop.
- **`samplesync-kmp`:** continue calling the high-level `sync()` for initial, automatic, and manual sync. Do not add a sample-owned normal-case retry loop. Show the exceptional bounded-incomplete result as pending work, not lost data; clear a prior transient error after a later successful sync. Preserve error visibility for terminal failures.
- **Other callers:** inventory Go examples/mobile flow, KMP generated consumers, and tests that assume one push always suffices or that `lastSync` cannot be absent. Update only affected call sites. No on-wire version change is proposed.

## 4. Required tests

Use deterministic barriers or controlled transports, not timing sleeps, for the race tests. Assert business rows, dirty payload/base metadata, outbox state, source-bundle sequence, checkpoint, notification behavior, and restart/replay state where the case can affect them. A count-only assertion is insufficient for preservation.

| Case | Required result |
| --- | --- |
| No dirty rows; one dirty row | Existing one-pull and one-push-then-pull traces remain exact. |
| Stationary backlog above row limit; backlog above soft byte target | One public high-level call sends multiple separately bounded pushes, then pulls and returns with no pending rows. No caller loop assists. |
| KMP dirty extra-effect key outside frozen bundle; trigger changes another pending key; restart replay | Desired local business rows and dirty intent survive all authoritative effects and triggers, metadata reflects the authoritative base, and the next push uploads preserved intent exactly once. Run the trigger case in Go too to verify the parity assumption. |
| Go FK cycle/group above soft target but within hard limit; group above hard limit | The former freezes and sends the whole group; the latter fails before outbox freeze, source-ID consumption, or HTTP. |
| New unrelated row after push freeze; same-key edit after freeze | New intent survives committed replay, is uploaded by the same high-level call, and final local/remote row values agree. |
| New write after initial pull gate but before fetched bundle apply | In-transaction gate prevents overwrite and checkpoint advance for that bundle; high-level sync drains and retries safely. Include both same-key and unrelated-key writes. |
| New write during a zero-bundle pull response or after the last applied bundle | Final pending-state check causes another drain or the documented incomplete result. |
| Attempts 64 and 65, including conflict recursion, frozen replay, and KMP whole-block retry | The 64th attempt may complete; an attempted 65th push does not run. The incomplete type reports accurate counts and unique confirmed bundles. HTTP retry of one prepared request is accounted for separately. |
| No-op pruning, conflict resolution, or concurrent replacement with unchanged count | Changing durable identity is recognized as progress; truly unchanged identity returns `no_progress`. Count equality cannot stop the call. |
| Transport failure after a confirmed push, then outer retry and final no-op | Final report remains `COMMITTED`; confirmed bundle count and notifications are not lost or doubled by replay. |
| Checkpoint recovery with several batches; dirty write at final snapshot gate | Recovery-authorized pushes drain within the shared allowance. A late write returns to recovery drainage without ordinary-push fallback, source rotation, or lost intent. |
| KMP and Go pause table, including pause between batches and recovery | Each entry point follows section 2.4. Go reports `uploads_paused` or skipped remote work without pulling over dirty rows; KMP explicit work remains enabled. |
| `syncThenDetach` across three rounds and one shared allowance | Bounded incomplete maps to blocked detach with correct reason/count, optional last completed report, and no detach mutation. Equal row counts with changed identity do not imply no progress. |
| Swift caller cancellation during a later batch or before detach | The owned operation is cancelled; no later batch/detach begins, and frozen/local intent remains recoverable. Merely cancelling the awaiting task is insufficient evidence. |
| KMP active watch after contention/failure/pause with no later bundle event | The pending target is retried on a bounded wake-up and reached after the blocker clears; pause and cancellation do not spin. |
| Go paused polling downloader | Separate child test proves no busy spin and prompt cancellation/shutdown; it is not used to pass direct `Sync(ctx)` acceptance. |
| Cancellation, transport ambiguity, conflict, rejected push, source recovery, history pruning | Existing typed boundaries and durable replay/recovery are preserved; unrelated failures are not reclassified as local-work retries. |
| Several bundles followed by a later failure | Checkpoint, outbox, dirty rows, aggregate result, and table-change notifications reflect exactly the durable prefix. |

Run focused state-machine, primitive-preservation, and race tests in both clients. Exercise KMP common behavior on JVM and supported platform lanes, then affected Android/JS/Wasm/Native and live-server lanes. Exercise Go focused/package tests and a **new or isolated live PostgreSQL scenario that calls public `Sync(ctx)` once** for a backlog above one batch, with no caller drain/retry wrapper, and asserts final durable state and push/pull request trace. The existing complex-multi-batch simulator loops over `PushPending`; its pass cannot satisfy this criterion. Avoid editing its two pre-existing dirty paths without resolving overlap.

Run Swift bridge and live-server coverage for changed public mappings and owned cancellation, not merely a Kotlin cancellation unit test. Treat host-only, platform, Go, Swift, and live-server evidence as separate lanes; do not substitute one for another. Use isolated database resources and a validation ledger. Before expensive lanes, verify paired spec identity, active corpus manifest integrity, and focused one-row wire trace. The compact wire fixtures and sorted manifest should remain unchanged; any actual wire change is a separate paired-contract decision.

## 5. Work sequence and gates

1. **Contract settlement:** independently rereview this revised draft, settle the public incomplete/blocked types and shared 64-attempt policy, then record the accepted behavior in the governing Go and SQLiteNow specifications. Reconcile one-row request-count criteria without weakening them. Check Go OpenAPI and corpus for impact; record explicit no-change decisions if their bytes stay unchanged.
2. **Implementation prerequisites:** capture both repositories' tracked, untracked, and ignored baselines. Preserve the two pre-existing Go mobile-flow edits unless their owner resolves overlap. Fix KMP extra-effect settlement, Go dependency-safe freeze, and both pull apply gates before relying on high-level drainage. Do not edit Go source until Go-repository authorization is explicit.
3. **Interactive behavior and consumers:** implement invocation-wide accounting, bounded ordinary/recovery drains, pause and detach behavior, KMP watch-target retention, Swift caller-cancellation ownership, and sample presentation. Implement the Go paused polling fix as a separately validated child, without making it a substitute for direct-sync evidence.
4. **Author validation:** run focused tests first, then required package, platform, Swift, live-server, and cross-repository lanes. Include the one-call public Go `Sync(ctx)` live scenario. Record failures and rerun only invalidated lanes after fixes, plus mandatory final aggregates. An aggregate timeout is inconclusive until isolated.
5. **Independent review, owner acceptance, and delivery:** separate gates after a stable, evidenced candidate. This proposal alone authorizes no implementation, staging, commit, push, publish, release, or deployment.

Completion requires that ordinary concurrent local edits and multi-batch backlogs converge through one high-level call within the shared bound; push settlement, pull apply, and recovery never erase newer dirty intent; bounded non-convergence and paused/blocked outcomes are explicit and durable; watch catch-up remains live after a temporary blocker; Swift cancellation owns the operation; and the paired client and sample evidence meets the separate validation lanes. The Go paused polling child has its own evidence and closeout.

## Maintenance validation amendment (2026-09-24)

The owner-authorized proportionate-validation implementation adopts the tracked
[Oversqlite validation runbook](../docs-engineering/oversqlite-validation.md) for subsequent maintenance candidates.
Development runs focused regressions. Maintenance reruns affected owning packages,
supported platform scenarios and live boundaries, with manual reviewer-confirmed
reuse of unchanged evidence. The runbook defines invalidation, boundedness/race
triggers, mandatory-aggregate identification and fail-closed execution counts.

Earlier instructions in this document requiring final aggregate reruns after every
source change apply to this document's initial implementation/qualification campaign
and to release profiles requiring them. They are not unconditional requirements for
later maintenance. The full initial protocol/release matrix remains mandatory;
contract changes require their own accepted profile. Historical acceptance records,
failed attempts and source-revision evidence are unchanged. This amendment does not
retroactively qualify convergent-sync remediation or grant review, acceptance or
delivery. Specification hash changes require a new authority record; no OpenAPI,
wire fixture, corpus manifest or runtime behavior changes are made by this amendment.


## Library-owned work slices — 2026-10-13

This owner-authorized amendment supersedes public work-yield behavior in earlier sections. Ordinary push, pull, sync, rebuild, attachment/recovery, conflict resolution and local lifecycle operations complete their existing semantic unit inside the library. Push handles one logical bundle; sync drains successive bundles before pulling. The existing work-limit field and its 1,000,000 default now select an internal computation slice, not a customer-visible completion cap. No new SDK operation timeout is introduced: caller cancellation/deadlines and existing convergence guards remain authoritative. Entry/byte, wire, snapshot and dependency-group hard limits remain unchanged.

Continuation resumes the current local phase after a useful safe checkpoint and successful cleanup, under the same outer operation owner. It preserves exact frozen request, snapshot/replacement, pull ceiling, retry budgets, attempt/round allowances and accumulated uniquely confirmed effects. Slice accounting and the first-ready-atomic allowance may reset only at a safe boundary. Every positive target must allow a bounded useful step or admissible ready atomic unit; setup must not starve progress. A slice that yields without actual progress fails explicitly rather than spinning. Mixed work/cleanup failures, cancellation and actionable errors retain precedence. Scheduling yields do not escape ordinary public APIs or enter transport backoff. Historical work-error types remain available; internal bounded-step tests may observe them. No local format or HTTP change is authorized by this amendment.

The scoped qualification profile is the paired work-slice completion program, not a routine-maintenance exemption or full protocol/release acceptance. Fresh evidence requires paired corpus/boundary consumers; Go owning-client and nested-module tests with Go 1.25.12 and focused race instrumentation; KMP common/JVM comprehensive coverage; affected Android/JVM/iOS/macOS/JS Node/browser/Wasm and Swift cancellation/mapping; isolated live multi-batch, replay/restart, snapshot and cancellation; default-budget 100-user/50-concurrency complex scenario with verification; and current 1,000/10,000-row resource evidence. Add a synthetic 14-table/3,399-row witness for the historical DayTempo shape. Do not schedule retired 50,000-row replay campaigns. Existing per-command deadlines, failure diagnosis, candidate freezes and evidence invalidation rules remain.

Implementation ledger: Go `.artifacts/work-slice-completion-20261013/ledger.txt`, with immutable paired baselines/preimages and a current execution ledger. Specification and corpus changes alone do not assert runtime qualification. Delivery remains separately authorized.


### Fresh-database completion profile — 2026-10-14

The owner authorizes completion on freshly created server and client databases. Backward compatibility with older database formats is not required. A measured optimization may change the local format or indexes after its paired contract is amended; create the new format directly, without old-format migration, dual readers or compatibility shims. Current method signatures, configuration defaults and HTTP shape remain unless a demonstrated problem requires an explicit amendment. No format change is currently selected.

Reopen, process restart, replay and recovery of databases created by the new implementation remain mandatory, including frozen identity, confirmed effects, checkpoints, cancellation, resource ownership and hard limits. Old-version upgrade and table-retirement compatibility journeys are outside this scoped completion profile; preserve their recorded baseline failures and historical requirements. Their exclusion never claims the broader historical suite passed.

Before another full load run, obtain phase-separated structural evidence and three comparable smaller complete-scenario timings, then a verified 50-user/50-concurrency rehearsal completing within 10 minutes. Final acceptance remains a freshly isolated nethttp_server followed by mobile_flow --scenario=complex-multi-batch --parallel=100 --concurrency=50, default client budgets, verification enabled, 100/100 completion within the existing 20-minute runner window and zero leaked scheduling failures. Keep resource measurements and comprehensive suites isolated from heavy load. No increased work target, reduced accounting weights, caller continuation loop, blanket retry, verification weakening or runner extension qualifies.

The latest KMP decision-reservation correction invalidates dependent replay/ownership public-call evidence. Freeze after focused checks and source review, then refresh exactly the affected platform, Swift, live and resource lanes plus independently mandatory aggregates. Current 1,000/10,000-row replay workloads use name widths 16/1,024; no retired 50,000-row replay campaign is reintroduced. Deliver paired patches, quantified findings, an audited qualification ledger and explicit database-recreation guidance; acceptance and delivery remain separate.


### Bounded planning statements and cleanup — 2026-10-14

Go may opt private planning reads and scalar/null mutations into the existing fully charged writer-owned statement reservation. Reuse remains limited by its admitted handle/SQL capacity, at most 16 arguments and a 64KiB encoded argument envelope; oversized arguments or optional capacity refusal retain one-shot execution. Original planning work charges remain; added argument/lease visits are charged. Close all handles and release the pool before writer commit or continuation. A per-step savepoint protects the prior saved frontier if added lease accounting yields inside a step. Prepay six boundary work units before the savepoint; rollback/release cannot themselves yield, and their failures retain precedence. Raw kernels keep their original single-step ownership. No quota, default, wire or database-layout change.

Both clients may group at most 32 existing push-plan cleanup records in one writer transaction. Preserve fixed cleanup identity, per-record work and charge release, cancellation checks and saved state after every record. Only a pure computation refusal after useful steps may publish the saved prefix after successful cleanup. Other failures roll back the writer. Kotlin's existing step accounting refuses before mutation; Go's leased mutations retain the savepoint boundary. Required witnesses compare prepares/closes, cleanup commits/descriptor reads, complete primary state, actual progress, charged work, hard fallback and tiny-target behavior separately from ordinary latency.


### Writer-owned planning descriptor reuse — 2026-10-14

Inside one bounded planning writer, retain the same admitted mutable push descriptor and the validated NONE outbox state. Do not copy payloads or retain the descriptor across writers, slices or bundles. The existing 2-entry/256-byte scalar holder covers only the added reference and flags; the descriptor remains fully charged by the existing current push-state admission on every step. Hydration and its temporary decode admission happen once per writer. Every original planning step, guard, current-state capacity check, saved cursor and cancellation check still runs. Clear the reference when the plan owner is deleted, including an all-no-op frontier. Discard the entire holder after cleanup before publication/continuation, including a rolled-back partial step; the next writer reloads durable state. Raw kernels keep fresh reads. Required witnesses compare descriptor/outbox reads and unchanged semantic progress/accounting coverage, plus external writes, cleanup, no-ops, hard refusals and tiny targets. No work weight, quota, format or wire change.


### Go bounded baseline reads and ready atomic statements — 2026-10-14

Go may reuse the existing fully charged writer-owned statement pool for scalar baseline reads and typed payload projections. Projections with more than 16 arguments retain the original one-shot route; oversized encoded bindings also fall back. Reuse holds statements, never read results, and preserves current identity/provenance, typed values, all validation, query ordering and existing accounting. Added bounded-argument and lease visits are charged. Planning calls remain inside their existing per-step savepoints; apply preparation retains its preadmitted checkpoint and durable-prefix rules.

After a ready apply unit has prepaid its existing atomic statement credits, Go may also reuse admitted mutation statements. Original write visits remain charged, additional argument visits debit those same conservative prepaid credits, and no work refusal is introduced inside the ready unit. Bounds remain 16 arguments and a 64KiB encoded envelope. Unsupported binding types, oversized bindings or optional pool-capacity refusal use the original one-shot driver path. Close/release every retained handle before publication or continuation; preserve failure/cancellation precedence and hard workspace limits. No process-wide SQLite/runtime configuration, work weight, public default, wire or layout change is selected. Required witnesses include typed and wide projections, trigger visibility, optional fallback, cleanup failures, public tiny-target completion, fixed-fixture accounting, native prepares/closes and repeatable ordinary complete-scenario timing. Kotlin retains its own charged statement machinery; representation identity is not asserted.


### Device-load process ownership amendment — 2026-10-14

The owner authorizes one operating-system process per simulated user for device-load qualification. Each child executes the complete existing scenario, including mixed CRUD, laptop recovery, both crash hooks, replay/restart and final verification, with unchanged SDK defaults and transport policies. The parent runs at most `--concurrency` children, requires a successful child result and independent per-user final verification, and owns cancellation, termination and reaping. Child verification uses a bounded single-connection PostgreSQL pool; credentials travel through private process input rather than command arguments or reports.

Parallel mobile_flow uses process workers by default; `--parallel-mode=goroutine` retains the original shared-process SDK stress mode. Previously failed shared-process attempts remain failed stress evidence and are not relabeled as device-load passes. The library completion/cancellation/resource contract applies in both modes. Device-load qualification still requires three smaller verified runs, then 50/50 within 600 seconds and 100/50 within 1,200 seconds, against the same freshly isolated authority with unchanged budgets, verification and workload. Parent cancellation stops queued work and joins every owned child; incomplete, malformed, missing or failing child results fail the run. No global SQLite configuration or SDK throttle is introduced.


### History-projection retry timing correction (2026-10-14)

A validated HTTP 503 `history_projection_capacity_exceeded` response with a positive delta-seconds `Retry-After` must postpone the next transport attempt by at least that delay. Both clients retain their existing generic attempt limit, backoff configuration and numeric defaults; this correction neither introduces another retry budget nor restarts a public operation. Invalid, absent, overflowing or nonpositive headers retain existing backoff. Caller cancellation interrupts the wait, and terminal/malformed responses retain error precedence. Local work slices never create transport delay. Server history-projection permits, workspace limits and wire shapes remain unchanged. Qualify header propagation, unchanged request identities/attempt count, cancellation, terminal errors and the existing public-call platform/live gates.


### Writer-owned replay reservation binding reuse (2026-10-14)

Kotlin may reuse the reservation context binding only while building replay decisions inside one apply writer. During that phase, reads and reservation-ledger updates do not modify any input of `contextGuard`; actual authoritative/business mutations begin after the decision map returns. The binding must match the database, runtime state, token, unit and writer-owned statement-cache identity. Recompute it in every resumed writer, and retain all ledger validation, per-row size measurement, row work charging and admission checks. A bounded binding holder is charged as four entries and512bytes against the existing stable outer reservation base; repeated slices must not add preceding slices' temporary charge. If this optional holder cannot fit, discard it and use the existing uncached path without concealing cleanup, cancellation or integrity failures. The holder does not escape the decision-building phase.

No work weight, public configuration default, wire field, database layout, atomic-dispatch allowance or slice quantum changes. Qualification requires a structural before/after binding-reconstruction witness, unchanged per-row accounting and committed results, ordinary timings separate from reservation observation, and fresh affected cleanup/workspace/replay/native/live lanes. Go implementation is unchanged by this Kotlin optimization; paired authority/corpus consumers must refresh, while intact Go runtime/load evidence may be reused by dependency.


### Guarded replay decisions across local slices (2026-10-14)

A Kotlin push may retain its already constructed replay-decision map under the same outer reservation while private apply preparation publishes prefixes. Before reuse in each new writer, reconstruct the complete existing reservation context guard, validate the ledger and exact operation owner, and compare the guard. Registered external business writes advance the guarded dirty revision; baseline/attachment/source/outbox/configuration inputs remain in the guard. A mismatch discards decisions before reconstructing them. No decision reuse is permitted after authoritative business mutation begins. Cancellation and failures clear the holder before outer release.

All decision copies remain fully charged against the stable original base between slices. This combined holder supersedes the earlier writer-only four-entry/512-byte holder; the two charges are not additive. Charge a bounded eight-entry/1,024-byte combined binding/continuation holder; if optional retention cannot fit, use the uncached construction path. Per-record16-work-unit,32-copy-entry and64-byte-scaling charges remain unchanged when a decision is actually reconstructed. Reuse charges guard bookkeeping and retains the already admitted copies; it does not charge a traversal that no longer occurs. Planning slice quanta and all hard limits/defaults remain unchanged. Qualify stale/external-write invalidation, frozen identities, cleanup precedence, constant reservation peaks, once-only outer release and separate ordinary timing. This does not change Go runtime or the wire.

### Empty dependency graph traversal

Kotlin apply preparation may retire one pending root in one existing bounded walk step when the completed dependency index records zero edges and a current indexed absence query confirms the edge table is empty. The pending root must exist before retirement, including when a nonzero visit cursor is loaded from durable state. A contradictory retained counter or missing root is an integrity failure. Skip per-table child traversal only under this proof; nonempty graphs retain the original walk. Preserve fresh writer guards, the16-unit step charge for each actual step, pending-row/cursor publication, hard workspace and dependency limits, and the unchanged slice quantum/defaults. This removes redundant empty traversal and writer commits; it does not enlarge a slice. Qualify empty/nonempty graphs, stale guards, cancellation/reopen, charged workspace, and browser tiny-target completion under the existing deadlines. Go runtime and wire remain unchanged.

### Independent push components

When completed push discovery records zero edges and a current indexed absence query confirms that result, Kotlin public planning may assign one pristine unvisited node directly to its singleton component. Preserve DFS/index/stack counters, component identity, blocked reason, ready ordering, encoded charge and guarded writer publication. A nonpristine or partially visited node uses the original DFS path. After packing a singleton in this proven empty graph, advance directly to component selection without traversing nonexistent dependency edges. Each transition remains bounded to one node and indexed scalar queries; original per-step work weights,32-step writer quantum, low-level kernel behavior and hard limits/defaults remain unchanged. Qualify exact wire order, independent/blocked/cyclic graphs, external writes, cancellation/reopen, workspace refusal and tiny browser/Wasm completion; no wire/layout change is introduced.

# SQLiteNow Oversqlite FK-safe synchronization and automatic conflict resolution

> Current durable-recovery authority: [r0001](../../go-oversync/specs/r0001-oversqlite-durable-recovery.md) and the active `swagger/two_way_sync.yaml` supersede conflicting lifecycle/envelope, request-count, source-sequence and pending-source-retargeting statements below. The sole marker remains `v1`; every sync request now requires its exact protocol header. All terminal outcomes consume source sequence, durable local disposition precedes ack, and ack precedes the successor. Old acceptance/evidence records below retain their historical status. Existing typed codecs, request/bundle hashes and unrelated contracts remain in force. Runtime phases after C01 remain pending.

Status: **REVISION 3 STAGED-IMPLEMENTATION AMENDMENT; READY FOR INDEPENDENT SPECIFICATION REVIEW;
REVISION 2 ACCEPTED; REVISION 3 NOT YET REVIEWED OR ACCEPTED**

Created: 2026-09-25

Revision 3: 2026-09-25. Adopt Go p0001 sections 1.3 and 5: ordinary FK correctness first,
targeted and automatic resolution later, with contract settlement and review per slice.
Revision 3 refinement: 2026-09-26. Adopt Go p0001 section 1.4's simplification and replacement
requirements at each existing slice gate; behavior and qualification obligations remain unchanged.
Revision 2 subsequently passed specification review and received
[owner acceptance](../../go-oversync/.artifacts/p0001-fk-safe-sync/owner-acceptance.md).
The revision notes below retain the history of earlier review rounds; they do not describe the
current acceptance status. This amendment requires fresh review and owner agreement before runtime work.

Revision: 2026-09-25, addressing the five findings listed in Go p0001 section 4.1 against manifest
`94bd5f7a7fbaf0e3a18ac9a399b48679375462dbd0a651cf2f5b54d35fd6055b`.
Addressed in draft does not mean independently verified or accepted.

Revision 2: 2026-09-25, addressing the remaining application-validation dependency blocker against
manifest `b3a38f47afde03a7524f4d0b705c5e7166aa056ed259290c4153f2e62d9e09f9`.
See Go p0001 sections 3.3.1 and 4.2. Four initial findings were resolved at specification level;
the new closed fallback-eligibility boundary remains pending independent rereview.

Repository: `/Users/pochkin/Projects/my/sqlitenow-kmp`

Normative proposed authority:
[Go p0001](../../go-oversync/specs/p0001-oversqlite-fk-safe-sync-and-automatic-resolution.md).
Historical accepted revision 2 identities:
[paired baseline manifest](../../go-oversync/specs/p0001-oversqlite-fk-safe-sync-baseline.json).
Revision 3 identity:
[candidate manifest](../../go-oversync/.artifacts/p0001-fk-safe-sync/revision-3-manifest.json).
Fresh review request:
[revision 3 read-only review](../../go-oversync/.artifacts/p0001-fk-safe-sync/revision-3-review.md).

## 1. Authority and gate

This companion adopts the complete proposed p0001 requirements, regression IDs and qualification
profile for common Kotlin, platform drivers/transports, Swift consumers, tests, fixtures and docs.
It does not independently redefine wire encoding, request identity, cursor shape, rejection
precedence, or server behavior. Go p0001 is the proposed authority for the expressly amended FK and
resolution subjects; retained p0000/convergent-sync behavior remains as described in p0001 section 1.
Resolve any additional contradiction in the paired specifications before implementing its consumer.

The owner authorized revision 2 implementation in both repositories, then requested replanning after
rollback. That authorization remains recorded; this turn amends specifications only. The revision 3
manifest identifies the proposed amended bytes. The accepted revision 2 manifest records an earlier
pre-checkpoint state and must remain unchanged; its HEADs are not today's expected HEADs. Review and
agree the amended approach before using the existing implementation authorization.

Keep `v1`, existing hash domains, coordinated breaking cutover, no compatibility aliases and
fail-closed incompatible prerelease persistence. Do not migrate/reset old sync state automatically
or alter application data on format rejection. Preserve restart/replay for the new supported layout.

### 1.1 Staged implementation

Follow Go p0001 section 5.3's A1 -> A2 -> A3 -> B1a -> B1b -> B1c -> B1d -> B2 -> B3
dependencies and section 5.4's review stops. Its stable checklist IDs are the implementation checklist
for both repositories; record Kotlin/Swift evidence against those IDs in the shared ledger rather
than maintaining a second independent checklist. Go-only proof cannot close a Kotlin obligation.
Stage A retains the current deferred default and explicit resolver APIs. First prove survivor-safe
pull and settlement/replay, then indexed baseline capture and directed push planning, then close
bounded preservation and affected lifecycle paths. Capture complete B with the new baseline layout;
do not postpone it until merging needs historical values.

Stage A adds no reconcile endpoint, automatic group policy, ClientWins rename or new report API.
Go p0001 section 1.3 defines the explicit `fk_reconciliation_required` pending result when a current
path needs later dependency evidence/repair. It preserves local intent and checkpoint; it cannot
excuse missing baseline maintenance or count as completed reconciliation. Settle its public shape
before consumers. Keep the supported FK topologies and safe application rules on reached paths.

Section 2 below describes the final program. Apply its survivor, replay, workspace and cancellation
rules as each path is introduced, with Go section 1.3's explicit A3 ownership of the existing FK-06
deficit and aggregate Stage A closeout. Stage B adds coherent targeted resolution together with historical
repair and revision guards; report-requiring decisions need reservations and journal lifecycle from
their first use. Then enable ServerWins and ClientPreferred in their assigned slices. Keep later
requirements pending; no milestone removes them or authorizes an intermediate deployment.

| Phase | Kotlin/Swift ownership and exit evidence |
| --- | --- |
| A1 | Pull and settlement/replay survivors; focused common/JVM state assertions and paired live public calls. |
| A2 | Complete B and relationship lifecycle, bounded directed planning; FK-02/FK-03 and one-call live drain. |
| A3 | Paged preservation, aggregate bounds, affected snapshot/recovery/conflict paths and safe pending results; Stage A platform/live/Swift qualification. |
| B1a | Consume settled session fixtures and fail-closed corpus identity; Go server session proof does not implement the Kotlin consumer. |
| B1b | Durable guarded capture and bounded session download; restart/budgets plus Swift root-upload/result-download cancellation. |
| B1c | Durable report reservations, paging/ack and detached account access; capacity and lifecycle component proof. |
| B1d | Integrate explicit resolution, guards, historical repair and report/replay; paired live FK-04 and Swift repair/network-resolution cancellation. |
| B2 | Exact B/L/S and E/D rules, ServerWins default and explicit merge/defer; public outcome/report parity. |
| B3 | ClientPreferred rename, closed eligibility/fallback and remaining assertions; complete final qualification and implementation-review handoff. |

B1a–B1c close components only. B1d is the first complete targeted-resolution slice and must integrate
their safety obligations before installing business state or sending a resolution candidate. Follow
the phase-assigned C01..C08 contract portions before consumers; later portions remain pending. At
each stop present the paired diff, evidence and outstanding work, then wait for owner agreement.

### 1.2 Simplification obligations

A2-03 owner clarification (2026-09-27): adopt Go p0001's distinct durable attachment incarnation
epoch and the tracked engineering/logical-format lifecycle. Kotlin's in-memory opening epoch
remains a separate publication guard. No HTTP, hash-domain or historical B semantics change.

Apply Go p0001 section 1.4's S01..S05 to Kotlin/Swift at each existing phase entry and exit. Record
the current/intended call flow, retained/replaced/removed mechanisms, and the purpose and owner of
new abstractions or persistent state. Change existing owning facilities and remove superseded paths;
justify code still serving an unreplaced path with its callers and later slice owner. Do not add
compatibility readers, migrations or a parallel synchronization coordinator. The owner explicitly
recreates incompatible databases; preserve rejection before mutation and new-layout restart/replay.

For A1, extend existing apply/transaction facilities and consolidate overlapping survivor handling
while preserving operation-specific guards. For A2, replace dependency-selection heuristics with
the authoritative planner. For A3, replace whole-backlog host materialization with paged preservation
and complete the affected shared application paths. Retain captures and checks still required by
the behavioral contract. At each stop show removed mechanisms, state ownership and justified growth,
with production/test changes reported separately. A passing suite or a lower line count alone does
not establish simplification. These requirements add no cleanup phase and waive no safety obligation.

## 2. Kotlin and Swift obligations

- Implement indexed server-visible FK baselines distinct from latest local values, with known-empty
  versus unknown state. Maintain them through attach, publication, settlement, replay, requeue,
  conflicts, pull, snapshots and recovery. Unknown requires reconciliation, not table-wide grouping.
- Retain complete typed base B independently of desired L and observed S. Missing historical B
  cannot be reconstructed from current S: fail with `resolution_base_unavailable` and preserve intent.
- Select directed record prerequisites beyond the initial page; keep SCCs indivisible and independent
  components separate. Respect hard rows/decoded bytes before freezing. Use durable paged graph
  storage and expose oversized component details through bounded summaries/pages.
- Share the final-state application mechanism across pull, settlement/replay, resolution and snapshot
  installation. Validate wire order/hashes first; protect clean and dirty survivors, including
  version-skipped rows. Verify payload survival as well as FK integrity before atomic metadata and
  checkpoint transitions. Preserve original history; do not disable FK enforcement.
- Capture desired intent and membership revisions before reconciliation. At the reserved writer gate,
  detect changed records and new dependents, preserve newer captures and replan. Never hold a SQLite
  transaction over HTTP or user callbacks. Retain cancellation and operation ownership through Swift.
- Default to `ServerWins`; opt in to `ClientPreferred`. Use the exact fallback exclusions and partial
  commit semantics in p0001. Preserve explicit merge/defer as advanced options. Remove `ClientWins`
  aliases. Document network work in explicit resolution and update facade/sample callers.
- Require core-owned pre-application FK evidence with complete consulted row/absence/membership
  coverage for automatic rejection-based fallback. Public application categories, SQLSTATEs,
  trigger-raised constraint errors and validators reading non-FK/predicate/external state cannot
  establish coverage. No application opt-in or dependency-capture callback is introduced here.
  Preserve normal application write/normalization behavior: early rejecting validation is permitted
  only under pinned readiness evidence with no application-controlled write/validation mechanisms.
- On a classified untracked validation rejection, return `automatic_fallback_ineligible` with
  detail `application_validation_dependencies_untracked`, safe category and a revision-bound handle;
  preserve B/L/S, intent and committed prefix, with no automatic discard or completed resolution.
  Offer explicit retry or whole-disposition-set `AcceptServer`, which selects S directly rather
  than re-running the default field merge. Bind either action to current evidence/revisions and
  normal reservation/replay rules; missing/malformed coverage never grants fallback permission.
- Implement the exact column/existence merge table and evidence/disposition membership sets in
  p0001 section 3.2.1. For server SET NULL plus a local name edit, adopt NULL and retain the name as
  dirty intent based on S. Competing reparent edits choose S's FK; preserve other noncompeting edits.
  Fetching an evidence-only parent never authorizes discarding its unrelated dirty values.
- Persist the resolution phases and paged outcome journal, with atomic record dispositions and
  explicit acknowledgment. Callback contexts and reports must not materialize unlimited closures.
- Add the application-data-read-only session consumer: bounded idempotent root upload, seal/build,
  immutable result pages and close/expiry. Discover the complete selected server closure in its
  consistent build; unrelated later writes never invalidate pages. Guard candidate pushes with the
  dependency read set, not whole-scope freshness. Replay committed requests before checking expiry.
- Persist observation floors and implement bounded historical dependency repair (p0001 section
  2.3.1). An unseen X->P in old bundle 11 after targeted P deletion at 12 requires explicit newer
  X evidence before supersession; process each original validated bundle and its checkpoint in
  order. No full-scope snapshot, blanket skip, invented tombstone, or unguarded apply over local work.
- Enforce p0001 section 2.5 aggregate workspace defaults: 131,072 entries, 256 MiB encoded copies
  and a soft target of 1,000,000 work units per invocation. Charge nodes/edges, survivor copies, sessions, revision
  metadata and floors together; stop/resume/clean up as specified. Cancellation checks occur at
  each page and at most every 256 work units; capacity is not permission to discard intent.
- Enforce report defaults of 10,000 entries and 16 MiB across retained plus reserved journal state.
  Reserve before report-requiring candidate pushes/local changes. Allow sealed-prefix acknowledgment
  while resolution remains incomplete. Preserve reports/reservations through detach/source changes
  with explicit account-scoped detached access. Finish previously reserved replay when otherwise full.
- Preserve 64 push attempts and share one 64-exchange reconciliation allowance across retry wrappers,
  recovery and all `syncThenDetach` rounds. Exhaustion retains resumable work, reports incomplete
  progress and prevents detach. Do not reset either allowance in nested helpers.
- Preserve dirty/outbox pull gates, background-only Kotlin pause semantics, recovery-authorized
  drainage, aggregate committed progress and cancellation. Default conflict policy does not permit
  ordinary authoritative settlement to discard unrelated pending writes without a policy decision.

No unsupported topology is newly admitted. Do not remove existing support for self-references,
cycles or FK actions as an implementation shortcut. Match Go outcomes at the public call boundary,
not only private helpers.

## 3. Validation mapping

All p0001 section 4 rows apply to the completed program; track assertion-level ownership across the
stages rather than marking a whole family passed from one focused case. FK-01 through FK-06 require
Kotlin tests even where a reviewed failure was demonstrated only in Go: equivalent outcomes and
resource bounds must be proved, not
inferred from similar code. Keep the recent dirty-capture, cyclic-delete and detach reservation
tests; they do not replace the expanded lifecycle matrix.

| Requirement family | Kotlin/paired evidence |
| --- | --- |
| FK-01..03, LC-01..02 | Common apply/selection/admission tests; supported driver/platform lanes; live FK topology. |
| FK-04..05, ER-01, API-01 | Stable session fixtures, exact B/L/S field/existence outcomes, evidence-only members, dependency-guarded rejection/fallback, journal capacity/reservations/ack across detach/source changes. |
| ER-02 | Application validator reads non-FK U or a predicate; U unlocks after rejection but C stays pending. Require explicit retry or AcceptServer, no category-based eligibility, and exact baseline/dirty/journal state. |
| FK-06, BD-01 | Combined entry/byte/work accounting, cancellation/resume and intrinsic capacity errors; wide/deep graphs and survivor copies; representative memory and heavy platform/live evidence. |
| RP-01, PF-01 | Phase crash/reopen, exact ambiguous replay, format rejection before mutation and report restart tests. |
| CW-01..02 | Local/second-connection races; unrelated writes between every session action across invocations; relevant membership changes invalidate candidates; expired-session replay and Swift cancellation during reconciliation/explicit resolution. |
| RC-01 | Historical unseen-child repair with per-identity evidence and sequential checkpoints; snapshot/recovery first discovered in pull; one-call multi-batch drain and shared detach allowance. |

Use p0001 section 5.2.1 for Stage A's scoped candidate and section 5.2 plus the existing p0000
companion section 11 for full qualification. Slice development uses focused common/JVM cases and
narrow paired live evidence first. Preserve affected platform, heavy/resource and Swift obligations;
a host pass cannot replace them. Full final aggregates run on the complete frozen program, not at
each development stop. New persistence/safety contracts cannot be classified as routine maintenance.
Follow Go p0001 section 5.2.2: justify broad runs by checkpoint/dependency/failure, batch compatible
focused selectors, reuse unchanged build outputs while freshly executing affected tests, and isolate
failures before expanding reruns. Record outcomes and preparation/execution/cleanup time in the
existing ledger. These scheduling rules preserve the mandatory final commands and their rerun flags.
Run paired contract checks for the explicit core-FK eligibility discriminator and negative application/error-origin
fixtures. Include p0001 section 4.2's merge-failure, workspace-floor pressure, interrupted repair and
full-journal replay assertions. All are future runtime obligations, not executed evidence. Run contract
checks before database-heavy suites. Keep common/JVM, each supported platform, Swift and live-server
evidence distinct. Record exact server/database identity, selectors, nonzero test counts, deadlines
and raw logs in the qualification ledger. No tests/builds run for this specification package.

## 4. Contract impact and handoff

OpenAPI and shared corpus remain unchanged during this amendment. Go p0001 section 5.1 requires
settling the active slice's changed persistence, public and wire surfaces before their consumers.
Record no impact for unchanged surfaces and defer later-only schemas/fixtures until their slice.
Freeze relevant request/error schemas, public types, classifications, sorted manifest and paired pins.
Corpus consumers must fail closed on missing/mismatched configured roots and execute every claimed family.

Do not change the existing source, test, generated output, instruction files, or unrelated worktree
state as part of this specification task. Capture the new complete paired identity after authoring;
old frozen review hashes no longer identify the amended authority package. Preserve the historical
acceptance and manifest; record original and resulting tracked/untracked/ignored-authority identities
in the revision 3 manifest and use its new review request.

Stop at **SPECIFICATION READY FOR REVIEW**. Independent specification review, owner acceptance,
runtime authorization, author qualification, independent implementation review, acceptance and
delivery remain separate gates. The next reviewer examines specification sufficiency and risks;
they do not qualify runtime code or grant delivery approval.


## A1 local contract settlement — 2026-09-26


This elaborates revision 3 without changing its requirements. No HTTP field, v1 encoding, canonical
wire ordering or hash domain changes. The frozen review identity is retained as historical evidence.

Public configuration: Go `MaxSyncWorkspaceEntries`, `MaxSyncWorkspaceBytes`,
`MaxSyncWorkUnitsPerInvocation`; Kotlin/Swift `maxSyncWorkspaceEntries`, `maxSyncWorkspaceBytes`,
`maxSyncWorkUnitsPerInvocation`. Current defaults are 131072, 268435456, 1000000. Values must be positive.
Go's zero-valued omitted options use these defaults, consistent with its existing configuration.

Public failures: Go `SyncWorkLimitError` / Kotlin `SyncWorkLimitException` have `unit`, `limit`, `used`
and reason `sync_work_limit`; progress is resumable, not a transport retry. Go
`SyncWorkspaceLimitError` / Kotlin `SyncWorkspaceLimitException` additionally have `dimension`,
`reserved` and `resumable`, with reason `sync_workspace_limit`. Dimensions are `entries`, `bytes`
only. The former `atomic_apply_work` dimension is historical and is not emitted by the current contract. `unit` is a bounded hash of the validated bundle identity and operation.
No source sequence or checkpoint is consumed by either incomplete result.

Persistence: `_sync_apply_plan` owns one validated-input/revision guard, root/closure cursors and
charged totals. `_sync_apply_rows` stores deduplicated intended rows plus an ordinal frontier.
The existing dirty revision, generation/source identity and ordinary checkpoint guard saved progress.
Resume only an exact match under the SQLite writer lock; page obsolete-row cleanup before reuse.
Pull refetches and validates the original complete bundle; settlement retains exact frozen replay.
Staged business copies are derived; existing business/dirty/outbox records remain authoritative.
New metadata belongs to the existing complete-layout census; there is no migration or compatibility reader.

Accounting: count every stored row and the plan descriptor, plus transient encoded copies before
loading them. Reserve encoded key/payload lengths and metadata bounds, including the saved cursor.
Use a bounded indexed primary-key step for discovery, charging visited nonmatching rows as well as
matches and duplicate lookups. No recursive SQL or hidden complete-table scan is one nominal unit.
Save progress at work exhaustion; stale-plan cleanup is also paged and charged. Reserve a bounded
final apply/restore/verification allowance before business writes; distinguish insufficient remaining
invocation allowance from an intrinsically oversized atomic apply. Cancellation rolls back the current
transaction; previously committed staging and earlier completed bundles remain usable.

The shared `local-apply/a1.json` family records defaults, local results and the FK-01 witness. Both
Go and JVM consumers must execute it and require the complete manifest. This is local state contract
coverage, not a new wire endpoint. Exact hashes are frozen in the A1 ledger before runtime consumers.
Final A1 review must include restart, stale-revision, limits, local-layout rejection and request counts.


## A1 first review remediation settlement — 2026-09-26

This replaces the earlier A1 discovery implementation detail; revision 3 behavior and limits remain
unchanged. Saved positional cursors bind the exact registered-table sequence and admitted FK traversal
shape. Reordered registration invalidates the old plan under the writer lock and uses charged cleanup
before restart. Every staged identity expands its current incoming descendants, including UPDATE,
locally preserved and version-skipped roots; expansion is not conditional on a live DELETE edge.

`_sync_apply_edges` is a derived, per-apply-unit incoming lookup containing only parent/child table
and key identities, an ordinal and encoded charge. Build it with bounded keyset visits once over the
registered FK tables for the guarded current view, persisting progress in the existing plan. Each
relationship is visited once during the build, independently of the number of affected parents;
subsequent closure visits use its parent/child index. No per-parent full child-table pass remains.
This is temporary charged workspace, not authoritative B or a persistent relationship authority.
It may contain unrelated current relationships; their storage and build work are fully charged.

The plan adds `edge_entries` and `edge_bytes`, sharing the existing aggregate entry/byte allowance
with row staging and simultaneous transient copies. Phase `index` lies between `roots` and `walk`.
Obsolete edges and rows are removed by bounded charged steps; final edge cleanup is preflighted and
commits atomically with apply. The additional table/columns belong to complete-layout admission;
recreate old development databases rather than migrate them. No HTTP or public option changes.

Pull seeds reserve the incoming payload's encoded size before allocating a string copy. Go uses
the received byte length; Kotlin measures JSON encoding without constructing its output, including
escaping, Unicode and nested JSON values, checking cancellation and temporary recursion capacity.
Only admitted payloads are serialized for the survivor stage.


## A2-01 contract settlement — 2026-09-26

Status: **AUTHOR CONTRACT CANDIDATE; independent C08 review and owner decision pending.**
Adopt [Oversqlite authoritative state and dependency planning](../docs-engineering/oversqlite-dependency-planning.md) as the complete paired A2 contract, including all public shape fields,
state discriminators, full typed B, indexed relationship views, directed prerequisite/SCC rules,
aggregate bounds, diagnostic paging, failure effects and new-layout restart/rejection requirements.
The complete concrete Go/Kotlin lifecycle and replacement map is
[dependency planning](../docs-engineering/oversqlite-dependency-planning.md); its future assertions remain unexecuted.

Kotlin extends OversqliteSyncStateStore, existing capture triggers/outbox ownership and
OversqliteStageStore/OversqlitePushWorkflow. It does not add a synchronization coordinator or retain
dependencyClosedPushSelection as fallback. Replace the polymorphic dirty pair reader and separate
PushDependencyGroupRowsExceedsAdmittedLimitException in the future implementation; keep complete B
in both dirty and frozen outbox intent. OversqlitePushReplay's PreserveLocal branch and the optimized
SnapshotRowStatePlan path must maintain A/full B/server edges, not just row version/deleted flags.
A1's current-business `_sync_apply_edges` remains derived survivor workspace with different semantics.

Matching Kotlin public shapes are PushDependencyGroupExceedsAdmittedLimitException,
FKReconciliationRequiredException, ResolutionBaseUnavailableException, SyncProgress,
PushDependencyDiagnosticPage and StalePushDependencyDiagnosticException, plus the existing work/
workspace exceptions with progress. Exact names, fields, dimensions, sample/page bounds and local
read/release method behavior are fixed by the engineering document's Bounds, restart and local public results section; expose equivalent Swift error fields through
the existing facade, not a separate traversal API implementation. No runtime/public-type/DDL edits
are part of this author candidate. C04's pending-result/base-error prerequisite portions are settled
now; A3 aggregate closeout and B2 merge behavior remain assigned later.

The classified local-planning/dependency-planning.json corpus family has paired pure-contract consumers; runtime
lifecycle/planning witnesses explicitly remain A2-02/A2-04/A2-05 obligations. C07 fresh pre-consumer
host measurements and the exact freeze are in the paired A2-01 ledger. OpenAPI, v1/hash domains and
wire families are unchanged. Earlier specification-only restrictions/status text above describes
historical authoring gates; the current scope is the owner's A2-01 request. Stop before A2-02,
independent review, owner acceptance and every delivery gate.

Durable fixture owners baseline-lifecycle, dependency-planning and public-sync-live map to A2-02,
A2-04 and A2-05 here; phase IDs are not durable test names. The Go settlement retains the complete
phase/requirement mapping. The naming amendment preserves all earlier accepted text and historical
evidence, and requires a fresh frozen candidate before review.

## Rekey ordering review amendment — 2026-09-26

Status: **proposed contract correction; independent rereview and owner acceptance pending.**
The A2-01 review found that the extra old-delete -> new-insert prerequisite lacked durable capture
state. This amendment explicitly removes that synthetic prerequisite; it does not add pairing state
or infer pairs from ordinals, payload similarity, transaction adjacency or update history.

Clarify the Go specification's section 2.2 key-change/dependent-operation requirement: each identity retains its own
operation, complete historical B and desired state. Actual authoritative/desired FK relationships
supply prerequisites. A local key update atomically captures delete-old and upsert-new, but does not
promise an atomic remote rename or unconditional insertion-before-deletion order. With no FK
prerequisite and max_push_rows=1, earlier delete-old may commit first. If the new insertion then
fails, preserve its intent and report the committed prefix; do not recreate the old identity or
claim rollback. This is an explicit weakening of the unaccepted author's extra ordering guarantee,
not merely an implementation detail or a claim that the reviewed wording was satisfiable.

The durable engineering contract now defines independent coalescing, chained rekeys, restart,
freeze/requeue, pruning and cleanup under existing per-identity owners. No new persistent object,
field, FK projection kind, wire payload or runtime consumer is authorized. Runtime lifecycle and
planner validation must include no-dependent rekeys, unrelated inserts with identical persisted
facts, chained rekeys, restart and partial remote failure. The existing child-reparent case remains.

The shared corpus adds six bounded pure relationship/order witnesses and five pending runtime
scenarios. There are now 33 explicitly unexecuted runtime scenarios, superseding the prior count
of 28. New assertions check only declared persisted facts and one-row acyclic scheduling; they do
not implement or qualify SQLite capture, crash recovery, SCCs or live behavior. C04/A2-01 is corrected
at author-candidate level only; C08 requires rereview. Preserve the original C07 evidence, Stage A
platform/live/Swift obligations and all later gates. Stop before A2-02 runtime implementation.

## Contract documentation separation — 2026-09-26

Status: **author amendment; independent rereview and owner acceptance pending.**
This is a documentation-only separation with no change to contract behavior or validation obligations.
The durable `docs-engineering/oversqlite-dependency-planning.md` reference retains the state model,
fixed ownership, ordering, rekey semantics, bounds, public results and complete lifecycle invariants.
Its descriptive name and content remain useful after this specification's delivery work is complete.

The [temporary implementation and validation plan](../../go-oversync/.artifacts/p0001-fk-safe-sync/contract-document-separation/implementation-plan.md) now owns current-versus-target call flows,
concrete replacement/removal instructions, remaining integration work, future test assignments,
command budgets and author review attention. Earlier references to those planning sections in the
engineering document now resolve to this plan. The lifecycle matrix itself remains in the engineering
reference; its contract witnesses remain explicitly distinct from executed runtime evidence.

No runtime/source/fixture requirements changed. Reuse unchanged focused contract evidence; refresh
corpus manifest integrity and frozen review identity. All 33 runtime scenarios and later platform,
live-server and Swift qualification remain pending. The rekey ordering amendment remains proposed,
including its owner-facing partial-progress tradeoff. This separation is not review, acceptance or
authorization to begin A2-02. Historical specifications and review records are preserved.

## Accepted baseline contract and lifecycle implementation — 2026-09-26

The owner accepted the independently reviewed contract candidate, including the rekey tradeoff,
and explicitly authorized A2-02 in both repositories with dependency-planner implementation excluded.
The accepted candidate SHA-256 is
`10b6078853de25aa4502fec354bca8cb9251ad63593a351115088b27130ef6e5`.
This acceptance supersedes the pending-acceptance wording in the historical amendments above;
those frozen artifacts remain historical evidence, not current worktree baselines.

The current author slice installs full authoritative A, per-intent historical B, server/desired
relationship projections and checked revisions under the existing owners. It covers capture,
freeze/requeue, reached conflict resolution, settlement, pull, snapshot/recovery, retained-format
rejection and cleanup. The old pair-valued capture readers and version-only baseline writers are
removed. Existing push selection is adapted only to read explicit fields; directed traversal,
SCCs, planner workspaces/bounds/diagnostics and the public live multi-batch proof remain A2-03..05.
The three planner tables in the settled logical inventory are intentionally not installed in this
baseline-only slice. This is not a complete planner format or qualification claim.

Runtime implementation remains an author candidate requiring independent review and owner
acceptance. Its focused validation ledger maps fresh and reused evidence without promoting the
33 shared contract scenario declarations to executed runtime traces. Package, platform, live-server,
Swift and later Stage A obligations remain pending. No A3 or Stage B work is authorized by this record.

Author evidence: [baseline lifecycle ledger](../../go-oversync/.artifacts/p0001-fk-safe-sync/baseline-lifecycle/implementation-ledger.md).


## A3-01 aggregate workspace and ordinary pending contract — 2026-09-28

Status: **AUTHOR CONTRACT READY FOR INDEPENDENT REVIEW; no A3 runtime authorization or qualification.**
Entry is the owner-accepted, independently reviewed A2-05 checkpoint: Go
`047a5c3f7c5b02c82c690ef0cd39dca2877bd888`, Kotlin
`964f42f30241533e15f08aba6120afb7cd24b06c`, both `ft-migration-support` and initially clean.
The read-only checkpoint verifier passed against manifest
`8ed729df0ff559d861f60a9536f79cf071cce343448b4be064de734428efbf49` before documentation edits.
These entry identities remain historical after the authorized authority amendment; no frozen packet
or acceptance record is rewritten. The [A3 entry packet](../../go-oversync/.artifacts/p0001-fk-safe-sync/a3-01-contract/README.md) owns the new identity overlay,
call-flow/replacement plan, assertion ledger, future selectors/budgets and contract-review request.

Adopt the [aggregate ordinary-operation workspace and safe pending contract](../docs-engineering/oversqlite-dependency-planning.md#aggregate-ordinary-operation-workspace-and-safe-pending-outcomes).
It settles C04's A3 portion: one database storage budget and outer-invocation work meter; all temporary
Stage A copies, including committed-effect/snapshot stages and direct conflict/pull buffers; bounded
paged preservation without a second full backlog copy; charged necessary backlog verification;
matching Go/Kotlin/Swift pending results and error precedence; safe restart, cleanup, cancellation,
complete local intent/historical B, clean survivors, exact replay and atomic checkpoints.

C05/C06 are a concrete future fixture/consumer activation plan in this packet. The active corpus,
logical-format declarations, sorted manifest, consumer pins and all 33 `executed:false` runtime
scenario declarations remain unchanged in A3-01. Activate the reviewed local contract fixtures,
classifications, logical field/index inventory and paired pins together before runtime consumers in
A3-02/03. C01–C03 and OpenAPI have no A3 impact; no HTTP fields or hash domains change. C07 retains
ordinary request-count requirements and assigns fresh affected-path proof; no runtime suite ran here.
C08 remains pending independent review and owner decision. No checklist item is promoted to an
independent PASS by this author preparation.

The packet assigns FK-06, LC-01/02, BD-01, RP-01, CW-01, RC-01 and PF-01 assertions to bounded
A3-02/03 implementation, A3-04 focused evidence and A3-05 qualification after candidate freeze.
Use p0001 sections 5.2.1–5.2.2, not a maintenance exemption or the later full-program aggregate.
Node remains transient. Historical repair, sessions, automatic policies, report APIs, Stage B and
full-program qualification remain later obligations. Stop for independent contract review.


## A3 supported ownership clarification — 2026-09-28

Status: **AUTHOR AMENDMENT READY FOR INDEPENDENT CONTRACT REVIEW.** The owner agreed to prepare
this bounded clarification in the receiving A3-02 task. Independent amendment review and owner
acceptance of the frozen candidate remain pending; runtime implementation remains stopped.

Adopt the engineering contract's [Supported database identity and reservation lifetime](../docs-engineering/oversqlite-dependency-planning.md#supported-database-identity-and-reservation-lifetime)
section. A3 retains SE03's existing same-runtime lifecycle boundary: hard-link aliases, mixed-runtime
ownership and hostile-filesystem behavior remain outside the completion contract. Within the
supported database identity, all admitted handles and retained/reserved derived state share one
aggregate storage budget; it is not a separate allowance per Client. Preserve stronger existing Go
physical-file exclusion behavior and the persistent browser worker's lifetime Web Lock. Node stays
transient.

Use short writer transactions for reservation admission, transfer and release in existing owners.
Reclaim an orphan transient reservation only after reacquiring the existing exclusive lifetime
lease proves that the preceding owner ended; retain the lease through cancellation/rollback/cleanup.
Internal clients sharing a live coordinated opening do not establish fresh abandonment proof.
Retained stage/input charges, complete A/B/L and historical bases, clean survivors, frozen replay
identity and checkpoints remain protected. No TTL reclamation, transaction across HTTP/callback,
new persistence owner, ownership protocol or Stage B work is introduced.

The [amendment packet](../../go-oversync/.artifacts/p0001-fk-safe-sync-a3-02-ownership-amendment/README.md) preserves A3-01 acceptance and the A3-02 preflight stop as
historical evidence and records this new authority overlay, paired diff and review identity.
Before A3-02 production changes, settle the exact physical inventory and activate matching local
fixtures/consumers/pins. Focused second-handle, supported-alias, live-buffer cancellation and physical
restart/reacquisition witnesses remain required. No runtime suite or fixture activation occurred
while preparing this amendment; all 33 original runtime declarations remain `executed:false`.
Stop for independent contract review and owner acceptance of this candidate before resuming A3-02.


## Server-write reliability amendment (2026-10-22)

The owner authorizes a paired correction for final-state capture, uniqueness-safe local application, and reserved-writer concurrency. Wire shape, canonical first-touch ordering, operation identities and receipt guarantees remain unchanged. Deployments use fresh server and client databases; no migration or historical repair is provided.

Raw PostgreSQL capture is a touched-identity set and forbidden-effect witness, not authority for the final after-image. After deferred effects finish, finalization reads actual final rows in the same transaction and uses prior authoritative row_state to classify insert/update/delete or absent-to-absent. Preliminary effect detection and finalization use identical rules and existing exact physical/wire codecs and limits.

Both clients durably stage desired full local images and metadata decisions, then schedule plain-column uniqueness dependencies using SQLite affinity/collation/NULL semantics. Acyclic transfers release a value before claiming it. Cycles stage and preflight their complete dependent closure before temporary vacating and restoration; application-owned triggers or unknown dependencies that make temporary deletion unsafe block the entire apply. Unsupported partial/expression uniqueness and conflicting final desired values fail explicitly. No REPLACE, disabled constraints, invented business values, unbounded host graph, or canonical history rewrite is permitted. Preparation may yield with guarded staging only; complete business application and metadata/checkpoints commit atomically after final-state, protected-intent and FK verification. All extra persistent/host/statement/graph work is charged under the existing limits; no new numeric defaults. The shared apply owner covers pull, settlement/replay, conflict and snapshots.

Go exposes FinalStateApplyBlockedError and Kotlin FinalStateApplyBlockedException with bounded reasons conflicting_desired_state, unsafe_trigger_or_dependency and unsupported_uniqueness. They are local failures; they do not auto-resolve conflicts or advance synchronization state. Matching fixtures cover values/existence/versions, local values, dirty/outbox protection, failure/cancellation/reopen, closure and capacity limits, and runtime parity.

Established reserved writers hold compatible FOR KEY SHARE reservation locks; conditional false-to-true first-use publication remains in the business/history/receipt transaction. Concurrent first use may wait; established different scopes must overlap. Removal, permanent use tombstones, missing reservations and rollback remain fail-closed.

The correction qualification profile requires paired fast-contract checks, Go owning and focused race/live suites, Kotlin common metadata plus affected JVM/platform/live and Swift cancellation leaves, and measured ordinary-path SQL/workspace behavior. Follow the paired validation runbook and retain failed attempts; no missing/skipped/cached lane is a fresh pass. Delivery and actual database recreation remain separate gates.

Blocker recovery contract: errors identify operation, exact table/index or application trigger, and bounded exact affected keys plus truncation status, without payloads. Supported valid transfers/cycles must execute automatically; an unexpected blocker remains an SDK defect. Application corrections use ordinary edit/conflict or coordinated schema-upgrade APIs. After a local schema/trigger or pending-intent correction, ordinary Sync invalidates only derived staging through the current guard and retries. Later server writes do not change a retained invalid bundle: correct the authoritative final state, then use the supported Rebuild/rebuild snapshot API (preserving pending intent) to recover from that history. Rebuild retains its dirty/outbox/conflict admission checks; a frozen pending operation is never discarded to force recovery, and may require schema correction or supported conflict resolution first. Never require direct sync-metadata edits, manual checkpoint skipping, or database deletion. Repeated unchanged attempts must preserve the same pending intent.

## Prospective validation mapping amendment — 2026-10-26

For candidates explicitly bound to the approved [full-profile manifest](../tools/qualification/profiles/oversqlite-full.json), section3 validation follows [r0001 validation section9](../../go-oversync/specs/r0001-oversqlite-durable-recovery-validation.md). Normal entries are at most300s; source-proven overlaps and the1/5/10/25-user load ladder replace the named repetition/envelopes and former50/100 load maximum, with zero current50/100 credit; all direct/coordinated/generated and supported platform/live/Swift/resource paths, scale/duration/full typed vectors and source deadlines remain except the named approved framework policies. No deferred Stage B surface, wire/default/runtime change or automatic reuse is authorized. Historical candidate approvals are not transferable execution evidence.

### Approved E42 repair amendment — 2026-10-27

The owner approved the [combined repair amendment](../../go-oversync/.artifacts/qualification-efficiency-e42-20261026T110553+0200/approved-repair-20261027T0338Z/owner-approval-and-plan.json). The current version2
profile binds its [successor review packet](../../go-oversync/.artifacts/qualification-efficiency-e42-20261026T110553+0200/approved-repair-20261027T0338Z/reviewed-profile/proposal.md):508 declarations,
499 normal entries and9 orchestration controllers. The original80-method local
Node entry is partitioned into32/48 whole-class groups, retaining all methods and
300-second wall /270-second work /30-second cleanup caps. Other entry membership,
source workload sizes, body deadlines and the25-user maximum remain unchanged.

The three identified stale fixture files use causal cascade-observation phases
and exclusive-lifetime migration ownership. The worker client immediately owns
its internal readiness rejection while preserving caller-visible errors and
bounded cleanup; only its client asset size/hash changes in the manifest.
These are approved fixture/ownership repairs, with no wire/schema/provider or
SDK-default change. Original coverage columns remain frozen baseline evidence;
additive current fixture bindings identify the four repaired method bodies.

Historical receipts and predecessor seals retain their original identities.
Reuse requires reviewed dependency nonreach; affected web Worker/Fetch/Browser
isolation evidence must be refreshed after the client repair. Linuxx64/ARM64
retain97selected methods each and remain owner-blocked. Approval does not grant
native qualification, E43 acceptance, commit, push, release or deployment.

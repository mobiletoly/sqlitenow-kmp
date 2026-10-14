# SQLiteNow Oversqlite protocol simplification and efficiency

Status: **OWNER-ACCEPTED; K0 COMPLETE; K1-K5 IMPLEMENTATION AUTHORIZED; NOT QUALIFIED**

Created: 2026-09-19

Proposed FK/resolution amendment (2026-09-25):
[the FK-safe sync companion](oversqlite-fk-safe-sync-and-automatic-resolution.md) and its Go p0001
authority define the proposed next behavior. The status and authority hash below record the earlier
p0000 candidate; they do not authorize p0001 or reaccept subsequently edited authority bytes. The
new package's manifest records current bytes and requires review/acceptance of the reconciled set
before runtime implementation. Retained wire/qualification requirements remain; p0001 section 1.1
lists only its proposed subject-specific amendments. OpenAPI/corpus settlement precedes consumers.

Repository: `/Users/pochkin/Projects/my/sqlitenow-kmp`

Normative protocol authority:
[p0000: Oversqlite Protocol Simplification and Efficiency](../../go-oversync/specs/p0000-oversqlite-protocol-simplification-and-efficiency.md)

Authority candidate SHA-256:
`5b846ea366db9022dcd817721b98897ff4d7e578cd466796a5c3e97b5221e24f`

Owner acceptance and K1-K5 implementation authorization for the exact p0000 and companion hashes
are recorded in `.artifacts/p0000-protocol-efficiency/kmp-ledger.md`. Any p0000 edit makes this
companion stale and requires reconciliation and a new pinned hash before implementation continues.

## 1. Purpose and authority boundary

This companion defines the SQLiteNow KMP and Swift-consumer work required to implement the accepted
Go-owned Oversqlite transport contract. It does not redefine endpoint shapes, tuple encoding,
compression thresholds, hash inputs, error precedence, or server behavior. If this document and the
accepted p0000 authority conflict on the wire contract, p0000 wins and implementation stops until
this companion is corrected.

This work is independent of SQLiteNow schema-evolution m0009, m0010 qualification, SE-03, and the
Go `m0000` child series. It may reuse their retained generation and opening behavior, but it neither
reopens their acceptance nor becomes a residual schema-migration phase.

Planning and owner acceptance are complete. K0 is complete and K1-K5 implementation is authorized.
Qualification, independent review, implementation acceptance, commit, push, publication,
deployment, and release remain separate later gates and are not authorized by this record.

## 2. Breaking-change posture

SQLiteNow must cut over to the simplified protocol directly:

- retain the exact protocol identity `v1`;
- do not introduce `v2`, version ranges, negotiation, compatibility modes, or a legacy-client flag;
- remove old push-session, committed-row-fetch, and per-operation capabilities code rather than
  wrapping it;
- remove obsolete models, persisted session locators, branches, tests, and documentation;
- do not accept both verbose row objects and compact row tuples on any network path; and
- coordinate the KMP, Swift, Go-server, direct-Go-client, fixture, sample, and documentation cutover
  before acceptance.

An incompatible change to internal SQLite metadata must use the repository's existing fail-closed
local-format mechanism. Breaking-change mode does not authorize silent loss of application rows,
but it also does not require a dual reader or a migration for unreleased upload-session metadata.

## 3. Current SQLiteNow observations

These are repository observations that identify replacement work:

- `OversqliteContractGateway.verifyProtocolGate` fetches and validates capabilities for an ordinary
  operation instead of using an opening-time admitted contract.
- `OversqliteClientSyncCoordinator.executeSyncRound` performs push and then pull. Under the current
  push-session workflow, a one-row successful sync can require capability fetch, session creation,
  chunk upload, commit, committed-row retrieval, and pull.
- `OversqliteAutomaticDownloads.runBundleChangeWatchIteration` invokes pull-to-stable from each
  watch event callback, allowing event bursts to request redundant pulls.
- `OversqliteConfig` has fixed row-count-oriented `uploadLimit` and `downloadLimit` defaults while
  the server already publishes limits.
- the owned HTTP client accepts gzip responses and decoded bodies are bounded, but ordinary upload
  requests are not selectively gzip encoded.
- existing model classes mirror verbose row objects and the multi-endpoint push-session lifecycle.

The implementation must replace these shapes; the observations are not reasons to preserve them.

## 4. Required KMP outcome

### 4.1 One admitted contract per ready-client lifetime

Opening fetches and validates `GET /sync/generations/{generation_id}` and retains its exact contract
inside the ready runtime state. The published ready client has immutable access to:

- protocol identity;
- generation ID and fingerprint;
- canonical table order and each table's key-column order;
- wire column types;
- hard row and decoded-byte limits; and
- watch availability.

`sync`, `push`, `pull`, automatic downloads, watch reconnect, snapshot creation, and snapshot chunk
fetches consume this admitted contract. They do not fetch capabilities or discovery first.
Reopening or explicit reattachment replaces the contract. Terminal generation/source errors enter
the existing opening or recovery workflow; they do not trigger an invisible in-operation refresh.

`GET /sync/capabilities`, `verifyProtocolGate`, operation-specific capability-gate strings, and
models or fixtures used only by that request are removed.

Discovery and connect success bodies are decoded as the exact OpenAPI objects, not permissive maps.
Connect accepts only `remote_authoritative`, `initialize_local`, `initialize_empty`, or
`retry_later`; its optional `initialization_id` is an exact lowercase dashed UUID and must not be
trimmed, normalized, or case-folded. The client rejects duplicate, folded/case-colliding, unknown,
missing, null-for-required, or trailing JSON members wherever OpenAPI sets
`additionalProperties: false`. A malformed success or typed-error representation preserves the
frozen outbox/checkpoint and becomes an admitted-contract/protocol failure rather than a retry-loop
signal or legacy fallback.

The KMP error classifier follows the frozen p0000/OpenAPI status matrix. It recognizes the complete
known error-code set and the retained deployment-declared HTTP 422 application-rejection category;
in particular, it does not collapse replay mismatch, projection overflow, committed-bundle
corruption, source/generation lifecycle errors, or snapshot limit errors into a generic retryable
failure.

### 4.2 Compact codec

The KMP codec implements p0000 table indices, contiguous row groups, tuple arity, operation codes,
key ordering, ordinals, and payload rules exactly. It must:

- derive `table_index` only from the admitted canonical discovery table array;
- map sync-key objects to and from values in exact `sync_key_columns` order;
- retain payload objects so omitted columns remain distinct from SQL `NULL`;
- reject malformed tuples before local database mutation;
- expand tuples to the existing semantic row model before canonical hashing or application;
- preserve logical row order across table-group boundaries; and
- encode compact JSON without pretty printing or optional whitespace.

Platform implementations must not each invent a codec. Common Kotlin owns semantic validation and
tuple construction/parsing; target-specific code may supply bounded byte streams and gzip engines.

### 4.3 Bounded whole-bundle push

The KMP client replaces create/chunk/commit/fetch with one `POST /sync/push` for each frozen local
bundle.

Before freezing a bundle, selection observes both `max_push_rows` and
`max_push_decoded_bytes`. The implementation incrementally measures the complete compact request,
including the envelope; a conservative estimate may be used only when it cannot reject a sendable
first row. `transferTargets.maxPushDecodedBytes` is a soft batching target and the admitted server
maximum is the hard protocol limit. If the first pending row plus the envelope exceeds the local
target but fits the server maximum, that row is frozen and sent alone. Additional rows are included
only while the complete body remains within the local target. An envelope larger only than the
local target uses the same one-row rule; the client never sends an empty push.

If the complete first-row request exceeds the admitted server maximum, the client leaves the row
dirty and does not freeze a bundle or consume a source bundle ID. A row whose logical size exceeds
`max_row_decoded_bytes` raises terminal `push_row_exceeds_admitted_limit`; a row within that limit
whose complete request still does not fit raises terminal `invalid_admitted_contract` because the
p0000 relationship is false. Recovery requires correcting the data/schema or opening against a
sufficient valid server contract. If the fixed envelope itself exceeds the server maximum, opening
fails with `invalid_admitted_contract` rather than allowing a retry loop. The one-row exception
remains bounded by the server contract.

Rows that must commit together because of pending foreign-key dependencies form an indivisible
selection group. Byte batching never freezes a prefix of such a group. A complete first group may
exceed the local soft target when it fits the admitted hard limit. If it cannot fit the hard limit,
the client raises terminal `push_dependency_group_exceeds_admitted_limit` before freezing, removing
dirty rows, or consuming a source bundle ID. This does not enlarge any server wire limit.

The frozen outbox retains the semantic rows, source bundle ID, generation proof, and canonical
request hash needed for an exact whole-request replay. It does not retain a push ID, uploaded chunk
ordinal, commit phase, or committed-row page locator. A validated success response and its compact
authoritative effects are applied atomically before the outbox is cleared.

Fault behavior is exact:

- death before request send preserves the frozen bundle;
- request failure before a known response preserves it;
- loss of the success response preserves it and retries the same request;
- exact committed replay settles it even when an initialization commit already changed the scope to
  initialized or later mutable source lifecycle/sequence state changed;
- `409 push_replay_mismatch` is the only committed-tuple hash-collision outcome and is terminal;
  it is distinct from a new-push `409 push_conflict`;
- `bundle_projection_too_large` is a known-not-committed terminal outcome: the client retains the
  prepared outbox, stops automatic retry, and requires data/schema/server-contract correction;
- mismatched replay, conflict, rejection, source retirement, and initialization errors remain
  explicit actionable outcomes; and
- a newer local edit to the same row is not erased while an older frozen bundle settles.

Internal SQLite columns, state values, and recovery branches that exist only for staged upload or a
separate committed-row settlement phase are removed according to section 5.1.

For a frozen replay, Kotlin must rebuild the same complete request and initialization ID. It does
not strip the initialization ID, allocate a new source bundle, refresh an initialization lease, or
run a compensating lifecycle transition. The server's p0000 replay-before-mutable-lifecycle rule is
authoritative; history-pruned or unavailable-generation outcomes enter the existing explicit
recovery path.

### 4.4 Pull and snapshots

Pull and snapshot decoders consume the compact groups from p0000 and retain existing atomic apply,
deferred-FK, stable-ceiling, checkpoint, history-pruned, source-replacement, staging-invisibility,
and final-swap behavior.

`committed_bundle_unrepresentable` is a terminal server-invariant failure: the client preserves its
durable pull checkpoint, stops automatic retry, and requires operator repair or explicit recovery.
`snapshot_session_limit_exceeded` during creation publishes no local session state and is terminal
until data or server limits change. Neither outcome may be converted into empty-page polling,
smaller-bundle speculation, or a legacy-protocol fallback.

Client validation derives row counts and decoded byte observations from the actual compact body and
expanded logical rows rather than requiring removed redundant fields. Snapshot durable state still
binds the session ID and snapshot bundle sequence, so chunk bodies need not repeat them.

Snapshot tuples accept only the p0000 `"i"` full-after-image form; `"u"`, `"d"`, a missing payload,
or a partial payload is a protocol failure before staging. Session and chunk
`logical_byte_count` values use `oversync.snapshot-logical-row.v1` exactly. The client checks each
chunk's value from expanded rows, adds it with overflow checks, persists the accumulated logical
total with staging progress, and requires exact equality with the session total before atomic
adoption. HTTP decoded-body sizes remain separate diagnostics and never enter that total.

The `transferTargets.maxPullBundles`, `maxSnapshotChunkRows`, and
`maxSnapshotChunkDecodedBytes` values defined in section 5 are reconciled with admitted hard limits:
configuration may request a smaller local bound, but it cannot enlarge a server bound.

The snapshot byte target is soft only for one-row progress. If
`snapshot_chunk_too_small.required_decoded_body_bytes` is greater than the requested target but no
greater than the admitted server maximum, the client retries the same ordinal once with
`max_rows=1` and that exact byte value. A required value above the server maximum, a changed
required value on the retry, or another too-small response is a protocol violation and leaves the
durable snapshot cursor unchanged. `required_decoded_body_bytes` is a complete compact HTTP-body
size and is never compared with or added to `logical_byte_count`.

### 4.5 Selective gzip on every supported target

The common transport policy implements the p0000 2,048-byte threshold:

- compact request bodies below the threshold use identity;
- larger push bodies are gzip encoded only when the encoded bytes are strictly smaller;
- raw/native bounded JSON requests explicitly advertise gzip response support;
- decoded response limits are enforced after content decoding without unbounded buffering;
- request encoded bytes are bounded on every target; raw/native response encoded bytes are bounded
  by the client, while Fetch-managed browser response intake uses the p0000 user-agent boundary;
- malformed, concatenated, or unsupported encodings fail closed where the transport exposes them;
  a Fetch rejection is a transport failure and preserves retry state; and
- watch event streams remain identity encoded.

JVM, Android, Apple/native, JS, and Wasm implementations use the same JSON and semantic contract.
JVM, Android, Apple/native, and other raw transports enforce both encoded and decoded response
ceilings and gzip-member rules. Browser JS/Wasm use Fetch, which owns `Accept-Encoding`, encoded
response bytes, and decoding before Kotlin sees the body; Kotlin incrementally enforces the decoded
ceiling and any metadata still exposed. Browser parity therefore requires request compression,
decoded-limit enforcement, retry-safe Fetch failures, and external network evidence of compliant
server encoding, but it does not falsely claim client inspection of discarded encoded bytes or gzip
members. Browser request compression supplies precompressed bytes with `Content-Encoding: gzip`;
cross-origin samples and real-server lanes must configure and prove the required CORS preflight and
header allowance. A denied preflight preserves the frozen operation as an environment/transport
failure rather than silently retrying the body with identity. No runtime capability negotiation is
added.

### 4.6 Watch conflation

Automatic downloads use one conflated high-water signal rather than calling pull once per event.
Common orchestration owns the state machine:

1. the first newer event starts or schedules pull-to-stable;
2. events during an active pull only raise the pending high-water mark;
3. completion schedules at most one trailing pull when the durable checkpoint remains behind; and
4. reconnect compares the durable checkpoint with the observed high-water mark before pulling.

The callback must not block SSE consumption on an entire pull. Cancellation closes both watch and
scheduled pull work under the ready client's existing structured-concurrency ownership. Polling
fallback remains correct when watch is disabled or fails.

## 5. Public API and configuration cleanup

The four transport-sizing properties currently spread across `OversqliteConfig` are replaced by one
coherent client-side value:

```kotlin
data class OversqliteTransferTargets(
    val maxPushRows: Int = 200,
    val maxPushDecodedBytes: Long = 1L * 1024L * 1024L,
    val maxPullBundles: Int = 1000,
    val maxSnapshotChunkRows: Int = 1000,
    val maxSnapshotChunkDecodedBytes: Long = 4L * 1024L * 1024L,
)
```

`OversqliteConfig` contains one `transferTargets` property with the default value above.
`uploadLimit`, `downloadLimit`, `snapshotChunkRows`, and `snapshotChunkBytes` are removed without
aliases. Snapshot apply-batch settings remain separate because they bound local database work rather
than wire transfer.

Every target is positive. The admitted server values are hard ceilings. `maxPullBundles` and the
ordinary requested push/snapshot values are the minimum of local target and matching server maximum.
`maxPushRows` and `maxPushDecodedBytes` jointly guide frozen outbox selection, subject to the
section 4.3 one-row soft-target exception. Both snapshot targets are normally sent on chunk requests,
subject to the section 4.4 exact one-row retry. Local targets never enlarge a server contract and
are not advertised back as capabilities.

### 5.1 Exact local persistence replacement

`_sync_outbox_bundle` retains:

- `singleton_key` and `canonical_json_contract`;
- `state`, whose only legal values become `none` and `prepared`;
- `source_id`, `source_bundle_id`, and optional `initialization_id`;
- `canonical_request_hash`; and
- `row_count`.

It removes `committed_remote`, `remote_bundle_hash`, `remote_bundle_seq`, `remote_row_count`, and
`remote_byte_count`. `_sync_outbox_rows` retains the frozen semantic row fields required to rebuild
the same complete push request. It does not persist compact transport tuples; those are encoded from
the semantic rows and the admitted contract on each exact replay.

The outbox remains `prepared` from freeze until the compact success response is validated and its
authoritative effects are applied in the existing atomic local transaction. A crash or cancellation
before that transaction completes retries the same whole push. There is no intermediate durable
receipt state and no separate committed-row settlement path.

The retained canonical hash remains `jcs_uniform_numeric_strings_v1` because the logical hash input
does not change. The exact retained-table DDL and physical contract do change. An existing local
database with the obsolete outbox shape fails closed during opening and must be recreated; the new
runtime does not migrate, reinterpret, or reset it automatically. This is an explicit development
cutover. Failure must occur before application access or sync mutation and must identify the
incompatible local control schema without deleting application rows.

The rest of the public cleanup is exact:

- no public push-session or committed-row-fetch type remains;
- no protocol-version selector or compatibility flag is added;
- Swift exports expose the simplified stable client API and do not leak tuple-codec internals; and
- generated products and samples use only the new path.

This draft does not freeze exact source names for internal Kotlin classes. The wire names and
observable behaviors are fixed by p0000; the implementation should delete obsolete abstractions and
choose the smallest common-code structure that expresses them.

## 6. Fixture and test ownership

SQLiteNow retains the one physical language-neutral fixture corpus in `oversqlite-contracts/`.
p0000 governs the required wire meanings and scenarios; this companion governs corpus storage,
manifest integrity, and consumer wiring. The corpus README must identify the accepted p0000 and
companion hashes without claiming that fixture storage transfers server authority to SQLiteNow.

Go and KMP tests consume the exact manifest-pinned files. The manifest is
`oversqlite-contracts/MANIFEST.sha256`; it contains sorted lowercase SHA-256 records for every corpus
file except itself, using repository-relative POSIX paths and two spaces between digest and path.
Go tests receive an explicit corpus root in standalone or paired qualification and fail closed when
it is missing or mismatched. Neither repository may maintain an independently edited fixture copy
or silently replace the corpus with equivalent-looking test literals.

The implementation adds or replaces these durable fixture families:

- `wire/compact-rows.json` for table indices, contiguous groups, tuple arities, key order, partial
  payloads, ordinals, insert-only full-after-image snapshots, and retained hashes;
- `wire/compression.json` for 2,047/2,048/2,049-byte boundaries, compressible and incompressible
  bodies, malformed gzip, trailing data, encoded/decoded overflows, and explicit `all`,
  `raw_transport`, or `fetch_server_wire` applicability;
- `wire/request-counts.json` for opening, no-change, one-row, replay, pagination, and recovery traces;
- `wire/faults.json` for interruption, response loss, process death, conflict, rejection,
  retirement, replay mismatch, and lost initialization-success replay after restart;
- `wire/limits.json` for local soft-target one-row progress, hard-limit rejection without source-ID
  consumption, indivisible pull/snapshot bodies, snapshot logical-byte totals,
  `required_decoded_body_bytes`, and empty-page-loop prevention;
- `wire/errors.json` for strict envelope `invalid_request` versus accepted-envelope tuple/logical
  `push_invalid` precedence; and
- the existing `protocol-handshake`, `protocol-http`, `push`, `pull-snapshot`, and `watch` fixtures,
  rewritten in place so no accepted network fixture still teaches the legacy routes or row objects.

KMP-owned tests must cover:

- discovery admission and absence of per-operation preflights;
- table-index/key-order encoding and decoding;
- all valid tuple arities, insert-only snapshot rows, and every malformed tuple class;
- retained request-hash and bundle-hash equivalence;
- one-request push, exact replay including initialization completion, mismatch, conflict, rejection,
  and process-death boundaries;
- selection by rows and complete decoded request bytes, including the local-target one-row
  exception and terminal server-hard-limit case without source-ID consumption;
- pull pagination, stable-ceiling behavior, indivisible bundle admission, legitimate generation-
  projected bundles with `rows: []` that verify the empty-row hash and advance the checkpoint,
  terminal retained `committed_bundle_unrepresentable`, and no empty-page or checkpoint-skipping
  loop;
- snapshot staging, logical-byte accounting, one-row chunk retry, resume, expiry, final apply, and
  source replacement with compact chunks;
- identity and gzip request/response behavior at 2,047, 2,048, and 2,049 decoded bytes;
- gzip bomb, truncation, trailing-data, unsupported-encoding, cancellation, and redirect refusal on
  every applicable raw transport, plus Fetch-managed decoded bounds and captured server-wire cases;
- watch bursts, reconnects, cancellation, and polling fallback; and
- removal of legacy routes, models, state values, tests, docs, and sample calls.

Mock transport tests prove deterministic edge cases. Real-server lanes prove actual HTTP content
encoding, headers, request counts, streaming cancellation, and cross-repository compatibility.

## 7. Required evidence and qualification

After separately authorized implementation, qualification must freeze one Go revision and one
SQLiteNow revision and record them in a paired ledger. At minimum it includes:

- focused common Kotlin codec, gateway, push, pull, snapshot, watch, and persistence tests;
- `:library-oversqlite` host and supported-platform suites required by the repository;
- `oversqliteComprehensive` with forced execution when execution evidence is claimed;
- JVM, Android, Apple/native, JS, and Wasm real-server lanes that the accepted repository matrix
  marks mandatory;
- Swift hybrid/export gates and a real-server Swift-facing lane where required;
- the manifest-pinned shared fixture corpus on every applicable consumer;
- request traces proving the one-request no-change and two-request one-row sync budgets;
- identity/gzip byte artifacts for the p0000 workload matrix;
- bounded-memory evidence for large pull and snapshot bodies;
- repository searches proving obsolete capability and push-session surfaces are absent; and
- docs/sample checks against the accepted OpenAPI hash.

A host-only KMP suite does not replace platform or live-server evidence. A passing Go server suite
does not prove KMP client behavior. `UP-TO-DATE` output is not execution evidence when a lane is
claimed as freshly run.

## 8. Implementation sequence and stop gates

This companion follows the p0000 gates:

1. review and owner-accept p0000;
2. pin its exact SHA-256 in this document, reconcile any companion differences, and separately
   owner-accept this companion;
3. freeze Go OpenAPI and shared fixtures;
4. complete and validate the Go authority and direct Go client;
5. implement the KMP/common, target transport, persistence, generated-product, Swift, sample, and
   documentation changes;
6. run focused author validation;
7. freeze paired revisions and run cross-runtime qualification;
8. conduct independent review and remediate against the same authority; and
9. stop for explicit owner acceptance before any delivery action.

If the Go authority changes after its freeze, KMP work stops until the authority hash, fixture hash,
OpenAPI hash, affected implementation, and invalidated evidence are reconciled. If a raw target
cannot enforce both byte ceilings/member rules, or a Fetch-managed browser cannot satisfy the
explicit section 4.5 boundary, that is a release blocker rather than authorization to keep the
legacy path.

## 9. KMP acceptance criteria

The SQLiteNow candidate is eligible for paired acceptance only when:

- the admitted discovery contract is fetched once per opening and no ordinary operation performs a
  capability preflight;
- a no-change sync after opening performs exactly one pull request;
- a one-row sync after opening performs exactly one push request followed by one pull request;
- a bounded multi-row source bundle is uploaded by one idempotent request;
- compact rows, table indices, key order, ordinals, payloads, and hashes match the p0000-governed,
  SQLiteNow-stored manifest-pinned fixtures on every supported runtime;
- request batching is constrained by row and complete decoded-body bounds, with only the bounded
  one-row soft-target exception needed for progress;
- small bodies use identity and beneficial larger bodies use gzip; raw transports enforce decoded
  and encoded response limits, while Fetch-managed browsers enforce decoded limits and rely on the
  explicit user-agent boundary for encoded response intake;
- watch bursts are conflated to one active and at most one trailing pull;
- every specified fault preserves or settles the correct frozen intent;
- pull and snapshot recovery retain their atomicity and boundedness guarantees, no indivisible
  result can cause a pagination stall, and snapshot totals are pagination-independent;
- Swift-facing products and samples use the new client without exposing transport tuples;
- the legacy capabilities, push-session, chunk, commit, committed-row-fetch, and associated durable
  state surfaces are absent; and
- paired real-server evidence uses the exact accepted Go revision and contract artifacts.

## 10. SQLiteNow implementation checklist

Every item is intentionally unchecked. The implementation task updates this list or a linked
ledger with evidence; prose claims do not close an item.

### K0 — authority and paired freeze

- [x] Record explicit owner acceptance and implementation authorization for the exact p0000 and
  companion hashes.
- [x] Verify the accepted Go implementation handoff has frozen OpenAPI and shared-fixture meanings;
  record Go revision, OpenAPI hash, and required corpus-manifest inputs.
- [x] Record SQLiteNow `git rev-parse HEAD`, branch, worktree status, Java/Gradle/Kotlin toolchain,
  Xcode/Swift versions where applicable, browser drivers, Android device/emulator, and Docker.
- [x] Preserve unrelated worktree state and stop on an unowned overlapping edit.

K0 evidence and the exact accepted identities are recorded in
`.artifacts/p0000-protocol-efficiency/kmp-ledger.md`.

### K1 — corpus and admitted contract

- [ ] Create `oversqlite-contracts/MANIFEST.sha256` with the exact format in section 6 and update the
  corpus README with accepted p0000 and companion hashes.
- [ ] Add or rewrite every fixture family named in section 6 and make malformed/error precedence
  explicit rather than inferred from prose.
- [ ] Update shared fixture readers so KMP and Go consume those exact files and fail on a missing,
  extra, stale, or mismatched manifest entry.
- [ ] Replace capabilities models and `OversqliteContractGateway.verifyProtocolGate` with one
  immutable admitted discovery contract stored in ready runtime state.
- [ ] Prove opening validates discovery then connect, publishes the contract only at ready, and
  performs no ordinary-operation discovery/capabilities preflight.

### K2 — common compact codec and configuration

- [ ] Implement compact table-index/group/tuple encode and decode in common Kotlin; target code owns
  byte streams and gzip only, not semantic codecs.
- [ ] Expand compact tuples to retained semantic rows before hashing, conflict handling, or local
  apply and prove exact hash equivalence against the manifest-pinned vectors.
- [ ] Reject every malformed index, group, ordinal, arity, operation, key, wire type, payload,
  hidden column, duplicate target, and trailing JSON case before mutation; snapshots accept only
  insert-coded full after-images.
- [ ] Replace the four legacy top-level configuration properties with the exact
  `OversqliteTransferTargets` API from section 5 and update generated Swift/export surfaces,
  samples, and API tests without aliases.
- [ ] Select pushes by both row and complete compact decoded-body size, implement the exact one-row
  local-target exception, and prove an over-server-limit first row remains dirty without freezing a
  bundle or consuming a source bundle ID.

### K3 — transport, push, pull, and persistence

- [ ] Replace push-session/chunk/commit/committed-row models and methods in
  `OversqliteRemoteApi.kt` and `OversqlitePushWorkflow.kt` with one strict `POST /sync/push`.
- [ ] Keep semantic frozen rows in `_sync_outbox_rows`, implement the exact two-state bundle table
  from section 5.1, and remove `committed_remote` plus all `remote_*` receipt fields and branches.
- [ ] Update `OversqliteRetainedTableDdl.kt`, `OversqliteOutboxStateStore.kt`, opening/contract
  validation, snapshot gates, source recovery, and tests for fail-closed old-layout behavior.
- [ ] Apply validated compact push effects atomically before clearing the prepared outbox; prove
  response loss, process death, replay before mutable lifecycle checks, initialization completion
  replay with the same initialization ID, mismatch, conflict, rejection, retirement, and newer-edit
  protection.
- [ ] Classify `bundle_projection_too_large`, `committed_bundle_unrepresentable`, and snapshot
  representation-limit failures as the exact terminal nonmutation outcomes in sections 4.3-4.4;
  preserve frozen intent/checkpoints and stop automatic retry.
- [ ] Replace pull and snapshot decoders with compact groups while retaining stable-ceiling,
  checkpoint, history-pruned, deferred-FK, stage/resume/final-swap, expiry, and source-replacement
  behavior.
- [ ] Implement `oversync.snapshot-logical-row.v1`, validate chunk/session
  `logical_byte_count`, implement the exact one-row `required_decoded_body_bytes` retry, and keep
  logical accounting separate from complete decoded HTTP-body limits.
- [ ] Derive removed row observations and body-size diagnostics locally only where a retained
  invariant needs them; do not recreate redundant bundle wire fields.

### K4 — bounded gzip and watch scheduling

- [ ] Implement identity/gzip selection, request bounds, and decoded response ceilings for JVM,
  Android, Apple/native, JS, and Wasm using the exact 2,048-byte rule; raw transports additionally
  enforce encoded response ceilings and gzip-member rules.
- [ ] Implement the explicit Fetch-managed browser boundary without setting forbidden
  `Accept-Encoding`, relabeling decoded bytes as encoded, or weakening redirect refusal,
  cancellation, decoded limits, request compression, required CORS preflight, or retry preservation.
- [ ] Reject malformed, concatenated, unsupported, or multiple encodings without identity fallback
  on raw transports and classify a user-agent Fetch decode failure as a retry-safe transport error.
- [ ] Keep SSE identity encoded and change watch parsing to the exact bundle-sequence-only event.
- [ ] Replace callback-driven pull-per-event behavior with the common conflated high-water state
  machine, non-blocking event consumption, structured cancellation, and polling fallback.

### K5 — public surface, products, docs, and author freeze

- [ ] Update `Models.kt`, `DefaultOversqliteClient.kt`, lifecycle/sync coordinators, protocol
  validation, errors, docs, samples, generated products, and Swift wrappers to expose only the new
  path.
- [ ] Update `docs-engineering/oversqlite_flow.md`, `docs-engineering/oversqlite_test.md`, sample
  documentation, and public API documentation with exact new requests and commands.
- [ ] Search source, tests, fixtures, generated output, samples, and durable docs for capabilities,
  push-session, chunk, commit, committed-row-fetch, `committed_remote`, `remote_*`, and removed config
  names; every remaining hit must be historical input or an explicit absence assertion.
- [ ] Run focused tests until stable, review every checklist item, and freeze production changes.
- [ ] Record changed paths, final SQLiteNow identity, accepted spec/OpenAPI/manifest hashes, and
  invalidation rules in `.artifacts/p0000-protocol-efficiency/kmp-ledger.md`.

## 11. Exact validation checklist

Run from `/Users/pochkin/Projects/my/sqlitenow-kmp`. Every command records start/end time, exit
status, tool versions, raw log path, and unchanged candidate identities. Use `--rerun-tasks` where
shown; `UP-TO-DATE` is not fresh execution evidence.

### Focused and cross-repository

- [ ] Focused common/JVM tests for discovery, compact codec, insert-only snapshots, hash vectors,
  soft-target batching, outbox DDL/recovery, initialization replay, error precedence, gzip/Fetch
  profiles, indivisible pull, snapshot logical bytes/one-row retry, watch, redirect, and cancellation
  pass.
- [ ] `GO_OVERSYNC_REPO=/Users/pochkin/Projects/my/go-oversync ./gradlew oversqliteCrossRepoConformance`
  passes and records the frozen Swagger and corpus-manifest hashes.
- [ ] `./gradlew oversqliteComprehensive --rerun-tasks` passes.
- [ ] `./gradlew verifyGeneratedProductionOutputs --rerun-tasks` passes when generated production
  outputs change.
- [ ] `./gradlew :sqlitenow-gradle-plugin:test --rerun-tasks` passes only if generator/plugin code
  changes; otherwise the ledger records it as not applicable with the unchanged paths.

### Platform and Swift

- [ ] `./gradlew oversqlitePlatformAll --rerun-tasks` passes, including Android, JVM, iOS simulator,
  macOS arm64, JS Node/browser, and Wasm browser wrappers.
- [ ] `./gradlew swiftHybridSupportGate --rerun-tasks` passes.
- [ ] Target evidence proves request gzip and decoded-limit behavior on JVM, Android, Apple/native,
  JS, and Wasm rather than only testing the common decision function; raw targets also prove encoded
  response ceilings/member rejection, while browser targets prove the Fetch boundary and do not
  claim raw-byte inspection; cross-origin browser evidence includes the gzip request preflight.

### Real server and paired evidence

- [ ] Start the frozen compatible Go server against a fresh supported PostgreSQL database and record
  its revision, OpenAPI hash, database image, base URLs, and logs.
- [ ] `./gradlew oversqliteRealserverAll --rerun-tasks` passes against that server.
- [ ] `./gradlew swiftHybridSupportRealserverGate --rerun-tasks` passes.
- [ ] `./gradlew oversqliteRealserverAllHeavy --rerun-tasks` passes for final boundedness/stress
  evidence; it is local-only and is not replaced by ordinary real-server lanes.
- [ ] Raw traces prove exactly discovery/connect at opening, one pull for admitted no-change sync,
  push then pull for admitted one-row sync, and no capabilities or legacy route.
- [ ] Byte artifacts cover every p0000 section 9.2 workload with identity bytes, gzip bytes, ratio,
  selected encoding, decoded bytes, semantic/hash equality, and provenance distinguishing raw
  receiver counts from externally captured Fetch browser network counts.
- [ ] Boundary artifacts prove a local-target oversized first row still progresses alone, a
  server-hard-limit row fails without source-ID consumption, every admitted indivisible pull or
  snapshot row fits, snapshot logical totals survive different pagination, and initialization
  replay succeeds after response loss and restart.
- [ ] A 100-event burst during one active pull causes no more than that pull and one trailing pull and
  reaches the high-water checkpoint.
- [ ] `git diff --check` passes for Git-visible changes, and direct SHA-256 validates ignored specs
  and corpus artifacts.

Environment limitations do not turn a mandatory lane into a pass. Record a blocker and stop paired
acceptance until the lane runs on a suitable host.

## 12. Cross-repository execution order and stop gates

1. Accept exact p0000 and companion hashes and authorize implementation separately.
2. Complete Go P0 OpenAPI decisions and coordinated shared-corpus meanings; freeze their hashes.
3. Implement and author-validate Go server/direct-client P1-P4; do not claim paired acceptance.
4. Implement SQLiteNow K0-K5 against that frozen authority.
5. Freeze both production candidates and run section 11 plus p0000 section 17 without changing
   production code between paired lanes.
6. Prepare one paired evidence ledger identifying required, focused, superseded, rerun-required,
   blocked, and not-applicable lanes.
7. Conduct independent review against the exact pair, remediate findings, and rerun invalidated
   evidence.
8. Stop for explicit owner acceptance before staging, commit, push, publication, deployment, or
   release.

If Go OpenAPI, codec semantics, errors, or fixtures change after step 2, SQLiteNow work stops until
the authority and companion are reconciled. If production code changes after step 5, the affected
focused/platform lane and final aggregate/paired lanes become rerun-required.

## 13. Fresh-session implementation handoff

K0 records owner acceptance and implementation authorization for the exact p0000 and companion
hashes, and freezes the Go authority candidate. Use this handoff from that recorded K0 baseline.

**Repository:** `/Users/pochkin/Projects/my/sqlitenow-kmp`, paired read-only authority initially at
`/Users/pochkin/Projects/my/go-oversync`.

**Objective:** implement the complete accepted SQLiteNow companion across common KMP, every target
transport, local persistence, shared fixtures, generated/Swift products, samples, tests, and durable
documentation. Replace the old protocol; do not add compatibility or a parallel path.

**Prerequisites:** re-read both accepted specs completely; verify exact hashes, Go revision, OpenAPI,
and corpus manifest; inspect and preserve unrelated worktree state; verify required Java, Gradle,
Android, Xcode/Swift, browser, Docker, and real-server prerequisites. Stop on any mismatch, missing
authority, or unowned overlapping edit.

**Execution:** complete K1-K5 in order, using focused tests during development. Freeze production
changes before section 11. Do not execute the section 11 qualification campaign in this session;
prepare its exact ledger and commands for a separate qualification session. Keep Go production
source read-only unless separately authorized to remediate a confirmed authority defect.

**Deliverables:** implementation; manifest-pinned corpus; exact local DDL; common and target gzip;
request-count/watch/byte test scaffolding; focused development evidence;
`.artifacts/p0000-protocol-efficiency/kmp-ledger.md`; changed-path manifest; exact final identities;
and a paste-ready section 11 qualification handoff. Comprehensive, platform, Swift, real-server,
heavy, and paired qualification evidence belongs to the separate qualification session.

**Exclusions:** no protocol `v2`, compatibility aliases, dual codecs/readers/writers, old database
migration, Go production change without separate authority, schema-evolution m-series work,
unrelated refactoring, staging, commit, push, publication, deployment, or release.

**Stop condition:** stop at `IMPLEMENTED: READY FOR PAIRED QUALIFICATION AND INDEPENDENT REVIEW`.
Do not claim owner acceptance, delivery, or release readiness.

## 14. Non-goals

This companion does not:

- define or negotiate a new protocol version;
- preserve compatibility with the replaced wire or client APIs;
- implement or accept changes in the Go repository;
- change schema-generation evolution, migrations, backfills, or m0010 qualification ownership;
- combine push and pull into one request;
- replace JSON with a binary codec;
- add positional payload masks;
- make SSE authoritative;
- relax body limits, hash validation, redirect refusal, generation proof, source lifecycle, conflict
  handling, or durable recovery; or
- authorize implementation, qualification, review acceptance, staging, commit, push, publication,
  deployment, or release.

## 15. Convergent interactive sync amendment (2026-09-24)

The locally authorized convergent-sync work extends the client behavior in this companion without
changing the p0000 wire contract. A public `sync()` drains successive bounded, independently
admitted push bundles before pull, including local writes that race push or pull. It shares a
64-entry push-preparation allowance across conflict reentry, prepared-outbox replay, checkpoint
recovery, whole-operation retries, and all rounds of `syncThenDetach()`. HTTP retries of the same
prepared request do not add entries. The next entry is refused before attempt 65; a completed
attempt 64 may still pull if no local work remains. Confirmed bundles are counted once by durable
source ID and source-bundle ID, and any confirmed bundle makes aggregate push outcome `COMMITTED`.

The client exposes a typed incomplete result with dirty and outbound counts, unique confirmed
bundle count, and reason `push_attempt_limit` or `no_progress`. Durable work identity, including
dirty keys, ordinals, payloads, outbox and source sequence, is required to prove no progress;
equal counts alone are insufficient. Cancellation and terminal conflict, rejection, recovery,
authentication, lifecycle, and unsendable-work errors retain their own precedence. Each pulled
bundle checks dirty and outbox state inside its applying write transaction. A typed local-work
rejection resumes the bounded drain; no other pull error does. Recovery uses recovery-authorized
pushes through the same allowance and retries its final snapshot gate after a racing local write.

Explicit KMP operations continue during background upload or download pause. Automatic downloads
remain paused and retain an unfulfilled watch target for a bounded wake-up. Committed push effects
preserve locally dirty extra-effect keys and any desired row changed by an application trigger,
including after restart replay. `syncThenDetach()` uses at most three rounds and returns a blocked
detach with an optional last fully completed sync report if drainage cannot finish; its typed
blocked reason includes `push_attempt_limit`, `no_progress`, or `sync_round_limit`. It never
detaches pending work. Swift owns cancellation of the delegated operation through cleanup.

The no-change one-pull and one-row push-then-pull traces in section 9 remain exact. No endpoint,
field, protocol version, OpenAPI change, or corpus change is required: multiple pushes and
authoritative effects already use existing messages. The OpenAPI SHA-256 at this amendment is
`94940822a3ba5f4b614a7e95bde1f528ec5f463793eb4feb4fe6ee2de93c21b4`; the corpus
manifest SHA-256 is `0e64ece3dce806b815b9833dac2be9c7835888a149678db3a540c0937f722f3f`.

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


### Seed-stage unpublished-delete pruning

During initial root seeding, a delete already validated as unpublished/absent/deleted may be pruned without discarding unchanged roots seeded earlier in that same guarded traversal. Charge its validation and the additional guard refresh. Under the same writer transaction, advance the seed cursor and rebind the existing guard after only this SDK-owned deletion; dependency/edge traversal has not begun and the deleted key was never inserted as a root. Per-node intent revisions remain their capture revisions. Any external relevant revision change still takes the normal bounded invalidation/cleanup path. No new persistent field, phase discriminator or wire behavior is introduced. Required witnesses compare seed visits for 125 live roots plus 10 unpublished deletes, retain malformed-base refusal and second-writer invalidation, and execute the mixed-CRUD complex workload.

Bounded planning transaction refinement (2026-10-13): public push preparation may run at most 32 existing planning steps in one writer transaction. Work admission, guards, capacity checks, cursor persistence and cancellation checks remain per step. A normal work refusal after successful steps commits their already saved private frontier only after cleanup succeeds; other failures roll back the entire group. Tiny targets still admit one useful step. Transaction-owned schema metadata may be reused within that writer snapshot and is released before continuation. Raw bounded-kernel tests keep one-step transaction entry points. No configured target, accounting weight, hard quota, public operation unit or stored representation changes.

Writer-scoped planning admission snapshots (2026-10-13): while a bounded push-planning writer transaction owns the database, operation/reservation/snapshot and apply-owner scalar charges are immutable. They may be read once and reused for that transaction. Every planning allocation still adds its current push-plan charge and requested temporary charge and applies the unchanged hard limits; no retained push workspace is discounted. The finite scalar snapshot is charged as 2 entries and 256 bytes, released before the writer is released, and never reused across a transaction or work slice. No payloads or live SQL handles are retained. Guard checks and per-step work charges remain unchanged. Raw kernels and non-planning operations retain uncached admission. Required structural witnesses compare metadata SQL reads, steps, charged work, hard refusals and cleanup, separately from timing.

Writer-scoped planning guard snapshots (2026-10-14): a guard hash, source-bundle ordinal, initialization identity and their previously admitted context-size bounds may be reused only inside the same bounded planning writer transaction. Every step still checks its plan guard and current workspace against those bounds and receives the unchanged full work charge. The cache is charged as 4 entries/8192 bytes in addition to the scalar-owner snapshot; initialization identities over 1024 UTF8 bytes or insufficient optional cache capacity use fresh reads instead. SDK-owned no-op deletion invalidates the cached guard before rebinding. Other guard inputs cannot change under this writer; external changes are read at the next writer and final freeze gate. Release before continuation; no persistence or wire change. Required witnesses retain identical planning steps/work and demonstrate fewer guard reconstructions plus external-write, no-op, hard-limit and cancellation safety.

Decision-copy reservation continuation correction (2026-10-14): capture the operation-owned buffer reservation base once before replay continuation. Each slice admits its complete simultaneous decision-copy workspace relative to that same base, never relative to a previous slice's temporary charge. The decision allowance may conservatively retain its maximum for the one outer operation; it is reused, not accumulated, and the existing once-only outer release remains mandatory. Statement pools remain slice-owned and close/release before progress commit. Retained apply inputs stay fully charged. A paired125-row/8-table/max-push40/default-and-target1 fixture must complete without growing reservation entries as slice count increases. No weights, quotas, public signatures or stored layout change.


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

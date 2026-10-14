# Oversqlite contracts

This directory contains versioned, language-neutral protocol and behavior fixtures consumed by Kotlin, Swift, and Go validation surfaces. Fixture changes must preserve manifest integrity and be exercised by their owning test suites.

`CLASSIFICATIONS.json` is the authoritative active-versus-historical index. The
entries under `currentProtocolFixtures` are the P0-P4 direct-protocol corpus.
Entries under `historicalFixtureInputs` are retained only as explicitly named
pre-simplification local-layout or migration inputs; their capability, push-session,
committed-row, verbose-row, or old runtime-state vocabulary is not active wire
behavior and must not be used to preserve compatibility.

The frozen protocol authority is Go p0000 SHA-256
`5b846ea366db9022dcd817721b98897ff4d7e578cd466796a5c3e97b5221e24f`.
The frozen SQLiteNow companion SHA-256 is
`02b98af92726da39e5b93618e10a406ac608efbb6f37e04a734bef6eb3f0196c`.
Go remains the normative server authority; this directory is the single
manifest-pinned, language-neutral corpus and does not transfer that authority.

`MANIFEST.sha256` contains sorted lowercase SHA-256 records for every corpus
file except itself. Consumers must fail closed for a missing, extra, stale, or
mismatched record rather than silently selecting copied fixture literals.

The KMP implementation owns local runtime behavior. Go owns server behavior and is read-only from this repository. Fixture edits do not establish product acceptance, server authority, or lifecycle closure.

The Go and JVM corpus consumers must validate the complete manifest and parse
or execute every current `wire/` family they claim to support. A configured or
repository-local corpus root that is missing, incomplete, extra, or mismatched
is a hard test failure, never a skip or copied-literal fallback.

Fault and request-count fixtures are deterministic traces: `initial` supplies
the reducer state, `events` are applied in order, and `expected` or
`expectedTerminal` records the derived result. Query cases retain raw arrays of
values so absence, an empty value, and repeated values remain distinguishable.

Use the focused JVM fixture tests while editing contracts, then run `oversqliteComprehensive`. Live-server and platform validation remain separate lanes and require their normal prerequisites.

`localContractCandidates` classifies local-only contract candidates. These entries
are neither historical input nor current HTTP fixtures. Each declares paired pure-contract consumers,
their exact executed coverage, and explicit future runtime owners. See `local-planning/README.md`.
This classification does not grant runtime acceptance or weaken manifest fail-closed behavior.

A3-02 adds the local-only `a3-workspace` and `a3-pending` candidates and their
paired independent consumers. Their physical inventory is in the workspace
fixture and `local-planning/README.md`. Neither family changes HTTP authority or
marks the original 33 runtime declarations executed.

The 2026-10-13 library-owned work-slice amendment is governed by Go p0001 SHA-256
`c1de82aa4b879e029cdf023c4443d3b8a1645abb23f0ff0579b9b838ca509288` and the amended SQLiteNow companion SHA-256
`108ea0d97431de9c8c44fb2ec5cc1a2213dd715f7dfe245efbdaaccc475aeea6`.
`executionContract` in local-apply fixtures distinguishes internal bounded-step
yields from public completion. Work-error decoding remains historical/low-level
coverage; ordinary public calls must not expose scheduling yields.

The fresh-database completion profile permits recreation and excludes older-format
upgrade compatibility. Current-format reopen, crash recovery and replay remain
required; historical failing migration attempts retain their original status.

Device-load qualification uses one process per user; shared-process goroutine mode remains SDK stress evidence. The paired governing profiles retain all previous failed attempts and unchanged SDK, workload, verification and deadline requirements.

History-projection transport retries honor the server delta-seconds minimum delay while retaining existing attempt budgets and caller cancellation.

Writer-owned replay reservation bindings and the guarded continuation holder share one eight-entry/1,024-byte charge, with exact owner matching and an uncached capacity fallback. The earlier four-entry/512-byte writer-only charge is superseded by this combined holder.

Guarded replay-decision continuation retains fully charged primary copies, compares a fresh full context guard per writer and invalidates before business mutation.

Empty dependency traversal requires a completed zero-edge index and current physical absence proof; it preserves step weights, slice quanta, hard limits and the nonempty-graph path.

Public independent-node planning requires zero recorded edges, current physical absence and a pristine node; nonempty/partial DFS and raw kernels retain their original path. Work weights and writer quantum are unchanged.

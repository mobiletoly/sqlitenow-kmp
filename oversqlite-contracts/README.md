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

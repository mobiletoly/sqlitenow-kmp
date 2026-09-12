# Client upgrade conformance v1

This directory is the language-neutral executable evidence for the client-upgrade behavior. Consumers must verify
the artifact entries in `manifest.json` before reading any scenario. Prose specifications are maintained
separately and are not hashed by this executable corpus. Semantic integer fields are decimal strings, bytes are
lowercase hex, and absent fields are distinct from explicit `null`, zero, false, and empty values.
Raw wire files are exact UTF-8 bytes and are not normalized. Capabilities GET request files are
zero-byte bodies. The replacement POST fixture includes its fixed JSON record terminator because
the production writer emits that byte.

`values.json` records each semantic value, SQLite codec/storage result, acceptance result, and exact
outer-JSON token bytes. `limits.json` records the production bound and observed count as canonical
decimal strings. Its repetition recipes are deterministic stand-ins for large row or byte inputs;
the verifier checks their fixed hashes without allocating the complete payload.

Execute each scenario from its `initial_state`, perform `actions` and `protocol_exchanges` in order,
inject the listed `faults`, then compare semantic observations and preserved facts with `expected`.
A conforming adapter records each action only where its production operation is dispatched, each
exchange only after its exact request matches and its response passes through the production
reader, and each fault only where it is injected and its durable aftermath is inspected. The
ordered consumed identities, rather than fixture cardinalities, are the authoritative execution
record; missing, duplicate, out-of-order, or merely declared consumption fails verification.
A runtime may adapt private storage and API names, but not phase names, outcomes, protocol bytes,
identity comparison, preservation facts, or recovery behavior. Runtime evidence maps through
`invariants.json`; pending non-Go runtime slots are deliberately not Go acceptance evidence.

The Go verifier rejects missing artifacts, hash drift, JSON Schema violations, an incomplete
U0-U8/mode inventory, unknown scenario or invariant IDs, changed fixed expected observations,
dangling raw-wire references, and incorrect value/limit hashes. `go-adapter.json` binds every
scenario to exact product tests. The runner discovers each mapped test in exactly one owning
package, executes it, and requires its Go test `PASS` event; it does not accept source-text matches.
Typed and limit vectors are loaded by mapped tests and passed through the production codecs,
generation-proof parser, bounded-body readers, row/count validators, and PostgreSQL snapshot
materializer. Snapshot row-byte vectors carry deterministic padding recipes and fixed hashes; the
materializer independently proves exact-limit acceptance and ceiling-plus-one rejection without
partial session or staged-row state.

`wire/protocol-hashes.json` provides fixed, language-neutral examples for the Q39
generation-pinned request hash, generation-projected committed effects, and complete snapshot
chunk wire framing. The snapshot entry hashes one exact portable response frame; it does not add a
whole-snapshot protocol hash. The verifier independently checks every canonical byte string and Go
tests reconstruct the Q39 and projected-effect hashes through production readers.

Verify the package from the repository root:

```text
env GOTOOLCHAIN=go1.25.12 go run ./cmd/client-upgrade-conformance -root conformance/client-upgrade/v1
```

Revision 7 records review-driven fixture and execution corrections in `corrections.json`, including
the prior manifest identity, rationale, and affected invariant IDs. Every scenario names a Go
executor; the runner supplies the canonical fixture and rejects missing input consumption or
observations that differ from the fixed outcomes. Go observations are constructed independently
from production-backed executions rather than copied from fixture expectations. Revision 7 has an
exact inventory of 24 scenarios, including unknown and mismatched generation negotiation,
structured conflict retry, and cancellation before and after every U0-U8 durable boundary. It also
binds preservation labels to fresh durable-state inspection and adds the two snapshot row-byte
boundaries. Further corrections require another revision; weakening an expected result to fit an
implementation is prohibited.

Revision 8 closes the subsequent review findings without changing the 24-scenario inventory. It
adds a SQLite-level read-only barrier around application version reads, compares every claimed
request body byte-for-byte, and emits cancellation preservation only after exact durable identity,
local-only digest, old-intent, replacement-tuple, and snapshot-stage inspection.

Revision 9 closes the next two review findings without changing the 24-scenario inventory. Ready
client construction now proves that the configured synchronized-table scope and ordered keys equal
the complete compiled local contract before mutable runtime initialization. Cancellation evidence
captures the exact durable replacement tuple and target proof at each U5-U8 boundary, and a
negative evaluator regression proves that source, reason, or proof substitution is rejected.

Revision 10 retains the 24-scenario inventory and closes the subsequent three review findings.
Local-only declarations are disjoint from every synchronized contract, durable upgrade journals
carry a canonical integrity witness and are compared with the immutable plan before resume, and
migration query results are canceled and closed when the callback-scoped capability expires. The
existing negotiation, migration, replacement, cancellation, and restart scenarios map these
negative Go regressions to I01, I05-I08, and I10-I12 without weakening their portable outcomes.

Revision 11 retains the 24-scenario inventory and corrects three implementation-review findings.
U2 durably records only the completed old-generation drain and U3 separately previews the target
and enters migration, so a restart after U2 no longer depends on old-generation availability.
Migration cleanup errors are composed with primary callback, cancellation, and panic causes, and
ordinary ready clients privately clone all mutable configuration state. The portable outcomes are
unchanged except that CU-U2 now exposes its committed drain witness and CU-U3 owns the distinct
target-preview transition required by the governing lifecycle.

### Discovery redesign development cut

The v1 local value vectors now derive int64/float64 conversion for every PostgreSQL integer/float family. The retained `int16-overflow` and `int32-overflow` scenario IDs describe values accepted by the client and rejected by the narrower server destination; server coverage remains in `TestUniformNumericWire` and the uniform-numeric integration suite. `float32-local-rounding` now preserves the input binary64 value on upload; PostgreSQL float4 rounding appears in committed effects. These historical scenario names do not select client codecs.

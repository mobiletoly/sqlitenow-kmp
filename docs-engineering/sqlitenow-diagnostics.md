# SQLiteNow diagnostics

Diagnostics are an explicit runtime choice, independent of ordinary debug logging and individual SQL tracing. Share one controller between a database connection and its Oversqlite configuration:

```kotlin
val diagnostics = SqliteNowDiagnostics() // disabled
appDatabase.connectionConfig = appDatabase.connectionConfig.copy(diagnostics = diagnostics)
val syncConfig = existingSyncConfig.copy(diagnostics = diagnostics)
// Open with these configurations, then enable capture when needed.
diagnostics.setEnabled(true)
// ... perform the operation being investigated ...
diagnostics.setEnabled(false)
```

Configure the connection before opening it. Pass the same controller to Core and Oversqlite to obtain physical SQL counters as well as sync phases. Rebuild consumers after upgrading: adding configuration fields changes Kotlin data-class constructor/copy binary signatures. This addition changes no database or wire format.

An outer operation captures its setting at entry. Disabling capture does not cut an operation in half; that operation finishes its trace, and subsequent operations use the new setting. Each watch request is an operation, so an existing watch uses its captured setting until it ends/reconnects. Cached statements use the current execution scope rather than their prepare-time policy.

When disabled, diagnostics do not obtain clocks, classify SQL, aggregate counters, serialize records or call the diagnostic logger. Existing operational resource/accounting counters remain unchanged. Enabled diagnostics add timing/aggregation/output costs; the synchronous logger can block the caller. They are diagnostic measurements, not a zero-overhead benchmark.

For application-owned orchestration, `diagnostics.operation("bootstrap") { ... }` and `sqliteNowDiagnosticPhase("import") { ... }` add correlation. Use fixed developer-authored labels and numeric operational counters. Never use user input, account/source IDs, SQL values, keys, URLs, payloads or exception text as labels. Schema/table names may be included explicitly in correctness events. Generic SQL shapes redact identifiers and literals; known library responsibilities use semantic family labels. There are no raw SQL hashes.

Individual statement tracing remains a separate explicit `SqlTracePolicy`; comprehensive diagnostics do not enable it. Ordinary logger level/filtering still applies: diagnostic records use Info. A sink/formatter failure stops diagnostic emission without changing operation results, exceptions, cancellation or cleanup. A filtered or failed logger can leave an incomplete capture, which the analyzer must not interpret as success.

## Application integration

Applications can keep a separate `sqliteDiagnosticsEnabled` configuration setting, independent of ordinary debug logging, and expose the shared controller through their runtime for programmatic toggling. The library controller defaults to disabled. Pass the same controller explicitly to the connection and sync configuration. There is no library settings UI or application file sink.

## Capture and analyze

The collector uses read-only ADB commands against an already running app. It does not clear logcat, restart the app, install builds or reset a database:

```sh
python3 tools/sqlitenow_diagnostics.py capture \
  --adb /Users/pochkin/Library/Android/sdk/platform-tools/adb \
  --serial emulator-5554 --package dev.example.syncapp \
  --duration 90 --output /private/tmp/sync-diagnostic-capture

python3 tools/sqlitenow_diagnostics.py analyze \
  /private/tmp/sync-diagnostic-capture/capture.log \
  --output /private/tmp/sync-diagnostic-report
```

The capture directory must be new. Omitting duration follows until Ctrl-C. Only diagnostic records and process-change metadata are retained. Saved logger output, including ADB prefixes, and normalized JSONL can also be analyzed. Results are `events.jsonl`, `report.json` and `report.md`. Retain the JSONL/JSON when handing evidence to an agent.

## Read the report

- Operation/span IDs and parent IDs describe execution nesting. Captures have independent monotonic clocks; do not compare their timestamps as wall time.
- Inclusive duration contains nested work. Exclusive/unattributed duration subtracts the **union** of immediate child intervals, so concurrent children are not counted twice. Unattributed time still includes orchestration, uninstrumented work and diagnostic overhead.
- Attempt/slice fields identify event context; zero means outside a numbered attempt/slice or a span-wide aggregate. SQL/work metrics declare `aggregation=span`; use their span hierarchy rather than treating them as one attempt or slice.
- SQL metrics separate prepares, cache hits/misses, step calls, returned rows, reset/clear/close and failures. Root metrics include descendants; do not sum root and child metrics. A cached statement can still execute an expensive query repeatedly.
- Step calls and returned rows are not SQLite VM instructions or scanned rows. Transport duration is not server CPU. Fetch-managed decompression makes encoded wire bytes unavailable; received/decoded bytes are labeled accordingly.
- Aggregated catalog enumeration and trigger-definition work exposes repeated preservation costs without per-row logging. Work measurements are inclusive.
- `snapshot.preservation_metadata` summarizes actual catalog traversals, foreign-key metadata loads (including empty results), trigger-table validations, cache reuse/fallback/eviction and peak retained entries/bytes. `row_checks` counts row-proof attempts, including retries; `incoming_row_checks` counts applicable constraint proofs. These are observed checks, not committed rows. Reuse lasts only for the final writer; tight workspace budgets use streaming checks.
- Snapshot events distinguish expected, validated, installed-but-uncommitted and confirmed-committed counts. Table scope checks report existing expected/observed counts; logging performs no additional validation SQL.
- Work yields, retries, cancellation and failures remain distinct. Correctness events describe checks that actually ran; absence is not proof that a check passed.

Default bounds are 1,024 families per operation and 128 live spans; constructor options adjust them. Overflow groups retain totals and expose lost detail (`family_overflow_calls` counts SQL/work metric updates assigned to overflow, not distinct queries or formatter invocations). Records stream rather than retaining completed operation history. Logger messages stay within 3,000 UTF-8 bytes; larger logical events use numbered base64 fragments.

The analyzer reports missing sequences/fragments, incomplete spans, unsupported versions, malformed/duplicate records and process changes. A successful operation can still have an incomplete capture. Shared application spans spanning multiple connections report partial connection attribution; independent connection/sync operations retain their connection aliases. No release, live-server, platform or performance qualification is implied by these reports.

Tracked statements classify SQL lazily on their first enabled use and reuse the redacted descriptor across executions. Numeric action slots avoid allocating counter keys for each step/reset/cache hit. Family IDs and attribution remain capture-local; cached statement links are revoked when spans finish. Enabled collection still incurs clocks, counters, context and logger costs; an off/on/off sample reports those costs for that fixture and does not establish zero overhead or a general speedup.

Restore preflight exposes admission, capture and dependency phases. Work-budget refusal precedes the two expensive scans; unique workspace validation remains after them. `row_processing.*` work metrics are zero-duration call counters: row leases, wire parses/normalizations, compatibility text encodes, packed authoritative decodes, fresh plan/authoritative reads and optional-reuse fallback. They are attributed to their containing span; root totals include descendants. Counts measure actual calls, including retries, and are not committed rows.

Snapshot row leases retain data for one iteration only. Every validation pass still reads fresh data. The required uninstalled/unscheduled ordinal partial indexes are part of the retained-object census; older development databases must be explicitly recreated before opening this library version. No automatic migration or reset is performed.

Hash verification separates `snapshot.hash.wire_validation`, `payload_canonicalization`, `byte_encoding` and `digest_update` aggregate work. Payload canonicalization counts one call per row per verification pass; the logical-byte and hash envelopes remain distinct. Timings are inclusive and do not represent pure cryptographic CPU time.

`row_processing.typed_scalar_decode` counts scalar decode attempts and failures in instrumented row validation/reuse paths, including fresh comparison inputs. It is not a library-wide total. Typed values expire with their row iteration; binding and retained-side comparisons can reuse them. Optional holders are admitted before allocation and dropped on pressure.

Read-only snapshot regions report `snapshot.workspace.summary`: one initial durable-base load per admitted view, reuse checks, fallback and peak retained charge. After fallback, actual fresh reads remain visible through the `workspace.usage` SQL family. Transient row/provenance/preservation charges are always current. Views expire before mutations, writer or slice exits. Fixed stage, authoritative-capture and relationship SQL labels keep these costs separate from redacted generic shapes.

### Snapshot final validation

The existing `final_validation` phase contains fixed-label children, in order:
`snapshot.final.restoration`, `snapshot.final.live_verification`,
`snapshot.final.authoritative_verification`, and `snapshot.final.scope_verification`.
Durations are inclusive: the parent includes its children, SQL and row-work
aggregates overlap phase durations, and these numbers must not be summed as
exclusive CPU time. `snapshot.final.restoration_pass` counts attempted restoration
passes (including the terminating no-change pass); it is an operation aggregate,
not a per-row or per-pass record. Failed/cancelled phases close through the normal
diagnostic helper. Diagnostics remain disabled by default.

`snapshot.final.typed_projection` counts admitted direct SQLite expected-projection
comparisons; `snapshot.final.typed_projection_fallback` counts comparisons using
the existing text path when optional workspace admission is unavailable. Both are
operation work aggregates. `row_processing.expected_projection_text` exposes text
projection calls across all owners; it must be absent from an admitted final
authoritative-verification phase. Stored authoritative decoding remains required.

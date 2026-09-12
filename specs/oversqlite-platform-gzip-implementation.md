# Oversqlite request gzip: platform engines

Status: proposed implementation specification, 2026-09-23. Owner approval of this document would authorize a separate implementation pass. This document does not change the accepted p0000 wire contract or authorize staging, commit, push, release, or Go edits.

## 1. Scope and baseline

Replace the fixed-Huffman DEFLATE encoder in `library-oversqlite/src/commonMain/kotlin/dev/goquick/sqlitenow/oversqlite/OversqliteGzip.kt` with maintained platform gzip engines. Keep common Kotlin responsible for request selection, byte limits, and HTTP headers. Leave response decompression and its strict one-member checks in place unless implementation exposes a defect that needs its own decision.

The implementation starts from the accepted SQLiteNow checkpoint `bcff6eafc60e199b6c20e3abf625a4dde87bd0a4`. At drafting time the checkout is clean. The ignored SQLiteNow p0000 companion has SHA-256 `11233d28d5ba514efccfa87d98eea459263c0f2593fbccc73a032d079a9bb534`; the 76-entry corpus manifest has SHA-256 `0e64ece3dce806b815b9833dac2be9c7835888a149678db3a540c0937f722f3f`.

The sibling Go checkout stays read-only. Its HEAD is `3839514ef08186900fa72acc05e313760f725d68`, its p0000 specification has SHA-256 `5b846ea366db9022dcd817721b98897ff4d7e578cd466796a5c3e97b5221e24f`, and `swagger/two_way_sync.yaml` has SHA-256 `94940822a3ba5f4b614a7e95bde1f528ec5f463793eb4feb4fe6ee2de93c21b4`. Two existing Go tracked edits give that checkout status SHA-256 `effa3a375f816ca270b3dccb3ba0012d3044539dbd400fd2dd8fc79bae22f227` and tracked-diff SHA-256 `8da8ac5ee0e0f2e9f9958b83ff4440fba5c109034730c96446413cb8351ad08f`. Verify these identities before implementation; stop on drift or overlap.

The governing rules are SQLiteNow p0000 section 4.5, Go p0000 section 9.1, and `oversqlite-contracts/wire/compression.json`. The contract permits target-specific gzip engines. This pass changes neither the 2,048-byte threshold nor the rule that a client sends gzip only when the complete encoded body is strictly smaller than the compact identity body. A 4 KiB or entropy heuristic belongs in a later paired-contract revision.

Git ignores this file under `/specs/`. A handoff in this checkout may point to its absolute path. For another checkout or task without this file, supply the exact document and its SHA-256; a path or checkpoint commit alone cannot convey this specification. Record the document hash with the implementation baseline.

## 2. Required behavior

Common Kotlin must choose the request representation from complete compact UTF-8 JSON bytes:

1. Reject a decoded body above its admitted decoded-body limit before compression or network activity.
2. Send identity for bodies shorter than 2,048 bytes.
3. For bodies of at least 2,048 bytes, ask the platform engine for one complete gzip member. Retain at most `min(decodedSize - 1, encodedHardLimit)` result bytes. If the next output byte would exceed this inclusive cap, stop that compression attempt and choose identity. Reaching the cap is not an abort: a complete member, including its trailer, that ends at the cap remains eligible for gzip selection.
4. Send gzip only after the engine finishes, the result fits its admitted encoded limit, and `gzipSize < decodedSize`. A tie selects identity. The selected bytes, `Content-Encoding`, and optional `Content-Length` must agree.
5. Preserve the decoded JSON, canonical request hash, bundle hash, source ID, and frozen outbox state. Compressor output bytes may differ by target; no protocol hash may depend on the gzip bitstream.

Pass the decoded limit and encoded ceiling to preparation as explicit inputs independent of `decoded.size`. Connect and snapshot-session creation use the existing 65,536-byte control-body limit. Push uses the opening-time admitted `max_push_decoded_bytes`, which the push workflow already uses as its hard request limit; the local transfer target remains a soft batching target. For each of these three endpoints, set `encodedHardLimit` equal to its applicable decoded limit, matching the Go receiver's encoded ceiling. Thread the admitted push request limit into `OversqliteRemoteApi.push` rather than inferring it from the body's length or from a response-only parameter. Replace the current `setBoundedJsonBody` call pattern, which passes `decoded.size` as its own limit. Both admission comparisons are inclusive: identity may equal the encoded ceiling, while gzip must still be smaller than decoded bytes. Reject a decoded body one byte over its limit before invoking the compressor or HTTP client.

The client must create the selected byte array before it constructs the Ktor `HttpRequestBuilder`. The current `setBoundedJsonBody` call sites are connect, push, and snapshot-session creation in `OversqliteRemoteApi.kt`. Compute one selected body per logical request and reuse it for any retry of that request. Browser compression requires a suspending preparation path; move byte preparation outside the non-suspending builder block rather than launching background work from the builder.

Keep three outcomes distinct. A cap overflow is a normal non-beneficial result and selects identity; it is neither a caller cancellation nor an engine failure. Caller cancellation stops compression, releases resources, and propagates `CancellationException` through the public client API. A missing engine or genuine engine error stops before HTTP and produces an explicit, non-retryable local compression or unsupported-environment failure. These failure paths preserve durable push state and source-ID sequencing. Do not send identity as a silent fallback after an engine failure: the client cannot establish whether a body above the threshold would have compressed. A CORS failure after a prepared gzip request retains the existing transport/retry behavior.

The client must keep output buffering bounded during compression. Do not allocate an unbounded `ByteArrayOutputStream`, use `Response(...).arrayBuffer()` for an unchecked browser stream, or finish a large gzip body before applying the output cap. Release compressors, readers, writers, native streams, and pinned buffers on success, cap abort, error, and cancellation.

## 3. Platform engines

| Target | Engine | Adapter requirements |
| --- | --- | --- |
| JVM and Android | `java.util.zip.GZIPOutputStream` | Write to a capped output sink. Finish one stream before selecting bytes. Close the stream on every path. Check caller cancellation between input chunks and after finishing. |
| Apple and Linux Native | zlib `deflateInit2` with gzip window bits `15 + 16` | Stream into a capped buffer. Call `deflateEnd` after success or failure. Check caller cancellation between input and output chunks. Keep the existing native zlib decoder separate. |
| Browser JS and Wasm JS | Web `CompressionStream("gzip")` | Use each Kotlin target's JS interop adapter. Drain the output while feeding input, count bytes as they arrive, stop on cap overflow, and return one complete member. On caller cancellation, cancel both stream directions and their feeding/draining tasks. |
| Node.js JS target | Global `CompressionStream("gzip")` on the repository's Node 22.15.0 test runtime | Exercise the same output cap and cancellation rules as browser JS. Keep a Node-only fallback out of browser bundles; add one only if a supported Node runtime lacks the API. |

Probe `CompressionStream` before an eligible request. If the runtime lacks it, fail before sending; do not relabel an above-threshold request as identity. Test the probe in browser JS, Wasm JS, and Node. The implementation may share a JavaScript helper between those targets if packaging tests prove that browser artifacts contain no Node-only import. Kotlin/JS and Kotlin/Wasm interop declarations may differ.

Do not use Ktor `ContentEncoding` as the request compressor for this pass. The inspected Ktor 3.6.0 web and POSIX `GZipEncoder` implementations delegate to identity, and a send-time plugin does not give the common selection code a bounded final byte array before transmission. Retain the existing response-encoding boundary: raw transports enforce encoded and decoded limits and one-member framing; Fetch owns browser response decompression and encoded-byte visibility.

## 4. Implementation boundaries

- Keep the threshold and final selection function in `commonMain`. Replace `encodeSingleMemberGzip`, its match finder, fixed-Huffman tables, bit writer, CRC writer, and accumulator with a narrow platform compression interface. A suspending interface may return `null` only when the next output byte would exceed the inclusive cap. It returns a member that completes exactly at the cap and propagates caller `CancellationException` after cleanup; genuine engine failures use a separate non-retryable local error.
- Implement JVM/Android, Native, JS, and Wasm adapters without a second gzip algorithm in repository source. Share thin adapters only where source-set and packaging rules permit it.
- Prepare bytes before the three request builders named above. Send the exact selected `ByteArray` with `Content-Encoding: gzip` only for gzip. Keep `Accept-Encoding` response behavior and watch identity encoding unchanged.
- Keep the existing raw response decoders and Fetch response path unchanged. Test code may build gzip fixtures through a standard library; it must not preserve the production fixed-Huffman encoder as a fixture helper.
- Leave Go source, Go OpenAPI, both p0000 specifications, and the corpus unchanged. If platform engines cannot satisfy an existing active fixture or hard limit, stop and report the conflict before editing the contract.

## 5. Tests and evidence

Focused tests must execute the runtime selection path on each claimed target, not only a JVM fixture-side `GZIPOutputStream` calculation. Cover 2,047, 2,048, and 2,049 decoded bytes; the 4,096-byte incompressible case; an injected complete member ending exactly at the output cap; an injected next byte that exceeds the cap; exact `Content-Encoding`; and absence of a request on engine failure. Test a caller-cancelled operation separately: it must propagate `CancellationException`, release compressor resources, and leave push/outbox/source-ID state unchanged. Check the three endpoint-to-limit mappings, then exercise the shared preparer with injected bodies at each distinct decoded limit and one byte above it; the over-limit case must stop before compression. Check encoded exact-cap success and cap-plus-one identity fallback. Inflate each selected gzip request with an independent decoder or send it to the pinned Go server. Assert one member, no trailing bytes, decoded-body equality, and valid server acceptance. Compare semantics and sizes across targets, not gzip byte-for-byte equality.

Run the active `wire/compression.json` fixture family through each applicable consumer. Keep its expected identity/gzip cases and the sorted 76-entry corpus manifest unchanged unless a real engine result exposes a contract conflict. Retain raw-response malformed, concatenated, oversized, and Fetch-decoded-limit tests as regression guards. Browser tests must prove precompressed request bytes and CORS `Content-Encoding` preflight on the network, including JS and Wasm; Node must send a gzip body that the Go server decodes.

Refresh the seven-workload identity/gzip artifact. Record decoded size, selected on-wire size, ratio, encoding, decoded semantic/hash equality, engine provenance, and target/runtime. Refresh the external browser Fetch capture because request gzip bytes and compressor provenance changed. Keep previous measurements as superseded historical evidence. Rerun the one-minute large-pull/snapshot memory gate unless an explicit dependency audit shows that the request-only change cannot reach it; retain its 128 MiB heap and 300 MiB RSS thresholds.

## 6. Qualification sequence

1. Capture paired checkout identities, ignored-spec hashes, corpus manifest, staged state, and the accepted checkpoint. Preserve unrelated drift. Apply the source edit only after the baseline matches.
2. Finish all platform adapters and request-builder changes before freezing. During development, run focused compressor and HTTP tests on each affected target, the 76-entry manifest integrity check, and `oversqliteCrossRepoConformance`. Use narrow live-server smoke tests for changed request paths. Fix failures in this phase; do not run the platform, Swift, or heavy aggregates as a substitute for these checks.
3. Freeze the revised KMP candidate after the focused tests pass. Run `oversqlitePlatformAll` across Android, JVM, iOS simulator, macOS arm64, JS browser/Node, and Wasm browser once for that candidate. This aggregate omits the configured Linux targets. Run focused `:library-oversqlite:linuxX64Test` and `:library-oversqlite:linuxArm64Test` request-compression tests on matching Linux runners; a cross-compile alone is not runtime evidence. If a matching runner is unavailable, record that target as unqualified rather than claiming full platform coverage.
4. Start a fresh isolated PostgreSQL database and pinned Go server. Run `oversqliteRealserverAll`, then `swiftHybridSupportRealserverGate`. The Swift live gate already depends on `swiftHybridSupportGate` and `oversqliteComprehensive`; do not repeat those tasks as separate qualification lanes when the recorded run proves their execution.
5. Start a second fresh isolated PostgreSQL database. Run the mandatory `oversqliteRealserverAllHeavy` aggregate after the last source change, with its constituent families serial against shared database resources. The ordinary JS-browser live lane supplies browser evidence; the heavy aggregate has no JS-browser-heavy task.
6. Verify postflight identities, artifacts, manifest, and `git diff --check`. Append changed, superseded, reused, failed, and passed lanes to the KMP ledger. Treat an aggregate timeout as inconclusive until its owning family runs in isolation.

For each Gradle gate, record its leaf test tasks and outcomes. An `UP-TO-DATE`, `FROM-CACHE`, or skipped leaf does not prove that an invalidated test ran against this candidate and environment. Execute such a leaf with fresh inputs or a targeted rerun, then record the result. Do not use blanket `--rerun-tasks` on the root aggregates to compensate for one skipped leaf. Reuse a prior leaf only when the ledger pins the same inputs, runtime, server, and database conditions and explains why this change cannot affect it.

If a frozen run fails, stop the campaign. Diagnose a timeout or failure in its owning family under isolated conditions; do not retry the unchanged aggregate to seek a pass. A source fix creates a new candidate. Compare the fix with the last freeze, record affected code paths and tasks, rerun their focused checks, and refreeze. A common selector or request-builder change invalidates every platform request lane and its relevant live lanes; a target-only adapter change invalidates that target's platform and live lanes, plus Swift-facing evidence if the Native adapter or shared Swift path changed. Refresh browser capture, seven-workload measurements, and memory evidence only when their producing inputs changed. Keep passed lanes whose inputs and environment remain identical, with a ledger explanation. Run the mandatory final heavy aggregate after the last fix; do not repeat unaffected platform, ordinary live, or Swift aggregates merely to rebuild a full campaign label.

The last accepted candidate took about 9 minutes for the platform aggregate, 17 minutes for ordinary live, 8 minutes for Swift live, and 14 minutes for heavy. These times suggest roughly 50 minutes for the first broad campaign, plus focused tests, capture, and setup. They are neither a timeout nor a pass criterion. Reuse unchanged Go and generator evidence with an input-dependency note; do not rerun their full campaigns. Do not claim the old author qualification or owner acceptance for the revised candidate.

## 7. Completion criteria and boundaries

The implementation is ready for a new acceptance decision when common source contains no custom DEFLATE or gzip-member writer; every supported request target applies the same threshold, size comparison, and hard limits; live receivers accept each target's gzip bytes; Fetch capture proves browser request compression; the affected qualification lanes and mandatory final heavy gate pass; and the ledger pins the revised candidate and evidence.

The checkpoint commit remains the accepted historical candidate. This work creates a new candidate. Independent review, owner acceptance, staging, commit, push, release, and delivery remain separate decisions. This document authorizes none of them.

## API references checked for this draft

- Java 17 [`GZIPOutputStream`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/zip/GZIPOutputStream.html)
- zlib [`deflateInit2` gzip window bits](https://www.zlib.net/manual.html)
- Web [Compression Streams API](https://developer.mozilla.org/en-US/docs/Web/API/Compression_Streams_API)
- Node [Web Streams `CompressionStream`](https://nodejs.org/api/webstreams.html)
- Kotlin [JS interop](https://kotlinlang.org/docs/js-interop.html) and [Wasm JS interop](https://kotlinlang.org/docs/wasm-js-interop.html)

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

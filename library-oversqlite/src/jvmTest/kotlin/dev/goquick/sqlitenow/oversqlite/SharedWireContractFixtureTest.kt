package dev.goquick.sqlitenow.oversqlite

import java.io.ByteArrayOutputStream
import java.security.MessageDigest
import java.util.zip.CRC32
import java.util.zip.GZIPOutputStream
import java.util.zip.Inflater
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlin.io.path.isDirectory
import kotlin.io.path.isRegularFile
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SharedWireContractFixtureTest {
    private val json = Json { ignoreUnknownKeys = false }

    @Test
    fun everyCurrentWireFixtureIsManifestPinnedAndValidJson() {
        val wireRoot = oversqliteContractFixture("wire")
        assertTrue(wireRoot.isDirectory())
        val expected = setOf(
            "compact-rows.json",
            "compression.json",
            "errors.json",
            "faults.json",
            "limits.json",
            "request-counts.json",
        )
        val actual = wireRoot.listDirectoryEntries("*.json").map { it.fileName.toString() }.toSet()
        assertEquals(expected, actual)
        actual.forEach { name ->
            json.parseToJsonElement(oversqliteContractFixture("wire/$name").readText()).jsonObject
        }
    }

    @Test
    fun replayMismatchAndRemovedRouteDecisionsAreExplicit() {
        val errors = objectFixture("wire/errors.json")
        assertEquals(
            listOf(
                "method_and_authentication",
                "source_and_generation_headers",
                "media_type_and_content_encoding",
                "encoded_limit",
                "gzip_decode_and_decoded_limit",
                "strict_json_and_envelope",
                "compact_tuple_logical_validation_and_canonical_request_hash",
                "committed_history_replay",
                "source_lifecycle_sequence_initialization_conflict_application_commit",
            ),
            errors.getValue("pushValidationPrecedence").jsonArray.map { it.jsonPrimitive.content },
        )
        val statusByCode = mapOf(
            "method_not_allowed" to 405,
            "authentication_failed" to 401,
            "malformed_schema_generation_fingerprint" to 400,
            "unsupported_media_type" to 415,
            "unsupported_content_encoding" to 415,
            "encoded_body_too_large" to 413,
            "decoded_body_too_large" to 413,
            "invalid_gzip" to 400,
            "invalid_request" to 400,
            "push_invalid" to 400,
            "push_replay_mismatch" to 409,
            "push_conflict" to 409,
        )
        val errorCases = errors.getValue("cases").jsonArray.map { it.jsonObject }
        errorCases.forEach { case ->
            assertEquals(
                statusByCode.getValue(case.getValue("error").jsonPrimitive.content),
                case.getValue("status").jsonPrimitive.int,
                case.getValue("name").jsonPrimitive.content,
            )
        }
        assertTrue(
            errorCases.map { it.getValue("name").jsonPrimitive.content }.containsAll(
                listOf(
                    "empty-content-encoding",
                    "duplicate-content-encoding",
                    "encoding-wins-over-encoded-overflow",
                    "connect-missing-has-local-pending-rows",
                    "connect-null-has-local-pending-rows",
                    "snapshot-source-replacement-null",
                    "snapshot-source-replacement-missing-reason",
                    "hidden-column-after-valid-canonical-hash",
                ),
            ),
        )
        val compact = objectFixture("wire/compact-rows.json")
        val generation = compact.getValue("generation").jsonObject
        val hidden = errorCases.single {
            it.getValue("name").jsonPrimitive.content == "hidden-column-after-valid-canonical-hash"
        }
        val hiddenRowsCase = hidden.getValue("compactRowsCase").jsonPrimitive.content
        val hiddenGroups = compact.getValue("invalidCases").jsonArray.map { it.jsonObject }
            .single { it.getValue("name").jsonPrimitive.content == hiddenRowsCase }
            .getValue("rows").jsonArray
        val hashRows = expandHashRows(generation, hiddenGroups)
        val requestEnvelope = buildJsonObject {
            put("generation_fingerprint", generation.getValue("fingerprint"))
            put("generation_id", generation.getValue("id"))
            put("rows", JsonArray(hashRows))
            put("type", JsonPrimitive("oversync.generation-pinned-push-request.v1"))
        }
        assertEquals(
            hidden.getValue("canonicalRequestHash").jsonPrimitive.content,
            fixtureSha256Hex(canonicalizeJsonElement(requestEnvelope).encodeToByteArray()),
        )
        assertTrue(runCatching { expandCompactRows(generation, hiddenGroups, snapshot = false) }.isFailure)
        val mismatch = errors.getValue("cases").jsonArray
            .map { it.jsonObject }
            .single { it.getValue("name").jsonPrimitive.content == "replay-hash-mismatch-after-valid-hash" }
        assertEquals("409", mismatch.getValue("status").jsonPrimitive.content)
        assertEquals("push_replay_mismatch", mismatch.getValue("error").jsonPrimitive.content)

        val traces = objectFixture("wire/request-counts.json").getValue("traces").jsonArray
        traces.map { it.jsonObject }.forEach { trace ->
            val activeRequests = trace["requests"] as? JsonArray ?: JsonArray(emptyList())
            activeRequests.forEach { request ->
                val value = request.jsonPrimitive.content
                assertFalse(value.contains("capabilities"), "active trace requests removed capabilities route")
                assertFalse(value.contains("push-sessions"), "active trace requests removed push-session route")
                assertFalse(value.contains("committed-bundles"), "active trace requests removed committed-row route")
            }
        }
    }

    @Test
    fun compactRowsExecuteTupleAndSnapshotRules() {
        val fixture = objectFixture("wire/compact-rows.json")
        val generation = fixture.getValue("generation").jsonObject
        fixture.getValue("equivalenceCases").jsonArray.map { it.jsonObject }.forEach { case ->
            val snapshot = case["snapshot"]?.jsonPrimitive?.boolean == true
            val expanded = expandCompactRows(generation, case.getValue("compactRows").jsonArray, snapshot)
            case["expectedExpandedOperations"]?.jsonArray?.let { expected ->
                assertEquals(expected.map { it.jsonPrimitive.content }, expanded.map { it.getValue("op").jsonPrimitive.content })
            }
            case["expectedOrdinals"]?.jsonArray?.let { expected ->
                assertEquals(expected.map { it.jsonPrimitive.long }, expanded.indices.map(Int::toLong))
            }
            case["expectedExpandedRows"]?.jsonArray?.let { expected ->
                assertEquals(
                    canonicalizeJsonElement(expected),
                    canonicalizeJsonElement(JsonArray(expanded)),
                    case.getValue("name").jsonPrimitive.content,
                )
            }
            case["hashDomain"]?.let {
                assertEquals("oversync.generation-pinned-push-request.v1", it.jsonPrimitive.content)
            }
            case["snapshotLogicalHashDomain"]?.let {
                assertEquals("oversync.snapshot-logical-row.v1", it.jsonPrimitive.content)
            }
        }
        fixture.getValue("invalidCases").jsonArray.map { it.jsonObject }.forEach { case ->
            val rejected = runCatching {
                expandCompactRows(
                    generation,
                    case.getValue("rows").jsonArray,
                    case["snapshot"]?.jsonPrimitive?.boolean == true,
                )
            }.isFailure
            assertTrue(rejected, case.getValue("name").jsonPrimitive.content)
        }
    }

    @Test
    fun compressionCasesExecuteThresholdAndFramingDecisions() {
        val fixture = objectFixture("wire/compression.json")
        val threshold = fixture.getValue("thresholdDecodedBytes").jsonPrimitive.int
        fixture.getValue("selectionCases").jsonArray.map { it.jsonObject }.forEach { case ->
            val body = deterministicBody(
                case.getValue("encodedBodyBytes").jsonPrimitive.int,
                case.getValue("payload").jsonPrimitive.content,
            )
            val compressed = gzip(body)
            val selected = if (body.size >= threshold && compressed.size < body.size) "gzip" else "identity"
            assertEquals(case.getValue("selectedEncoding").jsonPrimitive.content, selected, case.getValue("name").jsonPrimitive.content)
            case["gzipStrictlySmaller"]?.let {
                assertEquals(it.jsonPrimitive.boolean, compressed.size < body.size, case.getValue("name").jsonPrimitive.content)
            }
        }
        fixture.getValue("receiverCases").jsonArray.map { it.jsonObject }
            .filter { it["applicability"]?.jsonPrimitive?.content in setOf("raw_transport", "all") }
            .forEach { case ->
                assertEquals(
                    case.getValue("expectedError").jsonPrimitive.content,
                    executeReceiverCase(case),
                    case.getValue("name").jsonPrimitive.content,
                )
            }
    }

    @Test
    fun limitFaultAndRequestCountCasesExecuteTheirInvariants() {
        val limits = objectFixture("wire/limits.json").getValue("cases").jsonArray.map { it.jsonObject }
            .associateBy { it.getValue("name").jsonPrimitive.content }
        assertLimitOutcomes(limits)
        assertSnapshotLogicalBytesArePaginationIndependent(limits.getValue("snapshot-logical-total-pagination-independent"))

        val faultFixture = objectFixture("wire/faults.json")
        assertEquals(3, faultFixture.getValue("formatVersion").jsonPrimitive.int)
        faultFixture.getValue("cases").jsonArray.map { it.jsonObject }.forEach { case ->
            val actual = reduceFaultTrace(
                case.getValue("operation").jsonPrimitive.content,
                case.getValue("initial").jsonObject,
                case.getValue("events").jsonArray,
            )
            case.getValue("expected").jsonObject.forEach { (key, expected) ->
                assertEquals(expected, actual[key], "${case.getValue("name").jsonPrimitive.content}: $key")
            }
        }

        val requestFixture = objectFixture("wire/request-counts.json")
        assertEquals(3, requestFixture.getValue("formatVersion").jsonPrimitive.int)
        val traces = requestFixture.getValue("traces").jsonArray.map { it.jsonObject }
        val names = traces.map { it.getValue("name").jsonPrimitive.content }.toSet()
        assertTrue(names.containsAll(setOf("pull-defaults", "watch-default", "snapshot-chunk-defaults", "watch-burst")))
        traces.forEach { trace ->
            val result = reduceRequestTrace(trace.getValue("initial").jsonObject, trace.getValue("events").jsonArray)
            val requestSummaries = result.requests.map { it.summary }
            assertEquals(trace.getValue("requests").jsonArray.map { it.jsonPrimitive.content }, requestSummaries)
            trace["sequentialExchangeCount"]?.let {
                assertEquals(result.requests.size, it.jsonPrimitive.int, trace.getValue("name").jsonPrimitive.content)
            }
            trace["replayIsExact"]?.let {
                assertTrue(
                    it.jsonPrimitive.boolean && result.requests.size >= 2 &&
                        result.requests[0] == result.requests[1] &&
                        result.requests[0].sourceId.isNotEmpty() && result.requests[0].body.isNotEmpty(),
                )
            }
            trace["expectedTerminal"]?.jsonObject?.forEach { (key, expected) ->
                assertEquals(expected, result.terminal[key], "${trace.getValue("name").jsonPrimitive.content}: $key")
            }
            trace["forbiddenRequests"]?.jsonArray?.forEach { forbidden ->
                assertFalse(requestSummaries.contains(forbidden.jsonPrimitive.content))
            }
            requestSummaries.forEach {
                assertFalse(it.contains("capabilities") || it.contains("push-sessions") || it.contains("committed-bundles"))
            }
        }
        requestFixture.getValue("queryCases").jsonArray.map { it.jsonObject }.forEach(::assertQueryCase)
    }

    private fun assertLimitOutcomes(limits: Map<String, JsonObject>) {
        fun JsonObject.number(name: String) = getValue(name).jsonPrimitive.int
        limits.getValue("soft-target-first-row-progress").also {
            assertTrue(it.number("firstRowRequestBytes") > it.number("localPushTargetBytes"))
            assertTrue(it.number("firstRowRequestBytes") <= it.number("maxPushDecodedBytes"))
        }
        limits.getValue("row-exceeds-logical-limit").also {
            assertEquals(it.number("maxRowDecodedBytes") + 1, it.number("firstRowLogicalBytes"))
        }
        limits.getValue("first-row-exceeds-hard-request-limit").also {
            assertEquals(it.number("maxPushDecodedBytes") + 1, it.number("firstRowRequestBytes"))
        }
        limits.getValue("envelope-exceeds-hard-limit").also {
            assertEquals(it.number("maxPushDecodedBytes") + 1, it.number("emptyEnvelopeBytes"))
        }
        limits.getValue("indivisible-pull-fits-admitted-limit").also {
            assertEquals(it.number("maxPullDecodedBytes"), it.number("oneBundleResponseBytes"))
        }
        limits.getValue("indivisible-pull-rejected-before-publish").also {
            assertEquals(it.number("maxPullDecodedBytes") + 1, it.number("oneBundleResponseBytes"))
        }
        limits.getValue("snapshot-row-rejected-before-session").also {
            assertEquals(it.number("maxSnapshotDecodedBytesPerChunk") + 1, it.number("oneRowChunkBytes"))
            assertEquals(it.number("oneRowChunkBytes"), snapshotBodyAtSize(it.number("oneRowChunkBytes"), hasMore = false).size)
            assertFalse(it.getValue("sessionPublished").jsonPrimitive.boolean)
        }
        limits.getValue("snapshot-one-row-retry").also {
            assertTrue(it.number("requiredDecodedBodyBytes") > it.number("requestedMaxBytes"))
            assertEquals(it.number("serverMaxBytes"), it.number("requiredDecodedBodyBytes"))
            assertEquals(it.number("requiredDecodedBodyBytes"), snapshotBodyAtSize(it.number("requiredDecodedBodyBytes"), hasMore = false).size)
            assertEquals(1, it.number("retryMaxRows"))
        }
        limits.getValue("snapshot-one-row-retry-over-hard-max").also {
            assertEquals(it.number("serverMaxBytes") + 1, it.number("requiredDecodedBodyBytes"))
            assertEquals(it.number("requiredDecodedBodyBytes"), snapshotBodyAtSize(it.number("requiredDecodedBodyBytes"), hasMore = false).size)
        }
    }

    private fun snapshotBodyAtSize(target: Int, hasMore: Boolean): ByteArray {
        for (padding in 0..target) {
            val payload = buildJsonObject {
                put("id", JsonPrimitive("u1"))
                put("name", JsonPrimitive("A".repeat(padding)))
            }
            val key = JsonArray(listOf(JsonPrimitive("u1")))
            val logicalRow = JsonArray(
                listOf(JsonPrimitive("i"), JsonPrimitive(0), key, JsonPrimitive(1), payload),
            )
            val compactRow = JsonArray(
                listOf(JsonPrimitive("i"), key, JsonPrimitive(1), payload),
            )
            val groups = JsonArray(
                listOf(JsonArray(listOf(JsonPrimitive(0), JsonPrimitive(1), JsonArray(listOf(compactRow))))),
            )
            val body = buildJsonObject {
                put("rows", groups)
                put("next_row_ordinal", JsonPrimitive(2))
                put("logical_byte_count", JsonPrimitive(canonicalizeJsonElement(logicalRow).encodeToByteArray().size))
                put("has_more", JsonPrimitive(hasMore))
            }.toString().encodeToByteArray()
            if (body.size == target) return body
            if (body.size > target) error("snapshot body size skipped target $target at ${body.size} bytes")
        }
        error("snapshot body did not reach target $target")
    }

    private fun assertSnapshotLogicalBytesArePaginationIndependent(case: JsonObject) {
        val rows = case.getValue("rows").jsonArray
        val totals = case.getValue("paginationPlans").jsonArray.map { planElement ->
            var offset = 0
            var total = 0
            planElement.jsonArray.forEach { countElement ->
                val count = countElement.jsonPrimitive.int
                rows.subList(offset, offset + count).forEach { row ->
                    total += canonicalizeJsonElement(row).encodeToByteArray().size
                }
                offset += count
            }
            assertEquals(rows.size, offset)
            total
        }
        assertTrue(totals.isNotEmpty() && totals.all { it == totals.first() } && totals.first() > 0)
    }

    private data class ContractTraceRequest(
        val summary: String,
        val sourceId: String = "",
        val sourceBundleId: Long = 0,
        val body: String = "",
    )

    private fun frozenRequest(initial: JsonObject, key: String, summary: String): ContractTraceRequest? {
        val value = initial[key] ?: return null
        val frozen = value.jsonObject
        val request = ContractTraceRequest(
            summary = summary,
            sourceId = frozen.getValue("sourceId").jsonPrimitive.content,
            sourceBundleId = frozen.getValue("sourceBundleId").jsonPrimitive.long,
            body = frozen.getValue("body").jsonPrimitive.content,
        )
        require(request.sourceId.isNotEmpty() && request.sourceBundleId > 0 && request.body.isNotEmpty())
        json.parseToJsonElement(request.body)
        return request
    }

    private fun reduceFaultTrace(operation: String, initial: JsonObject, events: JsonArray): Map<String, JsonElement> {
        val state = mutableMapOf<String, JsonElement>(
            "committed" to JsonPrimitive(false),
            "requestCount" to JsonPrimitive(0),
            "running" to JsonPrimitive(true),
            "exactReplay" to JsonPrimitive(false),
            "mutableEffects" to JsonPrimitive(0),
        )
        listOf("checkpoint", "initializationId", "sessionPublished").forEach { key ->
            initial[key]?.let { state[key] = it }
        }
        val frozenRequest = frozenRequest(initial, "frozenRequest", "POST /sync/push")
        val originalFrozenRequest = frozenRequest
        val requestLog = mutableListOf<ContractTraceRequest>()
        fun requestForOperation() = when (operation) {
            "push" -> requireNotNull(frozenRequest) { "push fault has no frozen request" }
            "pull" -> ContractTraceRequest("GET /sync/pull")
            "snapshot_create" -> ContractTraceRequest("POST /sync/snapshot-sessions")
            else -> error("unknown fault operation $operation")
        }
        fun recordRequest() {
            requestLog += requestForOperation()
            state["requestCount"] = JsonPrimitive(requestLog.size)
        }
        events.forEach { eventElement ->
            val event = eventElement.jsonPrimitive.content
            when {
                event == "process_death" -> state["running"] = JsonPrimitive(false)
                event == "request_sent" -> recordRequest()
                event in setOf("transport_interrupted", "response_lost", "replay_lookup", "replay_return") -> Unit
                event == "server_commit" -> {
                    state["committed"] = JsonPrimitive(true)
                    state["mutableEffects"] = JsonPrimitive(state.getValue("mutableEffects").jsonPrimitive.int + 1)
                }
                event == "retry_exact" -> {
                    recordRequest()
                    state["exactReplay"] = JsonPrimitive(requestLog.size >= 2 && requestLog.first() == requestLog.last())
                }
                event == "restart" -> state["running"] = JsonPrimitive(true)
                event.startsWith("terminal:") -> {
                    state["terminalError"] = JsonPrimitive(event.removePrefix("terminal:"))
                    state["running"] = JsonPrimitive(false)
                }
                event.startsWith("recover:") -> {
                    state["recovery"] = JsonPrimitive(event.removePrefix("recover:"))
                    state["running"] = JsonPrimitive(false)
                }
                event == "retry_tick" && state.getValue("running").jsonPrimitive.boolean -> recordRequest()
                event == "retry_tick" -> Unit
                else -> error("unexecuted fault trace event $event")
            }
        }
        if (frozenRequest != null) {
            state["frozenRequest"] = JsonPrimitive(frozenRequest == originalFrozenRequest)
        }
        return state
    }

    private data class RequestTraceResult(
        val requests: List<ContractTraceRequest>,
        val terminal: Map<String, JsonElement>,
    )

    private fun reduceRequestTrace(initial: JsonObject, events: JsonArray): RequestTraceResult {
        val requests = mutableListOf<ContractTraceRequest>()
        val frozenPush = frozenRequest(initial, "frozenPush", "POST /sync/push")
        fun appendRequest(summary: String) {
            requests += ContractTraceRequest(summary)
        }
        val terminal = mutableMapOf<String, JsonElement>(
            "running" to JsonPrimitive(true),
            "checkpoint" to (initial["checkpoint"] ?: JsonPrimitive(0)),
            "activePulls" to JsonPrimitive(0),
            "trailingPulls" to JsonPrimitive(0),
        )
        events.forEach { eventElement ->
            when (val event = eventElement.jsonPrimitive.content) {
                "discover" -> appendRequest("GET /sync/generations/{generation_id}")
                "connect" -> appendRequest("POST /sync/connect")
                "sync" -> requests += frozenPush ?: ContractTraceRequest("GET /sync/pull")
                "push_complete" -> appendRequest("GET /sync/pull")
                "push_response_lost" -> Unit
                "retry_exact_push" -> requests += requireNotNull(frozenPush) { "exact push retry has no frozen request" }
                "pull_complete" -> {
                    if (requests.lastOrNull()?.summary == "POST /sync/push") appendRequest("GET /sync/pull")
                    terminal["running"] = JsonPrimitive(false)
                }
                "pull" -> appendRequest("GET /sync/pull")
                "watch" -> appendRequest("GET /sync/watch")
                "snapshot_chunk" -> appendRequest("GET /sync/snapshot-sessions/{snapshot_id}")
                else -> when {
                    event.startsWith("pull_has_more:") -> appendRequest("GET /sync/pull")
                    event.startsWith("pull_complete:") -> {
                        terminal["checkpoint"] = JsonPrimitive(event.substringAfter(':').toLong())
                        terminal["running"] = JsonPrimitive(false)
                        terminal["activePulls"] = JsonPrimitive(0)
                        terminal["trailingPulls"] = JsonPrimitive(0)
                    }
                    event.startsWith("watch_burst:") -> {
                        val eventCount = event.substringAfter(':').toInt()
                        require(eventCount >= 0)
                        if (eventCount > 0) {
                            appendRequest("GET /sync/pull")
                            terminal["activePulls"] = JsonPrimitive(1)
                        }
                        if (eventCount > 1) {
                            appendRequest("GET /sync/pull")
                            terminal["trailingPulls"] = JsonPrimitive(1)
                        }
                    }
                    else -> error("unexecuted request trace event $event")
                }
            }
        }
        return RequestTraceResult(requests, terminal)
    }

    private fun assertQueryCase(case: JsonObject) {
        val defaults = mutableMapOf(
            "after_bundle_seq" to "0",
            "max_bundles" to "server_default",
            "target_bundle_seq" to "0",
            "after_row_ordinal" to "0",
            "max_rows" to "server_default",
            "max_bytes" to "server_default",
        )
        val names = when (case.getValue("operation").jsonPrimitive.content) {
            "pull" -> listOf("after_bundle_seq", "max_bundles", "target_bundle_seq")
            "watch" -> listOf("after_bundle_seq")
            "snapshot" -> listOf("after_row_ordinal", "max_rows", "max_bytes")
            else -> error("unknown query operation")
        }
        val query = case.getValue("query").jsonObject
        var accepted = true
        names.forEach { name ->
            val values = query[name]?.jsonArray
            if (values != null) {
                if (values.size != 1 || values.single().jsonPrimitive.content.isEmpty()) {
                    accepted = false
                } else {
                    defaults[name] = values.single().jsonPrimitive.content
                }
            }
        }
        val expected = case.getValue("expected").jsonObject
        assertEquals(expected.getValue("accepted").jsonPrimitive.boolean, accepted, case.getValue("name").jsonPrimitive.content)
        if (accepted) {
            expected.filterKeys { it != "accepted" }.forEach { (name, value) ->
                assertEquals(value.jsonPrimitive.content, defaults[name], case.getValue("name").jsonPrimitive.content)
            }
        }
    }

    @Test
    fun retainedLegacyVocabularyIsExplicitlyHistoricalInput() {
        val classifications = objectFixture("CLASSIFICATIONS.json")
        val historical = classifications.getValue("historicalFixtureInputs").jsonArray.map { it.jsonObject }
        assertTrue(historical.isNotEmpty())
        historical.forEach { entry ->
            val path = entry.getValue("path").jsonPrimitive.content
            val reason = entry.getValue("reason").jsonPrimitive.content
            assertTrue(reason.contains("Historical", ignoreCase = true) || reason.contains("Pre-K0"))
            val resolved = oversqliteContractFixture(path)
            assertTrue(resolved.isDirectory() || resolved.isRegularFile(), "historical fixture path is missing: $path")
        }
    }

    private fun objectFixture(path: String): JsonObject =
        json.parseToJsonElement(oversqliteContractFixture(path).readText()).jsonObject

    private fun expandCompactRows(generation: JsonObject, groups: JsonArray, snapshot: Boolean): List<JsonObject> {
        val tables = generation.getValue("tables").jsonArray.map { it.jsonObject }
        val expanded = mutableListOf<JsonObject>()
        val logicalTargets = mutableSetOf<String>()
        var nextOrdinal = 0L
        var previousTable = -1
        groups.forEach { groupElement ->
            val group = groupElement.jsonArray
            require(group.size == 3)
            val tableIndex = group[0].jsonPrimitive.int
            require(tableIndex in tables.indices && tableIndex != previousTable)
            previousTable = tableIndex
            require(group[1].jsonPrimitive.long == nextOrdinal)
            val table = tables[tableIndex]
            val keyColumns = table.getValue("sync_key_columns").jsonArray.map { it.jsonPrimitive.content }
            val columns = table.getValue("columns").jsonArray.map { it.jsonPrimitive.content }.toSet()
            val tuples = group[2].jsonArray
            require(tuples.isNotEmpty())
            tuples.forEach { tupleElement ->
                val tuple = tupleElement.jsonArray
                val wireOp = tuple[0].jsonPrimitive.content
                require(wireOp in setOf("i", "u", "d"))
                require(tuple.size == if (wireOp == "d") 3 else 4)
                require(!snapshot || wireOp == "i")
                val keyValues = tuple[1].jsonArray
                require(keyValues.size == keyColumns.size)
                val payload = tuple.getOrNull(3) ?: JsonNull
                if (payload !is JsonNull) {
                    val payloadKeys = payload.jsonObject.keys
                    require(payloadKeys.all(columns::contains))
                    if (snapshot) require(payloadKeys == columns)
                }
                val key = JsonObject(keyColumns.zip(keyValues).toMap())
                val target = "${table.getValue("schema")}/${table.getValue("table")}/${canonicalizeJsonElement(key)}"
                require(logicalTargets.add(target))
                expanded += JsonObject(
                    linkedMapOf(
                        "schema" to table.getValue("schema"),
                        "table" to table.getValue("table"),
                        "key" to key,
                        "op" to JsonPrimitive(mapOf("i" to "INSERT", "u" to "UPDATE", "d" to "DELETE").getValue(wireOp)),
                        "version" to JsonPrimitive(tuple[2].jsonPrimitive.long),
                        "payload" to payload,
                    ),
                )
                nextOrdinal++
            }
        }
        return expanded
    }

    private fun expandHashRows(generation: JsonObject, groups: JsonArray): List<JsonObject> {
        val tables = generation.getValue("tables").jsonArray.map { it.jsonObject }
        val result = mutableListOf<JsonObject>()
        groups.forEach { groupElement ->
            val group = groupElement.jsonArray
            require(group.size == 3)
            val table = tables[group[0].jsonPrimitive.int]
            val keyColumns = table.getValue("sync_key_columns").jsonArray.map { it.jsonPrimitive.content }
            var ordinal = group[1].jsonPrimitive.long
            group[2].jsonArray.forEach { tupleElement ->
                val tuple = tupleElement.jsonArray
                val keyValues = tuple[1].jsonArray
                val op = mapOf("i" to "INSERT", "u" to "UPDATE", "d" to "DELETE")
                    .getValue(tuple[0].jsonPrimitive.content)
                result += buildJsonObject {
                    put("row_ordinal", JsonPrimitive(ordinal.toString()))
                    put("schema", table.getValue("schema"))
                    put("table", table.getValue("table"))
                    put("key", JsonObject(keyColumns.zip(keyValues).toMap()))
                    put("op", JsonPrimitive(op))
                    put("base_row_version", JsonPrimitive(tuple[2].jsonPrimitive.long.toString()))
                    put("payload", tuple.getOrNull(3) ?: JsonNull)
                }
                ordinal++
            }
        }
        return result
    }

    private fun fixtureSha256Hex(bytes: ByteArray): String = MessageDigest.getInstance("SHA-256")
        .digest(bytes)
        .joinToString("") { "%02x".format(it) }

    private fun deterministicBody(size: Int, payload: String): ByteArray {
        val body = ByteArray(size)
        val prefix = "{\"pad\":\"".encodeToByteArray()
        prefix.copyInto(body)
        body[size - 2] = '"'.code.toByte()
        body[size - 1] = '}'.code.toByte()
        if (payload == "repeat:A") {
            for (index in prefix.size until size - 2) body[index] = 'A'.code.toByte()
        } else {
            var state = 0x6f76657273716c69L
            for (index in prefix.size until size - 2) {
                state = state xor (state ushr 12)
                state = state xor (state shl 25)
                state = state xor (state ushr 27)
                body[index] = ((state * 2685821657736338717L) ushr 56).toByte()
            }
        }
        return body
    }

    private fun gzip(body: ByteArray): ByteArray = ByteArrayOutputStream().use { output ->
        GZIPOutputStream(output).use { it.write(body) }
        output.toByteArray()
    }

    private fun executeReceiverCase(case: JsonObject): String {
        val name = case.getValue("name").jsonPrimitive.content
        val encoding = case.getValue("contentEncoding").jsonPrimitive.content
        if (encoding != "gzip") return "unsupported_content_encoding"
        val body = when (name) {
            "malformed-gzip" -> "not-a-gzip-member".encodeToByteArray()
            "truncated-gzip" -> gzip("truncated".encodeToByteArray()).dropLast(2).toByteArray()
            "concatenated-gzip-members" -> gzip("first".encodeToByteArray()) + gzip("second".encodeToByteArray())
            "encoded-overflow" -> ByteArray(case.getValue("encodedBytes").jsonPrimitive.int)
            "decoded-overflow" -> gzip(ByteArray(case.getValue("decodedBytes").jsonPrimitive.int) { 'A'.code.toByte() })
            else -> error("unexecuted receiver case $name")
        }
        val encodedLimit = case["encodedLimit"]?.jsonPrimitive?.int ?: Int.MAX_VALUE
        if (body.size > encodedLimit) return "encoded_body_too_large"
        val decodedLimit = case["decodedLimit"]?.jsonPrimitive?.int ?: Int.MAX_VALUE
        return runCatching { strictGunzip(body, decodedLimit) }
            .fold(onSuccess = { error("receiver case $name unexpectedly succeeded") }, onFailure = { it.message ?: "invalid_gzip" })
    }

    private fun strictGunzip(input: ByteArray, decodedLimit: Int): ByteArray {
        if (input.size < 18 || input[0] != 0x1f.toByte() || input[1] != 0x8b.toByte() || input[2] != 8.toByte() || input[3] != 0.toByte()) {
            error("invalid_gzip")
        }
        val inflater = Inflater(true)
        inflater.setInput(input, 10, input.size - 10)
        val output = ByteArrayOutputStream()
        val buffer = ByteArray(8192)
        try {
            while (!inflater.finished()) {
                val count = inflater.inflate(buffer)
                if (count == 0) {
                    if (inflater.needsInput() || inflater.needsDictionary()) error("invalid_gzip")
                } else {
                    output.write(buffer, 0, count)
                    if (output.size() > decodedLimit) error("decoded_body_too_large")
                }
            }
            val memberEnd = 10 + inflater.bytesRead.toInt() + 8
            if (memberEnd != input.size) error("invalid_gzip")
            val decoded = output.toByteArray()
            val trailer = memberEnd - 8
            val expectedCrc = littleEndianUInt(input, trailer)
            val expectedSize = littleEndianUInt(input, trailer + 4)
            val crc = CRC32().apply { update(decoded) }.value
            if (crc != expectedCrc || decoded.size.toLong() and 0xffffffffL != expectedSize) error("invalid_gzip")
            return decoded
        } catch (error: java.util.zip.DataFormatException) {
            throw IllegalArgumentException("invalid_gzip", error)
        } finally {
            inflater.end()
        }
    }

    private fun littleEndianUInt(input: ByteArray, offset: Int): Long =
        (input[offset].toLong() and 0xff) or
            ((input[offset + 1].toLong() and 0xff) shl 8) or
            ((input[offset + 2].toLong() and 0xff) shl 16) or
            ((input[offset + 3].toLong() and 0xff) shl 24)
}

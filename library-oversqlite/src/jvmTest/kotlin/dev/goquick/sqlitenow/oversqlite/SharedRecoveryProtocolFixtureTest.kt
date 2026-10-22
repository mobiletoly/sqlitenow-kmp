package dev.goquick.sqlitenow.oversqlite

import java.nio.file.Path
import java.security.MessageDigest
import kotlin.io.path.readBytes
import kotlin.io.path.readText
import kotlin.test.*
import kotlinx.serialization.json.*

class SharedRecoveryProtocolFixtureTest {
    private val fixture get() = Json.parseToJsonElement(oversqliteContractFixture("recovery/protocol.json").readText()).jsonObject
    private val generation get() = Json.decodeFromJsonElement<SyncGeneration>(fixture.getValue("generation"))
    private fun cases(family: String, check: (JsonObject) -> Unit) {
        val cases = fixture.getValue(family).jsonArray
        assertTrue(cases.isNotEmpty(), "empty family $family")
        cases.forEach { raw ->
            val case = raw.jsonObject
            check(case)
            println("RECOVERY_CASE $family/${case.getValue("name").jsonPrimitive.content} PASS")
        }
    }
    @Test fun everyDeclaredFamilyAndAuthorityExecutes() {
        assertEquals(setOf("formatVersion", "contract", "hashProjection", "generation", "headers", "bindingCases", "outcomes", "outcomeInvalid",
            "resolveRequests", "ackRequests", "snapshotRequests", "snapshots", "snapshotInvalid", "snapshotMetadataInvalid", "workflows", "requestCounts", "errors"), fixture.keys)
        assertEquals("durable-recovery", fixture.getValue("contract").jsonPrimitive.content)
        assertEquals("protocol-integral-decimal-strings", fixture.getValue("hashProjection").jsonPrimitive.content)
        val file = oversqliteContractFixture("recovery/authority-pins.json")
        val kotlin = file.parent.parent.parent
        val go = Path.of(checkNotNull(System.getenv("GO_OVERSYNC_REPO")) { "GO_OVERSYNC_REPO required for authority pins" })
        val expected = setOf("go/AGENTS.md", "kotlin/AGENTS.md", "kotlin/docs-engineering/oversqlite-validation.md",
            "go/specs/p0000-oversqlite-protocol-simplification-and-efficiency.md", "go/specs/p0001-oversqlite-fk-safe-sync-and-automatic-resolution.md",
            "go/specs/r0001-oversqlite-durable-recovery.md", "go/specs/r0001-oversqlite-durable-recovery-checklist.md",
            "go/specs/r0001-oversqlite-durable-recovery-validation.md", "go/swagger/two_way_sync.yaml",
            "kotlin/specs/oversqlite-fk-safe-sync-and-automatic-resolution.md", "kotlin/specs/oversqlite-durable-recovery.md")
        val pins = Json.parseToJsonElement(file.readText()).jsonObject
        assertEquals(expected, pins.keys)
        for ((name, hash) in pins) {
            val root = if (name.startsWith("go/")) go else kotlin
            val bytes = root.resolve(name.substringAfter('/')).readBytes()
            val actual = MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }
            assertEquals(hash.jsonPrimitive.content, actual, name)
        }
    }
    @Test fun terminalOutcomeHashesAndStrictUnions() {
        cases("outcomes") { case ->
            val outcome = decodePushOutcome(case.getValue("raw").jsonPrimitive.content)
            assertEquals(outcome.outcomeHash, hashPushOutcome(outcome, generation))
        }
        cases("outcomeInvalid") { case ->
            val raw = case.getValue("raw").jsonPrimitive.content
            if (case.getValue("stage").jsonPrimitive.content == "hash") assertFails {
                val outcome = decodePushOutcome(raw)
                require(outcome.outcomeHash == hashPushOutcome(outcome, generation))
            } else assertFails { decodePushOutcome(raw) }
        }
        cases("bindingCases") { case ->
            val result = runCatching {
                require(case.getValue("frozenContext") == case.getValue("currentContext"))
                val binding = Json.decodeFromJsonElement<PushOperationBinding>(case.getValue("binding"))
                validatePushOutcomeBinding(decodePushOutcome(case.getValue("raw").jsonPrimitive.content), binding, generation)
            }
            assertEquals(case.getValue("accepted").jsonPrimitive.boolean, result.isSuccess)
        }
    }
    @Test fun exactHeadersAndClosedRequests() {
        cases("headers") { case ->
            val values = case.getValue("values").jsonArray.map { it.jsonPrimitive.content }
            assertEquals(case.getValue("accepted").jsonPrimitive.boolean, values == listOf(OVERSQLITE_PROTOCOL_VERSION))
        }
        for (family in listOf("resolveRequests", "ackRequests", "snapshotRequests")) cases(family) { case ->
            val raw = case.getValue("raw").jsonPrimitive.content
            val result = runCatching { when (family) { "resolveRequests" -> decodeOutcomeResolve(raw); "ackRequests" -> decodeOutcomeAck(raw); else -> decodeRecoverySnapshotRequest(raw) } }
            assertEquals(case.getValue("accepted").jsonPrimitive.boolean, result.isSuccess, "${case.getValue("name")}: ${result.exceptionOrNull()}")
        }
    }
    @Test fun sealedSnapshotsAcrossChunkBoundaries() {
        cases("snapshots") { case ->
            for (chunks in case.getValue("chunkings").jsonArray) verifySnapshot(case, chunks.jsonArray.map { it.jsonPrimitive.int })
        }
        cases("snapshotInvalid") { case -> assertFails { verifySnapshot(case, listOf(case.getValue("rows").jsonArray.size)) } }
        cases("snapshotMetadataInvalid") { case -> assertFails { decodeRecoverySnapshotMetadata(case.getValue("raw").jsonPrimitive.content,null) } }
    }
    private fun verifySnapshot(case: JsonObject, chunks: List<Int>) {
        val requested = case["requestedRecovery"]?.takeUnless { it == JsonNull }?.let { Json.decodeFromJsonElement<SnapshotRecovery>(it) }
        val metadata = decodeRecoverySnapshotMetadata(case.getValue("metadata").toString(), requested)
        val writer = SnapshotHashWriter(case.getValue("source").jsonPrimitive.content, generation, metadata.snapshotId, metadata.snapshotBundleSeq, metadata.recovery)
        val rows = case.getValue("rows").jsonArray
        var offset = 0
        chunks.forEach { size ->
            require(size >= 0 && offset + size <= rows.size)
            rows.subList(offset, offset + size).forEach { element ->
                val row = element.jsonObject
                writer.add(row.getValue("table_index").jsonPrimitive.int, row.getValue("key_values").jsonArray,
                    row.getValue("row_version").jsonPrimitive.long, row.getValue("payload").jsonObject)
            }
            offset += size
        }
        assertEquals(rows.size, offset)
        assertEquals(metadata.snapshotHash, writer.finish(metadata.rowCount, metadata.logicalByteCount))
    }
    @Test fun resolveAckReopenAndSequenceTraces() {
        cases("workflows") { case ->
            var sequence = 1; var frozen = false; var outcome = false; var ack = false; var serverAck = false; var snapshots = 0; var history = 0
            for (event in case.getValue("events").jsonArray.map { it.jsonPrimitive.content }) when (event) {
                "freeze" -> { require(!frozen && !outcome && !ack); frozen = true }
                "committed", "rejected", "cancelled" -> { require(frozen); outcome = true; if (event == "committed") history++ }
                "settle" -> { require(outcome); frozen = false; outcome = false; ack = true }
                "ack" -> { require(ack); ack = false; sequence++ }
                "ack_lost" -> { require(ack); serverAck = true }
                "duplicate_ack" -> { require(ack && serverAck); ack = false; sequence++ }
                "snapshot" -> if (!frozen && !outcome && !ack) snapshots++
                "unsafe", "reopen", "retire_rebind" -> Unit
                else -> error("unevaluated event $event")
            }
            val actual = buildJsonObject {
                put("history_commits", history); put("next_sequence", sequence); put("frozen", frozen); put("outcome", outcome); put("ack_pending", ack); put("snapshots", snapshots)
            }
            assertEquals(case.getValue("expected"), actual)
        }
    }
    @Test fun ordinaryAndRecoveryDispatchCounts() {
        cases("requestCounts") { case ->
            val requests = mutableListOf<String>()
            var ready = case.getValue("ackPending").jsonPrimitive.boolean
            for (event in case.getValue("events").jsonArray.map { it.jsonPrimitive.content }) when(event) {
                "local_disposition" -> ready = true
                "reopen" -> Unit
                "push" -> requests += "POST /sync/push"
                "resolve" -> requests += "POST /sync/push-outcomes/resolve"
                "ack" -> { require(ready); requests += "POST /sync/push-outcomes/ack"; ready = false }
                "pull" -> requests += "GET /sync/pull"
                else -> error("unevaluated dispatch $event")
            }
            assertEquals(case.getValue("requests").jsonArray.map { it.jsonPrimitive.content }, requests)
            assertEquals(case.getValue("count").jsonPrimitive.int, requests.size)
        }
    }
    @Test fun errorsAndAllExistingCurrentFamiliesExecute() {
        val statuses = mapOf("protocol_version_mismatch" to 426, "push_outcome_mismatch" to 409, "push_outcome_pending" to 409,
            "push_outcome_acknowledged" to 410, "push_outcome_too_large" to 413, "recovery_floor_ahead" to 409, "source_generation_conflict" to 409)
        val errors = fixture.getValue("errors").jsonArray
        assertEquals(statuses.keys, errors.map { it.jsonObject.getValue("code").jsonPrimitive.content }.toSet())
        errors.forEach { assertEquals(statuses[it.jsonObject.getValue("code").jsonPrimitive.content], it.jsonObject.getValue("status").jsonPrimitive.int) }
        SharedProtocolHandshakeFixtureTest().apply { currentDiscoveryAndConnectCasesDescribeTheDirectProtocol(); activeHandshakeFixtureContainsNoLegacyDiscoverySurface() }
        SharedProtocolHttpRequestFixtureTest().apply { currentRequestMatrixUsesOnlyDirectProtocolRoutes(); removedOperationsAreAssertionsNotExecutableCases() }
        SharedBundleChangeWatchFixtureTest().apply { discoveryAndSseCasesUseTheReducedWatchContract(); burstCasesBoundWatchPullConflation() }
        SharedWireContractFixtureTest().apply {
            everyCurrentWireFixtureIsManifestPinnedAndValidJson(); compactRowsExecuteTupleAndSnapshotRules(); compressionCasesExecuteThresholdAndFramingDecisions()
            // Explicit historical error/fault/request-count traces remain codec/regression input.
            replayMismatchAndRemovedRouteDecisionsAreExplicit(); limitFaultAndRequestCountCasesExecuteTheirInvariants(); retainedLegacyVocabularyIsExplicitlyHistoricalInput()
        }
        SharedLocalApplyFixtureTest().localApplyContractExecutesDefaultsResultsAndSurvivorWitness()
    }
}

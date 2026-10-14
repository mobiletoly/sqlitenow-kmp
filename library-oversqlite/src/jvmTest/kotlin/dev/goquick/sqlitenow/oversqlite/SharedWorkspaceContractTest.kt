package dev.goquick.sqlitenow.oversqlite

import java.security.MessageDigest
import kotlinx.serialization.json.*
import kotlin.io.path.readText
import kotlin.test.*

/** Independent pure local contract interpreter; no SQLite runtime evidence. */
internal class SharedWorkspaceContractTest {
    @Test
    fun configuredRootFailsClosed() {
        assertFails { configuredOversqliteContractRoot(null) }
        assertFails { configuredOversqliteContractRoot("") }
        val missing = java.nio.file.Files.createTempDirectory("a3-absent-corpus").resolve("absent")
        assertFails { validateManifestPinnedCorpus(configuredOversqliteContractRoot(missing.toString())) }
    }

    @Test
    fun contract() {
        val fixture = localApplyContractFixture("a3-workspace")
        fixture.assertContractFields("formatVersion classification family physicalInventory cases runtimeObligations runtimeExecuted executionContract")
        assertEquals("internal-work-slice", fixture.getValue("executionContract").jsonObject.contractText("workLimitScope"))
        assertFalse(fixture.getValue("executionContract").jsonObject.getValue("publicSchedulingYield").jsonPrimitive.boolean)
        val compatibility = fixture.getValue("executionContract").jsonObject.getValue("databaseCompatibility").jsonObject
        assertEquals(mapOf("olderDatabaseCompatibilityRequired" to false, "recreationPermitted" to true,
            "currentDatabaseReopenRequired" to true, "currentDatabaseRestartReplayRequired" to true,
            "legacyUpgradeQualificationRequired" to false), compatibility.mapValues { it.value.jsonPrimitive.boolean })
        val atomic = fixture.getValue("executionContract").jsonObject.getValue("goReadyAtomicStatements").jsonObject
        assertEquals(16L, atomic.getValue("maxArguments").jsonPrimitive.long)
        assertEquals(65536L, atomic.getValue("maxEncodedArgumentBytes").jsonPrimitive.long)
        for (field in listOf("sameReservedPool", "debitPrepaidCredits", "retainOriginalWriteCharges", "unsupportedOrOversizedOneShot", "cleanupBeforePublication")) assertTrue(atomic.getValue(field).jsonPrimitive.boolean)
        val memo = fixture.getValue("executionContract").jsonObject.getValue("planningDescriptorReuse").jsonObject
        for (field in listOf("writerScoped", "sameAdmittedMutableDescriptor", "validateOutboxOnce", "invalidateAfterOwnerDeletion", "currentPushWorkspaceCheckedPerStep")) assertTrue(memo.getValue(field).jsonPrimitive.boolean)
        assertFalse(memo.getValue("retainAcrossWriters").jsonPrimitive.boolean)
        assertEquals(2L, memo.getValue("holderEntries").jsonPrimitive.long)
        assertEquals(256L, memo.getValue("holderBytes").jsonPrimitive.long)
        val cleanup = fixture.getValue("executionContract").jsonObject.getValue("boundedCleanupWriter").jsonObject
        assertEquals(32L, cleanup.getValue("maximumRecords").jsonPrimitive.long)
        assertEquals(8L, cleanup.getValue("workPerRecord").jsonPrimitive.long)
        assertEquals(1L, cleanup.getValue("rawKernelMaximumRecords").jsonPrimitive.long)
        assertTrue(cleanup.getValue("fixedOuterIdentity").jsonPrimitive.boolean)
        assertTrue(cleanup.getValue("cleanupBeforeCommit").jsonPrimitive.boolean)
        val snapshot = fixture.getValue("executionContract").jsonObject.getValue("planningScalarSnapshot").jsonObject
        assertEquals(2L, snapshot.getValue("entries").jsonPrimitive.long)
        assertEquals(256L, snapshot.getValue("bytes").jsonPrimitive.long)
        for (field in listOf("writerScoped", "immutableNonPushOwnersOnly", "releaseBeforeContinuation")) assertTrue(snapshot.getValue(field).jsonPrimitive.boolean)
        val guard = fixture.getValue("executionContract").jsonObject.getValue("planningGuardSnapshot").jsonObject
        assertEquals(4L,guard.getValue("entries").jsonPrimitive.long)
        assertEquals(8192L,guard.getValue("bytes").jsonPrimitive.long)
        assertEquals(1024L,guard.getValue("maxInitializationUtf8Bytes").jsonPrimitive.long)
        for (field in listOf("writerScoped", "invalidateAfterOwnedDelete", "optionalCapacityFallback")) assertTrue(guard.getValue(field).jsonPrimitive.boolean)
        val decision = fixture.getValue("executionContract").jsonObject.getValue("decisionCopyReservation").jsonObject
        assertEquals("outer-operation",decision.contractText("baseScope"))
        assertEquals("complete-simultaneous-copies",decision.contractText("sliceCharge"))
        assertEquals("maximum-not-sum",decision.contractText("growthAcrossSlices"))
        assertEquals("outer-operation",decision.contractText("releaseScope"))
        val physical = canonicalContractValue(fixture.getValue("physicalInventory")).toString().encodeToByteArray()
        assertEquals("8580a0f75f18fc4eebd3604bc9c2142b866eb85af07f40b368752f27f45117b0",
            MessageDigest.getInstance("SHA-256").digest(physical).joinToString("") { "%02x".format(it) })
        assertEquals(listOf("second-supported-handle", "live-buffer-cancellation", "physical-owner-death-and-reacquisition", "old-partial-tampered-format-rejection", "paged-settlement-backlog", "trigger-live-deviation-preservation"),
            fixture.getValue("runtimeObligations").jsonArray.map { it.jsonPrimitive.content })
        executeContractCases(fixture, listOf("aggregate-exact", "aggregate-entries-first", "aggregate-bytes", "aggregate-below", "aggregate-overflow", "work-remaining", "work-intrinsic", "work-exact", "work-cancelled", "work-ready-after-validation", "work-after-overshoot", "work-saturates", "transfer-release-once", "page-row-cap", "page-byte-cap", "page-large-one", "guard-exact", "guard-changed-limits", "guard-changed-dirty", "guard-changed-baseline", "guard-changed-attachment", "guard-changed-input"), ::workspaceResult)
    }

    private fun workspaceResult(kind: String, input: JsonObject): JsonObject = when (kind) {
        "aggregate" -> aggregate(input)
        "work" -> {
            input.assertContractFields("limit used required cancelled firstReady")
            val limit = input.contractNumber("limit")
            var used = input.contractNumber("used")
            val requested = input.contractNumber("required")
            assertTrue(limit > 0 && used >= 0 && requested >= 0)
            val cancelled = input.getValue("cancelled").jsonPrimitive.boolean
            val firstReady = input.getValue("firstReady").jsonPrimitive.boolean
            val result = when {
                cancelled -> "cancelled"
                !firstReady && used >= limit -> "sync_work_limit"
                else -> "fits"
            }
            if (result == "fits") used = if (requested > Long.MAX_VALUE - used) Long.MAX_VALUE else used + requested
            buildJsonObject { put("result", result); put("used", used); put("reserved", requested)
                put("ready", true); put("businessChanged", result == "fits") }
        }
        "reservation" -> reservation(input)
        "page" -> {
            input.assertContractFields("rowBytes maxRows softBytes")
            val cap = input.contractNumber("maxRows"); val soft = input.contractNumber("softBytes")
            assertEquals(128L, cap); assertEquals(65536L, soft)
            var rows = 0L; var bytes = 0L
            for (value in input.getValue("rowBytes").jsonArray) {
                val size = value.jsonPrimitive.long
                assertTrue(size >= 0)
                if (rows == cap || (rows > 0 && (bytes >= soft || size > soft - bytes))) break
                assertTrue(size <= Long.MAX_VALUE - bytes)
                rows++; bytes += size
            }
            buildJsonObject { put("rows", rows); put("bytes", bytes) }
        }
        "guard" -> {
            input.assertContractFields("saved current")
            buildJsonObject { put("resume", input.contractText("saved") == input.contractText("current"))
                put("originalInputRetained", true); put("intentRetained", true) }
        }
        else -> error("unsupported A3 workspace case kind: $kind")
    }

    private fun aggregate(input: JsonObject): JsonObject {
        input.assertContractFields("owners requestedEntries requestedBytes maxEntries maxBytes")
        var entries = 0L; var bytes = 0L; var overflow = false
        val seen = mutableSetOf<String>()
        for (value in input.getValue("owners").jsonArray) {
            val owner = value.jsonObject
            owner.assertContractFields("owner entries bytes")
            val name = owner.contractText("owner")
            assertTrue(name in listOf("planner", "apply", "push-stage", "snapshot-stage", "transient"))
            assertTrue(seen.add(name))
            val e = owner.contractNumber("entries"); val b = owner.contractNumber("bytes")
            if (e > Long.MAX_VALUE - entries) overflow = true else entries += e
            assertTrue(b <= Long.MAX_VALUE - bytes); bytes += b
        }
        val requestedEntries = input.contractNumber("requestedEntries"); val requestedBytes = input.contractNumber("requestedBytes")
        val entryLimit = input.contractNumber("maxEntries"); val byteLimit = input.contractNumber("maxBytes")
        val result = when {
            overflow || entries > entryLimit || requestedEntries > entryLimit - entries -> "entries"
            bytes > byteLimit || requestedBytes > byteLimit - bytes -> "bytes"
            else -> "fits"
        }
        return buildJsonObject {
            put("result", result); put("usedEntries", entries); put("usedBytes", bytes)
            put("reservedEntries", requestedEntries); put("reservedBytes", requestedBytes)
            put("afterEntries", entries + if (result == "fits") requestedEntries else 0)
            put("afterBytes", bytes + if (result == "fits") requestedBytes else 0)
        }
    }

    private fun reservation(input: JsonObject): JsonObject {
        input.assertContractFields("entries bytes reservedEntries reservedBytes actions")
        var entries = input.contractNumber("entries"); var bytes = input.contractNumber("bytes")
        val e = input.contractNumber("reservedEntries"); val b = input.contractNumber("reservedBytes")
        assertTrue(e <= entries && b <= bytes)
        var transferred = false; var released = false; var result = ""
        for (action in input.getValue("actions").jsonArray) when (action.jsonPrimitive.content) {
            "transfer" -> { assertFalse(transferred); assertFalse(released); transferred = true }
            "release" -> if (released) result = "already_released" else { released = true; entries -= e; bytes -= b }
            else -> error("unsupported reservation action: $action")
        }
        assertTrue(transferred && released)
        return buildJsonObject { put("entries", entries); put("bytes", bytes)
            put("reservedEntries", 0L); put("reservedBytes", 0L); put("error", result) }
    }
}

internal fun localApplyContractFixture(family: String): JsonObject {
    val fixture = Json.parseToJsonElement(oversqliteContractFixture("local-apply/$family.json").readText()).jsonObject
    assertEquals(1L, fixture.contractNumber("formatVersion"))
    assertEquals("local-contract-runtime-pending", fixture.contractText("classification"))
    assertEquals(family, fixture.contractText("family"))
    assertFalse(fixture.getValue("runtimeExecuted").jsonPrimitive.boolean)
    val classification = Json.parseToJsonElement(oversqliteContractFixture("CLASSIFICATIONS.json").readText()).jsonObject
    val candidate = classification.getValue("localContractCandidates").jsonArray.map { it.jsonObject }
        .single { it.contractText("contract") == family }
    val title = family.removePrefix("a3-").replaceFirstChar { it.uppercase() }
    assertEquals("local-apply/$family.json", candidate.contractText("path"))
    assertEquals("oversync/contract_a3_test.go:TestContractCorpus_A3$title", candidate.contractText("goConsumer"))
    assertEquals("Shared${title}ContractTest.contract", candidate.contractText("jvmConsumer"))
    return fixture
}
internal fun JsonObject.assertContractFields(fields: String) = assertEquals(fields.split(' ').toSet(), keys)
internal fun JsonObject.contractNumber(field: String): Long = getValue(field).jsonPrimitive.let {
    assertFalse(it.isString); it.long.also { n -> assertTrue(n >= 0) }
}
internal fun JsonObject.contractText(field: String): String = getValue(field).jsonPrimitive.let {
    assertTrue(it.isString); it.content
}
internal fun canonicalContractValue(value: JsonElement): JsonElement = when (value) {
    is JsonObject -> JsonObject(value.toSortedMap().mapValues { canonicalContractValue(it.value) })
    is JsonArray -> JsonArray(value.map(::canonicalContractValue))
    else -> value
}
internal fun executeContractCases(fixture: JsonObject, ids: List<String>, execute: (String, JsonObject) -> JsonObject) {
    val cases = fixture.getValue("cases").jsonArray.map { it.jsonObject }
    assertEquals(ids, cases.map { it.contractText("id") }); assertEquals(ids.size, ids.toSet().size)
    cases.forEach {
        it.assertContractFields("id kind input expected")
        assertEquals(it.getValue("expected"), execute(it.contractText("kind"), it.getValue("input").jsonObject), it.contractText("id"))
    }
}

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
        fixture.assertContractFields("formatVersion classification family physicalInventory cases runtimeObligations runtimeExecuted")
        val physical = canonicalContractValue(fixture.getValue("physicalInventory")).toString().encodeToByteArray()
        assertEquals("9d32f6a618ebc62691bbe78c138f7778f68df9ea7bc269dcf25347728ea98259",
            MessageDigest.getInstance("SHA-256").digest(physical).joinToString("") { "%02x".format(it) })
        assertEquals(listOf("second-supported-handle", "live-buffer-cancellation", "physical-owner-death-and-reacquisition", "old-partial-tampered-format-rejection", "paged-settlement-backlog", "trigger-live-deviation-preservation"),
            fixture.getValue("runtimeObligations").jsonArray.map { it.jsonPrimitive.content })
        executeContractCases(fixture, listOf("aggregate-exact", "aggregate-entries-first", "aggregate-bytes", "aggregate-below", "aggregate-overflow", "work-remaining", "work-intrinsic", "work-exact", "work-cancelled", "transfer-release-once", "page-row-cap", "page-byte-cap", "page-large-one", "guard-exact", "guard-changed-limits", "guard-changed-dirty", "guard-changed-baseline", "guard-changed-attachment", "guard-changed-input"), ::workspaceResult)
    }

    private fun workspaceResult(kind: String, input: JsonObject): JsonObject = when (kind) {
        "aggregate" -> aggregate(input)
        "work" -> {
            input.assertContractFields("limit used required cancelled")
            val limit = input.contractNumber("limit")
            var used = input.contractNumber("used")
            val requested = input.contractNumber("required")
            assertTrue(used <= limit)
            val cancelled = input.getValue("cancelled").jsonPrimitive.boolean
            val result = when {
                cancelled -> "cancelled"
                requested > limit -> "atomic_apply_work"
                requested > limit - used -> "sync_work_limit"
                else -> "fits"
            }
            if (result == "fits") used += requested
            buildJsonObject { put("result", result); put("used", used); put("reserved", requested)
                put("ready", result != "atomic_apply_work"); put("businessChanged", result == "fits") }
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

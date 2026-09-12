package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.nio.file.Files
import java.security.MessageDigest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class ClientUpgradePortableAdapterTest {
    private val corpus = oversqliteContractFixture("schema-evolution/client-upgrade/v1")
    private val adapterPath = oversqliteContractFixture("schema-evolution/sqlitenow-adapter/v1.json")

    @Test
    fun revision14ArtifactsAndEveryApplicableDispatchAreBoundToProductionTests() {
        val adapter = parse(adapterPath.toFile().readText())
        val manifestPath = corpus.resolve("manifest.json")
        assertEquals("sqlitenow-client-upgrade-corpus/v1", adapter.string("format"))
        assertEquals(adapter.string("manifest_sha256"), sha256(Files.readAllBytes(manifestPath)))

        val manifest = parse(manifestPath.toFile().readText())
        assertEquals(adapter.string("corpus_revision"), manifest.string("revision"))
        val artifacts = manifest.getValue("artifacts").jsonArray
        assertEquals(22, artifacts.size)
        artifacts.forEach { entry ->
            val artifact = entry.jsonObject
            val path = corpus.resolve(artifact.string("path"))
            assertTrue(Files.isRegularFile(path), "missing revision-14 artifact $path")
            assertEquals(artifact.string("sha256"), sha256(Files.readAllBytes(path)), path.toString())
        }

        val scenarios = loadScenarios(manifest)
        val expectedIDs = adapter.getValue("scenario_ids").jsonArray.map { it.jsonPrimitive.content }.toSet()
        assertEquals(expectedIDs, scenarios.map { it.string("id") }.toSet())
        assertEquals(24, scenarios.size)

        val dispatchGroups = adapter.getValue("dispatch_groups").jsonObject
        val actionDispatch = adapter.getValue("action_dispatch").jsonObject
            .mapValues { it.value.jsonPrimitive.content }
        val preservationLabels = adapter.getValue("preservation_labels").jsonArray
            .map { it.jsonPrimitive.content }
            .toSet()
        validateCoverage(scenarios, actionDispatch, dispatchGroups.keys, preservationLabels)
        validateKmpOwners(dispatchGroups)

        assertEquals(
            adapter.getValue("value_vector_count").jsonPrimitive.content.toInt(),
            parse(corpus.resolve("values.json").toFile().readText()).getValue("vectors").jsonArray.size,
        )
        assertEquals(
            adapter.getValue("limit_vector_count").jsonPrimitive.content.toInt(),
            parse(corpus.resolve("limits.json").toFile().readText()).getValue("vectors").jsonArray.size,
        )
    }

    @Test
    fun negativeControlsRejectMissingDispatchAndCorruptPreservation() {
        val adapter = parse(adapterPath.toFile().readText())
        val manifest = parse(corpus.resolve("manifest.json").toFile().readText())
        val scenarios = loadScenarios(manifest)
        val groups = adapter.getValue("dispatch_groups").jsonObject.keys
        val dispatch = adapter.getValue("action_dispatch").jsonObject
            .mapValues { it.value.jsonPrimitive.content }
        val labels = adapter.getValue("preservation_labels").jsonArray.map { it.jsonPrimitive.content }.toSet()

        assertFailsWith<IllegalStateException> {
            validateCoverage(scenarios, dispatch - scenarios.first().getValue("actions").jsonArray.first().jsonObject.string("kind"), groups, labels)
        }
        assertFailsWith<IllegalStateException> {
            validatePreservation(listOf("corrupt_preservation_witness"), labels)
        }
    }

    private fun loadScenarios(manifest: JsonObject): List<JsonObject> =
        manifest.getValue("scenarios").jsonArray.flatMap { relative ->
            parse(corpus.resolve(relative.jsonPrimitive.content).toFile().readText())
                .getValue("scenarios").jsonArray.map { it.jsonObject }
        }

    private fun validateCoverage(
        scenarios: List<JsonObject>,
        actionDispatch: Map<String, String>,
        groups: Set<String>,
        preservationLabels: Set<String>,
    ) {
        scenarios.forEach { scenario ->
            scenario.getValue("actions").jsonArray.forEach { action ->
                val kind = action.jsonObject.string("kind")
                val group = checkNotNull(actionDispatch[kind]) { "missing production dispatch for $kind" }
                check(group in groups) { "unknown production dispatch group $group for $kind" }
            }
            validatePreservation(
                scenario.getValue("preserved").jsonArray.map { it.jsonPrimitive.content },
                preservationLabels,
            )
        }
    }

    private fun validatePreservation(observed: List<String>, allowed: Set<String>) {
        observed.forEach { label -> check(label in allowed) { "unknown or corrupt preservation label $label" } }
    }

    private fun validateKmpOwners(groups: JsonObject) {
        val root = oversqliteContractFixture("").parent
        groups.forEach { (groupName, groupElement) ->
            val owner = groupElement.jsonObject.getValue("kmp").jsonObject
            val source = root.resolve(owner.string("source"))
            val marker = owner.string("test")
            assertTrue(Files.isRegularFile(source), "$groupName has no KMP source $source")
            assertTrue(source.toFile().readText().contains(marker), "$groupName has no KMP test $marker")
        }
    }

    private fun parse(text: String): JsonObject = Json.parseToJsonElement(text).jsonObject

    private fun JsonObject.string(name: String): String = getValue(name).jsonPrimitive.content

    private fun sha256(bytes: ByteArray): String = MessageDigest.getInstance("SHA-256")
        .digest(bytes)
        .joinToString("") { "%02x".format(it) }
}

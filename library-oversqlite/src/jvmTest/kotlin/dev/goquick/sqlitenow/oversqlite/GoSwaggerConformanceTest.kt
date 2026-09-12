package dev.goquick.sqlitenow.oversqlite

import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import org.yaml.snakeyaml.LoaderOptions
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.SafeConstructor

class GoSwaggerConformanceTest {
    private val goRoot: File = File(
        System.getProperty("goOversyncRepo")
            ?: System.getenv("GO_OVERSYNC_REPO")
            ?: error("Go Swagger conformance requires a validated goOversyncRepo path"),
    ).canonicalFile

    private val openApi: Map<String, Any?> = stringMap(
        Yaml(SafeConstructor(LoaderOptions().apply { setAllowDuplicateKeys(false) }))
            .load(File(goRoot, "swagger/two_way_sync.yaml").readText()),
        "OpenAPI document",
    )

    @Test
    fun discoveryPresenceAndBoundsMatchTheNormativeSwaggerContract() {
        val discovery = schema("DiscoveryResponse")
        assertRequired(discovery, "protocol_version", "schema_generation", "limits", "watch_enabled")
        val discoveryProperties = properties(discovery)
        assertFalse("features" in discoveryProperties)
        assertFalse("bundle_limits" in discoveryProperties)
        assertFalse("schema_version" in discoveryProperties)
        assertEquals(listOf("v1"), stringList(property(discovery, "protocol_version")["enum"], "protocol_version.enum"))
        assertRef(property(discovery, "schema_generation"), "SchemaGeneration")

        val generation = schema("SchemaGeneration")
        assertRequired(generation, "id", "fingerprint", "tables")
        assertEquals(1L, integer(property(generation, "id")["minLength"], "SchemaGeneration.id.minLength"))
        assertEquals("^[0-9a-f]{64}$", property(generation, "fingerprint")["pattern"])

        assertRequired(schema("GenerationTable"), "schema", "table", "sync_key_columns", "columns", "foreign_keys")
        assertRequired(schema("GenerationColumn"), "name", "wire_type", "nullable")

        val limits = schema("WireLimits")
        val requiredLimits = listOf(
            "max_push_rows",
            "max_push_decoded_bytes",
            "max_pull_bundles",
            "max_pull_decoded_bytes",
            "max_snapshot_rows_per_chunk",
            "max_snapshot_decoded_bytes_per_chunk",
            "max_snapshot_rows",
            "max_snapshot_logical_bytes",
            "max_row_decoded_bytes",
        )
        assertRequired(limits, *requiredLimits.toTypedArray())
        requiredLimits.forEach { field ->
            assertEquals(1L, integer(property(limits, field)["minimum"], "WireLimits.$field.minimum"))
        }
    }

    @Test
    fun directPushAndCompactRowsAreNormative() {
        assertTrue("/sync/push" in paths)
        val request = schema("PushRequest")
        assertRequired(request, "source_bundle_id", "canonical_request_hash", "rows")
        listOf("push_id", "planned_row_count", "row_count", "byte_count").forEach {
            assertFalse(it in properties(request), "PushRequest must not retain $it")
        }
        assertRef(property(request, "rows"), "NonEmptyCompactRowGroups")

        assertRequired(schema("PushResponse"), "bundle_seq", "bundle_hash", "rows")

        val group = schema("CompactRowGroup")
        assertEquals(3L, integer(group["minItems"], "CompactRowGroup.minItems"))
        assertEquals(3L, integer(group["maxItems"], "CompactRowGroup.maxItems"))
        assertEquals(false, group["items"])

        val tupleVariants = list(schema("CompactRowTuple")["oneOf"], "CompactRowTuple.oneOf").map {
            stringMap(it, "CompactRowTuple variant")
        }
        assertTrue(tupleVariants.any { tupleVariant(it, 3L, listOf("d")) })
        assertTrue(tupleVariants.any { tupleVariant(it, 4L, listOf("i", "u")) })

        assertFalse("minItems" in schema("SnapshotCompactRowGroups"), "empty snapshot row groups must be representable")
        val snapshotTuple = schema("SnapshotCompactRowTuple")
        assertEquals(4L, integer(snapshotTuple["minItems"], "SnapshotCompactRowTuple.minItems"))
        assertEquals(4L, integer(snapshotTuple["maxItems"], "SnapshotCompactRowTuple.maxItems"))
        assertEquals(listOf("i"), tupleOperationEnum(snapshotTuple))
    }

    @Test
    fun initializationErrorsAndRemovedRoutesAreNormative() {
        val token = schema("CanonicalInitializationToken")
        assertEquals("string", token["type"])
        assertEquals("uuid", token["format"])
        assertEquals(36L, integer(token["minLength"], "CanonicalInitializationToken.minLength"))
        assertEquals(36L, integer(token["maxLength"], "CanonicalInitializationToken.maxLength"))
        assertEquals("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", token["pattern"])
        assertTrue((token["description"] as? String).orEmpty().contains("must not trim, normalize, or case-fold"))

        listOf("ConnectResponse", "PushRequest").forEach { schemaName ->
            assertRef(property(schema(schemaName), "initialization_id"), "CanonicalInitializationToken")
        }

        listOf("/sync/capabilities", "/sync/push-sessions", "/sync/committed-bundles/").forEach { path ->
            assertFalse(path in paths, "$path must be absent")
        }

        val errorCodes = list(property(schema("ErrorResponse"), "error")["oneOf"], "ErrorResponse.error.oneOf")
            .map { stringMap(it, "ErrorResponse.error variant") }
            .first { "enum" in it }
            .let { stringList(it["enum"], "ErrorResponse.error.enum") }
        listOf(
            "invalid_request",
            "push_invalid",
            "push_replay_mismatch",
            "bundle_projection_too_large",
            "committed_bundle_unrepresentable",
            "snapshot_chunk_too_small",
        ).forEach { code ->
            assertTrue(code in errorCodes, "$code must be a documented typed error")
        }
    }

    @Test
    fun defaultableQueriesAndNestedReplacementShapeMatchTheWireContract() {
        listOf("AfterBundleSeq", "MaxBundles", "AfterRowOrdinal", "MaxRows", "MaxBytes", "TargetBundleSeq").forEach { name ->
            assertEquals(false, parameter(name)["required"], "$name must remain optional so the documented server default applies")
        }

        val snapshotCreate = schema("SnapshotSessionCreateRequest")
        assertEquals(false, snapshotCreate["additionalProperties"])
        val replacement = property(snapshotCreate, "source_replacement")
        assertEquals(false, replacement["additionalProperties"])
        assertRequired(replacement, "previous_source_id", "new_source_id", "reason")
    }

    private val components: Map<String, Any?>
        get() = stringMap(openApi["components"], "components")

    private val paths: Map<String, Any?>
        get() = stringMap(openApi["paths"], "paths")

    private fun schema(name: String): Map<String, Any?> =
        named(components, "schemas", name)

    private fun parameter(name: String): Map<String, Any?> =
        named(components, "parameters", name)

    private fun named(parent: Map<String, Any?>, collectionName: String, name: String): Map<String, Any?> =
        stringMap(stringMap(parent[collectionName], collectionName)[name], "$collectionName.$name")

    private fun properties(schema: Map<String, Any?>): Map<String, Any?> =
        stringMap(schema["properties"], "schema properties")

    private fun property(schema: Map<String, Any?>, name: String): Map<String, Any?> =
        stringMap(properties(schema)[name], "schema property $name")

    private fun assertRequired(schema: Map<String, Any?>, vararg names: String) {
        val required = stringList(schema["required"], "schema required")
        names.forEach { name -> assertTrue(name in required, "$name must be required") }
    }

    private fun assertRef(value: Map<String, Any?>, schemaName: String) {
        assertEquals("#/components/schemas/$schemaName", value["\$ref"])
    }

    private fun tupleVariant(schema: Map<String, Any?>, size: Long, operationCodes: List<String>): Boolean =
        integer(schema["minItems"], "CompactRowTuple.minItems") == size &&
            integer(schema["maxItems"], "CompactRowTuple.maxItems") == size &&
            schema["items"] == false &&
            tupleOperationEnum(schema) == operationCodes

    private fun tupleOperationEnum(schema: Map<String, Any?>): List<String> =
        stringList(
            stringMap(list(schema["prefixItems"], "tuple prefixItems").first(), "tuple operation")["enum"],
            "tuple operation enum",
        )

    private fun integer(value: Any?, description: String): Long =
        (value as? Number)?.toLong() ?: error("$description must be a number")

    private fun list(value: Any?, description: String): List<Any?> =
        value as? List<Any?> ?: error("$description must be an array")

    private fun stringList(value: Any?, description: String): List<String> =
        list(value, description).map { it as? String ?: error("$description must contain strings") }

    private fun stringMap(value: Any?, description: String): Map<String, Any?> =
        (value as? Map<*, *>)?.entries?.associate { (key, entryValue) ->
            (key as? String ?: error("$description must have string keys")) to entryValue
        } ?: error("$description must be an object")
}

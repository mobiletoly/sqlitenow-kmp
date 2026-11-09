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
        assertEquals("string", property(discovery, "protocol_version")["type"])
        assertEquals("v1", property(discovery, "protocol_version")["const"])
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
        assertRequired(limits, "max_push_outcome_decoded_bytes")
        val outcomeLimit = property(limits, "max_push_outcome_decoded_bytes")
        assertEquals("int64", outcomeLimit["format"])
        assertEquals(8194L, integer(outcomeLimit["minimum"], "outcome limit minimum"))
        assertEquals(67117056L, integer(outcomeLimit["maximum"], "outcome limit maximum"))
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

        val response = schema("PushResponse")
        assertEquals(false, response["additionalProperties"])
        assertRequired(response, "source_id", "schema_generation_id", "schema_generation_fingerprint",
            "source_bundle_id", "canonical_request_hash", "initialization_id", "status", "outcome_hash")
        assertEquals(listOf("committed", "rejected", "cancelled"),
            stringList(property(response, "status")["enum"], "PushResponse.status.enum"))
        assertRef(property(response, "initialization_id"), "NullableInitializationToken")
        assertRef(property(response, "committed"), "CommittedPushEffect")
        assertRef(property(response, "rejection"), "PushOutcomeRejection")
        assertRequired(schema("CommittedPushEffect"), "bundle_seq", "bundle_hash", "row_count", "rows")
        assertRef(property(schema("CommittedPushEffect"), "rows"), "NonEmptyCompactRowGroups")
        val variants = list(response["oneOf"], "PushResponse.oneOf").map { stringMap(it, "terminal variant") }
        assertEquals(3, variants.size)
        val byStatus = variants.associateBy { property(it, "status")["const"] }
        assertEquals(setOf("committed", "rejected", "cancelled"), byStatus.keys)
        assertRequired(byStatus.getValue("committed"), "committed")
        assertRequired(byStatus.getValue("rejected"), "rejection")
        assertEquals(listOf("rejection"), stringList(
            stringMap(byStatus.getValue("committed")["not"], "committed exclusion")["required"], "excluded rejection"))
        assertEquals(listOf("committed"), stringList(
            stringMap(byStatus.getValue("rejected")["not"], "rejected exclusion")["required"], "excluded committed"))
        val cancelledExclusions = list(stringMap(byStatus.getValue("cancelled")["not"], "cancelled exclusion")["anyOf"],
            "cancelled excluded effects").map {
            stringList(stringMap(it, "excluded effect")["required"], "excluded effect required")
        }
        assertEquals(setOf(listOf("committed"), listOf("rejection")), cancelledExclusions.toSet())

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
        listOf("/sync/push-outcomes/resolve", "/sync/push-outcomes/retained", "/sync/push-outcomes/ack").forEach { path ->
            assertTrue(path in paths, "$path must be a documented durable-outcome route")
        }
        val retained = schema("PushOutcomeRetainedRequest")
        assertEquals(false, retained["additionalProperties"])
        assertEquals(setOf("source_bundle_id"), properties(retained).keys)
        assertRequired(retained, "source_bundle_id")

        val errorCodes = list(property(schema("ErrorResponse"), "error")["anyOf"], "ErrorResponse.error.anyOf")
            .map { stringMap(it, "ErrorResponse.error variant") }
            .first { "enum" in it }
            .let { stringList(it["enum"], "ErrorResponse.error.enum") }
        listOf(
            "invalid_request",
            "push_invalid",
            "bundle_projection_too_large",
            "committed_bundle_unrepresentable",
            "snapshot_chunk_too_small",
            "protocol_version_mismatch",
            "push_outcome_mismatch",
            "push_outcome_pending",
            "push_outcome_acknowledged",
            "push_outcome_too_large",
            "push_outcome_absent",
            "recovery_floor_ahead",
        ).forEach { code ->
            assertTrue(code in errorCodes, "$code must be a documented typed error")
        }
        assertFalse("push_replay_mismatch" in errorCodes, "history replay is no longer the outcome authority")
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
        assertRef(property(snapshotCreate, "recovery"), "SnapshotRecovery")
        assertEquals(setOf("recovery", "source_replacement"), stringList(
            stringMap(snapshotCreate["not"], "snapshot request exclusion")["required"], "excluded request combination").toSet())
        val recovery = schema("SnapshotRecovery")
        assertEquals(false, recovery["additionalProperties"])
        assertEquals(setOf("minimum_bundle_seq"), properties(recovery).keys)
        assertRequired(recovery, "minimum_bundle_seq")
        assertEquals("int64", property(recovery, "minimum_bundle_seq")["format"])
        assertEquals(0L, integer(property(recovery, "minimum_bundle_seq")["minimum"], "snapshot floor minimum"))
        val snapshot = schema("SnapshotSessionResponse")
        assertEquals(false, snapshot["additionalProperties"])
        assertRequired(snapshot, "snapshot_id", "snapshot_bundle_seq", "row_count", "logical_byte_count", "expires_at", "snapshot_hash")
        assertEquals("^[0-9a-f]{64}$", property(snapshot, "snapshot_hash")["pattern"])
        assertRef(property(snapshot, "recovery"), "SnapshotRecovery")
    }

    @Test
    fun outcomeEndpointsAndProtocolHeaderBindTheCurrentLifecycle() {
        val protocol = parameter("ProtocolVersionHeader")
        assertEquals("Oversync-Protocol-Version", protocol["name"])
        assertEquals("header", protocol["in"])
        assertEquals(true, protocol["required"])
        assertEquals("v1", stringMap(protocol["schema"], "protocol header schema")["const"])
        var checked = 0
        paths.filterKeys { it.startsWith("/sync/") }.forEach { (path, item) ->
            val pathItem = stringMap(item, path)
            listOf("get", "post", "put", "patch", "delete", "head", "options", "trace").forEach { method ->
                pathItem[method]?.let { value ->
                    val operation = stringMap(value, "$method $path")
                    val parameters = list(operation["parameters"], "$method $path parameters")
                        .map { stringMap(it, "operation parameter")["\$ref"] }
                    assertTrue("#/components/parameters/ProtocolVersionHeader" in parameters, "$method $path requires v1")
                    checked++
                }
            }
        }
        assertTrue(checked > 0)
        listOf("resolve" to "PushOutcomeResolveRequest", "ack" to "PushOutcomeAckRequest").forEach { (endpoint, requestName) ->
            val operation = stringMap(stringMap(paths["/sync/push-outcomes/$endpoint"], endpoint)["post"], "$endpoint post")
            assertEquals("#/components/requestBodies/$requestName", stringMap(operation["requestBody"], "request body")["\$ref"])
            val parameters = list(operation["parameters"], "$endpoint parameters")
                .map { stringMap(it, "binding header")["\$ref"] }
            listOf("SourceIDHeader", "SchemaGenerationIDHeader", "SchemaGenerationFingerprintHeader").forEach {
                assertTrue("#/components/parameters/$it" in parameters)
            }
            val request = schema(requestName)
            assertEquals(false, request["additionalProperties"])
            val lastField = if (endpoint == "resolve") "initialization_id" else "outcome_hash"
            assertRequired(request, "source_bundle_id", "canonical_request_hash", lastField)
            assertEquals(setOf("source_bundle_id", "canonical_request_hash", lastField), properties(request).keys)
            val responses = stringMap(operation["responses"], "$endpoint responses")
            if (endpoint == "resolve") {
                assertRef(property(request, "initialization_id"), "NullableInitializationToken")
                assertEquals("#/components/responses/PushOK", stringMap(responses["200"], "resolve success")["\$ref"])
            } else {
                assertFalse("content" in stringMap(responses["204"], "empty ack success"))
            }
        }
        val nullableToken = list(schema("NullableInitializationToken")["oneOf"], "nullable initialization").map { stringMap(it, "token variant") }
        assertEquals(2, nullableToken.size)
        assertTrue(nullableToken.any { it["\$ref"] == "#/components/schemas/CanonicalInitializationToken" })
        assertTrue(nullableToken.any { it["type"] == "null" })
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

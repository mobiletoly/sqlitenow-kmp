package dev.goquick.sqlitenow.oversqlite

import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class GoSwaggerConformanceTest {
    private val goRoot: File = File(
        System.getProperty("goOversyncRepo")
            ?: System.getenv("GO_OVERSYNC_REPO")
            ?: error("Go Swagger conformance requires a validated goOversyncRepo path"),
    ).canonicalFile

    private val swagger: String = File(goRoot, "swagger/two_way_sync.yaml").readText()

    @Test
    fun capabilityPresenceAndBoundsMatchTheNormativeSwaggerContract() {
        val capabilities = schemaBlock("CapabilitiesResponse")
        assertRequired(
            capabilities,
            "protocol_version",
            "schema_version",
            "registered_table_specs",
            "features",
            "bundle_limits",
        )
        val protocol = propertyBlock(capabilities, "protocol_version")
        assertTrue(protocol.contains(Regex("(?m)^\\s+enum:\\s*\\[v1]\\s*$")))

        val tableSpec = schemaBlock("RegisteredTableSpec")
        assertRequired(tableSpec, "schema", "table", "sync_key_columns")
        val syncKeys = propertyBlock(tableSpec, "sync_key_columns")
        assertTrue(syncKeys.contains(Regex("(?m)^\\s+minItems:\\s*1\\s*$")))
        assertTrue(syncKeys.contains(Regex("(?m)^\\s+maxItems:\\s*1\\s*$")))

        val limits = schemaBlock("BundleCapabilitiesLimits")
        val requiredLimits = listOf(
            "default_rows_per_snapshot_chunk",
            "max_rows_per_snapshot_chunk",
            "default_bytes_per_snapshot_chunk",
            "max_bytes_per_snapshot_chunk",
            "max_bytes_per_snapshot_row",
            "max_concurrent_snapshot_builds",
            "max_concurrent_snapshot_chunk_requests",
        )
        assertRequired(limits, *requiredLimits.toTypedArray())
        requiredLimits.forEach { field ->
            assertTrue(
                propertyBlock(limits, field).contains(Regex("(?m)^\\s+minimum:\\s*1\\s*$")),
                "$field must have minimum: 1",
            )
        }
    }

    @Test
    fun sourceRetirementPresenceAndRequestIdentityAreNormative() {
        val retired = schemaBlock("SourceRetiredResponse")
        assertRequired(retired, "error", "message", "source_id")
        val sourceId = propertyBlock(retired, "source_id")
        assertTrue(sourceId.contains("failed request", ignoreCase = true))
        val replacement = propertyBlock(retired, "replaced_by_source_id")
        assertFalse(replacement.contains(Regex("(?m)^\\s+nullable:\\s*true\\s*$")))
        assertTrue(replacement.contains("omitted", ignoreCase = true))
        assertTrue(replacement.contains("null", ignoreCase = true))
        assertTrue(replacement.contains("empty", ignoreCase = true))
    }

    @Test
    fun canonicalSessionTokensAreReferencedAtEveryPushAndInitializationBoundary() {
        val token = schemaBlock("CanonicalSessionToken")
        assertTrue(token.contains(Regex("(?m)^      type:\\s*string\\s*$")))
        assertTrue(token.contains(Regex("(?m)^      format:\\s*uuid\\s*$")))
        assertTrue(token.contains(Regex("(?m)^      minLength:\\s*36\\s*$")))
        assertTrue(token.contains(Regex("(?m)^      maxLength:\\s*36\\s*$")))
        assertTrue(
            token.contains(
                "pattern: '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'",
            ),
        )
        assertTrue(token.contains("must not trim, normalize, or case-fold", ignoreCase = true))

        val propertyLocations = listOf(
            "PushSessionCreateRequest" to "initialization_id",
            "ConnectResponse" to "initialization_id",
            "PushSessionCreateStagingResponse" to "push_id",
            "PushSessionChunkResponse" to "push_id",
        )
        propertyLocations.forEach { (schemaName, propertyName) ->
            assertEquals(
                "\$ref: '#/components/schemas/CanonicalSessionToken'",
                propertyBlock(schemaBlock(schemaName), propertyName).trim(),
                "$schemaName.$propertyName must reference CanonicalSessionToken",
            )
        }

        listOf(
            "/sync/push-sessions/{push_id}/chunks",
            "/sync/push-sessions/{push_id}/commit",
            "/sync/push-sessions/{push_id}",
        ).forEach { path ->
            val pushIdParameter = Regex(
                "(?ms)^        - name: push_id\\s*\\n" +
                    "          in: path\\s*\\n" +
                    "          required: true\\s*\\n" +
                    "          schema:\\s*\\n" +
                    "            \\\$ref: '#/components/schemas/CanonicalSessionToken'\\s*$",
            )
            assertTrue(
                pushIdParameter.containsMatchIn(pathBlock(path)),
                "$path push_id path parameter must reference CanonicalSessionToken",
            )
        }
    }

    private fun schemaBlock(name: String): String {
        val marker = "\n    $name:\n"
        val start = swagger.indexOf(marker)
        check(start >= 0) { "Swagger schema $name is missing" }
        val bodyStart = start + marker.length
        val next = Regex("(?m)^    [A-Za-z][A-Za-z0-9]*:\\s*$").find(swagger, bodyStart)?.range?.first
            ?: swagger.length
        return swagger.substring(bodyStart, next)
    }

    private fun pathBlock(path: String): String {
        val marker = "\n  $path:\n"
        val start = swagger.indexOf(marker)
        check(start >= 0) { "Swagger path $path is missing" }
        val bodyStart = start + marker.length
        val next = Regex("(?m)^  /[^:]+:\\s*$").find(swagger, bodyStart)?.range?.first
            ?: swagger.indexOf("\ncomponents:\n", bodyStart).takeIf { it >= 0 }
            ?: swagger.length
        return swagger.substring(bodyStart, next)
    }

    private fun propertyBlock(schema: String, name: String): String {
        val marker = "\n        $name:\n"
        val start = schema.indexOf(marker)
        check(start >= 0) { "Swagger property $name is missing" }
        val bodyStart = start + marker.length
        val next = Regex("(?m)^        [a-z][a-z0-9_]*:\\s*$").find(schema, bodyStart)?.range?.first
            ?: schema.length
        return schema.substring(bodyStart, next)
    }

    private fun assertRequired(schema: String, vararg names: String) {
        val inline = Regex("(?m)^      required:\\s*\\[([^]]+)]\\s*$").find(schema)
        val required = if (inline != null) {
            inline.groupValues[1].split(',').joinToString("\n") { "        - ${it.trim()}" }
        } else {
            val requiredStart = schema.indexOf("\n      required:\n")
            check(requiredStart >= 0) { "Swagger required list is missing" }
            val requiredEnd = schema.indexOf("\n      properties:\n", requiredStart).let {
                if (it >= 0) it else schema.length
            }
            schema.substring(requiredStart, requiredEnd)
        }
        names.forEach { name ->
            assertTrue(required.contains(Regex("(?m)^\\s+- $name\\s*$")), "$name must be required")
        }
    }
}

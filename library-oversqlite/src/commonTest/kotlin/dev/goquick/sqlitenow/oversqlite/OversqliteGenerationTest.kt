package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails

class OversqliteGenerationTest {
    // Go schema_generation_test.go's first generation fingerprinting fingerprint frame, with columns reversed on the wire.
    private val valid = """{"protocol_version":"v1","schema_generation":{"id":"1","fingerprint":"992eb04cc323c90fdaac0de49ed9832fc896b770ab74d26166c7302e72c65e87","tables":[{"schema":"business","table":"person","sync_key_columns":["id"],"columns":[{"name":"id","wire_type":"uuid","nullable":false},{"name":"full_name","wire_type":"text","nullable":false}],"foreign_keys":[]}]},"limits":${Json.encodeToString(testWireLimits())},"watch_enabled":false}"""

    @Test
    fun recomputesGoFingerprintWithoutRequiringWireMemberOrder() {
        val generation = decodeGenerationDiscovery(valid, 1).generation
        assertEquals("992eb04cc323c90fdaac0de49ed9832fc896b770ab74d26166c7302e72c65e87", generation.fingerprint)
        assertEquals("business", generation.tables.single().schema)
    }

    @Test
    fun acceptsOnlyTheGoSigned32BitGenerationRange() {
        val raw = valid.replace("\"id\":\"1\"", "\"id\":\"2147483647\"")
        val fullRange = raw.replace(
            "992eb04cc323c90fdaac0de49ed9832fc896b770ab74d26166c7302e72c65e87",
            "1692039f94399074cf4145a6464c96e2bc8d23b7283e6fcf8bf1676939aa5c5e",
        )
        assertEquals(Int.MAX_VALUE.toString(), decodeGenerationDiscovery(fullRange, Int.MAX_VALUE.toLong()).generation.id)
        for (invalid in listOf(0L, -1L, Int.MAX_VALUE.toLong() + 1L, Long.MAX_VALUE)) {
            assertFails("generation $invalid must be rejected") { decodeGenerationDiscovery(valid, invalid) }
            assertFails("configuration $invalid must be rejected") {
                OversqliteConfig(generationId = invalid, databaseId = "fixture", serviceId = "fixture", syncTables = listOf(SyncTable("person", "id")))
            }
        }
    }

    @Test
    fun rejectsMalformedOrChangedDescriptors() {
        val scenarios = linkedMapOf(
            "wrong revision" to valid.replace("\"v1\"", "\"v2\""),
            "missing revision" to valid.replace("\"protocol_version\":\"v1\",", ""),
            "duplicate revision" to valid.replace("\"protocol_version\":", "\"protocol_version\":\"v1\",\"protocol_version\":"),
            "escaped duplicate" to valid.replace("\"protocol_version\":", "\"protocol_\\u0076ersion\":\"v1\",\"protocol_version\":"),
            "unknown envelope field" to valid.replace("{\"protocol_version", "{\"extra\":0,\"protocol_version"),
            "wrong generation" to valid.replace("\"id\":\"1\"", "\"id\":\"2\""),
            "noncanonical generation" to valid.replace("\"id\":\"1\"", "\"id\":\"01\""),
            "numeric generation" to valid.replace("\"id\":\"1\"", "\"id\":1"),
            "wrong fingerprint" to valid.replace("992eb04cc323c90fdaac0de49ed9832fc896b770ab74d26166c7302e72c65e87", "0".repeat(64)),
            "unknown wire type" to valid.replace("\"text\"", "\"int16\""),
            "null foreign keys" to valid.replace("\"foreign_keys\":[]", "\"foreign_keys\":null"),
            "missing foreign keys" to valid.replace(",\"foreign_keys\":[]", ""),
            "unknown column field" to valid.replace("\"name\":\"full_name\"", "\"extra\":true,\"name\":\"full_name\""),
            "string nullability" to valid.replace("\"nullable\":false", "\"nullable\":\"false\""),
            "missing nullability" to valid.replace(",\"nullable\":false", ""),
            "null nullability" to valid.replace("\"nullable\":false", "\"nullable\":null"),
            "duplicate column member" to valid.replace("\"wire_type\":\"text\"", "\"wire_type\":\"text\",\"wire_type\":\"text\""),
            "changed table" to valid.replace("\"person\"", "\"people\""),
            "invalid identifier" to valid.replace("\"person\"", "\"PERSON\""),
            "nullable key" to valid.replace("\"nullable\":false", "\"nullable\":true"),
        )
        for ((name, raw) in scenarios) {
            assertFails(name) { decodeGenerationDiscovery(raw, 1) }
        }
        for (selectedId in listOf(0L, -1L, 2L)) {
            assertFails("selected ID $selectedId") { decodeGenerationDiscovery(valid, selectedId) }
        }
    }

    @Test
    fun enforcesDecodedControlBodyBound() {
        val atLimit = valid.padEnd(generationControlBodyLimit.toInt(), ' ')
        decodeGenerationDiscovery(atLimit, 1)
        assertFails { decodeGenerationDiscovery(atLimit + " ", 1) }
    }
    @Test
    fun validatesDiscoveryErrorIdentityStatusAndRemediation() {
        val cases = listOf(
            Triple(409, "unknown_schema_generation", "upgrade_or_reconfigure_client"),
            Triple(426, "retired_schema_generation", "upgrade_application"),
            Triple(400, "invalid_generation_discovery_request", "correct_request"),
        )
        for ((status, code, remediation) in cases) {
            val raw = """{"error":"$code","message":"unavailable","retryable":false,"remediation":"$remediation","schema_generation_id":"1"}"""
            val decoded = decodeGenerationDiscoveryError(status, raw, 1) as OversqliteRemoteContractException
            assertEquals(code, decoded.code)
            assertEquals(remediation, decoded.remediation)
            assertEquals(1L, decoded.generationId)
            val corruptions = mapOf(
                "missing context" to raw.replace(",\"schema_generation_id\":\"1\"", ""),
                "wrong ID" to raw.replace("\"schema_generation_id\":\"1\"", "\"schema_generation_id\":\"2\""),
                "invented proof" to raw.replace("\"message\":", "\"fingerprint\":\"unknown\",\"message\":"),
                "retryable" to raw.replace("\"retryable\":false", "\"retryable\":true"),
                "string retryability" to raw.replace("\"retryable\":false", "\"retryable\":\"false\""),
                "wrong remediation" to raw.replace(remediation, "retry"),
                "duplicate code" to raw.replace("\"error\":", "\"error\":\"$code\",\"error\":"),
            )
            for ((name, corrupted) in corruptions) {
                assertFails("$code: $name") { decodeGenerationDiscoveryError(status, corrupted, 1) }
            }
            assertFails { decodeGenerationDiscoveryError(if (status == 409) 426 else 409, raw, 1) }
        }
        assertEquals(null, decodeGenerationDiscoveryError(503, "unavailable", 1))
    }

}

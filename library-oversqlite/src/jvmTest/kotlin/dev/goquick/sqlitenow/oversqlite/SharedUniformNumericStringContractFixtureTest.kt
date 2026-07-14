package dev.goquick.sqlitenow.oversqlite

import java.security.MessageDigest
import java.util.Base64
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SharedUniformNumericStringContractFixtureTest {
    private val fixture = Json.parseToJsonElement(
        oversqliteContractFixture("canonical-json/jcs-uniform-numeric-strings.json").readText(),
    ).jsonObject

    @Test
    fun kmpReadsUniformNumericStringContractWithoutProductionCodecs() {
        assertEquals(
            setOf(
                "contract",
                "contract_id",
                "fixture_schema_version",
                "required_protocol_version",
                "integer_cases",
                "decimal_cases",
                "float_cases",
                "boolean_cases",
                "replay_cases",
                "invalid_cases",
                "canonical_examples",
                "protocol",
            ),
            fixture.keys,
        )
        assertEquals("jcs_uniform_numeric_strings_v1", fixture.string("contract_id"))
        assertEquals(1, fixture.getValue("fixture_schema_version").jsonPrimitive.int)
        assertEquals("v1", fixture.string("required_protocol_version"))

        assertValueCases(
            section = "integer_cases",
            requiredNames = setOf(
                "int64_min",
                "int64_max",
                "above_javascript_safe_integer",
                "int2_min",
                "int2_max",
                "int4_min",
                "int4_max",
            ),
        )
        assertValueCases(
            section = "decimal_cases",
            requiredNames = setOf(
                "decimal_precision_and_scale",
                "decimal_exponent",
                "decimal_postgresql_authoritative_scale",
            ),
        )
        assertValueCases(
            section = "float_cases",
            requiredNames = setOf(
                "binary64_min_subnormal",
                "binary64_max_finite",
                "binary64_negative_max_finite",
                "float4_rounding",
                "floating_negative_zero_normalizes",
                "floating_integer_spelling",
            ),
        )
        assertBooleanCases()
        assertReplayCases()
        assertInvalidCases()
        assertCanonicalExamples()
        assertProtocolAndResetExpectations()
    }

    private fun assertValueCases(section: String, requiredNames: Set<String>) {
        val cases = fixture.array(section).objects()
        assertEquals(requiredNames, cases.names(), section)
        cases.forEach { case ->
            assertEquals(
                setOf("name", "local_sqlite", "uploaded_wire", "postgres_destination", "committed_server"),
                case.keys,
                case.string("name"),
            )
            val local = case.getValue("local_sqlite").jsonObject
            assertEquals(setOf("storage_class", "value_text"), local.keys, case.string("name"))
            assertTrue(case.getValue("uploaded_wire").jsonPrimitive.isString, case.string("name"))
            assertTrue(case.getValue("committed_server").jsonPrimitive.isString, case.string("name"))
        }
    }

    private fun assertBooleanCases() {
        val cases = fixture.array("boolean_cases").objects()
        assertEquals(setOf("sqlite_boolean_false", "sqlite_boolean_true"), cases.names())
        assertEquals(setOf("0", "1"), cases.map { it.string("uploaded_wire") }.toSet())
        assertEquals(setOf(false, true), cases.map { it.getValue("committed_server").jsonPrimitive.boolean }.toSet())
        cases.forEach { case ->
            assertEquals("integer", case.getValue("local_sqlite").jsonObject.string("storage_class"))
            assertEquals("boolean", case.string("postgres_destination"))
        }
    }

    private fun assertReplayCases() {
        val cases = fixture.array("replay_cases").objects()
        assertEquals(
            setOf("committed_replay_unchanged_local_intent", "committed_replay_later_local_edit"),
            cases.names(),
        )
        cases.forEach { case ->
            assertEquals(
                setOf("name", "frozen_local", "uploaded_wire", "committed_server", "live_local", "decision"),
                case.keys,
                case.string("name"),
            )
        }
        assertEquals(
            setOf("apply_committed_and_clear_dirty", "preserve_later_local_edit_and_requeue"),
            cases.map { it.string("decision") }.toSet(),
        )
    }

    private fun assertInvalidCases() {
        val cases = fixture.array("invalid_cases").objects()
        val requiredNames = setOf(
            "integer_leading_plus",
            "integer_leading_zero",
            "integer_negative_zero",
            "integer_fraction",
            "integer_exponent",
            "integer_out_of_range_high",
            "integer_out_of_range_low",
            "integer_legacy_json_number",
            "decimal_leading_plus",
            "decimal_leading_zero",
            "decimal_negative_zero",
            "decimal_malformed_fraction",
            "decimal_malformed_exponent",
            "decimal_nan",
            "decimal_legacy_json_number",
            "float_noncanonical_fraction",
            "float_noncanonical_exponent",
            "float_negative_zero",
            "float_malformed",
            "float_nan",
            "float_positive_infinity",
            "float_negative_infinity",
            "float_legacy_json_number",
            "boolean_string_true",
            "boolean_integer_two",
        )
        assertEquals(requiredNames, cases.names())

        val categoriesByFamily = mapOf(
            "integer" to setOf("leading_plus", "leading_zero", "negative_zero", "fraction", "exponent", "out_of_range", "legacy_json_number"),
            "decimal" to setOf("leading_plus", "leading_zero", "negative_zero", "malformed", "non_finite", "legacy_json_number"),
            "float" to setOf("noncanonical", "malformed", "non_finite", "legacy_json_number"),
            "boolean" to setOf("invalid_boolean_bridge"),
        )
        cases.forEach { case ->
            assertEquals(
                setOf("name", "family", "input", "input_json_type", "category", "outcome"),
                case.keys,
                case.string("name"),
            )
            assertTrue(case.string("category") in categoriesByFamily.getValue(case.string("family")), case.string("name"))
            assertEquals("reject_before_mutation", case.string("outcome"), case.string("name"))
            val input = case.getValue("input").jsonPrimitive
            when (case.string("input_json_type")) {
                "string" -> assertTrue(input.isString, case.string("name"))
                "number" -> {
                    assertFalse(input.isString, case.string("name"))
                    assertEquals("legacy_json_number", case.string("category"), case.string("name"))
                }
                else -> error("unknown input_json_type for ${case.string("name")}")
            }
        }
    }

    private fun assertCanonicalExamples() {
        val examples = fixture.getValue("canonical_examples").jsonObject
        assertEquals(
            setOf("push_request", "committed_bundle", "pull_response", "snapshot_response", "conflict_response"),
            examples.keys,
        )
        examples.forEach { (name, rawCase) ->
            val case = rawCase.jsonObject
            assertEquals(setOf("canonical_bytes", "utf8_base64", "sha256"), case.keys, name)
            val bytes = case.string("canonical_bytes").encodeToByteArray()
            assertEquals(case.string("utf8_base64"), Base64.getEncoder().encodeToString(bytes), name)
            assertEquals(case.string("sha256"), sha256(bytes), name)
            Json.parseToJsonElement(bytes.decodeToString())
        }
    }

    private fun assertProtocolAndResetExpectations() {
        val protocol = fixture.getValue("protocol").jsonObject
        assertEquals("v1", protocol.getValue("capabilities").jsonObject.string("protocol_version"))
        val rejections = protocol.getValue("updated_client_rejections").jsonArray.objects()
        assertEquals(setOf("reject_v0", "reject_empty_version", "reject_unknown_version"), rejections.names())
        assertEquals(setOf("v0", "", "v-next"), rejections.map { it.string("actual") }.toSet())
        rejections.forEach { case ->
            assertEquals("protocol_version_mismatch", case.string("category"))
            assertEquals("before_connect_or_outbox_freeze", case.string("timing"))
        }

        val reset = protocol.getValue("full_reset").jsonObject
        assertEquals("recreate", reset.string("client_database"))
        assertEquals("recreate_including_business_data", reset.string("server_database"))
        assertFalse(reset.getValue("preserve_frozen_outbox").jsonPrimitive.boolean)
        val incompatible = protocol.getValue("incompatible_development_build").jsonObject
        assertTrue(incompatible.getValue("same_v1_may_be_incompatible").jsonPrimitive.boolean)
        assertEquals("unsupported", incompatible.string("mixed_versions"))
    }

    private fun JsonObject.string(name: String): String =
        getValue(name).jsonPrimitive.contentOrNull ?: error("$name must be a JSON string")

    private fun JsonObject.array(name: String): JsonArray = getValue(name).jsonArray

    private fun JsonArray.objects(): List<JsonObject> = map { it.jsonObject }

    private fun List<JsonObject>.names(): Set<String> = map { it.string("name") }.toSet()

    private fun sha256(bytes: ByteArray): String = MessageDigest.getInstance("SHA-256")
        .digest(bytes)
        .joinToString("") { byte -> "%02x".format(byte.toInt() and 0xff) }
}

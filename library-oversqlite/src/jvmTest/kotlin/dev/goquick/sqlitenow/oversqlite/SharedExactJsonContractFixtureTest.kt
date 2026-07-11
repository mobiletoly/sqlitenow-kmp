package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.Serializable
import kotlinx.serialization.SerialName
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails

class SharedExactJsonContractFixtureTest {
    private val json = Json { ignoreUnknownKeys = true }
    private val fixture = json.decodeFromString<ExactJsonContract>(
        oversqliteContractFixture("canonical-json/jcs-typed-numerics.json").readText(),
    )

    @Test
    fun kmpConsumesAuthoritativeJcsTypedNumericVectors() {
        assertEquals("RFC 8785 JCS with schema-typed exact numeric strings", fixture.contract)
        fixture.jsonCases.forEach { case ->
            assertEquals(
                case.canonical,
                canonicalizeJsonElement(json.parseToJsonElement(case.input)),
                case.name,
            )
        }
        fixture.invalidCases.forEach { case ->
            assertFails(case.name) {
                canonicalizeJsonElement(json.parseToJsonElement(case.input))
            }
        }
        assertEquals(
            fixture.hashCases.canonicalRequest.sha256,
            sha256Hex(fixture.hashCases.canonicalRequest.canonical.encodeToByteArray()),
        )
        assertEquals(
            fixture.hashCases.committedBundle.sha256,
            sha256Hex(fixture.hashCases.committedBundle.canonical.encodeToByteArray()),
        )
		fixture.sqliteCases.forEach { case ->
			val baseTable = TableInfo(
				table = "fixture",
				columns = listOf(ColumnInfo("value", case.declaredType, false, true, null)),
			)
			val configuredKind = when (case.numericKind) {
				"exact_int64" -> NumericColumnKind.EXACT_INT64
				"exact_decimal" -> NumericColumnKind.EXACT_DECIMAL
				"approximate" -> NumericColumnKind.APPROXIMATE
				else -> error("unknown numeric kind ${case.numericKind}")
			}
			if (case.name == "exact_decimal_wrong_affinity") {
				assertFails(case.name) { baseTable.withNumericColumnKinds(mapOf("value" to configuredKind)) }
			} else {
				val column = baseTable.withNumericColumnKinds(mapOf("value" to configuredKind)).columns.single()
				if (case.outcome == "reject_before_mutation") {
					assertFails(case.name) {
						OversqliteValueCodec.decodePayloadValue(column, case.wire, PayloadSource.AUTHORITATIVE_WIRE)
					}
				} else {
					OversqliteValueCodec.decodePayloadValue(column, case.wire, PayloadSource.AUTHORITATIVE_WIRE)
				}
			}
		}
		assertEquals("recreate_database", fixture.reset.legacyState)
		assertEquals("unsupported", fixture.reset.mixedVersions)
    }
}

@Serializable
private data class ExactJsonContract(
    val contract: String,
    @SerialName("json_cases")
    val jsonCases: List<ExactJsonCase>,
    @SerialName("invalid_cases")
    val invalidCases: List<InvalidExactJsonCase>,
    @SerialName("hash_cases")
    val hashCases: ExactHashCases,
	@SerialName("sqlite_cases")
	val sqliteCases: List<ExactSQLiteCase>,
	val reset: ExactResetCase,
)

@Serializable
private data class ExactJsonCase(val name: String, val input: String, val canonical: String)

@Serializable
private data class InvalidExactJsonCase(val name: String, val input: String)

@Serializable
private data class ExactHashCases(
    @SerialName("canonical_request")
    val canonicalRequest: ExactHashCase,
    @SerialName("committed_bundle")
    val committedBundle: ExactHashCase,
)

@Serializable
private data class ExactHashCase(val canonical: String, val sha256: String)

@Serializable
private data class ExactSQLiteCase(
	val name: String,
	@SerialName("numeric_kind") val numericKind: String,
	@SerialName("declared_type") val declaredType: String,
	val wire: JsonElement,
	val outcome: String,
)

@Serializable
private data class ExactResetCase(
	@SerialName("legacy_state") val legacyState: String,
	@SerialName("mixed_versions") val mixedVersions: String,
)

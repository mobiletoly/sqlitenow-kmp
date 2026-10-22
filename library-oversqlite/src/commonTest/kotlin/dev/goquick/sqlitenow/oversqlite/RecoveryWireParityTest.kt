package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals

/**
 * Portable copies of the independent recovery corpus golden vectors.
 * Provenance: oversqlite-contracts/recovery/protocol.json SHA256 3bb196350d3411b0bfbc86db171ac0d3955fa45dea1d882b5d2875511da8f2e1.
 * Expected hashes are copied from the pinned oracle, never computed from candidate output.
 * The JVM corpus consumer separately verifies the complete manifest and authority pins.
 */
internal class RecoveryWireParityTest {
    private val generation = Json.decodeFromString<SyncGeneration>("""{"id":"1","fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","tables":[{"schema":"business","table":"records","sync_key_columns":["id"],"columns":[{"name":"id","wire_type":"text","nullable":false},{"name":"label","wire_type":"text","nullable":true},{"name":"enabled","wire_type":"boolean","nullable":false},{"name":"integer","wire_type":"int64","nullable":false},{"name":"decimal","wire_type":"decimal","nullable":false},{"name":"ratio","wire_type":"float64","nullable":false},{"name":"blob","wire_type":"bytes","nullable":false},{"name":"uuid","wire_type":"uuid","nullable":false},{"name":"json","wire_type":"json","nullable":false}],"foreign_keys":[]}]}""")

    @Test
    fun terminalGoldenVectorsRetainExactIntegersAndTypedValues() {
        val hashes = mutableMapOf<String, String>()
        for ((name, raw) in outcomes) {
            val expected = Json.parseToJsonElement(raw).jsonObject
            val outcome = decodePushOutcome(raw)
            val encoded = Json.parseToJsonElement(Json.encodeToString(outcome)).jsonObject
            assertEquals(expected.getValue("source_bundle_id"), encoded.getValue("source_bundle_id"), name)
            assertFalse(encoded.getValue("source_bundle_id").jsonPrimitive.isString, name)
            assertEquals(expected["committed"], encoded["committed"], "$name complete typed effect")
            val rejection = expected["rejection"]?.jsonObject
            assertEquals(rejection?.getValue("code")?.jsonPrimitive?.content, outcome.rejection?.code, name)
            val conflict = outcome.rejection?.conflict
            val conflictWire = conflict?.let { Json { encodeDefaults = true }.encodeToJsonElement(it) }
            assertEquals(rejection?.get("conflict"), conflictWire, "$name exact conflict versions and NULL")
            assertEquals(outcome.outcomeHash, hashPushOutcome(outcome, generation), name)
            hashes[name] = outcome.outcomeHash
            println("RECOVERY_WIRE_CASE outcome/$name PASS")
        }
        assertNotEquals(hashes.getValue("adjacent-large-a"), hashes.getValue("adjacent-large-b"))
    }

    @Test
    fun sealedGoldenSnapshotsRetainExactBoundariesAcrossChunkings() {
        val hashes = mutableMapOf<String, String>()
        for (raw in snapshots) {
            val case = Json.parseToJsonElement(raw).jsonObject
            val name = case.getValue("name").jsonPrimitive.content
            val recovery = case["requestedRecovery"]?.takeUnless { it == JsonNull }
                ?.let { Json.decodeFromJsonElement<SnapshotRecovery>(it) }
            val metadata = decodeRecoverySnapshotMetadata(case.getValue("metadata").toString(), recovery)
            val rows = case.getValue("rows").jsonArray
            for (chunking in case.getValue("chunkings").jsonArray) {
                val writer = SnapshotHashWriter(case.getValue("source").jsonPrimitive.content, generation,
                    metadata.snapshotId, metadata.snapshotBundleSeq, metadata.recovery)
                var offset = 0
                for (chunk in chunking.jsonArray) {
                    val end = offset + chunk.jsonPrimitive.int
                    for (index in offset until end) {
                        val row = rows[index].jsonObject
                        writer.add(row.getValue("table_index").jsonPrimitive.int,
                            row.getValue("key_values").jsonArray,
                            row.getValue("row_version").jsonPrimitive.long,
                            row.getValue("payload").jsonObject)
                    }
                    offset = end
                }
                assertEquals(rows.size, offset, name)
                assertEquals(metadata.snapshotHash, writer.finish(metadata.rowCount, metadata.logicalByteCount), name)
            }
            hashes[name] = metadata.snapshotHash
            println("RECOVERY_WIRE_CASE snapshot/$name PASS")
        }
        assertNotEquals(hashes.getValue("large-a"), hashes.getValue("large-b"))
    }

    @Test
    fun resolveAndAckKeepProtocolIntegersAsExactJsonNumbers() {
        for (token in listOf("1", "9007199254740992", "9007199254740993", "9223372036854775807")) {
            val sequence = token.toLong()
            val resolve = PushOutcomeResolveRequest(sequence, "a".repeat(64), null)
            val resolveWire = Json.encodeToString(resolve)
            assertEquals(token, Json.parseToJsonElement(resolveWire).jsonObject.getValue("source_bundle_id").jsonPrimitive.content)
            assertEquals(resolve, decodeOutcomeResolve(resolveWire))
            val ack = PushOutcomeAckRequest(sequence, "a".repeat(64), "b".repeat(64))
            val ackWire = Json.encodeToString(ack)
            assertEquals(token, Json.parseToJsonElement(ackWire).jsonObject.getValue("source_bundle_id").jsonPrimitive.content)
            assertEquals(ack, decodeOutcomeAck(ackWire))
            println("RECOVERY_WIRE_CASE controls/$token PASS")
        }
    }

    @Test
    fun closedOutcomesRequireExplicitNullAndExactUnionMembers() {
        val deleted = Json.parseToJsonElement(outcomes.single { it.first == "rejected-deleted" }.second).jsonObject
        val rejection = deleted.getValue("rejection").jsonObject
        val conflict = rejection.getValue("conflict").jsonObject
        assertEquals(JsonNull, conflict.getValue("server_row"))
        val missingNull = JsonObject(deleted + ("rejection" to JsonObject(rejection +
            ("conflict" to JsonObject(conflict - "server_row")))))
        assertFails { decodePushOutcome(missingNull.toString()) }
        assertFails { decodePushOutcome(JsonObject(deleted - "initialization_id").toString()) }
        val cancelled = Json.parseToJsonElement(outcomes.single { it.first == "cancelled-null" }.second).jsonObject
        assertFails { decodePushOutcome(JsonObject(cancelled + ("committed" to JsonNull)).toString()) }
        println("RECOVERY_WIRE_CASE closed_union/required_null PASS")
    }

    private val outcomes = listOf(
        "committed-complete-effects" to """{"source_id":"550e8400-e29b-41d4-a716-446655440000","schema_generation_id":"1","schema_generation_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","source_bundle_id":7,"canonical_request_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","initialization_id":null,"status":"committed","outcome_hash":"f35087afc08b0eeb52afb17bd48969593950c046d24e1ec65fed23b8ce19f5a9","committed":{"bundle_seq":42,"bundle_hash":"6410304a66664967ab888f51441f0b8c6491bb7e4fd29a3e72a4a09f9b661c44","row_count":3,"rows":[[0,0,[["u",["first"],42,{"id":"first","label":"λ","enabled":true,"integer":"9223372036854775807","decimal":"123.45","ratio":"1.25","blob":"AAEC","uuid":"550e8400-e29b-41d4-a716-446655440003","json":"{\"nested\":[\"exact\",null,true]}"}],["i",["second"],42,{"id":"second","label":null,"enabled":true,"integer":"9223372036854775807","decimal":"123.45","ratio":"1.25","blob":"AAEC","uuid":"550e8400-e29b-41d4-a716-446655440003","json":"{\"nested\":[\"exact\",null,true]}"}],["d",["gone"],42]]]]}}""",
        "rejected-conflict" to """{"source_id":"550e8400-e29b-41d4-a716-446655440000","schema_generation_id":"1","schema_generation_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","source_bundle_id":7,"canonical_request_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","initialization_id":null,"status":"rejected","outcome_hash":"1b89ecc8c617d13d6f7580b77dfbdf0971f27b5e5cdca80525400cad2f15bf5e","rejection":{"code":"push_conflict","conflict":{"schema":"business","table":"records","key":{"id":"first"},"op":"u","base_row_version":6,"server_row_version":7,"server_row_deleted":false,"server_row":{"id":"first","label":"λ","enabled":true,"integer":"9223372036854775807","decimal":"123.45","ratio":"1.25","blob":"AAEC","uuid":"550e8400-e29b-41d4-a716-446655440003","json":"{\"nested\":[\"exact\",null,true]}"}}}}""",
        "rejected-deleted" to """{"source_id":"550e8400-e29b-41d4-a716-446655440000","schema_generation_id":"1","schema_generation_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","source_bundle_id":7,"canonical_request_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","initialization_id":null,"status":"rejected","outcome_hash":"0f4573d9d0bd68420f293adc94c673b384ba6e2d9e9f49e789bb55d221695d17","rejection":{"code":"push_conflict","conflict":{"schema":"business","table":"records","key":{"id":"first"},"op":"u","base_row_version":6,"server_row_version":7,"server_row_deleted":true,"server_row":null}}}""",
        "rejected-application" to """{"source_id":"550e8400-e29b-41d4-a716-446655440000","schema_generation_id":"1","schema_generation_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","source_bundle_id":7,"canonical_request_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","initialization_id":null,"status":"rejected","outcome_hash":"1db46c7ba41b48adeec16a36e49b9ad2589bfa120390b37b89334de88d964df2","rejection":{"code":"business_rule_rejected"}}""",
        "cancelled-null" to """{"source_id":"550e8400-e29b-41d4-a716-446655440000","schema_generation_id":"1","schema_generation_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","source_bundle_id":7,"canonical_request_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","initialization_id":null,"status":"cancelled","outcome_hash":"5a376ce2a63930bf5f5233ec8200ebed6239ee289838555b575e10c7510b50af"}""",
        "cancelled-token" to """{"source_id":"550e8400-e29b-41d4-a716-446655440000","schema_generation_id":"1","schema_generation_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","source_bundle_id":7,"canonical_request_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","initialization_id":"550e8400-e29b-41d4-a716-446655440003","status":"cancelled","outcome_hash":"440e80715dacf04f558472c5afb02d50696b7236577382811be390799465261f"}""",
        "adjacent-large-a" to """{"source_id":"550e8400-e29b-41d4-a716-446655440000","schema_generation_id":"1","schema_generation_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","source_bundle_id":9007199254740992,"canonical_request_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","initialization_id":null,"status":"cancelled","outcome_hash":"7a9d16eee8b51beb6a899b7d7c4c4b34a68f4d18ae6e3011d4797ad5af6d2ceb"}""",
        "adjacent-large-b" to """{"source_id":"550e8400-e29b-41d4-a716-446655440000","schema_generation_id":"1","schema_generation_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","source_bundle_id":9007199254740993,"canonical_request_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","initialization_id":null,"status":"cancelled","outcome_hash":"24bc03231149d658518e8d617f2b9cf47d678b6869b6ec2e090951170ba8dbae"}""",
        "int64-max-committed" to """{"source_id":"550e8400-e29b-41d4-a716-446655440000","schema_generation_id":"1","schema_generation_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","source_bundle_id":9223372036854775807,"canonical_request_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","initialization_id":null,"status":"committed","outcome_hash":"b3473cf02b72f5d45582a4cd61409d10c3efea8b871cb51787fad0e7c957652d","committed":{"bundle_seq":9223372036854775807,"bundle_hash":"df1f0cfb3009a711cf06a25ce7a8046c1829bfc233453641edbb98a99f998dda","row_count":3,"rows":[[0,0,[["u",["first"],9223372036854775807,{"id":"first","label":"λ","enabled":true,"integer":"9223372036854775807","decimal":"123.45","ratio":"1.25","blob":"AAEC","uuid":"550e8400-e29b-41d4-a716-446655440003","json":"{\"nested\":[\"exact\",null,true]}"}],["i",["second"],9223372036854775807,{"id":"second","label":null,"enabled":true,"integer":"9223372036854775807","decimal":"123.45","ratio":"1.25","blob":"AAEC","uuid":"550e8400-e29b-41d4-a716-446655440003","json":"{\"nested\":[\"exact\",null,true]}"}],["d",["gone"],9223372036854775807]]]]}}""",
        "int64-max-rejected" to """{"source_id":"550e8400-e29b-41d4-a716-446655440000","schema_generation_id":"1","schema_generation_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","source_bundle_id":9223372036854775807,"canonical_request_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","initialization_id":null,"status":"rejected","outcome_hash":"38de5e6dff1c9d677694ecdf2cfb53c13a5d5974596b5364fc2198be31eb80d1","rejection":{"code":"push_conflict","conflict":{"schema":"business","table":"records","key":{"id":"first"},"op":"u","base_row_version":9223372036854775806,"server_row_version":9223372036854775807,"server_row_deleted":false,"server_row":{"id":"first","label":"λ","enabled":true,"integer":"9223372036854775807","decimal":"123.45","ratio":"1.25","blob":"AAEC","uuid":"550e8400-e29b-41d4-a716-446655440003","json":"{\"nested\":[\"exact\",null,true]}"}}}}"""
    )

    private val snapshots = listOf(
        """{"name":"empty-null","source":"550e8400-e29b-41d4-a716-446655440000","metadata":{"snapshot_id":"snapshot-fixture","snapshot_bundle_seq":0,"row_count":0,"logical_byte_count":0,"expires_at":"2026-10-23T13:00:00Z","snapshot_hash":"9b2ad9cd022f64ec2a3797f4bec44afa150fc71c2fa32e957087912ff6d0b565"},"rows":[],"chunkings":[[0],[]],"requestedRecovery":null}""",
        """{"name":"empty-recovery","source":"550e8400-e29b-41d4-a716-446655440000","metadata":{"snapshot_id":"snapshot-fixture","snapshot_bundle_seq":42,"row_count":0,"logical_byte_count":0,"expires_at":"2026-10-23T13:00:00Z","snapshot_hash":"d293c0d8c2d8bd27d98d74ec351e0610add37cb8850111c4a1e4d0ca3d481942","recovery":{"minimum_bundle_seq":42}},"rows":[],"chunkings":[[0],[]],"requestedRecovery":{"minimum_bundle_seq":42}}""",
        """{"name":"multipage","source":"550e8400-e29b-41d4-a716-446655440000","metadata":{"snapshot_id":"snapshot-fixture","snapshot_bundle_seq":42,"row_count":2,"logical_byte_count":468,"expires_at":"2026-10-23T13:00:00Z","snapshot_hash":"4d4c0a27e84ed987f8d8a10b88f918878d74a73af8f39cf12d58bd90a8ca9d88","recovery":{"minimum_bundle_seq":0}},"rows":[{"table_index":0,"key_values":["first"],"row_version":42,"payload":{"id":"first","label":"λ","enabled":true,"integer":"9223372036854775807","decimal":"123.45","ratio":"1.25","blob":"AAEC","uuid":"550e8400-e29b-41d4-a716-446655440003","json":"{\"nested\":[\"exact\",null,true]}"}},{"table_index":0,"key_values":["second"],"row_version":42,"payload":{"id":"second","label":null,"enabled":true,"integer":"9223372036854775807","decimal":"123.45","ratio":"1.25","blob":"AAEC","uuid":"550e8400-e29b-41d4-a716-446655440003","json":"{\"nested\":[\"exact\",null,true]}"}}],"chunkings":[[2],[1,1]],"requestedRecovery":{"minimum_bundle_seq":0}}""",
        """{"name":"large-a","source":"550e8400-e29b-41d4-a716-446655440000","metadata":{"snapshot_id":"snapshot-fixture","snapshot_bundle_seq":9007199254740992,"row_count":1,"logical_byte_count":247,"expires_at":"2026-10-23T13:00:00Z","snapshot_hash":"7e27db65e860175870f0cdf5cb76742798debccc80025c0d427198a354d0b095","recovery":{"minimum_bundle_seq":9007199254740992}},"rows":[{"table_index":0,"key_values":["first"],"row_version":9007199254740992,"payload":{"id":"first","label":"λ","enabled":true,"integer":"9223372036854775807","decimal":"123.45","ratio":"1.25","blob":"AAEC","uuid":"550e8400-e29b-41d4-a716-446655440003","json":"{\"nested\":[\"exact\",null,true]}"}}],"chunkings":[[1],[1]],"requestedRecovery":{"minimum_bundle_seq":9007199254740992}}""",
        """{"name":"large-b","source":"550e8400-e29b-41d4-a716-446655440000","metadata":{"snapshot_id":"snapshot-fixture","snapshot_bundle_seq":9007199254740993,"row_count":1,"logical_byte_count":247,"expires_at":"2026-10-23T13:00:00Z","snapshot_hash":"85ea351ab76dc6f28dcbe4aac0d463eb7cec5e11145c987ecb562f9bd16122d6","recovery":{"minimum_bundle_seq":9007199254740993}},"rows":[{"table_index":0,"key_values":["first"],"row_version":9007199254740993,"payload":{"id":"first","label":"λ","enabled":true,"integer":"9223372036854775807","decimal":"123.45","ratio":"1.25","blob":"AAEC","uuid":"550e8400-e29b-41d4-a716-446655440003","json":"{\"nested\":[\"exact\",null,true]}"}}],"chunkings":[[1],[1]],"requestedRecovery":{"minimum_bundle_seq":9007199254740993}}""",
        """{"name":"int64-max","source":"550e8400-e29b-41d4-a716-446655440000","metadata":{"snapshot_id":"snapshot-fixture","snapshot_bundle_seq":9223372036854775807,"row_count":1,"logical_byte_count":250,"expires_at":"2026-10-23T13:00:00Z","snapshot_hash":"bcd6de004513b790997c229e5dd1642f0b9597ec468fb25afc28e9d5e0923212","recovery":{"minimum_bundle_seq":9223372036854775807}},"rows":[{"table_index":0,"key_values":["first"],"row_version":9223372036854775807,"payload":{"id":"first","label":"λ","enabled":true,"integer":"9223372036854775807","decimal":"123.45","ratio":"1.25","blob":"AAEC","uuid":"550e8400-e29b-41d4-a716-446655440003","json":"{\"nested\":[\"exact\",null,true]}"}}],"chunkings":[[1],[1]],"requestedRecovery":{"minimum_bundle_seq":9223372036854775807}}"""
    )
}

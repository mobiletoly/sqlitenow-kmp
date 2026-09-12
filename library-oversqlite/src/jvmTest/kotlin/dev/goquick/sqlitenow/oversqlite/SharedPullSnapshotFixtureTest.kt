package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SharedPullSnapshotFixtureTest {
    private val root = Json.parseToJsonElement(
        oversqliteContractFixture("pull-snapshot/basic.json").readText(),
    ).jsonObject

    @Test
    fun compactPullAndSnapshotCasesExerciseCurrentWireShapes() {
        assertEquals(2, root.getValue("formatVersion").jsonPrimitive.int)
        root.getValue("cases").jsonArray.map { it.jsonObject }.forEach { case ->
            val users = mutableSetOf<String>()
            var deletedRows = 0
            var checkpoint = case["afterBundleSeq"]?.jsonPrimitive?.long ?: 0L
            case["pullResponse"]?.jsonObject?.let { response ->
                assertEquals(setOf("stable_bundle_seq", "bundles", "has_more"), response.keys)
                var expectedBundleSeq = case.getValue("afterBundleSeq").jsonPrimitive.long + 1
                response.getValue("bundles").jsonArray.map { it.jsonObject }.forEach { bundle ->
                    assertEquals(setOf("bundle_seq", "source_id", "source_bundle_id", "bundle_hash", "rows"), bundle.keys)
                    assertEquals(expectedBundleSeq++, bundle.getValue("bundle_seq").jsonPrimitive.long)
                    val rows = bundle.getValue("rows").jsonArray
                    if (rows.isEmpty()) {
                        assertEquals(
                            "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945",
                            bundle.getValue("bundle_hash").jsonPrimitive.content,
                        )
                    } else {
                        validateCompactGroups(rows, allowDelete = true)
                        deletedRows += applyUserRows(rows, users, allowDelete = true)
                    }
                    checkpoint = bundle.getValue("bundle_seq").jsonPrimitive.long
                }
                assertEquals(response.getValue("stable_bundle_seq").jsonPrimitive.long, checkpoint)
            }
            case["snapshotSession"]?.jsonObject?.let { session ->
                users.clear()
                deletedRows = 0
                assertEquals(
                    setOf("snapshot_id", "snapshot_bundle_seq", "row_count", "logical_byte_count", "expires_at"),
                    session.keys,
                )
                val chunk = case.getValue("snapshotChunkResponse").jsonObject
                assertEquals(setOf("rows", "next_row_ordinal", "logical_byte_count", "has_more"), chunk.keys)
                val rows = chunk.getValue("rows").jsonArray
                validateCompactGroups(rows, allowDelete = false)
                deletedRows += applyUserRows(rows, users, allowDelete = false)
                val logicalBytes = rows.sumOf { groupElement ->
                    val group = groupElement.jsonArray
                    val tableIndex = group[0]
                    group[2].jsonArray.sumOf { tupleElement ->
                        val tuple = tupleElement.jsonArray
                        val logical = JsonArray(listOf(tuple[0], tableIndex, tuple[1], tuple[2], tuple[3]))
                        canonicalizeJsonElement(logical).encodeToByteArray().size.toLong()
                    }
                }
                assertEquals(chunk.getValue("logical_byte_count").jsonPrimitive.long, logicalBytes)
                assertEquals(session.getValue("logical_byte_count").jsonPrimitive.long, logicalBytes)
                assertEquals(session.getValue("row_count").jsonPrimitive.long, chunk.getValue("next_row_ordinal").jsonPrimitive.long)
                checkpoint = session.getValue("snapshot_bundle_seq").jsonPrimitive.long
            }
            val expected = case.getValue("expectedFinalState").jsonObject
            assertEquals(expected.getValue("lastBundleSeqSeen").jsonPrimitive.long, checkpoint)
            assertEquals(expected.getValue("usersRowCount").jsonPrimitive.int, users.size)
            assertEquals(expected.getValue("rowStateDeleted").jsonPrimitive.int, deletedRows)
        }
    }

    private fun applyUserRows(groups: JsonArray, users: MutableSet<String>, allowDelete: Boolean): Int {
        var deleted = 0
        groups.forEach { groupElement ->
            val group = groupElement.jsonArray
            require(group[0].jsonPrimitive.int == 0) { "unexpected fixture table index" }
            group[2].jsonArray.forEach { tupleElement ->
                val tuple = tupleElement.jsonArray
                val key = tuple[1].jsonArray.single().jsonPrimitive.content
                when (tuple[0].jsonPrimitive.content) {
                    "i", "u" -> users += key
                    "d" -> {
                        require(allowDelete) { "snapshot fixture contains delete" }
                        users -= key
                        deleted++
                    }
                    else -> error("unknown compact operation")
                }
            }
        }
        return deleted
    }

    private fun validateCompactGroups(groups: JsonArray, allowDelete: Boolean) {
        groups.forEach { groupElement ->
            val group = groupElement.jsonArray
            assertEquals(3, group.size)
            assertTrue(group[0].jsonPrimitive.int >= 0)
            group[2].jsonArray.forEach { tupleElement ->
                val tuple = tupleElement.jsonArray
                val op = tuple[0].jsonPrimitive.content
                assertTrue(op in if (allowDelete) setOf("i", "u", "d") else setOf("i"))
                assertEquals(if (op == "d") 3 else 4, tuple.size)
            }
        }
    }
}

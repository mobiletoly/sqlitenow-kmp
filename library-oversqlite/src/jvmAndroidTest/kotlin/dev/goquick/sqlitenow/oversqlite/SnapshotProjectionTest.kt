package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.*
import kotlin.test.*

internal class SnapshotProjectionTest : SurvivorTestSupport() {
    private fun config(info: TableInfo) = ValidatedConfig(null,
        listOf(ValidatedSyncTable(info.table, "id")), mapOf(info.table to "id"),
        mapOf(info.table to listOf("id")), mapOf(info.table to 0), mapOf(info.table to info))

    @Test fun directProjectionMatchesTextWithAllAdmittedStorageAndAffinityCasts() = runOwnedWebDatabaseTest {
        val db = newDb()
        val info = TableInfo("projection_fixture", listOf(
            ColumnInfo("id", "TEXT", true, true, null, wireType = "text"),
            ColumnInfo("integer_value", "INTEGER", false, false, null, wireType = "int64"),
            ColumnInfo("real_value", "DOUBLE", false, false, null, wireType = "float64"),
            ColumnInfo("blob_value", "BLOB", false, false, null, wireType = "bytes"),
            ColumnInfo("note", "TEXT", false, false, null, wireType = "text")))
        val validated = config(info)
        val local = OversqliteLocalStore(db, Json) { validated }
        try {
            val realValues = listOf(0.0, -0.0, 1.0, 0.1, 1.2345678901234567, Double.MIN_VALUE, Double.MAX_VALUE, -1.0e-200)
            for ((index, value) in realValues.withIndex()) {
                val payload = buildJsonObject {
                    put("id", "Ž🧩\"\\-$index")
                    put("integer_value", if (index % 2 == 0) Long.MIN_VALUE.toString() else Long.MAX_VALUE.toString())
                    put("real_value", canonicalizeFiniteJsonNumber(value.toString()))
                    put("blob_value", if (index % 2 == 0) "" else "AAH/")
                    put("note", if (index % 2 == 0) JsonNull else JsonPrimitive("Unicode Ж🧩\n\""))
                }
                for (reuse in listOf(false, true)) {
                    val wire = if (reuse) SnapshotTypedValues.decode(db, info, PayloadSource.AUTHORITATIVE_WIRE, payload, null) else null
                    val expected = local.captureAuthoritativeTypedProjection(info.table, payload, null, wire)
                    val text = local.captureAuthoritativePayload(info.table, payload, null, wire)
                    val decoded = SnapshotTypedValues.decode(db, info, PayloadSource.LOCAL_STATE, Json.parseToJsonElement(text).jsonObject, null)
                    try {
                        assertTrue(expected.matches(decoded), "real=$value reuse=$reuse")
                        assertTrue(expected.matches(Json.parseToJsonElement(text).jsonObject, PayloadSource.LOCAL_STATE))
                    } finally { expected.close(); decoded.close(); wire?.close() }
                    assertFails { expected.requireOwner(db, info) }
                }
            }
            val nulls = buildJsonObject { put("id", "nulls"); for (c in info.columns.drop(1)) put(c.name, JsonNull) }
            val direct = local.captureAuthoritativeTypedProjection(info.table, nulls, null, null)
            try { assertTrue(direct.matches(Json.parseToJsonElement(local.captureAuthoritativePayload(info.table, nulls)).jsonObject, PayloadSource.LOCAL_STATE)) }
            finally { direct.close() }
            assertFails { local.captureAuthoritativeTypedProjection(info.table, JsonObject(nulls - "note"), null, null) }
            assertFails { local.captureAuthoritativeTypedProjection(info.table, JsonObject(nulls + ("blob_value" to JsonPrimitive("bad"))), null, null) }
            val owner = SnapshotTypedValues.decode(db, info, PayloadSource.AUTHORITATIVE_WIRE, nulls, null)
            try {
                val wrong = OversqliteLocalStore(db, Json) { config(info.copy()) }
                assertFails { wrong.captureAuthoritativeTypedProjection(info.table, nulls, null, owner) }
                val otherDb = newDb()
                try { assertFails { owner.requireOwner(otherDb, info) } } finally { otherDb.close() }
            } finally { owner.close() }
            assertFails { local.captureAuthoritativeTypedProjection(info.table, nulls, null, owner) }
            assertFalse(validated.replaySql.table(info.table).authoritativeTypedSelect.contains("json_object"))
            assertTrue(validated.replaySql.table(info.table).authoritativeSelect.contains(validated.replaySql.table(info.table).authoritativeTypedSelect))
        } finally { db.close() }
    }

    @Test fun numericAffinityRemainsRejectedAndRawStorageMismatchIsNotFallback() = runOwnedWebDatabaseTest {
        val db = newDb()
        val info = TableInfo("projection_fixture", listOf(ColumnInfo("id", "TEXT", true, true, null, wireType = "text"),
            ColumnInfo("number", "NUMERIC", false, true, null, wireType = "float64")))
        val validated = config(info)
        val local = OversqliteLocalStore(db, Json) { validated }
        try {
            // NUMERIC executes the same cast, but is not an admitted storage affinity.
            for (value in listOf("1", "1.25")) {
                val payload = buildJsonObject { put("id", "P"); put("number", value) }
                assertTrue(validated.replaySql.table(info.table).authoritativeTypedSelect.contains("CAST(? AS NUMERIC)"))
                val text = local.captureAuthoritativePayload(info.table, payload)
                assertFails { SnapshotTypedValues.decode(db, info, PayloadSource.LOCAL_STATE, Json.parseToJsonElement(text).jsonObject, null) }
                assertFails { local.captureAuthoritativeTypedProjection(info.table, payload, null, null) }
            }
            val textInfo = info.copy(columns = info.columns.take(1))
            db.withPreparedStatement("SELECT 1") { st ->
                check(st.step()); assertFails { SnapshotTypedValues.readProjection(textInfo, st) }
            }
        } finally { db.close() }
    }

    @Test fun sqlCancellationAndCleanupFailuresPropagateWithoutTextFallback() = runOwnedWebDatabaseTest {
        val db = newDb()
        val info = TableInfo("projection_fixture", listOf(ColumnInfo("id", "TEXT", true, true, null, wireType = "text")))
        val validated = config(info)
        val local = OversqliteLocalStore(db, Json) { validated }
        val payload = buildJsonObject { put("id", "P") }
        try {
            for (fault in listOf("prepare", "reset", "cancel")) {
                val failure = if (fault == "cancel") CancellationException("fixture cancellation") else IllegalStateException("fixture SQL failure")
                var prepares = 0
                var clears = 0
                val cache = StatementCache(db, StatementCacheOperations(
                    prepare = { connection, sql ->
                        prepares++
                        assertFalse(sql.contains("json_object"), "no fallback after a real failure")
                        if (fault != "reset") throw failure
                        connection.prepare(sql)
                    },
                    reset = { it.reset(); throw failure },
                    clearBindings = { clears++; it.clearBindings() },
                ))
                try {
                    val caught = assertFails { local.captureAuthoritativeTypedProjection(info.table, payload, cache, null) }
                    // Coroutine stack recovery may copy the exception, retaining the original cause.
                    assertTrue(generateSequence(caught) { it.cause }.any { it === failure })
                    assertEquals(1, prepares)
                    assertEquals(if (fault == "reset") 1 else 0, clears)
                } finally { cache.close() }
            }
            // A healthy retry still reads the independent parameter projection.
            val expected = local.captureAuthoritativeTypedProjection(info.table, payload, null, null)
            try { assertEquals(TypedValue.Text("P"), expected.value("id")) } finally { expected.close() }
        } finally { db.close() }
    }

    @Test fun projectionPairAdmissionHasExactBoundariesAndReleasesOnCancellation() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, _, client -> try {
            val base = OversqliteConfig(1, nodeTables, "fixture", "main")
            val validated = SyncRuntimeInitializer(base, TableInfoCache()).validateConfig(db)
                .withGeneration(OversqliteContractStore(db).readGeneration(1))
            val info = validated.tableInfoByName.getValue("nodes")
            val local = OversqliteLocalStore(db, Json) { validated }
            val payload = buildJsonObject { put("id", "P"); put("name", "parent"); put("parent_id", JsonNull) }
            val text = local.captureAuthoritativePayload("nodes", payload)
            val ws = OversqliteWorkspace(db, base)
            val bytes = utf8Size(payload.toString())
            val pair = ws.add("projection", SnapshotTypedValues.charge(ws, "projection", info, bytes),
                SnapshotTypedValues.charge(ws, "projection", info, utf8Size(text)))
            val used = ws.usage("projection")
            for (dimension in listOf("entries", "bytes")) for (delta in listOf(-1L, 0L, 1L)) {
                val cfg = if (dimension == "entries") base.copy(maxSyncWorkspaceEntries = used.entries + pair.entries + delta)
                    else base.copy(maxSyncWorkspaceBytes = used.bytes + pair.bytes + delta)
                val bounded = OversqliteWorkspace(db, cfg)
                var expected: SnapshotTypedValues? = null
                var observed: SnapshotTypedValues? = null
                withContext(OversqliteWorkSlice()) {
                    assertFailsWith<CancellationException> {
                        SnapshotRowLease.use(db, ApplySurvivor("nodes", "P", null, true), 1, bytes, "projection", null, bounded, Json, payload) { lease ->
                            val admitted = lease.tryRetain(pair)
                            assertEquals(delta >= 0, admitted)
                            try {
                                val same = if (admitted) {
                                    expected = local.captureAuthoritativeTypedProjection("nodes", payload, null, null)
                                    observed = SnapshotTypedValues.decode(db, info, PayloadSource.LOCAL_STATE, Json.parseToJsonElement(text).jsonObject, null)
                                    expected!!.matches(observed!!)
                                } else OversqliteValueCodec.equivalentPayloadTexts(info, local.captureAuthoritativePayload("nodes", payload), text,
                                    PayloadSource.LOCAL_STATE, PayloadSource.LOCAL_STATE, Json)
                                assertTrue(same)
                                assertEquals(if (admitted) pair else WorkspaceCharge(), lease.retainedCharge)
                                throw CancellationException("fixture cancellation")
                            } finally { expected?.close(); observed?.close() }
                        }
                    }
                }
                expected?.let { assertFails { it.requireOwner(db, info) } }
                observed?.let { assertFails { it.requireOwner(db, info) } }
                assertEquals(used, bounded.usage("projection"))
            }
        } finally { client.closeFromCoordinatedOwner() } }
    }
}

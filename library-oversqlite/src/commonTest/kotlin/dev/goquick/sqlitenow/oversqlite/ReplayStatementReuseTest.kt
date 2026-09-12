package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteStatement
import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import io.ktor.client.HttpClient
import kotlinx.serialization.json.*
import kotlin.test.*

internal class ReplayStatementReuseTest : SurvivorTestSupport() {
    @Test fun cachedTextKeyReadsObserveTypedChangesAndMissingRows() = runOwnedWebDatabaseTest {
        typedReads(blobKey = false)
    }

    @Test fun cachedBlobKeyReadsObserveTypedChangesAndMissingRows() = runOwnedWebDatabaseTest {
        typedReads(blobKey = true)
    }

    private suspend fun typedReads(blobKey: Boolean) {
        val db = newDb()
        val server = MockSyncServer()
        val columns = listOf(
            GenerationColumn("id", "uuid", false), GenerationColumn("i", "int64", false),
            GenerationColumn("r", "float64", false), GenerationColumn("t", "text", false),
            GenerationColumn("b", "bytes", false), GenerationColumn("n", "int64", true),
        )
        server.generationOverride = SyncGeneration("1", "", listOf(
            GenerationTable("main", "records", listOf("id"), columns, emptyList()),
        )).let { it.copy(fingerprint = it.computeFingerprint()) }
        val http = server.newHttpClient()
        val tables = listOf(SyncTable("records", syncKeyColumnName = "id"))
        val config = OversqliteConfig(1, tables, "fixture", "main")
        val client = newClient(db, http, syncTables = tables)
        val first = "00112233-4455-6677-8899-aabbccddeeff"
        val second = "10112233-4455-6677-8899-aabbccddeeff"
        fun localKey(key: String) = if (blobKey) key.replace("-", "") else key
        fun literal(key: String) = if (blobKey) "X'${localKey(key)}'" else "'$key'"
        try {
            db.execSQL("CREATE TABLE records(id ${if (blobKey) "BLOB" else "TEXT"} PRIMARY KEY NOT NULL,i INTEGER NOT NULL,r REAL NOT NULL,t TEXT NOT NULL,b BLOB NOT NULL,n INTEGER)")
            db.execSQL("CREATE TABLE signals(id INTEGER PRIMARY KEY)")
            db.execSQL("CREATE TRIGGER change_observed AFTER INSERT ON signals BEGIN UPDATE records SET i=-9223372036854775808,r=-2.5,t='trigger 猫',b=X'',n=7 WHERE id=${literal(first)}; END")
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO records VALUES(${literal(first)},9007199254740993,1.25,'first 猫',X'00ff',NULL),(${literal(second)},17,3.5,'second',X'0100',9)")
            val state = state(db, config)
            val owner = owner(db, http, config, state)
            val observed = CacheObservation()
            val executor = OversqliteApplyExecutor(db, OversqliteApplyStateStore(db))
            executor.faultInjector = SnapshotApplyFaultInjector(statementCacheOperations = observed.operations)
            executor.inApplyModeTransaction(state) { cache ->
                owner.prepare(state, applyUnit("typed-read", 0, ""), 0, replayStatementCache = cache) {
                    error("zero roots must not call seed")
                }
                suspend fun read(key: String): JsonObject? = owner.capture(
                    state, "records", localKey(key), replayStatementCache = cache,
                ).payload?.let { Json.parseToJsonElement(it).jsonObject }

                val original = assertNotNull(read(first))
                assertCell(original, "id", if (blobKey) "blob" else "text", localKey(first))
                assertCell(original, "i", "integer", "9007199254740993")
                assertCell(original, "r", "real", "1.25")
                assertCell(original, "t", "text", "first 猫")
                assertCell(original, "b", "blob", "00ff")
                assertCell(original, "n", "null", null)
                assertNull(read("20112233-4455-6677-8899-aabbccddeeff"))
                val other = assertNotNull(read(second))
                assertCell(other, "i", "integer", "17")
                assertCell(other, "n", "integer", "9")
                assertEquals(original, read(first), "alternating bindings must not retain the previous row")

                db.execSQL("INSERT INTO signals VALUES(1)")
                val changed = assertNotNull(read(first))
                assertCell(changed, "i", "integer", "-9223372036854775808")
                assertCell(changed, "r", "real", "-2.5")
                assertCell(changed, "t", "text", "trigger 猫")
                assertCell(changed, "b", "blob", "")
                assertCell(changed, "n", "integer", "7")
                assertEquals(other, read(second), "a trigger update must not change another key")
                db.execSQL("DELETE FROM records WHERE id=${literal(first)}")
                assertNull(read(first), "a previously observed key is now absent")
                db.execSQL("INSERT INTO records VALUES(${literal(first)},9223372036854775807,0.5,'returned',X'ff00',NULL)")
                val returned = assertNotNull(read(first))
                assertCell(returned, "i", "integer", "9223372036854775807")
                assertCell(returned, "n", "null", null)
                val liveQueries = observed.prepares.filterKeys { it.contains("FROM \"records\" live_row") }
                assertEquals(2, liveQueries.size, "capture size and typed projection both use the opted-in cache")
                liveQueries.forEach { (sql, count) -> assertEquals(1, count, sql) }
                assertTrue(liveQueries.keys.all { observed.resets.getValue(it) > 1 })
                owner.finish()
            }
            observed.assertClosed()
            assertEquals(0L, scalarLong(db, "SELECT apply_mode FROM _sync_apply_state"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_apply_plan"))
        } finally { client.closeFromCoordinatedOwner(); http.close(); db.close() }
    }

    @Test fun cachedRestoreRejectsMalformedTypedPayloadAndRollsBack() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, http, client ->
            try {
                val config = OversqliteConfig(1, nodeTables, "fixture", "main")
                val state = state(db, config)
                val owner = owner(db, http, config, state)
                val observed = CacheObservation()
                val executor = OversqliteApplyExecutor(db, OversqliteApplyStateStore(db))
                executor.faultInjector = SnapshotApplyFaultInjector(statementCacheOperations = observed.operations)
                val before = baselineQualificationVector(db, listOf("nodes") + oversqliteRetainedTableDdl.keys)
                val failure = assertFailsWith<IllegalArgumentException> {
                    executor.inApplyModeTransaction(state) { cache ->
                        owner.prepare(state, applyUnit("malformed-replay", 0, ""), 1, replayStatementCache = cache) {
                            owner.capture(state, "nodes", "P", replayStatementCache = cache)
                        }
                        owner.restore(state, cache, verifyOnly = true, replayStatementCache = cache)
                        db.execSQL("UPDATE _sync_apply_rows SET payload=json_set(payload,'$.name',json('{\"storage\":\"integer\",\"value\":\"1\"}')) WHERE table_name='nodes' AND local_pk='P'")
                        owner.restore(state, cache, verifyOnly = true, replayStatementCache = cache)
                    }
                }
                assertTrue(generateSequence<Throwable>(failure) { it.cause }.any {
                    it.message?.contains("actual SQLite storage integer does not match its declared affinity") == true
                })
                assertEquals(before, baselineQualificationVector(db, listOf("nodes") + oversqliteRetainedTableDdl.keys))
                assertEquals(1, observed.prepares.getValue("SELECT table_name,local_pk,payload,wire FROM _sync_apply_rows WHERE ordinal=?"))
                assertTrue(observed.resets.getValue("SELECT table_name,local_pk,payload,wire FROM _sync_apply_rows WHERE ordinal=?") > 1)
                observed.assertClosed()
            } finally { client.closeFromCoordinatedOwner() }
        }
    }

    @Test fun cachedPlanSavesReplaceEveryBindingAndClearStaleContext() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, http, client ->
            try {
                val config = OversqliteConfig(1, nodeTables, "fixture", "main", maxSyncWorkUnitsPerInvocation = 16)
                val state = state(db, config)
                val observed = CacheObservation()
                val executor = OversqliteApplyExecutor(db, OversqliteApplyStateStore(db))
                executor.faultInjector = SnapshotApplyFaultInjector(statementCacheOperations = observed.operations)
                executor.inApplyModeTransaction(state) { cache ->
                    suspend fun checkpoint(unit: String, kind: String, exhaust: Boolean = true) {
                        val operations = OversqliteClientOperationController(config, OversqliteClientResourceOwner(db, http), SnapshotDiagnosticsState()) {}
                        if (exhaust) operations.reserveSyncWork(unit, 16)
                        val owner = OversqliteApplySurvivors(db, OversqliteLocalStore(db, json) { state.validated }, config, operations, json)
                        val pending = assertFailsWith<ApplyProgressPending> {
                            owner.prepare(state, unit, 5, kind, replayStatementCache = cache) {
                                error("work refusal precedes root callback")
                            }
                        }
                        assertEquals(16L, pending.result.used)
                    }
                    for (kind in listOf("pull", "settlement", "snapshot", "conflict")) {
                        db.execSQL("DELETE FROM _sync_apply_plan")
                        val unit = applyUnit("initial-$kind", 0, "")
                        val guard = OversqliteWorkspace(db, config).contextGuard(state, unit)
                        checkpoint(unit, kind)
                        assertEquals(emptyPlan(unit, guard, kind), readPlan(db))
                    }
                    val unit = applyUnit("loaded-plan", 0, "")
                    val guard = OversqliteWorkspace(db, config).contextGuard(state, unit)
                    // Deliberately unreachable cursor values exercise all bindings after the
                    // work meter is exhausted. This is a persistence-codec witness, not a
                    // valid resumable discovery fixture; stale cleanup resets it before commit.
                    for (cursor in listOf(null, "", "é🙂", null)) {
                        val values = listOf(unit, guard, "roots", Long.MAX_VALUE, 9_007_199_254_740_993L,
                            7L, Long.MIN_VALUE, cursor, 9L, 113L, 2L, 31L, "conflict", "schéma", "表",
                            "{\"id\":\"🙂\"}", Long.MAX_VALUE, 1L)
                        replacePlan(db, values)
                        checkpoint(unit, "conflict")
                        assertEquals(values, readPlan(db), "all 18 typed fields survive reuse, including cursor=$cursor")
                    }
                    val next = applyUnit("replacement-plan", 0, "")
                    val nextGuard = OversqliteWorkspace(db, config).contextGuard(state, next)
                    checkpoint(next, "pull", exhaust = false)
                    assertEquals(emptyPlan(next, nextGuard, "pull"), readPlan(db), "stale context clears all old fields")
                    val writes = observed.prepares.filterKeys { it.startsWith("INSERT INTO _sync_apply_plan(") }
                    assertEquals(1, writes.size)
                    val sql = writes.keys.single()
                    assertEquals(18, sql.count { it == '?' })
                    assertEquals(1, writes.getValue(sql), "one real prepared UPSERT across all payloads")
                    assertEquals(10, observed.resets.getValue(sql), "four initial, four loaded and two stale-clear saves")
                    db.execSQL("DELETE FROM _sync_apply_plan")
                }
                observed.assertClosed()
                assertEquals(0L, scalarLong(db, "SELECT apply_mode FROM _sync_apply_state"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_apply_plan"))
            } finally { client.closeFromCoordinatedOwner() }
        }
    }

    private fun assertCell(row: JsonObject, name: String, storage: String, value: String?) {
        assertEquals(buildJsonObject {
            put("storage", storage)
            put("value", value?.let(::JsonPrimitive) ?: JsonNull)
        }, row[name], name)
    }

    private suspend fun state(db: SafeSQLiteConnection, config: OversqliteConfig): RuntimeState {
        val generation = OversqliteContractStore(db).readGeneration(1)
        val validated = SyncRuntimeInitializer(config, TableInfoCache()).validateConfig(db).withGeneration(generation)
        return RuntimeState(validated, AdmittedOversqliteContract("v1", generation, testWireLimits(), false),
            "user-1", scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
    }

    private fun owner(db: SafeSQLiteConnection, http: HttpClient, config: OversqliteConfig, state: RuntimeState) =
        OversqliteApplySurvivors(db, OversqliteLocalStore(db, json) { state.validated }, config,
            OversqliteClientOperationController(config, OversqliteClientResourceOwner(db, http), SnapshotDiagnosticsState()) {}, json)

    private val planColumns = listOf("unit", "guard", "phase", "root_cursor", "visit_cursor", "table_cursor",
        "fk_cursor", "row_cursor", "entries", "bytes", "edge_entries", "edge_bytes", "operation_kind",
        "preserve_schema", "preserve_table", "preserve_key", "atomic_work", "preservation_complete")
    private val integerColumns = setOf(3, 4, 5, 6, 8, 9, 10, 11, 16, 17)

    private fun emptyPlan(unit: String, guard: String, kind: String): List<Any?> = listOf(
        unit, guard, "roots", 0L, 0L, 0L, 0L, null, 0L, 0L, 0L, 0L, kind, "", "", "", 0L, 0L,
    )

    private suspend fun replacePlan(db: SafeSQLiteConnection, values: List<Any?>) {
        db.withPreparedStatement("UPDATE _sync_apply_plan SET ${planColumns.joinToString(",") { "$it=?" }} WHERE singleton_key=1") { statement ->
            values.forEachIndexed { index, value -> when (value) {
                is Long -> statement.bindLong(index + 1, value)
                is String -> statement.bindText(index + 1, value)
                null -> statement.bindNull(index + 1)
            } }
            statement.step()
        }
    }

    private suspend fun readPlan(db: SafeSQLiteConnection): List<Any?> =
        db.withPreparedStatement("SELECT ${planColumns.joinToString(",")} FROM _sync_apply_plan WHERE singleton_key=1") { statement ->
            assertTrue(statement.step())
            planColumns.indices.map { index ->
                if (index in integerColumns) statement.getLong(index) else if (statement.isNull(index)) null else statement.getText(index)
            }.also { assertFalse(statement.step()) }
        }

    private class CacheObservation {
        val prepares = linkedMapOf<String, Int>()
        val resets = linkedMapOf<String, Int>()
        private val closes = linkedMapOf<String, Int>()
        private val sqlByStatement = mutableMapOf<SQLiteStatement, String>()
        val operations = StatementCacheOperations(
            prepare = { db, sql ->
                prepares[sql] = (prepares[sql] ?: 0) + 1
                db.prepare(sql).also { sqlByStatement[it] = sql }
            },
            reset = { statement ->
                val sql = sqlByStatement.getValue(statement)
                resets[sql] = (resets[sql] ?: 0) + 1
                statement.reset()
            },
            close = { statement ->
                val sql = sqlByStatement.getValue(statement)
                statement.close()
                closes[sql] = (closes[sql] ?: 0) + 1
            },
        )

        fun assertClosed() = assertEquals(prepares, closes, "every cache-owned handle was closed")
    }
}

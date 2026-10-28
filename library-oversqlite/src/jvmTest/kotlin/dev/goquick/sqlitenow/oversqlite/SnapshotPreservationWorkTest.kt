package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.common.SqliteNowLogger
import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.SqliteNowDiagnostics
import androidx.sqlite.SQLiteStatement
import androidx.sqlite.SQLiteConnection
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.serialization.json.*
import kotlin.test.*

internal class SnapshotPreservationWorkTest : SurvivorTestSupport() {
    private class Capture : SqliteNowLogger {
        val records = mutableListOf<JsonObject>()
        private fun write(message: () -> String) { val text = message(); if (text.startsWith("SQLITENOW_DIAG ")) records += Json.parseToJsonElement(text.substringAfter("SQLITENOW_DIAG ")).jsonObject }
        override fun e(throwable: Throwable?, message: () -> String) = write(message)
        override fun w(throwable: Throwable?, message: () -> String) = write(message)
        override fun i(throwable: Throwable?, message: () -> String) = write(message)
        override fun d(throwable: Throwable?, message: () -> String) = write(message)
    }

    @Test fun diagnosticsPreserveCompleteSqlAndRequestSequences() = runOwnedWebDatabaseTest {
        data class Result(val sql: List<String>, val requests: List<String>, val rows: String, val checkpoint: Long)
        suspend fun scenario(enabled: Boolean): Result {
            var result: Result? = null
            withSurvivorFixture { server, db, http, seed ->
                seed.closeFromCoordinatedOwner()
                db.execSQL("CREATE TABLE app_settings(id INTEGER PRIMARY KEY,value TEXT)")
                db.execSQL("INSERT INTO app_settings VALUES(1,'preserved')")
                val config = OversqliteConfig(1, nodeTables, "fixture", "main", diagnostics = SqliteNowDiagnostics(enabled))
                val client = DefaultOversqliteClient.forOpening(db, config, http, ServerWinsResolver, Any())
                val field = SafeSQLiteConnection::class.java.getDeclaredField("ref").also { it.isAccessible = true }
                val original = field.get(db) as SQLiteConnection
                val sequence = mutableListOf<String>()
                try {
                    client.openAndConnect("user-1").getOrThrow()
                    server.addRemoteBundleForTest(listOf(BundleRow("main", "nodes", mapOf("id" to "P"), "UPDATE", 2,
                        buildJsonObject { put("id", "P"); put("name", "changed"); put("parent_id", JsonNull) })))
                    server.requestTrace.clear()
                    field.set(db, object : SQLiteConnection by original {
                        override fun prepare(sql: String): SQLiteStatement {
                            sequence += "prepare:$sql"
                            val statement = original.prepare(sql)
                            return object : SQLiteStatement by statement {
                                override fun step(): Boolean { sequence += "step:$sql"; return statement.step() }
                            }
                        }
                    })
                    client.rebuild().getOrThrow()
                    field.set(db, original)
                    result = Result(sequence.toList(), server.requestTrace.toList(),
                        scalarText(db, "SELECT group_concat(id||':'||name,'|') FROM (SELECT id,name FROM nodes ORDER BY id)"),
                        scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                    assertEquals("preserved", scalarText(db, "SELECT value FROM app_settings WHERE id=1"))
                } finally { field.set(db, original); client.closeFromCoordinatedOwner() }
            }
            return checkNotNull(result)
        }
        val previous = sqliteNowLogger
        sqliteNowLogger = Capture()
        try { assertEquals(withContext(Dispatchers.Default) { scenario(false) }, withContext(Dispatchers.Default) { scenario(true) }) }
        finally { sqliteNowLogger = previous }
    }

    @Test fun memoryPressureEvictsAtRowBoundaryAndGenuineFailuresStillPropagate() = runOwnedWebDatabaseTest {
        for (persistent in listOf(false, true)) withSurvivorFixture { server, db, _, client ->
            try {
                server.addRemoteBundleForTest(listOf(BundleRow("main", "nodes", mapOf("id" to "P"), "UPDATE", 2,
                    buildJsonObject { put("id", "P"); put("name", "changed"); put("parent_id", JsonNull) })))
                var injected = 0
                val releasedOwners = mutableListOf<Pair<Boolean, Boolean>>()
                val operations = StatementCacheOperations(prepare = { connection, sql ->
                    val statement = connection.prepare(sql)
                    val slice = currentCoroutineContext()[OversqliteWorkSlice]
                    object : SQLiteStatement by statement {
                        override fun step(): Boolean {
                            val memo = slice?.preservationMetadata
                            if (sql == "SELECT bytes,payload IS NOT NULL,wire,role,source_ordinal,saved_rowid FROM _sync_apply_rows WHERE ordinal=?" && memo != null &&
                                (memo.charge != WorkspaceCharge() || persistent && injected > 0)) {
                                releasedOwners += (slice.snapshotPlanReader?.retainedCharge != WorkspaceCharge()) to (memo.charge != WorkspaceCharge())
                                injected++
                                throw SyncWorkspaceLimitException("preservation fixture", "bytes", 1, 1, 1, false)
                            }
                            return statement.step()
                        }
                    }
                })
                client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(statementCacheOperations = operations))
                if (persistent) {
                    assertIs<SyncWorkspaceLimitException>(client.rebuild().exceptionOrNull())
                    assertEquals(3, injected, "retry after releasing the plan reader, then preservation metadata, then propagate genuine pressure")
                    assertEquals("parent", scalarText(db, "SELECT name FROM nodes WHERE id='P'"))
                } else {
                    client.rebuild().getOrThrow()
                    assertEquals(2, injected)
                    assertEquals("changed", scalarText(db, "SELECT name FROM nodes WHERE id='P'"))
                }
                assertEquals(listOf(true to true, false to true) + if (persistent) listOf(false to false) else emptyList(), releasedOwners,
                    "each row retry releases one actual optional owner before retrying")
                client.setSnapshotApplyFaultInjectorForTest(null)
                client.rebuild().getOrThrow()
                assertEquals("changed", scalarText(db, "SELECT name FROM nodes WHERE id='P'"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
            } finally { client.setSnapshotApplyFaultInjectorForTest(null); client.closeFromCoordinatedOwner() }
        }
    }

    @Test fun metadataWorkDependsOnSchemaRatherThanRestoredRowCount() = runOwnedWebDatabaseTest {
        for (rows in listOf(16, 64)) withSurvivorFixture { server, db, http, seed ->
            seed.closeFromCoordinatedOwner()
            db.execSQL("CREATE TABLE app_settings(id INTEGER PRIMARY KEY,value TEXT)")
            db.execSQL("INSERT INTO app_settings VALUES(1,'preserved')")
            db.execSQL("CREATE TABLE local_refs(id INTEGER PRIMARY KEY,parent TEXT REFERENCES nodes(id))")
            val incoming = (0 until rows - 4).map { index -> BundleRow("main", "nodes", mapOf("id" to "R$index"), "INSERT", 2,
                buildJsonObject { put("id", "R$index"); put("name", "row-$index"); put("parent_id", JsonNull) }) } + listOf("P" to null, "Q" to null, "C" to "P", "D" to "C").map { (id, parent) ->
                BundleRow("main", "nodes", mapOf("id" to id), "UPDATE", 2, buildJsonObject {
                    put("id", id); put("name", "changed-$id"); put("parent_id", parent?.let(::JsonPrimitive) ?: JsonNull)
                })
            }
            val capture = Capture(); val previous = sqliteNowLogger; sqliteNowLogger = capture
            val diagnostics = SqliteNowDiagnostics(true)
            val config = OversqliteConfig(1, nodeTables, "fixture", "main", diagnostics = diagnostics)
            val client = DefaultOversqliteClient.forOpening(db, config, http, ServerWinsResolver, Any())
            try {
                client.openAndConnect("user-1").getOrThrow()
                server.addRemoteBundleForTest(incoming)
                capture.records.clear()
                val observation = ReplaySqlObservation.install(db)
                try {
                    client.rebuild().getOrThrow()
                    val summary = capture.records.single { it["name"]?.jsonPrimitive?.content == "snapshot.preservation_metadata" }["counts"]!!.jsonObject
                    assertEquals(1, summary["catalog_enumerations"]!!.jsonPrimitive.int)
                    assertEquals(2, summary["foreign_key_metadata_loads"]!!.jsonPrimitive.int)
                    assertEquals(1, summary["trigger_tables_checked"]!!.jsonPrimitive.int)
                    assertEquals(rows, summary["row_checks"]!!.jsonPrimitive.int)
                    assertEquals(rows, summary["incoming_row_checks"]!!.jsonPrimitive.int)
                    assertEquals(0, summary["cache_fallbacks"]!!.jsonPrimitive.int)
                    val catalog = observation.counts.filterKeys { it == "SELECT name FROM sqlite_schema WHERE type='table' AND name>'' AND name NOT LIKE 'sqlite_%' ORDER BY name" }.values.single()
                    assertEquals(1L, catalog.prepares)
                    assertEquals(1L, catalog.resets)
                    assertEquals(1L, catalog.closes)
                    assertFalse(observation.counts.keys.any { it.startsWith("SELECT id FROM pragma_foreign_key_list(?) WHERE lower") })
                    assertEquals(rows.toLong(), scalarLong(db, "SELECT count(*) FROM nodes"))
                    assertEquals("preserved", scalarText(db, "SELECT value FROM app_settings WHERE id=1"))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_snapshot_stage"))
                } finally { observation.restore() }
            } finally { client.closeFromCoordinatedOwner(); sqliteNowLogger = previous }
        }
    }
}

package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import kotlinx.serialization.json.*
import kotlin.test.*

internal fun provenanceTestState(client: DefaultOversqliteClient): RuntimeState {
    val field = DefaultOversqliteClient::class.java.getDeclaredField("sessionState").also { it.isAccessible = true }
    val session = field.get(client) as OversqliteClientSessionState
    return RuntimeState(session.requireValidatedConfig(), session.requireAdmittedContract(),
        checkNotNull(session.currentUserId), checkNotNull(session.currentSourceId))
}

internal fun provenanceTestConfig(client: DefaultOversqliteClient): OversqliteConfig =
    DefaultOversqliteClient::class.java.getDeclaredField("config").let { it.isAccessible = true; it.get(client) as OversqliteConfig }

internal class NormalizedProvenanceStorageTest : SurvivorTestSupport() {
    @Test fun snapshotIncludedAndExcludedRowsUseTargetContextAndPhysicallyReopen() = runOwnedWebDatabaseTest {
        val path = createTempSqliteNowTestDbPath("snapshot-context-target")
        try {
            withSurvivorFixture(dbPath = path) { server, db, http, client ->
                server.addRemoteBundleForTest(listOf(
                    BundleRow("main", "nodes", mapOf("id" to "Q"), "DELETE", 2, null),
                    BundleRow("main", "nodes", mapOf("id" to "R"), "INSERT", 2,
                        buildJsonObject { put("id", "R"); put("name", "replacement"); put("parent_id", JsonNull) }),
                ))
                val replacement = randomTestSourceId()
                markSourceRecoveryRequired(db, SourceRecoveryReason.HISTORY_PRUNED, replacement)
                val oldSource = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
                val original = db.ref
                var observedBeforePublication = false
                fun scalar(sql: String): String {
                    val st = original.prepare(sql)
                    try { check(st.step()); return st.getText(0) } finally { st.close() }
                }
                val wrapper = object : SQLiteConnection by original {
                    override fun prepare(sql: String): SQLiteStatement {
                        val st = original.prepare(sql)
                        if (!sql.trimStart().startsWith("UPDATE _sync_attachment_state")) return st
                        return object : SQLiteStatement by st {
                            override fun step(): Boolean {
                                if (!observedBeforePublication) {
                                    val all = scalar("SELECT count(*) FROM _sync_row_state").toLong()
                                    val target = scalar("SELECT count(*) FROM _sync_row_state r JOIN _sync_provenance_contexts c ON c.context_id=r.server_context_id WHERE c.origin_source_id='$replacement' AND c.account_id='user-1' AND c.generation_id=1").toLong()
                                    if (all > 0 && target == all) {
                                        assertEquals(oldSource, scalar("SELECT current_source_id FROM _sync_attachment_state"))
                                        observedBeforePublication = true
                                    }
                                }
                                return st.step()
                            }
                        }
                    }
                }
                val field = SafeSQLiteConnection::class.java.getDeclaredField("ref").also { it.isAccessible = true }
                db.withExclusiveAccess { field.set(db, wrapper) }
                try { client.rebuild().getOrThrow() } finally { db.withExclusiveAccess { field.set(db, original) } }
                assertTrue(observedBeforePublication, "included and excluded target contexts exist before attachment publication")
                assertEquals(replacement, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
                assertEquals("absent", scalarText(db, "SELECT server_kind FROM _sync_row_state WHERE json_extract(key_json,'$.id')='Q'"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_row_state r JOIN _sync_provenance_contexts c ON c.context_id=r.server_context_id WHERE c.origin_source_id<>'$replacement' OR c.account_id<>'user-1' OR c.generation_id<>1"))
                assertEquals(1L, scalarLong(db, "SELECT count(DISTINCT server_context_id) FROM _sync_row_state"))
                for (table in listOf("_sync_dirty_rows", "_sync_outbox_rows", "_sync_snapshot_stage", "_sync_apply_rows"))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM $table"), table)
                client.closeFromCoordinatedOwner(); db.close()
                val reopened = newFileBackedDb(path)
                val next = newClient(reopened, http, syncTables = nodeTables)
                try {
                    next.openAndConnect("user-1").getOrThrow()
                    assertEquals("replacement", scalarText(reopened, "SELECT name FROM nodes WHERE id='R'"))
                    assertEquals(0L, scalarLong(reopened, "SELECT count(*) FROM nodes WHERE id='Q'"))
                    assertEquals(replacement, scalarText(reopened, "SELECT current_source_id FROM _sync_attachment_state"))
                    assertEquals(0L, scalarLong(reopened, "SELECT count(*) FROM pragma_foreign_key_check"))
                    assertEquals("ok", scalarText(reopened, "PRAGMA integrity_check"))
                } finally { next.closeFromCoordinatedOwner(); reopened.close() }
            }
        } finally { deleteTempSqliteNowTestDbArtifacts(path) }
    }

    @Test fun oneContextStorageReductionOnDeterministicFourteenTableFixture() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val tables = (0 until 14).map { index -> if (index == 0) "daily_logs" else "restored_records_$index" }
        val columns = listOf(GenerationColumn("id", "text", false), GenerationColumn("content", "text", false),
            GenerationColumn("recorded_on", "text", true), GenerationColumn("annotation", "text", true))
        server.generationOverride = SyncGeneration("1", "", tables.map { GenerationTable("main", it, listOf("id"), columns, emptyList()) })
            .let { it.copy(fingerprint = it.computeFingerprint()) }
        val db = newDb(); val http = server.newHttpClient()
        val client = newClient(db, http, syncTables = tables.map { SyncTable(it, syncKeyColumnName = "id") }, workUnits = 100_000_000)
        try {
            for (table in tables) db.execSQL("CREATE TABLE $table(id TEXT PRIMARY KEY NOT NULL,content TEXT NOT NULL,recorded_on TEXT,annotation TEXT)")
            client.openAndConnect("user-1").getOrThrow()
            val counts = listOf(1329) + List(12) { 53 } + 54
            val rows = tables.flatMapIndexed { tableIndex, table -> (0 until counts[tableIndex]).map { row ->
                val id = "row-$row"
                BundleRow("main", table, mapOf("id" to id), "INSERT", 1, buildJsonObject {
                    put("id", id); put("content", "synthetic-${row % 17}-" + "x".repeat(row % 37))
                    put("recorded_on", java.time.LocalDate.of(2026, 7, 30).plusDays((row % 90).toLong()).toString())
                    put("annotation", JsonNull)
                })
            } }
            assertEquals(2019, rows.size)
            server.addRemoteBundleForTest(rows)
            client.rebuild().getOrThrow()
            val business = buildJsonArray {
                for (table in tables) {
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM $table WHERE typeof(id)<>'text' OR typeof(content)<>'text' OR typeof(recorded_on)<>'text' OR typeof(annotation)<>'null'"))
                    val actual = db.queryList("SELECT id,content,recorded_on,annotation FROM $table ORDER BY id") { st ->
                        buildJsonObject { put("id", st.getText(0)); put("content", st.getText(1)); put("recorded_on", st.getText(2)); put("annotation", if (st.isNull(3)) JsonNull else JsonPrimitive(st.getText(3))) }
                    }
                    add(buildJsonObject { put("table", table); put("rows", JsonArray(actual)) })
                }
            }
            assertEquals("ca113e857968c3d7b332063074cdc2adbfead618dc9d4dd126f44449e1802dc9", sha256Hex(canonicalizeJsonElement(business).encodeToByteArray()))
            assertEquals(2019L, scalarLong(db, "SELECT count(*) FROM _sync_row_state"))
            assertEquals(1329L, scalarLong(db, "SELECT count(*) FROM daily_logs"))
            assertEquals(90L, scalarLong(db, "SELECT count(DISTINCT recorded_on) FROM daily_logs"))
            assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_provenance_contexts"))
            val contextBytes = scalarLong(db, "SELECT sum(length(CAST(database_id AS BLOB))+length(CAST(service_id AS BLOB))+length(CAST(account_binding AS BLOB))+length(CAST(account_id AS BLOB))+length(CAST(generation_fingerprint AS BLOB))+length(CAST(admission AS BLOB))+length(CAST(physical_shape AS BLOB))+length(CAST(origin_source_id AS BLOB))+24) FROM _sync_provenance_contexts")
            val retained = contextBytes + scalarLong(db, "SELECT sum(length(CAST(server_provenance AS BLOB))+8) FROM _sync_row_state") + 64
            val legacyPerRow = scalarLong(db, "SELECT length(CAST(json_object('database_id',database_id,'service_id',service_id,'account_scope',json_object('binding',account_binding,'account_id',account_id),'generation_id',generation_id,'generation_fingerprint',generation_fingerprint,'admission',admission,'local_version',local_version,'physical_shape',physical_shape,'origin_source_id',origin_source_id,'origin','snapshot','bundle_sequence',1,'authoritative_version',1) AS BLOB)) FROM _sync_provenance_contexts")
            assertTrue(retained * 10 <= legacyPerRow * 2019, "logical provenance: $retained versus reconstructed legacy ${legacyPerRow * 2019}")
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
            println("NORMALIZED_STORAGE rows=2019 daily_logs=1329 dates=90 contexts=1 logical_before=${legacyPerRow * 2019} logical_after=$retained payload_bytes=" + scalarLong(db, "SELECT sum(length(CAST(server_payload AS BLOB))) FROM _sync_row_state"))
        } finally { client.closeFromCoordinatedOwner(); http.close(); db.close() }
    }

    @Test fun danglingContextIsRejectedOnPhysicalReopenWithoutChangingBusinessData() = runOwnedWebDatabaseTest {
        corruptedReopen(dropLayout = false)
    }

    @Test fun oldLayoutAbsenceIsRejectedWithoutImplicitRecreation() = runOwnedWebDatabaseTest {
        corruptedReopen(dropLayout = true)
    }

    private suspend fun corruptedReopen(dropLayout: Boolean) {
        val path = createTempSqliteNowTestDbPath("context-integrity")
        try {
            withSurvivorFixture(dbPath = path) { _, db, http, client ->
                client.closeFromCoordinatedOwner()
                db.execSQL("PRAGMA foreign_keys=OFF")
                if (dropLayout) {
                    db.execSQL("DROP TABLE _sync_provenance_state")
                    db.execSQL("DROP TABLE _sync_provenance_contexts")
                } else db.execSQL("UPDATE _sync_row_state SET server_provenance=json_set(server_provenance,'$.context_id',999999)")
                db.close()
                val reopened = newFileBackedDb(path)
                var next: DefaultOversqliteClient? = null
                try {
                    val failure = runCatching {
                        next = newClient(reopened, http, syncTables = nodeTables)
                        checkNotNull(next).openAndConnect("user-1").getOrThrow()
                    }.exceptionOrNull()
                    assertNotNull(failure)
                    assertEquals(4L, scalarLong(reopened, "SELECT count(*) FROM nodes"))
                    assertEquals("parent", scalarText(reopened, "SELECT name FROM nodes WHERE id='P'"))
                } finally { next?.closeFromCoordinatedOwner(); reopened.close() }
            }
        } finally { deleteTempSqliteNowTestDbArtifacts(path) }
    }
}

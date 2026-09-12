package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.*
import kotlin.test.*
import kotlin.time.Duration.Companion.seconds

/** Exercises validated ownership, schema recreation and durable committed replay observation. */
internal class ReplayObservationLifecycleTest : SurvivorTestSupport() {
    @Test fun admissionAndReopenExposeActualValidatedOwners() = runOwnedWebDatabaseTest {
        val path = createTempSqliteNowTestDbPath("replay-owner")
        val server = MockSyncServer()
        val http = server.newHttpClient()
        val db = newFileBackedDb(path)
        val client = newClient(db, http)
        try {
            createUsersAndPostsTables(db)
            db.execSQL("INSERT INTO users VALUES('u1', X'00ff')")
            client.open().getOrThrow()
            val unconfirmed = replayValidatedOwner(client)
            assertNull(unconfirmed.generation)
            assertSame(unconfirmed, unconfirmed.withGeneration(null))
            assertNotSame(unconfirmed, unconfirmed.copy())
            val before = baselineQualificationVector(db)
            assertTrue(client.attach("user-1").isFailure)
            assertSame(unconfirmed, replayValidatedOwner(client), "rejected candidate is not published")
            assertEquals(before, baselineQualificationVector(db))
            db.execSQL("UPDATE users SET name='valid' WHERE id='u1'")
            client.attach("user-1").getOrThrow()
            val admitted = replayValidatedOwner(client)
            assertNotSame(unconfirmed, admitted)
            assertEquals("text", admitted.tableInfoByName.getValue("users").column("name").wireType)
            assertNull(unconfirmed.tableInfoByName.getValue("users").column("name").wireType)
            assertSame(admitted, replayValidatedOwner(client))
            val durable = baselineQualificationVector(db)
            client.closeFromCoordinatedOwner()
            db.close()
            val reopenedDb = newFileBackedDb(path)
            val reopened = newClient(reopenedDb, http)
            try {
                assertEquals(durable, baselineQualificationVector(reopenedDb))
                reopened.openAndConnect("user-1").getOrThrow()
                val next = replayValidatedOwner(reopened)
                assertNotSame(admitted, next)
                assertEquals(admitted, next, "equal schemas still have distinct owners")
                assertEquals(durable, baselineQualificationVector(reopenedDb))
            } finally { reopened.closeFromCoordinatedOwner(); reopenedDb.close() }
        } finally {
            withContext(NonCancellable) { client.closeFromCoordinatedOwner(); db.close(); http.close(); deleteTempSqliteNowTestDbArtifacts(path) }
        }
    }

    @Test fun ownedSchemaRecreationRevalidatesColumnsForeignKeysAndKeys() = runOwnedWebDatabaseTest {
        val path = createTempSqliteNowTestDbPath("replay-schema")
        var previous: ValidatedConfig? = null
        try {
            for (blob in listOf(false, true)) {
                val server = MockSyncServer()
                val parentColumns = if (blob) listOf(GenerationColumn("label", "text", false), GenerationColumn("id", "uuid", false))
                    else listOf(GenerationColumn("id", "uuid", false), GenerationColumn("label", "text", false))
                val columns = listOf(GenerationColumn("id", "uuid", false), GenerationColumn("parent_id", "uuid", true))
                server.generationOverride = SyncGeneration("1", "", listOf(
                    GenerationTable("main", "parents", listOf("id"), parentColumns, emptyList()),
                    GenerationTable("main", "children", listOf("id"), columns,
                        listOf(GenerationForeignKey(listOf("parent_id"), "main", "parents", listOf("id"), "no_action", "no_action"))),
                )).let { it.copy(fingerprint = it.computeFingerprint()) }
                val http = server.newHttpClient()
                val db = newFileBackedDb(path)
                val client = newClient(db, http, syncTables = listOf(SyncTable("parents", "id"), SyncTable("children", "id")))
                try {
                    val keyType = if (blob) "BLOB" else "TEXT"
                    db.execSQL(if (blob) "CREATE TABLE parents(label TEXT NOT NULL,id BLOB PRIMARY KEY NOT NULL)"
                        else "CREATE TABLE parents(id TEXT PRIMARY KEY NOT NULL,label TEXT NOT NULL)")
                    db.execSQL("CREATE TABLE children(id $keyType PRIMARY KEY NOT NULL,parent_id $keyType REFERENCES parents(id))")
                    client.openAndConnect("user-1").getOrThrow()
                    val current = replayValidatedOwner(client)
                    assertEquals(blob, current.tableInfoByName.getValue("parents").primaryKeyIsBlob)
                    assertEquals(blob, current.tableInfoByName.getValue("children").primaryKeyIsBlob)
                    assertEquals(parentColumns.map { it.name }, current.tableInfoByName.getValue("parents").columns.map { it.name })
                    assertEquals("parents", current.tableInfoByName.getValue("children").foreignKeys.single().refTable)
                    previous?.let { old ->
                        assertNotSame(old, current)
                        assertFalse(old.tableInfoByName.getValue("parents").primaryKeyIsBlob)
                        assertEquals(listOf("id", "label"), old.tableInfoByName.getValue("parents").columns.map { it.name })
                    }
                    previous = current
                    client.closeFromCoordinatedOwner()
                    // Negative control: arbitrary DDL cannot masquerade as a supported reopen.
                    db.execSQL("ALTER TABLE parents ADD COLUMN extra TEXT")
                    val before = baselineQualificationVector(db, listOf("parents", "children"))
                    val rejected = newClient(db, http, syncTables = listOf(SyncTable("parents", "id"), SyncTable("children", "id")))
                    try {
                        assertTrue(rejected.open().isFailure)
                        assertEquals(before, baselineQualificationVector(db, listOf("parents", "children")))
                    } finally { rejected.closeFromCoordinatedOwner() }
                } finally { client.closeFromCoordinatedOwner(); http.close(); db.close() }
                // Explicitly recreate only this owned fixture after all owners have closed.
                deleteTempSqliteNowTestDbArtifacts(path)
            }
        } finally { deleteTempSqliteNowTestDbArtifacts(path) }
    }

    @Test fun committedReplayObservationsCoverSmallAndOneThousandRows() = runOwnedWebDatabaseTest(timeout = 180.seconds) {
        val construction = ReplayConstructionObservation.load()
        assertEquals(System.getenv("REPLAY_CONSTRUCTION_OVERLAY") == "true", construction != null,
            "normal output must exclude the temporary construction observer; instrumented execution must include it")
        for (size in listOf(0, 1000)) {
            val path = createTempSqliteNowTestDbPath("replay-observation-$size")
            try {
                withSurvivorFixture(workUnits = 5_000_000, dbPath = path) { server, db, _, client ->
                    try {
                        db.execSQL("UPDATE nodes SET name='changed parent' WHERE id='P'")
                        server.normalizePushEffects = { effects() }
                        server.beforePushResponse = { throw CancellationException("lost committed response") }
                        assertFailsWith<CancellationException> { client.pushPending().getOrThrow() }
                        val frozen = server.pushBodies.last()
                        // Add unrelated dirty intent only after the one-row upload is frozen.
                        db.transaction {
                            repeat(size) { db.execSQL("INSERT INTO nodes VALUES('Z${it.toString().padStart(5, '0')}','${"x".repeat(16)}',NULL)") }
                        }
                        val unrelatedSql = "SELECT coalesce(json_group_array(payload),'[]') FROM (SELECT payload FROM _sync_dirty_rows WHERE json_extract(key_json,'$.id') LIKE 'Z%' ORDER BY key_json)"
                        val unrelated = scalarText(db, unrelatedSql)
                        val observer = ReplaySqlObservation.install(db)
                        val cachedSql = mutableListOf<String>()
                        construction?.reset()
                        client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(statementCacheOperations = StatementCacheOperations(
                            prepare = { connection, sql -> cachedSql += sql; connection.prepare(sql) },
                        )))
                        try { client.pushPending().getOrThrow() }
                        finally { client.setSnapshotApplyFaultInjectorForTest(null); observer.restore() }
                        val constructionSnapshot = construction?.snapshot()
                        requireSurvivors(db)
                        assertEquals(frozen, server.pushBodies.last())
                        assertEquals(3, server.pushRequestCount)
                        assertEquals(3L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
                        assertEquals(2L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
                        assertEquals(size.toLong(), scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
                        assertEquals(unrelated, scalarText(db, unrelatedSql))
                        assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes FROM _sync_operation_state"))
                        assertTrue(observer.selectedExecutions > 0)
                        assertTrue(observer.counts.keys.any { ReplaySqlObservation.selected(it) && it !in cachedSql }, "uncached business queries observed")
                        assertTrue(cachedSql.any { it.contains("FROM _sync_operation_state o") }, "real settlement cache observed")
                        observer.counts.forEach { (sql, count) -> assertEquals(count.prepares, count.closes, "all observed handles closed: $sql") }
                        val durable = baselineQualificationVector(db, listOf("nodes"))
                        client.closeFromCoordinatedOwner()
                        db.close()
                        val reopened = newFileBackedDb(path)
                        try { assertEquals(durable, baselineQualificationVector(reopened, listOf("nodes"))) }
                        finally { reopened.close() }
                        println("REPLAY_OBSERVATION " + buildJsonObject {
                            put("size", size); put("bindingTrace", observer.bindingTraceHash())
                            put("selectedExecutions", observer.selectedExecutions)
                            put("accounting", client.workspaceDiagnostics().toString())
                            put("sql", buildJsonObject { observer.counts.toSortedMap().forEach { (sql, c) ->
                                put(sql, buildJsonObject { put("prepares", c.prepares); put("steps", c.steps); put("resets", c.resets); put("clears", c.clears); put("closes", c.closes) })
                            } })
                            put("construction", constructionSnapshot?.let(Json::parseToJsonElement) ?: JsonNull)
                        })
                    } finally { client.closeFromCoordinatedOwner() }
                }
            } finally { deleteTempSqliteNowTestDbArtifacts(path) }
        }
    }
}

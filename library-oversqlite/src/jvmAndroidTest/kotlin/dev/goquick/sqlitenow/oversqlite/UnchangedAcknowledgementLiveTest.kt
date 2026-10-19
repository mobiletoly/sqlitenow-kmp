package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import io.ktor.client.HttpClient
import io.ktor.client.statement.HttpReceivePipeline
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/** Real transport and durable SQLite complement the mock settlement benchmark. */
internal class UnchangedAcknowledgementLiveTest : RealServerSupport() {
    @Test
    fun oneRowPushAvoidsRelationshipIndexAcrossDatabaseSizes() = benchmark(0)

    @Test
    fun oneRowPushWithPendingIntentAvoidsRelationshipIndexAcrossDatabaseSizes() = benchmark(1)

    private fun benchmark(pending: Int) = runOwnedWebDatabaseTest(timeout = 240.seconds) {
        withContext(Dispatchers.Default) {
            if (realServerEnv("OVERSQLITE_REALSERVER_TESTS")?.lowercase() !in listOf("true", "1", "yes", "on")) return@withContext
            val config = checkNotNull(requireRealServerConfig()) { "Opted-in real server must be available" }
            val path = createTempSqliteNowTestDbPath("unchanged-ack-live")
            val db = newFileBackedDb(path)
            val user = randomRealServerId("unchanged-ack")
            val parent = randomRealServerUuid()
            val water = randomRealServerUuid()
            var client: DefaultOversqliteClient? = null
            var http: HttpClient? = null
            val pendingIds = if (pending == 0) emptyList() else List(32) { randomRealServerUuid() }
            var pendingCount = pending
            var pendingBefore: List<String>? = null
            val observations = mutableListOf<Pair<Int, Counts>>()
            var observeResponse = false
            var observation: SettlementObservation? = null
            var responseStarted = 0L
            try {
                createBusinessRichSchemaTables(db)
                val source = bootstrapManagedSourceId(db, config.baseUrl)
                val original = newRealServerHttpClient(config.baseUrl, issueDummySigninToken(config.baseUrl, user, source))
                val transport = original.config {
                    install("MeasureCommittedAcknowledgement") {
                        receivePipeline.intercept(HttpReceivePipeline.Before) { response ->
                            if (observeResponse && response.call.request.url.encodedPath == "/sync/push") {
                                assertTrue(response.status.value in 200..299)
                                observeResponse = false
                                if (pendingCount > 0) {
                                    repeat(pendingCount) { index -> db.execSQL("UPDATE categories SET name='pending' WHERE id='${pendingIds[index]}'") }
                                    pendingBefore = pendingState(db)
                                }
                                observation = SettlementObservation.install(db)
                                responseStarted = System.nanoTime()
                            }
                        }
                    }
                }
                original.close()
                http = transport
                val writer = newRealServerClient(db, transport, maxPushRows = 200)
                client = writer
                writer.open().getOrThrow()
                writer.attach(user).getOrThrow()
                db.execSQL("INSERT INTO categories VALUES('$parent','unrelated parent',NULL),('$water','water',NULL)")
                pendingIds.forEach { db.execSQL("INSERT INTO categories VALUES('$it','published','$water')") }
                writer.pushPending().getOrThrow()
                var population = 0
                var expectedShape: Pair<Long, Long>? = null
                val cases = if (pending == 0) listOf(100 to 0, 1000 to 0, 10000 to 0)
                    else listOf(100 to 1, 1000 to 1, 10000 to 1, 10000 to 32)
                for ((size, retained) in cases) {
                    pendingCount = retained
                    // Publish clean batches so fixture setup does not benchmark a pending backlog.
                    // Every measured relationship also exists on the real server.
                    while (population < size) {
                        val batch = minOf(200, size - population)
                        db.transaction {
                            repeat(batch) {
                                db.execSQL("INSERT INTO categories VALUES('${randomRealServerUuid()}','unrelated','$parent')")
                            }
                        }
                        writer.pushPending().getOrThrow()
                        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"), "seed batch must be fully acknowledged")
                        population += batch
                    }
                    assertEquals(size + 2L + pendingIds.size, scalarLong(db, "SELECT count(*) FROM _sync_row_state"))
                    repeat(4) { sample ->
                        val name = "water-$size-$sample"
                        db.execSQL("UPDATE categories SET name='$name' WHERE id='$water'")
                        assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
                        val sequence = scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state")
                        val checkpoint = scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state")
                        observeResponse = true
                        val callStarted = System.nanoTime()
                        try {
                            writer.pushPending().getOrThrow()
                            val finished = System.nanoTime()
                            val counts = checkNotNull(observation) { "successful real push response hook must execute" }.counts.copy()
                            val settlementMs = (finished - responseStarted) / 1_000_000.0
                            val callMs = (finished - callStarted) / 1_000_000.0
                            observation?.restore()
                            observation = null
                            if (pending == 0) {
                                assertEquals(0L, counts.edges, "unchanged acknowledgement must skip the survivor edge index")
                                assertEquals(0L, counts.scans, "no explicit managed-row/FK enumeration")
                                assertTrue(counts.steps < 1000)
                            } else assertEquals(pendingBefore, pendingState(db), "pending payload/base/provenance/relationships remain exact")
                            if (sample > 0) {
                                observations += pendingCount to counts
                                if (pending == 0) {
                                    val shape = counts.prepares to counts.steps
                                    expectedShape?.let { assertEquals(it, shape, "whole settlement SQL count must not grow with unrelated rows") }
                                    expectedShape = shape
                                }
                                println("LIVE_ACK_SQL rows=$size pending=$pendingCount sample=$sample prepares=${counts.prepares} steps=${counts.steps} edges=${counts.edges} scans=${counts.scans} settlementMs=$settlementMs callMs=$callMs")
                            }
                            assertEquals(sequence + 1, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
                            assertEquals(checkpoint + 1, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                            assertEquals(name, scalarText(db, "SELECT name FROM categories WHERE id='$water'"))
                            assertEquals(size.toLong(), scalarLong(db, "SELECT count(*) FROM categories WHERE parent_id='$parent'"))
                            assertClean(db, pendingCount)
                            if (pendingCount > 0) writer.pushPending().getOrThrow()
                        } finally {
                            observeResponse = false
                            observation?.restore()
                            observation = null
                        }
                    }
                }
                // A separate durable client verifies authoritative snapshot and a subsequent incremental pull.
                val followerPath = createTempSqliteNowTestDbPath("unchanged-ack-follower")
                val followerDb = newFileBackedDb(followerPath)
                var follower: DefaultOversqliteClient? = null
                var followerHttp: HttpClient? = null
                try {
                    createBusinessRichSchemaTables(followerDb)
                    val followerSource = bootstrapManagedSourceId(followerDb, config.baseUrl)
                    followerHttp = newRealServerHttpClient(config.baseUrl, issueDummySigninToken(config.baseUrl, user, followerSource))
                    val reader = newRealServerClient(followerDb, followerHttp)
                    follower = reader
                    reader.open().getOrThrow()
                    reader.attach(user).getOrThrow()
                    reader.rebuild().getOrThrow()
                    assertEquals(10002L + pendingIds.size, scalarLong(followerDb, "SELECT count(*) FROM categories"))
                    assertEquals(10000L, scalarLong(followerDb, "SELECT count(*) FROM categories WHERE parent_id='$parent'"))
                    assertEquals("water-10000-3", scalarText(followerDb, "SELECT name FROM categories WHERE id='$water'"))
                    db.execSQL("UPDATE categories SET name='water-followed' WHERE id='$water'")
                    writer.pushPending().getOrThrow()
                    reader.pullToStable().getOrThrow()
                    assertEquals("water-followed", scalarText(followerDb, "SELECT name FROM categories WHERE id='$water'"))
                    assertEquals(scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"), scalarLong(followerDb, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                    assertClean(db)
                    assertClean(followerDb)
                    println("LIVE_ACK_VERIFIED rows=10000 pendingMode=$pending durableWriter=true durableFollower=true snapshot=true incrementalPull=true")
                } finally {
                    follower?.closeFromCoordinatedOwner()
                    followerHttp?.close()
                    followerDb.close()
                    deleteTempSqliteNowTestDbArtifacts(followerPath)
                }
                observations.forEach { (_, counts) ->
                    assertEquals(0L, counts.edges, "unchanged acknowledgement with pending edits skips global index")
                    assertEquals(0L, counts.scans)
                }
                if (pending > 0) assertEquals(1, observations.filter { it.first == 1 }.map { it.second.prepares to it.second.steps }.distinct().size,
                    "fixed pending population must not scale SQL count with clean rows")
            } finally {
                observation?.restore()
                client?.closeFromCoordinatedOwner()
                http?.close()
                db.close()
                deleteTempSqliteNowTestDbArtifacts(path)
            }
        }
    }

    @Test
    fun pendingIntentSurvivesCancelledAcknowledgementAndPhysicalReopen() = runOwnedWebDatabaseTest(timeout = 240.seconds) {
        withContext(Dispatchers.Default) {
            if (realServerEnv("OVERSQLITE_REALSERVER_TESTS")?.lowercase() !in listOf("true", "1", "yes", "on")) return@withContext
            val config = checkNotNull(requireRealServerConfig())
            for (operation in listOf("INSERT", "UPDATE", "DELETE")) {
                val path = createTempSqliteNowTestDbPath("pending-ack-reopen")
                var db = newFileBackedDb(path)
                var client: DefaultOversqliteClient? = null
                var http: HttpClient? = null
                val root = randomRealServerUuid()
                val other = randomRealServerUuid()
                val user = randomRealServerId("pending-reopen")
                var injectIntent = false
                var before: List<String>? = null
                var frozenHash: String? = null
                try {
                    createBusinessRichSchemaTables(db)
                    val source = bootstrapManagedSourceId(db, config.baseUrl)
                    val original = newRealServerHttpClient(config.baseUrl, issueDummySigninToken(config.baseUrl, user, source), beforeRequest = { route ->
                        if (route == "/sync/push" && frozenHash != null) assertEquals(frozenHash, scalarText(db, "SELECT canonical_request_hash FROM _sync_outbox_bundle"))
                    })
                    val transport = original.config {
                        install("PendingIntentResponse") {
                            receivePipeline.intercept(HttpReceivePipeline.Before) { response ->
                                if (injectIntent && response.call.request.url.encodedPath == "/sync/push") {
                                    assertTrue(response.status.value in 200..299)
                                    injectIntent = false
                                    db.execSQL(when (operation) {
                                        "INSERT" -> "INSERT INTO categories VALUES('$other','pending','$root')"
                                        "UPDATE" -> "UPDATE categories SET name='pending' WHERE id='$other'"
                                        else -> "DELETE FROM categories WHERE id='$other'"
                                    })
                                    before = pendingState(db)
                                }
                            }
                        }
                    }
                    original.close(); http = transport
                    var active = newRealServerClient(db, transport)
                    client = active
                    active.open().getOrThrow(); active.attach(user).getOrThrow()
                    db.execSQL("INSERT INTO categories VALUES('$root','published',NULL)")
                    if (operation != "INSERT") db.execSQL("INSERT INTO categories VALUES('$other','published','$root')")
                    active.pushPending().getOrThrow()
                    db.execSQL("UPDATE categories SET name='frozen' WHERE id='$root'")
                    injectIntent = true
                    active.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(beforeCommit = {
                        if (scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state") == 2L) throw CancellationException("cancel pending acknowledgement before commit")
                    }))
                    try { assertFailsWith<CancellationException> { active.pushPending().getOrThrow() } }
                    finally { active.setSnapshotApplyFaultInjectorForTest(null) }
                    assertTrue(!injectIntent)
                    assertEquals(before, pendingState(db))
                    frozenHash = scalarText(db, "SELECT canonical_request_hash FROM _sync_outbox_bundle")
                    assertEquals(64, frozenHash.length)
                    assertEquals(1L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                    assertEquals(2L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
                    active.closeFromCoordinatedOwner(); db.close()
                    db = newFileBackedDb(path)
                    assertEquals(before, pendingState(db))
                    if (operation == "UPDATE") {
                        db.execSQL("CREATE TABLE apply_probe(value INTEGER NOT NULL)")
                        db.execSQL("INSERT INTO apply_probe VALUES(0)")
                        db.execSQL("CREATE TRIGGER pending_recovery AFTER UPDATE ON categories BEGIN UPDATE apply_probe SET value=value+1; END")
                    }
                    active = newRealServerClient(db, transport); client = active
                    active.open().getOrThrow(); active.attach(user).getOrThrow()
                    val observer = SettlementObservation.install(db)
                    try { active.pushPending().getOrThrow() } finally { observer.restore() }
                    if (operation == "UPDATE") assertTrue(observer.counts.edges > 0, "trigger added between writers forces full fallback")
                    else assertEquals(0L, observer.counts.edges)
                    frozenHash = null
                    assertEquals(before, pendingState(db))
                    assertEquals("frozen", scalarText(db, "SELECT name FROM categories WHERE id='$root'"))
                    assertEquals(2L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                    assertEquals(3L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
                    assertClean(db, 1)
                    active.pushPending().getOrThrow()
                    assertClean(db)
                } finally {
                    client?.closeFromCoordinatedOwner(); http?.close(); db.close()
                    deleteTempSqliteNowTestDbArtifacts(path)
                }
            }
            println("LIVE_PENDING_REOPEN_VERIFIED operations=INSERT,UPDATE,DELETE physicalRestart=true frozenReplay=true")
        }
    }

    private suspend fun pendingState(db: SafeSQLiteConnection): List<String> =
        listOf("_sync_dirty_rows" to "", "_sync_relationships" to " WHERE view='desired'").map { (table, filter) ->
            val columns = db.queryList("PRAGMA table_info($table)") { it.getText(1) }
            val projection = columns.joinToString(",") { "'$it',json_object('storage',typeof($it),'value',CAST($it AS TEXT))" }
            scalarText(db, "SELECT json_group_array(json_object($projection)) FROM (SELECT * FROM $table$filter ORDER BY ${columns.joinToString(",")})")
        }

    private suspend fun assertClean(db: SafeSQLiteConnection, pendingRows: Int = 0) {
        assertEquals(pendingRows.toLong(), scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
        for (table in listOf("_sync_outbox_rows", "_sync_apply_rows", "_sync_apply_edges", "_sync_apply_plan")) {
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM $table"), table)
        }
        assertEquals("none", scalarText(db, "SELECT state FROM _sync_outbox_bundle"))
        assertEquals(0L, scalarLong(db, "SELECT row_count+source_bundle_id FROM _sync_outbox_bundle"))
        assertEquals("", scalarText(db, "SELECT canonical_request_hash FROM _sync_outbox_bundle"))
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
        assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+apply_mode FROM _sync_operation_state CROSS JOIN _sync_apply_state"))
    }

    private data class Counts(var prepares: Long = 0, var steps: Long = 0, var edges: Long = 0, var scans: Long = 0)

    /** Count cached step calls too; instrumentation stays outside product code. */
    private class SettlementObservation(private val db: SafeSQLiteConnection, private val delegate: SQLiteConnection) : SQLiteConnection by delegate {
        val counts = Counts()
        override fun prepare(sql: String): SQLiteStatement {
            counts.prepares++
            val statement = delegate.prepare(sql)
            return object : SQLiteStatement by statement {
                override fun step(): Boolean {
                    counts.steps++
                    if (sql.startsWith("INSERT OR IGNORE INTO _sync_apply_edges")) counts.edges++
                    if (sql.contains("FROM \"categories\"") && (sql.startsWith("SELECT CAST(") || sql.startsWith("SELECT length(CAST(CAST("))) counts.scans++
                    return statement.step()
                }
            }
        }
        suspend fun restore() = db.withExclusiveAccess {
            val field = SafeSQLiteConnection::class.java.getDeclaredField("ref").also { it.isAccessible = true }
            assertSame(this@SettlementObservation, field.get(db))
            field.set(db, delegate)
        }
        companion object {
            suspend fun install(db: SafeSQLiteConnection): SettlementObservation = db.withExclusiveAccess {
                val field = SafeSQLiteConnection::class.java.getDeclaredField("ref").also { it.isAccessible = true }
                SettlementObservation(db, field.get(db) as SQLiteConnection).also { field.set(db, it) }
            }
        }
    }
}

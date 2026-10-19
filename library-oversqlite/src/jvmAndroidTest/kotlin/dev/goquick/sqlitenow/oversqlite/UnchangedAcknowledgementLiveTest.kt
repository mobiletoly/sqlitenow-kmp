package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import io.ktor.client.HttpClient
import io.ktor.client.statement.HttpReceivePipeline
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/** Real transport and durable SQLite complement the mock settlement benchmark. */
internal class UnchangedAcknowledgementLiveTest : RealServerSupport() {
    @Test
    fun oneRowPushAvoidsRelationshipIndexAcrossDatabaseSizes() = runOwnedWebDatabaseTest(timeout = 240.seconds) {
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
                writer.pushPending().getOrThrow()
                var population = 0
                var expectedShape: Pair<Long, Long>? = null
                for (size in listOf(100, 1000, 10000)) {
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
                    assertEquals(size + 2L, scalarLong(db, "SELECT count(*) FROM _sync_row_state"))
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
                            assertEquals(0L, counts.edges, "unchanged acknowledgement must skip the survivor edge index")
                            assertEquals(0L, counts.scans, "no explicit managed-row/FK enumeration")
                            assertTrue(counts.steps < 1000, "one-row acknowledgement must remain bounded independently of population")
                            if (sample > 0) {
                                val shape = counts.prepares to counts.steps
                                expectedShape?.let { assertEquals(it, shape, "whole settlement SQL count must not grow with unrelated rows") }
                                expectedShape = shape
                                println("LIVE_ACK_SQL rows=$size sample=$sample prepares=${counts.prepares} steps=${counts.steps} edges=${counts.edges} scans=${counts.scans} settlementMs=$settlementMs callMs=$callMs")
                            }
                            assertEquals(sequence + 1, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
                            assertEquals(checkpoint + 1, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                            assertEquals(name, scalarText(db, "SELECT name FROM categories WHERE id='$water'"))
                            assertEquals(size.toLong(), scalarLong(db, "SELECT count(*) FROM categories WHERE parent_id='$parent'"))
                            assertClean(db)
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
                    assertEquals(10002L, scalarLong(followerDb, "SELECT count(*) FROM categories"))
                    assertEquals(10000L, scalarLong(followerDb, "SELECT count(*) FROM categories WHERE parent_id='$parent'"))
                    assertEquals("water-10000-3", scalarText(followerDb, "SELECT name FROM categories WHERE id='$water'"))
                    db.execSQL("UPDATE categories SET name='water-followed' WHERE id='$water'")
                    writer.pushPending().getOrThrow()
                    reader.pullToStable().getOrThrow()
                    assertEquals("water-followed", scalarText(followerDb, "SELECT name FROM categories WHERE id='$water'"))
                    assertEquals(scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"), scalarLong(followerDb, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                    assertClean(db)
                    assertClean(followerDb)
                    println("LIVE_ACK_VERIFIED rows=10000 durableWriter=true durableFollower=true snapshot=true incrementalPull=true")
                } finally {
                    follower?.closeFromCoordinatedOwner()
                    followerHttp?.close()
                    followerDb.close()
                    deleteTempSqliteNowTestDbArtifacts(followerPath)
                }
            } finally {
                observation?.restore()
                client?.closeFromCoordinatedOwner()
                http?.close()
                db.close()
                deleteTempSqliteNowTestDbArtifacts(path)
            }
        }
    }

    private suspend fun assertClean(db: SafeSQLiteConnection) {
        for (table in listOf("_sync_dirty_rows", "_sync_outbox_rows", "_sync_apply_rows", "_sync_apply_edges", "_sync_apply_plan")) {
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

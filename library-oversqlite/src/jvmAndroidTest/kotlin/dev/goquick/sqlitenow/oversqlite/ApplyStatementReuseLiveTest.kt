package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import io.ktor.client.HttpClient
import io.ktor.client.statement.HttpReceivePipeline
import java.io.File
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.*
import kotlin.test.*
import kotlin.time.Duration.Companion.seconds

/** Ordinary nethttp authority, genuine stale versions, and durable clients. Each population has its own budget. */
internal class ApplyStatementReuseLiveTest : ApplyStatementTestSupport() {
    @Test fun pullHundredRows() = live("pull", 100)
    @Test fun pullThousandRows() = live("pull", 1000)
    @Test fun pullTenThousandRows() = live("pull", 10000)
    @Test fun conflictHundredRows() = live("conflict", 100)
    @Test fun conflictThousandRows() = live("conflict", 1000)
    @Test fun conflictTenThousandRows() = live("conflict", 10000)

    private class Endpoint(val path: String, val db: SafeSQLiteConnection, val http: HttpClient,
        var client: DefaultOversqliteClient)
    private suspend fun endpoint(config: RealServerConfig, user: String, afterPull: suspend () -> Unit = {}): Endpoint {
        val path = createTempSqliteNowTestDbPath("apply-statements-live")
        val db = newFileBackedDb(path)
        var http: HttpClient? = null
        var client: DefaultOversqliteClient? = null
        try {
            createBusinessRichSchemaTables(db)
            val source = bootstrapManagedSourceId(db, config.baseUrl)
            val original = newRealServerHttpClient(config.baseUrl, issueDummySigninToken(config.baseUrl, user, source))
            http = original.config {
                install("ObserveApplyResponse") {
                    receivePipeline.intercept(HttpReceivePipeline.Before) { response ->
                        if (response.call.request.url.encodedPath == "/sync/pull" && response.status.value in 200..299) afterPull()
                    }
                }
            }
            original.close()
            client = newRealServerClient(db, http, maxPushRows = 200)
            client.open().getOrThrow(); client.attach(user).getOrThrow()
            return Endpoint(path, db, http, client)
        } catch (failure: Throwable) {
            client?.closeFromCoordinatedOwner(); http?.close(); db.close(); deleteTempSqliteNowTestDbArtifacts(path)
            throw failure
        }
    }
    private suspend fun close(endpoint: Endpoint, deleteFiles: Boolean = true) {
        try { endpoint.client.closeFromCoordinatedOwner() }
        finally {
            endpoint.http.close(); endpoint.db.close()
            if (deleteFiles) deleteTempSqliteNowTestDbArtifacts(endpoint.path)
        }
    }
    private suspend fun closeAll(endpoints: List<Endpoint>, deleteFiles: Boolean = true) {
        var failure: Throwable? = null
        for (endpoint in endpoints) try { close(endpoint, deleteFiles) }
        catch (error: Throwable) { if (failure == null) failure = error else attachSuppressedOnce(failure, error) }
        failure?.let { throw it }
    }
    private suspend fun reopen(config: RealServerConfig, user: String, path: String, afterPull: suspend () -> Unit = {}): Endpoint {
        val db = newFileBackedDb(path)
        var http: HttpClient? = null
        var client: DefaultOversqliteClient? = null
        try {
            val source = bootstrapManagedSourceId(db, config.baseUrl)
            val original = newRealServerHttpClient(config.baseUrl, issueDummySigninToken(config.baseUrl, user, source))
            http = original.config {
                install("ObserveReopenedApplyResponse") {
                    receivePipeline.intercept(HttpReceivePipeline.Before) { response ->
                        if (response.call.request.url.encodedPath == "/sync/pull" && response.status.value in 200..299) afterPull()
                    }
                }
            }
            original.close()
            client = newRealServerClient(db, http, maxPushRows = 200)
            client.open().getOrThrow(); client.attach(user).getOrThrow()
            return Endpoint(path, db, http, client)
        } catch (failure: Throwable) {
            client?.closeFromCoordinatedOwner(); http?.close(); db.close()
            throw failure
        }
    }
    private suspend fun config(): RealServerConfig? {
        if (realServerEnv("OVERSQLITE_REALSERVER_TESTS")?.lowercase() !in listOf("true", "1", "yes", "on")) return null
        return checkNotNull(requireRealServerConfig()) { "Explicitly enabled real-server authority is unavailable" }
    }
    private suspend fun publishRelatedRows(config: RealServerConfig, user: String, prefix: String, parent: String, rows: Int) {
        val source = randomRealServerUuid()
        val http = newRealServerHttpClient(config.baseUrl, issueDummySigninToken(config.baseUrl, user, source))
        try {
            var generation: SyncGeneration? = null
            val api = OversqliteRemoteApi(http, Json { ignoreUnknownKeys = true }, generation = { checkNotNull(generation) }, log = {})
            val admitted = api.fetchGeneration(source, 1); generation = admitted.generation
            assertEquals(8, admitted.generation.tables.size, "ordinary complete generation")
            assertEquals("remote_authoritative", api.connect(source, false).resolution)
            val schema = admitted.generation.tables.single { it.table == "categories" }.schema
            var published = 0
            var sequence = 1L
            while (published < rows) {
                val batch = minOf(200, admitted.limits.maxPushRows, rows - published)
                val request = List(batch) { offset ->
                    val id = prefix + (published + offset + 2L).toString().padStart(12, '0')
                    PushRequestRow(schema, "categories", mapOf("id" to id), "INSERT", 0,
                        buildJsonObject { put("id", id); put("name", "unrelated"); put("parent_id", parent) })
                }
                val committed = api.push(source, sequence++, computePushRequestHashForTest(request, admitted.generation), null,
                    request, admitted.limits.maxPushDecodedBytes, admitted.limits.maxPushDecodedBytes)
                assertEquals(canonicalCommittedBundleHash(committed.rows), committed.bundleHash)
                assertEquals(request.map { it.key }, committed.rows.map { it.key })
                // The ordinary authority assigns the committed bundle sequence to each effect.
                committed.rows.forEach { assertEquals(committed.bundleSeq, it.rowVersion); assertEquals("INSERT", it.op) }
                published += batch
            }
            println("APPLY_LIVE_SEED_VERIFIED rows=$published batchLimit=200 published=true")
        } finally { http.close() }
    }
    private data class Population(val config: RealServerConfig, val user: String, val prefix: String,
        val parent: String, val root: String, val writerPath: String, val readerPath: String)

    private fun live(operation: String, rows: Int) {
        var population: Population? = null
        val paths = mutableListOf<String>()
        try {
            // Setup and the measured operation have separate clocks. The focused command
            // still has its outer300s deadline; no connection or statement crosses phases.
            runOwnedWebDatabaseTest(timeout = 240.seconds) {
                withContext(Dispatchers.Default) {
                    val config = config() ?: return@withContext
                    val user = randomRealServerId("apply-statements")
                    val prefix = randomRealServerUuid().take(24)
                    val parent = prefix + "000000000000"; val root = prefix + "000000000001"
                    val writer = endpoint(config, user).also { paths += it.path }
                    var reader: Endpoint? = null
                    val setupStarted = System.nanoTime()
                    try {
                        writer.db.execSQL("INSERT INTO categories VALUES('$parent','unrelated parent',NULL),('$root','root',NULL)")
                        writer.client.pushPending().getOrThrow()
                        publishRelatedRows(config, user, prefix, parent, rows)
                        println("APPLY_LIVE_SETUP stage=published rows=$rows elapsedMs=${(System.nanoTime()-setupStarted)/1_000_000.0}")
                        writer.client.rebuild().getOrThrow()
                        println("APPLY_LIVE_SETUP stage=writerSnapshot rows=$rows elapsedMs=${(System.nanoTime()-setupStarted)/1_000_000.0}")
                        val follower = endpoint(config, user).also { paths += it.path; reader = it }
                        println("APPLY_LIVE_SETUP stage=followerSnapshot rows=$rows elapsedMs=${(System.nanoTime()-setupStarted)/1_000_000.0}")
                        assertConverged(writer, follower, rows, parent)
                        population = Population(config,user,prefix,parent,root,writer.path,follower.path)
                    } finally { closeAll(listOfNotNull(reader, writer), deleteFiles = false) }
                    println("APPLY_LIVE_PHASE operation=$operation rows=$rows prepared=true closed=true elapsedMs=${(System.nanoTime()-setupStarted)/1_000_000.0}")
                }
            }
            population?.let { prepared ->
                runOwnedWebDatabaseTest(timeout = 240.seconds) {
                    withContext(Dispatchers.Default) { measurePopulation(operation, rows, prepared) }
                }
            }
        } finally {
            // JVM/Android files remain available between the two bounded test bodies only.
            for (path in paths) for (suffix in listOf("", "-wal", "-shm", "-journal")) File(path + suffix).delete()
        }
    }
    private suspend fun measurePopulation(operation: String, rows: Int, population: Population) {
        withContext(Dispatchers.Default) {
            val phaseStarted = System.nanoTime()
            val (config, user, prefix, parent, root, writerPath, readerPath) = population
            val writer = reopen(config, user, writerPath)
            var reader: Endpoint? = null
            var observation: ApplyStatementObservation? = null
            var observe = false
            var localStarted = 0L
            try {
                val follower = reopen(config, user, readerPath) {
                    if (observe) {
                        observe = false
                        observation = ApplyStatementObservation.install(checkNotNull(reader).db, prefix)
                        localStarted = System.nanoTime()
                    }
                }
                reader = follower
                assertConverged(writer, follower, rows, parent)
                println("APPLY_LIVE_PHASE operation=$operation rows=$rows physicallyReopened=true")
                repeat(4) { sample ->
                    val remote = "remote-$sample"
                    if (operation == "conflict") follower.db.execSQL("UPDATE categories SET name='local rejected' WHERE id='$root'")
                    writer.db.execSQL("UPDATE categories SET name='$remote' WHERE id='$root'")
                    writer.client.pushPending().getOrThrow()
                    val detectionStarted = System.nanoTime()
                    val handle = if (operation == "conflict") {
                        assertIs<ConflictNeedsResolutionException>(follower.client.pushPending().exceptionOrNull())
                        checkNotNull(follower.client.pendingConflict().getOrThrow())
                    } else null
                    val detectionMs = (System.nanoTime() - detectionStarted) / 1_000_000.0
                    val frozen = if (handle != null) scalarText(follower.db, "SELECT canonical_request_hash FROM _sync_outbox_bundle") else null
                    val sourceSequence = scalarLong(follower.db, "SELECT next_source_bundle_id FROM _sync_source_state")
                    if (handle == null) observe = true
                    else { observation = ApplyStatementObservation.install(follower.db, prefix); localStarted = System.nanoTime() }
                    val started = System.nanoTime()
                    try {
                        if (handle == null) follower.client.pullToStable().getOrThrow()
                        else follower.client.resolveConflict(handle, MergeResult.AcceptServer).getOrThrow()
                        val ended = System.nanoTime()
                        val observed = checkNotNull(observation) { "response/apply execution must be observed" }
                        observed.assertClosed()
                        observed.publish("live-$operation", rows, sample, (ended-localStarted)/1_000_000.0,
                            (ended-started)/1_000_000.0, follower.client.workSliceDiagnostics(), follower.client.workspaceDiagnostics())
                        println("APPLY_LIVE_RESOLUTION rows=$rows sample=$sample detectionMs=$detectionMs frozenObserved=${frozen != null} sourceSequence=$sourceSequence")
                        assertEquals(rows.toLong(), observed.steps.filterKeys { it.startsWith("INSERT OR IGNORE INTO _sync_apply_edges") }.values.sum())
                    } finally { observe = false; observation?.restore(); observation = null }
                    assertEquals(remote, scalarText(follower.db, "SELECT name FROM categories WHERE id='$root' AND parent_id IS NULL"))
                    assertEquals(sourceSequence, scalarLong(follower.db, "SELECT next_source_bundle_id FROM _sync_source_state"), "local resolution/pull does not allocate another request")
                    val checkpointStarted = System.nanoTime()
                    follower.client.pullToStable().getOrThrow()
                    println("APPLY_LIVE_CHECKPOINT rows=$rows sample=$sample elapsedMs=${(System.nanoTime()-checkpointStarted)/1_000_000.0}")
                    val verifyStarted = System.nanoTime()
                    assertConverged(writer, follower, rows, parent)
                    println("APPLY_LIVE_VERIFY rows=$rows sample=$sample elapsedMs=${(System.nanoTime()-verifyStarted)/1_000_000.0}")
                }
                println("APPLY_LIVE_VERIFIED operation=$operation rows=$rows samples=3 warmups=1 durable=true staleConflict=${operation == "conflict"} fullPayloads=true checkpoints=true")
                println("APPLY_LIVE_PHASE operation=$operation rows=$rows measured=true elapsedMs=${(System.nanoTime()-phaseStarted)/1_000_000.0}")
            } finally { observation?.restore(); closeAll(listOfNotNull(reader, writer)) }
        }
    }
    private suspend fun assertConverged(writer: Endpoint, reader: Endpoint, rows: Int, parent: String) {
        val sql = "SELECT json_group_array(json_object('id',id,'name',name,'parent_id',parent_id)) FROM (SELECT id,name,parent_id FROM categories ORDER BY id)"
        assertEquals(scalarText(writer.db, sql), scalarText(reader.db, sql), "complete payloads and parent identities")
        for (endpoint in listOf(writer, reader)) {
            assertEquals(rows + 2L, scalarLong(endpoint.db, "SELECT count(*) FROM categories"))
            assertEquals(rows.toLong(), scalarLong(endpoint.db, "SELECT count(*) FROM categories WHERE parent_id='$parent' AND name='unrelated'"))
            assertEquals(rows + 2L, scalarLong(endpoint.db, "SELECT count(*) FROM _sync_row_state"))
            assertApplyClean(endpoint.db, "categories")
        }
        assertEquals(scalarLong(writer.db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"),
            scalarLong(reader.db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
    }

    @Test fun snapshotCleanupCancellationReopens() = runOwnedWebDatabaseTest(timeout = 240.seconds) {
        withContext(Dispatchers.Default) {
            val config = config() ?: return@withContext
            val user = randomRealServerId("apply-cleanup-reopen")
            val writer = endpoint(config, user)
            var reader: Endpoint? = null
            val root = randomRealServerUuid()
            try {
                writer.db.execSQL("INSERT INTO categories VALUES('$root','before',NULL)")
                writer.client.pushPending().getOrThrow()
                val follower = endpoint(config, user); reader = follower
                follower.client.rebuild().getOrThrow()
                val checkpoint = scalarLong(follower.db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state")
                val source = scalarText(follower.db, "SELECT current_source_id FROM _sync_attachment_state")
                val sequence = scalarLong(follower.db, "SELECT next_source_bundle_id FROM _sync_source_state")
                writer.db.execSQL("UPDATE categories SET name='after' WHERE id='$root'")
                writer.client.pushPending().getOrThrow()
                var reached = false
                follower.client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(statementCacheOperations = StatementCacheOperations(
                    prepare = { db, sql ->
                        val delegate = db.prepare(sql)
                        object : SQLiteStatement by delegate {
                            override fun step(): Boolean {
                                val result = delegate.step()
                                if (sql == applyCleanupSql[1]) { reached = true; throw CancellationException("cancel after snapshot cleanup DELETE") }
                                return result
                            }
                        }
                    },
                )))
                assertFailsWith<CancellationException> { follower.client.rebuild().getOrThrow() }
                assertTrue(reached)
                assertEquals(1L, scalarLong(follower.db, "SELECT rebuild_required FROM _sync_attachment_state"))
                assertEquals(1L, scalarLong(follower.db, "SELECT count(*) FROM _sync_snapshot_stage"))
                follower.client.setSnapshotApplyFaultInjectorForTest(null)
                follower.client.closeFromCoordinatedOwner(); follower.db.close()
                val reopened = newFileBackedDb(follower.path)
                try {
                    assertEquals("before", scalarText(reopened, "SELECT name FROM categories WHERE id='$root'"))
                    assertEquals(checkpoint, scalarLong(reopened, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                    assertEquals(source, scalarText(reopened, "SELECT current_source_id FROM _sync_attachment_state"))
                    assertEquals(sequence, scalarLong(reopened, "SELECT next_source_bundle_id FROM _sync_source_state"))
                    assertEquals(1L, scalarLong(reopened, "SELECT rebuild_required FROM _sync_attachment_state"))
                    assertEquals(1L, scalarLong(reopened, "SELECT count(*) FROM _sync_snapshot_stage"))
                    val retry = newRealServerClient(reopened, follower.http)
                    try {
                        retry.open().getOrThrow(); retry.attach(user).getOrThrow(); retry.rebuild().getOrThrow()
                        assertEquals("after", scalarText(reopened, "SELECT name FROM categories WHERE id='$root'"))
                        assertApplyClean(reopened, "categories")
                        assertEquals(source, scalarText(reopened, "SELECT current_source_id FROM _sync_attachment_state"))
                        assertEquals(sequence, scalarLong(reopened, "SELECT next_source_bundle_id FROM _sync_source_state"))
                        assertEquals(0L, scalarLong(reopened, "SELECT rebuild_required FROM _sync_attachment_state"))
                        assertEquals(0L, scalarLong(reopened, "SELECT count(*) FROM _sync_snapshot_stage"))
                        assertEquals(OversqliteOperationState(), OversqliteOperationStateStore(reopened).loadState())
                        assertEquals(scalarLong(writer.db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"), scalarLong(reopened, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                        println("APPLY_LIVE_SNAPSHOT_VERIFIED cleanupDelete=true cancelled=true physicalReopen=true retry=true checkpoint=true")
                    } finally { retry.closeFromCoordinatedOwner() }
                } finally { reopened.close() }
            } finally { reader?.let { close(it) }; close(writer) }
        }
    }
}

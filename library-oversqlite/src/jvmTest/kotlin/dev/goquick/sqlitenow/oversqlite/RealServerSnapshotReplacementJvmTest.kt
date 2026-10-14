package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.request.post
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.launch
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.job
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

/** File-backed replacement through the separate, real Go capacity authority. */
internal class RealServerSnapshotReplacementJvmTest : RealServerSupport() {
    @Test
    fun replacesConfiguredRowsAndRetainsPreparedIntent(): Unit = runBlocking {
        check(System.getenv("GITHUB_ACTIONS") != "true") { "capacity replacement is local-heavy" }
        require(System.getenv("OVERSQLITE_REALSERVER_HEAVY") == "true")
        val controlUrl = requireNotNull(System.getenv("OVERSQLITE_CAPACITY_CONTROL_URL"))
        val initialBundle = requireNotNull(System.getenv("OVERSQLITE_CAPACITY_INITIAL_BUNDLE")).toLong()
        val server = requireNotNull(requireRealServerConfig())
        val rowCount = System.getenv("OVERSQLITE_CAPACITY_ROWS")?.toInt() ?: SnapshotCapacityFixture.rows
        require(rowCount in 50..SnapshotCapacityFixture.rows)
        val memory = SnapshotProcessMemoryObservation.fromEnvironment()
        val phases = CapacityPhaseTimer()
        val user = "snapshot-capacity-user"
        val directory = Files.createTempDirectory("oversqlite-real-capacity-")
        val path = directory.resolve("capacity.sqlite")
        val sampler = JvmMemorySampler.start()
        try {
            memory?.capture("before_setup")
            withTimeout(660_000L) {
                var originalSource = ""
                var preparedHash = ""
                var preparedRows: List<List<Any?>> = emptyList()
                val initialDb = BundledSqliteConnectionProvider.openConnection(path.toString(), debug = false)
                var initialHttp: HttpClient? = null
                var initialClient: DefaultOversqliteClient? = null
                try {
                    SnapshotCapacityFixture.createTables(initialDb)
                    initialDb.execSQL("PRAGMA journal_mode = WAL")
                    originalSource = bootstrapManagedSourceId(initialDb, server.baseUrl,
                        SnapshotCapacityFixture.syncTables)
                    val token = issueDummySigninToken(server.baseUrl, user, originalSource)
                    var refusePush = false
                    initialHttp = newRealServerHttpClient(server.baseUrl, token,
                        beforeRequest = { path ->
                            if (refusePush && path == "/sync/push") {
                                throw CancellationException("retain prepared intent")
                            }
                        })
                    initialClient = capacityClient(initialDb, initialHttp)
                    val progress = launch(start = CoroutineStart.UNDISPATCHED) {
                        initialClient.progress.collect {
                            println("capacity_progress initial=$it epoch_ms=${System.currentTimeMillis()}")
                            if (it is OversqliteProgress.Active && it.phase == OversqlitePhase.APPLYING_REMOTE_STATE)
                                memory?.capture("initial_stage_complete")
                        }
                    }
                    initialClient.open().getOrThrow()
                    phases.next("initial_install")
                    memory?.capture("before_initial_sync")
                    val attachment = SnapshotStatementObservation.ifEnabled(initialDb, initialClient, "initial") {
                        initialClient.attach(user).getOrThrow()
                    }
                    assertIs<AttachResult.Connected>(attachment)
                    println("capacity_diagnostics initial=${initialClient.snapshotRestoreDiagnosticsForTest()}")
                    memory?.capture("after_initial_sync")
                    progress.cancel()
                    phases.next("initial_readback")
                    assertEquals(rowCount.toLong(),
                        SnapshotCapacityFixture.totalBusinessRows(initialDb))
                    assertEquals(rowCount.toLong(),
                        scalarLong(initialDb, "SELECT count(*) FROM _sync_row_state"))
                    assertEquals(initialBundle,
                        scalarLong(initialDb, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                    SnapshotCapacityFixture.assertCompleteRows(initialDb, firstBundle = 1,
                        replacement = false, localIntent = false, rowCount = rowCount)
                    assertEquals(0L, scalarLong(initialDb, "SELECT count(*) FROM pragma_foreign_key_check"))
                    assertEmptyWorkTables(initialDb)

                    phases.next("prepare_intent")
                    initialDb.execSQL("UPDATE records_13 SET name='local-intent' WHERE id='row-00049'")
                    refusePush = true
                    assertFailsWith<CancellationException> { initialClient.pushPending().getOrThrow() }
                    val rows = OversqliteOutboxStateStore(initialDb).loadRows()
                    assertEquals(1, rows.size)
                    preparedRows = rows.map { listOf(it.base, it.wirePayload, it.localPayload,
                        it.keyJson, it.wireKeyJson, it.op, it.baseRowVersion, it.rowOrdinal) }
                    preparedHash = scalarText(initialDb,
                        "SELECT canonical_request_hash FROM _sync_outbox_bundle")
                } finally {
                    phases.next("initial_close")
                    initialClient?.closeFromCoordinatedOwner()
                    initialHttp?.close()
                    initialDb.close()
                }
                memory?.capture("after_initial_close", initialHttp?.coroutineContext?.job?.isCompleted)

                phases.next("server_replace")
                val control = if (memory == null) newRealServerHttpClient(controlUrl) else HttpClient(CIO) {
                    // Diagnostic fixture mutation can outlast CIO's default15s; sync clients keep their defaults.
                    engine { requestTimeout = 60_000 }
                    defaultRequest { url(controlUrl) }
                }
                val finalBundle = try {
                    val response = control.post("/replace")
                    assertEquals(HttpStatusCode.OK, response.status, response.bodyAsText())
                    val result = Json.parseToJsonElement(response.bodyAsText()).jsonObject
                    assertEquals(rowCount,
                        result.getValue("rows").jsonPrimitive.content.toInt())
                    assertEquals(14, result.getValue("tables").jsonPrimitive.content.toInt())
                    result.getValue("bundle_seq").jsonPrimitive.content.toLong()
                } finally {
                    control.close()
                }

                phases.next("reopen")
                var db = BundledSqliteConnectionProvider.openConnection(path.toString(), debug = false)
                var http: HttpClient? = null
                var client: DefaultOversqliteClient? = null
                try {
                    val token = issueDummySigninToken(
                        System.getenv("OVERSQLITE_CAPACITY_TOKEN_URL") ?: server.baseUrl, user, originalSource)
                    http = newRealServerHttpClient(server.baseUrl, token)
                    client = capacityClient(db, http,
                        System.getenv("OVERSQLITE_CAPACITY_REBUILD_WORK")?.toLong() ?: 1_000_000)
                    val observedClient = client
                    var progress = launch(start = CoroutineStart.UNDISPATCHED) {
                        observedClient.progress.collect { println("capacity_progress replacement=$it epoch_ms=${System.currentTimeMillis()}") }
                    }
                    client.open().getOrThrow()
                    assertIs<AttachResult.Connected>(client.attach(user).getOrThrow())
                    assertEquals(rowCount.toLong(),
                        SnapshotCapacityFixture.totalBusinessRows(db))
                    assertEquals(rowCount.toLong(),
                        scalarLong(db, "SELECT count(*) FROM _sync_row_state"))
                    assertEquals(preparedHash,
                        scalarText(db, "SELECT canonical_request_hash FROM _sync_outbox_bundle"))
                    val replacementSource = randomTestSourceId()
                    markSourceRecoveryRequired(db, SourceRecoveryReason.HISTORY_PRUNED,
                        replacementSource)
                    phases.next("replacement_rebuild")
                    memory?.capture("before_rebuild")
                    val activeClient = checkNotNull(client)
                    SnapshotStatementObservation.ifEnabled(db, activeClient, "replacement") {
                        activeClient.rebuild().getOrThrow()
                    }
                    println("capacity_diagnostics replacement=${checkNotNull(client).snapshotRestoreDiagnosticsForTest()}")
                    memory?.capture("after_rebuild")
                    progress.cancel()
                    phases.next("replacement_readback")
                    if (rowCount == SnapshotCapacityFixture.rows) {
                        assertTrue(activeClient.workSliceDiagnostics().workSlices > 0, "large replacement must exercise internal durable slices")
                    }
                    assertEquals(rowCount.toLong(),
                        SnapshotCapacityFixture.totalBusinessRows(db))
                    assertEquals(rowCount.toLong(),
                        scalarLong(db, "SELECT count(*) FROM _sync_row_state"))
                    assertEquals(finalBundle,
                        scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                    assertTrue(originalSource != replacementSource)
                    assertEquals(replacementSource,
                        scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
                    assertEquals(0L, scalarLong(db,
                        "SELECT rebuild_required FROM _sync_attachment_state"))
                    assertEquals(preparedHash,
                        scalarText(db, "SELECT canonical_request_hash FROM _sync_outbox_bundle"))
                    val retainedRows = OversqliteOutboxStateStore(db).loadRows()
                    assertEquals(preparedRows, retainedRows.map { listOf(it.base, it.wirePayload,
                        it.localPayload, it.keyJson, it.wireKeyJson, it.op, it.baseRowVersion,
                        it.rowOrdinal) })
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
                    assertEmptyWorkTables(db)
                    SnapshotCapacityFixture.assertCompleteRows(db,
                        firstBundle = initialBundle + 1, replacement = true, localIntent = true, rowCount = rowCount)
                    println("real_snapshot_replacement rows=$rowCount tables=14 " +
                        "initial_bundle=$initialBundle final_bundle=$finalBundle " +
                        "prepared_outbox=${retainedRows.size} internal_slices=${activeClient.workSliceDiagnostics().workSlices}")
                } finally {
                    phases.next("final_close")
                    client?.closeFromCoordinatedOwner()
                    http?.close()
                    db.close()
                }
                memory?.capture("after_final_close", http?.coroutineContext?.job?.isCompleted)
                memory?.afterClosure()
            }
        } finally {
            phases.finish()
            val metrics = sampler.stop()
            println("real_snapshot_replacement_client_memory peak_heap_bytes=${metrics.peakHeapBytes} " +
                "peak_rss_bytes=${metrics.peakRssBytes} samples=${metrics.sampleCount}")
            directory.toFile().deleteRecursively()
        }
    }

    private fun capacityClient(db: SafeSQLiteConnection, http: HttpClient, work: Long = 1_000_000): DefaultOversqliteClient =
        DefaultOversqliteClient.forTesting(db = db, http = http,
            config = OversqliteConfig(generationId = 1, databaseId = "fixture",
                maxSyncWorkUnitsPerInvocation = work,
                serviceId = "business", syncTables = SnapshotCapacityFixture.syncTables,
                transferTargets = OversqliteTransferTargets(
                    maxSnapshotChunkRows = System.getenv("OVERSQLITE_CAPACITY_CHUNK_ROWS")?.toInt() ?: 1000)))

    private suspend fun assertEmptyWorkTables(db: SafeSQLiteConnection) {
        for (table in listOf("_sync_snapshot_stage", "_sync_apply_plan", "_sync_apply_rows",
            "_sync_apply_edges")) {
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM $table"), table)
        }
        assertEquals(0L,
            scalarLong(db, "SELECT reserved_entries+reserved_bytes+snapshot_stage_entries+snapshot_stage_bytes FROM _sync_operation_state"))
    }
}

/** Monotonic phase boundaries; records incomplete phases on failure too. */
private class CapacityPhaseTimer {
    private var phase = "setup"
    private var started = System.nanoTime()

    fun next(name: String) {
        finish()
        phase = name
        started = System.nanoTime()
    }

    fun finish() {
        println("capacity_phase name=$phase millis=${(System.nanoTime() - started) / 1_000_000} epoch_ms=${System.currentTimeMillis()}")
    }
}

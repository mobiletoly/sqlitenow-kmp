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
    fun replacesFiftyThousandRowsAndRetainsPreparedIntent(): Unit = runBlocking {
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
                    var initialYields = 0
                    while (true) {
                        val result = runCatching {
                            SnapshotStatementObservation.ifEnabled(initialDb, initialClient, "initial-$initialYields") {
                                initialClient.attach(user).getOrThrow()
                            }
                        }
                        val failure = result.exceptionOrNull()
                        if (failure == null) {
                            assertIs<AttachResult.Connected>(result.getOrThrow())
                            break
                        }
                        assertIs<SyncWorkLimitException>(failure, "unexpected initial install failure: $failure")
                        initialYields++
                        assertTrue(initialYields <= 64)
                        assertEquals(0L, SnapshotCapacityFixture.totalBusinessRows(initialDb))
                        assertEquals(0L, scalarLong(initialDb, "SELECT count(*) FROM _sync_row_state"))
                    }
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
                    var yields = 0
                    var retainedSnapshotId: String? = null
                    var previousRoot = -1L
                    var previousStagedRows = -1L
                    while (true) {
                        val activeClient = checkNotNull(client)
                        val failure = runCatching {
                            SnapshotStatementObservation.ifEnabled(db, activeClient, "replacement-$yields") {
                                activeClient.rebuild().getOrThrow()
                            }
                        }.exceptionOrNull() ?: break
                        if (failure !is SyncWorkLimitException) failure.printStackTrace()
                        assertIs<SyncWorkLimitException>(failure,
                            "unexpected replacement failure: $failure")
                        yields++
                        val retained = OversqliteOperationStateStore(db).loadState()
                        retainedSnapshotId?.let { assertEquals(it, retained.stagedSnapshotId) }
                        retainedSnapshotId = retained.stagedSnapshotId
                        val stagedRows = scalarLong(db, "SELECT count(*) FROM _sync_snapshot_stage")
                        assertEquals(stagedRows, retained.snapshotStagedRowCount)
                        val root = scalarLong(db, "SELECT COALESCE((SELECT root_cursor FROM _sync_apply_plan),-1)")
                        assertTrue(stagedRows > previousStagedRows || root > previousRoot,
                            "retained staging or preparation must advance")
                        if (retained.snapshotStageComplete) assertEquals(rowCount.toLong(), stagedRows)
                        else assertTrue(root == -1L && stagedRows in 1 until rowCount.toLong())
                        previousRoot = root
                        previousStagedRows = stagedRows
                        println("capacity_yield attempt=$yields diagnostics=${activeClient.snapshotRestoreDiagnosticsForTest()} " +
                            "plan_rows=${scalarLong(db, "SELECT count(*) FROM _sync_apply_rows")} " +
                            "stage_rows=${scalarLong(db, "SELECT count(*) FROM _sync_snapshot_stage")}")
                        val diagnosticLimit = System.getenv("OVERSQLITE_CAPACITY_MAX_YIELDS")?.toInt()
                        check(diagnosticLimit == null || yields < diagnosticLimit) { "diagnostic yield limit reached" }
                        assertTrue(yields <= 64, "replacement did not finish after 64 yields")
                        assertEquals(rowCount.toLong(),
                            SnapshotCapacityFixture.totalBusinessRows(db))
                        assertEquals(rowCount.toLong(),
                            scalarLong(db, "SELECT count(*) FROM _sync_row_state"))
                        assertEquals("local-intent", scalarText(db,
                            "SELECT name FROM records_13 WHERE id='row-00049'"))
                        assertEquals(initialBundle,
                            scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                        if (System.getenv("OVERSQLITE_CAPACITY_REOPEN_EACH_YIELD") == "true") {
                            progress.cancel()
                            activeClient.closeFromCoordinatedOwner()
                            db.close()
                            db = BundledSqliteConnectionProvider.openConnection(path.toString(), debug = false)
                            client = capacityClient(db, http,
                                System.getenv("OVERSQLITE_CAPACITY_REBUILD_WORK")?.toLong() ?: 1_000_000)
                            client.open().getOrThrow()
                            assertIs<AttachResult.Connected>(client.attach(user).getOrThrow())
                            val reopenedClient = client
                            progress = launch(start = CoroutineStart.UNDISPATCHED) {
                                reopenedClient.progress.collect { println("capacity_progress replacement=$it epoch_ms=${System.currentTimeMillis()}") }
                            }
                            println("capacity_reopened yield=$yields snapshot=${retained.stagedSnapshotId}")
                        }
                    }
                    println("capacity_diagnostics replacement=${checkNotNull(client).snapshotRestoreDiagnosticsForTest()}")
                    memory?.capture("after_rebuild")
                    progress.cancel()
                    phases.next("replacement_readback")
                    if (rowCount == SnapshotCapacityFixture.rows) {
                        assertTrue(yields > 0, "large replacement must exercise durable preparation")
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
                        "prepared_outbox=${retainedRows.size} yields=$yields")
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

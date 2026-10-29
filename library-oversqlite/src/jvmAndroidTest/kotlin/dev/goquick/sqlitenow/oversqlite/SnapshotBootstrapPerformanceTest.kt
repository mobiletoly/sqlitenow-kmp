@file:OptIn(io.ktor.utils.io.InternalAPI::class)

package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.*
import dev.goquick.sqlitenow.common.initializePlatformTestContext
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.*
import java.io.File
import dev.goquick.sqlitenow.common.sqliteNowLogger
import kotlin.test.*

/** Matched route measurements and the retained early direct-kernel comparison. */
internal class SnapshotBootstrapPerformanceTest : SnapshotBootstrapTestSupport() {
    @Test fun richSchemaGeneralVersusBootstrap() = runMatchedBootstrapSamples(0..7)
    @Test fun richSchemaFirstDiagnosticsOffPair() = runMatchedBootstrapSamples(0..1)
    @Test fun richSchemaSecondDiagnosticsOffPair() = runMatchedBootstrapSamples(2..3)
    @Test fun richSchemaThirdDiagnosticsOffPair() = runMatchedBootstrapSamples(4..5)
    @Test fun richSchemaDiagnosticsPair() = runMatchedBootstrapSamples(6..7)

    private fun runMatchedBootstrapSamples(indices: IntRange) = runBlocking {
        withTimeout(180_000) {
            require(indices in listOf(0..7, 0..1, 2..3, 4..5, 6..7))
            initializePlatformTestContext()
            val fixture = RichSnapshotRestoreFixture()
            assertEquals("0ca9b5691c0be6bb80b3358526e31eb3e997051ded0e2387f04c9ca03b69524f", fixture.schemaHash)
            assertEquals("c4cd188e2e68a14e21e23a147ee2b8ccacda63b34f426aa9eaea3a8dd51278a5", fixture.dataHash)
            assertEquals(2034, fixture.rows.size); assertEquals(1_723_170L, fixture.logicalBytes)
            assertEquals(14, fixture.names.size); assertEquals(15, fixture.generation.tables.sumOf { it.foreignKeys.size })
            assertEquals(44, fixture.generation.tables.maxOf { it.columns.size })
            val diagnostics = SqliteNowDiagnostics(false)
            val capture = BootstrapDiagnosticCapture(printRecords = true)
            val previous = sqliteNowLogger
            val samples = mutableListOf<JsonObject>()
            var expectedRequests: List<String>? = null
            sqliteNowLogger = capture
            try { for (index in indices) {
                val bulk = index % 2 == 1
                val enabled = index >= 6
                diagnostics.setEnabled(false)
                val path = createTempSqliteNowTestDbPath("bulk-matched")
                assertTrue(!File(path).exists() || File(path).length() == 0L)
                val prep = MockSyncServer().also { it.generationOverride = fixture.generation }
                val prepHttp = prep.newHttpClient(); val richHttp = fixture.client()
                var useRich = false; var engineNanos = 0L
                var localStart = 0L; var localEngineStart = 0L; var localEnd = 0L
                var cleanupEnd = 0L; var localEngineEnd = 0L
                val http = HttpClient(MockEngine { request ->
                    val start = System.nanoTime()
                    if (useRich && request.method.value == "DELETE" && "/snapshot-sessions/" in request.url.encodedPath) {
                        cleanupEnd = start; localEngineEnd = engineNanos
                    }
                    try { if (useRich) richHttp.engine.execute(request) else prepHttp.engine.execute(request) }
                    finally {
                        val ended = System.nanoTime()
                        if (useRich) {
                            engineNanos += ended - start
                            if (request.method.value == "POST" && request.url.encodedPath == "/sync/snapshot-sessions") {
                                localStart = ended; localEngineStart = engineNanos
                            }
                        }
                    }
                })
                val config = OversqliteConfig(1, fixture.names.map { SyncTable(it, "id") }, "fixture-service", "main",
                    diagnostics = diagnostics, maxSyncWorkUnitsPerInvocation = 1_000_000,
                    transferTargets = OversqliteTransferTargets(maxSnapshotChunkRows = 1000))
                fun database() = SqliteNowDatabase(path, object : DatabaseMigrations {
                    override val targetVersion = 1
                    override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int): Int {
                        if (currentVersion <= 0) for (ddl in fixture.ddl) conn.execSQL(ddl)
                        return 1
                    }
                }, debug = false, syncManaged = true).also { it.connectionConfig = SqliteConnectionConfig(diagnostics = diagnostics) }
                var opening: OversqliteOpening<SqliteNowDatabase>? = null
                var sample: JsonObject? = null
                var phaseResult: JsonObject? = null
                try {
                    val openedAt = System.nanoTime()
                    opening = database().openForSync(config, http, ServerWinsResolver)
                    assertEquals(OversqliteOpeningOutcome.READY, opening.advance().outcome)
                    val ready = assertNotNull(opening.readyOrNull())
                    ready.client.attach("fixture-user").getOrThrow(); ready.client.detach().getOrThrow()
                    val db = ready.database.connection()
                    db.execSQL("CREATE TABLE fixture_settings(id INTEGER PRIMARY KEY,value TEXT)")
                    db.execSQL("INSERT INTO fixture_settings VALUES(1,'preserved')")
                    assertEquals(0L, db.scalarLong("SELECT count(*) FROM _sync_row_state"))
                    val source = db.withPreparedStatement("SELECT current_source_id FROM _sync_attachment_state") { require(it.step()); it.getText(0) }
                    val openingNanos = System.nanoTime() - openedAt
                    val adapter = BootstrapFixture(db, prep, http, ready.client as DefaultOversqliteClient, config, fixture.generation)
                    val descriptorAt = System.nanoTime()
                    bindFixtureDescriptor(adapter).let { Unit }
                    val descriptorNanos = System.nanoTime() - descriptorAt
                    fixture.bindSource(source); fixture.requests.clear(); capture.clear(); useRich = true
                    diagnostics.setEnabled(enabled)
                    val started = System.nanoTime()
                    diagnostics.operation("sync.attach") {
                        withAutomaticBootstrapWorkflow(adapter, userId = "fixture-user", beforeTransfer = { remote, state, recorder ->
                            assertEquals("remote_authoritative", remote.connect(state.sourceId, false).resolution)
                            db.transaction(TransactionMode.IMMEDIATE) {
                                assertEquals(outboxStateNone, OversqliteOutboxStateStore(db).loadBundleState().state)
                                assertEquals(0L, OversqliteOutboxStateStore(db).countRows())
                                OversqliteAttachmentStateStore(db).persistAnonymousState(source)
                                OversqliteOperationStateStore(db).persistState(OversqliteOperationState(kind = operationKindRemoteReplace, targetUserId = "fixture-user"))
                                OversqliteSyncStateStore(db) { OversqliteLocalStore(db, Json) { state.validated } }.clearDirtyRows()
                            }
                            if (!bulk) recorder.markRestoreAttempt()
                        }) { workflow, state ->
                            val result = if (bulk) assertNotNull(workflow.tryBootstrapSnapshot(state, SnapshotRebuildOutboxMode.CLEAN,
                                remoteReplace = true, finalizer = { localEnd = System.nanoTime() }))
                            else {
                                val (session, guard) = workflow.ensureOperationSnapshotStaged(state, SnapshotRebuildOutboxMode.CLEAN, remoteReplace = true)
                                workflow.applyStagedSnapshot(state, session, rotatedSourceId = null, outboxMode = SnapshotRebuildOutboxMode.CLEAN,
                                    pinnedGuard = guard, finalizer = { localEnd = System.nanoTime() })
                            }
                            assertEquals(2034L, result.restore?.rowCount)
                        }
                    }
                    val elapsed = System.nanoTime() - started
                    val records = capture.records()
                    diagnostics.setEnabled(false)
                    assertTrue(localStart > 0 && localEnd > localStart && cleanupEnd >= localEnd)
                    if (expectedRequests == null) expectedRequests = fixture.requests.toList()
                    else assertEquals(expectedRequests, fixture.requests.toList(), "both routes and diagnostics use identical requests")
                    fixture.assertData(db)
                    assertEquals(0L, db.scalarLong("SELECT (SELECT count(*) FROM _sync_bootstrap_wire)+(SELECT count(*) FROM _sync_bootstrap_pages)+(SELECT count(*) FROM _sync_snapshot_stage)+(SELECT count(*) FROM _sync_apply_rows)+(SELECT count(*) FROM _sync_apply_edges)+(SELECT count(*) FROM _sync_apply_plan)"))
                    assertEquals(0L, db.scalarLong("SELECT snapshot_stage_entries+snapshot_stage_bytes+reserved_entries+reserved_bytes FROM _sync_operation_state"))
                    val counts = if (enabled) bootstrapDiagnosticCounts(records, "sync.attach") else emptyMap()
                    if (enabled) {
                        assertTrue(records.any { it["name"]?.jsonPrimitive?.content == "snapshot.committed" && it["outcome"]?.jsonPrimitive?.content == "confirmed" })
                        for (name in listOf("verification_last_chunk", "verification_seal", "verification_final_apply", "final_validation"))
                            assertEquals(1, records.count { it["kind"]?.jsonPrimitive?.content == "span_end" && it["name"]?.jsonPrimitive?.content == name && it["outcome"]?.jsonPrimitive?.content == "success" })
                        assertTrue(records.all { it.getValue("lost_spans").jsonPrimitive.long == 0L && it.getValue("family_overflow_calls").jsonPrimitive.long == 0L })
                        if (bulk) {
                            val pages = counts.getValue("stage_pages") + counts.getValue("install_pages")
                            assertEquals(2034L, counts.getValue("received_rows")); assertEquals(2034L, counts.getValue("canonicalizations"))
                            assertEquals(fixture.rows.sumOf { fixture.generation.tables[it.table].columns.size.toLong() }, counts.getValue("scalar_decodes"))
                            assertEquals(6102L, counts.getValue("wire_rows")); assertEquals(4068L, counts.getValue("projection_rows"))
                            assertTrue(counts.getValue("peak_workspace_entries") <= config.maxSyncWorkspaceEntries)
                            assertTrue(counts.getValue("peak_workspace_bytes") <= config.maxSyncWorkspaceBytes)
                            assertTrue(counts.getValue("driver_steps") <= 8 * 2034 + 20 * 14 + 8 * 15 + 20 * pages + 128, counts.toString())
                            assertTrue(counts.getValue("quota_reads") <= 4 * pages + 12 * 14 + 4 * 15 + 32, counts.toString())
                        }
                        val names = setOf("row_installation", "final_validation", "verification_last_chunk", "verification_seal", "verification_final_apply",
                            "snapshot.preservation", "snapshot.unique_state_installation", "snapshot.publication", "snapshot.cleanup",
                            "snapshot.bootstrap.admission", "snapshot.bootstrap.staging", "snapshot.bootstrap.projection_verification", "snapshot.bootstrap.foreign_keys",
                            "snapshot.bootstrap.business_installation", "snapshot.bootstrap.baseline_installation", "snapshot.bootstrap.relationship_installation",
                            "snapshot.bootstrap.final_verification", "snapshot.bootstrap.publication", "snapshot.bootstrap.cleanup")
                        val phases = records.filter { it["kind"]?.jsonPrimitive?.content == "span_end" && it["name"]?.jsonPrimitive?.content in names }
                            .groupBy { it.getValue("name").jsonPrimitive.content }.mapValues { (_, values) -> values.sumOf { it.getValue("elapsed_ns").jsonPrimitive.long } }
                        phaseResult = buildJsonObject { put("route", if (bulk) typedSnapshotEngine else generalSnapshotEngine); put("inclusive", true)
                            put("phases_ns", JsonObject(phases.mapValues { JsonPrimitive(it.value) })) }
                    } else assertTrue(records.isEmpty())
                    opening.close(); opening = null
                    val reopened = database().openForSync(config, http, ServerWinsResolver)
                    try { assertEquals(OversqliteOpeningOutcome.READY, reopened.advance().outcome)
                        fixture.assertData(assertNotNull(reopened.readyOrNull()).database.connection())
                    } finally { reopened.close() }
                    sample = buildJsonObject {
                        put("route", if (bulk) typedSnapshotEngine else generalSnapshotEngine); put("enabled", enabled); put("sample", index / 2 + 1)
                        put("opening_preparation_ns", openingNanos); put("cold_descriptor_ns", descriptorNanos)
                        put("attach_ns", elapsed); put("total_including_opening_ns", openingNanos + descriptorNanos + elapsed)
                        put("mock_engine_ns", engineNanos)
                        put("local_stage_install_verify_ns", localEnd - localStart - (localEngineEnd - localEngineStart))
                        put("cleanup_and_commit_ns", cleanupEnd - localEnd); put("records", records.size)
                        put("structural_counts", JsonObject(counts.mapValues { JsonPrimitive(it.value) })); put("reopen_and_cleanup", true)
                    }
                } finally {
                    diagnostics.setEnabled(false); opening?.close(); http.close(); prepHttp.close(); richHttp.close()
                    deleteTempSqliteNowTestDbArtifacts(path); assertFalse(File(path).exists())
                }
                phaseResult?.let { println("BOOTSTRAP_MATCHED_PHASES $it") }
                samples += assertNotNull(sample); println("BOOTSTRAP_MATCHED_SAMPLE $sample")
            }
                if (samples.size == 8) {
                fun median(route: String, field: String): Long = samples.filter { it["route"]?.jsonPrimitive?.content == route && !it.getValue("enabled").jsonPrimitive.boolean }
                    .map { it.getValue(field).jsonPrimitive.long }.sorted().let { assertEquals(3, it.size); it[1] }
                val general = median(generalSnapshotEngine, "local_stage_install_verify_ns")
                val bootstrap = median(typedSnapshotEngine, "local_stage_install_verify_ns")
                println("BOOTSTRAP_MATCHED_RESULT " + buildJsonObject {
                    put("schema_sha256", fixture.schemaHash); put("data_sha256", fixture.dataHash); put("off_samples_per_route", 3)
                    put("general_local_median_ns", general); put("bootstrap_local_median_ns", bootstrap)
                    put("general_attach_median_ns", median(generalSnapshotEngine, "attach_ns")); put("bootstrap_attach_median_ns", median(typedSnapshotEngine, "attach_ns"))
                    put("general_total_median_ns", median(generalSnapshotEngine, "total_including_opening_ns")); put("bootstrap_total_median_ns", median(typedSnapshotEngine, "total_including_opening_ns"))
                    put("numerical_criterion_met", bootstrap <= general / 2 && median(typedSnapshotEngine, "attach_ns") <= median(generalSnapshotEngine, "attach_ns"))
                    put("timing_basis", "matched attach adapter; local includes publication; cleanup and commit separate; mock engine subtracted")
                    put("java_runtime", System.getProperty("java.runtime.version")); put("architecture", System.getProperty("os.arch")); put("activation_accepted", false)
                })
                } else {
                    assertEquals(2, samples.size)
                    assertEquals(listOf(generalSnapshotEngine, typedSnapshotEngine), samples.map { it.getValue("route").jsonPrimitive.content })
                    println("BOOTSTRAP_MATCHED_PAIR_RESULT " + buildJsonObject {
                        put("schema_sha256", fixture.schemaHash); put("data_sha256", fixture.dataHash)
                        put("pair", indices.first / 2 + 1); put("samples", samples.size)
                        put("java_runtime", System.getProperty("java.runtime.version")); put("architecture", System.getProperty("os.arch"))
                        put("activation_accepted", false)
                    })
                }
            } finally { diagnostics.setEnabled(false); sqliteNowLogger = previous }
        }
    }

    @Test fun richSchemaGeneralAndBulkExploratoryTiming() = runBlocking {
        withTimeout(180_000) {
            initializePlatformTestContext()
            println("BULK_RUNTIME " + buildJsonObject { put("java_version",System.getProperty("java.runtime.version")); put("java_home",System.getProperty("java.home")); put("os_arch",System.getProperty("os.arch")) })
            val fixture = RichSnapshotRestoreFixture()
            assertEquals("0ca9b5691c0be6bb80b3358526e31eb3e997051ded0e2387f04c9ca03b69524f", fixture.schemaHash)
            assertEquals("c4cd188e2e68a14e21e23a147ee2b8ccacda63b34f426aa9eaea3a8dd51278a5", fixture.dataHash)
            assertEquals(2034, fixture.rows.size); assertEquals(1_723_170L, fixture.logicalBytes)
            val results = mutableListOf<JsonObject>()
            for (bulk in listOf(false, true, false, true, false, true)) {
                val path = createTempSqliteNowTestDbPath("bulk-comparison")
                assertTrue(!File(path).exists() || File(path).length() == 0L)
                val prep = MockSyncServer().also { it.generationOverride = fixture.generation }
                val prepHttp = prep.newHttpClient()
                val richHttp = fixture.client()
                var useRich = false
                var engineNanos = 0L
                val http = HttpClient(MockEngine { request ->
                    val start = System.nanoTime()
                    try { if (useRich) richHttp.engine.execute(request) else prepHttp.engine.execute(request) }
                    finally { if (useRich) engineNanos += System.nanoTime() - start }
                })
                val config = OversqliteConfig(1, fixture.names.map { SyncTable(it, "id") }, "fixture-service", "main",
                    maxSyncWorkUnitsPerInvocation = 1_000_000)
                fun database() = SqliteNowDatabase(path, object : DatabaseMigrations {
                    override val targetVersion = 1
                    override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int): Int {
                        if (currentVersion <= 0) for (ddl in fixture.ddl) conn.execSQL(ddl)
                        return 1
                    }
                }, debug = false, syncManaged = true)
                var opening: OversqliteOpening<SqliteNowDatabase>? = null
                try {
                    val openingStart = System.nanoTime()
                    opening = database().openForSync(config, http, ServerWinsResolver)
                    assertEquals(OversqliteOpeningOutcome.READY, opening.advance().outcome)
                    val ready = checkNotNull(opening.readyOrNull())
                    ready.client.attach("fixture-user").getOrThrow()
                    ready.client.detach().getOrThrow()
                    val db = ready.database.connection()
                    assertEquals(0L, db.scalarLong("SELECT count(*) FROM _sync_row_state"))
                    db.execSQL("CREATE TABLE fixture_settings(id INTEGER PRIMARY KEY,value TEXT)")
                    db.execSQL("INSERT INTO fixture_settings VALUES(1,'preserved')")
                    val source = db.withPreparedStatement("SELECT current_source_id FROM _sync_attachment_state") { require(it.step()); it.getText(0) }
                    val openingNanos = System.nanoTime() - openingStart
                    fixture.bindSource(source); fixture.requests.clear(); useRich = true
                    val start = System.nanoTime()
                    val parts = if (bulk) runBulk(db, http, config, fixture, source) else {
                        ready.client.attach("fixture-user").getOrThrow()
                        emptyMap()
                    }
                    val elapsed = System.nanoTime() - start
                    fixture.assertData(db)
                    assertEquals(0L, db.scalarLong("SELECT count(*) FROM _sync_bootstrap_wire"))
                    assertEquals(0L, db.scalarLong("SELECT count(*) FROM _sync_bootstrap_pages"))
                    opening.close(); opening = null
                    val reopened = database().openForSync(config, http, ServerWinsResolver)
                    try {
                        assertEquals(OversqliteOpeningOutcome.READY, reopened.advance().outcome)
                        fixture.assertData(checkNotNull(reopened.readyOrNull()).database.connection())
                    } finally { reopened.close() }
                    val result = buildJsonObject {
                        put("route", if (bulk) typedSnapshotEngine else generalSnapshotEngine)
                        put("sample", results.count { it["route"]?.jsonPrimitive?.content == if (bulk) typedSnapshotEngine else generalSnapshotEngine } + 1)
                        put("opening_preparation_ns", openingNanos); put("total_ns", elapsed)
                        put("mock_engine_ns", engineNanos); put("local_including_adapter_ns", elapsed - engineNanos)
                        for ((name, value) in parts) put(name, value)
                        put("reopen_and_cleanup", true)
                    }
                    results += result
                    println("BULK_EXPLORATORY_SAMPLE $result")
                } finally {
                    opening?.close(); http.close(); prepHttp.close(); richHttp.close()
                    deleteTempSqliteNowTestDbArtifacts(path); assertFalse(File(path).exists())
                }
            }
            println("BULK_EXPLORATORY_RESULT " + buildJsonObject {
                put("schema_sha256", fixture.schemaHash); put("data_sha256", fixture.dataHash)
                put("legacy_http_adapter_included", false); put("activation_evidence", false); put("runs", JsonArray(results))
            })
        }
    }

    private suspend fun runBulk(db: SafeSQLiteConnection, http: HttpClient, config: OversqliteConfig,
                                fixture: RichSnapshotRestoreFixture, source: String): Map<String, Long> {
        val operations = OversqliteClientOperationController(config, OversqliteClientResourceOwner(db, http), SnapshotDiagnosticsState()) {}
        val parts = linkedMapOf<String, Long>()
        operations.runExclusiveOperation {
            val coldStart = System.nanoTime()
            val (metadata, admission) = db.transaction(TransactionMode.IMMEDIATE) {
                db.withStatementCache(cleanupTimeoutMillis = 5_000) { cache ->
                    val initializer = SyncRuntimeInitializer(config, TableInfoCache())
                    val physical = initializer.validateConfig(db).withGeneration(fixture.generation)
                    val workspace = OversqliteWorkspace(db, config)
                    val initial = assertIs<BootstrapSchemaAdmission.Eligible>(admitBootstrapSchema(db, physical,
                        db.scalarLong("PRAGMA main.schema_version"), workspace, "bootstrap-cold", cache))
                    bindBootstrapDescriptor(db, initial, physical, workspace, cache)
                    // Owned DDL invalidates the earlier schema pin.
                    physical to assertIs<BootstrapSchemaAdmission.Eligible>(admitBootstrapSchema(db, physical,
                        db.scalarLong("PRAGMA main.schema_version"), workspace, "bootstrap-cold", cache))
                }
            }
            parts["cold_descriptor_ns"] = System.nanoTime() - coldStart
            val state = RuntimeState(metadata, checkNotNull(OversqliteContractStore(db).readAdmission(1)), "fixture-user", source)
            val remote = OversqliteRemoteApi(http, Json, generation = { fixture.generation }, log = {}, beforeRecoveryExchange = operations::chargeRecoveryExchange)
            assertEquals("remote_authoritative", remote.connect(source, false).resolution)
            val session = remote.createSnapshotSession(source, null)
            val transfer = Json.encodeToString(RetainedSnapshotSeal.serializer(), RetainedSnapshotSeal(session, source, config.serviceId))
            OversqliteOperationStateStore(db).persistState(OversqliteOperationState(kind = operationKindRemoteReplace, targetUserId = "fixture-user"))
            val receiveStart = System.nanoTime()
            TypedSnapshotStage.begin(db, config, admission, metadata, session, fixture.generation, source,
                "a".repeat(64), transfer, operations) { stage ->
                var after = 0L
                while (true) {
                    val fetchStart = System.nanoTime()
                    val progress = BootstrapTransportScope.use(OversqliteWorkspace(db, config)) { transport ->
                        remote.fetchAndStageBootstrapChunk(stage, after, 1000, config.transferTargets.maxSnapshotChunkDecodedBytes,
                            state.admittedContract.limits.maxSnapshotDecodedBytesPerChunk, state.admittedContract.limits.maxRowDecodedBytes, transport)
                    }
                    parts["http_typed_receipt_ns"] = (parts["http_typed_receipt_ns"] ?: 0) + System.nanoTime() - fetchStart
                    after = progress.nextOrdinal
                    if (!progress.hasMore) break
                }
                parts["receipt_including_typed_http_ns"] = System.nanoTime() - receiveStart
                val sealStart = System.nanoTime(); stage.seal(); parts["seal_ns"] = System.nanoTime() - sealStart
                val installStart = System.nanoTime()
                OversqliteApplyExecutor(db, OversqliteApplyStateStore(db), operations).inApplyModeTransaction(state) { cache ->
                    val target = OversqliteProvenanceContextStore(db).target(state, source)
                    SnapshotBootstrapInstaller(db, config, operations).install(stage, admission, metadata, target, cache)
                    OversqliteAttachmentStateStore(db).persistAttachedState(source, "fixture-user", unconfirmedLocalSchema,
                        session.snapshotBundleSeq, false, "", cache)
                    stage.clearAfterInstallation(cache)
                }
                parts["install_final_verification_publication_cleanup_ns"] = System.nanoTime() - installStart
            }
            remote.deleteSnapshotSessionBestEffort(session.snapshotId, source)
        }.getOrThrow()
        return parts
    }
}

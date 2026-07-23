package dev.goquick.sqlitenow.oversqlite.platform

import androidx.sqlite.SQLiteStatement
import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.DatabaseMigrations
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import dev.goquick.sqlitenow.core.SqliteNowDatabase
import dev.goquick.sqlitenow.core.worker.SqliteWorkerConnectionProvider
import dev.goquick.sqlitenow.core.worker.SqliteWorkerSQLiteConnection
import dev.goquick.sqlitenow.oversqlite.DefaultOversqliteClient
import dev.goquick.sqlitenow.oversqlite.BundleCapabilitiesLimits
import dev.goquick.sqlitenow.oversqlite.DetachOutcome
import dev.goquick.sqlitenow.oversqlite.Phase6OwnedStorage
import dev.goquick.sqlitenow.oversqlite.ServerWinsResolver
import dev.goquick.sqlitenow.oversqlite.SnapshotApplyFaultInjector
import dev.goquick.sqlitenow.oversqlite.StatementCacheOperations
import dev.goquick.sqlitenow.oversqlite.SyncTable
import dev.goquick.sqlitenow.oversqlite.oversqliteTestConnectionProvider
import dev.goquick.sqlitenow.oversqlite.phase6WorkerStorageEvidence
import dev.goquick.sqlitenow.oversqlite.platformsupport.sha256Hex
import dev.goquick.sqlitenow.oversqlite.webRuntimeKind
import dev.goquick.sqlitenow.core.sqlite.use
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

internal class Phase6WorkerOversqliteEvidenceTest : PlatformCrossTargetTestSupport() {
    @Test
    fun providerInitializationLifecycleRecoveryAndReopenAreObservedDirectly() =
        runTest(timeout = 180.seconds) {
            val ownedStorage = Phase6OwnedStorage()
            val dbName = ownedStorage.newDatabaseName()
            val provider = oversqliteTestConnectionProvider()
            var db: SafeSQLiteConnection? = null
            try {
                db = provider.openConnection(dbName, debug = true)
                val worker = db.ref as? SqliteWorkerSQLiteConnection
                assertNotNull(worker, "The ordinary web default must construct the worker provider.")
                assertWorkerRuntime(worker)

                createUsersAndPostsTables(db)
                insertUser(db, "local-1", "Pre-existing Ada")
                val server = MockSyncServer()
                val http = server.newHttpClient()
                val client = newClient(db, http)
                try {
                    client.open().getOrThrow()
                    assertPhase6ControlTableSchemasAndIndexes(db)
                    val sourceId = assertInitialControlSingletons(db)
                    assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))

                    client.attach("phase6-user").getOrThrow()
                    client.pushPending().getOrThrow()
                    assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))

                    insertUser(db, "local-2", "Lifecycle Grace")
                    client.sync().getOrThrow()
                    assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM users"))

                    assertEquals(DetachOutcome.DETACHED, client.detach().getOrThrow())
                    val detachedSource =
                        scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
                    assertNotEquals(sourceId, detachedSource)

                    client.open().getOrThrow()
                    client.attach("phase6-user").getOrThrow()
                    client.rebuild().getOrThrow()
                    assertEquals(
                        detachedSource,
                        scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"),
                    )
                    assertEquals("Pre-existing Ada", scalarText(db, "SELECT name FROM users WHERE id = 'local-1'"))
                    assertEquals("Lifecycle Grace", scalarText(db, "SELECT name FROM users WHERE id = 'local-2'"))
                    assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
                    assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM pragma_foreign_key_check"))
                } finally {
                    client.close()
                    http.close()
                }

                worker?.metricsForTest()?.let { metrics ->
                    assertEquals(0, metrics.liveStatements)
                    assertEquals(0L, metrics.integerNumberViolations)
                    assertEquals(0L, metrics.snapshotExports)
                }
                db.close()
                db = null

                val reopened = provider.openConnection(dbName, debug = true)
                db = reopened
                val tableCount = scalarLong(
                    reopened,
                    "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'users'",
                )
                when (webRuntimeKind()) {
                    "js-node" -> assertEquals(0L, tableCount)
                    "js-browser", "wasm-browser" -> {
                        assertEquals(1L, tableCount)
                        assertEquals(2L, scalarLong(reopened, "SELECT COUNT(*) FROM users"))
                        assertEquals("ok", scalarText(reopened, "PRAGMA integrity_check"))
                        assertEquals(0L, scalarLong(reopened, "SELECT COUNT(*) FROM pragma_foreign_key_check"))
                    }
                    else -> error("Unexpected web runtime ${webRuntimeKind()}.")
                }

                val deterministicTarget =
                    "sqlitenow-worker-v1-${sha256Hex(dbName.encodeToByteArray())}.sqlite3"
                val storageEvidence = phase6WorkerStorageEvidence(dbName).split('\t', limit = 4)
                assertEquals(deterministicTarget, storageEvidence[0])
                if (webRuntimeKind() == "js-node") {
                    assertEquals("0", storageEvidence[1])
                } else {
                    assertEquals("1", storageEvidence[1])
                    assertEquals("0", storageEvidence[2])
                    assertEquals("", storageEvidence[3], "Direct-only targets remain markerless.")
                }
                println(
                    "phase7_default_provider provider=BundledSqliteConnectionProvider runtime=${webRuntimeKind()} " +
                        "target=$deterministicTarget reopenTables=$tableCount",
                )
            } finally {
                db?.close()
                ownedStorage.cleanup()
            }
        }

    @Test
    fun numericBooleanBoundsAndStatementLifecycleUseTheSelectedRuntime() =
        runTest(timeout = 180.seconds) {
            val ownedStorage = Phase6OwnedStorage()
            val leaderName = ownedStorage.newDatabaseName()
            val followerName = ownedStorage.newDatabaseName()
            val provider = oversqliteTestConnectionProvider()
            var leader: SafeSQLiteConnection? = null
            var follower: SafeSQLiteConnection? = null
            try {
                leader = provider.openConnection(leaderName, debug = true)
                follower = provider.openConnection(followerName, debug = true)
                createTypedRowsTable(leader)
                createTypedRowsTable(follower)

                val server = MockSyncServer(
                    registeredTableSpecs = testRegisteredTableSpecs("typed_rows"),
                    bundleLimits = smallBundleLimits(),
                )
                server.authoritativePayloadTransform = { row ->
                    row.payload?.jsonObject?.let { payload ->
                        JsonObject(
                            payload + (
                                "enabled_flag" to JsonPrimitive(
                                    payload.getValue("enabled_flag").jsonPrimitive.content == "1",
                                )
                            ),
                        )
                    }
                }
                val leaderHttp = server.newHttpClient()
                val followerHttp = server.newHttpClient()
                val syncTables = listOf(SyncTable("typed_rows", syncKeyColumnName = "id"))
                val leaderClient = newClient(
                    leader,
                    leaderHttp,
                    uploadLimit = 1,
                    downloadLimit = 1,
                    syncTables = syncTables,
                )
                val followerClient = newClient(
                    follower,
                    followerHttp,
                    uploadLimit = 1,
                    downloadLimit = 1,
                    snapshotChunkRows = 1,
                    snapshotChunkBytes = 4096,
                    snapshotApplyBatchRows = 1,
                    snapshotApplyBatchBytes = 4096,
                    syncTables = syncTables,
                )
                var applyPrepare = 0
                var applyReset = 0
                var applyClear = 0
                var applyClose = 0
                var stageReset = 0
                var stageClear = 0
                var stageClose = 0
                followerClient.setSnapshotApplyFaultInjectorForTest(
                    SnapshotApplyFaultInjector(
                        statementCacheOperations = StatementCacheOperations(
                            prepare = { db, sql ->
                                applyPrepare++
                                db.prepare(sql)
                            },
                            reset = {
                                applyReset++
                                it.reset()
                            },
                            clearBindings = {
                                applyClear++
                                it.clearBindings()
                            },
                            close = {
                                applyClose++
                                it.close()
                            },
                        ),
                    ),
                )
                followerClient.setSnapshotStageReusableStatementCleanupForTest {
                    stageReset++
                    it.reset()
                    stageClear++
                    it.clearBindings()
                }
                followerClient.setSnapshotStageStatementCloseForTest {
                    stageClose++
                    it.close()
                }
                try {
                    leaderClient.open().getOrThrow()
                    leaderClient.attach("phase6-user").getOrThrow()
                    followerClient.open().getOrThrow()
                    followerClient.attach("phase6-user").getOrThrow()

                    leader.execSQL(
                        """
                        INSERT INTO typed_rows(
                          id, signed_value, real_value, decimal_text, nullable_value, enabled_flag
                        ) VALUES(
                          'min', -9223372036854775808, -1.25, '1234567890.123456789', NULL, 0
                        )
                        """.trimIndent(),
                    )
                    leader.execSQL(
                        """
                        INSERT INTO typed_rows(
                          id, signed_value, real_value, decimal_text, nullable_value, enabled_flag
                        ) VALUES(
                          'max', 9223372036854775807, 1.25, '-0.000000000000000001', 7, 1
                        )
                        """.trimIndent(),
                    )
                    leaderClient.pushPending().getOrThrow()
                    assertEquals(2, server.uploadedChunkCount)

                    val chunkBodies = server.requestBodies
                        .filter { (path, _) -> path.endsWith("/chunks") }
                        .map { it.second }
                    assertEquals(2, chunkBodies.size)
                    val rows = chunkBodies.map { body ->
                        json.decodeFromString(
                            dev.goquick.sqlitenow.oversqlite.PushSessionChunkRequest.serializer(),
                            body,
                        ).rows.single()
                    }.associateBy { it.key.getValue("id") }
                    assertEquals("-9223372036854775808", rows.getValue("min").payload?.jsonObject?.get("signed_value")?.jsonPrimitive?.content)
                    assertEquals("9223372036854775807", rows.getValue("max").payload?.jsonObject?.get("signed_value")?.jsonPrimitive?.content)
                    assertEquals("-1.25", rows.getValue("min").payload?.jsonObject?.get("real_value")?.jsonPrimitive?.content)
                    assertEquals("1.25", rows.getValue("max").payload?.jsonObject?.get("real_value")?.jsonPrimitive?.content)
                    assertEquals("1234567890.123456789", rows.getValue("min").payload?.jsonObject?.get("decimal_text")?.jsonPrimitive?.content)
                    assertEquals("-0.000000000000000001", rows.getValue("max").payload?.jsonObject?.get("decimal_text")?.jsonPrimitive?.content)
                    assertEquals(JsonNull, rows.getValue("min").payload?.jsonObject?.get("nullable_value"))
                    assertEquals("0", rows.getValue("min").payload?.jsonObject?.get("enabled_flag")?.jsonPrimitive?.content)
                    assertEquals("1", rows.getValue("max").payload?.jsonObject?.get("enabled_flag")?.jsonPrimitive?.content)
                    assertEquals(
                        server.requestBodies.maxOf { (_, body) -> body.encodeToByteArray().size },
                        server.maxRequestBodyBytes,
                    )

                    followerClient.rebuild().getOrThrow()
                    assertEquals(Long.MIN_VALUE, scalarLong(follower, "SELECT signed_value FROM typed_rows WHERE id = 'min'"))
                    assertEquals(Long.MAX_VALUE, scalarLong(follower, "SELECT signed_value FROM typed_rows WHERE id = 'max'"))
                    assertEquals(-1.25, scalarDouble(follower, "SELECT real_value FROM typed_rows WHERE id = 'min'"))
                    assertEquals(1.25, scalarDouble(follower, "SELECT real_value FROM typed_rows WHERE id = 'max'"))
                    assertEquals("1234567890.123456789", scalarText(follower, "SELECT decimal_text FROM typed_rows WHERE id = 'min'"))
                    assertEquals(0L, scalarLong(follower, "SELECT enabled_flag FROM typed_rows WHERE id = 'min'"))
                    assertEquals(1L, scalarLong(follower, "SELECT enabled_flag FROM typed_rows WHERE id = 'max'"))

                    val diagnostics = followerClient.snapshotRestoreDiagnosticsForTest()
                    assertEquals(1L, diagnostics.sessionCount)
                    assertEquals(2L, diagnostics.fetchedChunks)
                    assertEquals(1, diagnostics.maxValidatedChunkRows)
                    assertEquals(1, diagnostics.maxLiveApplyPageRows)
                    assertEquals(2L, diagnostics.applyPages)
                    assertEquals(2L, diagnostics.finalStagedRows)
                    assertEquals(2L, diagnostics.finalAppliedRows)
                    assertEquals(
                        3,
                        server.snapshotChunkRequestCount,
                        "Two one-row chunks plus one exact terminal chunk request are required.",
                    )
                    assertTrue(diagnostics.maxDeclaredChunkBytes in 1..4096)
                    assertTrue(diagnostics.maxCompletelyDecodedChunkBodyBytes in 1..4096)
                    assertTrue(diagnostics.maxLiveApplyPageStagedTextBytes in 1..4096)

                    assertTrue(applyPrepare > 0)
                    assertEquals(applyReset, applyClear)
                    assertEquals(applyPrepare, applyClose)
                    assertEquals(0, stageReset)
                    assertEquals(stageReset, stageClear)
                    assertTrue(stageClose > 0)
                } finally {
                    leaderClient.close()
                    followerClient.close()
                    leaderHttp.close()
                    followerHttp.close()
                }

                listOfNotNull(
                    leader.ref as? SqliteWorkerSQLiteConnection,
                    follower.ref as? SqliteWorkerSQLiteConnection,
                ).forEach { worker ->
                    val metrics = worker.metricsForTest()
                    assertEquals(0L, metrics.integerNumberViolations)
                    assertEquals(0, metrics.liveStatements)
                    assertEquals(0L, metrics.snapshotExports)
                    assertTrue(metrics.pageRequests > 0)
                    assertTrue(metrics.maxPageRows <= 256)
                }
                println(
                    "phase7_default_worker_bounds uploadChunks=${server.uploadedChunkCount} " +
                        "snapshotChunks=${server.snapshotChunkRequestCount} maxRequestBytes=${server.maxRequestBodyBytes} " +
                        "applyPrepare=$applyPrepare applyReset=$applyReset applyClear=$applyClear " +
                        "applyClose=$applyClose stageReset=$stageReset stageClear=$stageClear stageClose=$stageClose",
                )
            } finally {
                leader?.close()
                follower?.close()
                ownedStorage.cleanup()
            }
        }

    @Test
    fun automaticAndExplicitInvalidationEmitExactFlowStates() =
        runTest(timeout = 180.seconds) {
            val ownedStorage = Phase6OwnedStorage()
            val leaderName = ownedStorage.newDatabaseName()
            val reactiveName = ownedStorage.newDatabaseName()
            val provider = oversqliteTestConnectionProvider()
            var leader: SafeSQLiteConnection? = null
            var reactiveDatabase: Phase6ReactiveDatabase? = null
            try {
                leader = provider.openConnection(leaderName, debug = true)
                createUsersAndPostsTables(leader)
                reactiveDatabase = Phase6ReactiveDatabase(reactiveName, provider)
                reactiveDatabase.open()

                val server = MockSyncServer()
                val leaderHttp = server.newHttpClient()
                val reactiveHttp = server.newHttpClient()
                val leaderClient = newClient(leader, leaderHttp)
                val reactiveClient = newClient(reactiveDatabase.connection(), reactiveHttp)
                try {
                    leaderClient.open().getOrThrow()
                    leaderClient.attach("phase6-user").getOrThrow()
                    reactiveClient.open().getOrThrow()
                    reactiveClient.attach("phase6-user").getOrThrow()
                    reactiveClient.rebuild().getOrThrow()

                    val initialObserved = CompletableDeferred<Unit>()
                    val emissions = mutableListOf<List<String>>()
                    val collecting = async(start = CoroutineStart.UNDISPATCHED) {
                        reactiveDatabase.userNames().take(3).toList(emissions)
                    }
                    while (emissions.isEmpty()) yield()
                    initialObserved.complete(Unit)
                    initialObserved.await()

                    insertUser(leader, "remote-1", "Automatic Ada")
                    leaderClient.pushPending().getOrThrow()
                    reactiveClient.pullToStable().getOrThrow()
                    while (emissions.size < 2) yield()

                    reactiveDatabase.connection().execSQL(
                        "INSERT INTO users(id, name) VALUES('external-1', 'Explicit Grace')",
                    )
                    reactiveDatabase.reportExternalTableChanges(setOf("users"))
                    collecting.await()

                    assertEquals(
                        listOf(
                            emptyList(),
                            listOf("Automatic Ada"),
                            listOf("Automatic Ada", "Explicit Grace"),
                        ),
                        emissions,
                    )
                } finally {
                    leaderClient.close()
                    reactiveClient.close()
                    leaderHttp.close()
                    reactiveHttp.close()
                }

                val metrics =
                    reactiveDatabase.connection().ref as? SqliteWorkerSQLiteConnection
                metrics?.metricsForTest()?.let {
                    assertEquals(0, it.liveStatements)
                    assertEquals(0L, it.snapshotExports)
                }
            } finally {
                leader?.close()
                reactiveDatabase?.close()
                ownedStorage.cleanup()
            }
        }

    @Test
    fun cancellationRetryTableRollsBackCleansStatementsAndRemainsReusable() =
        runTest(timeout = 180.seconds) {
            val ownedStorage = Phase6OwnedStorage()
            val scenarios = listOf(
                CancellationScenario("local-capture/open commit") {
                    verifyOpenCommitCancellation(ownedStorage)
                },
                CancellationScenario("outbox preparation") {
                    verifyOutboxPreparationCancellation(ownedStorage)
                },
                CancellationScenario("push request") {
                    verifyPushCancellation(ownedStorage)
                },
                CancellationScenario("pull request") {
                    verifyPullCancellation(ownedStorage)
                },
                CancellationScenario("staged apply") {
                    verifyStagedApplyCancellation(ownedStorage)
                },
                CancellationScenario("worker close") {
                    verifyWorkerCloseCancellation(ownedStorage)
                },
            )
            try {
                scenarios.forEach { scenario ->
                    scenario.verify()
                    println(
                        "phase7_default_worker_cancellation phase=${scenario.name} retry=passed",
                    )
                }
            } finally {
                ownedStorage.cleanup()
            }
        }

    private suspend fun verifyOpenCommitCancellation(
        ownedStorage: Phase6OwnedStorage,
    ) {
        val db = oversqliteTestConnectionProvider().openConnection(
            ownedStorage.newDatabaseName(),
            debug = true,
        )
        val server = MockSyncServer()
        val http = server.newHttpClient()
        val client = newClient(db, http)
        try {
            createUsersAndPostsTables(db)
            insertUser(db, "local-open", "Cancelled Open Ada")
            var armed = true
            db.beforeTransactionCommitForTest = {
                if (armed) {
                    armed = false
                    throw CancellationException("cancel local capture commit")
                }
            }
            assertFailsWith<CancellationException> { client.open() }
            db.beforeTransactionCommitForTest = null
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM users"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM sqlite_schema " +
                "WHERE type = 'table' AND name = '_sync_attachment_state'"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            client.open().getOrThrow()
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertReusableWorker(db)
        } finally {
            db.beforeTransactionCommitForTest = null
            client.close()
            http.close()
            db.close()
        }
    }

    private suspend fun verifyOutboxPreparationCancellation(
        ownedStorage: Phase6OwnedStorage,
    ) {
        val db = oversqliteTestConnectionProvider().openConnection(
            ownedStorage.newDatabaseName(),
            debug = true,
        )
        val server = MockSyncServer()
        val http = server.newHttpClient()
        val client = newClient(db, http)
        try {
            createUsersAndPostsTables(db)
            client.open().getOrThrow()
            client.attach("phase6-user").getOrThrow()
            insertUser(db, "local-outbox", "Cancelled Outbox Ada")
            var armed = true
            db.beforeTransactionCommitForTest = {
                if (armed) {
                    armed = false
                    throw CancellationException("cancel outbox preparation")
                }
            }
            assertFailsWith<CancellationException> { client.pushPending() }
            db.beforeTransactionCommitForTest = null
            assertEquals("none", scalarText(db, "SELECT state FROM _sync_outbox_bundle"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            client.pushPending().getOrThrow()
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertReusableWorker(db)
        } finally {
            db.beforeTransactionCommitForTest = null
            client.close()
            http.close()
            db.close()
        }
    }

    private suspend fun verifyPushCancellation(
        ownedStorage: Phase6OwnedStorage,
    ) {
        val db = oversqliteTestConnectionProvider().openConnection(
            ownedStorage.newDatabaseName(),
            debug = true,
        )
        val server = MockSyncServer()
        val http = server.newHttpClient()
        val client = newClient(db, http)
        try {
            createUsersAndPostsTables(db)
            client.open().getOrThrow()
            client.attach("phase6-user").getOrThrow()
            insertUser(db, "local-push", "Cancelled Push Ada")
            server.cancelNextRequestPathPrefix = "/sync/push-sessions"
            assertFailsWith<CancellationException> { client.pushPending() }
            assertEquals("prepared", scalarText(db, "SELECT state FROM _sync_outbox_bundle"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            client.pushPending().getOrThrow()
            assertEquals("none", scalarText(db, "SELECT state FROM _sync_outbox_bundle"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertReusableWorker(db)
        } finally {
            client.close()
            http.close()
            db.close()
        }
    }

    private suspend fun verifyPullCancellation(
        ownedStorage: Phase6OwnedStorage,
    ) {
        val provider = oversqliteTestConnectionProvider()
        val leader = provider.openConnection(ownedStorage.newDatabaseName(), debug = true)
        val follower = provider.openConnection(ownedStorage.newDatabaseName(), debug = true)
        val server = MockSyncServer()
        val leaderHttp = server.newHttpClient()
        val followerHttp = server.newHttpClient()
        val leaderClient = newClient(leader, leaderHttp)
        val followerClient = newClient(follower, followerHttp)
        try {
            createUsersAndPostsTables(leader)
            createUsersAndPostsTables(follower)
            leaderClient.open().getOrThrow()
            leaderClient.attach("phase6-user").getOrThrow()
            followerClient.open().getOrThrow()
            followerClient.attach("phase6-user").getOrThrow()
            insertUser(leader, "remote-pull", "Cancelled Pull Ada")
            leaderClient.pushPending().getOrThrow()
            server.cancelNextRequestPathPrefix = "/sync/pull"
            assertFailsWith<CancellationException> { followerClient.pullToStable() }
            assertEquals(0L, scalarLong(follower, "SELECT COUNT(*) FROM users"))
            followerClient.pullToStable().getOrThrow()
            assertEquals("Cancelled Pull Ada", scalarText(follower, "SELECT name FROM users"))
            assertReusableWorker(leader)
            assertReusableWorker(follower)
        } finally {
            leaderClient.close()
            followerClient.close()
            leaderHttp.close()
            followerHttp.close()
            leader.close()
            follower.close()
        }
    }

    private suspend fun verifyStagedApplyCancellation(
        ownedStorage: Phase6OwnedStorage,
    ) {
        val provider = oversqliteTestConnectionProvider()
        val leader = provider.openConnection(ownedStorage.newDatabaseName(), debug = true)
        val follower = provider.openConnection(ownedStorage.newDatabaseName(), debug = true)
        val server = MockSyncServer()
        val leaderHttp = server.newHttpClient()
        val followerHttp = server.newHttpClient()
        val leaderClient = newClient(leader, leaderHttp)
        val followerClient = newClient(follower, followerHttp)
        try {
            createUsersAndPostsTables(leader)
            createUsersAndPostsTables(follower)
            leaderClient.open().getOrThrow()
            leaderClient.attach("phase6-user").getOrThrow()
            followerClient.open().getOrThrow()
            followerClient.attach("phase6-user").getOrThrow()
            insertUser(leader, "remote-apply", "Cancelled Apply Ada")
            leaderClient.pushPending().getOrThrow()
            var armed = true
            followerClient.setSnapshotApplyFaultInjectorForTest(
                SnapshotApplyFaultInjector(
                    afterAppliedRow = {
                        if (armed) {
                            armed = false
                            throw CancellationException("cancel staged apply")
                        }
                    },
                ),
            )
            assertFailsWith<CancellationException> { followerClient.rebuild() }
            assertEquals(0L, scalarLong(follower, "SELECT COUNT(*) FROM users"))
            assertTrue(scalarLong(follower, "SELECT COUNT(*) FROM _sync_snapshot_stage") > 0L)
            assertEquals(0L, scalarLong(follower, "SELECT apply_mode FROM _sync_apply_state"))
            followerClient.setSnapshotApplyFaultInjectorForTest(null)
            followerClient.rebuild().getOrThrow()
            assertEquals("Cancelled Apply Ada", scalarText(follower, "SELECT name FROM users"))
            assertEquals(0L, scalarLong(follower, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
            assertReusableWorker(leader)
            assertReusableWorker(follower)
        } finally {
            followerClient.setSnapshotApplyFaultInjectorForTest(null)
            leaderClient.close()
            followerClient.close()
            leaderHttp.close()
            followerHttp.close()
            leader.close()
            follower.close()
        }
    }

    private suspend fun verifyWorkerCloseCancellation(
        ownedStorage: Phase6OwnedStorage,
    ) {
        val dbName = ownedStorage.newDatabaseName()
        val connection = SqliteWorkerConnectionProvider().openConnectionForTest(
            dbName = dbName,
            debug = true,
            config = SqliteConnectionConfig(),
            startupModeForTest = "normal",
            cleanupTimeoutMillis = 50,
        )
        val worker = connection.ref as SqliteWorkerSQLiteConnection
        connection.execSQL("CREATE TABLE close_probe(value INTEGER NOT NULL)")
        connection.execSQL("INSERT INTO close_probe VALUES(1)")
        worker.holdNextResponseForTest("shutdown")
        supervisorScope {
            val observedFailure = CompletableDeferred<Throwable>()
            val closing = launch(start = CoroutineStart.UNDISPATCHED) {
                observedFailure.complete(
                    runCatching { connection.close() }.exceptionOrNull()
                        ?: error("cancelled worker close unexpectedly succeeded"),
                )
            }
            repeat(1_000) {
                if (worker.diagnosticsForTest().contains("\"completedResponses\":1")) return@repeat
                yield()
            }
            closing.cancel(CancellationException("cancel worker close"))
            assertTrue(observedFailure.await() is CancellationException)
            closing.join()
        }

        val reopened = oversqliteTestConnectionProvider().openConnection(dbName, debug = true)
        try {
            val tableCount = scalarLong(
                reopened,
                "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'close_probe'",
            )
            if (webRuntimeKind() == "js-node") {
                assertEquals(0L, tableCount)
            } else {
                assertEquals(1L, tableCount)
                assertEquals(1L, scalarLong(reopened, "SELECT value FROM close_probe"))
            }
            assertReusableWorker(reopened)
        } finally {
            reopened.close()
        }
    }

    private suspend fun assertReusableWorker(db: SafeSQLiteConnection) {
        db.prepare("SELECT 1").use { statement ->
            assertTrue(statement.step())
            assertEquals(1L, statement.getLong(0))
        }
        (db.ref as? SqliteWorkerSQLiteConnection)?.metricsForTest()?.let { metrics ->
            assertEquals(0, metrics.liveStatements)
            assertEquals(0L, metrics.integerNumberViolations)
            assertEquals(0L, metrics.snapshotExports)
        }
    }

    private suspend fun assertWorkerRuntime(worker: SqliteWorkerSQLiteConnection) {
        val metrics = worker.metricsForTest()
        assertEquals(
            if (webRuntimeKind() == "js-node") "js-node-worker" else "browser-worker",
            metrics.runtimeKind,
        )
        assertEquals(
            if (metrics.runtimeKind == "js-node-worker") "memory" else "direct-opfs",
            metrics.storageMode,
        )
        assertEquals(0L, metrics.snapshotExports)
    }

    private fun smallBundleLimits(): BundleCapabilitiesLimits =
        testBundleCapabilitiesLimits().copy(
            defaultRowsPerSnapshotChunk = 1,
            maxRowsPerSnapshotChunk = 1,
            defaultBytesPerSnapshotChunk = 4096,
            maxBytesPerSnapshotChunk = 4096,
            maxBytesPerSnapshotRow = 4096,
            snapshotMaterializationBatchRows = 1,
            snapshotMaterializationBatchBytes = 4096,
        )

    private suspend fun assertInitialControlSingletons(db: SafeSQLiteConnection): String {
        val sourceId =
            scalarText(db, "SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1")
        assertTrue(sourceId.isNotBlank())
        assertEquals("1|0", scalarText(db, "SELECT singleton_key || '|' || apply_mode FROM _sync_apply_state"))
        assertEquals(
            "1|$sourceId|anonymous|||0|0|",
            scalarText(
                db,
                "SELECT singleton_key || '|' || current_source_id || '|' || binding_state || '|' || " +
                    "attached_user_id || '|' || schema_name || '|' || last_bundle_seq_seen || '|' || " +
                    "rebuild_required || '|' || pending_initialization_id FROM _sync_attachment_state",
            ),
        )
        assertEquals(
            "1|none|||0|0|0|0||",
            scalarText(
                db,
                "SELECT singleton_key || '|' || kind || '|' || target_user_id || '|' || " +
                    "staged_snapshot_id || '|' || snapshot_bundle_seq || '|' || snapshot_row_count || " +
                    "'|' || snapshot_byte_count || '|' || snapshot_stage_complete || '|' || reason || " +
                    "'|' || replacement_source_id FROM _sync_operation_state",
            ),
        )
        assertEquals(
            "1|jcs_uniform_numeric_strings_v1|none||0|||0||0",
            scalarText(
                db,
                "SELECT singleton_key || '|' || canonical_json_contract || '|' || state || '|' || " +
                    "source_id || '|' || source_bundle_id || '|' || initialization_id || '|' || " +
                    "canonical_request_hash || '|' || row_count || '|' || remote_bundle_hash || '|' || " +
                    "remote_bundle_seq FROM _sync_outbox_bundle",
            ),
        )
        assertEquals(
            "main|posts|main|users",
            scalarText(
                db,
                "SELECT group_concat(schema_name || '|' || table_name, '|') FROM (" +
                    "SELECT schema_name, table_name FROM _sync_managed_tables ORDER BY table_name)",
            ),
        )
        return sourceId
    }

    private suspend fun createTypedRowsTable(db: SafeSQLiteConnection) {
        db.execSQL(
            """
            CREATE TABLE typed_rows (
              id TEXT PRIMARY KEY NOT NULL,
              signed_value INTEGER NOT NULL,
              real_value REAL NOT NULL,
              decimal_text TEXT NOT NULL,
              nullable_value INTEGER,
              enabled_flag INTEGER NOT NULL
            )
            """.trimIndent(),
        )
    }
}

private class CancellationScenario(
    val name: String,
    val verify: suspend () -> Unit,
)

private class Phase6ReactiveDatabase(
    dbName: String,
    provider: dev.goquick.sqlitenow.core.SqliteConnectionProvider,
) : SqliteNowDatabase(
    dbName = dbName,
    migration = object : DatabaseMigrations {
        override suspend fun applyMigration(
            conn: SafeSQLiteConnection,
            currentVersion: Int,
        ): Int {
            conn.execSQL(
                "CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)",
            )
            conn.execSQL(
                """
                CREATE TABLE IF NOT EXISTS posts (
                  id TEXT PRIMARY KEY NOT NULL,
                  user_id TEXT NOT NULL REFERENCES users(id),
                  title TEXT NOT NULL
                )
                """.trimIndent(),
            )
            return 1
        }
    },
    debug = true,
    connectionProvider = provider,
) {
    fun userNames(): Flow<List<String>> =
        createReactiveQueryFlow(setOf("users")) {
            val names = mutableListOf<String>()
            connection().prepare("SELECT name FROM users ORDER BY name").use { statement ->
                while (statement.step()) names += statement.getText(0)
            }
            names
        }
}

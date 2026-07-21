/*
 * Copyright 2025 Toly Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.TransactionMode
import dev.goquick.sqlitenow.core.sqlite.SqliteStatement
import dev.goquick.sqlitenow.core.sqlite.use
import java.lang.management.ManagementFactory
import java.nio.file.Files
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue

class SnapshotStageStatementReuseTest : BundleClientContractTestSupport() {
    @Test
    fun canonicalSnapshotPayloadValidationReusesInputMap() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        try {
            createUsersTable(db)
            val tableInfo = TableInfoCache().get(db, "users")
            val validated = ValidatedConfig(
                schema = "main",
                tables = listOf(ValidatedSyncTable("users", "id")),
                pkByTable = mapOf("users" to "id"),
                keyByTable = mapOf("users" to listOf("id")),
                tableOrder = mapOf("users" to 0),
                tableInfoByName = mapOf("users" to tableInfo),
            )
            val state = RuntimeState(validated, userId = "user", sourceId = "source")
            val store = OversqliteLocalStore(db, Json) { validated }
            val key = mapOf("id" to "user-1")
            val payload = buildJsonObject {
                put("id", "user-1")
                put("name", "x".repeat(1_024))
            }

            repeat(1_000) {
                store.validateAndNormalizeSnapshotPayload(state, "users", key, payload)
            }
            val threadBean = ManagementFactory.getThreadMXBean() as com.sun.management.ThreadMXBean
            threadBean.isThreadAllocatedMemoryEnabled = true
            val threadId = Thread.currentThread().id
            val beforeBytes = threadBean.getThreadAllocatedBytes(threadId)
            var normalized = payload
            repeat(10_000) {
                normalized = store.validateAndNormalizeSnapshotPayload(state, "users", key, payload)
            }
            val allocatedBytes = threadBean.getThreadAllocatedBytes(threadId) - beforeBytes

            println(
                "phase6c_canonical_snapshot_validation " +
                    "allocated_bytes=$allocatedBytes bytes_per_row=${allocatedBytes / 10_000.0}",
            )
            assertTrue(
                allocatedBytes <= 2_560_000L,
                "canonical payload validation allocated ${allocatedBytes / 10_000.0} bytes per row",
            )
            assertSame(payload, normalized, "canonical payload validation must not copy the input map")

            val mixedCasePayload = buildJsonObject {
                put("ID", "user-1")
                put("NAME", "Ada")
            }
            val normalizedMixedCase = store.validateAndNormalizeSnapshotPayload(
                state,
                "users",
                key,
                mixedCasePayload,
            )
            assertEquals(setOf("id", "name"), normalizedMixedCase.keys)
            assertEquals("user-1", normalizedMixedCase.getValue("id").toString().trim('"'))
            assertTrue(normalizedMixedCase !== mixedCasePayload)
        } finally {
            db.close()
        }
    }

    @Test
    fun preparedOutboxReplayQueryIsPreparedOnceAndReboundAcrossRows() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        db.execSQL(
            """
            CREATE TABLE _sync_outbox_rows (
              source_bundle_id INTEGER NOT NULL,
              row_ordinal INTEGER NOT NULL,
              table_name TEXT NOT NULL,
              key_json TEXT NOT NULL,
              op TEXT NOT NULL,
              local_payload TEXT
            )
            """.trimIndent(),
        )
        db.execSQL(
            """
            INSERT INTO _sync_outbox_rows(
              source_bundle_id, row_ordinal, table_name, key_json, op, local_payload
            ) VALUES
              (7, 0, 'users', '{"id":"zero"}', 'INSERT', '{"id":"zero","name":"Zero"}'),
              (7, 1, 'users', '{"id":"one"}', 'UPDATE', '{"id":"one","name":"One"}'),
              (7, 2, 'users', '{"id":"two"}', 'DELETE', NULL)
            """.trimIndent(),
        )
        db.execSQL("CREATE TABLE replay_mutation_probe (ordinal INTEGER NOT NULL)")
        var prepareCalls = 0
        var resetCalls = 0
        val cache = StatementCache(
            db = db,
            operations = StatementCacheOperations(
                prepare = { connection, sql ->
                    prepareCalls++
                    connection.prepare(sql)
                },
                reset = { statement ->
                    resetCalls++
                    statement.reset()
                },
            ),
        )
        try {
            val store = OversqliteOutboxStateStore(db)
            val rows = buildList {
                var afterRowOrdinal = -1L
                while (true) {
                    val row = store.loadReplayRowAfter(7L, afterRowOrdinal, cache) ?: break
                    add(row)
                    assertEquals(size, resetCalls, "replay cursor must reset once before caller mutation")
                    db.execSQL("INSERT INTO replay_mutation_probe(ordinal) VALUES(${row.rowOrdinal})")
                    afterRowOrdinal = row.rowOrdinal
                }
            }

            assertEquals(listOf(0L, 1L, 2L), rows.map { it.rowOrdinal })
            assertEquals(listOf("INSERT", "UPDATE", "DELETE"), rows.map { it.op })
            assertEquals(1, prepareCalls)
            assertEquals(3L, scalarLong(db, "SELECT COUNT(*) FROM replay_mutation_probe"))
        } finally {
            cache.close()
            db.close()
        }
    }

    @Test
    fun snapshotStageCloseSeamFailureLeavesCoreOwnedConnectionReusable() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        var closeAttempts = 0
        withConnectedClient(
            db = db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            configureServer = {
                usersSnapshotRoutes(
                    snapshotId = "stage-close-failure",
                    snapshotBundleSeq = 7,
                    userId = "remote",
                    rowVersion = 7,
                    payloadJson = """{"id":"remote","name":"Remote"}""",
                )
            },
        ) { client ->
            client.setSnapshotStageStatementCloseForTest {
                closeAttempts++
                throw IllegalStateException("stage-statement-close-failure")
            }

            assertTrue(client.rebuild().isFailure)
            assertEquals(1, closeAttempts)
            assertEquals(1L, scalarLong(db, "SELECT 1"))
        }
    }

    @Test
    fun stageSnapshotChunkReusesBindingsAcrossRowsAndRollsBackInvalidChunk() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        var sessionAttempt = 0
        withConnectedClient(
            db = db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            configureServer = {
                createContext("/sync/snapshot-sessions") { exchange ->
                    sessionAttempt++
                    respondJson(
                        exchange,
                        200,
                        """
                        {
                          "snapshot_id": "statement-reuse-$sessionAttempt",
                          "snapshot_bundle_seq": 7,
                          "row_count": 2,
                          "byte_count": 64,
                          "expires_at": "2099-01-01T00:00:00Z"
                        }
                        """.trimIndent(),
                    )
                }
                createContext("/sync/snapshot-sessions/statement-reuse-1") { exchange ->
                    respondJson(
                        exchange,
                        200,
                        """
                        {
                          "snapshot_id": "statement-reuse-1",
                          "snapshot_bundle_seq": 7,
                          "byte_count": 64,
                          "next_row_ordinal": 2,
                          "has_more": false,
                          "rows": [
                            {
                              "schema": "main",
                              "table": "users",
                              "key": {"id":"user-1"},
                              "row_version": 11,
                              "payload": {"id":"user-1","name":"Ada"}
                            },
                            {
                              "schema": "main",
                              "table": "users",
                              "key": {"id":"user-2"},
                              "row_version": 12,
                              "payload": {"id":"user-2"}
                            }
                          ]
                        }
                        """.trimIndent(),
                    )
                }
                createContext("/sync/snapshot-sessions/statement-reuse-2") { exchange ->
                    respondJson(
                        exchange,
                        200,
                        """
                        {
                          "snapshot_id": "statement-reuse-2",
                          "snapshot_bundle_seq": 7,
                          "byte_count": 64,
                          "next_row_ordinal": 2,
                          "has_more": false,
                          "rows": [
                            {
                              "schema": "main",
                              "table": "users",
                              "key": {"id":"user-1"},
                              "row_version": 11,
                              "payload": {"id":"user-1","name":"Ada"}
                            },
                            {
                              "schema": "main",
                              "table": "not_configured",
                              "key": {"id":"user-2"},
                              "row_version": 12,
                              "payload": {"id":"user-2","name":"Grace"}
                            }
                          ]
                        }
                        """.trimIndent(),
                    )
                }
                createContext("/sync/snapshot-sessions/statement-reuse-3") { exchange ->
                    respondJson(
                        exchange,
                        200,
                        """
                        {
                          "snapshot_id": "statement-reuse-3",
                          "snapshot_bundle_seq": 7,
                          "byte_count": 64,
                          "next_row_ordinal": 2,
                          "has_more": false,
                          "rows": [
                            {
                              "schema": "main",
                              "table": "users",
                              "key": {"id":"user-1"},
                              "row_version": 11,
                              "payload": {"id":"user-1","name":"Ada"}
                            },
                            {
                              "schema": "main",
                              "table": "hostile\\nsecret",
                              "key": {"id":"user-2"},
                              "row_version": 12,
                              "payload": {"id":"user-2","name":"Grace"}
                            }
                          ]
                        }
                        """.trimIndent(),
                    )
                }
            },
        ) { client ->
            val payloadValidationError = assertIs<SnapshotSemanticException>(client.rebuild().exceptionOrNull())
            assertEquals(SnapshotSemanticFailure.INVALID_PAYLOAD_SHAPE, payloadValidationError.failure)
            assertEquals(emptyList(), stagedRows(db))

            val validationError = client.rebuild().exceptionOrNull()
            assertTrue(
                validationError?.message.orEmpty().contains(
                    "snapshot row main.not_configured is not configured for sync",
                ),
            )
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users"))

            val redactedError = client.rebuild().exceptionOrNull()
            assertTrue(redactedError?.message.orEmpty().contains("snapshot row main.<redacted>"))
            assertFalse(redactedError?.message.orEmpty().contains("hostile"))
            assertFalse(redactedError?.message.orEmpty().contains("secret"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
        }
    }

    @Test
    fun stageSnapshotChunkRejectsMalformedKeysAndPayloadColumnsBeforeStaging() = runBlocking<Unit> {
        val cases = listOf(
            Triple(
                "key-payload-mismatch",
                """{"id":"user-2","name":"Ada"}""",
                SnapshotSemanticFailure.KEY_PAYLOAD_MISMATCH,
            ),
            Triple(
                "duplicate-payload-column",
                """{"id":"user-1","ID":"user-1","name":"Ada"}""",
                SnapshotSemanticFailure.INVALID_PAYLOAD_SHAPE,
            ),
        )
        for ((snapshotId, payload, expectedFailure) in cases) {
            val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
            createUsersTable(db)
            withConnectedClient(
                    db = db,
                    syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
                    transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
                    configureServer = {
                        snapshotRoutes(
                            snapshotId = snapshotId,
                            snapshotBundleSeq = 9,
                            rows = listOf(
                                SnapshotChunkRow(
                                    "users",
                                    """{"id":"user-1"}""",
                                    21,
                                    payload,
                                ),
                            ),
                            byteCount = 32,
                        )
                    },
                ) { client ->
                    val error = assertIs<SnapshotSemanticException>(client.rebuild().exceptionOrNull())
                    assertEquals(expectedFailure, error.failure, snapshotId)
                    assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
                    assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users"))
                }
        }
    }

    @Test
    fun stageSnapshotChunkRejectsBlobKeyPayloadMismatchBeforeStaging() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createBlobUsersTable(db)
        withConnectedClient(
                db = db,
                syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
                transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
                configureServer = {
                    snapshotRoutes(
                        snapshotId = "blob-key-payload-mismatch",
                        snapshotBundleSeq = 9,
                        rows = listOf(
                            SnapshotChunkRow(
                                "users",
                                """{"id":"00000000-0000-0000-0000-000000000001"}""",
                                21,
                                """{"id":"00000000-0000-0000-0000-000000000002","name":"Ada"}""",
                            ),
                        ),
                        byteCount = 64,
                    )
                },
            ) { client ->
                val error = assertIs<SnapshotSemanticException>(client.rebuild().exceptionOrNull())
                assertEquals(SnapshotSemanticFailure.KEY_PAYLOAD_MISMATCH, error.failure)
                assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
                assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users"))
            }
    }

    @Test
    fun stageSnapshotChunkStepFailureRollsBackAndLeavesConnectionReusable() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        withConnectedClient(
            db = db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            configureServer = {
                snapshotRoutes(
                    snapshotId = "statement-step-failure",
                    snapshotBundleSeq = 9,
                    rows = listOf(
                        SnapshotChunkRow(
                            "users",
                            """{"id":"user-1"}""",
                            21,
                            """{"id":"user-1","name":"Ada"}""",
                        ),
                        SnapshotChunkRow(
                            "users",
                            """{"id":"user-2"}""",
                            22,
                            """{"id":"user-2","name":"Grace"}""",
                        ),
                    ),
                    byteCount = 64,
                )
            },
        ) { client ->
            db.execSQL(
                """
                CREATE TRIGGER fail_second_snapshot_stage_insert
                BEFORE INSERT ON _sync_snapshot_stage
                WHEN NEW.row_ordinal = 2
                BEGIN
                  SELECT RAISE(ABORT, 'stage-step-marker');
                END
                """.trimIndent(),
            )

            val error = client.rebuild().exceptionOrNull()
            assertTrue(error?.message.orEmpty().contains("stage-step-marker"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users"))

            db.execSQL("DROP TRIGGER fail_second_snapshot_stage_insert")
            client.rebuild().getOrThrow()
            assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM users"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
            assertEquals(9L, client.syncStatus().getOrThrow().lastBundleSeqSeen)
        }
    }

    @Test
    fun snapshotUpsertPlanResetFailureStillClearsAndPreventsTheNextMutation() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        db.execSQL("CREATE TABLE plan_probe(id TEXT PRIMARY KEY NOT NULL)")
        val cleanup = FailingReusableCleanup(failAtCall = 1)

        val observed = assertFailsWith<IllegalStateException> {
            db.withExclusiveAccess {
                db.prepare("INSERT INTO plan_probe(id) VALUES(?)").use { statement ->
                    val plan = SnapshotUpsertPlan(
                        preparedByTable = mapOf(
                            "plan_probe" to SnapshotUpsertPlan.PreparedTable(
                                tableInfo = singleTextKeyTableInfo("plan_probe"),
                                statement = statement,
                            ),
                        ),
                        reusableStatementCleanup = cleanup,
                    )
                    plan.upsertAuthoritativeRow("plan_probe", buildJsonObject { put("id", "first") })
                    plan.upsertAuthoritativeRow("plan_probe", buildJsonObject { put("id", "second") })
                }
            }
        }

        cleanup.assertFailureChain(observed)
        assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM plan_probe"))
        assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM plan_probe WHERE id = 'second'"))
        db.close()
    }

    @Test
    fun snapshotRowStatePlanResetFailureStillClearsAndPreventsTheNextMutation() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        db.execSQL(
            "CREATE TABLE row_state_probe(schema_name TEXT, table_name TEXT, key_json TEXT, row_version INTEGER, deleted INTEGER)",
        )
        val cleanup = FailingReusableCleanup(failAtCall = 1)

        val observed = assertFailsWith<IllegalStateException> {
            db.withExclusiveAccess {
                db.prepare("INSERT INTO row_state_probe VALUES(?, ?, ?, ?, ?)").use { statement ->
                    val plan = SnapshotRowStatePlan(statement, cleanup)
                    plan.update("main", "users", "first", 1L)
                    plan.update("main", "users", "second", 2L)
                }
            }
        }

        cleanup.assertFailureChain(observed)
        assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM row_state_probe"))
        assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM row_state_probe WHERE key_json = 'second'"))
        db.close()
    }

    @Test
    fun directSnapshotStageResetFailureStillClearsAndRollsBackTheChunk() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val cleanup = FailingReusableCleanup(failAtCall = 1)
        withConnectedClient(
            db = db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            configureServer = {
                snapshotRoutes(
                    snapshotId = "stage-reset-clear-failure",
                    snapshotBundleSeq = 9,
                    rows = listOf(
                        SnapshotChunkRow("users", """{"id":"first"}""", 1, """{"id":"first","name":"First"}"""),
                        SnapshotChunkRow("users", """{"id":"second"}""", 2, """{"id":"second","name":"Second"}"""),
                    ),
                    byteCount = 64,
                )
            },
        ) { client ->
            client.setSnapshotStageReusableStatementCleanupForTest(cleanup)

            val observed = requireNotNull(client.rebuild().exceptionOrNull()).deepestCause()

            cleanup.assertFailureChain(observed)
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users"))
            assertEquals(1L, scalarLong(db, "SELECT 1"))
        }
    }

    @Test
    fun firstCaptureKeysetSelectResetFailureStillClearsAndRollsBackCapture() = runBlocking<Unit> {
        val cleanup = FailingReusableCleanup(failAtCall = 1)
        val db = preparedTextCaptureDatabase(cleanup)

        val observed = assertFailsWith<IllegalStateException> {
            db.transaction(TransactionMode.IMMEDIATE) {
                capturePreparedRows(db, cleanup)
            }
        }

        cleanup.assertFailureChain(observed)
        assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        assertEquals(1L, scalarLong(db, "SELECT 1"))
        db.close()
    }

    @Test
    fun firstCaptureInsertResetFailureStillClearsAndRollsBackCapture() = runBlocking<Unit> {
        val cleanup = FailingReusableCleanup(failAtCall = 2)
        val db = preparedTextCaptureDatabase(cleanup)

        val observed = assertFailsWith<IllegalStateException> {
            db.transaction(TransactionMode.IMMEDIATE) {
                capturePreparedRows(db, cleanup)
            }
        }

        cleanup.assertFailureChain(observed)
        assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        assertEquals(1L, scalarLong(db, "SELECT 1"))
        db.close()
    }

    @Test
    fun managedTableRegistrationResetFailureStillClearsAndRollsBackRegistration() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        initializeOversqliteControlTables(db)
        val cleanup = FailingReusableCleanup(failAtCall = 1)
        val store = OversqliteManagedTableStore(db, cleanup)
        val validated = ValidatedConfig(
            schema = "main",
            tables = listOf(
                ValidatedSyncTable("first", "id"),
                ValidatedSyncTable("second", "id"),
            ),
            pkByTable = emptyMap(),
            keyByTable = emptyMap(),
            tableOrder = emptyMap(),
            tableInfoByName = emptyMap(),
        )

        val observed = assertFailsWith<IllegalStateException> {
            db.transaction(TransactionMode.IMMEDIATE) {
                store.registerManagedTables(validated)
            }
        }

        cleanup.assertFailureChain(observed)
        assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_managed_tables"))
        assertEquals(1L, scalarLong(db, "SELECT 1"))
        db.close()
    }

    @Test
    fun snapshotUpsertPlanResetFailure_usesPlatformObserverAndFatallyDisposesConnection() = runBlocking<Unit> {
        assertSnapshotUpsertObserverFailure(JvmCleanupFailureMode.RESET_THEN_CLEAR)
    }

    @Test
    fun snapshotUpsertPlanClearFailure_usesPlatformObserverAndFatallyDisposesConnection() = runBlocking<Unit> {
        assertSnapshotUpsertObserverFailure(JvmCleanupFailureMode.CLEAR_ONLY)
    }

    @Test
    fun snapshotRowStatePlanResetFailure_usesPlatformObserverAndFatallyDisposesConnection() = runBlocking<Unit> {
        assertSnapshotRowStateObserverFailure(JvmCleanupFailureMode.RESET_THEN_CLEAR)
    }

    @Test
    fun snapshotRowStatePlanClearFailure_usesPlatformObserverAndFatallyDisposesConnection() = runBlocking<Unit> {
        assertSnapshotRowStateObserverFailure(JvmCleanupFailureMode.CLEAR_ONLY)
    }

    @Test
    fun directSnapshotStageResetFailure_usesPlatformObserverAndFatallyDisposesConnection() = runBlocking<Unit> {
        assertDirectSnapshotStageObserverFailure(JvmCleanupFailureMode.RESET_THEN_CLEAR)
    }

    @Test
    fun directSnapshotStageClearFailure_usesPlatformObserverAndFatallyDisposesConnection() = runBlocking<Unit> {
        assertDirectSnapshotStageObserverFailure(JvmCleanupFailureMode.CLEAR_ONLY)
    }

    @Test
    fun firstCaptureKeysetSelectResetFailure_usesPlatformObserverAndFatallyDisposesConnection() =
        runBlocking<Unit> {
            assertFirstCaptureObserverFailure(
                mode = JvmCleanupFailureMode.RESET_THEN_CLEAR,
                sqlMatcher = { sql -> sql.contains("WHERE existing_row.\"id\" > ?") },
            )
        }

    @Test
    fun firstCaptureKeysetSelectClearFailure_usesPlatformObserverAndFatallyDisposesConnection() =
        runBlocking<Unit> {
            assertFirstCaptureObserverFailure(
                mode = JvmCleanupFailureMode.CLEAR_ONLY,
                sqlMatcher = { sql -> sql.contains("WHERE existing_row.\"id\" > ?") },
            )
        }

    @Test
    fun firstCaptureInsertResetFailure_usesPlatformObserverAndFatallyDisposesConnection() = runBlocking<Unit> {
        assertFirstCaptureObserverFailure(
            mode = JvmCleanupFailureMode.RESET_THEN_CLEAR,
            sqlMatcher = { sql -> sql.contains("INSERT INTO _sync_dirty_rows") },
        )
    }

    @Test
    fun firstCaptureInsertClearFailure_usesPlatformObserverAndFatallyDisposesConnection() = runBlocking<Unit> {
        assertFirstCaptureObserverFailure(
            mode = JvmCleanupFailureMode.CLEAR_ONLY,
            sqlMatcher = { sql -> sql.contains("INSERT INTO _sync_dirty_rows") },
        )
    }

    @Test
    fun managedTableRegistrationResetFailure_usesPlatformObserverAndFatallyDisposesConnection() =
        runBlocking<Unit> {
            assertManagedTableRegistrationObserverFailure(JvmCleanupFailureMode.RESET_THEN_CLEAR)
        }

    @Test
    fun managedTableRegistrationClearFailure_usesPlatformObserverAndFatallyDisposesConnection() =
        runBlocking<Unit> {
            assertManagedTableRegistrationObserverFailure(JvmCleanupFailureMode.CLEAR_ONLY)
        }

    @Test
    fun globalCleanupObserver_detectsAndRejectsWorkOnDifferentPreparedStatement() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        db.execSQL("CREATE TABLE observer_probe(id TEXT)")
        val fault = JvmStatementCleanupFault.install(
            db = db,
            mode = JvmCleanupFailureMode.RESET_THEN_CLEAR,
            sqlMatcher = { sql -> sql.contains("SELECT id FROM observer_probe") },
        )

        val fatalDisposalFailure = assertFailsWith<Throwable> {
            db.withExclusiveAccess {
                db.prepare("INSERT INTO observer_probe(id) VALUES(?)").use { differentStatement ->
                    db.prepare("SELECT id FROM observer_probe").use { selectedStatement ->
                        val cleanupFailure = assertFailsWith<Throwable> {
                            DefaultReusableStatementCleanup(selectedStatement)
                        }
                        fault.assertFailureGraph(cleanupFailure)

                        val rejectedWork = assertFailsWith<Throwable> {
                            differentStatement.bindNull(1)
                        }
                        assertEquals(1, rejectedWork.countIdentity(fault.postCleanupWorkFailure))
                        assertEquals(1, fault.allStatementBindOrStepCallsAfterFailure)
                        assertEquals(1, fault.differentStatementBindOrStepCallsAfterFailure)
                        assertEquals(0, fault.selectedStatement.bindOrStepCallsAfterFailure)
                    }
                }
            }
        }

        fault.assertFailureGraph(fatalDisposalFailure)
        assertEquals(1, fault.rawConnectionCloseCalls)
    }

    @Test
    fun firstCaptureUsesBoundedTextAndBlobKeysets() = runBlocking<Unit> {
        val textDb = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(textDb)
        textDb.execSQL("INSERT INTO users(id, name) VALUES('a', 'A'), ('b', 'B'), ('c', 'C')")
        prepareAndCapture(textDb, SyncTable("users", syncKeyColumnName = "id"))
        assertEquals(3L, scalarLong(textDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        assertEquals(
            listOf("{\"id\":\"a\"}", "{\"id\":\"b\"}", "{\"id\":\"c\"}"),
            scalarTexts(textDb, "SELECT key_json FROM _sync_dirty_rows ORDER BY dirty_ordinal"),
        )
        textDb.close()

        val blobDb = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createBlobUsersTable(blobDb)
        blobDb.execSQL(
            "INSERT INTO users(id, name) VALUES(x'00000000000000000000000000000001', 'A'), " +
                "(x'00000000000000000000000000000002', 'B')",
        )
        prepareAndCapture(blobDb, SyncTable("users", syncKeyColumnName = "id"))
        assertEquals(2L, scalarLong(blobDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        assertEquals(
            listOf(
                "{\"id\":\"00000000000000000000000000000001\"}",
                "{\"id\":\"00000000000000000000000000000002\"}",
            ),
            scalarTexts(blobDb, "SELECT key_json FROM _sync_dirty_rows ORDER BY dirty_ordinal"),
        )
        blobDb.close()
    }

    private suspend fun assertSnapshotUpsertObserverFailure(mode: JvmCleanupFailureMode) {
        val databasePath = Files.createTempDirectory("snapshot-upsert-observer-").resolve("client.sqlite")
        val db = BundledSqliteConnectionProvider.openConnection(databasePath.toString(), debug = false)
        db.execSQL("CREATE TABLE plan_probe(id TEXT PRIMARY KEY NOT NULL)")
        val fault = JvmStatementCleanupFault.install(
            db = db,
            mode = mode,
            sqlMatcher = { sql -> sql.contains("INSERT INTO plan_probe") },
        )

        val observed = assertFailsWith<Throwable> {
            db.withExclusiveAccess {
                db.prepare("INSERT INTO plan_probe(id) VALUES(?)").use { statement ->
                    val plan = SnapshotUpsertPlan(
                        preparedByTable = mapOf(
                            "plan_probe" to SnapshotUpsertPlan.PreparedTable(
                                tableInfo = singleTextKeyTableInfo("plan_probe"),
                                statement = statement,
                            ),
                        ),
                    )
                    plan.upsertAuthoritativeRow("plan_probe", buildJsonObject { put("id", "first") })
                    plan.upsertAuthoritativeRow("plan_probe", buildJsonObject { put("id", "second") })
                }
            }
        }

        assertFatalObserverOutcome(db, fault, observed)
        assertEquals(1L, reopenedScalarLong(databasePath.toString(), "SELECT COUNT(*) FROM plan_probe"))
        assertEquals(
            0L,
            reopenedScalarLong(databasePath.toString(), "SELECT COUNT(*) FROM plan_probe WHERE id = 'second'"),
        )
    }

    private suspend fun assertSnapshotRowStateObserverFailure(mode: JvmCleanupFailureMode) {
        val databasePath = Files.createTempDirectory("snapshot-row-state-observer-").resolve("client.sqlite")
        val db = BundledSqliteConnectionProvider.openConnection(databasePath.toString(), debug = false)
        db.execSQL(
            "CREATE TABLE row_state_probe(schema_name TEXT, table_name TEXT, key_json TEXT, row_version INTEGER, deleted INTEGER)",
        )
        val fault = JvmStatementCleanupFault.install(
            db = db,
            mode = mode,
            sqlMatcher = { sql -> sql.contains("INSERT INTO row_state_probe") },
        )

        val observed = assertFailsWith<Throwable> {
            db.withExclusiveAccess {
                db.prepare("INSERT INTO row_state_probe VALUES(?, ?, ?, ?, ?)").use { statement ->
                    val plan = SnapshotRowStatePlan(statement)
                    plan.update("main", "users", "first", 1L)
                    plan.update("main", "users", "second", 2L)
                }
            }
        }

        assertFatalObserverOutcome(db, fault, observed)
        assertEquals(1L, reopenedScalarLong(databasePath.toString(), "SELECT COUNT(*) FROM row_state_probe"))
        assertEquals(
            0L,
            reopenedScalarLong(
                databasePath.toString(),
                "SELECT COUNT(*) FROM row_state_probe WHERE key_json = 'second'",
            ),
        )
    }

    private suspend fun assertDirectSnapshotStageObserverFailure(mode: JvmCleanupFailureMode) {
        val databasePath = Files.createTempDirectory("snapshot-stage-observer-").resolve("client.sqlite")
        val db = BundledSqliteConnectionProvider.openConnection(databasePath.toString(), debug = false)
        createUsersTable(db)
        val fault = JvmStatementCleanupFault.install(
            db = db,
            mode = mode,
            sqlMatcher = { sql -> sql.contains("INSERT INTO _sync_snapshot_stage") },
        )
        withConnectedClient(
            db = db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            configureServer = {
                snapshotRoutes(
                    snapshotId = "stage-observer-failure",
                    snapshotBundleSeq = 9,
                    rows = listOf(
                        SnapshotChunkRow("users", """{"id":"first"}""", 1, """{"id":"first","name":"First"}"""),
                        SnapshotChunkRow("users", """{"id":"second"}""", 2, """{"id":"second","name":"Second"}"""),
                    ),
                    byteCount = 64,
                )
            },
        ) { client ->
            val observed = requireNotNull(client.rebuild().exceptionOrNull())
            assertFatalObserverOutcome(db, fault, observed)
        }

        assertEquals(
            0L,
            reopenedScalarLong(databasePath.toString(), "SELECT COUNT(*) FROM _sync_snapshot_stage"),
        )
        assertEquals(0L, reopenedScalarLong(databasePath.toString(), "SELECT COUNT(*) FROM users"))
    }

    private suspend fun assertFirstCaptureObserverFailure(
        mode: JvmCleanupFailureMode,
        sqlMatcher: (String) -> Boolean,
    ) {
        val databasePath = Files.createTempDirectory("first-capture-observer-").resolve("client.sqlite")
        val db = BundledSqliteConnectionProvider.openConnection(databasePath.toString(), debug = false)
        createUsersTable(db)
        db.execSQL("INSERT INTO users(id, name) VALUES('a', 'A'), ('b', 'B')")
        val initializer = captureInitializer()
        val validated = db.transaction(TransactionMode.IMMEDIATE) {
            initializer.prepareLocalRuntime(db, OversqliteManagedTableStore(db))
        }
        val fault = JvmStatementCleanupFault.install(db, mode, sqlMatcher)

        val observed = assertFailsWith<Throwable> {
            db.transaction(TransactionMode.IMMEDIATE) {
                initializer.capturePreexistingAnonymousRows(db, validated)
            }
        }

        assertFatalObserverOutcome(db, fault, observed)
        assertEquals(0L, reopenedScalarLong(databasePath.toString(), "SELECT COUNT(*) FROM _sync_dirty_rows"))
    }

    private suspend fun assertManagedTableRegistrationObserverFailure(mode: JvmCleanupFailureMode) {
        val databasePath = Files.createTempDirectory("managed-registration-observer-").resolve("client.sqlite")
        val db = BundledSqliteConnectionProvider.openConnection(databasePath.toString(), debug = false)
        initializeOversqliteControlTables(db)
        val fault = JvmStatementCleanupFault.install(
            db = db,
            mode = mode,
            sqlMatcher = { sql -> sql.contains("INSERT INTO _sync_managed_tables") },
        )
        val store = OversqliteManagedTableStore(db)
        val validated = ValidatedConfig(
            schema = "main",
            tables = listOf(
                ValidatedSyncTable("first", "id"),
                ValidatedSyncTable("second", "id"),
            ),
            pkByTable = emptyMap(),
            keyByTable = emptyMap(),
            tableOrder = emptyMap(),
            tableInfoByName = emptyMap(),
        )

        val observed = assertFailsWith<Throwable> {
            db.transaction(TransactionMode.IMMEDIATE) {
                store.registerManagedTables(validated)
            }
        }

        assertFatalObserverOutcome(db, fault, observed)
        assertEquals(
            0L,
            reopenedScalarLong(databasePath.toString(), "SELECT COUNT(*) FROM _sync_managed_tables"),
        )
    }

    private suspend fun assertFatalObserverOutcome(
        db: SafeSQLiteConnection,
        fault: JvmStatementCleanupFault,
        observed: Throwable,
    ) {
        assertEquals(0, fault.differentStatementBindOrStepCallsAfterFailure)
        assertEquals(0, fault.selectedStatement.bindOrStepCallsAfterFailure)
        assertEquals(0, fault.allStatementBindOrStepCallsAfterFailure)
        fault.assertFailureGraph(observed)
        val prepareCallsBeforeRejection = fault.prepareCalls
        assertTrue(runCatching { db.execSQL("SELECT rejected-after-fatal-cleanup") }.isFailure)
        assertEquals(prepareCallsBeforeRejection, fault.prepareCalls)
        assertEquals(1, fault.rawConnectionCloseCalls)
        assertEquals(1, fault.selectedStatement.closeCalls)
    }

    private suspend fun reopenedScalarLong(databasePath: String, sql: String): Long {
        val reopened = BundledSqliteConnectionProvider.openConnection(databasePath, debug = false)
        return try {
            scalarLong(reopened, sql)
        } finally {
            reopened.close()
        }
    }

    private suspend fun preparedTextCaptureDatabase(
        cleanup: ReusableStatementCleanup,
    ): SafeSQLiteConnection {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        db.execSQL("INSERT INTO users(id, name) VALUES('a', 'A'), ('b', 'B')")
        val initializer = captureInitializer(cleanup)
        db.transaction(TransactionMode.IMMEDIATE) {
            initializer.prepareLocalRuntime(db, OversqliteManagedTableStore(db))
        }
        return db
    }

    private suspend fun capturePreparedRows(
        db: SafeSQLiteConnection,
        cleanup: ReusableStatementCleanup,
    ) {
        val initializer = captureInitializer(cleanup)
        val validated = ValidatedConfig(
            schema = "main",
            tables = listOf(ValidatedSyncTable("users", "id")),
            pkByTable = mapOf("users" to "id"),
            keyByTable = mapOf("users" to listOf("id")),
            tableOrder = mapOf("users" to 0),
            tableInfoByName = mapOf("users" to TableInfoCache().get(db, "users")),
        )
        initializer.capturePreexistingAnonymousRows(db, validated)
    }

    private suspend fun prepareAndCapture(db: SafeSQLiteConnection, table: SyncTable) {
        val initializer = SyncRuntimeInitializer(
            config = OversqliteConfig(schema = "main", syncTables = listOf(table)),
            tableInfoCache = TableInfoCache(),
        )
        db.transaction(TransactionMode.IMMEDIATE) {
            val validated = initializer.prepareLocalRuntime(db, OversqliteManagedTableStore(db))
            initializer.capturePreexistingAnonymousRows(db, validated)
        }
    }

    private fun captureInitializer(
        cleanup: ReusableStatementCleanup = DefaultReusableStatementCleanup,
    ) = SyncRuntimeInitializer(
        config = OversqliteConfig(
            schema = "main",
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
        ),
        tableInfoCache = TableInfoCache(),
        reusableStatementCleanup = cleanup,
    )

    private suspend fun scalarTexts(db: SafeSQLiteConnection, sql: String): List<String> =
        db.prepare(sql).use { statement ->
            buildList {
                while (statement.step()) add(statement.getText(0))
            }
        }

    private fun singleTextKeyTableInfo(tableName: String) = TableInfo(
        table = tableName,
        columns = listOf(
            ColumnInfo(
                name = "id",
                declaredType = "TEXT",
                isPrimaryKey = true,
                notNull = true,
                defaultValue = null,
            ),
        ),
    )

    private class FailingReusableCleanup(
        private val failAtCall: Int,
    ) : ReusableStatementCleanup {
        val resetFailure = IllegalStateException("RESET_SENTINEL")
        val clearFailure = IllegalStateException("CLEAR_SENTINEL")
        var calls: Int = 0
            private set
        var resetCalls: Int = 0
            private set
        var clearCalls: Int = 0
            private set

        override fun invoke(statement: SqliteStatement) {
            calls++
            if (calls != failAtCall) {
                DefaultReusableStatementCleanup(statement)
                return
            }
            resetAndClearReusableStatement(
                statement = statement,
                reset = {
                    resetCalls++
                    throw resetFailure
                },
                clearBindings = {
                    clearCalls++
                    throw clearFailure
                },
            )
        }

        fun assertFailureChain(observed: Throwable) {
            val original = generateSequence(observed) { it.cause }.last()
            assertTrue(original === resetFailure)
            assertEquals(resetFailure.message, original.message)
            assertEquals(listOf(clearFailure.message), original.suppressed.map { it.message })
            assertEquals(1, resetCalls)
            assertEquals(1, clearCalls)
        }
    }

    private fun Throwable.deepestCause(): Throwable = generateSequence(this) { it.cause }.last()

    private suspend fun stagedRows(db: SafeSQLiteConnection): List<String> {
        return db.prepare(
            """
            SELECT row_ordinal, schema_name, table_name, key_json, row_version, payload
            FROM _sync_snapshot_stage
            ORDER BY row_ordinal
            """.trimIndent(),
        ).use { st ->
            buildList {
                while (st.step()) {
                    add(
                        listOf(
                            st.getLong(0).toString(),
                            st.getText(1),
                            st.getText(2),
                            st.getText(3),
                            st.getLong(4).toString(),
                            st.getText(5),
                        ).joinToString("|"),
                    )
                }
            }
        }
    }
}

internal enum class JvmCleanupFailureMode {
    RESET_THEN_CLEAR,
    CLEAR_ONLY,
}

internal class JvmStatementCleanupFault private constructor(
    private val mode: JvmCleanupFailureMode,
    originalConnection: SQLiteConnection,
    sqlMatcher: (String) -> Boolean,
) {
    val resetFailure = IllegalStateException("RAW_RESET_FAILURE")
    val clearFailure = IllegalStateException("RAW_CLEAR_FAILURE")
    val postCleanupWorkFailure = IllegalStateException("statement work rejected after cleanup failure")
    private val connection = FaultingConnection(originalConnection, sqlMatcher)
    private var cleanupFailureObserved = false

    var allStatementBindOrStepCallsAfterFailure: Int = 0
        private set

    var differentStatementBindOrStepCallsAfterFailure: Int = 0
        private set

    val prepareCalls: Int
        get() = connection.prepareCalls

    val rawConnectionCloseCalls: Int
        get() = connection.closeCalls

    val selectedStatement: FaultingStatement
        get() = requireNotNull(connection.selectedStatement) { "the expected raw statement was not prepared" }

    fun assertFailureGraph(observed: Throwable) {
        val statement = selectedStatement
        assertEquals(listOf("reset", "clear"), statement.events.take(2))
        assertEquals(1, statement.resetCalls)
        assertEquals(1, statement.clearCalls)
        when (mode) {
            JvmCleanupFailureMode.RESET_THEN_CLEAR -> {
                assertEquals(1, observed.countIdentity(resetFailure))
                assertEquals(1, observed.countIdentity(clearFailure))
                assertEquals(listOf(clearFailure), resetFailure.suppressed.toList())
            }
            JvmCleanupFailureMode.CLEAR_ONLY -> {
                assertEquals(0, observed.countIdentity(resetFailure))
                assertEquals(1, observed.countIdentity(clearFailure))
                assertTrue(clearFailure.suppressed.isEmpty())
            }
        }
    }

    private inner class FaultingConnection(
        private val delegate: SQLiteConnection,
        private val sqlMatcher: (String) -> Boolean,
    ) : SQLiteConnection {
        var prepareCalls: Int = 0
            private set
        var closeCalls: Int = 0
            private set
        var selectedStatement: FaultingStatement? = null
            private set

        override fun prepare(sql: String): SQLiteStatement {
            prepareCalls++
            val statement = delegate.prepare(sql)
            val injectCleanupFailure = selectedStatement == null && sqlMatcher(sql)
            // Fatal transaction cleanup must still issue its rollback; reject any other later statement work.
            val observePostCleanupWork = !cleanupFailureObserved || sql != "ROLLBACK"
            return FaultingStatement(statement, injectCleanupFailure, observePostCleanupWork).also { wrapped ->
                if (injectCleanupFailure) selectedStatement = wrapped
            }
        }

        override fun inTransaction(): Boolean = delegate.inTransaction()

        override fun close() {
            closeCalls++
            delegate.close()
        }
    }

    internal inner class FaultingStatement(
        private val delegate: SQLiteStatement,
        private val injectCleanupFailure: Boolean,
        private val observePostCleanupWork: Boolean,
    ) : SQLiteStatement {
        val events = mutableListOf<String>()
        var resetCalls: Int = 0
            private set
        var clearCalls: Int = 0
            private set
        var closeCalls: Int = 0
            private set
        var bindOrStepCallsAfterFailure: Int = 0
            private set

        override fun bindBlob(index: Int, value: ByteArray) {
            observeBindOrStep()
            delegate.bindBlob(index, value)
        }

        override fun bindDouble(index: Int, value: Double) {
            observeBindOrStep()
            delegate.bindDouble(index, value)
        }

        override fun bindLong(index: Int, value: Long) {
            observeBindOrStep()
            delegate.bindLong(index, value)
        }

        override fun bindText(index: Int, value: String) {
            observeBindOrStep()
            delegate.bindText(index, value)
        }

        override fun bindNull(index: Int) {
            observeBindOrStep()
            delegate.bindNull(index)
        }

        override fun getBlob(index: Int): ByteArray = delegate.getBlob(index)
        override fun getDouble(index: Int): Double = delegate.getDouble(index)
        override fun getLong(index: Int): Long = delegate.getLong(index)
        override fun getText(index: Int): String = delegate.getText(index)
        override fun isNull(index: Int): Boolean = delegate.isNull(index)
        override fun getColumnCount(): Int = delegate.getColumnCount()
        override fun getColumnName(index: Int): String = delegate.getColumnName(index)
        override fun getColumnType(index: Int): Int = delegate.getColumnType(index)

        override fun step(): Boolean {
            observeBindOrStep()
            return delegate.step()
        }

        override fun reset() {
            events += "reset"
            resetCalls++
            if (injectCleanupFailure && mode == JvmCleanupFailureMode.RESET_THEN_CLEAR) {
                cleanupFailureObserved = true
                throw resetFailure
            }
            delegate.reset()
        }

        override fun clearBindings() {
            events += "clear"
            clearCalls++
            if (injectCleanupFailure) {
                cleanupFailureObserved = true
                throw clearFailure
            }
            delegate.clearBindings()
        }

        override fun close() {
            events += "close"
            closeCalls++
            delegate.close()
        }

        private fun observeBindOrStep() {
            if (!cleanupFailureObserved || !observePostCleanupWork) return
            allStatementBindOrStepCallsAfterFailure++
            if (injectCleanupFailure) {
                bindOrStepCallsAfterFailure++
            } else {
                differentStatementBindOrStepCallsAfterFailure++
            }
            throw postCleanupWorkFailure
        }
    }

    companion object {
        fun install(
            db: SafeSQLiteConnection,
            mode: JvmCleanupFailureMode,
            sqlMatcher: (String) -> Boolean,
        ): JvmStatementCleanupFault {
            val delegateField = db.ref.javaClass.declaredFields.single { it.name == "delegate" }
            delegateField.isAccessible = true
            val originalConnection = delegateField.get(db.ref) as SQLiteConnection
            return JvmStatementCleanupFault(mode, originalConnection, sqlMatcher).also { fault ->
                delegateField.set(db.ref, fault.connection)
            }
        }
    }
}

private fun Throwable.countIdentity(
    target: Throwable,
    visited: MutableList<Throwable> = mutableListOf(),
): Int {
    if (visited.any { it === this }) return 0
    visited += this
    return (if (this === target) 1 else 0) +
        (cause?.countIdentity(target, visited) ?: 0) +
        suppressed.sumOf { it.countIdentity(target, visited) }
}

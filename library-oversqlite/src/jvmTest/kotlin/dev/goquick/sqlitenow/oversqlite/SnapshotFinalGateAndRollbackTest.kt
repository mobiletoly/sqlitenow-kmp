/*
 * Copyright 2025 Toly Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import java.nio.file.Files
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue

class SnapshotFinalGateAndRollbackTest : BundleClientContractTestSupport() {
    @Test
    fun exactCountEvaluationKeepsGuardAndPreparedProofsAtLongPrecision() {
        val overInt = Int.MAX_VALUE.toLong() + 41L
        val counts = OversqliteCountEvaluation(
            dirtyRowCount = overInt,
            outboundRowCount = overInt + 1L,
            sourceBundleRowCount = overInt + 1L,
            expectedOutboxRowCount = overInt + 1L,
        )

        assertTrue(counts.hasDirtyRows)
        assertTrue(counts.hasOutboundRows)
        assertTrue(counts.hasPendingRows)
        assertEquals(overInt * 2L + 1L, counts.totalPendingRowCount)
        assertTrue(counts.preparedOutboxMatches)
        assertTrue(
            !counts.copy(sourceBundleRowCount = overInt).preparedOutboxMatches,
            "prepared proof must compare the unsaturated Long values",
        )
    }

    @Test
    fun legacyCountPropertiesSaturateOnlyAtExceptionBoundaries() {
        val overInt = Int.MAX_VALUE.toLong() + 41L

        assertEquals(Int.MAX_VALUE - 1, saturatingLegacyCount(Int.MAX_VALUE.toLong() - 1L))
        assertEquals(Int.MAX_VALUE, saturatingLegacyCount(Int.MAX_VALUE.toLong()))
        assertEquals(Int.MAX_VALUE, saturatingLegacyCount(overInt))
        assertEquals(
            Int.MAX_VALUE,
            CheckpointRecoveryBlockedException(
                reason = CheckpointRecoveryBlockedReason.PENDING_WORK,
                dirtyCount = saturatingLegacyCount(overInt),
                outboundCount = saturatingLegacyCount(overInt + 1L),
                replayState = "prepared",
            ).dirtyCount,
        )
        assertEquals(Int.MAX_VALUE, PendingPushReplayException(saturatingLegacyCount(overInt)).outboundCount)
        assertEquals(
            Int.MAX_VALUE,
            PushConflictRetryExhaustedException(
                retryCount = 2,
                remainingDirtyCount = saturatingLegacyCount(overInt),
            ).remainingDirtyCount,
        )
    }

    @Test
    fun pendingWorkModesAcceptOnlyTheirExactOutboxShapes() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        withConnectedClient(
            db = db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
        ) {
            db.execSQL("UPDATE _sync_attachment_state SET rebuild_required = 1 WHERE singleton_key = 1")
            val gate = newGate(db)
            assertEquals(SnapshotRebuildOutboxMode.CLEAR_ALL, gate.pin(SnapshotRebuildOutboxMode.CLEAR_ALL).mode)

            db.execSQL(
                """
                UPDATE _sync_outbox_bundle
                SET state = 'committed_remote', source_id = 'source', source_bundle_id = 1,
                    canonical_request_hash = 'request-hash', row_count = 1,
                    remote_bundle_seq = 7, remote_bundle_hash = 'bundle-hash'
                WHERE singleton_key = 1
                """.trimIndent(),
            )
            insertOutboxProbeRow(db)
            assertEquals(
                SnapshotRebuildOutboxMode.PRESERVE_COMMITTED_REMOTE,
                gate.pin(SnapshotRebuildOutboxMode.PRESERVE_COMMITTED_REMOTE).mode,
            )

            db.execSQL(
                """
                UPDATE _sync_operation_state
                SET kind = 'source_recovery', reason = 'history_pruned', replacement_source_id = 'replacement'
                WHERE singleton_key = 1
                """.trimIndent(),
            )
            assertIs<SourceRecoveryRequiredException>(
                runCatching { gate.pin(SnapshotRebuildOutboxMode.PRESERVE_SOURCE_RECOVERY) }.exceptionOrNull(),
            )

            db.execSQL(
                """
                UPDATE _sync_outbox_bundle
                SET state = 'prepared', remote_bundle_seq = 0, remote_bundle_hash = ''
                WHERE singleton_key = 1
                """.trimIndent(),
            )
            assertEquals(
                SnapshotRebuildOutboxMode.PRESERVE_SOURCE_RECOVERY,
                gate.pin(SnapshotRebuildOutboxMode.PRESERVE_SOURCE_RECOVERY).mode,
            )
        }
    }

    @Test
    fun finalGateRejectsEveryPinnedOutboxFieldAndActualRowCountMutation() = runBlocking {
        val mutations = listOf(
            "UPDATE _sync_outbox_bundle SET state = 'prepared', source_id = 'source' WHERE singleton_key = 1",
            "UPDATE _sync_outbox_bundle SET source_id = 'changed' WHERE singleton_key = 1",
            "UPDATE _sync_outbox_bundle SET source_bundle_id = 9 WHERE singleton_key = 1",
            "UPDATE _sync_outbox_bundle SET canonical_request_hash = 'changed' WHERE singleton_key = 1",
            "UPDATE _sync_outbox_bundle SET row_count = 1 WHERE singleton_key = 1",
            "UPDATE _sync_outbox_bundle SET remote_bundle_seq = 9 WHERE singleton_key = 1",
            "UPDATE _sync_outbox_bundle SET remote_bundle_hash = 'changed' WHERE singleton_key = 1",
            """
                INSERT INTO _sync_outbox_rows(
                  source_bundle_id, row_ordinal, schema_name, table_name, key_json,
                  wire_key_json, op, base_row_version, local_payload, wire_payload
                ) VALUES (1, 1, 'main', 'users', '{"id":"secret"}', '{"id":"secret"}',
                          'INSERT', 0, '{"id":"secret","name":"Secret"}',
                          '{"id":"secret","name":"Secret"}')
            """.trimIndent(),
        )

        for (mutation in mutations) {
            val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
            createUsersTable(db)
            withConnectedClient(
                db = db,
                syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            ) {
                db.execSQL("UPDATE _sync_attachment_state SET rebuild_required = 1 WHERE singleton_key = 1")
                val gate = newGate(db)
                val pinned = gate.pin(SnapshotRebuildOutboxMode.CLEAR_ALL)
                db.execSQL(mutation)
                val error = assertFailsWith<SnapshotFinalApplyGateException>(mutation) {
                    gate.validateFinal(
                        pinned,
                        SnapshotSession("snapshot", 0, 0, 0, "2099-01-01T00:00:00Z"),
                    )
                }
                assertEquals("CLEAR_ALL", error.mode)
                assertTrue(error.message.orEmpty().contains("final apply gate"))
                assertTrue(error.message.orEmpty().contains("secret").not())
            }
        }
    }

    @Test
    fun managedWriteDuringDownloadTripsFinalGateBeforeClear() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        withConnectedClient(
            db = db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            configureServer = {
                jsonRoute(
                    "/sync/snapshot-sessions",
                    body = """{"snapshot_id":"late-write","snapshot_bundle_seq":7,"row_count":1,"byte_count":32,"expires_at":"2099-01-01T00:00:00Z"}""",
                )
                createContext("/sync/snapshot-sessions/late-write") { exchange ->
                    when (exchange.requestMethod) {
                        "GET" -> {
                            runBlocking {
                                db.execSQL("INSERT INTO users(id, name) VALUES('late-local', 'Must survive')")
                            }
                            respondJson(
                                exchange,
                                200,
                                """{"snapshot_id":"late-write","snapshot_bundle_seq":7,"byte_count":32,"rows":[{"schema":"main","table":"users","key":{"id":"remote"},"row_version":7,"payload":{"id":"remote","name":"Remote"}}],"next_row_ordinal":1,"has_more":false}""",
                            )
                        }
                        "DELETE" -> {
                            exchange.sendResponseHeaders(204, -1)
                            exchange.close()
                        }
                    }
                }
            },
        ) { client ->
            assertIs<SnapshotFinalApplyGateException>(client.rebuild().exceptionOrNull())
            assertEquals("Must survive", scalarText(db, "SELECT name FROM users WHERE id = 'late-local'"))
            assertEquals(0, scalarLong(db, "SELECT COUNT(*) FROM users WHERE id = 'remote'"))
            assertEquals(1, scalarLong(db, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
        }
    }

    @Test
    fun beforeCommitFailureRollsBackAndKeepsConnectionReusable() = runBlocking {
        val directory = Files.createTempDirectory("oversqlite-rollback-disposal-")
        val databasePath = directory.resolve("client.sqlite")
        val db = BundledSqliteConnectionProvider.openConnection(databasePath.toString(), debug = false)
        createUsersTable(db)
        val primary = IllegalStateException("primary-commit-failure")

        withConnectedClient(
            db = db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            configureServer = {
                usersSnapshotRoutes(
                    snapshotId = "rollback-disposal",
                    snapshotBundleSeq = 7,
                    userId = "remote",
                    rowVersion = 7,
                    payloadJson = """{"id":"remote","name":"Remote"}""",
                )
            },
        ) { client ->
            db.execSQL("UPDATE _sync_apply_state SET apply_mode = 1 WHERE singleton_key = 1")
            db.execSQL("INSERT INTO users(id, name) VALUES('prior', 'Prior')")
            db.execSQL("UPDATE _sync_apply_state SET apply_mode = 0 WHERE singleton_key = 1")
            client.setSnapshotApplyFaultInjectorForTest(
                SnapshotApplyFaultInjector(
                    beforeCommit = { throw primary },
                ),
            )

            val error = requireNotNull(client.rebuild().exceptionOrNull())
            val originalFailure = error.originalRecoveredFailure()
            assertSame(primary, originalFailure)
            assertEquals(emptyList(), originalFailure.suppressed.toList())
            assertEquals(1L, scalarLong(db, "SELECT 1"))
        }

        val reopened = BundledSqliteConnectionProvider.openConnection(databasePath.toString(), debug = false)
        try {
            assertEquals("Prior", scalarText(reopened, "SELECT name FROM users WHERE id = 'prior'"))
            assertEquals(0, scalarLong(reopened, "SELECT COUNT(*) FROM users WHERE id = 'remote'"))
        } finally {
            reopened.close()
        }
    }

    @Test
    fun statementCleanupTimeoutPreservesOriginalAndCoreClosesTheLiveStatements() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val primary = CancellationException("original-apply-cancellation")
        withConnectedClient(
            db = db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            configureServer = {
                usersSnapshotRoutes(
                    snapshotId = "statement-cleanup-timeout",
                    snapshotBundleSeq = 7,
                    userId = "remote",
                    rowVersion = 7,
                    payloadJson = """{"id":"remote","name":"Remote"}""",
                )
            },
        ) { client ->
            client.setSnapshotApplyFaultInjectorForTest(
                SnapshotApplyFaultInjector(
                    afterAppliedRow = { throw primary },
                    statementCacheOperations = StatementCacheOperations(
                        close = { awaitCancellation() },
                    ),
                    cleanupTimeoutMillis = 50L,
                ),
            )

            val returnedError = assertFailsWith<CancellationException> { client.rebuild() }
            val originalFailure = returnedError.originalRecoveredFailure()

            assertSame(primary, originalFailure)
            assertTrue(
                originalFailure.suppressed.any { it is TimeoutCancellationException },
                originalFailure.stackTraceToString(),
            )
            assertEquals(1L, scalarLong(db, "SELECT 1"))
        }
    }

    @Test
    fun statementCacheSeamCloseFailureRollsBackAfterExhaustiveCleanup() = runBlocking {
        val directory = Files.createTempDirectory("oversqlite-statement-close-disposal-")
        val databasePath = directory.resolve("client.sqlite")
        val db = BundledSqliteConnectionProvider.openConnection(databasePath.toString(), debug = false)
        createUsersTable(db)
        var closeCalls = 0
        withConnectedClient(
            db = db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            configureServer = {
                usersSnapshotRoutes(
                    snapshotId = "statement-close-disposal",
                    snapshotBundleSeq = 7,
                    userId = "remote",
                    rowVersion = 7,
                    payloadJson = """{"id":"remote","name":"Remote"}""",
                )
            },
        ) { client ->
            db.execSQL("UPDATE _sync_apply_state SET apply_mode = 1 WHERE singleton_key = 1")
            db.execSQL("INSERT INTO users(id, name) VALUES('prior', 'Prior')")
            db.execSQL("UPDATE _sync_apply_state SET apply_mode = 0 WHERE singleton_key = 1")
            client.setSnapshotApplyFaultInjectorForTest(
                SnapshotApplyFaultInjector(
                    statementCacheOperations = StatementCacheOperations(
                        close = {
                            closeCalls++
                            throw IllegalStateException("statement-close-$closeCalls")
                        },
                    ),
                ),
            )

            val error = assertIs<IllegalStateException>(client.rebuild().exceptionOrNull())
            val originalFailure = error.originalRecoveredFailure()

            assertEquals("statement-close-1", originalFailure.message)
            assertTrue(closeCalls > 1)
            assertEquals(closeCalls - 1, originalFailure.suppressed.size)
            assertEquals(1L, scalarLong(db, "SELECT 1"))
        }

        val reopened = BundledSqliteConnectionProvider.openConnection(databasePath.toString(), debug = false)
        try {
            assertEquals("Prior", scalarText(reopened, "SELECT name FROM users WHERE id = 'prior'"))
            assertEquals(0, scalarLong(reopened, "SELECT COUNT(*) FROM users WHERE id = 'remote'"))
        } finally {
            reopened.close()
        }
    }

    @Test
    fun cleanupFailureOrderingPreservesCancellationThenStatementCleanup() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val primary = CancellationException("original-cancellation")
        val statementCleanup = IllegalStateException("statement-cleanup")
        var closeCalls = 0
        withConnectedClient(
            db = db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            configureServer = {
                usersSnapshotRoutes(
                    snapshotId = "cleanup-failure-ordering",
                    snapshotBundleSeq = 7,
                    userId = "remote",
                    rowVersion = 7,
                    payloadJson = """{"id":"remote","name":"Remote"}""",
                )
            },
        ) { client ->
            client.setSnapshotApplyFaultInjectorForTest(
                SnapshotApplyFaultInjector(
                    afterAppliedRow = { throw primary },
                    statementCacheOperations = StatementCacheOperations(
                        close = {
                            closeCalls++
                            throw statementCleanup
                        },
                    ),
                ),
            )

            val error = assertFailsWith<CancellationException> { client.rebuild() }
            val originalFailure = error.originalRecoveredFailure()

            assertSame(primary, originalFailure)
            assertEquals(1, originalFailure.suppressed.size)
            assertSame(statementCleanup, originalFailure.suppressed.single().originalRecoveredFailure())
            assertTrue(closeCalls > 1)
            assertEquals(1L, scalarLong(db, "SELECT 1"))
        }
    }

    @Test
    fun cancellationAfterDestructiveWorkRollsBackManagedReplacement() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        withConnectedClient(
            db = db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            configureServer = {
                usersSnapshotRoutes(
                    snapshotId = "cancel-after-clear",
                    snapshotBundleSeq = 8,
                    userId = "remote",
                    rowVersion = 8,
                    payloadJson = """{"id":"remote","name":"Remote"}""",
                )
            },
        ) { client ->
            db.execSQL("UPDATE _sync_apply_state SET apply_mode = 1 WHERE singleton_key = 1")
            db.execSQL("INSERT INTO users(id, name) VALUES('prior', 'Prior')")
            db.execSQL("UPDATE _sync_apply_state SET apply_mode = 0 WHERE singleton_key = 1")
            client.setSnapshotApplyFaultInjectorForTest(
                SnapshotApplyFaultInjector(beforeCommit = { throw CancellationException("cancel after apply") }),
            )

            assertFailsWith<CancellationException> { client.rebuild() }
            assertEquals("Prior", scalarText(db, "SELECT name FROM users WHERE id = 'prior'"))
            assertEquals(0, scalarLong(db, "SELECT COUNT(*) FROM users WHERE id = 'remote'"))
            assertEquals(0, scalarLong(db, "SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1"))
        }
    }

    @Test
    fun externalJobCancellationClosesPreparedStatementsAndRollsBackReplacement() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        withConnectedClient(
            db = db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            configureServer = {
                usersSnapshotRoutes(
                    snapshotId = "external-job-cancel",
                    snapshotBundleSeq = 8,
                    userId = "remote",
                    rowVersion = 8,
                    payloadJson = """{"id":"remote","name":"Remote"}""",
                )
            },
        ) { client ->
            db.execSQL("UPDATE _sync_apply_state SET apply_mode = 1 WHERE singleton_key = 1")
            db.execSQL("INSERT INTO users(id, name) VALUES('prior', 'Prior')")
            db.execSQL("UPDATE _sync_apply_state SET apply_mode = 0 WHERE singleton_key = 1")
            val rowApplied = CompletableDeferred<Unit>()
            val statementsClosed = CompletableDeferred<Unit>()
            client.setSnapshotApplyFaultInjectorForTest(
                SnapshotApplyFaultInjector(
                    afterAppliedRow = {
                        rowApplied.complete(Unit)
                        awaitCancellation()
                    },
                    afterStatementCacheClosed = { statementsClosed.complete(Unit) },
                ),
            )

            val rebuild = async { client.rebuild() }
            rowApplied.await()
            rebuild.cancel()
            assertFailsWith<CancellationException> { rebuild.await() }
            statementsClosed.await()

            assertEquals("Prior", scalarText(db, "SELECT name FROM users WHERE id = 'prior'"))
            assertEquals(0, scalarLong(db, "SELECT COUNT(*) FROM users WHERE id = 'remote'"))
            assertEquals(0, scalarLong(db, "SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1"))
            assertEquals(1, scalarLong(db, "SELECT 1"))
        }
    }

    @Test
    fun externalJobCancellationWithCacheSeamFailureStillLetsCoreCloseStatements() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        var closeAttempts = 0
        withConnectedClient(
            db = db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            configureServer = {
                usersSnapshotRoutes(
                    snapshotId = "external-job-cancel-discard",
                    snapshotBundleSeq = 8,
                    userId = "remote",
                    rowVersion = 8,
                    payloadJson = """{"id":"remote","name":"Remote"}""",
                )
            },
        ) { client ->
            val rowApplied = CompletableDeferred<Unit>()
            val closeFailure = IllegalStateException("statement-close-failure")
            client.setSnapshotApplyFaultInjectorForTest(
                SnapshotApplyFaultInjector(
                    afterAppliedRow = {
                        rowApplied.complete(Unit)
                        awaitCancellation()
                    },
                    statementCacheOperations = StatementCacheOperations(
                        close = {
                            closeAttempts++
                            throw closeFailure
                        },
                    ),
                ),
            )

            val rebuild = async { client.rebuild() }
            rowApplied.await()
            rebuild.cancel()
            assertFailsWith<CancellationException> { rebuild.await() }

            assertTrue(closeAttempts > 0)
            assertEquals(1L, scalarLong(db, "SELECT 1"))
        }
    }

    @Test
    fun laterApplyPageFailureRollsBackEarlierPageAndCountsHeldPageHighWater() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        withConnectedClient(
            db = db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            snapshotApplyBatchRows = 1,
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            configureServer = {
                twoChunkSnapshotRoutes(
                    snapshotId = "later-page-failure",
                    snapshotBundleSeq = 9,
                    firstRow = SnapshotChunkRow(
                        table = "users",
                        keyJson = """{"id":"first"}""",
                        rowVersion = 9,
                        payloadJson = """{"id":"first","name":"First"}""",
                    ),
                    secondRow = SnapshotChunkRow(
                        table = "users",
                        keyJson = """{"id":"second"}""",
                        rowVersion = 9,
                        payloadJson = """{"id":"second","name":"Second"}""",
                    ),
                    unexpectedOrdinalMessage = "unexpected ordinal",
                )
            },
        ) { client ->
            db.execSQL("UPDATE _sync_apply_state SET apply_mode = 1 WHERE singleton_key = 1")
            db.execSQL("INSERT INTO users(id, name) VALUES('prior', 'Prior')")
            db.execSQL("UPDATE _sync_apply_state SET apply_mode = 0 WHERE singleton_key = 1")
            var appliedRows = 0
            client.setSnapshotApplyFaultInjectorForTest(
                SnapshotApplyFaultInjector(
                    afterAppliedRow = {
                        appliedRows++
                        if (appliedRows == 2) error("later-page-apply-failure")
                    },
                ),
            )

            assertIs<SnapshotRowApplyException>(client.rebuild().exceptionOrNull())
            assertEquals("Prior", scalarText(db, "SELECT name FROM users WHERE id = 'prior'"))
            assertEquals(0, scalarLong(db, "SELECT COUNT(*) FROM users WHERE id IN ('first', 'second')"))
            val diagnostics = client.snapshotRestoreDiagnosticsForTest()
            assertEquals(1, diagnostics.maxLiveApplyPageRows)
            assertTrue(diagnostics.maxLiveApplyPageStagedTextBytes > 0)
            assertEquals(2, diagnostics.finalStagedRows)
            assertEquals(0, diagnostics.finalAppliedRows)
        }
    }

    @Test
    fun secondLoadedPageUpdatesDiagnosticsBeforeItsFirstRowRuns() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        withConnectedClient(
            db = db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            snapshotApplyBatchRows = 1,
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            configureServer = {
                twoChunkSnapshotRoutes(
                    snapshotId = "second-page-diagnostics",
                    snapshotBundleSeq = 9,
                    firstRow = SnapshotChunkRow(
                        table = "users",
                        keyJson = """{"id":"first"}""",
                        rowVersion = 9,
                        payloadJson = """{"id":"first","name":"First"}""",
                    ),
                    secondRow = SnapshotChunkRow(
                        table = "users",
                        keyJson = """{"id":"second"}""",
                        rowVersion = 9,
                        payloadJson = """{"id":"second","name":"Second"}""",
                    ),
                    unexpectedOrdinalMessage = "unexpected ordinal",
                )
            },
        ) { client ->
            db.execSQL("UPDATE _sync_apply_state SET apply_mode = 1 WHERE singleton_key = 1")
            db.execSQL("INSERT INTO users(id, name) VALUES('prior', 'Prior')")
            db.execSQL("UPDATE _sync_apply_state SET apply_mode = 0 WHERE singleton_key = 1")
            var loadedPages = 0
            client.setSnapshotApplyFaultInjectorForTest(
                SnapshotApplyFaultInjector(
                    afterApplyPageLoaded = {
                        loadedPages++
                        if (loadedPages == 2) error("second-page-loaded")
                    },
                ),
            )

            val error = client.rebuild().exceptionOrNull()

            assertEquals("second-page-loaded", error?.message)
            assertEquals("Prior", scalarText(db, "SELECT name FROM users WHERE id = 'prior'"))
            assertEquals(0, scalarLong(db, "SELECT COUNT(*) FROM users WHERE id IN ('first', 'second')"))
            val diagnostics = client.snapshotRestoreDiagnosticsForTest()
            assertEquals(2L, diagnostics.applyPages)
            assertEquals(1, diagnostics.maxLiveApplyPageRows)
            assertTrue(diagnostics.maxLiveApplyPageStagedTextBytes > 0)
            assertEquals(2L, diagnostics.finalStagedRows)
            assertEquals(0L, diagnostics.finalAppliedRows)
        }
    }

    @Test
    fun deferredConstraintFailureAtCommitRollsBackAuthoritativeReplacement() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersAndPostsTables(db)
        withConnectedClient(
            db = db,
            syncTables = listOf(
                SyncTable("users", syncKeyColumnName = "id"),
                SyncTable("posts", syncKeyColumnName = "id"),
            ),
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            configureServer = {
                snapshotRoutes(
                    snapshotId = "deferred-constraint",
                    snapshotBundleSeq = 10,
                    rows = listOf(
                        SnapshotChunkRow(
                            table = "posts",
                            keyJson = """{"id":"orphan"}""",
                            rowVersion = 10,
                            payloadJson = """{"id":"orphan","user_id":"missing","title":"Orphan"}""",
                        ),
                    ),
                )
            },
        ) { client ->
            db.execSQL("UPDATE _sync_apply_state SET apply_mode = 1 WHERE singleton_key = 1")
            db.execSQL("INSERT INTO users(id, name) VALUES('prior', 'Prior')")
            db.execSQL("UPDATE _sync_apply_state SET apply_mode = 0 WHERE singleton_key = 1")

            assertTrue(client.rebuild().isFailure)
            assertEquals("Prior", scalarText(db, "SELECT name FROM users WHERE id = 'prior'"))
            assertEquals(0, scalarLong(db, "SELECT COUNT(*) FROM posts"))
        }
    }

    @Test
    fun actualSqliteFullAfterDestructiveWorkRollsBackReplacement() = runBlocking {
        val directory = Files.createTempDirectory("oversqlite-full-rollback-")
        val databasePath = directory.resolve("client.sqlite")
        val db = BundledSqliteConnectionProvider.openConnection(databasePath.toString(), debug = false)
        createUsersTable(db)
        db.execSQL("CREATE TABLE fill_probe (payload BLOB NOT NULL)")
        withConnectedClient(
            db = db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            configureServer = {
                usersSnapshotRoutes(
                    snapshotId = "sqlite-full",
                    snapshotBundleSeq = 11,
                    userId = "remote",
                    rowVersion = 11,
                    payloadJson = """{"id":"remote","name":"Remote"}""",
                )
            },
        ) { client ->
            db.execSQL("UPDATE _sync_apply_state SET apply_mode = 1 WHERE singleton_key = 1")
            db.execSQL("INSERT INTO users(id, name) VALUES('prior', 'Prior')")
            db.execSQL("UPDATE _sync_apply_state SET apply_mode = 0 WHERE singleton_key = 1")
            client.setSnapshotApplyFaultInjectorForTest(
                SnapshotApplyFaultInjector(
                    beforeCommit = {
                        val pages = scalarLong(db, "PRAGMA page_count")
                        db.execSQL("PRAGMA max_page_count = $pages")
                        db.execSQL("INSERT INTO fill_probe(payload) VALUES(zeroblob(1048576))")
                    },
                ),
            )

            val returnedError = requireNotNull(client.rebuild().exceptionOrNull())
            val failureChain = generateSequence(returnedError) { it.cause }.toList()
            val error = failureChain.last()
            assertTrue(
                error.message.orEmpty().contains("full", ignoreCase = true),
                returnedError.stackTraceToString(),
            )
            assertTrue(
                failureChain.any { it.suppressed.isNotEmpty() },
                returnedError.stackTraceToString(),
            )
            assertTrue(runCatching { db.execSQL("SELECT 1") }.isFailure)
        }
        val reopened = BundledSqliteConnectionProvider.openConnection(databasePath.toString(), debug = false)
        try {
            assertEquals("Prior", scalarText(reopened, "SELECT name FROM users WHERE id = 'prior'"))
            assertEquals(0, scalarLong(reopened, "SELECT COUNT(*) FROM users WHERE id = 'remote'"))
        } finally {
            reopened.close()
        }
    }

    @Test
    fun writerArrivingAfterImmediateOwnershipRunsOnlyAfterSnapshotCommit() = runBlocking {
        val directory = Files.createTempDirectory("oversqlite-late-writer-")
        val databasePath = directory.resolve("client.sqlite")
        val db = BundledSqliteConnectionProvider.openConnection(databasePath.toString(), debug = false)
        db.execSQL("PRAGMA busy_timeout = 5000")
        createUsersTable(db)
        withConnectedClient(
            db = db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            configureServer = {
                usersSnapshotRoutes(
                    snapshotId = "late-writer",
                    snapshotBundleSeq = 12,
                    userId = "remote",
                    rowVersion = 12,
                    payloadJson = """{"id":"remote","name":"Remote"}""",
                )
            },
        ) { client ->
            val writerDb = BundledSqliteConnectionProvider.openConnection(databasePath.toString(), debug = false)
            writerDb.execSQL("PRAGMA busy_timeout = 5000")
            val attempted = CountDownLatch(1)
            var writerFailure: Throwable? = null
            var writer: Thread? = null
            try {
                client.setSnapshotApplyFaultInjectorForTest(
                    SnapshotApplyFaultInjector(
                        beforeCommit = {
                            writer = thread(name = "snapshot-late-writer") {
                                attempted.countDown()
                                try {
                                    runBlocking {
                                        writerDb.execSQL("INSERT INTO users(id, name) VALUES('late-local', 'Late Local')")
                                    }
                                } catch (error: Throwable) {
                                    writerFailure = error
                                }
                            }
                            assertTrue(attempted.await(2, TimeUnit.SECONDS))
                            delay(100)
                            assertTrue(writer.isAlive)
                        },
                    ),
                )

                client.rebuild().getOrThrow()
                writer?.join(5_000)
                assertTrue(writer?.isAlive == false)
                assertEquals(null, writerFailure)
                assertEquals("Remote", scalarText(db, "SELECT name FROM users WHERE id = 'remote'"))
                assertEquals("Late Local", scalarText(db, "SELECT name FROM users WHERE id = 'late-local'"))
                assertEquals(1, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows WHERE table_name = 'users'"))
            } finally {
                writerDb.close()
            }
        }
    }

    @Test
    fun maximumAcceptedApplyRowUsesTheExactConfiguredByteBudget() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val limit = 4L * 1024L * 1024L
        val key = """{"id":"maximum"}"""
        val payload = payloadForRetainedBytes(key, "maximum", limit)
        withConnectedClient(
            db = db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            configureServer = {
                snapshotRoutes(
                    snapshotId = "maximum-row",
                    snapshotBundleSeq = 13,
                    rows = listOf(SnapshotChunkRow("users", key, 13, payload)),
                    byteCount = limit,
                )
            },
        ) { client ->
            client.rebuild().getOrThrow()
            assertEquals(1, scalarLong(db, "SELECT COUNT(*) FROM users WHERE id = 'maximum'"))
            assertEquals(limit, client.snapshotRestoreDiagnosticsForTest().maxLiveApplyPageStagedTextBytes)
        }
    }

    @Test
    fun oneByteOverApplyBudgetFailsBeforeManagedClear() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val limit = 4L * 1024L * 1024L
        val key = """{"id":"over"}"""
        val payload = payloadForRetainedBytes(key, "over", limit + 1L)
        withConnectedClient(
            db = db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            configureServer = {
                snapshotRoutes(
                    snapshotId = "over-row",
                    snapshotBundleSeq = 14,
                    rows = listOf(SnapshotChunkRow("users", key, 14, payload)),
                    byteCount = limit,
                )
            },
        ) { client ->
            db.execSQL("UPDATE _sync_apply_state SET apply_mode = 1 WHERE singleton_key = 1")
            db.execSQL("INSERT INTO users(id, name) VALUES('prior', 'Prior')")
            db.execSQL("UPDATE _sync_apply_state SET apply_mode = 0 WHERE singleton_key = 1")

            val error = assertIs<SnapshotApplyRowTooLargeException>(client.rebuild().exceptionOrNull())
            assertEquals(limit + 1L, error.retainedTextBytes)
            assertEquals("Prior", scalarText(db, "SELECT name FROM users WHERE id = 'prior'"))
            assertEquals(0, scalarLong(db, "SELECT COUNT(*) FROM users WHERE id = 'over'"))
        }
    }

    private fun payloadForRetainedBytes(
        keyJson: String,
        id: String,
        retainedBytes: Long,
    ): String {
        val prefix = """{"id":"$id","name":""""
        val suffix = "\"}"
        val fixed = "main".encodeToByteArray().size.toLong() +
            "users".encodeToByteArray().size.toLong() +
            keyJson.encodeToByteArray().size.toLong() +
            prefix.encodeToByteArray().size.toLong() +
            suffix.encodeToByteArray().size.toLong()
        val padding = retainedBytes - fixed
        require(padding in 0..Int.MAX_VALUE.toLong())
        return prefix + "x".repeat(padding.toInt()) + suffix
    }

    private suspend fun insertOutboxProbeRow(db: dev.goquick.sqlitenow.core.SafeSQLiteConnection) {
        db.execSQL(
            """
            INSERT INTO _sync_outbox_rows(
              source_bundle_id, row_ordinal, schema_name, table_name, key_json,
              wire_key_json, op, base_row_version, local_payload, wire_payload
            ) VALUES(
              1, 1, 'main', 'users', '{"id":"probe"}', '{"id":"probe"}',
              'INSERT', 0, '{"id":"probe","name":"Probe"}', '{"id":"probe","name":"Probe"}'
            )
            """.trimIndent(),
        )
    }

    private fun newGate(db: dev.goquick.sqlitenow.core.SafeSQLiteConnection) = OversqliteSnapshotGate(
        db = db,
        syncStateStore = OversqliteSyncStateStore(db),
        outboxStateStore = OversqliteOutboxStateStore(db),
        attachmentStateStore = OversqliteAttachmentStateStore(db),
        operationStateStore = OversqliteOperationStateStore(db),
    )

    private fun Throwable.originalRecoveredFailure(): Throwable =
        generateSequence(this) { it.cause }.last()

}

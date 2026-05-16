package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class BundleSnapshotContractTest : BundleClientContractTestSupport() {
    private suspend fun <T> withUserSnapshotRecoveryClient(
        snapshotId: String,
        snapshotBundleSeq: Long,
        userId: String,
        userName: String,
        rowVersion: Long,
        block: suspend (SafeSQLiteConnection, DefaultOversqliteClient) -> T,
    ): T {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        return withConnectedClient(
            db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            configureServer = {
                usersSnapshotRoutes(
                    snapshotId = snapshotId,
                    snapshotBundleSeq = snapshotBundleSeq,
                    userId = userId,
                    rowVersion = rowVersion,
                    payloadJson = """{"id":"$userId","name":"$userName"}""",
                )
            },
        ) { client ->
            block(db, client)
        }
    }

    private suspend fun assertSourceRotationRecovered(
        db: SafeSQLiteConnection,
        client: DefaultOversqliteClient,
        sourceBefore: String,
        expectedLastBundleSeq: Long,
        assertOldSourceReplacement: Boolean = false,
    ) {
        val newSourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
        assertNotEquals(sourceBefore, newSourceId)
        assertEquals(newSourceId, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
        if (assertOldSourceReplacement) {
            assertEquals(newSourceId, scalarText(db, "SELECT replaced_by_source_id FROM _sync_source_state WHERE source_id = '$sourceBefore'"))
        }
        assertEquals(1L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$newSourceId'"))
        assertEquals(expectedLastBundleSeq, client.syncStatus().getOrThrow().lastBundleSeqSeen)
    }

    @Test
    fun hydrate_rejectsSnapshotSessionWithMalformedExpiresAt() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        withJsonRouteConnectedClient(
            db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            routePath = "/sync/snapshot-sessions",
            responseBody = """
            {
              "snapshot_id": "snapshot-invalid-expiry",
              "snapshot_bundle_seq": 9,
              "row_count": 0,
              "byte_count": 32,
              "expires_at": "not-a-timestamp"
            }
            """.trimIndent(),
        ) { client ->
            val error = client.rebuild().exceptionOrNull()
            assertTrue(error != null)
            assertTrue(error.message?.contains("oversqlite timestamp must be RFC3339/ISO-8601 instant") == true)
        }
    }

    @Test
    fun hydrate_reportsRestoreSummary_andProgressPhases() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer().apply {
            createContext("/sync/snapshot-sessions") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "snapshot_id": "snapshot-report",
                      "snapshot_bundle_seq": 5,
                      "row_count": 1,
                      "byte_count": 32,
                      "expires_at": "2026-03-22T00:00:00Z"
                    }
                    """.trimIndent(),
                )
            }
            createContext("/sync/snapshot-sessions/snapshot-report") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "snapshot_id": "snapshot-report",
                      "snapshot_bundle_seq": 5,
                      "rows": [
                        {
                          "schema": "main",
                          "table": "users",
                          "key": {"id":"user-1"},
                          "row_version": 1,
                          "payload": {"id":"user-1","name":"Ada"}
                        }
                      ],
                      "next_row_ordinal": 1,
                      "has_more": false
                    }
                    """.trimIndent(),
                )
            }
        }
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            val observedProgress = mutableListOf<OversqliteProgress>()
            val collectJob = launch(start = CoroutineStart.UNDISPATCHED) {
                client.progress.collect { observedProgress += it }
            }

            val report = client.rebuild().getOrThrow()

            collectJob.cancel()
            assertEquals(RemoteSyncOutcome.APPLIED_SNAPSHOT, report.outcome)
            assertEquals(RestoreSummary(bundleSeq = 5, rowCount = 1), report.restore)
            assertEquals(AuthorityStatus.AUTHORITATIVE_MATERIALIZED, report.status.authority)
            assertTrue(observedProgress.contains(OversqliteProgress.Active(OversqliteOperation.REBUILD_KEEP_SOURCE, OversqlitePhase.STAGING_REMOTE_STATE)))
            assertTrue(observedProgress.contains(OversqliteProgress.Active(OversqliteOperation.REBUILD_KEEP_SOURCE, OversqlitePhase.APPLYING_REMOTE_STATE)))
            assertEquals(OversqliteProgress.Idle, client.progress.value)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun hydrate_oneChunkStagesBeforeFinalApply_andClearsStaleStageFirst() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        withConnectedClient(
            db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            configureServer = {
                usersSnapshotRoutes(
                    snapshotId = "snapshot-one",
                    snapshotBundleSeq = 9,
                    userId = "user-1",
                    rowVersion = 2,
                    payloadJson = """{"id":"user-1"}""",
                )
            },
        ) { client ->
            db.execSQL(
                """
                INSERT INTO _sync_snapshot_stage(snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload)
                VALUES ('stale-snapshot', 1, 'main', 'users', '{"id":"stale"}', 1, '{"id":"stale","name":"Old"}')
                """.trimIndent()
            )

            val error = client.rebuild().exceptionOrNull()
            assertTrue(error != null)
            assertTrue(error.message?.contains("payload for users must contain every table column") == true)
            assertEquals(0L, client.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
            assertEquals("snapshot-one", scalarText(db, "SELECT DISTINCT snapshot_id FROM _sync_snapshot_stage"))
        }
    }

    @Test
    fun hydrate_multiChunkStaysInvisibleUntilFinalApply() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val requestedOrdinals = mutableListOf<String>()
        withConnectedClient(
            db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            configureServer = {
                createContext("/sync/snapshot-sessions") { exchange ->
                    respondJson(
                        exchange,
                        200,
                        """
                        {
                          "snapshot_id": "snapshot-multi",
                          "snapshot_bundle_seq": 9,
                          "row_count": 2,
                          "byte_count": 64,
                          "expires_at": "2026-03-22T00:00:00Z"
                        }
                        """.trimIndent()
                    )
                }
                createContext("/sync/snapshot-sessions/snapshot-multi") { exchange ->
                    if (exchange.requestMethod == "GET") {
                        requestedOrdinals += queryParam(exchange, "after_row_ordinal")
                    }
                    when (queryParam(exchange, "after_row_ordinal")) {
                        "0" -> respondJson(
                            exchange,
                            200,
                            """
                            {
                              "snapshot_id": "snapshot-multi",
                              "snapshot_bundle_seq": 9,
                              "next_row_ordinal": 1,
                              "has_more": true,
                              "rows": [
                                {
                                  "schema": "main",
                                  "table": "users",
                                  "key": {"id":"user-1"},
                                  "row_version": 2,
                                  "payload": {"id":"user-1","name":"Ada"}
                                }
                              ]
                            }
                            """.trimIndent()
                        )
                        "1" -> respondJson(
                            exchange,
                            200,
                            """
                            {
                              "snapshot_id": "snapshot-multi",
                              "snapshot_bundle_seq": 9,
                              "next_row_ordinal": 2,
                              "has_more": false,
                              "rows": [
                                {
                                  "schema": "main",
                                  "table": "users",
                                  "key": {"id":"user-2"},
                                  "row_version": 2,
                                  "payload": {"id":"user-2"}
                                }
                              ]
                            }
                            """.trimIndent()
                        )
                        else -> error("unexpected after_row_ordinal")
                    }
                }
            },
        ) { client ->
            val error = client.rebuild().exceptionOrNull()
            assertTrue(error != null)
            assertTrue(error.message?.contains("payload for users must contain every table column") == true)
            assertEquals(listOf("0", "1"), requestedOrdinals)
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users"))
            assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
            assertEquals(0L, client.syncStatus().getOrThrow().lastBundleSeqSeen)
        }
    }

    @Test
    fun hydrate_blobPrimaryKeySnapshotConvertsWireUuidOnlyOnce() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        withBlobDocsConnectedClient(
            db,
            configureServer = {
                jsonRoute(
                    "/sync/snapshot-sessions",
                    body = """
                    {
                      "snapshot_id": "snapshot-blob-docs",
                      "snapshot_bundle_seq": 7,
                      "row_count": 1,
                      "byte_count": 96,
                      "expires_at": "2026-03-22T00:00:00Z"
                    }
                    """.trimIndent(),
                )
                jsonRoute(
                    "/sync/snapshot-sessions/snapshot-blob-docs",
                    body = """
                    {
                      "snapshot_id": "snapshot-blob-docs",
                      "snapshot_bundle_seq": 7,
                      "next_row_ordinal": 1,
                      "has_more": false,
                      "rows": [
                        {
                          "schema": "main",
                          "table": "blob_docs",
                          "key": {"id":"601c32ed-8299-c541-c389-749094a88cf2"},
                          "row_version": 7,
                          "payload": {
                            "id":"601c32ed-8299-c541-c389-749094a88cf2",
                            "name":"Doc one",
                            "payload":"AQID"
                          }
                        }
                      ]
                    }
                    """.trimIndent(),
                )
            },
        ) { client ->
            client.rebuild().getOrThrow()

            assertEquals(7L, client.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM blob_docs"))
            assertEquals("601c32ed8299c541c389749094a88cf2", scalarText(db, "SELECT lower(hex(id)) FROM blob_docs"))
            assertEquals("010203", scalarText(db, "SELECT hex(payload) FROM blob_docs"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
        }
    }

    @Test
    fun hydrate_restartClearsStageAndRestartsFromZero() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val requestedOrdinals = mutableListOf<String>()
        var sessionAttempts = 0
        withConnectedClient(
            db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            configureServer = {
                createContext("/sync/snapshot-sessions") { exchange ->
                    sessionAttempts++
                    if (sessionAttempts == 1) {
                        respondJson(
                            exchange,
                            200,
                            """
                            {
                              "snapshot_id": "snapshot-restart-a",
                              "snapshot_bundle_seq": 9,
                              "row_count": 2,
                              "byte_count": 64,
                              "expires_at": "2026-03-22T00:00:00Z"
                            }
                            """.trimIndent()
                        )
                    } else {
                        respondJson(
                            exchange,
                            200,
                            """
                            {
                              "snapshot_id": "snapshot-restart-b",
                              "snapshot_bundle_seq": 12,
                              "row_count": 1,
                              "byte_count": 32,
                              "expires_at": "2026-03-22T00:00:00Z"
                            }
                            """.trimIndent()
                        )
                    }
                }
                createContext("/sync/snapshot-sessions/snapshot-restart-a") { exchange ->
                    if (exchange.requestMethod == "GET") {
                        requestedOrdinals += "a:${queryParam(exchange, "after_row_ordinal")}"
                    }
                    when (queryParam(exchange, "after_row_ordinal")) {
                        "0" -> respondJson(
                            exchange,
                            200,
                            """
                            {
                              "snapshot_id": "snapshot-restart-a",
                              "snapshot_bundle_seq": 9,
                              "next_row_ordinal": 1,
                              "has_more": true,
                              "rows": [
                                {
                                  "schema": "main",
                                  "table": "users",
                                  "key": {"id":"user-1"},
                                  "row_version": 2,
                                  "payload": {"id":"user-1","name":"Ada"}
                                }
                              ]
                            }
                            """.trimIndent()
                        )
                        "1" -> {
                            exchange.sendResponseHeaders(500, 0)
                            exchange.responseBody.close()
                        }
                        else -> error("unexpected restart-a after_row_ordinal")
                    }
                }
                createContext("/sync/snapshot-sessions/snapshot-restart-b") { exchange ->
                    if (exchange.requestMethod == "GET") {
                        requestedOrdinals += "b:${queryParam(exchange, "after_row_ordinal")}"
                    }
                    respondJson(
                        exchange,
                        200,
                        """
                        {
                          "snapshot_id": "snapshot-restart-b",
                          "snapshot_bundle_seq": 12,
                          "next_row_ordinal": 1,
                          "has_more": false,
                          "rows": [
                            {
                              "schema": "main",
                              "table": "users",
                              "key": {"id":"user-2"},
                              "row_version": 3,
                              "payload": {"id":"user-2","name":"Grace"}
                            }
                          ]
                        }
                        """.trimIndent()
                    )
                }
            },
        ) { client ->
            val firstError = client.rebuild().exceptionOrNull()
            assertTrue(firstError != null)
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users"))

            client.rebuild().getOrThrow()

            assertEquals(listOf("a:0", "a:1", "b:0"), requestedOrdinals)
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
            assertEquals("Grace", scalarText(db, "SELECT name FROM users WHERE id = 'user-2'"))
            assertEquals(12L, client.syncStatus().getOrThrow().lastBundleSeqSeen)
        }
    }

    @Test
    fun hydrate_selfReferentialRowsSurviveChunkBoundaries() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createEmployeesTable(db)
        withConnectedClient(
            db,
            syncTables = listOf(SyncTable("employees", syncKeyColumnName = "id")),
            configureServer = {
                twoChunkSnapshotRoutes(
                    snapshotId = "snapshot-employees",
                    snapshotBundleSeq = 5,
                    firstRow = SnapshotChunkRow(
                        table = "employees",
                        keyJson = """{"id":"employee-2"}""",
                        rowVersion = 2,
                        payloadJson = """{"id":"employee-2","manager_id":"employee-1","name":"Bob"}""",
                    ),
                    secondRow = SnapshotChunkRow(
                        table = "employees",
                        keyJson = """{"id":"employee-1"}""",
                        rowVersion = 2,
                        payloadJson = """{"id":"employee-1","manager_id":null,"name":"Alice"}""",
                    ),
                    unexpectedOrdinalMessage = "unexpected after_row_ordinal for employees snapshot",
                )
            },
        ) { client ->
            client.rebuild().getOrThrow()

            assertEquals(5L, client.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM employees"))
            assertEquals("employee-1", scalarText(db, "SELECT manager_id FROM employees WHERE id = 'employee-2'"))
            assertEquals("Alice", scalarText(db, "SELECT name FROM employees WHERE id = 'employee-1'"))
        }
    }

    @Test
    fun hydrate_cyclicFkGraphsSurviveChunkBoundaries() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createAuthorsAndProfilesCycleTables(db)
        withConnectedClient(
            db,
            syncTables = listOf(
                SyncTable("authors", syncKeyColumnName = "id"),
                SyncTable("profiles", syncKeyColumnName = "id"),
            ),
            configureServer = {
                twoChunkSnapshotRoutes(
                    snapshotId = "snapshot-cycle",
                    snapshotBundleSeq = 6,
                    firstRow = SnapshotChunkRow(
                        table = "authors",
                        keyJson = """{"id":"author-1"}""",
                        rowVersion = 2,
                        payloadJson = """{"id":"author-1","profile_id":"profile-1","name":"Author"}""",
                    ),
                    secondRow = SnapshotChunkRow(
                        table = "profiles",
                        keyJson = """{"id":"profile-1"}""",
                        rowVersion = 2,
                        payloadJson = """{"id":"profile-1","author_id":"author-1","bio":"Cyclic"}""",
                    ),
                    unexpectedOrdinalMessage = "unexpected after_row_ordinal for cycle snapshot",
                )
            },
        ) { client ->
            client.rebuild().getOrThrow()

            assertEquals(6L, client.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals("profile-1", scalarText(db, "SELECT profile_id FROM authors WHERE id = 'author-1'"))
            assertEquals("author-1", scalarText(db, "SELECT author_id FROM profiles WHERE id = 'profile-1'"))
            assertEquals("Cyclic", scalarText(db, "SELECT bio FROM profiles WHERE id = 'profile-1'"))
        }
    }

    @Test
    fun recover_failedFinalApplyLeavesSourceIdUnchanged() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        withConnectedClient(
            db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            configureServer = {
                usersSnapshotRoutes(
                    snapshotId = "snapshot-recover-fail",
                    snapshotBundleSeq = 9,
                    userId = "user-1",
                    rowVersion = 2,
                    payloadJson = """{"id":"user-1"}""",
                )
            },
        ) { client ->
            val originalSourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")

            markSourceRecoveryRequired(db)
            val error = client.rebuild().exceptionOrNull()
            assertTrue(error != null)
            assertTrue(error.message?.contains("payload for users must contain every table column") == true)
            assertEquals(originalSourceId, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
            assertEquals(0L, client.syncStatus().getOrThrow().lastBundleSeqSeen)
        }
    }

    @Test
    fun recover_rotatesSourceId_andResetsBundleState() = runBlocking<Unit> {
        withUserSnapshotRecoveryClient(
            snapshotId = "snapshot-recover",
            snapshotBundleSeq = 9,
            userId = "user-1",
            userName = "Ada",
            rowVersion = 2,
        ) { db, client ->
            val sourceBefore = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            db.execSQL("UPDATE _sync_source_state SET next_source_bundle_id = 5 WHERE source_id = '$sourceBefore'")
            db.execSQL("UPDATE _sync_attachment_state SET last_bundle_seq_seen = 4 WHERE singleton_key = 1")

            markSourceRecoveryRequired(db)
            client.rebuild().getOrThrow()

            assertSourceRotationRecovered(
                db = db,
                client = client,
                sourceBefore = sourceBefore,
                expectedLastBundleSeq = 9,
                assertOldSourceReplacement = true,
            )
        }
    }

    @Test
    fun recover_sourceRotationClearsManagedTables_andRemainingLocalSyncState() = runBlocking<Unit> {
        withUserSnapshotRecoveryClient(
            snapshotId = "snapshot-recover-reset",
            snapshotBundleSeq = 11,
            userId = "user-2",
            userName = "Grace",
            rowVersion = 11,
        ) { db, client ->
            val sourceBefore = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")

            db.execSQL("UPDATE _sync_apply_state SET apply_mode = 1 WHERE singleton_key = 1")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada stale')")
            db.execSQL("UPDATE _sync_apply_state SET apply_mode = 0 WHERE singleton_key = 1")
            db.execSQL(
                """
                INSERT INTO _sync_row_state(schema_name, table_name, key_json, row_version, deleted)
                VALUES ('main', 'users', '{"id":"user-1"}', 4, 0)
                """.trimIndent()
            )
            db.execSQL(
                """
                INSERT INTO _sync_snapshot_stage(snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload)
                VALUES ('stale-snapshot', 0, 'main', 'users', '{"id":"user-9"}', 9, '{"id":"user-9","name":"Old snapshot"}')
                """.trimIndent()
            )
            db.execSQL("UPDATE _sync_source_state SET next_source_bundle_id = 5 WHERE source_id = '$sourceBefore'")
            db.execSQL(
                """
                UPDATE _sync_attachment_state
                SET last_bundle_seq_seen = 4,
                    rebuild_required = 1
                WHERE singleton_key = 1
                """.trimIndent()
            )

            markSourceRecoveryRequired(db)
            client.rebuild().getOrThrow()

            assertSourceRotationRecovered(
                db = db,
                client = client,
                sourceBefore = sourceBefore,
                expectedLastBundleSeq = 11,
            )
            assertEquals(0L, scalarLong(db, "SELECT rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM users"))
            assertEquals("Grace", scalarText(db, "SELECT name FROM users WHERE id = 'user-2'"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users WHERE id = 'user-1'"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_row_state WHERE table_name = 'users'"))
            assertEquals(11L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE table_name = 'users' AND key_json = '{\"id\":\"user-2\"}'"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
        }
    }

    @Test
    fun hydrateAndRecover_clearRebuildRequired_andAllowNormalSyncAgain() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        withConnectedClient(
            db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            configureServer = {
                usersSnapshotRoutes(
                    snapshotId = "snapshot-rebuild",
                    snapshotBundleSeq = 9,
                    userId = "user-1",
                    rowVersion = 2,
                    payloadJson = """{"id":"user-1","name":"Ada"}""",
                )
                createContext("/sync/pull") { exchange ->
                    respondJson(
                        exchange,
                        200,
                        """
                        {
                          "stable_bundle_seq": 9,
                          "has_more": false,
                          "bundles": []
                        }
                        """.trimIndent()
                    )
                }
            },
        ) { client ->
            db.execSQL("UPDATE _sync_attachment_state SET rebuild_required = 1 WHERE singleton_key = 1")
            assertTrue(client.sync().exceptionOrNull() is RebuildRequiredException)

            client.rebuild().getOrThrow()
            assertEquals(0L, scalarLong(db, "SELECT rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1"))
            client.sync().getOrThrow()

            db.execSQL("UPDATE _sync_attachment_state SET rebuild_required = 1 WHERE singleton_key = 1")
            assertTrue(client.sync().exceptionOrNull() is RebuildRequiredException)

            markSourceRecoveryRequired(db)
            assertTrue(client.sync().exceptionOrNull() is SourceRecoveryRequiredException)
            client.rebuild().getOrThrow()
            assertEquals(0L, scalarLong(db, "SELECT rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1"))
            client.sync().getOrThrow()
        }
    }
}

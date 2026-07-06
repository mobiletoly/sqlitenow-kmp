package dev.goquick.sqlitenow.oversqlite

import com.sun.net.httpserver.HttpServer
import dev.goquick.sqlitenow.core.TransactionMode
import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.time.OffsetDateTime
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class BundlePushContractTest : BundleClientContractTestSupport() {
    private class TypedRowsCommittedReplaySuccessScenario(
        val displayName: String,
        val pushId: String,
        val enabledFlag: JsonPrimitive,
        val createdAt: JsonElement,
        val enabledFlagJson: String,
        val createdAtJson: String,
        val localCreatedAtSql: String,
        val assertReplayedState: suspend (SafeSQLiteConnection) -> Unit,
    )

    private data class NoUploadScenario(
        val displayName: String,
        val beforePushSql: List<String>,
    )

    private data class InsertUploadScenario(
        val displayName: String,
        val insertBeforeConnect: Boolean,
        val verifyMaterializedState: Boolean,
    )

    private data class SyncedMutationUploadScenario(
        val displayName: String,
        val mutationSql: String,
        val expectedOp: String,
        val expectedPayloadJson: String?,
    )

    private data class SameKeyRebaseScenario(
        val displayName: String,
        val seedSyncedUser: Boolean,
        val beforeInFlightPushSql: String,
        val duringInFlightPushSql: String,
        val expectedLocalName: String?,
        val expectedPayloadName: String?,
        val expectedOp: String,
        val expectedBaseRowVersion: Long,
        val expectedDeleted: Long,
    )

    private data class InFlightPushDirtyScenario(
        val displayName: String,
        val configurePushServer: FakeChunkedSyncServer.(CountDownLatch, CountDownLatch) -> Unit,
    )

    private data class StructuredConflictResolverScenario(
        val displayName: String,
        val resolver: Resolver,
    )

    private data class FinalReplayForeignKeyScenario(
        val displayName: String,
        val createTables: suspend (SafeSQLiteConnection) -> Unit,
        val syncTables: List<SyncTable>,
        val insertRows: suspend (SafeSQLiteConnection) -> Unit,
        val assertReplayedState: suspend (SafeSQLiteConnection) -> Unit,
    )

    private enum class CommittedReplayRetryMode {
        SAME_CLIENT,
        RESTARTED_CLIENT,
    }

    private fun keepMergedUserResolver(): Resolver = Resolver {
        MergeResult.KeepMerged(
            buildJsonObject {
                put("id", JsonPrimitive("user-1"))
                put("name", JsonPrimitive("Merged"))
            }
        )
    }

    private suspend fun <T> withUsersPushServerClient(
        uploadLimit: Int = 200,
        transientRetryPolicy: OversqliteTransientRetryPolicy = OversqliteTransientRetryPolicy(),
        resolver: Resolver = ServerWinsResolver,
        userId: String = "user-1",
        beforeConnect: suspend (SafeSQLiteConnection) -> Unit = {},
        configurePushServer: FakeChunkedSyncServer.() -> Unit = {},
        block: suspend (SafeSQLiteConnection, DefaultOversqliteClient, FakeChunkedSyncServer) -> T,
    ): T {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        beforeConnect(db)
        return withPushServerClient(
            db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            uploadLimit = uploadLimit,
            transientRetryPolicy = transientRetryPolicy,
            resolver = resolver,
            userId = userId,
            configurePushServer = configurePushServer,
        ) { client, pushServer ->
            block(db, client, pushServer)
        }
    }

    private suspend fun <T> withStructuredConflictRetryPushClient(
        createSchema: suspend (SafeSQLiteConnection) -> Unit,
        syncTable: SyncTable,
        conflictResponseBody: String,
        resolver: Resolver,
        block: suspend (SafeSQLiteConnection, DefaultOversqliteClient, FakeChunkedSyncServer) -> T,
    ): T {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createSchema(db)
        var commitAttempt = 0
        return withPushServerClient(
            db,
            syncTables = listOf(syncTable),
            resolver = resolver,
            configurePushServer = {
                commitError = { _, _ ->
                    commitAttempt++
                    if (commitAttempt == 1) {
                        409 to conflictResponseBody
                    } else {
                        null
                    }
                }
            },
        ) { client, pushServer ->
            block(db, client, pushServer)
        }
    }

    private suspend fun <T> withUserInsertStructuredConflictRetryPushClient(
        resolver: Resolver,
        block: suspend (SafeSQLiteConnection, DefaultOversqliteClient, FakeChunkedSyncServer) -> T,
    ): T {
        return withStructuredConflictRetryPushClient(
            createSchema = ::createUsersTable,
            syncTable = SyncTable("users", syncKeyColumnName = "id"),
            conflictResponseBody = """
                {
                  "error":"push_conflict",
                  "message":"insert conflict on main.users user-1",
                  "conflict":{
                    "schema":"main",
                    "table":"users",
                    "key":{"id":"user-1"},
                    "op":"INSERT",
                    "base_row_version":0,
                    "server_row_version":7,
                    "server_row_deleted":false,
                    "server_row":{"id":"user-1","name":"Server"}
                  }
                }
            """.trimIndent(),
            resolver = resolver,
            block = block,
        )
    }

    private suspend fun assertStructuredConflictRestoresDirtyRows(
        conflictResponseBody: String,
        resolver: Resolver,
        beforeConflictPush: suspend (SafeSQLiteConnection) -> Unit,
        assertLocalUserState: suspend (SafeSQLiteConnection) -> Unit,
        expectedDirtyOp: String,
        expectedDirtyBaseRowVersion: Long = 1L,
        assertDirtyPayload: Boolean = false,
        expectedDirtyPayload: String? = null,
    ) {
        var conflictEnabled = false
        withUsersPushServerClient(
            resolver = resolver,
            configurePushServer = {
                commitError = { _, _ ->
                    if (conflictEnabled) {
                        409 to conflictResponseBody
                    } else {
                        null
                    }
                }
            },
        ) { db, client, _ ->
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Seed')")
            client.pushPending().getOrThrow()

            beforeConflictPush(db)
            conflictEnabled = true

            val error = client.pushPending().exceptionOrNull()
            assertNotNull(error)
            assertTrue(error is InvalidConflictResolutionException)
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertLocalUserState(db)
            val dirty = dirtyRow(db, "users", """{"id":"user-1"}""")
            assertEquals(expectedDirtyOp, dirty.op)
            assertEquals(expectedDirtyBaseRowVersion, dirty.baseRowVersion)
            if (assertDirtyPayload) {
                assertEquals(expectedDirtyPayload, dirty.payload)
            }
        }
    }

    private suspend fun assertPushPendingSourceRecoveryState(
        configurePushServer: FakeChunkedSyncServer.() -> Unit,
        expectedReason: SourceRecoveryReason,
        expectedStoredReason: String,
        expectedUploadedChunkCount: Int? = null,
        verifyRestartedSourceInfo: Boolean = false,
    ) {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply(configurePushServer)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            val sourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val error = client.pushPending().exceptionOrNull()

            assertNotNull(error)
            assertTrue(error is SourceRecoveryRequiredException)
            assertEquals(expectedReason, error.reason)
            assertEquals("source_recovery", scalarText(db, "SELECT kind FROM _sync_operation_state WHERE singleton_key = 1"))
            assertEquals(expectedStoredReason, scalarText(db, "SELECT reason FROM _sync_operation_state WHERE singleton_key = 1"))
            val reservedReplacementSourceId = scalarText(
                db,
                "SELECT replacement_source_id FROM _sync_operation_state WHERE singleton_key = 1",
            )
            assertTrue(reservedReplacementSourceId.isNotBlank())
            assertNotEquals(sourceId, reservedReplacementSourceId)
            assertEquals("prepared", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))

            if (expectedUploadedChunkCount != null) {
                assertEquals(expectedUploadedChunkCount, pushServer.uploadedChunks.size)
            }
            if (verifyRestartedSourceInfo) {
                client.close()
                val restartedClient = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
                restartedClient.open().getOrThrow()
                val info = restartedClient.sourceInfo().getOrThrow()
                assertTrue(info.rebuildRequired)
                assertTrue(info.sourceRecoveryRequired)
                assertEquals(expectedReason, info.sourceRecoveryReason)
                assertEquals("prepared", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
                assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            }
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    private suspend fun assertBadHashCommittedReplayDoesNotReupload(
        retryMode: CommittedReplayRetryMode,
        verifyFirstFailureMessage: Boolean = false,
        verifyRemoteBundleSeq: Boolean = false,
    ) {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        var chunkReads = 0
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            bundleChunkOverride = { bundle, afterRowOrdinal, maxRows ->
                chunkReads++
                val normal = buildCommittedBundleChunk(bundle, afterRowOrdinal, maxRows)
                if (chunkReads == 1) {
                    normal.copy(bundleHash = "bad-hash")
                } else {
                    normal
                }
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val firstClient = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            firstClient.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val firstError = firstClient.pushPending().exceptionOrNull()
            assertNotNull(firstError)
            if (verifyFirstFailureMessage) {
                assertTrue(firstError.message?.contains("bundle hash") == true || firstError.message?.contains("bundle_hash") == true)
            }
            val uploadedChunkCount = pushServer.uploadedChunks.size
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals("committed_remote", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            if (verifyRemoteBundleSeq) {
                assertEquals(1L, scalarLong(db, "SELECT remote_bundle_seq FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            }

            when (retryMode) {
                CommittedReplayRetryMode.SAME_CLIENT -> {
                    firstClient.pushPending().getOrThrow()
                }
                CommittedReplayRetryMode.RESTARTED_CLIENT -> {
                    val restartedClient = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
                    restartedClient.open().getOrThrow()
                    assertConnectedOutcome(
                        expectedOutcome = AttachOutcome.RESUMED_ATTACHED_STATE,
                        actual = restartedClient.attach("user-1").getOrThrow(),
                    )
                    restartedClient.pushPending().getOrThrow()
                }
            }

            assertEquals(uploadedChunkCount, pushServer.uploadedChunks.size)
            assertEquals(listOf(1L), pushServer.createRequests.map { it.sourceBundleId })
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals("none", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(
                2L,
                scalarLong(
                    db,
                    "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = (SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1)",
                ),
            )
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    private fun typedCommittedRows(
        enabledFlag: JsonPrimitive,
        createdAt: JsonElement,
    ): List<BundleRow> = listOf(
        BundleRow(
            schema = "main",
            table = "typed_rows",
            key = mapOf("id" to "typed-1"),
            op = "INSERT",
            rowVersion = 1,
            payload = buildJsonObject {
                put("id", JsonPrimitive("typed-1"))
                put("name", JsonPrimitive("Typed Row"))
                put("note", JsonNull)
                put("count_value", JsonNull)
                put("enabled_flag", enabledFlag)
                put("rating", JsonPrimitive(1.25))
                put("data", JsonNull)
                put("created_at", createdAt)
            },
        )
    )

    private fun typedCommittedRowJson(
        enabledFlag: String,
        createdAt: String,
    ): String = """
        {
          "schema": "main",
          "table": "typed_rows",
          "key": {"id":"typed-1"},
          "op": "INSERT",
          "row_version": 1,
          "payload": {
            "id":"typed-1",
            "name":"Typed Row",
            "note": null,
            "count_value": null,
            "enabled_flag": $enabledFlag,
            "rating": 1.25,
            "data": null,
            "created_at": $createdAt
          }
        }
    """.trimIndent()

    private fun userCommittedInsertRow(payload: JsonElement): BundleRow {
        return BundleRow(
            schema = "main",
            table = "users",
            key = mapOf("id" to "user-1"),
            op = "INSERT",
            rowVersion = 1,
            payload = payload,
        )
    }

    private fun userCommittedInsertHash(payloadJson: String): String {
        return computeCommittedBundleHash(
            listOf(userCommittedInsertRow(json.parseToJsonElement(payloadJson))),
        )
    }

    private fun userCommittedInsertRowJson(payloadJson: String): String = """
        {
          "schema": "main",
          "table": "users",
          "key": {"id":"user-1"},
          "op": "INSERT",
          "row_version": 1,
          "payload": $payloadJson
        }
    """.trimIndent()

    private fun HttpServer.installCommittedReplayRoutes(
        pushId: String,
        committedHash: String,
        committedRowsJson: String,
        alreadyCommitted: Boolean = false,
    ) {
        var sourceId = ""
        val indentedCommittedRowsJson = committedRowsJson.trimIndent().prependIndent("                        ")

        createContext("/sync/push-sessions") { exchange ->
            json.decodeFromString(
                PushSessionCreateRequest.serializer(),
                exchange.requestBody.readBytes().decodeToString(),
            )
            sourceId = exchange.requestHeaders.getFirst("Oversync-Source-ID").orEmpty()
            val responseBody = if (alreadyCommitted) {
                """
                {
                  "status": "already_committed",
                  "push_id": "",
                  "next_expected_row_ordinal": 0,
                  "bundle_seq": 1,
                  "source_id": "$sourceId",
                  "source_bundle_id": 1,
                  "row_count": 1,
                  "bundle_hash": "$committedHash"
                }
                """.trimIndent()
            } else {
                """
                {
                  "push_id": "$pushId",
                  "status": "staging",
                  "planned_row_count": 1,
                  "next_expected_row_ordinal": 0
                }
                """.trimIndent()
            }
            respondJson(exchange, 200, responseBody)
        }
        if (!alreadyCommitted) {
            createContext("/sync/push-sessions/$pushId/chunks") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "push_id": "$pushId",
                      "next_expected_row_ordinal": 1
                    }
                    """.trimIndent()
                )
            }
            createContext("/sync/push-sessions/$pushId/commit") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "bundle_seq": 1,
                      "source_id": "$sourceId",
                      "source_bundle_id": 1,
                      "row_count": 1,
                      "bundle_hash": "$committedHash"
                    }
                    """.trimIndent()
                )
            }
        }
        createContext("/sync/committed-bundles/1/rows") { exchange ->
            respondJson(
                exchange,
                200,
                """
                {
                  "bundle_seq": 1,
                  "source_id": "$sourceId",
                  "source_bundle_id": 1,
                  "row_count": 1,
                  "bundle_hash": "$committedHash",
                  "rows": [
$indentedCommittedRowsJson
                  ],
                  "next_row_ordinal": 0,
                  "has_more": false
                }
                """.trimIndent()
            )
        }
    }

    private suspend fun <T> withTypedRowsCommittedReplayClient(
        pushId: String,
        committedRows: List<BundleRow>,
        committedRowsJson: String,
        alreadyCommitted: Boolean = false,
        block: suspend (SafeSQLiteConnection, DefaultOversqliteClient) -> T,
    ): T {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createTypedRowsTable(db)
        val committedHash = computeCommittedBundleHash(committedRows)
        val server = newServer().apply {
            installCommittedReplayRoutes(
                pushId = pushId,
                committedHash = committedHash,
                committedRowsJson = committedRowsJson,
                alreadyCommitted = alreadyCommitted,
            )
        }
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("typed_rows", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            return block(db, client)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    private fun assertUploadedUserRow(
        uploaded: PushRequestRow,
        op: String,
        baseRowVersion: Long,
        payloadJson: String?,
        context: String? = null,
    ) {
        val messagePrefix = context?.let { "$it: " }.orEmpty()
        assertEquals(op, uploaded.op, "${messagePrefix}op")
        assertEquals(baseRowVersion, uploaded.baseRowVersion, "${messagePrefix}base row version")
        assertEquals(payloadJson, uploaded.payload?.toString(), "${messagePrefix}payload")
    }

    @Test
    fun pauseUploads_doesNotSkipExplicitPushPending() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        withPushServerClient(
            db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
        ) { client, pushServer ->
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            client.pauseUploads()

            val report = client.pushPending().getOrThrow()

            assertEquals(PushOutcome.COMMITTED, report.outcome)
            assertEquals(AuthorityStatus.AUTHORITATIVE_MATERIALIZED, report.status.authority)
            assertEquals(1L, client.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(1, pushServer.bundles.size)
        }
    }

    @Test
    fun pushPending_noUploadScenariosAreLocalNoOps() = runBlocking {
        val scenarios = listOf(
            NoUploadScenario(
                displayName = "pushPending_withNoDirtyRows_isLocalNoOpBeforeOutboundState",
                beforePushSql = emptyList(),
            ),
            NoUploadScenario(
                displayName = "pushPending_unsyncedInsertThenDelete_isDroppedAsNoOp",
                beforePushSql = listOf(
                    "INSERT INTO users(id, name) VALUES('user-1', 'Ada')",
                    "DELETE FROM users WHERE id = 'user-1'",
                ),
            ),
        )

        for (scenario in scenarios) {
            withUsersPushServerClient { db, client, pushServer ->
                for (sql in scenario.beforePushSql) {
                    db.execSQL(sql)
                }

                client.pushPending().getOrThrow()

                assertEquals(0, pushServer.createRequests.size, "${scenario.displayName}: create requests")
                assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"), "${scenario.displayName}: dirty rows")
                assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"), "${scenario.displayName}: outbox rows")
            }
        }
    }

    @Test
    fun pushPending_freezesDirtyRowsIntoOutboundAtomically() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()

            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-2', 'Grace')")
            db.execSQL(
                """
                CREATE TRIGGER fail_second_outbound_insert
                BEFORE INSERT ON _sync_outbox_rows
                WHEN NEW.row_ordinal = 1
                BEGIN
                  SELECT RAISE(ABORT, 'freeze abort');
                END
                """.trimIndent()
            )

            val error = client.pushPending().exceptionOrNull()
            assertNotNull(error)
            assertTrue(error.message?.contains("freeze abort") == true)
            assertEquals(listOf("user-1:INSERT", "user-2:INSERT"), dirtyKeysAndOps(db))
            assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_insertScenariosUploadAsInsert() = runBlocking {
        val insertUserSql = "INSERT INTO users(id, name) VALUES('user-1', 'Ada')"
        val scenarios = listOf(
            InsertUploadScenario(
                displayName = "pushPending_localInsert_uploadsDirtyPayloadAsInsert",
                insertBeforeConnect = false,
                verifyMaterializedState = false,
            ),
            InsertUploadScenario(
                displayName = "pushPending_preexistingRowsUploadAsInsert",
                insertBeforeConnect = true,
                verifyMaterializedState = true,
            ),
        )

        for (scenario in scenarios) {
            withUsersPushServerClient(
                beforeConnect = { db ->
                    if (scenario.insertBeforeConnect) {
                        db.execSQL(insertUserSql)
                    }
                },
            ) { db, client, pushServer ->
                if (!scenario.insertBeforeConnect) {
                    db.execSQL(insertUserSql)
                }

                client.pushPending().getOrThrow()

                val uploaded = pushServer.uploadedChunks.single().rows.single()
                assertUploadedUserRow(
                    uploaded,
                    op = "INSERT",
                    baseRowVersion = 0L,
                    payloadJson = """{"id":"user-1","name":"Ada"}""",
                    context = scenario.displayName,
                )
                if (scenario.verifyMaterializedState) {
                    assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"), "${scenario.displayName}: dirty rows")
                    assertEquals(1L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE schema_name = 'main' AND table_name = 'users' AND key_json = '{\"id\":\"user-1\"}'"), "${scenario.displayName}: row version")
                }
            }
        }
    }

    @Test
    fun pushPending_syncedMutationsUploadExpectedRows() = runBlocking {
        val scenarios = listOf(
            SyncedMutationUploadScenario(
                displayName = "pushPending_syncedUpdate_uploadsUpdateWithBaseRowVersion",
                mutationSql = "UPDATE users SET name = 'Ada v2' WHERE id = 'user-1'",
                expectedOp = "UPDATE",
                expectedPayloadJson = """{"id":"user-1","name":"Ada v2"}""",
            ),
            SyncedMutationUploadScenario(
                displayName = "pushPending_syncedDelete_uploadsDeleteWithNullPayload",
                mutationSql = "DELETE FROM users WHERE id = 'user-1'",
                expectedOp = "DELETE",
                expectedPayloadJson = null,
            ),
        )

        for (scenario in scenarios) {
            withUsersPushServerClient { db, client, pushServer ->
                db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
                client.pushPending().getOrThrow()

                db.execSQL(scenario.mutationSql)
                client.pushPending().getOrThrow()

                val uploaded = pushServer.uploadedChunks.last().rows.single()
                assertUploadedUserRow(
                    uploaded,
                    op = scenario.expectedOp,
                    baseRowVersion = 1L,
                    payloadJson = scenario.expectedPayloadJson,
                    context = scenario.displayName,
                )
            }
        }
    }

    @Test
    fun pushPending_preexistingRowsUseNormalInsertConflictPath() = runBlocking {
        withUsersPushServerClient(
            resolver = ServerWinsResolver,
            configurePushServer = {
                commitError = { _, _ ->
                    409 to """
                        {
                          "error":"push_conflict",
                          "message":"insert conflict on main.users user-1",
                          "conflict":{
                            "schema":"main",
                            "table":"users",
                            "key":{"id":"user-1"},
                            "op":"INSERT",
                            "base_row_version":0,
                            "server_row_version":7,
                            "server_row_deleted":false,
                            "server_row":{"id":"user-1","name":"Server"}
                          }
                        }
                    """.trimIndent()
                }
            },
        ) { db, client, pushServer ->
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Client')")
            client.pushPending().getOrThrow()

            val uploaded = pushServer.uploadedChunks.single().rows.single()
            assertEquals("INSERT", uploaded.op)
            assertEquals(0L, uploaded.baseRowVersion)
            assertEquals("Server", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(7L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE schema_name = 'main' AND table_name = 'users' AND key_json = '{\"id\":\"user-1\"}'"))
        }
    }

    @Test
    fun pushPending_usesPushSessions_chunksTransport_andReplaysCommittedBundle() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersAndPostsTables(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = listOf(
                    SyncTable("users", syncKeyColumnName = "id"),
                    SyncTable("posts", syncKeyColumnName = "id"),
                ),
                uploadLimit = 1,
                transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            )
            client.openAndConnect("user-1").getOrThrow()

            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            db.execSQL("INSERT INTO posts(id, user_id, title) VALUES('post-1', 'user-1', 'Notes')")

            client.pushPending().getOrThrow()

            assertEquals(1, pushServer.createRequests.size)
            assertEquals(2L, pushServer.createRequests.single().plannedRowCount)
            assertEquals(listOf(0L, 1L), pushServer.uploadedChunks.map { it.startRowOrdinal })
            assertEquals(listOf("users:INSERT", "posts:INSERT"), pushServer.uploadedChunks.map { it.rows.single() }.map { "${it.table}:${it.op}" })
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(1L, client.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(
                2L,
                scalarLong(
                    db,
                    "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = (SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1)",
                ),
            )
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_newLocalWritesDuringInFlightPushRemainDirty() = runBlocking {
        val scenarios = listOf(
            InFlightPushDirtyScenario(
                displayName = "during upload",
                configurePushServer = { started, release ->
                    beforeCreateSessionResponse = {
                        started.countDown()
                        check(release.await(5, TimeUnit.SECONDS))
                    }
                },
            ),
            InFlightPushDirtyScenario(
                displayName = "during replay",
                configurePushServer = { started, release ->
                    beforeCommittedBundleChunkResponse = { _, _ ->
                        started.countDown()
                        check(release.await(5, TimeUnit.SECONDS))
                    }
                },
            ),
        )

        scenarios.forEach { scenario ->
            val pushBlocked = CountDownLatch(1)
            val releasePush = CountDownLatch(1)
            withUsersPushServerClient(
                configurePushServer = {
                    scenario.configurePushServer(this, pushBlocked, releasePush)
                },
            ) { db, client, _ ->
                db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

                val inFlightPush = async(Dispatchers.Default) { client.pushPending() }
                assertTrue(pushBlocked.await(5, TimeUnit.SECONDS), scenario.displayName)

                db.execSQL("INSERT INTO users(id, name) VALUES('user-2', 'Grace')")
                releasePush.countDown()
                inFlightPush.await().getOrThrow()

                assertEquals(listOf("user-2:INSERT"), dirtyKeysAndOps(db), scenario.displayName)
                assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"), scenario.displayName)
                assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"), scenario.displayName)
                assertEquals("Ada", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"), scenario.displayName)
                assertEquals("Grace", scalarText(db, "SELECT name FROM users WHERE id = 'user-2'"), scenario.displayName)
            }
        }
    }

    @Test
    fun pushPending_sameKeyPayloadScenariosRebaseDirtyIntent() = runBlocking {
        val scenarios = listOf(
            SameKeyRebaseScenario(
                displayName = "pushPending_sameKeyUpdateThenLaterUpdate_rebasesDirtyIntent",
                seedSyncedUser = true,
                beforeInFlightPushSql = "UPDATE users SET name = 'Ada uploaded' WHERE id = 'user-1'",
                duringInFlightPushSql = "UPDATE users SET name = 'Ada newer' WHERE id = 'user-1'",
                expectedLocalName = "Ada newer",
                expectedPayloadName = "Ada newer",
                expectedOp = "UPDATE",
                expectedBaseRowVersion = 2L,
                expectedDeleted = 0L,
            ),
            SameKeyRebaseScenario(
                displayName = "pushPending_sameKeyDeleteThenLaterRecreate_rebasesDirtyIntent",
                seedSyncedUser = true,
                beforeInFlightPushSql = "DELETE FROM users WHERE id = 'user-1'",
                duringInFlightPushSql = "INSERT INTO users(id, name) VALUES('user-1', 'Ada recreated')",
                expectedLocalName = "Ada recreated",
                expectedPayloadName = "Ada recreated",
                expectedOp = "INSERT",
                expectedBaseRowVersion = 2L,
                expectedDeleted = 1L,
            ),
            SameKeyRebaseScenario(
                displayName = "pushPending_sameKeyInsertThenLaterUpdate_rebasesDirtyIntent",
                seedSyncedUser = false,
                beforeInFlightPushSql = "INSERT INTO users(id, name) VALUES('user-1', 'Ada')",
                duringInFlightPushSql = "UPDATE users SET name = 'Ada newer' WHERE id = 'user-1'",
                expectedLocalName = "Ada newer",
                expectedPayloadName = "Ada newer",
                expectedOp = "UPDATE",
                expectedBaseRowVersion = 1L,
                expectedDeleted = 0L,
            ),
            SameKeyRebaseScenario(
                displayName = "pushPending_sameKeyUpdateThenLaterDelete_rebasesDirtyIntent",
                seedSyncedUser = true,
                beforeInFlightPushSql = "UPDATE users SET name = 'Ada uploaded' WHERE id = 'user-1'",
                duringInFlightPushSql = "DELETE FROM users WHERE id = 'user-1'",
                expectedLocalName = null,
                expectedPayloadName = null,
                expectedOp = "DELETE",
                expectedBaseRowVersion = 2L,
                expectedDeleted = 0L,
            ),
            SameKeyRebaseScenario(
                displayName = "pushPending_sameKeyInsertThenLaterDelete_rebasesDirtyIntent",
                seedSyncedUser = false,
                beforeInFlightPushSql = "INSERT INTO users(id, name) VALUES('user-1', 'Ada')",
                duringInFlightPushSql = "DELETE FROM users WHERE id = 'user-1'",
                expectedLocalName = null,
                expectedPayloadName = null,
                expectedOp = "DELETE",
                expectedBaseRowVersion = 1L,
                expectedDeleted = 0L,
            ),
        )

        for (scenario in scenarios) {
            val fetchStarted = CountDownLatch(1)
            val releaseFetch = CountDownLatch(1)
            withUsersPushServerClient { db, client, pushServer ->
                if (scenario.seedSyncedUser) {
                    db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
                    client.pushPending().getOrThrow()
                }

                pushServer.beforeCommittedBundleChunkResponse = { _, _ ->
                    fetchStarted.countDown()
                    check(releaseFetch.await(5, TimeUnit.SECONDS))
                }
                db.execSQL(scenario.beforeInFlightPushSql)

                val inFlightPush = async(Dispatchers.Default) { client.pushPending() }
                assertTrue(fetchStarted.await(5, TimeUnit.SECONDS), "${scenario.displayName}: fetch started")
                db.execSQL(scenario.duringInFlightPushSql)
                releaseFetch.countDown()
                inFlightPush.await().getOrThrow()

                val dirty = dirtyRow(db, "users", """{"id":"user-1"}""")
                if (scenario.expectedLocalName == null) {
                    assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users WHERE id = 'user-1'"), "${scenario.displayName}: local row count")
                } else {
                    assertEquals(scenario.expectedLocalName, scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"), "${scenario.displayName}: local name")
                }
                assertEquals(scenario.expectedOp, dirty.op, "${scenario.displayName}: dirty op")
                assertEquals(scenario.expectedBaseRowVersion, dirty.baseRowVersion, "${scenario.displayName}: base row version")
                if (scenario.expectedPayloadName == null) {
                    assertEquals(null, dirty.payload, "${scenario.displayName}: dirty payload")
                } else {
                    val dirtyPayload = json.parseToJsonElement(dirty.payload ?: error("expected dirty payload")).jsonObject
                    assertEquals("user-1", dirtyPayload["id"]?.jsonPrimitive?.content, "${scenario.displayName}: payload id")
                    assertEquals(scenario.expectedPayloadName, dirtyPayload["name"]?.jsonPrimitive?.content, "${scenario.displayName}: payload name")
                }
                assertEquals(
                    scenario.expectedBaseRowVersion,
                    scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE table_name = 'users' AND key_json = '{\"id\":\"user-1\"}'"),
                    "${scenario.displayName}: row state version",
                )
                assertEquals(
                    scenario.expectedDeleted,
                    scalarLong(db, "SELECT deleted FROM _sync_row_state WHERE table_name = 'users' AND key_json = '{\"id\":\"user-1\"}'"),
                    "${scenario.displayName}: row state deleted",
                )
            }
        }
    }

    @Test
    fun pushPending_ordersParentFirstUpserts_andChildFirstDeletes() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersAndPostsTables(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = listOf(
                    SyncTable("users", syncKeyColumnName = "id"),
                    SyncTable("posts", syncKeyColumnName = "id"),
                ),
                uploadLimit = 10,
            )
            client.openAndConnect("user-1").getOrThrow()

            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            db.execSQL("INSERT INTO posts(id, user_id, title) VALUES('post-1', 'user-1', 'Notes')")
            client.pushPending().getOrThrow()

            assertEquals(listOf("users:INSERT", "posts:INSERT"), pushServer.uploadedChunks[0].rows.map { "${it.table}:${it.op}" })

            db.execSQL("DELETE FROM posts WHERE id = 'post-1'")
            db.execSQL("DELETE FROM users WHERE id = 'user-1'")
            client.pushPending().getOrThrow()

            assertEquals(listOf("posts:DELETE", "users:DELETE"), pushServer.uploadedChunks[1].rows.map { "${it.table}:${it.op}" })
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun localDeleteCapturesFkCascadeIntoDirtyRows() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersAndCascadePostsTables(db)
        withPushServerClient(
            db,
            syncTables = listOf(
                SyncTable("users", syncKeyColumnName = "id"),
                SyncTable("posts", syncKeyColumnName = "id"),
            ),
        ) { client, _ ->
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            db.execSQL("INSERT INTO posts(id, user_id, title) VALUES('post-1', 'user-1', 'Notes')")
            client.pushPending().getOrThrow()

            db.execSQL("DELETE FROM users WHERE id = 'user-1'")

            assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(listOf("posts:DELETE", "users:DELETE"), dirtyOps(db))
        }
    }

    @Test
    fun keyChangingLocalUpdate_emitsDeletePlusUpsert() = runBlocking {
        withUsersPushServerClient { db, client, _ ->
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            client.pushPending().getOrThrow()

            db.execSQL("UPDATE users SET id = 'user-2' WHERE id = 'user-1'")

            assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(listOf("user-1:DELETE", "user-2:INSERT"), dirtyKeysAndOps(db))
        }
    }

    @Test
    fun pushPending_structuredConflict_keepLocalRetriesAndCommitsLatestLocalIntent() = runBlocking {
        withUserInsertStructuredConflictRetryPushClient(
            resolver = Resolver { MergeResult.KeepLocal },
        ) { db, client, pushServer ->
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Client')")

            client.pushPending().getOrThrow()

            assertEquals(listOf(1L, 1L), pushServer.createRequests.map { it.sourceBundleId })
            assertEquals(listOf("INSERT", "UPDATE"), pushServer.uploadedChunks.map { it.rows.single().op })
            assertEquals("Client", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(1L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE schema_name = 'main' AND table_name = 'users' AND key_json = '{\"id\":\"user-1\"}'"))
        }
    }

    @Test
    fun pushPending_structuredConflict_keepLocalRetriesAndCommitsLatestLocalIntent_forBlobPrimaryKey() = runBlocking {
        withStructuredConflictRetryPushClient(
            createSchema = ::createBlobDocsTable,
            syncTable = SyncTable("blob_docs", syncKeyColumnName = "id"),
            conflictResponseBody = """
                {
                  "error":"push_conflict",
                  "message":"insert conflict on main.blob_docs 00112233-4455-6677-8899-aabbccddeeff",
                  "conflict":{
                    "schema":"main",
                    "table":"blob_docs",
                    "key":{"id":"00112233-4455-6677-8899-aabbccddeeff"},
                    "op":"INSERT",
                    "base_row_version":0,
                    "server_row_version":7,
                    "server_row_deleted":false,
                    "server_row":{
                      "id":"00112233-4455-6677-8899-aabbccddeeff",
                      "name":"Server Blob",
                      "payload":"c2VydmVy"
                    }
                  }
                }
            """.trimIndent(),
            resolver = Resolver { MergeResult.KeepLocal },
        ) { db, client, pushServer ->
            db.execSQL(
                """
                INSERT INTO blob_docs(id, name, payload)
                VALUES (x'00112233445566778899aabbccddeeff', 'Client Blob', x'68656c6c6f')
                """.trimIndent()
            )

            client.pushPending().getOrThrow()

            assertEquals(listOf(1L, 1L), pushServer.createRequests.map { it.sourceBundleId })
            assertEquals(listOf("INSERT", "UPDATE"), pushServer.uploadedChunks.map { it.rows.single().op })
            val retriedPayload = (pushServer.uploadedChunks.last().rows.single().payload ?: error("expected retried payload")).jsonObject
            assertEquals("00112233-4455-6677-8899-aabbccddeeff", retriedPayload["id"]?.jsonPrimitive?.content)
            assertEquals("Client Blob", retriedPayload["name"]?.jsonPrimitive?.content)
            assertEquals("aGVsbG8=", retriedPayload["payload"]?.jsonPrimitive?.content)
            assertEquals("Client Blob", scalarText(db, "SELECT name FROM blob_docs WHERE lower(hex(id)) = '00112233445566778899aabbccddeeff'"))
            assertEquals("68656C6C6F", scalarText(db, "SELECT hex(payload) FROM blob_docs WHERE lower(hex(id)) = '00112233445566778899aabbccddeeff'"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        }
    }

    @Test
    fun pushPending_rejectsHiddenScopeColumnInConflictServerRow() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            commitError = { _, _ ->
                409 to """
                    {
                      "error":"push_conflict",
                      "message":"insert conflict on main.users user-1",
                      "conflict":{
                        "schema":"main",
                        "table":"users",
                        "key":{"id":"user-1"},
                        "op":"INSERT",
                        "base_row_version":0,
                        "server_row_version":7,
                        "server_row_deleted":false,
                        "server_row":{"id":"user-1","_sync_scope_id":"forbidden","name":"Server"}
                      }
                    }
                """.trimIndent()
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
                resolver = Resolver { MergeResult.KeepLocal },
            )
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Client')")

            val error = client.pushPending().exceptionOrNull()
            assertNotNull(error)
            assertTrue(error.message?.contains("_sync_scope_id") == true)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_structuredConflict_keepMergedRetriesMergedPayloadAndCommits() = runBlocking {
        withUserInsertStructuredConflictRetryPushClient(
            resolver = keepMergedUserResolver(),
        ) { db, client, pushServer ->
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Client')")

            client.pushPending().getOrThrow()

            assertEquals(listOf(1L, 1L), pushServer.createRequests.map { it.sourceBundleId })
            assertEquals(listOf("INSERT", "UPDATE"), pushServer.uploadedChunks.map { it.rows.single().op })
            assertEquals("""{"id":"user-1","name":"Merged"}""", pushServer.uploadedChunks.last().rows.single().payload?.toString())
            assertEquals("Merged", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        }
    }

    @Test
    fun pushPending_structuredConflict_invalidResolverForDeletedAuthoritativeUpdateRestoresDirtyRows() = runBlocking {
        val scenarios = listOf(
            StructuredConflictResolverScenario(
                displayName = "KeepLocal",
                resolver = Resolver { MergeResult.KeepLocal },
            ),
            StructuredConflictResolverScenario(
                displayName = "KeepMerged",
                resolver = keepMergedUserResolver(),
            ),
        )

        for (scenario in scenarios) {
            runCatching {
                assertStructuredConflictRestoresDirtyRows(
                    conflictResponseBody = """
                        {
                          "error":"push_conflict",
                          "message":"update conflict on main.users user-1",
                          "conflict":{
                            "schema":"main",
                            "table":"users",
                            "key":{"id":"user-1"},
                            "op":"UPDATE",
                            "base_row_version":1,
                            "server_row_version":7,
                            "server_row_deleted":true,
                            "server_row":null
                          }
                        }
                    """.trimIndent(),
                    resolver = scenario.resolver,
                    beforeConflictPush = { db ->
                        db.execSQL("UPDATE users SET name = 'Local Name' WHERE id = 'user-1'")
                    },
                    assertLocalUserState = { db ->
                        assertEquals("Local Name", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
                    },
                    expectedDirtyOp = "UPDATE",
                )
            }.getOrElse { error ->
                throw AssertionError("${scenario.displayName} scenario failed", error)
            }
        }
    }

    @Test
    fun pushPending_structuredConflict_invalidKeepMergedForDeleteRestoresDirtyRows() = runBlocking {
        assertStructuredConflictRestoresDirtyRows(
            conflictResponseBody = """
                {
                  "error":"push_conflict",
                  "message":"delete conflict on main.users user-1",
                  "conflict":{
                    "schema":"main",
                    "table":"users",
                    "key":{"id":"user-1"},
                    "op":"DELETE",
                    "base_row_version":1,
                    "server_row_version":7,
                    "server_row_deleted":false,
                    "server_row":{"id":"user-1","name":"Server"}
                  }
                }
            """.trimIndent(),
            resolver = keepMergedUserResolver(),
            beforeConflictPush = { db ->
                db.execSQL("DELETE FROM users WHERE id = 'user-1'")
            },
            assertLocalUserState = { db ->
                assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users WHERE id = 'user-1'"))
            },
            expectedDirtyOp = "DELETE",
            assertDirtyPayload = true,
        )
    }

    @Test
    fun pushPending_structuredConflict_preservesSiblingRowsFromRejectedBundle() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersAndPostsTables(db)
        val server = newServer()
        var conflictEnabled = false
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            commitError = { _, rowCount ->
                if (conflictEnabled && rowCount == 2) {
                    conflictEnabled = false
                    409 to """
                        {
                          "error":"push_conflict",
                          "message":"update conflict on main.users user-1",
                          "conflict":{
                            "schema":"main",
                            "table":"users",
                            "key":{"id":"user-1"},
                            "op":"UPDATE",
                            "base_row_version":1,
                            "server_row_version":7,
                            "server_row_deleted":false,
                            "server_row":{"id":"user-1","name":"Server"}
                          }
                        }
                    """.trimIndent()
                } else {
                    null
                }
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = listOf(
                    SyncTable("users", syncKeyColumnName = "id"),
                    SyncTable("posts", syncKeyColumnName = "id"),
                ),
                uploadLimit = 10,
                resolver = ClientWinsResolver,
            )
            client.openAndConnect("user-1").getOrThrow()

            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Seed')")
            client.pushPending().getOrThrow()

            db.execSQL("UPDATE users SET name = 'Local Name' WHERE id = 'user-1'")
            db.execSQL("INSERT INTO posts(id, user_id, title) VALUES('post-1', 'user-1', 'Sibling Post')")
            conflictEnabled = true

            client.pushPending().getOrThrow()

            assertEquals(listOf(1L, 2L, 2L), pushServer.createRequests.map { it.sourceBundleId })
            assertEquals(
                listOf("users:INSERT", "users:UPDATE,posts:INSERT", "users:UPDATE,posts:INSERT"),
                pushServer.uploadedChunks.map { chunk ->
                    chunk.rows.joinToString(",") { "${it.table}:${it.op}" }
                },
            )
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals("Local Name", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals("Sibling Post", scalarText(db, "SELECT title FROM posts WHERE id = 'post-1'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_structuredConflict_retryExhaustionLeavesReplayableDirtyState() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        var commitAttempt = 0
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            commitError = { _, _ ->
                commitAttempt++
                val serverVersion = 6L + commitAttempt
                val conflictOp = if (commitAttempt == 1) "INSERT" else "UPDATE"
                409 to """
                    {
                      "error":"push_conflict",
                      "message":"$conflictOp conflict on main.users user-1",
                      "conflict":{
                        "schema":"main",
                        "table":"users",
                        "key":{"id":"user-1"},
                        "op":"$conflictOp",
                        "base_row_version":0,
                        "server_row_version":$serverVersion,
                        "server_row_deleted":false,
                        "server_row":{"id":"user-1","name":"Server $commitAttempt"}
                      }
                    }
                """.trimIndent()
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
                resolver = Resolver { MergeResult.KeepLocal },
            )
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Client')")

            val error = client.pushPending().exceptionOrNull()
            assertNotNull(error)
            assertTrue(error is PushConflictRetryExhaustedException)
            assertEquals(2, error.retryCount)
            assertEquals(1, error.remainingDirtyCount)
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals("Client", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
            val dirty = dirtyRow(db, "users", """{"id":"user-1"}""")
            assertEquals("UPDATE", dirty.op)
            assertEquals(9L, dirty.baseRowVersion)
            assertEquals(9L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE schema_name = 'main' AND table_name = 'users' AND key_json = '{\"id\":\"user-1\"}'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_structuredConflict_serverWinsResolverAppliesAuthoritativeRow() = runBlocking {
        withUsersPushServerClient(
            resolver = ServerWinsResolver,
            configurePushServer = {
                commitError = { _, _ ->
                    409 to """
                        {
                          "error":"push_conflict",
                          "message":"insert conflict on main.users user-1",
                          "conflict":{
                            "schema":"main",
                            "table":"users",
                            "key":{"id":"user-1"},
                            "op":"INSERT",
                            "base_row_version":0,
                            "server_row_version":7,
                            "server_row_deleted":false,
                            "server_row":{"id":"user-1","name":"Server"}
                          }
                        }
                    """.trimIndent()
                }
            },
        ) { db, client, _ ->
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            client.pushPending().getOrThrow()

            assertEquals("Server", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(7L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE schema_name = 'main' AND table_name = 'users' AND key_json = '{\"id\":\"user-1\"}'"))
        }
    }

    @Test
    fun pushPending_preCommitRetry_reuploadsAllChunksFromZero() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersAndPostsTables(db)
        val server = newServer()
        var failFirstCommit = true
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            commitError = { _, _ ->
                if (failFirstCommit) {
                    failFirstCommit = false
                    500 to """{"error":"push_session_commit_failed","message":"simulated pre-commit failure"}"""
                } else {
                    null
                }
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = listOf(
                    SyncTable("users", syncKeyColumnName = "id"),
                    SyncTable("posts", syncKeyColumnName = "id"),
                ),
                uploadLimit = 1,
                transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            )
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            db.execSQL("INSERT INTO posts(id, user_id, title) VALUES('post-1', 'user-1', 'Notes')")

            val firstError = client.pushPending().exceptionOrNull()
            assertNotNull(firstError)
            assertEquals(listOf(0L, 1L), pushServer.uploadedChunks.map { it.startRowOrdinal })
            assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))

            client.pushPending().getOrThrow()

            assertEquals(listOf(0L, 1L, 0L, 1L), pushServer.uploadedChunks.map { it.startRowOrdinal })
            assertEquals(listOf(1L, 1L), pushServer.createRequests.map { it.sourceBundleId })
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_postCommitReplayRecovery_reusesCommittedBundleWithoutReupload() = runBlocking {
        assertBadHashCommittedReplayDoesNotReupload(
            retryMode = CommittedReplayRetryMode.SAME_CLIENT,
            verifyFirstFailureMessage = true,
            verifyRemoteBundleSeq = true,
        )
    }

    @Test
    fun pushPending_postCommitReplayWaitsForCommittedBundleVisibility() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        var notFoundResponsesRemaining = 2
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            committedBundleChunkError = { _, afterRowOrdinal ->
                if (afterRowOrdinal == null && notFoundResponsesRemaining > 0) {
                    notFoundResponsesRemaining--
                    404 to """{"error":"committed_bundle_not_found","message":"committed bundle not visible yet"}"""
                } else {
                    null
                }
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            client.pushPending().getOrThrow()

            assertEquals(0, notFoundResponsesRemaining)
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals("none", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(
                2L,
                scalarLong(
                    db,
                    "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = (SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1)",
                ),
            )
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_restartAfterRemoteCommitBeforeReplay_reusesCommittedBundleWithoutReupload() = runBlocking {
        assertBadHashCommittedReplayDoesNotReupload(
            retryMode = CommittedReplayRetryMode.RESTARTED_CLIENT,
        )
    }

    @Test
    fun pushPending_sourceRecoveryPreservesFrozenOutboxAcrossRebuildRestartAndFollowupPush() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        var sourceRecoveryResponsesRemaining = 1
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            createError = { request ->
                if (sourceRecoveryResponsesRemaining > 0) {
                    sourceRecoveryResponsesRemaining--
                    409 to """{"error":"source_sequence_out_of_order","message":"source bundle id is stale"}"""
                } else {
                    null
                }
            }
        }
        pushServer.install(server)
        server.snapshotRoutes(
            snapshotId = "snapshot-recover",
            snapshotBundleSeq = 0,
            rows = emptyList(),
            rowCount = 0,
            byteCount = 0,
        )
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            val originalSourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val firstError = client.pushPending().exceptionOrNull()

            assertNotNull(firstError)
            assertTrue(firstError is SourceRecoveryRequiredException)
            assertEquals(SourceRecoveryReason.SOURCE_SEQUENCE_OUT_OF_ORDER, firstError.reason)
            assertEquals("source_recovery", scalarText(db, "SELECT kind FROM _sync_operation_state WHERE singleton_key = 1"))
            assertEquals("source_sequence_out_of_order", scalarText(db, "SELECT reason FROM _sync_operation_state WHERE singleton_key = 1"))
            val reservedReplacementSourceId = scalarText(
                db,
                "SELECT replacement_source_id FROM _sync_operation_state WHERE singleton_key = 1",
            )
            assertTrue(reservedReplacementSourceId.isNotBlank())
            assertNotEquals(originalSourceId, reservedReplacementSourceId)
            assertEquals("prepared", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(1L, scalarLong(db, "SELECT source_bundle_id FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))

            client.rebuild().getOrThrow()

            val rotatedSourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1")
            assertNotEquals(originalSourceId, rotatedSourceId)
            assertEquals("none", scalarText(db, "SELECT kind FROM _sync_operation_state WHERE singleton_key = 1"))
            assertEquals("prepared", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(rotatedSourceId, scalarText(db, "SELECT source_id FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(1L, scalarLong(db, "SELECT source_bundle_id FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(
                2L,
                scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$rotatedSourceId'"),
            )

            client.close()
            val resumedClient = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            resumedClient.open().getOrThrow()
            assertTrue(resumedClient.attach("user-1").getOrThrow() is AttachResult.Connected)

            val pushReport = resumedClient.pushPending().getOrThrow()

            assertEquals(PushOutcome.COMMITTED, pushReport.outcome)
            assertEquals(listOf(1L, 1L), pushServer.createRequests.map { it.sourceBundleId })
            assertEquals(listOf(originalSourceId, rotatedSourceId), pushServer.createRequests.map { it.sourceId })
            assertEquals(rotatedSourceId, pushServer.bundles.last().sourceId)
            assertEquals(1L, pushServer.bundles.last().sourceBundleId)
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals("none", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(
                2L,
                scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$rotatedSourceId'"),
            )
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_createTimeHistoryPruned_setsDurableSourceRecoveryState() = runBlocking {
        assertPushPendingSourceRecoveryState(
            configurePushServer = {
                createError = {
                    409 to """{"error":"history_pruned","message":"stale source history was pruned"}"""
                }
            },
            expectedReason = SourceRecoveryReason.HISTORY_PRUNED,
            expectedStoredReason = "history_pruned",
            verifyRestartedSourceInfo = true,
        )
    }

    @Test
    fun pushPending_commitTimeSourceSequenceChanged_setsDurableSourceRecoveryState() = runBlocking {
        assertPushPendingSourceRecoveryState(
            configurePushServer = {
                commitError = { _, _ ->
                    409 to """{"error":"source_sequence_changed","message":"server source sequence changed"}"""
                }
            },
            expectedReason = SourceRecoveryReason.SOURCE_SEQUENCE_CHANGED,
            expectedStoredReason = "source_sequence_changed",
            expectedUploadedChunkCount = 1,
        )
    }

    @Test
    fun pushPending_committedReplayPruned_requiresKeepSourceRebuild_andAdvancesSourceFloor() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            committedBundleChunkError = { _, _ ->
                409 to """{"error":"history_pruned","message":"committed replay is below retained floor"}"""
            }
        }
        pushServer.install(server)
        server.snapshotRoutes(
            snapshotId = "snapshot-committed-pruned",
            snapshotBundleSeq = 1,
            rows = listOf(
                SnapshotChunkRow(
                    table = "users",
                    keyJson = """{"id":"user-1"}""",
                    rowVersion = 1,
                    payloadJson = """{"id":"user-1","name":"Ada"}""",
                )
            ),
        )
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            val sourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val firstError = client.pushPending().exceptionOrNull()

            assertTrue(firstError is RebuildRequiredException)
            assertEquals(1L, scalarLong(db, "SELECT rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1"))
            assertEquals("committed_remote", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(1L, scalarLong(db, "SELECT source_bundle_id FROM _sync_outbox_bundle WHERE singleton_key = 1"))

            client.rebuild().getOrThrow()

            assertEquals(sourceId, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1"))
            assertEquals(0L, scalarLong(db, "SELECT rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1"))
            assertEquals("none", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(2L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$sourceId'"))
            assertEquals("Ada", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_committedRemoteMismatch_marksRebuildRequired_andRebuildAdvancesSourceFloor() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        pushServer.install(server)
        server.snapshotRoutes(
            snapshotId = "snapshot-committed-mismatch",
            snapshotBundleSeq = 2,
            rows = emptyList(),
        )
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            val sourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-2', 'Grace')")
            client.pushPending().getOrThrow()

            pushServer.committedRowsTransform = { rows -> rows.reversed() }
            db.execSQL("DELETE FROM users WHERE id = 'user-1'")
            db.execSQL("DELETE FROM users WHERE id = 'user-2'")

            val firstError = client.pushPending().exceptionOrNull()

            assertTrue(firstError is RebuildRequiredException)
            assertEquals(1L, scalarLong(db, "SELECT rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1"))
            assertEquals("committed_remote", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(2L, scalarLong(db, "SELECT source_bundle_id FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))

            client.rebuild().getOrThrow()

            assertEquals(sourceId, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1"))
            assertEquals(0L, scalarLong(db, "SELECT rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1"))
            assertEquals("none", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(3L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$sourceId'"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_alreadyCommittedWithDifferentCommittedRows_throwsSourceSequenceMismatch() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val committedPayloadJson = """{"id":"user-1","name":"Server Ada"}"""
        val committedHash = userCommittedInsertHash(committedPayloadJson)
        val server = newServer().apply {
            installCommittedReplayRoutes(
                pushId = "",
                committedHash = committedHash,
                committedRowsJson = userCommittedInsertRowJson(committedPayloadJson),
                alreadyCommitted = true,
            )
        }
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Local Ada')")

            val error = client.pushPending().exceptionOrNull()

            assertNotNull(error)
            assertTrue(
                error is SourceSequenceMismatchException,
                "expected SourceSequenceMismatchException, got ${error::class.qualifiedName}: ${error.message}",
            )
            assertEquals(0L, scalarLong(db, "SELECT rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1"))
            assertEquals("prepared", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(
                1L,
                scalarLong(
                    db,
                    "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = (SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1)",
                ),
            )
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_restartReusesExistingOutboundSnapshot_insteadOfFreezingNewSnapshot() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        var failFirstCommit = true
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            commitError = { _, _ ->
                if (failFirstCommit) {
                    failFirstCommit = false
                    500 to """{"error":"push_session_commit_failed","message":"simulated restart window"}"""
                } else {
                    null
                }
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val firstClient = newClient(
                db,
                http,
                syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
                transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            )
            firstClient.openAndConnect("user-1").getOrThrow()
            val sourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val firstError = firstClient.pushPending().exceptionOrNull()
            assertNotNull(firstError)
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))

            db.execSQL("INSERT INTO users(id, name) VALUES('user-2', 'Grace')")

            val secondClient = newClient(
                db,
                http,
                syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
                transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            )
            secondClient.openAndConnect("user-1").getOrThrow()
            secondClient.pushPending().getOrThrow()

            assertEquals(listOf(1L, 1L), pushServer.createRequests.map { it.sourceBundleId })
            assertEquals(listOf("user-1", "user-1"), pushServer.uploadedChunks.map { it.rows.single().key.getValue("id") })
            assertEquals(listOf("user-2:INSERT"), dirtyKeysAndOps(db))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(
                2L,
                scalarLong(
                    db,
                    "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = (SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1)",
                ),
            )
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_finalReplayDefersForeignKeys() = runBlocking {
        val scenarios = listOf(
            FinalReplayForeignKeyScenario(
                displayName = "self-referential rows",
                createTables = ::createEmployeesTable,
                syncTables = listOf(SyncTable("employees", syncKeyColumnName = "id")),
                insertRows = { db ->
                    db.execSQL("INSERT INTO employees(id, manager_id, name) VALUES('employee-2', 'employee-1', 'Bob')")
                    db.execSQL("INSERT INTO employees(id, manager_id, name) VALUES('employee-1', NULL, 'Alice')")
                },
                assertReplayedState = { db ->
                    assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM employees"))
                    assertEquals("employee-1", scalarText(db, "SELECT manager_id FROM employees WHERE id = 'employee-2'"))
                    assertEquals("Alice", scalarText(db, "SELECT name FROM employees WHERE id = 'employee-1'"))
                },
            ),
            FinalReplayForeignKeyScenario(
                displayName = "immediate cyclic graph",
                createTables = ::createImmediateAuthorsAndProfilesCycleTables,
                syncTables = listOf(
                    SyncTable("authors", syncKeyColumnName = "id"),
                    SyncTable("profiles", syncKeyColumnName = "id"),
                ),
                insertRows = { db ->
                    db.execSQL("INSERT INTO authors(id, profile_id, name) VALUES('author-1', 'profile-1', 'Author')")
                    db.execSQL("INSERT INTO profiles(id, author_id, bio) VALUES('profile-1', 'author-1', 'Cyclic')")
                },
                assertReplayedState = { db ->
                    assertEquals("profile-1", scalarText(db, "SELECT profile_id FROM authors WHERE id = 'author-1'"))
                    assertEquals("author-1", scalarText(db, "SELECT author_id FROM profiles WHERE id = 'profile-1'"))
                    assertEquals("Cyclic", scalarText(db, "SELECT bio FROM profiles WHERE id = 'profile-1'"))
                },
            ),
        )

        scenarios.forEach { scenario ->
            val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
            scenario.createTables(db)
            withPushServerClient(
                db,
                syncTables = scenario.syncTables,
            ) { client, _ ->
                db.transaction(TransactionMode.IMMEDIATE) {
                    db.execSQL("PRAGMA defer_foreign_keys = ON")
                    scenario.insertRows(db)
                }

                client.pushPending().getOrThrow()

                assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"), scenario.displayName)
                scenario.assertReplayedState(db)
            }
        }
    }

    @Test
    fun pushThenPull_withStaleRemoteBundleAndRealColumn_doesNotRequireSecondSync() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createScoredUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        val remoteRows = listOf(
            BundleRow(
                schema = "main",
                table = "users",
                key = mapOf("id" to "remote-user"),
                op = "INSERT",
                rowVersion = 1,
                payload = buildJsonObject {
                    put("id", JsonPrimitive("remote-user"))
                    put("name", JsonPrimitive("Remote Ada"))
                    put("score", JsonPrimitive(1.25))
                },
            )
        )
        pushServer.bundles += FakeChunkedSyncServer.StoredBundle(
            bundleSeq = 1,
            sourceId = "peer-a",
            sourceBundleId = 11,
            bundleHash = computeCommittedBundleHash(remoteRows),
            rows = remoteRows,
        )
        pushServer.nextBundleSeq = 2L
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()

            db.execSQL("INSERT INTO users(id, name, score) VALUES('local-user', 'Local Bob', 6.57111473696007)")

            client.pushPending().getOrThrow()

            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, client.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(2, pushServer.bundles.size)

            client.pullToStable().getOrThrow()

            assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM users"))
            assertEquals("Remote Ada", scalarText(db, "SELECT name FROM users WHERE id = 'remote-user'"))
            assertEquals("Local Bob", scalarText(db, "SELECT name FROM users WHERE id = 'local-user'"))
            assertEquals(2, pushServer.bundles.size)
            assertEquals(2L, client.syncStatus().getOrThrow().lastBundleSeqSeen)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_successfulReplayRestoresCaptureForOrdinaryWrites() = runBlocking {
        withUsersPushServerClient { db, client, _ ->
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            client.pushPending().getOrThrow()

            assertEquals(0L, scalarLong(db, "SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1"))

            db.execSQL("UPDATE users SET name = 'Ada updated' WHERE id = 'user-1'")

            assertEquals(listOf("user-1:UPDATE"), dirtyKeysAndOps(db))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        }
    }

    @Test
    fun pushPending_failedReplayRollbackCannotLeaveLeakedApplyMode_andLaterWritesAreCaptured() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val committedPayloadJson = """{"id":"user-1"}"""
        val committedHash = userCommittedInsertHash(committedPayloadJson)
        val server = newServer().apply {
            installCommittedReplayRoutes(
                pushId = "push-bad-replay",
                committedHash = committedHash,
                committedRowsJson = userCommittedInsertRowJson(committedPayloadJson),
            )
        }
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val error = client.pushPending().exceptionOrNull()
            assertNotNull(error)
            assertTrue(
                error is SourceSequenceMismatchException ||
                    error.message?.contains("does not match the canonical prepared outbox rows") == true ||
                    error.message?.contains("payload for users must contain every table column") == true,
            )
            assertEquals(0L, scalarLong(db, "SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))

            db.execSQL("INSERT INTO users(id, name) VALUES('user-2', 'Grace')")

            assertEquals(listOf("user-2:INSERT"), dirtyKeysAndOps(db))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_successfulDetachRotatesSourceAndStartsFreshSequence() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            val sourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            client.pushPending().getOrThrow()
            assertEquals(
                2L,
                scalarLong(
                    db,
                    "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = (SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1)",
                ),
            )

            assertEquals(DetachOutcome.DETACHED, client.detach().getOrThrow())
            client.open().getOrThrow()
            val rotatedSourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            assertNotEquals(sourceId, rotatedSourceId)
            assertEquals(
                rotatedSourceId,
                scalarText(db, "SELECT replaced_by_source_id FROM _sync_source_state WHERE source_id = '$sourceId'"),
            )
            client.attach("user-1").getOrThrow()
            assertEquals(
                1L,
                scalarLong(
                    db,
                    "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = (SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1)",
                ),
            )

            db.execSQL("INSERT INTO users(id, name) VALUES('user-2', 'Grace')")

            val report = client.pushPending().getOrThrow()

            assertEquals(PushOutcome.COMMITTED, report.outcome)
            assertEquals(listOf(sourceId, rotatedSourceId), pushServer.createRequests.map { it.sourceId })
            assertEquals(listOf(1L, 1L), pushServer.createRequests.map { it.sourceBundleId })
            assertEquals(2, pushServer.bundles.size)
            assertEquals(1L, pushServer.bundles.last().sourceBundleId)
            assertEquals(emptyList<String>(), dirtyKeysAndOps(db))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun syncThenDetach_oneRoundSuccess_detachesAfterCleanSync() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            val sourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val result = client.syncThenDetach().getOrThrow()

            assertTrue(result.isSuccess())
            assertEquals(DetachOutcome.DETACHED, result.detach)
            assertEquals(1, result.syncRounds)
            assertEquals(0L, result.remainingPendingRowCount)
            assertEquals(PushOutcome.COMMITTED, result.lastSync.pushOutcome)
            client.open().getOrThrow()
            val rotatedSourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            assertNotEquals(sourceId, rotatedSourceId)
            assertEquals(
                rotatedSourceId,
                scalarText(db, "SELECT replaced_by_source_id FROM _sync_source_state WHERE source_id = '$sourceId'"),
            )
            assertEquals(1L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$rotatedSourceId'"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun syncThenDetach_retriesWhenFreshWritesArriveDuringFirstRound_andEventuallyDetaches() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        var insertedFollowup = false
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            beforePullResponse = { _, _ ->
                if (!insertedFollowup) {
                    insertedFollowup = true
                    runBlocking {
                        db.execSQL("INSERT INTO users(id, name) VALUES('user-2', 'Grace')")
                    }
                }
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            val sourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val result = client.syncThenDetach().getOrThrow()

            assertTrue(result.isSuccess())
            assertEquals(DetachOutcome.DETACHED, result.detach)
            assertEquals(2, result.syncRounds)
            assertEquals(0L, result.remainingPendingRowCount)
            assertEquals(listOf(1L, 2L), pushServer.createRequests.map { it.sourceBundleId })
            client.open().getOrThrow()
            val rotatedSourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            assertNotEquals(sourceId, rotatedSourceId)
            assertEquals(
                rotatedSourceId,
                scalarText(db, "SELECT replaced_by_source_id FROM _sync_source_state WHERE source_id = '$sourceId'"),
            )
            assertEquals(1L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$rotatedSourceId'"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun syncThenDetach_stopsAfterNoPendingProgress_andReturnsBlocked() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        var insertedRounds = 0
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            beforePullResponse = { _, _ ->
                if (insertedRounds < 2) {
                    insertedRounds += 1
                    val userId = "late-user-$insertedRounds"
                    runBlocking {
                        db.execSQL("INSERT INTO users(id, name) VALUES('$userId', 'Late $insertedRounds')")
                    }
                }
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            val sourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val result = client.syncThenDetach().getOrThrow()

            assertFalse(result.isSuccess())
            assertEquals(DetachOutcome.BLOCKED_UNSYNCED_DATA, result.detach)
            assertEquals(2, result.syncRounds)
            assertEquals(1L, result.remainingPendingRowCount)
            assertEquals(listOf(1L, 2L), pushServer.createRequests.map { it.sourceBundleId })
            client.open().getOrThrow()
            assertEquals(sourceId, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals("attached", scalarText(db, "SELECT binding_state FROM _sync_attachment_state"))
            assertEquals("user-1", scalarText(db, "SELECT attached_user_id FROM _sync_attachment_state"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun syncThenDetach_syncFailure_doesNotImplicitlyDetach() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val committedPayloadJson = """{"id":"user-1","name":"Ada"}"""
        val committedBundleHash = userCommittedInsertHash(committedPayloadJson)
        val server = newServer().apply {
            installCommittedReplayRoutes(
                pushId = "push-1",
                committedHash = committedBundleHash,
                committedRowsJson = userCommittedInsertRowJson(committedPayloadJson),
            )
            createContext("/sync/pull") { exchange ->
                respondJson(
                    exchange,
                    500,
                    """
                    {"error":"pull_failed","message":"forced pull failure"}
                    """.trimIndent(),
                )
            }
        }
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val error = client.syncThenDetach().exceptionOrNull()

            assertNotNull(error)
            client.open().getOrThrow()
            assertEquals("attached", scalarText(db, "SELECT binding_state FROM _sync_attachment_state"))
            assertEquals("user-1", scalarText(db, "SELECT attached_user_id FROM _sync_attachment_state"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM users"))
            assertEquals("Ada", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_serializesBlobUuidKeysAndPayloadsForCanonicalWireFormat() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createBlobDocsTable(db)
        withPushServerClient(
            db,
            syncTables = listOf(SyncTable("blob_docs", syncKeyColumnName = "id")),
        ) { client, pushServer ->
            db.execSQL(
                """
                INSERT INTO blob_docs(id, name, payload)
                VALUES (x'00112233445566778899aabbccddeeff', 'Blob Doc', x'68656c6c6f')
                """.trimIndent()
            )

            client.pushPending().getOrThrow()

            val row = pushServer.uploadedChunks.single().rows.single()
            assertEquals("00112233-4455-6677-8899-aabbccddeeff", row.key["id"])
            val payload = (row.payload ?: error("expected uploaded payload")).jsonObject
            assertEquals("00112233-4455-6677-8899-aabbccddeeff", payload["id"]?.jsonPrimitive?.content)
            assertEquals("Blob Doc", payload["name"]?.jsonPrimitive?.content)
            assertEquals("aGVsbG8=", payload["payload"]?.jsonPrimitive?.content)
        }
    }

    @Test
    fun pushPending_preservesNullBlobPayloadFieldsOnWire() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createTypedRowsTable(db)
        withPushServerClient(
            db,
            syncTables = listOf(SyncTable("typed_rows", syncKeyColumnName = "id")),
        ) { client, pushServer ->
            db.execSQL(
                """
                INSERT INTO typed_rows(id, name, note, count_value, enabled_flag, rating, data, created_at)
                VALUES('typed-1', 'Typed Row', NULL, NULL, 0, 1.25, NULL, NULL)
                """.trimIndent()
            )

            client.pushPending().getOrThrow()

            val row = pushServer.uploadedChunks.single().rows.single()
            val payload = (row.payload ?: error("expected uploaded payload")).jsonObject
            assertEquals("typed-1", row.key["id"])
            assertEquals(JsonNull, payload["note"])
            assertEquals(JsonNull, payload["count_value"])
            assertEquals("0", payload["enabled_flag"]?.jsonPrimitive?.content)
            assertEquals("1.25", payload["rating"]?.jsonPrimitive?.content)
            assertEquals(JsonNull, payload["data"])
            assertEquals(JsonNull, payload["created_at"])
        }
    }

    @Test
    fun pushPending_replaysCommittedTypedRowsWithEquivalentPayloads() = runBlocking {
        val scenarios = listOf(
            TypedRowsCommittedReplaySuccessScenario(
                displayName = "accepts timestamp payload equivalent by instant across offsets",
                pushId = "push-typed-offset",
                enabledFlag = JsonPrimitive(0),
                createdAt = JsonPrimitive("2026-03-24T20:42:11+02:00"),
                enabledFlagJson = "0",
                createdAtJson = "\"2026-03-24T20:42:11+02:00\"",
                localCreatedAtSql = "'2026-03-24T18:42:11Z'",
                assertReplayedState = { db ->
                    assertEquals(
                        OffsetDateTime.parse("2026-03-24T18:42:11Z").toInstant(),
                        OffsetDateTime.parse(scalarText(db, "SELECT created_at FROM typed_rows WHERE id = 'typed-1'")).toInstant(),
                    )
                },
            ),
            TypedRowsCommittedReplaySuccessScenario(
                displayName = "replays boolean payload into integer column",
                pushId = "push-typed-bool",
                enabledFlag = JsonPrimitive(false),
                createdAt = JsonNull,
                enabledFlagJson = "false",
                createdAtJson = "null",
                localCreatedAtSql = "NULL",
                assertReplayedState = { db ->
                    assertEquals(0L, scalarLong(db, "SELECT enabled_flag FROM typed_rows WHERE id = 'typed-1'"))
                },
            ),
        )

        for (scenario in scenarios) {
            withTypedRowsCommittedReplayClient(
                pushId = scenario.pushId,
                committedRows = typedCommittedRows(
                    enabledFlag = scenario.enabledFlag,
                    createdAt = scenario.createdAt,
                ),
                committedRowsJson = typedCommittedRowJson(
                    enabledFlag = scenario.enabledFlagJson,
                    createdAt = scenario.createdAtJson,
                ),
            ) { db, client ->
                db.execSQL(
                    """
                    INSERT INTO typed_rows(id, name, note, count_value, enabled_flag, rating, data, created_at)
                    VALUES('typed-1', 'Typed Row', NULL, NULL, 0, 1.25, NULL, ${scenario.localCreatedAtSql})
                    """.trimIndent()
                )

                client.pushPending().getOrThrow()

                assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"), scenario.displayName)
                assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"), scenario.displayName)
                scenario.assertReplayedState(db)
            }
        }
    }

    @Test
    fun pushPending_rejectsCommittedTimestampPayloadWhenPreparedValueIsNaiveLocalText() = runBlocking {
        withTypedRowsCommittedReplayClient(
            pushId = "",
            committedRows = typedCommittedRows(
                enabledFlag = JsonPrimitive(0),
                createdAt = JsonPrimitive("2026-03-24T20:42:11+02:00"),
            ),
            committedRowsJson = typedCommittedRowJson(
                enabledFlag = "0",
                createdAt = "\"2026-03-24T20:42:11+02:00\"",
            ),
            alreadyCommitted = true,
        ) { db, client ->
            db.execSQL(
                """
                INSERT INTO typed_rows(id, name, note, count_value, enabled_flag, rating, data, created_at)
                VALUES('typed-1', 'Typed Row', NULL, NULL, 0, 1.25, NULL, '2026-03-24 18:42:11')
                """.trimIndent()
            )

            val error = client.pushPending().exceptionOrNull()

            assertNotNull(error)
            assertTrue(
                error is SourceSequenceMismatchException,
                "expected SourceSequenceMismatchException, got ${error::class.qualifiedName}: ${error.message}",
            )
            assertEquals("prepared", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals("2026-03-24 18:42:11", scalarText(db, "SELECT created_at FROM typed_rows WHERE id = 'typed-1'"))
        }
    }

}

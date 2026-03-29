package dev.goquick.sqlitenow.oversqlite

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.coroutines.runBlocking
import java.net.InetSocketAddress
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class LifecycleContractTest : BundleClientContractTestSupport() {
    @Test
    fun open_isRequired_andConstructionIsSideEffectFree() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))

            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM sqlite_master WHERE name = '_sync_lifecycle_state'"))
            assertTrue(client.pushPending().exceptionOrNull() is OpenRequiredException)

            val openState = client.open().getOrThrow()

            assertEquals(OpenState.ReadyAnonymous, openState)
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM sqlite_master WHERE name = '_sync_lifecycle_state'"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM sqlite_master WHERE name = '_sync_attachment_state'"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM sqlite_master WHERE name = '_sync_operation_state'"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM sqlite_master WHERE name = '_sync_managed_tables'"))
            assertTrue(scalarText(db, "SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1").isNotBlank())
            assertTrue(client.pushPending().exceptionOrNull() is ConnectRequiredException)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun open_persistsCallerOwnedSourceId_andRejectsMismatchedSourceIdOnRestart() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val sourceId = "install-source-a"
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))

            assertEquals(OpenState.ReadyAnonymous, client.open(sourceId).getOrThrow())
            assertEquals(sourceId, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1"))

            client.close()
            val restartedClient = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            assertEquals(OpenState.ReadyAnonymous, restartedClient.open(sourceId).getOrThrow())

            val mismatch = restartedClient.open("install-source-b").exceptionOrNull()
            assertIs<SourceBindingMismatchException>(mismatch)
            assertEquals(sourceId, mismatch.persistedSourceId)
            assertEquals("install-source-b", mismatch.requestedSourceId)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun open_rejectsOmittedSyncKeyConfiguration() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users")))
            val error = client.open().exceptionOrNull()
            assertTrue(error != null)
            assertTrue(error.message?.contains("syncKeyColumnName or syncKeyColumns explicitly") == true)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun open_rejectsMissingSyncKeyColumn() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "missing_id")))
            val error = client.open().exceptionOrNull()
            assertTrue(error != null)
            assertTrue(error.message?.contains("does not contain configured primary key column missing_id") == true)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun open_acceptsBlobPrimaryKeySyncTables() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createBlobUsersTable(db)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.open().getOrThrow()
            val removedLegacyTableName = "_sync_" + "client_state"

            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM sqlite_master WHERE name = '_sync_apply_state'"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM sqlite_master WHERE name = '$removedLegacyTableName'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun syncStatus_readsLastBundleFromAttachmentState_andLegacyClientTableIsAbsent() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("UPDATE _sync_attachment_state SET last_bundle_seq_seen = 7 WHERE singleton_key = 1")
            val removedLegacyTableName = "_sync_" + "client_state"

            assertEquals(7L, client.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM sqlite_master WHERE name = '$removedLegacyTableName'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun anonymousApplyModeSuppressesDirtyCapture_withoutAttachedUserRow() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.open().getOrThrow()

            db.execSQL("UPDATE _sync_apply_state SET apply_mode = 1 WHERE singleton_key = 1")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada during apply')")
            db.execSQL("UPDATE _sync_apply_state SET apply_mode = 0 WHERE singleton_key = 1")

            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))

            db.execSQL("INSERT INTO users(id, name) VALUES('user-2', 'Grace after apply')")

            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals("""{"id":"user-2"}""", scalarText(db, "SELECT key_json FROM _sync_dirty_rows LIMIT 1"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun open_rejectsUnsupportedIntegerAndBigIntSyncKeys() = runBlocking {
        suspend fun assertRejected(
            db: SafeSQLiteConnection,
            expectedType: String,
        ) {
            val server = newServer()
            val http = newHttpClient(server)
            try {
                val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
                val error = client.open().exceptionOrNull()
                assertTrue(error != null)
                assertTrue(error.message?.contains("TEXT PRIMARY KEY or BLOB PRIMARY KEY") == true, "expected $expectedType rejection")
            } finally {
                http.close()
                server.stop(0)
                db.close()
            }
        }

        val integerDb = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createIntegerUsersTable(integerDb)
        assertRejected(integerDb, "INTEGER")

        val bigIntDb = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createBigIntUsersTable(bigIntDb)
        assertRejected(bigIntDb, "BIGINT")
    }

    @Test
    fun rotateSource_requiresExplicitFreshSourceId() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            assertEquals(OpenState.ReadyAnonymous, client.open("install-source-a").getOrThrow())

            val firstRotation = client.rotateSource("install-source-b").getOrThrow()
            assertEquals("install-source-b", firstRotation.sourceId)
            assertEquals("install-source-b", scalarText(db, "SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1"))

            val sameSourceError = client.rotateSource("install-source-b").exceptionOrNull()
            assertIs<IllegalArgumentException>(sameSourceError)

            val reusedSourceError = client.rotateSource("install-source-a").exceptionOrNull()
            assertIs<SourceSequenceMismatchException>(reusedSourceError)
            Unit
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun open_rejectsReservedServerScopeColumnInLocalSchema() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTableWithReservedScopeColumn(db)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            val error = client.open().exceptionOrNull()
            assertTrue(error != null)
            assertTrue(error.message?.contains("must not declare reserved server column _sync_scope_id") == true)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun open_rejectsNonFkClosedManagedSchema() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersAndPostsTables(db)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("posts", syncKeyColumnName = "id")))
            val error = client.open().exceptionOrNull()
            assertTrue(error != null)
            assertTrue(error.message?.contains("managed tables are not FK-closed") == true)
            assertTrue(error.message?.contains("posts.user_id -> users.id") == true)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun open_rejectsCompositeKeysAndCompositeForeignKeys() = runBlocking {
        val compositeKeyDb = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createCompositePkTable(compositeKeyDb)
        val compositeKeyServer = newServer()
        val compositeKeyHttp = newHttpClient(compositeKeyServer)
        try {
            val client = newClient(
                compositeKeyDb,
                compositeKeyHttp,
                syncTables = listOf(SyncTable("pairs", syncKeyColumns = listOf("id_a", "id_b"))),
            )
            val error = client.open().exceptionOrNull()
            assertTrue(error != null)
            assertTrue(error.message?.contains("must declare exactly one sync key column") == true)
        } finally {
            compositeKeyHttp.close()
            compositeKeyServer.stop(0)
            compositeKeyDb.close()
        }

        val compositeFkDb = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createCompositeForeignKeyTables(compositeFkDb)
        val compositeFkServer = newServer()
        val compositeFkHttp = newHttpClient(compositeFkServer)
        try {
            val client = newClient(
                compositeFkDb,
                compositeFkHttp,
                syncTables = listOf(
                    SyncTable("parent_pairs", syncKeyColumnName = "id"),
                    SyncTable("child_pairs", syncKeyColumnName = "id"),
                ),
            )
            val error = client.open().exceptionOrNull()
            assertTrue(error != null)
            assertTrue(error.message?.contains("unsupported composite foreign keys") == true)
            assertTrue(error.message?.contains("child_pairs -> parent_pairs") == true)
        } finally {
            compositeFkHttp.close()
            compositeFkServer.stop(0)
            compositeFkDb.close()
        }
    }

    @Test
    fun open_rejectsSchemaQualifiedTableRegistration() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("main.users", syncKeyColumnName = "id")))
            val error = client.open().exceptionOrNull()
            assertTrue(error != null)
            assertTrue(error.message?.contains("must not include a schema qualifier") == true)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun open_capturesPreexistingManagedRowsIntoDirtyRowsWithoutStructuredState() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersAndPostsTables(db)
        db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
        db.execSQL("INSERT INTO posts(id, user_id, title) VALUES('post-1', 'user-1', 'Notes')")
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = listOf(
                    SyncTable("users", syncKeyColumnName = "id"),
                    SyncTable("posts", syncKeyColumnName = "id"),
                ),
            )

            client.open().getOrThrow()

            assertEquals(listOf("users:INSERT", "posts:INSERT"), dirtyOps(db))
            assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_row_state"))
            val userDirty = dirtyRow(db, "users", """{"id":"user-1"}""")
            assertEquals("INSERT", userDirty.op)
            assertEquals(0L, userDirty.baseRowVersion)
            assertEquals("""{"id":"user-1","name":"Ada"}""", userDirty.payload)
            val postDirty = dirtyRow(db, "posts", """{"id":"post-1"}""")
            assertEquals("INSERT", postDirty.op)
            assertEquals(0L, postDirty.baseRowVersion)
            assertEquals("""{"id":"post-1","user_id":"user-1","title":"Notes"}""", postDirty.payload)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun open_capturesBlobPrimaryKeysIntoDirtyRows() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createBlobDocsTable(db)
        db.execSQL(
            """
            INSERT INTO blob_docs(id, name, payload)
            VALUES (x'00112233445566778899aabbccddeeff', 'Blob Doc', x'68656c6c6f')
            """.trimIndent(),
        )
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("blob_docs", syncKeyColumnName = "id")))

            client.open().getOrThrow()

            assertEquals(
                """{"id":"00112233445566778899aabbccddeeff"}""",
                scalarText(db, "SELECT key_json FROM _sync_dirty_rows WHERE table_name = 'blob_docs'"),
            )
            val dirty = dirtyRow(db, "blob_docs", """{"id":"00112233445566778899aabbccddeeff"}""")
            assertEquals("INSERT", dirty.op)
            assertEquals(0L, dirty.baseRowVersion)
            assertEquals(
                """{"id":"00112233445566778899aabbccddeeff","name":"Blob Doc","payload":"68656c6c6f"}""",
                dirty.payload,
            )
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun open_isIdempotent_andPreservesInstalledTriggers() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersAndPostsTables(db)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = listOf(
                    SyncTable("users", syncKeyColumnName = "id"),
                    SyncTable("posts", syncKeyColumnName = "id"),
                ),
            )

            client.open().getOrThrow()
            val before = readTriggerNames(db)

            client.open().getOrThrow()
            val after = readTriggerNames(db)

            assertEquals(12, before.size)
            assertEquals(before, after)
            assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM _sync_managed_tables"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun open_preservesUnknownSyncPrefixedApplicationTables() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        db.execSQL(
            """
            CREATE TABLE _sync_audit_log (
              id INTEGER PRIMARY KEY NOT NULL,
              message TEXT NOT NULL
            )
            """.trimIndent()
        )
        db.execSQL("INSERT INTO _sync_audit_log(id, message) VALUES(1, 'keep me')")
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))

            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM sqlite_master WHERE name = '_sync_audit_log'"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_audit_log"))

            client.open().getOrThrow()
            client.open().getOrThrow()

            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM sqlite_master WHERE name = '_sync_audit_log'"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_audit_log"))
            assertEquals("keep me", scalarText(db, "SELECT message FROM _sync_audit_log WHERE id = 1"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun open_reusesExistingInternalSourceIdentity_andPreservesExistingState() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.open().getOrThrow()
            val persistedSourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val reopened = client.open().getOrThrow()

            assertEquals(OpenState.ReadyAnonymous, reopened)
            assertEquals(persistedSourceId, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM users"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun open_resumesAttachedScopeFromAttachmentSingleton_whenOperationStateIsIdle() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.open("device-a").getOrThrow()
            OversqliteAttachmentStateStore(db).persistState(
                OversqliteAttachmentState(
                    currentSourceId = "device-a",
                    bindingState = attachmentBindingAttached,
                    attachedUserId = "user-1",
                    schemaName = "main",
                    lastBundleSeqSeen = 0,
                    rebuildRequired = false,
                    pendingInitializationId = "",
                ),
            )
            assertEquals(OpenState.ReadyAttached("user-1"), client.open("device-a").getOrThrow())
            assertEquals("attached", scalarText(db, "SELECT binding_state FROM _sync_attachment_state"))
            assertEquals("user-1", scalarText(db, "SELECT attached_user_id FROM _sync_attachment_state"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun syncStatus_requiresConnectedScope() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))

            assertTrue(client.syncStatus().exceptionOrNull() != null)
            client.open().getOrThrow()
            assertTrue(client.syncStatus().exceptionOrNull() is ConnectRequiredException)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun connect_initializeEmpty_andResumeSameUserOffline() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))

            assertEquals(OpenState.ReadyAnonymous, client.open().getOrThrow())
            val firstConnect = client.attach("user-1").getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.STARTED_EMPTY,
                actual = firstConnect,
                expectedAuthority = AuthorityStatus.AUTHORITATIVE_EMPTY,
            )
            assertEquals("attached", scalarText(db, "SELECT binding_state FROM _sync_attachment_state"))
            assertEquals("user-1", scalarText(db, "SELECT attached_user_id FROM _sync_attachment_state"))

            server.stop(0)
            http.close()

            val offlineServer = newServer()
            val offlineHttp = newHttpClient(offlineServer)
            val resumedClient = newClient(db, offlineHttp, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            try {
                assertEquals(OpenState.ReadyAttached("user-1"), resumedClient.open().getOrThrow())
                assertConnectedOutcome(
                    expectedOutcome = AttachOutcome.RESUMED_ATTACHED_STATE,
                    actual = resumedClient.attach("user-1").getOrThrow(),
                )
            } finally {
                offlineHttp.close()
                offlineServer.stop(0)
            }
        } finally {
            db.close()
        }
    }

    @Test
    fun connect_initializeLocal_persistsPendingInitializationId_andBlocksPlainSignOut() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
        val server = newServer()
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))

            client.open().getOrThrow()
            val result = client.attach("user-1").getOrThrow()

            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.SEEDED_FROM_LOCAL,
                actual = result,
                expectedAuthority = AuthorityStatus.PENDING_LOCAL_SEED,
            )
            assertEquals("attached", scalarText(db, "SELECT binding_state FROM _sync_attachment_state"))
            assertEquals("user-1", scalarText(db, "SELECT attached_user_id FROM _sync_attachment_state"))
            assertEquals("init-connect", scalarText(db, "SELECT pending_initialization_id FROM _sync_attachment_state"))
            assertEquals(
                PendingSyncStatus(
                    hasPendingSyncData = true,
                    pendingRowCount = 1,
                    blocksDetach = true,
                ),
                client.syncStatus().getOrThrow().pending,
            )
            assertEquals(DetachOutcome.BLOCKED_UNSYNCED_DATA, client.detach().getOrThrow())
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM users"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun connect_retryLater_isSurfacedAsNormalLifecycleOutcome() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newLifecycleServer(
            capabilitiesBody = """
                {"protocol_version":"v1","schema_version":1,"features":{"connect_lifecycle":true}}
            """.trimIndent(),
            connectHandler = { _ ->
                200 to """
                    {"resolution":"retry_later","retry_after_seconds":7,"lease_expires_at":"2099-01-01T00:00:00Z"}
                """.trimIndent()
            },
        )
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.open().getOrThrow()

            assertEquals(AttachResult.RetryLater(7), client.attach("user-1").getOrThrow())
            assertEquals("anonymous", scalarText(db, "SELECT binding_state FROM _sync_attachment_state"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun connect_transportFailure_isRetryable_andLeavesLocalStateUntouched() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
                transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 2, initialBackoffMillis = 1, maxBackoffMillis = 2),
            )
            client.open().getOrThrow()

            val error = client.attach("user-1").exceptionOrNull()

            assertTrue(error != null)
            assertEquals("anonymous", scalarText(db, "SELECT binding_state FROM _sync_attachment_state"))
            assertEquals("none", scalarText(db, "SELECT kind FROM _sync_operation_state"))
            assertEquals("", scalarText(db, "SELECT pending_initialization_id FROM _sync_attachment_state"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun connect_failsClosed_whenServerDoesNotAdvertiseLifecycleCapability() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newLifecycleServer(
            capabilitiesBody = """
                {"protocol_version":"v1","schema_version":1,"features":{"connect_lifecycle":false}}
            """.trimIndent(),
            connectHandler = { _ -> error("connect should not be called without capability") },
        )
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.open().getOrThrow()

            assertIs<ConnectLifecycleUnsupportedException>(client.attach("user-1").exceptionOrNull())
            Unit
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun initializationId_isClearedAfterSuccessfulFirstSeedPush() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.SEEDED_FROM_LOCAL,
                actual = client.attach("user-1").getOrThrow(),
                expectedAuthority = AuthorityStatus.PENDING_LOCAL_SEED,
            )

            assertEquals("init-connect", scalarText(db, "SELECT pending_initialization_id FROM _sync_attachment_state"))

            val report = client.pushPending().getOrThrow()

            assertEquals("", scalarText(db, "SELECT pending_initialization_id FROM _sync_attachment_state"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(PushOutcome.COMMITTED, report.outcome)
            assertEquals(AuthorityStatus.AUTHORITATIVE_MATERIALIZED, report.status.authority)
            assertFalse(report.status.pending.hasPendingSyncData)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun open_surfacesPendingRemoteReplace_and_matchingConnectRequiredToFinishRecovery() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.open("device-a").getOrThrow()
            OversqliteAttachmentStateStore(db).persistState(
                OversqliteAttachmentState(
                    currentSourceId = "device-a",
                    bindingState = attachmentBindingAnonymous,
                    attachedUserId = "",
                    schemaName = "",
                    lastBundleSeqSeen = 0,
                    rebuildRequired = false,
                    pendingInitializationId = "",
                ),
            )
            OversqliteOperationStateStore(db).persistState(
                OversqliteOperationState(
                    kind = operationKindRemoteReplace,
                    targetUserId = "user-1",
                    stagedSnapshotId = "snapshot-1",
                    snapshotBundleSeq = 0,
                    snapshotRowCount = 0,
                ),
            )
            assertEquals(
                OpenState.AttachRecoveryRequired("user-1"),
                client.open("device-a").getOrThrow(),
            )
            assertEquals("remote_replace", scalarText(db, "SELECT kind FROM _sync_operation_state"))
            assertIs<ConnectLocalStateConflictException>(client.attach("user-2").exceptionOrNull())
            Unit
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun attach_restartAfterPartialRemoteReplace_restagesSnapshotAndAppliesFreshRemoteState() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        var sessionAttempts = 0
        val server = newLifecycleServer(
            capabilitiesBody = """
                {"protocol_version":"v1","schema_version":1,"features":{"connect_lifecycle":true}}
            """.trimIndent(),
            connectHandler = { _ ->
                200 to """{"resolution":"remote_authoritative"}"""
            },
        ).apply {
            createContext("/sync/snapshot-sessions") { exchange ->
                check(exchange.requestMethod == "POST")
                sessionAttempts += 1
                val body = if (sessionAttempts == 1) {
                    """
                    {
                      "snapshot_id": "snapshot-a",
                      "snapshot_bundle_seq": 9,
                      "row_count": 2,
                      "byte_count": 64,
                      "expires_at": "2099-01-01T00:00:00Z"
                    }
                    """.trimIndent()
                } else {
                    """
                    {
                      "snapshot_id": "snapshot-b",
                      "snapshot_bundle_seq": 12,
                      "row_count": 1,
                      "byte_count": 32,
                      "expires_at": "2099-01-01T00:00:00Z"
                    }
                    """.trimIndent()
                }
                respondJson(exchange, 200, body)
            }
            createContext("/sync/snapshot-sessions/") { exchange ->
                val snapshotId = exchange.requestURI.path.removePrefix("/sync/snapshot-sessions/")
                when {
                    exchange.requestMethod == "DELETE" -> {
                        exchange.sendResponseHeaders(204, -1)
                        exchange.close()
                    }

                    exchange.requestMethod == "GET" && snapshotId == "snapshot-a" -> {
                        when (queryParam(exchange, "after_row_ordinal")) {
                            "0" -> respondJson(
                                exchange,
                                200,
                                """
                                {
                                  "snapshot_id": "snapshot-a",
                                  "snapshot_bundle_seq": 9,
                                  "next_row_ordinal": 1,
                                  "has_more": true,
                                  "rows": [
                                    {
                                      "schema": "main",
                                      "table": "users",
                                      "key": {"id":"user-a"},
                                      "row_version": 2,
                                      "payload": {"id":"user-a","name":"Ada"}
                                    }
                                  ]
                                }
                                """.trimIndent(),
                            )
                            "1" -> {
                                exchange.sendResponseHeaders(500, 0)
                                exchange.responseBody.close()
                            }
                            else -> error("unexpected snapshot-a after_row_ordinal")
                        }
                    }

                    exchange.requestMethod == "GET" && snapshotId == "snapshot-b" -> {
                        respondJson(
                            exchange,
                            200,
                            """
                            {
                              "snapshot_id": "snapshot-b",
                              "snapshot_bundle_seq": 12,
                              "next_row_ordinal": 1,
                              "has_more": false,
                              "rows": [
                                {
                                  "schema": "main",
                                  "table": "users",
                                  "key": {"id":"user-b"},
                                  "row_version": 3,
                                  "payload": {"id":"user-b","name":"Grace"}
                                }
                              ]
                            }
                            """.trimIndent(),
                        )
                    }

                    else -> error("unexpected snapshot request for $snapshotId")
                }
            }
        }
        server.start()
        val http = newHttpClient(server)
        try {
            val firstClient = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            assertEquals(OpenState.ReadyAnonymous, firstClient.open().getOrThrow())

            val firstError = firstClient.attach("user-1").exceptionOrNull()

            assertTrue(firstError != null)
            assertEquals(OpenState.AttachRecoveryRequired("user-1"), firstClient.open().getOrThrow())
            assertEquals("remote_replace", scalarText(db, "SELECT kind FROM _sync_operation_state"))
            assertEquals("snapshot-a", scalarText(db, "SELECT staged_snapshot_id FROM _sync_operation_state"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_snapshot_stage"))

            val restartedClient = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            assertEquals(OpenState.AttachRecoveryRequired("user-1"), restartedClient.open().getOrThrow())
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.USED_REMOTE_STATE,
                actual = restartedClient.attach("user-1").getOrThrow(),
            )

            assertEquals(2, sessionAttempts)
            assertEquals("none", scalarText(db, "SELECT kind FROM _sync_operation_state"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM users WHERE id = 'user-b'"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users WHERE id = 'user-a'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun writeGuardsRejectApplicationWritesDuringDestructiveTransition() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.open().getOrThrow()
            OversqliteOperationStateStore(db).persistState(
                OversqliteOperationState(
                    kind = operationKindRemoteReplace,
                    targetUserId = "user-1",
                ),
            )

            val error = runCatching {
                db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            }.exceptionOrNull()

            assertTrue(error != null)
            assertTrue(error.message?.contains("SYNC_TRANSITION_PENDING") == true)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun detach_restartAfterMidCleanupFailure_rollsBackAndLaterDetachesCleanly() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            val sourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")

            db.execSQL("UPDATE _sync_apply_state SET apply_mode = 1 WHERE singleton_key = 1")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            db.execSQL("UPDATE _sync_apply_state SET apply_mode = 0 WHERE singleton_key = 1")
            db.execSQL(
                """
                CREATE TRIGGER fail_detach_attachment_update
                BEFORE UPDATE ON _sync_attachment_state
                BEGIN
                  SELECT RAISE(ABORT, 'simulated detach crash');
                END
                """.trimIndent(),
            )

            val error = client.detach().exceptionOrNull()

            assertTrue(error != null)
            assertTrue(error.message?.contains("simulated detach crash") == true)
            assertEquals("attached", scalarText(db, "SELECT binding_state FROM _sync_attachment_state"))
            assertEquals("user-1", scalarText(db, "SELECT attached_user_id FROM _sync_attachment_state"))
            assertEquals(sourceId, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM users"))

            db.execSQL("DROP TRIGGER fail_detach_attachment_update")

            val restartedClient = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            assertEquals(OpenState.ReadyAttached("user-1"), restartedClient.open(sourceId).getOrThrow())
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.RESUMED_ATTACHED_STATE,
                actual = restartedClient.attach("user-1").getOrThrow(),
            )
            assertEquals(DetachOutcome.DETACHED, restartedClient.detach().getOrThrow())
            assertEquals(OpenState.ReadyAnonymous, restartedClient.open(sourceId).getOrThrow())
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users"))
            assertEquals(sourceId, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun signOut_cancelsPendingRemoteReplace_andReturnsAnonymousState() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.open().getOrThrow()
            db.execSQL(
                """
                INSERT INTO _sync_snapshot_stage(snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload)
                VALUES ('snapshot-1', 0, 'main', 'users', '{"id":"user-1"}', 1, '{"id":"user-1","name":"Ada"}')
                """.trimIndent(),
            )
            OversqliteAttachmentStateStore(db).persistState(
                OversqliteAttachmentState(
                    currentSourceId = "device-a",
                    bindingState = attachmentBindingAnonymous,
                    attachedUserId = "",
                    schemaName = "",
                    lastBundleSeqSeen = 0,
                    rebuildRequired = false,
                    pendingInitializationId = "",
                ),
            )
            OversqliteOperationStateStore(db).persistState(
                OversqliteOperationState(
                    kind = operationKindRemoteReplace,
                    targetUserId = "user-1",
                    stagedSnapshotId = "snapshot-1",
                    snapshotBundleSeq = 1,
                    snapshotRowCount = 1,
                ),
            )

            assertEquals(DetachOutcome.DETACHED, client.detach().getOrThrow())
            assertEquals("anonymous", scalarText(db, "SELECT binding_state FROM _sync_attachment_state"))
            assertEquals("none", scalarText(db, "SELECT kind FROM _sync_operation_state"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun anonymousConnectSyncThenSignOut_roundTripsOneDatabaseInstance() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))

            assertEquals(OpenState.ReadyAnonymous, client.open().getOrThrow())
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.STARTED_EMPTY,
                actual = client.attach("user-1").getOrThrow(),
                expectedAuthority = AuthorityStatus.AUTHORITATIVE_EMPTY,
            )

            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            val report = client.sync().getOrThrow()

            assertEquals(1L, client.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(
                PendingSyncStatus(
                    hasPendingSyncData = false,
                    pendingRowCount = 0,
                    blocksDetach = false,
                ),
                client.syncStatus().getOrThrow().pending,
            )
            assertEquals(PushOutcome.COMMITTED, report.pushOutcome)
            assertEquals(RemoteSyncOutcome.ALREADY_AT_TARGET, report.remoteOutcome)
            assertEquals(AuthorityStatus.AUTHORITATIVE_MATERIALIZED, report.status.authority)
            assertEquals(DetachOutcome.DETACHED, client.detach().getOrThrow())
            assertEquals(OpenState.ReadyAnonymous, client.open().getOrThrow())
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun signOut_clearsManagedRows_forNonDeferrableFkGraph() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createActivityProgramTables(db)
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
                    SyncTable("activity", syncKeyColumnName = "id"),
                    SyncTable("daily_log", syncKeyColumnName = "id"),
                    SyncTable("program_item", syncKeyColumnName = "id"),
                ),
            )

            assertEquals(OpenState.ReadyAnonymous, client.open().getOrThrow())
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.STARTED_EMPTY,
                actual = client.attach("user-1").getOrThrow(),
                expectedAuthority = AuthorityStatus.AUTHORITATIVE_EMPTY,
            )

            db.execSQL("INSERT INTO activity(id, title) VALUES('activity-1', 'Stretch')")
            db.execSQL("INSERT INTO program_item(id, activity_id, title) VALUES('program-1', 'activity-1', 'Day 1')")
            db.execSQL(
                """
                INSERT INTO daily_log(id, activity_id, program_item_id, notes)
                VALUES('log-1', 'activity-1', 'program-1', 'Done')
                """.trimIndent(),
            )
            client.sync().getOrThrow()

            assertEquals(
                PendingSyncStatus(
                    hasPendingSyncData = false,
                    pendingRowCount = 0,
                    blocksDetach = false,
                ),
                client.syncStatus().getOrThrow().pending,
            )
            assertEquals(DetachOutcome.DETACHED, client.detach().getOrThrow())
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM daily_log"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM program_item"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM activity"))
            assertEquals(OpenState.ReadyAnonymous, client.open().getOrThrow())
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun rotateSource_rotatesCurrentSource_andPreservesPendingLocalEdits() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.open().getOrThrow()
            client.attach("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val sourceIdBefore = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            val rotation = client.rotateSource().getOrThrow()

            assertNotEquals(sourceIdBefore, rotation.sourceId)
            assertEquals(rotation.sourceId, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
            assertEquals(rotation.sourceId, scalarText(db, "SELECT replaced_by_source_id FROM _sync_source_state WHERE source_id = '$sourceIdBefore'"))
            assertEquals(1L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '${rotation.sourceId}'"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM users"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))

            val push = client.pushPending().getOrThrow()

            assertEquals(PushOutcome.COMMITTED, push.outcome)
            assertEquals(rotation.sourceId, pushServer.createRequests.last().sourceId)
            assertEquals(1L, pushServer.createRequests.last().sourceBundleId)
            assertEquals("attached", scalarText(db, "SELECT binding_state FROM _sync_attachment_state"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun rotateSource_restartAfterFailedTransaction_keepsPreviousSourceActive() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            val sourceIdBefore = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")

            db.execSQL(
                """
                CREATE TRIGGER fail_rotate_attachment_update
                BEFORE UPDATE ON _sync_attachment_state
                BEGIN
                  SELECT RAISE(ABORT, 'simulated rotate crash');
                END
                """.trimIndent(),
            )

            val error = client.rotateSource("rotate-source-failed").exceptionOrNull()

            assertTrue(error != null)
            assertTrue(error.message?.contains("simulated rotate crash") == true)
            assertEquals(sourceIdBefore, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
            assertEquals("", scalarText(db, "SELECT replaced_by_source_id FROM _sync_source_state WHERE source_id = '$sourceIdBefore'"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM users"))

            db.execSQL("DROP TRIGGER fail_rotate_attachment_update")

            val restartedClient = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            assertEquals(OpenState.ReadyAttached("user-1"), restartedClient.open(sourceIdBefore).getOrThrow())
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.RESUMED_ATTACHED_STATE,
                actual = restartedClient.attach("user-1").getOrThrow(),
            )
            assertEquals(sourceIdBefore, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun rotateSource_restartAfterCommit_usesRotatedSourceOnNextLaunch() = runBlocking {
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
            val sourceIdBefore = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            val rotation = client.rotateSource("rotate-source-committed").getOrThrow()

            val restartedClient = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            assertEquals(OpenState.ReadyAttached("user-1"), restartedClient.open(rotation.sourceId).getOrThrow())
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.RESUMED_ATTACHED_STATE,
                actual = restartedClient.attach("user-1").getOrThrow(),
            )

            db.execSQL("INSERT INTO users(id, name) VALUES('user-2', 'Grace')")
            restartedClient.pushPending().getOrThrow()

            assertNotEquals(sourceIdBefore, rotation.sourceId)
            assertEquals(rotation.sourceId, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
            assertEquals(rotation.sourceId, pushServer.createRequests.last().sourceId)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun rotateSource_isBlocked_whilePendingInitializationLeaseIsActive() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.open().getOrThrow()
            val sourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            OversqliteAttachmentStateStore(db).persistState(
                OversqliteAttachmentState(
                    currentSourceId = sourceId,
                    bindingState = attachmentBindingAttached,
                    attachedUserId = "user-1",
                    schemaName = "main",
                    lastBundleSeqSeen = 0,
                    rebuildRequired = false,
                    pendingInitializationId = "init-1",
                ),
            )
            val error = client.rotateSource().exceptionOrNull()

            assertIs<SourceRotationBlockedException>(error)
            assertEquals(sourceId, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
            assertEquals("init-1", scalarText(db, "SELECT pending_initialization_id FROM _sync_attachment_state"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    private fun newLifecycleServer(
        capabilitiesBody: String,
        connectHandler: (HttpExchange) -> Pair<Int, String>,
    ): HttpServer {
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/sync/capabilities") { exchange ->
            if (exchange.requestMethod != "GET") {
                exchange.sendResponseHeaders(405, -1)
                exchange.close()
                return@createContext
            }
            respondJson(exchange, 200, capabilitiesBody)
        }
        server.createContext("/sync/connect") { exchange ->
            if (exchange.requestMethod != "POST") {
                exchange.sendResponseHeaders(405, -1)
                exchange.close()
                return@createContext
            }
            val (status, body) = connectHandler(exchange)
            respondJson(exchange, status, body)
        }
        return server
    }

    private suspend fun readTriggerNames(db: SafeSQLiteConnection): List<String> {
        return db.prepare(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'trigger' AND tbl_name IN ('users', 'posts')
            ORDER BY name
            """.trimIndent(),
        ).use { st ->
            buildList {
                while (st.step()) {
                    add(st.getText(0))
                }
            }
        }
    }
}

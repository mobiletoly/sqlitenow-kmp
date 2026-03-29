package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.DatabaseMigrations
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteNowDatabase
import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import java.nio.file.Files
import kotlin.io.path.deleteIfExists
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

internal class OversqliteSqliteNowInvalidationTest : CrossTargetSyncTestSupport() {
    @Test
    fun pullToStable_reEmitsReactiveQueriesWithoutManualWiring() = runBlocking {
        val server = MockSyncServer()
        val leaderDb = newDb()
        val leaderHttp = server.newHttpClient()
        withReactiveDatabase { followerDb ->
            val followerHttp = server.newHttpClient()
            val leader = newClient(leaderDb, leaderHttp)
            followerDb.open()
            val follower = newDatabaseClient(followerDb, followerHttp)
            try {
                createUsersAndPostsTables(leaderDb)

                leader.openAndConnect("user-1").getOrThrow()
                follower.openAndConnect("user-1").getOrThrow()
                leader.rebuild(RebuildMode.KEEP_SOURCE).getOrThrow()
                follower.rebuild(RebuildMode.KEEP_SOURCE).getOrThrow()

                val (emissions, collector) = collectFlow(followerDb.userNamesFlow())
                try {
                    assertEquals(emptyList(), receiveNext(emissions))

                    insertUser(leaderDb, "u1", "Ada")
                    leader.pushPending().getOrThrow()
                    follower.pullToStable().getOrThrow()

                    assertEquals(listOf("Ada"), receiveNext(emissions))
                } finally {
                    collector.cancel()
                }
            } finally {
                follower.close()
                leader.close()
                followerHttp.close()
                leaderHttp.close()
                leaderDb.close()
            }
        }
    }

    @Test
    fun connect_remoteAuthoritative_reEmitsReactiveQueries() = runBlocking {
        val server = MockSyncServer()
        val leaderDb = newDb()
        val leaderHttp = server.newHttpClient()
        withReactiveDatabase { followerDb ->
            val followerHttp = server.newHttpClient()
            val leader = newClient(leaderDb, leaderHttp)
            followerDb.open()
            val follower = newDatabaseClient(followerDb, followerHttp)
            try {
                createUsersAndPostsTables(leaderDb)

                leader.openAndConnect("user-1").getOrThrow()
                leader.rebuild(RebuildMode.KEEP_SOURCE).getOrThrow()
                insertUser(leaderDb, "u1", "Ada")
                leader.pushPending().getOrThrow()

                follower.open().getOrThrow()

                val (emissions, collector) = collectFlow(followerDb.userNamesFlow())
                try {
                    assertEquals(emptyList(), receiveNext(emissions))

                    follower.attach("user-1").getOrThrow()

                    assertEquals(listOf("Ada"), receiveNext(emissions))
                } finally {
                    collector.cancel()
                }
            } finally {
                follower.close()
                leader.close()
                followerHttp.close()
                leaderHttp.close()
                leaderDb.close()
            }
        }
    }

    @Test
    fun hydrate_reEmitsReactiveQueries() = runBlocking {
        val server = MockSyncServer()
        val leaderDb = newDb()
        val leaderHttp = server.newHttpClient()
        withReactiveDatabase { followerDb ->
            val followerHttp = server.newHttpClient()
            val leader = newClient(leaderDb, leaderHttp)
            followerDb.open()
            val follower = newDatabaseClient(followerDb, followerHttp)
            try {
                createUsersAndPostsTables(leaderDb)

                leader.openAndConnect("user-1").getOrThrow()
                follower.openAndConnect("user-1").getOrThrow()
                leader.rebuild(RebuildMode.KEEP_SOURCE).getOrThrow()
                follower.rebuild(RebuildMode.KEEP_SOURCE).getOrThrow()

                val (emissions, collector) = collectFlow(followerDb.userNamesFlow())
                try {
                    assertEquals(emptyList(), receiveNext(emissions))

                    insertUser(leaderDb, "u1", "Ada")
                    leader.pushPending().getOrThrow()
                    follower.rebuild(RebuildMode.KEEP_SOURCE).getOrThrow()

                    assertEquals(listOf("Ada"), receiveNext(emissions))
                } finally {
                    collector.cancel()
                }
            } finally {
                follower.close()
                leader.close()
                followerHttp.close()
                leaderHttp.close()
                leaderDb.close()
            }
        }
    }

    @Test
    fun pushPending_committedReplay_reEmitsReactiveQueries() = runBlocking {
        val server = MockSyncServer()
        val leaderHttp = server.newHttpClient()
        withReactiveDatabase { database ->
            database.open()
            val client = newDatabaseClient(database, leaderHttp)
            try {
                client.openAndConnect("user-1").getOrThrow()
                client.rebuild(RebuildMode.KEEP_SOURCE).getOrThrow()

                val (emissions, collector) = collectFlow(database.userNamesFlow())
                try {
                    assertEquals(emptyList(), receiveNext(emissions))

                    database.connection().execSQL("INSERT INTO users(id, name) VALUES('u1', 'Ada')")
                    client.pushPending().getOrThrow()

                    assertEquals(listOf("Ada"), receiveNext(emissions))
                } finally {
                    collector.cancel()
                }
            } finally {
                client.close()
                leaderHttp.close()
            }
        }
    }

    @Test
    fun pushPending_conflictResolutionRewrite_reEmitsReactiveQueries() = runBlocking {
        val server = MockSyncServer()
        val leaderDb = newDb()
        val leaderHttp = server.newHttpClient()
        withReactiveDatabase { database ->
            val followerHttp = server.newHttpClient()
            val leader = newClient(leaderDb, leaderHttp)
            database.open()
            val follower = newDatabaseClient(database, followerHttp)
            try {
                createUsersAndPostsTables(leaderDb)

                leader.openAndConnect("user-1").getOrThrow()
                follower.openAndConnect("user-1").getOrThrow()
                leader.rebuild(RebuildMode.KEEP_SOURCE).getOrThrow()
                follower.rebuild(RebuildMode.KEEP_SOURCE).getOrThrow()

                insertUser(leaderDb, "u1", "Original")
                leader.pushPending().getOrThrow()
                follower.pullToStable().getOrThrow()

                val (emissions, collector) = collectFlow(database.userNamesFlow())
                try {
                    assertEquals(listOf("Original"), receiveNext(emissions))

                    database.connection().execSQL("UPDATE users SET name = 'Local Edit' WHERE id = 'u1'")
                    server.conflictOverride = { row, _, _ ->
                        if (row.table == "users" && row.key["id"] == "u1") {
                            PushConflictDetails(
                                schema = row.schema,
                                table = row.table,
                                key = row.key,
                                op = row.op,
                                baseRowVersion = row.baseRowVersion,
                                serverRowVersion = 99,
                                serverRowDeleted = false,
                                serverRow = buildJsonObject {
                                    put("id", JsonPrimitive("u1"))
                                    put("name", JsonPrimitive("Server Wins"))
                                },
                            )
                        } else {
                            null
                        }
                    }

                    follower.pushPending().getOrThrow()

                    assertEquals(listOf("Server Wins"), receiveNext(emissions))
                } finally {
                    server.conflictOverride = null
                    collector.cancel()
                }
            } finally {
                follower.close()
                leader.close()
                followerHttp.close()
                leaderHttp.close()
                leaderDb.close()
            }
        }
    }

    @Test
    fun sync_invalidatesUsingUnionOfPushAndPullChanges() = runBlocking {
        val server = MockSyncServer()
        val leaderDb = newDb()
        val leaderHttp = server.newHttpClient()
        withReactiveDatabase { database ->
            val followerHttp = server.newHttpClient()
            val leader = newClient(leaderDb, leaderHttp)
            database.open()
            val follower = newDatabaseClient(database, followerHttp)
            try {
                createUsersAndPostsTables(leaderDb)

                leader.openAndConnect("user-1").getOrThrow()
                follower.openAndConnect("user-1").getOrThrow()
                leader.rebuild(RebuildMode.KEEP_SOURCE).getOrThrow()
                follower.rebuild(RebuildMode.KEEP_SOURCE).getOrThrow()

                database.connection().execSQL("INSERT INTO users(id, name) VALUES('local-user', 'Local User')")
                database.connection().execSQL("INSERT INTO posts(id, user_id, title) VALUES('p1', 'local-user', 'Local Post')")

                insertUser(leaderDb, "remote-user", "Remote User")
                leader.pushPending().getOrThrow()

                val (emissions, collector) = collectFlow(database.usersAndPostsFlow())
                try {
                    assertEquals(UsersAndPosts(users = listOf("Local User"), posts = listOf("Local Post")), receiveNext(emissions))

                    follower.sync().getOrThrow()

                    val expected = UsersAndPosts(
                        users = listOf("Local User", "Remote User"),
                        posts = listOf("Local Post"),
                    )
                    assertEquals(expected, receiveNext(emissions))
                    withTimeoutOrNull(300) { emissions.receive() }?.let { duplicate ->
                        assertEquals(expected, duplicate)
                    }
                } finally {
                    collector.cancel()
                }
            } finally {
                follower.close()
                leader.close()
                followerHttp.close()
                leaderHttp.close()
                leaderDb.close()
            }
        }
    }

    @Test
    fun recover_reEmitsReactiveQueries() = runBlocking {
        val server = MockSyncServer()
        val leaderDb = newDb()
        val leaderHttp = server.newHttpClient()
        withReactiveDatabase { database ->
            val followerHttp = server.newHttpClient()
            val leader = newClient(leaderDb, leaderHttp)
            database.open()
            val follower = newDatabaseClient(database, followerHttp)
            try {
                createUsersAndPostsTables(leaderDb)

                leader.openAndConnect("user-1").getOrThrow()
                follower.openAndConnect("user-1").getOrThrow()
                leader.rebuild(RebuildMode.KEEP_SOURCE).getOrThrow()
                follower.rebuild(RebuildMode.KEEP_SOURCE).getOrThrow()

                val (emissions, collector) = collectFlow(database.userNamesFlow())
                try {
                    assertEquals(emptyList(), receiveNext(emissions))

                    insertUser(leaderDb, "u1", "Ada")
                    leader.pushPending().getOrThrow()
                    val report = follower.rebuild(
                        mode = RebuildMode.ROTATE_SOURCE,
                        newSourceId = "follower-rebuild-source",
                    ).getOrThrow()

                    assertTrue(report.rotatedSourceId != null && report.rotatedSourceId!!.isNotBlank())
                    assertEquals(listOf("Ada"), receiveNext(emissions))
                } finally {
                    collector.cancel()
                }
            } finally {
                follower.close()
                leader.close()
                followerHttp.close()
                leaderHttp.close()
                leaderDb.close()
            }
        }
    }

    @Test
    fun signOut_success_reEmitsReactiveQueriesAfterManagedCleanup() = runBlocking {
        val server = MockSyncServer()
        val leaderDb = newDb()
        val leaderHttp = server.newHttpClient()
        withReactiveDatabase { database ->
            val followerHttp = server.newHttpClient()
            val leader = newClient(leaderDb, leaderHttp)
            database.open()
            val follower = newDatabaseClient(database, followerHttp)
            try {
                createUsersAndPostsTables(leaderDb)

                leader.openAndConnect("user-1").getOrThrow()
                follower.openAndConnect("user-1").getOrThrow()
                leader.rebuild(RebuildMode.KEEP_SOURCE).getOrThrow()
                follower.rebuild(RebuildMode.KEEP_SOURCE).getOrThrow()
                insertUser(leaderDb, "u1", "Ada")
                leader.pushPending().getOrThrow()
                follower.pullToStable().getOrThrow()

                val (emissions, collector) = collectFlow(database.userNamesFlow())
                try {
                    assertEquals(listOf("Ada"), receiveNext(emissions))

                    assertEquals(DetachOutcome.DETACHED, follower.detach().getOrThrow())

                    assertEquals(emptyList(), receiveNext(emissions))
                } finally {
                    collector.cancel()
                }
            } finally {
                follower.close()
                leader.close()
                followerHttp.close()
                leaderHttp.close()
                leaderDb.close()
            }
        }
    }

    @Test
    fun signOut_blockedDoesNotEmitInvalidation() = runBlocking {
        val server = MockSyncServer()
        val http = server.newHttpClient()
        withReactiveDatabase { database ->
            database.open()
            val client = newDatabaseClient(database, http)
            try {
                client.openAndConnect("user-1").getOrThrow()
                client.rebuild(RebuildMode.KEEP_SOURCE).getOrThrow()

                val (emissions, collector) = collectFlow(database.userNamesFlow())
                try {
                    assertEquals(emptyList(), receiveNext(emissions))

                    database.connection().execSQL("INSERT INTO users(id, name) VALUES('u1', 'Unsynced')")

                    assertEquals(DetachOutcome.BLOCKED_UNSYNCED_DATA, client.detach().getOrThrow())
                    assertNull(withTimeoutOrNull(300) { emissions.receive() })
                } finally {
                    collector.cancel()
                }
            } finally {
                client.close()
                http.close()
            }
        }
    }

    @Test
    fun signOut_metadataOnlyRemoteReplaceCancellationDoesNotEmitInvalidation() = runBlocking {
        val server = MockSyncServer()
        val http = server.newHttpClient()
        withReactiveDatabase { database ->
            database.open()
            val client = newDatabaseClient(database, http)
            try {
                client.open().getOrThrow()
                database.connection().execSQL(
                    """
                    UPDATE _sync_operation_state
                    SET kind = 'remote_replace',
                        target_user_id = 'user-1',
                        staged_snapshot_id = '',
                        snapshot_bundle_seq = 0,
                        snapshot_row_count = 0
                    WHERE singleton_key = 1
                    """.trimIndent(),
                )

                val (emissions, collector) = collectFlow(database.userNamesFlow())
                try {
                    assertEquals(emptyList(), receiveNext(emissions))

                    assertEquals(DetachOutcome.DETACHED, client.detach().getOrThrow())
                    assertNull(withTimeoutOrNull(300) { emissions.receive() })
                } finally {
                    collector.cancel()
                }
            } finally {
                client.close()
                http.close()
            }
        }
    }

    private fun newDatabaseClient(
        database: ReactiveSyncDatabase,
        http: io.ktor.client.HttpClient,
        resolver: Resolver = ServerWinsResolver,
    ): DefaultOversqliteClient {
        return DefaultOversqliteClient(
            db = database.connection(),
            config = OversqliteConfig(
                schema = "main",
                syncTables = listOf(
                    SyncTable("users", syncKeyColumnName = "id"),
                    SyncTable("posts", syncKeyColumnName = "id"),
                ),
            ),
            http = http,
            resolver = resolver,
        )
    }

    private suspend fun withReactiveDatabase(
        block: suspend (ReactiveSyncDatabase) -> Unit,
    ) {
        val dbPath = Files.createTempFile("sqlitenow-oversqlite-reactive", ".db")
        val database = ReactiveSyncDatabase(dbPath.toString())
        try {
            block(database)
        } finally {
            if (database.isOpen()) {
                database.close()
            }
            dbPath.deleteIfExists()
        }
    }

    private fun <T> collectFlow(flow: Flow<T>): Pair<Channel<T>, Job> {
        val emissions = Channel<T>(Channel.UNLIMITED)
        val collector = CoroutineScope(Dispatchers.Default).launch {
            flow.collect { value ->
                emissions.send(value)
            }
        }
        return emissions to collector
    }

    private suspend fun <T> receiveNext(channel: Channel<T>): T {
        return withTimeout(5_000) { channel.receive() }
    }

    private class ReactiveSyncDatabase(
        dbName: String,
    ) : SqliteNowDatabase(dbName, ReactiveSyncMigration()) {
        fun userNamesFlow(): Flow<List<String>> = createReactiveQueryFlow(setOf("users")) {
            connection().prepare("SELECT name FROM users ORDER BY id").use { statement ->
                buildList {
                    while (statement.step()) {
                        add(statement.getText(0))
                    }
                }
            }
        }

        fun usersAndPostsFlow(): Flow<UsersAndPosts> = createReactiveQueryFlow(setOf("users", "posts")) {
            UsersAndPosts(
                users = connection().prepare("SELECT name FROM users ORDER BY id").use { statement ->
                    buildList {
                        while (statement.step()) {
                            add(statement.getText(0))
                        }
                    }
                },
                posts = connection().prepare("SELECT title FROM posts ORDER BY id").use { statement ->
                    buildList {
                        while (statement.step()) {
                            add(statement.getText(0))
                        }
                    }
                },
            )
        }
    }

    private data class UsersAndPosts(
        val users: List<String>,
        val posts: List<String>,
    )

    private class ReactiveSyncMigration : DatabaseMigrations {
        override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int): Int {
            if (currentVersion == -1) {
                conn.execSQL(
                    """
                    CREATE TABLE users (
                        id TEXT PRIMARY KEY NOT NULL,
                        name TEXT NOT NULL
                    )
                    """.trimIndent(),
                )
                conn.execSQL(
                    """
                    CREATE TABLE posts (
                        id TEXT PRIMARY KEY NOT NULL,
                        user_id TEXT NOT NULL REFERENCES users(id),
                        title TEXT NOT NULL
                    )
                    """.trimIndent(),
                )
                return 0
            }
            return currentVersion
        }
    }
}
